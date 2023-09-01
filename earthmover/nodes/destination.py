import jinja2
import os
import pandas as pd
import re

from earthmover import util
from earthmover.node import Node

class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: tuple = ('debug', 'expect', 'show_progress', 'chunksize', 'source',)

    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source = self.get_config('source', dtype=str)
        self.upstream_sources[self.source] = None


class FileDestination(Destination):
    """

    """
    mode: str = 'file'
    allowed_configs: tuple = (
        'debug', 'expect', 'show_progress', 'chunksize', 'source',
        'template', 'extension', 'linearize', 'header', 'footer',
    )

    EXP = re.compile(r"\s+")
    TEMPLATED_COL = "____OUTPUT____"

    def __init__(self, *args, output_dir: str, macros: str, **kwargs):
        super().__init__(*args, **kwargs)
        self.output_dir: str = output_dir
        self.macros: str = macros

        self.file: str = None
        self.template: str = None
        self.jinja_template: jinja2.Template = None
        self.header: str = None
        self.footer: str = None
        self.extension: str = None
        self.linearize: bool = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.template = self.get_config('template', dtype=str)
        self.header = self.get_config('header', "", dtype=str)
        self.footer = self.get_config('footer', "", dtype=str)
        self.extension = self.get_config('extension', "", dtype=str)  # otherwise, assume filename has extension
        self.linearize = self.get_config('linearize', True, dtype=bool)

        filename = f"{self.name}.{self.extension}" if self.extension else self.name
        self.file = os.path.join(self.output_dir, filename)

        #
        try:
            with open(self.template, 'r', encoding='utf-8') as fp:
                template_string = fp.read()

        except Exception as err:
            self.logger.critical(
                f"`template` file {self.template} cannot be opened ({err})"
            )
            raise

        if self.linearize:
            template_string = self.EXP.sub(" ", template_string)  # Replace multiple spaces with a single space.

        #
        try:
            self.jinja_template = util.build_jinja_template(template_string, macros=self.macros)

        except Exception as err:
            self.logger.critical(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )
            raise

    def execute(self):
        """

        :return:
        """
        super().execute()

        # this renders each row without having to itertuples() (which is much slower)
        # (meta=... below is how we prevent dask warnings that it can't infer the output data type)
        self.data = (
            self.upstream_sources[self.source].data
                .fillna('')
                .map_partitions(lambda x: x.apply(self.render_row, axis=1), meta=pd.Series('str'))
        )

        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        with open(self.file, 'w', encoding='utf-8') as fp:
            self.logger.debug(f"writing output file `{self.file}`...")

            if self.header:
                fp.write(self.header + "\n")

            for row in self.data.compute():
                fp.write(row + "\n")

            if self.footer:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)

    def render_row(self, row):
        _data_tuple = row.to_dict()
        _data_tuple["__row_data__"] = row

        try:
            json_string = self.jinja_template.render(_data_tuple)

        except Exception as err:
            self.logger.critical(
                f"error rendering Jinja template in `template` file {self.template} ({err})"
            )
            raise

        return json_string
