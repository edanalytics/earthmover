import csv
import jinja2
import os
import pandas as pd
import re

from earthmover.nodes.node import Node
from earthmover import util

from typing import Tuple


class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'repartition', 'source',)

    def __new__(cls, *args, **kwargs):
        return object.__new__(FileDestination)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source: str = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None


class FileDestination(Destination):
    """

    """
    mode: str = 'file'
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'source',
        'template', 'extension', 'linearize', 'header', 'footer',
    )

    EXP = re.compile(r"\s+")
    TEMPLATED_COL = "____OUTPUT____"
    DEFAULT_TEMPLATE = """{ {% for col,val in __row_data__.items() %}"{{col}}": {{val|tojson}}{%if not loop.last%}, {%endif%}{% endfor %} }"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template = self.error_handler.assert_get_key(self.config, 'template', dtype=str, required=False)
        self.header = self.config.get("header")
        self.footer = self.config.get("footer")

        #config->extension is optional: if not present, we assume the destination name has an extension
        extension = ""
        if "extension" in self.config:
            extension = f".{self.config['extension']}"
            
        self.file = os.path.join(
            self.earthmover.state_configs['output_dir'],
            f"{self.name}{extension}"
        )

        
    def execute(self, **kwargs):
        """
        
        :return:
        """
        super().execute(**kwargs)

        #
        if self.template:
            try:
                with open(self.template, 'r', encoding='utf-8') as fp:
                    template_string = fp.read()

            except Exception as err:
                self.error_handler.throw(
                    f"`template` file {self.template} cannot be opened ({err})"
                )
                raise
        else: template_string = self.DEFAULT_TEMPLATE

        #
        if self.config.get('linearize', True):
            template_string = self.EXP.sub(" ", template_string)  # Replace multiple spaces with a single space.

        #
        try:
            self.jinja_template = util.build_jinja_template(template_string, macros=self.earthmover.macros)

        except Exception as err:
            self.earthmover.error_handler.throw(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )
            raise

        # this renders each row without having to itertuples() (which is much slower)
        # (meta=... below is how we prevent dask warnings that it can't infer the output data type)
        self.data = (
            self.upstream_sources[self.source].data
                .map_partitions(lambda x: x.apply(self.render_row, axis=1), meta=pd.Series('str'))
        )

        # Repartition before writing, if specified.
        self.data = self.opt_repartition(self.data)

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)

        # Write the optional header, each line, and the optional footer.
        with open(self.file, 'w+', encoding='utf-8') as fp:
            if self.header:
                fp.write(self.header)

            self.data.apply(self.write_row, meta=pd.Series('str'), fp=fp).compute()

            if self.footer:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)

    def write_row(self, row: pd.Series, fp):
        fp.write(row + "\n")
        return None # this wipes out data in the dataframe after it's written, which should save some memory

    def render_row(self, row: pd.Series):
        row = row.astype("string").fillna('')
        _data_tuple = row.to_dict()
        _data_tuple["__row_data__"] = row

        try:
            json_string = self.jinja_template.render(_data_tuple)

        except Exception as err:
            self.error_handler.throw(
                f"error rendering Jinja template in `template` file {self.template} ({err})"
            )
            raise

        return json_string
