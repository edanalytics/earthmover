import csv
import dask.dataframe as dd
import jinja2
import os
import pandas as pd
import re

from earthmover.node import Node
from earthmover import util

from typing import Tuple


class Destination(Node):
    """

    """
    type: str = 'destination'
    mode: str = None  # Documents which class was chosen.
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'partition_size', 'source',)

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
        'debug', 'expect', 'show_progress', 'partition_size', 'source',
        'template', 'extension', 'linearize', 'header', 'footer',
    )

    EXP = re.compile(r"\s+")
    TEMPLATED_COL = "____OUTPUT____"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file: str = None
        self.template: str = None
        self.jinja_template: jinja2.Template = None
        self.header: str = None
        self.footer: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.template = self.error_handler.assert_get_key(self.config, 'template', dtype=str)

        #config->extension is optional: if not present, we assume the destination name has an extension
        extension = ""
        if "extension" in self.config:
            extension = f".{self.config['extension']}"
            
        self.file = os.path.join(
            self.earthmover.state_configs['output_dir'],
            f"{self.name}{extension}"
        )

        #
        try:
            with open(self.template, 'r', encoding='utf-8') as fp:
                template_string = fp.read()

        except Exception as err:
            self.error_handler.throw(
                f"`template` file {self.template} cannot be opened ({err})"
            )
            raise

        #
        if self.config.get('linearize', True):
            template_string = self.EXP.sub(" ", template_string)  # Replace multiple spaces with a single space.

        if 'header' in self.config:
            self.header = self.config["header"]

        if 'footer' in self.config:
            self.footer = self.config["footer"]

        #
        try:
            self.jinja_template = util.build_jinja_template(template_string, macros=self.earthmover.macros)

        except Exception as err:
            self.earthmover.error_handler.throw(
                f"syntax error in Jinja template in `template` file {self.template} ({err})"
            )
            raise

    def execute(self, **kwargs):
        """

        :return:
        """
        super().execute(**kwargs)

        self.data = (
            self.upstream_sources[self.source].data
                .fillna('')
                .map_partitions(lambda x: x.apply(self.render_row, axis=1), meta=pd.Series('str'))
        )

    def post_execute(self, **kwargs):
        """
        Note: there is a bug in dask where one cannot append to an existing file:
        https://docs.dask.org/en/stable/changelog.html#id7

        As a workaround, `header=[self.header]` only when defined.

        :return:
        """
        super().post_execute(**kwargs)

        # Use a second progress bar specifically during writing.
        self.start_progress(logging_message=f"Writing to disk...")

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)

        # Write the optional header, the JSON lines as CSV (for performance), and the optional footer.
        dd.to_csv(
            self.data, filename=self.file, single_file=True, mode='wt',  # Dask arguments
            header=[self.header] if self.header else False,
            # We must write the header directly due to aforementioned bug.
            index=False, escapechar="\x01", sep="\x02", quoting=csv.QUOTE_NONE  # Pandas arguments (use a fake-CSV sep)
        )

        if self.footer:
            with open(self.file, 'a', encoding='utf-8') as fp:
                fp.write(self.footer)

        self.end_progress()  # Exit progress bar context handler.

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)



    def render_row(self, row: pd.Series):
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
