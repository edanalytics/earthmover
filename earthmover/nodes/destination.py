import csv
import dask.dataframe as dd
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

    def __new__(cls, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        if config.get('extension') == 'csv' or config.get('extension') == 'tsv':
            return object.__new__(CsvDestination)
        else:
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.template = self.error_handler.assert_get_key(self.config, 'template', dtype=str)
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
        There is a bug in Dask where `dd.to_csv(mode='a', single_file=True)` fails.
        This is resolved in 2023.8.1: https://docs.dask.org/en/stable/changelog.html#id7 

        :return:
        """
        super().execute(**kwargs)

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

        # Write the optional header, the JSON lines as CSV (for performance), and the optional footer.
        self.data.to_csv(
            filename=self.file, single_file=True, mode='wt', index=False,
            header=[self.header] if self.header else False,  # We must write the header directly due to aforementioned bug.
            escapechar="\x01", sep="\x02", quoting=csv.QUOTE_NONE,  # Pretend to be CSV to improve performance
        )

        if self.footer:
            with open(self.file, 'a', encoding='utf-8') as fp:
                fp.write(self.footer)

        self.logger.debug(f"output `{self.file}` written")
        self.size = os.path.getsize(self.file)

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


class CsvDestination(Destination):
    """

    """
    mode: str = 'csv'  # Documents which class was chosen.
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'source',
        'extension', 'header', 'separator', 'limit', 'keep_columns'
    )


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.header = self.error_handler.assert_get_key(self.config, 'header', dtype=bool, required=False, default=True)
        self.separator = self.error_handler.assert_get_key(self.config, 'separator', dtype=str, required=False, default=",")
        self.limit = self.error_handler.assert_get_key(self.config, 'limit', dtype=int, required=False, default=None)
        self.extension = self.error_handler.assert_get_key(self.config, 'extension', dtype=str, required=False, default="csv")
        self.keep_columns = self.error_handler.assert_get_key(self.config, 'keep_columns', required=False, default=None)
        
        self.file = os.path.join(
            self.earthmover.state_configs['output_dir'],
            f"{self.name}.{self.extension}"
        )   

    def execute(self, **kwargs):
        """
        
        :return:
        """
        super().execute(**kwargs)
        
        self.data = self.upstream_sources[self.source].data

        # Apply limit to dataframe if specified.
        if self.limit:
            if self.limit > len(self.data):
                self.error_handler.throw(
                    f"Limit value exceeds the number of rows in the data"
                )
                raise 
            
            self.data = dd.from_pandas(self.data.head(n=self.limit), npartitions=1)

        # Verify the output directory exists.
        os.makedirs(os.path.dirname(self.file), exist_ok=True)
        self.logger.info(f"Directory created: {os.path.dirname(self.file)}")

        # Subset dataframe columns if specified
        try:
            if self.keep_columns:
                self.data = self.data[self.keep_columns]

        except KeyError as e:
            self.error_handler.throw(
                f"Error occurred while subsetting the data: {e.args[0]}"
            )
            raise

        # Change separator to tab if extension is tsv
        if self.extension == 'tsv':
            self.separator = '\t'

        try:
            self.data.to_csv(
                filename=self.file, single_file=True, index=False,
                sep=self.separator, header=self.header
            )
            self.logger.info(f"Output `{self.file}` written")

        except Exception as err:
            self.error_handler.throw(
                f"Error writing data to {self.extension} file: ({err})"
            )
            raise