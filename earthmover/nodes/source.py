import dask.dataframe as dd
import ftplib
import io
import os
import pandas as pd
import re

from typing import Callable

from earthmover.node import Node
from earthmover import util


class Source(Node):
    """

    """
    type: str = 'source'
    mode: str = None  # Documents which class was chosen.
    is_remote: bool = None
    allowed_configs: tuple = ('debug', 'expect', 'show_progress', 'chunksize', 'optional',)

    def __new__(cls, name: str, config: dict, **kwargs):
        """
        Logic for assigning sources to their respective classes.

        :param name:
        :param config:
        """
        if 'connection' in config and 'query' not in config:
            return object.__new__(FtpSource)

        elif 'connection' in config and 'query' in config:
            return object.__new__(SqlSource)

        elif 'file' in config:
            return object.__new__(FileSource)

        else:
            cls.logger.critical(
                "all sources must specify either a `file` and/or `connection` string and `query`"
            )
            raise


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # A source can be blank if `optional=True` is specified in its configs.
        # (In this case, `columns` must be specified, and are used to construct an empty
        # dataframe which is passed through to downstream transformations and destinations.)
        self.optional = self.get_config('optional', False, dtype=bool)


    def ensure_dask_dataframe(self):
        """
        Converts a Pandas DataFrame to a Dask DataFrame.
        """
        if isinstance(self.data, pd.DataFrame):
            self.logger.debug(
                f"Casting data in {self.type} node `{self.name}` to a Dask dataframe."
            )
            self.data = dd.from_pandas(
                self.data,
                chunksize=self.chunksize
            )


class FileSource(Source):
    """

    """
    mode: str = 'file'
    is_remote: bool = False
    allowed_configs: tuple = (
        'debug', 'expect', 'show_progress', 'chunksize', 'optional',
        'file', 'type', 'columns', 'header_rows',
        'encoding', 'sheet', 'object_type', 'match', 'orientation', 'xpath',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file: str = None
        self.file_type: str = None
        self.read_lambda: Callable = None
        self.columns_list: list = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.file = self.get_config('file', "", dtype=(str, type(None)))  # Files can be NULL if optional.

        #
        if not self.file:
            self.file = ''
            self.file_type = ''
        #
        else:
            self.file_type = self.get_config('type', self._get_filetype(self.file), dtype=str)

            if not self.file_type:
                self.logger.critical(
                    f"file `{self.file}` is of unrecognized file format - specify the `type` manually or see documentation for supported file types"
                )
                raise

        #
        if not self.file and self.optional and ('columns' not in self.config or not isinstance(self.config['columns'], list)):
            self.logger.critical(
                f"source `{self.name}` is optional and missing, but does not specify `columns` (which are required in this case)"
            )
            raise

        # Initialize the read_lambda.
        _sep = util.get_sep(self.file_type)
        try:
            self.read_lambda = self._get_read_lambda(self.file_type, sep=_sep)

        except Exception as _:
            self.logger.critical(
                f"no lambda defined for file type `{self.file_type}`"
            )
            raise

        #
        self.columns_list = self.get_config('columns', [], dtype=list)

        #
        if "://" in self.file:
            self.is_remote = True

        elif self.file and not self.optional:
            try:
                self.size = os.path.getsize(self.file)
            except FileNotFoundError:
                self.logger.critical(
                    f"Source file {self.file} not found"
                )
                raise

    def execute(self):
        """

        :return:
        """
        super().execute()

        try:
            if not self.file and self.optional:
                self.data = pd.DataFrame(columns = self.columns_list)
            else:
                self.data = self.read_lambda(self.file, self.config)
            self.ensure_dask_dataframe()

            # Verify the column list provided matches the number of columns in the dataframe.
            if self.columns_list:
                _num_data_cols = len(self.data.columns)
                _num_list_cols = len(self.columns_list)
                if _num_data_cols != _num_list_cols:
                    self.logger.critical(
                        f"source file {self.file} specified {_num_list_cols} `columns` but has {_num_data_cols} columns"
                    )
                    raise

            if self.columns_list:
                self.data.columns = self.columns_list

            self.logger.debug(
                f"source `{self.name}` loaded"
            )

        # error handling:
        except ImportError:
            self.logger.critical(
                f"processing .{self.file_type} file {self.file} requires the pyarrow library... please `pip install pyarrow`"
            )
        except FileNotFoundError:
            self.logger.critical(
                f"source file {self.file} not found"
            )
        except Exception as err:
            self.logger.critical(
                f"error with source file {self.file} ({err})"
            )

    @staticmethod
    def _get_filetype(file):
        """
        Determine file type from file extension

        :param file:
        :return:
        """
        ext_mapping = {
            'csv'     : 'csv',
            'dta'     : 'stata',
            'feather' : 'feather',
            'html'    : 'html',
            'json'    : 'json',
            'jsonl'   : 'jsonl',
            'ndjson'  : 'jsonl',
            'odf'     : 'excel',
            'ods'     : 'excel',
            'odt'     : 'excel',
            'orc'     : 'orc',
            'parquet' : 'parquet',
            'pickle'  : 'pickle',
            'pkl'     : 'pickle',
            'sas7bdat': 'sas',
            'sav'     : 'spss',
            'tsv'     : 'tsv',
            'txt'     : 'fixedwidth',
            'xls'     : 'excel',
            'xlsb'    : 'excel',
            'xlsm'    : 'excel',
            'xlsx'    : 'excel',
            'xml'     : 'xml',
        }

        ext = file.lower().rsplit('.', 1)[-1]
        return ext_mapping.get(ext)

    def _get_read_lambda(self, file_type: str, sep: str = None):
        """

        :param file_type:
        :param sep:
        :return:
        """
        # Define any other helpers that will be used below.
        def __get_skiprows(config: dict):
            """ Retrieve or set default for header_rows value for CSV reads. """
            _header_rows = config.get('header_rows', 1)
            return int(_header_rows) - 1  # If header_rows = 1, skip none.


        # We don't want to activate the function inside this helper function.
        read_lambda_mapping = {
            'csv'       : lambda file, config: dd.read_csv(file, sep=sep, dtype=str, encoding=config.get('encoding', "utf8"), keep_default_na=False, skiprows=__get_skiprows(config)),
            'excel'     : lambda file, config: pd.read_excel(file, sheet_name=config.get("sheet", 0), keep_default_na=False),
            'feather'   : lambda file, _     : pd.read_feather(file),
            'fixedwidth': lambda file, _     : dd.read_fwf(file),
            'html'      : lambda file, config: pd.read_html(file, match=config.get('match', ".+"), keep_default_na=False)[0],
            'orc'       : lambda file, _     : dd.read_orc(file),
            'json'      : lambda file, config: dd.read_json(file, typ=config.get('object_type', "frame"), orient=config.get('orientation', "columns")),
            'jsonl'     : lambda file, config: dd.read_json(file, lines=True),
            'parquet'   : lambda file, _     : dd.read_parquet(file),
            'sas'       : lambda file, config: pd.read_sas(file, encoding=config.get('encoding', "utf-8")),
            'spss'      : lambda file, _     : pd.read_spss(file),
            'stata'     : lambda file, _     : pd.read_stata(file),
            'xml'       : lambda file, config: pd.read_xml(file, xpath=config.get('xpath', "./*")),
            'tsv'       : lambda file, config: dd.read_csv(file, sep=sep, dtype=str, encoding=config.get('encoding', "utf8"), keep_default_na=False, skiprows=__get_skiprows(config)),
        }
        return read_lambda_mapping.get(file_type)


class FtpSource(Source):
    """

    """
    mode: str = 'ftp'
    is_remote: bool = True
    allowed_configs: tuple = (
        'debug', 'expect', 'show_progress', 'chunksize', 'optional',
        'connection', 'query',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection: str = None
        self.ftp: ftplib.FTP = None
        self.file: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.connection = self.get_config('connection', dtype=str)

        # There's probably a network builtin to simplify this.
        user, passwd, host, port, self.file = re.match(
            r"ftp://(.*?):?(.*?)@?([^:/]*):?(.*?)/(.*)",
            self.connection
        ).groups()

        try:
            self.ftp = ftplib.FTP(host)

            if user and passwd:
                self.ftp.login(user=user, passwd=passwd)
            else:
                self.ftp.login()

            self.size = self.ftp.size(self.file)

        except Exception as err:
            self.logger.critical(
                f"source file {self.connection} could not be accessed: {err}"
            )

    def execute(self):
        """
        ftp://user:pass@host:port/path/to/file.ext
        :return:
        """
        super().execute()

        try:
            # TODO: Can Dask read from FTP directly without this workaround?
            flo = io.BytesIO()
            self.ftp.retrbinary('RETR ' + self.file, flo.write)
            flo.seek(0)

            self.data = pd.read_csv(flo)
            self.ensure_dask_dataframe()

        except Exception as err:
            self.logger.critical(
                f"error with source file {self.file} ({err})"
            )
            raise

        self.logger.debug(
            f"source `{self.name}` loaded (via FTP)"
        )


class SqlSource(Source):
    """

    """
    mode: str = 'sql'
    is_remote: bool = True
    allowed_configs: tuple = (
        'debug', 'expect', 'show_progress', 'chunksize', 'optional',
        'connection', 'query',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection: str = None
        self.query: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()
        self.connection = self.get_config('connection', dtype=str)
        self.query = self.get_config('query', dtype=str)

        # JK: I turned this off in the Dask refactor. Should it be turned back on?
        # # replace columns from outer query with count(*), to measure the size of the datasource (and determine is_chunked):
        # count_query = re.sub(
        #     r"(^select\s+)(.*?)(\s+from.*$)",
        #     r"\1count(*)\3",
        #     self.query,
        #     count=1,
        #     flags=re.M | re.I
        # )
        # self.size = pd.read_sql(sql=count_query, con=self.connection).iloc[0, 0]

    def execute(self):
        """

        :return:
        """
        super().execute()

        try:
            self.data = pd.read_sql(sql=self.query, con=self.connection)
            self.ensure_dask_dataframe()

            self.logger.debug(
                f"source `{self.name}` loaded (via SQL)"
            )

        except Exception as err:
            self.logger.critical(
                f"source {self.name} error ({err}); check `connection` and `query`"
            )
            raise
