import dask.dataframe as dd
import ftplib
import io
import os
import pandas as pd
import re

from earthmover.node import Node
from earthmover import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Source(Node):
    """

    """
    def __new__(cls, name: str, config: dict, *, earthmover: 'Earthmover'):
        """
        Logic for assigning sources to their respective classes.

        :param name:
        :param config:
        :param earthmover:
        """
        if 'connection' in config and 'query' not in config:
            return object.__new__(FtpSource)

        elif 'connection' in config and 'query' in config:
            return object.__new__(SqlSource)

        elif 'file' in config:
            return object.__new__(FileSource)

        else:
            earthmover.error_handler.throw(
                "sources must specify either a `file` and/or `connection` string and `query`"
            )
            raise


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.type = 'source'

        self.allowed_configs.update(['optional'])

        self.mode = None  # Documents which class was chosen.
        self.is_remote = False  # False only for local files.

        # A source can be blank if `optional=True` is specified in its configs.
        # (In this case, `columns` must be specified, and are used to construct an empty
        # dataframe which is passed through to downstream transformations and destinations.)
        self.optional = self.config.get('optional', False)


class FileSource(Source):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'file'

        self.allowed_configs.update([
            'file', 'type', 'columns', 'header_rows',
            'encoding', 'sheet', 'object_type', 'match', 'orientation', 'xpath'
        ])

        self.file = None
        self.file_type = None
        self.read_lambda = None
        self.columns_list = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.file = self.error_handler.assert_get_key(self.config, 'file', dtype=str, required=False)

        #
        if not self.file:
            self.file = ''
            self.file_type = ''
        #
        else:
            self.file_type = self.error_handler.assert_get_key(
                self.config, 'type', dtype=str, required=False,
                default=self._get_filetype(self.file)
            )

            if not self.file_type:
                self.error_handler.throw(
                    f"file `{self.file}` is of unrecognized file format - specify the `type` manually or see documentation for supported file types"
                )
                raise

        #
        if not self.file and self.optional and ('columns' not in self.config or not isinstance(self.config['columns'], list)):
            self.error_handler.throw(
                f"source `{self.name}` is optional and missing, but does not specify `columns` (which are required in this case)"
            )
            raise

        # Initialize the read_lambda.
        _sep = util.get_sep(self.file_type)
        try:
            self.read_lambda = self._get_read_lambda(self.file_type, sep=_sep)

        except Exception as _:
            self.error_handler.throw(
                f"no lambda defined for file type `{self.file_type}`"
            )
            raise

        #
        self.columns_list = self.error_handler.assert_get_key(self.config, 'columns', dtype=list, required=False)

        #
        if "://" in self.file:
            self.is_remote = True

        elif self.file and not self.optional:
            try:
                self.size = os.path.getsize(self.file)
            except FileNotFoundError:
                self.error_handler.throw(
                    f"Source file {self.file} not found"
                )
                raise


    def verify(self):
        """

        :return:
        """
        if self.columns_list:
            _num_data_cols = len(self.data.columns)
            _num_list_cols = len(self.columns_list)
            if _num_data_cols != _num_list_cols:
                self.error_handler.throw(
                    f"source file {self.file} specified {_num_list_cols} `columns` but has {_num_data_cols} columns"
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

            self.verify()  # Verify the column list provided matches the number of columns in the dataframe.

            if self.columns_list:
                self.data.columns = self.columns_list

            self.logger.debug(
                f"source `{self.name}` loaded"
            )

        # error handling:
        except ImportError:
            self.error_handler.throw(
                f"processing .{self.file_type} file {self.file} requires the pyarrow library... please `pip install pyarrow`"
            )
        except FileNotFoundError:
            self.error_handler.throw(
                f"source file {self.file} not found"
            )
        except Exception as err:
            self.error_handler.throw(
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
            'sas'       : lambda file, _     : pd.read_sas(file),
            'spss'      : lambda file, _     : pd.read_spss(file),
            'stata'     : lambda file, _     : pd.read_stata(file),
            'xml'       : lambda file, config: pd.read_xml(file, xpath=config.get('xpath', "./*")),
            'tsv'       : lambda file, config: dd.read_csv(file, sep=sep, dtype=str, encoding=config.get('encoding', "utf8"), keep_default_na=False, skiprows=__get_skiprows(config)),
        }
        return read_lambda_mapping.get(file_type)



class FtpSource(Source):
    """

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'ftp'
        self.is_remote = True

        self.allowed_configs.update(['connection', 'query'])

        self.connection = None
        self.ftp = None
        self.file = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.connection = self.error_handler.assert_get_key(self.config, 'connection', dtype=str)

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
            self.error_handler.throw(
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
            self.error_handler.throw(
                f"error with source file {self.file} ({err})"
            )
            raise

        self.logger.debug(
            f"source `{self.name}` loaded (via FTP)"
        )



class SqlSource(Source):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'sql'
        self.is_remote = True

        self.allowed_configs.update(['connection', 'query'])

        self.connection = None
        self.query = None


    def compile(self):
        """

        :return:
        """
        super().compile()
        self.connection = self.error_handler.assert_get_key(self.config, 'connection', dtype=str)
        self.query = self.error_handler.assert_get_key(self.config, 'query', dtype=str)

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
            self.error_handler.throw(
                f"source {self.name} error ({err}); check `connection` and `query`"
            )
            raise
