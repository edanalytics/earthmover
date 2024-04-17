import dask.dataframe as dd
import ftplib
import io
import os
import pandas as pd
import re

from earthmover.nodes.node import Node
from earthmover import util

from typing import Callable, List, Optional, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame
    from earthmover.earthmover import Earthmover
    from earthmover.yaml_parser import YamlMapping


class Source(Node):
    """

    """
    type: str = 'source'
    mode: str = None  # Documents which class was chosen.
    is_remote: bool = None
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',)

    NUM_ROWS_PER_CHUNK: int = 1000000

    def __new__(cls, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
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
        self.chunksize: int = self.error_handler.assert_get_key(self.config, 'chunksize', dtype=int, required=False, default=self.NUM_ROWS_PER_CHUNK)

        # A source can be blank if `optional=True` is specified in its configs.
        # (In this case, `columns` must be specified, and are used to construct an empty
        # dataframe which is passed through to downstream transformations and destinations.)
        self.optional: bool = self.config.get('optional', False)

        # Optional fields can be defined to be added as null columns if not present in the DataFrame.
        self.optional_fields: List[str] = self.config.get('optional_fields', [])

    def post_execute(self, **kwargs):
        """

        :param kwargs:
        :return:
        """
        if isinstance(self.data, pd.DataFrame):
            self.logger.debug(
                f"Casting data in {self.type} node `{self.name}` to a Dask dataframe."
            )
            self.data = dd.from_pandas(self.data, chunksize=self.chunksize)

        self.data = self.opt_repartition(self.data)  # Repartition if specified.

        # Add missing columns if defined under `optional_fields`.
        if self.optional_fields:
            for field in self.optional_fields:
                if field not in self.data.columns:
                    self.logger.debug(f"Optional column will be added to dataset: '{field}'")
                    self.data[field] = ""  # Default to empty string.

        super().post_execute(**kwargs)


class FileSource(Source):
    """

    """
    mode: str = 'file'
    is_remote: bool = False
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',
        'file', 'type', 'columns', 'header_rows',
        'encoding', 'sheet', 'object_type', 'match', 'orientation', 'xpath',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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

    def execute(self):
        """

        :return:
        """
        super().execute()

        # Verify necessary packages are installed.
        self._verify_packages(self.file_type)

        try:
            if not self.file and self.optional:
                self.data = pd.DataFrame(columns=self.columns_list, dtype="string")
            else:
                self.data = self.read_lambda(self.file, self.config)

            # Verify the column list provided matches the number of columns in the dataframe.
            if self.columns_list:
                _num_data_cols = len(self.data.columns)
                _num_list_cols = len(self.columns_list)
                if _num_data_cols != _num_list_cols:
                    self.error_handler.throw(
                        f"source file {self.file} specified {_num_list_cols} `columns` but has {_num_data_cols} columns"
                    )
                    raise

            if self.columns_list:
                self.data.columns = self.columns_list

            self.logger.debug(
                f"source `{self.name}` loaded"
            )

        # error handling:
        except FileNotFoundError:
            self.error_handler.throw(
                f"source file {self.file} not found"
            )
        except Exception as err:
            self.error_handler.throw(
                f"error with source file {self.file} ({err})"
            )

    @staticmethod
    def _get_filetype(file: str):
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

    @staticmethod
    def _get_read_lambda(file_type: str, sep: Optional[str] = None):
        """

        :param file_type:
        :param sep:
        :return:
        """
        # Define any other helpers that will be used below.
        def __get_skiprows(config: 'YamlMapping'):
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

    def _verify_packages(self, file_type: str):
        """
        Verify necessary packages are installed before attempting load.
        """
        if file_type == 'parquet':
            try:
                import pyarrow
            except ImportError:
                self.error_handler.throw(
                    "loading a Parquet source requires additional libraries... please install using `pip install earthmover[parquet]`"
                )
                raise
        elif file_type == 'excel':
            try:
                import pyarrow
                import openpyxl
            except ImportError:
                self.error_handler.throw(
                    "loading an Excel source requires additional libraries... please install using `pip install earthmover[excel]`"
                )
                raise
        elif file_type == 'xml':
            try:
                import pyarrow
                import lxml
            except ImportError:
                self.error_handler.throw(
                    "loading an XML source requires additional libraries... please install using `pip install earthmover[xml]`"
                )
                raise


class FtpSource(Source):
    """

    """
    mode: str = 'ftp'
    is_remote: bool = True
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',
        'connection', 'query',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
    mode: str = 'sql'
    is_remote: bool = True
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',
        'connection', 'query',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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

        # Verify necessary packages are installed.
        self._verify_packages(self.connection)

        try:
            self.data = self.load_sql_dataframe()

            self.logger.debug(
                f"source `{self.name}` loaded (via SQL)"
            )

        except Exception as err:
            self.error_handler.throw(
                f"source {self.name} error ({err}); check `connection` and `query`"
            )
            raise

    def load_sql_dataframe(self):
        """
        SQLAlchemy 2.x breaks our original method of loading a SQL dataframe.
        Because SQLAlchemy is not a required library, we must account for either version.
        :return:
        """
        try:
            return pd.read_sql(sql=self.query, con=self.connection)

        except AttributeError:
            self.logger.debug(
                "SQLAlchemy 1.x approach failed! Attempting SQLAlchemy 2.x approach..."
            )
            import sqlalchemy

            with sqlalchemy.create_engine(self.connection).connect() as engine_cloud:
                return pd.DataFrame(engine_cloud.execute(sqlalchemy.text(self.query)))

    def _verify_packages(self, connection: str):
        """
        Verify necessary packages are installed before attempting load.
        """
        if connection.startswith('postgres'):
            try:
                import sqlalchemy
                import psycopg2
            except ImportError:
                self.error_handler.throw(
                    "connecting to a Postgres database requires additional libraries... please install using `pip install earthmover[postgres]`"
                )
                raise
        else:
            try:
                import sqlalchemy
            except ImportError:
                self.error_handler.throw(
                    "connecting to a database requires additional libraries... please install using `pip install earthmover[sql]`"
                )
                raise
