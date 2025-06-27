import dask.config as dask_config
import dask.dataframe as dd
import ftplib
import io
import os
import pandas as pd
import re
import hashlib

from earthmover.nodes.node import Node
from earthmover import util

from typing import List, Optional, Tuple
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
    is_hashable: bool = None
    allowed_configs: Tuple[str] = ('debug', 'expect', 'require_rows', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',)

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
            # Get all existing columns
            existing_columns = self.data.columns.tolist()

            # Combine existing columns with optional fields
            all_columns = list(set(existing_columns).union(self.optional_fields))

            # Construct a schema with all columns, initializing optional fields to empty strings
            meta = pd.DataFrame(columns=all_columns)
            meta = meta.astype({col: "object" for col in self.optional_fields})  # Ensure optional fields have correct type

            # Apply to each partition
            self.data = self.data.map_partitions(
                lambda df: df.reindex(columns=all_columns, fill_value=""),
                meta=meta
            )

        super().post_execute(**kwargs)


class FileSource(Source):
    """

    """
    mode: str = 'file'
    is_hashable: bool = True
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',
        'file', 'type', 'columns', 'header_rows', 'colspec_file', 'colspecs', 'colspec_headers', 'rename_cols',
        'encoding', 'sheet', 'object_type', 'match', 'orientation', 'xpath',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = self.error_handler.assert_get_key(self.config, 'file', dtype=str, required=False)

        if not self.file or os.path.isdir(self.file): self.is_hashable = False

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
        
        # Columns are required if a source is optional.
        self.columns_list = self.error_handler.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if self.optional and not self.columns_list:
            self.error_handler.throw(
                f"source `{self.name}` is optional, but does not specify `columns` (which are required in this case)"
            )
            raise

        self.rename_cols = self.error_handler.assert_get_key(self.config, 'rename_cols', dtype=bool, required=False, default=False)
        if self.rename_cols and not self.columns_list:
            self.error_handler.throw(
                f"argument `rename_cols` is set, but does not specify `columns` (which are required in this case)"
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

        # Remote files cannot be size-checked in execute.
        if "://" in self.file:
            self.is_hashable = False

    def execute(self):
        """

        :return:
        """
        super().execute()

        # Verify necessary packages are installed.
        self._verify_packages(self.file_type)

        try:
            # Build an empty dataframe if the path is not populated or if an empty directory is passed (for Parquet files).
            if self.optional and not os.path.exists(self.file) or (os.path.isdir(self.file) and not os.listdir(self.file)):
                self.data = pd.DataFrame(columns=self.columns_list, dtype="string")
            else:
                dask_config.set({'dataframe.convert-string': False})
                self.data = self.read_lambda(self.file, self.config)
                if self.is_hashable:
                    self.size = os.path.getsize(self.file)

            # Rename columns if specified. Note that optional columns are ignored in this case.
            if self.columns_list and self.rename_cols:
                _num_data_cols = len(self.data.columns)
                _num_list_cols = len(self.columns_list)
                if _num_data_cols != _num_list_cols:
                    self.error_handler.throw(
                        f"source file {self.file} specified {_num_list_cols} `columns` but has {_num_data_cols} columns"
                    )
                    raise

                self.data.columns = self.columns_list
                
            # Select columns if specified, being aware of optional fields.
            elif self.columns_list:
                undefined_optional_fields = set(self.optional_fields).difference(self.data.columns)  # Columns to be ignored in the select and added in post_execute()
                expected_cols = list(set(self.columns_list).difference(undefined_optional_fields))   # Subset columns, ignoring undefined optionals.

                undefined_cols = []
                for col in expected_cols:
                    if col not in self.data.columns:
                        undefined_cols.append(col)
                
                if undefined_cols:
                    self.error_handler.throw(
                        f"One or more columns not found in dataset and not marked as optional using `optional_fields`: [{', '.join(undefined_cols)}]"
                    )

                self.data = self.data[expected_cols]

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
            'fwf'     : 'fixedwidth',
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

    def __read_fwf(self, file: str, config: 'YamlMapping'):
        colspec_file = config.get('colspec_file')
        if not colspec_file:
            names = config.get('columns')
            if not names:
                self.error_handler.throw("No `colspec_file` specified for fixedwidth source. In this case, `columns` must be specified, and `colspecs` may be specified, or else will be inferred")

            return dd.read_fwf(file, colspecs=config.get('colspecs', "infer"), header=config.get('header_rows', "infer"), names=names, converters={c:str for c in names})
        try:
            # ensure we find the colspec file relative to the config file that references it (in case of project composition)
            file_format = pd.read_csv(os.path.join(os.path.dirname(self.config.__file__), colspec_file))
        # we need to handle this separately because otherwise EM will report that the source file
        # (instead of the colspec file) could not be found
        except FileNotFoundError:
            self.error_handler.throw(
                f"colspec file '{colspec_file}' not found"
            )

        colspec_headers = config.get("colspec_headers")
        if not colspec_headers:
            self.error_handler.throw(
                "`colspec_headers` must be specified when supplying a colspec file"
            )

        try:
            # name column is required
            name_col = colspec_headers["name"]
        except KeyError:
            self.error_handler.throw(
                "a `name` column must be provided when supplying colspec_headers"
            )

        start_col = colspec_headers.get("start")
        end_col = colspec_headers.get("end")
        width_col = colspec_headers.get("width")
        # pandas does not allow specifying both start/end and widths, but we just let start/end take precedence
        if start_col and end_col:
            use_widths = False
        elif width_col:
            use_widths = True
        else:
            self.error_handler.throw(
                "either `width` or (`start`, `end`) must be specified when supplying colspec_headers"
            )

        names = file_format[name_col]
        header = config.get('header_rows', "infer")
        converters = {c:str for c in names}
        if use_widths:
            widths = list(file_format[width_col])
            return dd.read_fwf(file, widths=widths, header=header, names=names, converters=converters)
        else:
            colspecs = list(zip(file_format.start_index, file_format.end_index))
            return dd.read_fwf(file, colspecs=colspecs, header=header, names=names, converters=converters)

    def _get_read_lambda(self, file_type: str, sep: Optional[str] = None):
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
            'fixedwidth': self.__read_fwf,
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
    
    def get_hash(self) -> str:
        return util.get_file_hash(self.file, 'md5')


class FtpSource(Source):
    """

    """
    mode: str = 'ftp'
    is_hashable: bool = False
    allowed_configs: Tuple[str] = (
        'debug', 'expect', 'show_progress', 'repartition', 'chunksize', 'optional', 'optional_fields',
        'connection', 'query',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.connection = self.error_handler.assert_get_key(self.config, 'connection', dtype=str)
        self.ftp = None  # FTP connection is made during execute.

    def execute(self):
        """
        ftp://user:pass@host:port/path/to/file.ext
        :return:
        """
        super().execute()

        ### Parse the connection string and attempt connection to FTP.
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
    is_hashable: bool = True
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

        except (AttributeError, ImportError):
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
    
    def get_hash(self) -> str:
        # We have to read the data in order to be able to hash it.
        self.execute()
        # SqlSources produce a pandas dataframe, not a dask dataframe... so this works. In the
        # future, we should probably both refactor SqlSource to return a dask dataframe, and
        # this method to iterate over the partitions and hash them each separately. Though ordering
        # of partitions is not guaranteed to be deterministic, so that could be a problem.
        row_hashes = pd.util.hash_pandas_object(self.data, index=False)
        df_hash = hashlib.sha1(row_hashes.values).hexdigest()
        # delete the data so that the real `earthmover run` wouldn't fail:
        self.data = None
        return df_hash
