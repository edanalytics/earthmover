import abc
import ftplib
import io
import re
import os

import pandas as pd


from earthmover.refactor.nodes.node import Node
from earthmover.refactor import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.refactor.earthmover import Earthmover


class Source(Node):
    """

    """
    def __new__(cls, name: str, config: dict, *, earthmover: Earthmover):
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


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.type = 'source'
        self.mode = None  # Documents which class was chosen.

        self.is_remote = None  # False only for local files.
        self.expectations = None
        self.skip = False  # A source can be turned off if `required=False` is specified in its configs.


    @abc.abstractmethod
    def compile(self):
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config['__line__'], node=self, operation=None
        )

        if isinstance(self.config.get('expect'), list):
            self.expectations = self.config['expect']

        if not self.config.get('required', True):
            self.skip = True

        pass


    @abc.abstractmethod
    def execute(self):
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config["__line__"], node=self, operation=None
        )
        pass



class FileSource(Source):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'file'

        self.file = None
        self.file_type = None
        self.read_lambda = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_type_is(self.config, "file", str)
        self.file = self.config['file']

        # Determine the file type (used to specify the read-lambda).
        self.file_type = self.config.get('type', self.get_filetype(self.file))
        if not self.file_type:
            self.error_handler.throw(
                f"file `{self.file}` is of unrecognized file format - specify the `type` manually or see documentation for supported file types"
            )
            raise

        # Initialize the read_lambda.
        _sep = util.get_sep(self.file_type)  # Inherited from Node
        try:
            self.read_lambda = self.get_read_lambda(self.file_type, sep=_sep)
        except Exception as err:
            self.error_handler.throw(
                f"no lambda defined for file type `{self.file_type}`"
            )
            raise

        # TODO: Make remote files `FtpSource`s
        if "://" in self.file:
            self.is_remote = True
        else:
            try:
                self.size = os.path.getsize(self.file)

            except FileNotFoundError:
                self.error_handler.throw(f"Source file {self.file} not found")
                raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        # Get the read function for this specific file.


        # read in data:
        try:
            self.data = self.read_lambda(self.file, self.config)

            # rename columns (if specified)
            if _columns := self.config.get('columns'):

                if isinstance(_columns, list):
                    if len(self.data.columns) == len(_columns):
                        self.data.columns = _columns
                    else:
                        _data_columns = self.data.columns
                        self.error_handler.throw(
                            f"source file {self.file} specified {len(_data_columns)} `columns` but has {len(_columns)} columns"
                        )
                else:
                    self.error_handler.throw(
                        f"source file {self.file} specified `columns` but not as a list (of new column names)"
                    )

            self.is_done = True
            self.logger.debug(
                f"source `{self.name}` loaded ({self.config['size']} bytes, {self.rows} rows)"
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
        except pd.errors.EmptyDataError:
            self.error_handler.throw(
                f"no data in source file {self.file}"
            )
        except pd.errors.ParserError:
            self.error_handler.throw(
                f"error parsing source file {self.file}"
            )
        except Exception as err:
            self.error_handler.throw(
                f"error with source file {self.file} ({err})"
            )


    @staticmethod
    def get_filetype(file):
        """
        Determine file type from file extension

        :param file:
        :return:
        """
        EXT_MAPPING = {
            'csv' : 'csv',
            'dta' : 'stata',
            'feather': 'feather',
            'html': 'html',
            'json': 'json',
            'odf' : 'excel',
            'ods' : 'excel',
            'odt' : 'excel',
            'orc' : 'orc',
            'parquet': 'parquet',
            'pickle': 'pickle',
            'pkl' : 'pickle',
            'sas7bdat': 'sas',
            'sav' : 'spss',
            'tsv' : 'tsv',
            'txt' : 'fixedwidth',
            'xls' : 'excel',
            'xlsb': 'excel',
            'xlsm': 'excel',
            'xlsx': 'excel',
            'xml' : 'xml',
        }

        ext = file.lower().rsplit('.', 1)[-1]
        return EXT_MAPPING.get(ext)


    @staticmethod
    def get_read_lambda(file_type: str, sep: str = None):
        """

        :param file_type:
        :param sep:
        :return:
        """
        # We don't watn to activate the function inside this helper function.
        READ_LAMBDA_MAPPING = {
            'csv'       : lambda file, config: pd.read_csv(file, sep=sep, dtype=str, encoding=config.get('encoding', "utf8")),
            'excel'     : lambda file, config: pd.read_excel(file, sheet_name=config.get("sheet", 0)),
            'feather'   : lambda file, _     : pd.read_feather(file),
            'fixedwidth': lambda file, _     : pd.read_fwf(file),
            'html'      : lambda file, config: pd.read_html(file, match=config.get('match', ".+")),
            'orc'       : lambda file, _     : pd.read_orc(file),
            'json'      : lambda file, config: pd.read_json(file, typ=config.get('object_type', "frame"), orient=config.get('orientation', "columns")),
            'parquet'   : lambda file, _     : pd.read_parquet(file),
            'sas'       : lambda file, _     : pd.read_sas(file),
            'spss'      : lambda file, _     : pd.read_spss(file),
            'stata'     : lambda file, _     : pd.read_stata(file),
            'xml'       : lambda file, config: pd.read_xml(file, xpath=config.get('xpath', "./*")),
            'tsv'       : lambda file, config: pd.read_csv(file, sep=sep, dtype=str, encoding=config.get('encoding', "utf8")),
        }
        return READ_LAMBDA_MAPPING.get(file_type)



class FtpSource(Source):
    """

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'ftp'

        self.connection = None
        self.ftp = None
        self.file = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_type_is(self.config, "connection", str)
        self.connection = self.config['connection']

        user, passwd, host, port, path = re.match(r'ftp:\/\/(.*?):?(.*?)@?([^:\/]*):?(.*?)\/(.*)', self.connection).groups()
        try:
            self.ftp = ftplib.FTP(host)

            if user and passwd:
                self.ftp.login(user=user, passwd=passwd)
            else:
                self.ftp.login()

            self.file = path
            self.size = self.ftp.size(self.path)

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
            flo = io.BytesIO()
            self.ftp.retrbinary('RETR ' + self.file, flo.write)
            flo.seek(0)
            self.data = pd.read_csv(flo)

        except Exception as err:
            self.error_handler.throw(
                f"error with source file {self.file} ({err})"
            )
            raise

        self.is_done = True
        self.logger.debug(
            f"source `{self.name}` loaded {self.rows} rows (via FTP)"
        )



class SqlSource(Source):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.mode = 'sql'

        self.connection = None
        self.query = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_type_is(self.config, "connection", str)
        self.connection = self.config['connection']

        self.error_handler.assert_key_type_is(self.config, "query", str)
        self.query = self.config['query']

        # replace columns from outer query with count(*), to measure the size of the datasource (and determine is_chunked):
        count_query = re.sub(
            r"(^select\s+)(.*?)(\s+from.*$)",
            r"\1count(*)\3",
            self.query,
            count=1,
            flags=re.M | re.I
        )
        self.size = pd.read_sql(sql=count_query, con=self.connection).iloc[0, 0]


    def execute(self):
        """

        :return:
        """
        super().execute()

        try:
            self.data = pd.read_sql(sql=self.query, con=self.connection)

            self.is_done = True
            self.logger.debug(
                f"source `{self.name}` loaded ({self.rows} rows)"
            )

        except Exception as err:
            self.error_handler.throw(
                f"source {self.name} error ({err}); check `connection` and `query`"
            )
            raise
