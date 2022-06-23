import os
import re
import time
import ftplib
from io import BytesIO
import pandas as pd
from earthmover.earthmover_node import Node

class Source(Node):
    def __init__(self, name, source_config, loader):
        super().__init__(name, loader)
        self.meta = self.loader.to_dotdict(source_config)
        self.config = self.loader.to_dotdict(source_config)
        self.type = "source"
        self.loader.error_handler.ctx.update(file=self.loader.config_file, line=self.meta.__line__, node=self, operation=None)
        
        if "expect" in self.meta.keys() and isinstance(self.config.expect, list):
            self.expectations = self.config.expect

        # ftp:
        if "connection" in source_config.keys() and "ftp://" in self.config.connection:
            self.mode = "ftp"
            self.file = source_config["connection"]
            user, passwd, host, port, path = re.match(r'ftp:\/\/(.*?):?(.*?)@?([^:\/]*):?(.*?)\/(.*)', self.file).groups()
            try:
                ftp = ftplib.FTP(host)
                if user and passwd: ftp.login(user=user, passwd=passwd)
                else: ftp.login()
                self.meta.file_size = ftp.size(path)
            except Exception as e:
                self.loader.error_handler.throw("Source file {0} could not be accessed".format(self.file))
            if self.meta.file_size > self.loader.config.memory_limit / 2:
                self.is_chunked = True
                self.reader = None
            self.size = self.meta.file_size
            self.is_loaded = False

        # database connection:
        elif "connection" in source_config.keys() and "query" in source_config.keys():
            self.loader.error_handler.assert_key_exists_and_type_is(self.meta, "connection", str)
            self.loader.error_handler.assert_key_exists_and_type_is(self.meta, "query", str)
            self.mode = "sql"
            self.is_loaded = False
            # replace columns from outer query with count(*), to measure the size of the datasource (and determine is_chunked):
            query = re.sub(r'(^select\s+)(.*?)(\s+from.*$)', r'\1count(*)\3', self.meta.query, count=1, flags=re.M|re.I)
            tmp = pd.read_sql(sql=query, con=self.meta.connection)
            num_records = tmp.iloc[0,0]
            # print("num_records: ", num_records)
            self.meta["num_rows"] = num_records
            if num_records > self.get_chunksize() / 2:
                self.is_chunked = True
                self.page = 0

        # local file:
        elif "file" in source_config.keys():
            self.loader.error_handler.assert_key_exists_and_type_is(self.meta, "file", str)
            self.file = source_config.file
            self.mode = "file"
            self.is_loaded = False
            try:
                self.meta["file_size"] = os.path.getsize(self.file)
            except FileNotFoundError:
                self.loader.error_handler.throw("Source file {0} not found".format(self.file))
            self.size = self.meta["file_size"]
            if self.meta["file_size"] > self.loader.config.memory_limit / 2:
                self.is_chunked = True
                self.reader = None

        else: self.loader.error_handler.throw("sources must specify either a `file` or a `connection` string and `query`")

    def get_chunksize(self):
        mb = 1024 * 1024
        bytes_to_rows_mapping = {
            5 * 1024 * mb: 2 * 10 ** 7, #   >5GB -> 20M
            2 * 1024 * mb: 1 * 10 ** 7, #   >2GB -> 10M
                1024 * mb: 5 * 10 ** 6, #   >1GB -> 5M
                 500 * mb: 2 * 10 ** 6, # >500MB -> 2M
                 200 * mb: 1 * 10 ** 6, # >200MB -> 1M
                 100 * mb: 5 * 10 ** 5, # >100MB -> 500K
                  50 * mb: 2 * 10 ** 5, #  >50MB -> 200K
                  20 * mb: 1 * 10 ** 5, #  >20MB -> 100K
                  10 * mb: 5 * 10 ** 4, #  >10MB -> 50K
                   5 * mb: 2 * 10 ** 4, #   >5MB -> 20K
                   2 * mb: 1 * 10 ** 4, #   >2MB -> 10K
                       mb: 2 * 10 ** 3, #   >1MB -> 5K
        }
        for k,v in bytes_to_rows_mapping.items():
            if self.loader.config.memory_limit > k: return v
        return 5 * 10 ** 2 #  <=1MB -> 500

    def do(self):
        if not self.is_loaded:
            self.loader.error_handler.ctx.update(file=self.loader.config_file, line=self.meta["__line__"], node=self, operation=None)
            
            if self.mode=="file":
                sep = self.loader.get_sep(self.file)
                encoding = "utf8" # default encoding
                if "encoding" in self.meta.keys(): encoding = self.meta["encoding"]
                # read in data:
                try:
                    if self.is_chunked:
                        chunksize = self.get_chunksize()
                        self.reader = pd.read_csv(self.file, sep=sep, dtype=str, encoding=encoding, chunksize=chunksize)
                        self.data = self.reader.get_chunk()
                    else:
                        self.data = pd.read_csv(self.file, sep=sep, dtype=str, encoding=encoding)
                        self.is_done = True
                        self.loader.profile("   source `{0}` loaded ({1} bytes)".format(self.name, self.meta["file_size"]))
                # error handling:
                except FileNotFoundError:
                    self.loader.error_handler.throw("Source file {0} not found".format(self.file))
                except pd.errors.EmptyDataError:
                    self.loader.error_handler.throw("No data in source file {0}".format(self.file))
                except pd.errors.ParserError:
                    self.loader.error_handler.throw("Error parsing source file {0}".format(self.file))
                except Exception as e:
                    self.loader.error_handler.throw("Error with source file {0} ({1})".format(self.file, e))
                # detect categorical columns and convert their type to save memory:
            
            elif self.mode=="ftp":
                # ftp://user:pass@host:port/path/to/file.ext
                user, passwd, host, port, path = re.match(r'ftp:\/\/(.*?):?(.*?)@?([^:\/]*):?(.*?)\/(.*)', self.file).groups()
                try:
                    ftp = ftplib.FTP(host)
                    if user and passwd: ftp.login(user=user, passwd=passwd)
                    else: ftp.login()
                    flo = BytesIO()
                    ftp.retrbinary('RETR ' + path, flo.write)
                    flo.seek(0)
                    self.data = pd.read_csv(flo)
                except Exception as e:
                    self.loader.error_handler.throw("Error with source file {0} ({1})".format(self.file, e))
                self.is_done = True
                self.loader.profile("   source `{0}` loaded {1} rows (via FTP)".format(self.name, len(self.data)))
            
            elif self.mode=="sql":
                try:
                    if self.is_chunked:
                        chunksize = self.get_chunksize()
                        query = self.meta["query"] + " LIMIT " + str(self.page*chunksize) + ", " + str(chunksize)
                        self.data = pd.read_sql(sql=query, con=self.meta.connection)
                    else:
                        self.data = pd.read_sql(sql=self.meta["query"], con=self.meta["connection"])
                        self.is_done = True
                        self.loader.profile("   source `{0}` loaded ({1} rows)".format(self.name, self.meta["num_rows"]))
                except Exception as e:
                    self.loader.error_handler.throw("Source {0} error ({1}); check `connection` and `query`".format(self.file, e))
            
            self.data = self.loader.pack_dataframe(self.data)
            # check expectations
            if "expect" in self.meta.keys():
                self.check_expectations()
            
            self.is_loaded = True
            self.meta["header_row"] = self.data.columns.values.tolist()
            self.meta["num_rows"] = len(self.data)
            self.rows += self.data.shape[0]
            self.cols = self.data.shape[1]
            self.memory_usage = self.data.memory_usage(deep=True).sum()
            self.age = time.time()
            self.loader.profile_memory()

    def clear(self):
        if self.is_done and not self.is_chunked:
            self.is_done = False
            self.data = None
            self.memory_usage = 0
            self.age = 0