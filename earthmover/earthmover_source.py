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
        self.skip = False
        self.error_handler.ctx.update(file=self.loader.config_file, line=self.meta.__line__, node=self, operation=None)
        
        if "expect" in self.meta.keys() and isinstance(self.config.expect, list):
            self.expectations = self.config.expect

        # ftp:
        if "connection" in source_config.keys() and "query" not in source_config.keys():
            self.mode = "ftp"
            self.file = source_config["connection"]
            if "required" in source_config.keys() and not source_config["required"] and (
                source_config["connection"] is None or source_config["connection"]==""
                ):
                self.skip = True
            else:
                user, passwd, host, port, path = re.match(r'ftp:\/\/(.*?):?(.*?)@?([^:\/]*):?(.*?)\/(.*)', self.file).groups()
                try:
                    ftp = ftplib.FTP(host)
                    if user and passwd: ftp.login(user=user, passwd=passwd)
                    else: ftp.login()
                    self.meta.file_size = ftp.size(path)
                except Exception as e:
                    self.error_handler.throw("source file {0} could not be accessed".format(self.file))
                if self.meta.file_size > self.loader.config.memory_limit / 2:
                    self.is_chunked = True
                    self.reader = None
                self.size = self.meta.file_size
                self.is_loaded = False
                self.is_remote = True

        # database connection:
        elif "connection" in source_config.keys() and "query" in source_config.keys():
            self.error_handler.assert_key_exists(self.meta, "connection")
            self.error_handler.assert_key_exists(self.meta, "query")
            self.mode = "sql"
            self.is_loaded = False
            self.is_remote = True
            if "required" in source_config.keys() and not source_config["required"] and (
                source_config["connection"] is None or source_config["connection"]==""
                ):
                self.skip = True
            else:
                self.error_handler.assert_key_type_is(self.meta, "connection", str)
                self.error_handler.assert_key_type_is(self.meta, "query", str)
                # replace columns from outer query with count(*), to measure the size of the datasource (and determine is_chunked):
                query = re.sub(r'(^select\s+)(.*?)(\s+from.*$)', r'\1count(*)\3', self.meta.query, count=1, flags=re.M|re.I)
                tmp = pd.read_sql(sql=query, con=self.meta.connection)
                num_records = tmp.iloc[0,0]
                # print("num_records: ", num_records)
                self.meta["num_rows"] = num_records
                if num_records > self.get_chunksize() / 2:
                    self.is_chunked = True
                    self.page = 0

        # file:
        elif "file" in source_config.keys():
            self.error_handler.assert_key_exists(self.meta, "file")
            self.file = source_config.file
            self.mode = "file"
            self.is_loaded = False
            self.is_remote = False
            if "required" in source_config.keys() and not source_config["required"] and (
                source_config["file"] is None or source_config["file"]==""
                ):
                self.skip = True
            else:
                self.error_handler.assert_key_type_is(self.meta, "file", str)
                try:
                    if "://" not in self.file: self.meta["file_size"] = os.path.getsize(self.file)
                    else:
                        self.is_remote = True
                        self.meta["file_size"] = None
                except FileNotFoundError:
                    self.error_handler.throw("Source file {0} not found".format(self.file))
                self.size = self.meta["file_size"]
                if not self.is_remote and self.meta["file_size"] > self.loader.config.memory_limit / 2:
                    self.is_chunked = True
                    self.reader = None

        else: self.error_handler.throw("sources must specify either a `file` or a `connection` string and `query`")

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
            self.error_handler.ctx.update(file=self.loader.config_file, line=self.meta["__line__"], node=self, operation=None)
            
            if self.mode=="file":
                if "type" in self.meta.keys(): file_type = self.meta["type"]
                else: file_type = self.loader.get_type(self.file)
                
                # theoretically we could use `self.loader.get_sep(self.file)`, but not if the user overrode the file_type...
                sep = ""
                if file_type=="csv": sep = ","
                elif file_type=="tsv": sep = "\t"

                encoding = "utf8" # default encoding
                if "encoding" in self.meta.keys(): encoding = self.meta["encoding"]
                # read in data:
                try:
                    if self.is_chunked:
                        chunksize = self.get_chunksize()
                        if sep=="": self.error_handler.throw("({0}) .{1} files do not support chunked processing... increase memory_limit or split the file manually".format(self.file, file_type))
                        else: self.reader = pd.read_csv(self.file, sep=sep, dtype=str, encoding=encoding, chunksize=chunksize)
                        self.data = self.reader.get_chunk()
                    else:
                        if file_type=="parquet": self.data = pd.read_parquet(self.file)
                        elif file_type=="feather": self.data = pd.read_feather(self.file)
                        elif file_type=="orc": self.data = pd.read_orc(self.file)
                        elif file_type=="sas": self.data = pd.read_sas(self.file)
                        elif file_type=="spss": self.data = pd.read_spss(self.file)
                        elif file_type=="stata": self.data = pd.read_stata(self.file)
                        elif file_type=="excel": self.data = pd.read_excel(self.file, sheet_name=(self.meta["sheet"] if "sheet" in self.meta.keys() else 0) )
                        elif file_type=="fixedwidth": self.data = pd.read_fwf(self.file)
                        elif file_type=="xml": self.data = pd.read_xml(self.file, xpath=(self.meta["xpath"] if "xpath" in self.meta.keys() else "./*") )
                        elif file_type=="html": self.data = pd.read_html(self.file, match=(self.meta["match"] if "match" in self.meta.keys() else ".+") )
                        elif file_type=="json": self.data = pd.read_json(self.file, typ=(self.meta["object_type"] if "object_type" in self.meta.keys() else "frame"), orient=(self.meta["orientation"] if "orientation" in self.meta.keys() else "columns")  )
                        else: self.data = pd.read_csv(self.file, sep=sep, dtype=str, encoding=encoding)

                        # rename columns (if specified)
                        if "columns" in self.meta.keys():
                            if type(self.meta["columns"])==list:
                                if len(self.data.columns)==len(self.meta["columns"]):
                                    self.data.columns = self.meta["columns"]
                                else: self.error_handler.throw("source file {0} specified {1} `columns` but has {2} columns".format(self.file, len(self.meta["columns"]), len(self.data.columns) ))
                            else: self.error_handler.throw("source file {0} specified `columns` but not as a list (of new column names)".format(self.file))

                        self.is_done = True
                        self.logger.debug("source `{0}` loaded ({1} bytes, {2} rows)".format(self.name, self.meta["file_size"], len(self.data)))
                # error handling:
                except ImportError:
                    self.error_handler.throw("processing .{0} file {1} requires the pyarrow library... please `pip install pyarrow`".format(file_type, self.file))
                except FileNotFoundError:
                    self.error_handler.throw("source file {0} not found".format(self.file))
                except pd.errors.EmptyDataError:
                    self.error_handler.throw("no data in source file {0}".format(self.file))
                except pd.errors.ParserError:
                    self.error_handler.throw("error parsing source file {0}".format(self.file))
                except Exception as e:
                    self.error_handler.throw("error with source file {0} ({1})".format(self.file, e))
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
                    self.error_handler.throw("error with source file {0} ({1})".format(self.file, e))
                self.is_done = True
                self.logger.debug("source `{0}` loaded {1} rows (via FTP)".format(self.name, len(self.data)))
            
            elif self.mode=="sql":
                try:
                    if self.is_chunked:
                        chunksize = self.get_chunksize()
                        query = self.meta["query"] + " LIMIT " + str(self.page*chunksize) + ", " + str(chunksize)
                        self.data = pd.read_sql(sql=query, con=self.meta.connection)
                    else:
                        self.data = pd.read_sql(sql=self.meta["query"], con=self.meta["connection"])
                        self.is_done = True
                        self.logger.debug("source `{0}` loaded ({1} rows)".format(self.name, self.meta["num_rows"]))
                except Exception as e:
                    self.error_handler.throw("source {0} error ({1}); check `connection` and `query`".format(self.file, e))
            
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