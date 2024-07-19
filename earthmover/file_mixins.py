import abc
import dask.dataframe as dd
import pandas as pd

from typing import Dict, Optional


class File:
    """
    
    """
    file_type: Optional[str] = None
    allowed_read_configs = ()

    def __new__(cls, file: str, config: 'YamlMapping', error_handler: 'ErrorHandler'):
        """
        
        """
        mixin_mapping = {
            'csv'       : CSVFile,
            # 'excel'     : ExcelFile,
            # 'feather'   : FeatherFile,
            # 'fixedwidth': FixedWidthFile,
            # 'html'      : HTMLFile,
            # 'orc'       : ORCFile,
            # 'json'      : JsonFile,
            'jsonl'     : JsonlFile,
            'parquet'   : ParquetFile,
            # 'sas'       : SASFile,
            # 'spss'      : SPSFile,
            # 'stata'     : StataFile,
            # 'xml'       : XMLFile,
            'tsv'       : TSVFile,
        }

        file_type = error_handler.assert_get_key(
            config, 'type', dtype=str, required=False,
            default=cls.infer_file_type(file)
        )

        file_class = mixin_mapping.get(file_type)
        if not file_class:
            cls.error_handler.throw(
                f"file `{file}` is of unrecognized file format - specify the `type` manually or see documentation for supported file types"
            )
            raise

        return object.__new__(file_class)

    def __init__(self, file: str, config: 'YamlMapping', error_handler: 'ErrorHandler'):
        self.file = file
        self.config = config
        self.error_handler = error_handler

    @abc.abstractmethod
    def read(self, **kwargs):
        raise NotImplementedError(
            "This filetype does not have a defined read!"
        )
    
    @abc.abstractmethod
    def write(self, **kwargs):
        raise NotImplementedError(
            "This filetype does not have a defined write!"
        )
    
    @abc.abstractmethod
    def validate_read_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        raise NotImplementedError(
            "This filetype does not have a defined read!"
        )
    
    @abc.abstractmethod
    def validate_write_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        raise NotImplementedError(
            "This filetype does not have a defined write!"
        )
    
    @classmethod
    def infer_file_type(cls, file: str):
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



class CSVFile(File):
    file_type: str = 'csv'
    allowed_read_configs = (
        "sep", "encoding", "header_rows",
    )

    def validate_read_configs(self, configs: 'YamlMapping') -> Dict[str, object]:    
        return {
            "dtype": str,
            "keep_default_na": False,
            "sep": ',',
            "encoding": self.error_handler.assert_get_key(configs, 'encoding', dtype=str, required=False, default="utf8"),
            "skiprows": self.error_handler.assert_get_key(configs, 'header_rows', dtype=int, required=False, default=1) - 1,  # If header_rows = 1, skip none.
            # Maybe TODO: quote and escape chars
        }
    
    def read(self, **kwargs):
        return dd.read_csv(self.file, **kwargs)


class TSVFile(CSVFile):
    file_type: str = 'tsv'

    def validate_read_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        kwargs = super().validate_read_configs(configs)
        kwargs['sep'] = '\t'
        return kwargs


class JsonlFile(File):
    file_type: str = 'jsonl'

    def validate_read_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        return {
            "lines": True
        }
    
    def read(self, **kwargs):
        return dd.read_json(self.file, **kwargs)
    
    def validate_write_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        pass

    def write(self, **kwargs):
        return dd.to_json(self.file, **kwargs)     


class ParquetFile(File):
    file_type: str = 'parquet'

    def validate_read_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        return {}
    
    def read(self, **kwargs):
        return dd.read_parquet(self.file, **kwargs)

    def validate_write_configs(self, configs: 'YamlMapping') -> Dict[str, object]:
        return {
            "append": self.error_handler.assert_get_key(configs, 'append', dtype=bool, required=False, default=False),
            "overwrite": self.error_handler.assert_get_key(configs, 'overwrite', dtype=bool, required=False, default=True),
        }
    
    def write(self, **kwargs):
        return dd.to_parquet(self.file, **kwargs)
    