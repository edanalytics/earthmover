from pydantic import BaseModel, ValidationError, ConfigDict, model_validator, create_model
from pydantic_core import PydanticCustomError 
from rich import print_json

from typing import Dict, List, Self
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame

class NodeConfig(BaseModel):
    '''
    The parent class for all node configs
    '''
    debug: bool = False
    expect: List[str] = None
    require_rows: bool | int = False
    show_progress: bool = False
    repartition: bool = False

    model_config = ConfigDict(extra='forbid')


# Dynamically create model for operation configs, which inherits from NodeConfig
OperationConfig = create_model('OperationConfig', __base__=NodeConfig, operation=str)


class AddColumnsConfig(OperationConfig):
    '''
    The specific model for configs of the add columns operation. A child of the OperationConfig model.
    '''
    columns: Dict[str, str]  # TODO: might this be a list of dictionaries as well?


class MapValuesConfig(OperationConfig):
    '''
    The specific model for configs of the add columns operation. A child of the OperationConfig model.
    '''
    column: str = None
    columns: List[str] = None
    mapping: Dict = None
    map_file: str = None

    # Handle mutually exclusive values TODO: move this to the parent-most class and make the values dynamic
    @model_validator(mode='after')  # documentation unclear on why to use 'after' instead of 'before'
    def mutually_exclusive(self) -> Self:
        if (self.column and self.columns) or (not self.column and not self.columns):
            raise PydanticCustomError('mutually_exc', "a `map_values` operation must specify either one `column` or several `columns` to convert, but not both.")
        if (self.mapping and self.map_file) or (not self.mapping and not self.map_file):
            raise PydanticCustomError('mutually_exc', "must define either `mapping` (list of old_value: new_value) or a `map_file` (two-column CSV or TSV), but not both.")
        return self
    

