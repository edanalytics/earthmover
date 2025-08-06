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
    

def assert_valid_schema(cls, operation_name, configs):
    '''
    Validate the config schema for a given operation.

    :param cls: the class instance of the operation in question, use `self` when inside that class
    :param operation_name: the name of the operation to perform in PascalCase (e.g., AddColumns, MapValues, etc.)
    :param configs: the config schema for the given operation
    :return: a pydantic model object that contains the config schema
    :raise: ValidationError if the schema is incorrect
    '''

    try:
        config_class = globals().get(f"{operation_name}Config")
        config_model = config_class(**configs.to_dict())
        # print_json(data=config_model.model_dump()) # show successful model configs
        return config_model
    except ValidationError as e:
        print("A compilation error occurred. Input data:")
        print_json(data=configs.to_dict())  # can only print the configs for the specific operation

        dtls = e.errors()[0]  # take the first error only, if multiple
        # Handle missing values
        if dtls['type'] == 'missing':
            cls.logger.warning(f"`{cls.name}` must define `{dtls['loc'][0]}`")
        # Handle unexpected values
        if dtls['type'] == 'extra_forbidden':
            cls.logger.warning(f"Config `{dtls['loc'][0]}` not defined for node `{cls.name}`")
        # Handle other errors (mutually exclusive, etc.)
        else:
            cls.error_handler.throw(dtls['msg'])