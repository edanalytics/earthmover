import abc
import dask
import jinja2
import logging
import pandas as pd
import warnings

from dask.diagnostics import ProgressBar

from pydantic import ValidationError, ConfigDict, model_validator, create_model, BaseModel
from pydantic_core import PydanticCustomError 

from rich import print_json

from earthmover import util

from typing import Dict, List, Tuple, Optional, Union, Self
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame
    from earthmover.earthmover import Earthmover
    from earthmover.error_handler import ErrorHandler
    from earthmover.yaml_parser import YamlMapping
    from logging import Logger


class Node:
    """

    """
    type: str = None
    allowed_configs: Dict[str, Tuple] = {
        'debug': (bool, False),
        'expect': (List[str], None),
        'require_rows': (bool | int, False),
        'show_progress': (bool, False),
        'repartition': (bool, False)
    }
    
    def __init__(self, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        self.name: str = name
        self.config: 'YamlMapping' = config
        self.full_name: str = f"${self.type}s.{self.name}"

        self.earthmover: 'Earthmover' = earthmover
        self.logger: 'Logger' = earthmover.logger
        self.error_handler: 'ErrorHandler' = earthmover.error_handler

        self.error_handler.ctx.update(
            file=self.config.__file__, line=self.config.__line__, node=self, operation=None
        )

        self.upstream_sources: Dict[str, Optional['Node']] = {}

        self.data: 'DataFrame' = None

        self.size: int = None
        self.num_rows: int = None
        self.num_cols: int = None

        self.expectations: List[str] = None
        self.require_rows: bool = False
        self.debug: bool = (self.logger.level <= logging.DEBUG)  # Default to Logger's level.

        # Internal Dask configs
        self.partition_size: Union[str, int] = self.config.get('repartition')

        # Optional variables for displaying progress and diagnostics.
        self.show_progress: bool = self.config.get('show_progress', self.earthmover.state_configs["show_progress"])
        self.progress_bar: ProgressBar = ProgressBar(minimum=10, dt=5.0)  # Always instantiate, but only use if `show_progress is True`.
        self.head_was_displayed: bool = False  # Workaround to prevent displaying the head twice when debugging.
    
        # Schema validation using pydantic, TODO: get rid of if statement when all operations support pydantic
        if 'operations:add_columns' in self.full_name or 'operations:map_values' in self.full_name:
            self.pydantic_config = self.assert_valid_schema(self.config)  
        else: self.pydantic_config = None

        # Verify all configs provided by the user are specified for the node.
        # (This ensures the user doesn't pass in unexpected or misspelled configs.)
        # This should be handled by pydantic's schema validation, remove when all operations work with pydantic
        # for _config in self.config:
        #     if _config not in self.allowed_configs:
        #         self.logger.warning(
        #             f"[old_functionality] Config `{_config}` not defined for node `{self.name}`."
        #         )

        # Always check for debug and expectations
        self.debug = self.debug or self.config.get('debug', False)
        self.expectations = self.error_handler.assert_get_key(self.config, 'expect', dtype=list, required=False)
        self.require_rows = int(self.require_rows or self.config.get('require_rows', False))
        if self.require_rows < 0:
            self.error_handler.throw(
                f"Source `{self.full_name}` require_rows cannot be negative"
            )


    @abc.abstractmethod
    def execute(self, **kwargs):
        """
        Node.execute()          :: Saves data into memory
        Operation.execute(data) :: Does NOT save data into memory

        :return:
        """
        self.error_handler.ctx.update(
            file=self.config.__file__, line=self.config.__line__, node=self, operation=None
        )

        # Turn on the progress bar manually.
        if self.show_progress:
            self.logger.info(f"Displaying progress for {self.type} node: {self.name}")
            self.progress_bar.__enter__()  # Open context manager manually to avoid with-clause

        pass

    @abc.abstractmethod
    def post_execute(self, **kwargs):
        """
        Function to run generic logic following execute.

        1. Complete any post-transformations to self.data (currently unused).
        2. Check the dataframe aligns with expectations.
        3. Prepare row and column counts for graphing.
        4. Display row and column counts if debug is True.

        :return:
        """
        # Close context manager manually to avoid with-clause.
        if self.show_progress:
            self.progress_bar.__exit__(None, None, None)

        self.check_expectations(self.expectations)

        # Get lazy row and column counts to display in graph.png.
        if isinstance(self.data, (pd.Series, dask.dataframe.Series)):
            self.num_rows, self.num_cols = self.data.size, 1
        else:
            self.num_rows, self.num_cols = self.data.shape

        # Only actually compute() and count the rows if `require_rows` was defined for this node.
        if self.require_rows > 0:
            self.check_require_rows(self.require_rows)

        # Display row-count and dataframe shape if debug is enabled.
        if self.debug:
            self.display_head()

        pass

    def check_require_rows(self, num_required_rows):
        self.num_rows = dask.compute(self.num_rows)[0]
        if self.num_rows < num_required_rows:
            self.error_handler.throw(
                f"Source `{self.full_name}` failed require_rows >= {num_required_rows}` (only {self.num_rows} rows found)"
            )
        else:
            self.logger.info(
                f"Assertion passed! {self.name}: require_rows >= {num_required_rows}"
            )

    def display_head(self, nrows: int = 5):
        """
        Originally, this outputted twice due to multiple optimization passes: https://github.com/dask/dask/issues/7545
        """
        if self.head_was_displayed:
            return None

        # Turn off UserWarnings when the number of rows is less than the head-size.
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message="Insufficient elements for `head`")

            # Complete all computes at once to reduce duplicate computation.
            self.num_rows, data_head = dask.compute([self.num_rows, self.data.head(nrows)])[0]

            self.logger.info(f"Node {self.name}: {int(self.num_rows)} rows; {self.num_cols} columns")
            with pd.option_context('display.max_columns', None, 'display.width', None):
                print(f"\n{data_head.to_string(index=False)}\n")

            self.head_was_displayed = True  # Mark that state was shown to avoid double-logging.

    def check_expectations(self, expectations: List[str]):
        """

        :return:
        """
        expectation_result_col = "__expectation_result__"

        if expectations:
            result = self.data.copy()

            for expectation in expectations:
                template = jinja2.Template("{{" + expectation + "}}")

                result[expectation_result_col] = result.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=expectation_result_col),
                    template=template,
                    template_str="{{" + expectation + "}}",
                    error_handler = self.error_handler
                )

                num_failed = len(result.query(f"{expectation_result_col}=='False'").index)
                if num_failed > 0:
                    self.error_handler.throw(
                        f"Source `{self.full_name}` failed expectation `{expectation}` ({num_failed} rows fail)"
                    )
                else:
                    self.logger.info(
                        f"Assertion passed! {self.name}: {expectation}"
                    )

    def opt_repartition(self, data: 'DataFrame'):
        if self.partition_size:
            data = data.repartition(partition_size=self.partition_size)
        return data

    def set_upstream_source(self, source_name: str, node: 'Node'):
        """ Upstream sources initialize as strings and are replaced during Earthmover.build_graph(). """
        if source_name not in self.upstream_sources:
            self.error_handler.throw(f"Source {source_name} not found in Node sources list.")
        self.upstream_sources[source_name] = node

    def assert_valid_schema(self, configs: 'YamlMapping'):
        '''
        Validate the config schema for a given operation.

        :param cls: the class instance of the operation in question, use `self` when inside that class
        :param operation_name: the name of the operation to perform in PascalCase (e.g., AddColumns, MapValues, etc.)
        :param configs: the config schema for the given operation
        :return: a pydantic model object that contains the config schema
        :raise: ValidationError if the schema is incorrect
        '''
        # Let user know the schema is being validated
        self.logger.info(f"validating {self.name} schema with pydantic...")
        
        try:
            # Create pydantic model for a generic node
            NodeConfig = self.create_node_config()

            # Add operation to allowed configs if needed
            if 'operation' in self.full_name:
                self.allowed_configs['operation'] = str

            # Create pydantic model for specific operation
            SpecificConfig = create_model('SpecificConfig', __base__=NodeConfig, 
                                          __validators__={
                                              'mutually_exclusive': model_validator(mode='after')(self.mutually_exclusive)
                                              }, 
                                          **self.allowed_configs)
 
            # Create the pydantic model
            config_model = SpecificConfig(**configs.to_dict())
            # print_json(data=config_model.model_dump()) # show successful model configs
            return config_model
        except ValidationError as e:
            self.handle_schema_errors(configs, e)

    def mutually_exclusive(self) -> Self:
        """
        Handle mutually-exclusive fields constraint, used as a `model_validator` in a pydantic model.
        TODO: this currently works only for the `map_values` operation. Must be adapted to work with any

        :return: self
        :raise: ValidationError if the config defines two values that are mutually exclusive     
        """
        if self.operation == 'map_values':
            if (self.column and self.columns) or (not self.column and not self.columns):
                raise PydanticCustomError('mutually_exc', "a `map_values` operation must specify either one `column` or several `columns` to convert, but not both.")
            if (self.mapping and self.map_file) or (not self.mapping and not self.map_file):
                raise PydanticCustomError('mutually_exc', "must define either `mapping` (list of old_value: new_value) or a `map_file` (two-column CSV or TSV), but not both.")
        return self

    def handle_schema_errors(self, configs: 'YamlMapping', e: ValidationError):
        """
        Helper to handle exceptions raised by pydantic's schema validation.

        :param configs: the YAML mapping of the configs for this earthmover project
        :param e: the exception thrown by pydantic
        :raise: the relevant error message
        """
        print("Invalid configs were given. See input:")
        print_json(data=configs.to_dict())  # can only print the configs for the specific operation

        dtls = e.errors()[0]  # take the first error only, if multiple
        # Handle missing values
        if dtls['type'] == 'missing':
            self.error_handler.throw(f"`{self.name}` must define `{dtls['loc'][0]}`")
        # Handle unexpected values
        if dtls['type'] == 'extra_forbidden':
            self.error_handler.throw(f"Config `{dtls['loc'][0]}` not defined for node `{self.name}`")
        # Handle other errors (mutually exclusive, etc.)
        else:
            self.error_handler.throw(dtls['msg'])

    def create_node_config(self):
        """
        Helper to create the generic NodeConfig pydantic model.
        
        :return: the `NodeConfig` class
        """
        NodeConfig: 'BaseModel' = create_model('NodeConfig', 
                            __config__= ConfigDict(extra='forbid'),
                            **Node.allowed_configs
                        )
        return NodeConfig