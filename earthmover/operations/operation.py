import abc

from earthmover.nodes.node import Node

from typing import Dict, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame
    from earthmover.earthmover import Earthmover
    from earthmover.yaml_parser import YamlMapping


class Operation(Node):
    """

    """
    type: str = "transformation"
    allowed_configs: Tuple[str] = ('operation', 'repartition',)

    def __new__(cls, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        """
        :param config:
        :param earthmover:
        """
        from earthmover.operations import groupby as groupby_operations
        from earthmover.operations import dataframe as dataframe_operations
        from earthmover.operations import column as column_operations
        from earthmover.operations import row as row_operations

        operation_mapping = {
            'join': dataframe_operations.JoinOperation,
            'union': dataframe_operations.UnionOperation,

            'add_columns': column_operations.AddColumnsOperation,
            'modify_columns': column_operations.ModifyColumnsOperation,
            'duplicate_columns': column_operations.DuplicateColumnsOperation,
            'rename_columns': column_operations.RenameColumnsOperation,
            'drop_columns': column_operations.DropColumnsOperation,
            'keep_columns': column_operations.KeepColumnsOperation,
            'combine_columns': column_operations.CombineColumnsOperation,
            'map_values': column_operations.MapValuesOperation,
            'date_format': column_operations.DateFormatOperation,
            'snake_case_columns': column_operations.SnakeCaseColumnsOperation,

            'distinct_rows': row_operations.DistinctRowsOperation,
            'filter_rows': row_operations.FilterRowsOperation,
            'sort_rows': row_operations.SortRowsOperation,

            'group_by_with_rank': groupby_operations.GroupByWithRankOperation,
            'group_by': groupby_operations.GroupByOperation,
        }

        operation = config.get('operation')
        operation_class = operation_mapping.get(operation)

        if operation_class is None:
            earthmover.error_handler.throw(
                f"invalid transformation operation `{operation}`"
            )
            raise

        return object.__new__(operation_class)

    def __init__(self, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        full_name = f"{name}.operations:{config.get('operation')}"
        super().__init__(full_name, config, earthmover=earthmover)

    @abc.abstractmethod
    def execute(self, data: 'DataFrame', *, data_mapping: Dict[str, Node], **kwargs) -> 'DataFrame':
        """
        Operation.execute() takes a DataFrame as input and outputs a DataFrame.
        This differs from Node.execute(), which mutates and returns self.data.

        In operations, `self.data` should NEVER be called, as this unnecessarily persists data in Operation nodes.

        :param data:
        :param data_mapping:
        :param kwargs:
        :return:
        """
        self.error_handler.ctx.update(
            file=self.config.__file__, line=self.config.__line__, node=self, operation=None
        )

        pass

    def post_execute(self, data: 'DataFrame'):
        """
        Operation.post_execute() takes a DataFrame as input and outputs a DataFrame.
        This differs from Node.post_execute(), which mutates and returns self.data.

        In operations, `self.data` should NEVER be called, as this unnecessarily persists data in Operation nodes.

        :param data:
        :return:
        """

        data = self.opt_repartition(data)

        return data

        # raise NotImplementedError(
        #     "Operation.post_execute() is not permitted! Data is not persisted within Operations."
        # )
