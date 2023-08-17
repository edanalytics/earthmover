import abc

from earthmover.node import Node

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Operation(Node):
    """

    """
    type: str = "operation"
    allowed_configs: tuple = ('operation',)

    def __new__(cls, name: str, config: dict, **kwargs):
        """
        :param name:
        :param config:
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

            'group_by_with_count': groupby_operations.GroupByWithCountOperation,
            'group_by_with_ag': groupby_operations.GroupByWithAggOperation,
            'group_by': groupby_operations.GroupByOperation,
        }

        operation = config.get('operation')
        operation_class = operation_mapping.get(operation)

        if operation_class is None:
            cls.logger.critical(
                f"invalid transformation operation `{operation}`"
            )
            raise

        return object.__new__(operation_class)

    def __init__(self, name: str, config: dict, **kwargs):
        full_name = f"{name}.operations:{config.get('operation')}"
        super().__init__(full_name, config, **kwargs)

        self.source_data_mapping: dict = None

    @abc.abstractmethod
    def execute(self, data: 'DataFrame', *, data_mapping: dict, **kwargs) -> 'DataFrame':
        """
        Operation.execute() takes a DataFrame as input and outputs a DataFrame.
        Operation.execute() uses different arguments than Node.execute().

        In operations, `self.data` should NEVER be called, as this unnecessarily persists data in Operation nodes.

        :param data:
        :param data_mapping:
        :param kwargs:
        :return:
        """
        super().execute()
        pass
