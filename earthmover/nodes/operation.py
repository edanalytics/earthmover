import abc

from earthmover.node import Node

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Operation(Node):
    """

    """
    def __new__(cls, config: dict, *, earthmover: 'Earthmover'):
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

            'distinct_rows': row_operations.DistinctRowsOperation,
            'filter_rows': row_operations.FilterRowsOperation,

            'group_by_with_count': groupby_operations.GroupByWithCountOperation,
            'group_by_with_ag': groupby_operations.GroupByWithAggOperation,
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


    def __init__(self, config: dict, *, earthmover: 'Earthmover'):
        _name = config.get('operation')
        super().__init__(_name, config, earthmover=earthmover)

        self.type = self.config.get('operation')

        self.allowed_configs.update(['operation', 'sources', 'source'])

        # `source` and `source_list` are mutually-exclusive attributes.
        # `source_list` is for operations with multiple sources (i.e., dataframe operations)
        self.source = self.error_handler.assert_get_key(self.config, 'source', dtype=str, required=False)
        self.source_list = self.error_handler.assert_get_key(self.config, 'sources', dtype=list, required=False)
        self.source_data_list = None  # Retrieved data for operations with multiple sources

        if bool(self.source) == bool(self.source_list):  # Fail if both or neither are populated.
            self.error_handler.throw(
                "A `source` or a list of `sources` must be defined for any operation!"
            )
            raise


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        super().compile()


    def verify(self):
        """
        Because verifications are optional, this is not an abstract method.

        :return:
        """
        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        super().execute()

        # If multiple sources are required for an operation, self.data must be defined in the child class execute().
        if self.source_list:
            self.source_data_list = [
                self.get_source_node(source).data for source in self.source_list
            ]
        else:
            self.data = self.get_source_node(self.source).data

        self.verify()

        pass
