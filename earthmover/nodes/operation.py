import abc

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Operation:
    """

    """
    type: str = "operation"
    allowed_configs: tuple = ('operation',)

    def __new__(cls, name: str, config: dict, *, earthmover: 'Earthmover'):
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


    def __init__(self, name: str, config: dict, *, earthmover: 'Earthmover'):
        self.name = f"{name}.operations:{config.get('operation')}"
        self.config = config

        self.earthmover = earthmover
        self.logger = earthmover.logger
        self.error_handler = earthmover.error_handler

        self.data: 'DataFrame' = None
        self.source_data_mapping: dict = None


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config.__line__, node=self, operation=None
        )

        # Verify all configs provided by the user are specified for the node.
        # (This ensures the user doesn't pass in unexpected or misspelled configs.)
        for _config in self.config:
            if _config not in self.allowed_configs:
                self.logger.warning(
                    f"Config `{_config}` not defined for node `{self.name}`."
                )

        pass

    @abc.abstractmethod
    def execute(self) -> 'DataFrame':
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config.__line__, node=self, operation=None
        )

        pass


    def run(self, data: 'DataFrame', data_mapping: dict):
        self.data = data

        if hasattr(self, 'sources'):
            self.source_data_mapping = {
                source: data_mapping[source].data.copy()
                for source in self.sources
            }

        return self.execute()
