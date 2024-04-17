import dask

from earthmover.operations.operation import Operation

from typing import List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


class DistinctRowsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'column', 'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Only 'column' or 'columns' can be populated
        _column  = self.error_handler.assert_get_key(self.config, 'column', dtype=str, required=False)
        _columns = self.error_handler.assert_get_key(self.config, 'columns', dtype=list, required=False)

        if _column:
            self.columns_list = [_column]
        elif _columns:
            self.columns_list = _columns
        else:
            self.columns_list = []

    def execute(self, data: 'DataFrame', **kwargs):
        """

        :return:
        """
        super().execute(data, **kwargs)

        if not set(self.columns_list).issubset(data.columns):
            self.error_handler.throw(
                "one or more columns for checking for distinctness are undefined in the dataset"
            )
            raise

        if not self.columns_list:
            self.columns_list = data.columns

        return data.drop_duplicates(subset=self.columns_list)


class FilterRowsOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'query', 'behavior',
    )

    BEHAVIORS = ["include", "exclude"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query    = self.error_handler.assert_get_key(self.config, 'query', dtype=str)
        self.behavior = self.error_handler.assert_get_key(self.config, 'behavior', dtype=str)

        if self.behavior not in self.BEHAVIORS:
            self.error_handler.throw(
                "`behavior` must be one of [include, exclude]"
            )
            raise

    def execute(self, data: 'DataFrame', **kwargs):
        """

        :return:
        """
        super().execute(data, **kwargs)

        #
        if self.behavior == 'exclude':
            _query = f"not( {self.query} )"
        else:
            _query = self.query

        try:
            data = data.query(_query, engine='python')  #`numexpr` is used by default if installed.

        except Exception as _:
            self.error_handler.throw(
                "error during `filter_rows` operation... check query format?"
            )
            raise

        return data

class SortRowsOperation(Operation):
        """

        """
        allowed_configs: Tuple[str] = (
            'operation', 'repartition',
            'columns', 'descending',
        )

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.columns_list = self.error_handler.assert_get_key(self.config, 'columns', dtype=list)
            self.descending = self.error_handler.assert_get_key(self.config, 'descending', required=False, default=False)

        def execute(self, data: 'DataFrame', **kwargs):
            """

            :return:
            """
            super().execute(data, **kwargs)

            if not set(self.columns_list).issubset(data.columns):
                self.error_handler.throw(
                    "one or more columns for sorting are undefined in the dataset"
                )
                raise

            return data.sort_values(by=self.columns_list, ascending=(not self.descending))
