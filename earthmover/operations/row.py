from earthmover.operations.operation import Operation

import warnings
from typing import Tuple
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
                # supports both ["col1", "col2"] and ["+col1", "-col2"] kinds of arguments
                    # the previous version of the code only accepted the first format
                    # and sorted in one direction
            self.descending = self.error_handler.assert_get_key(self.config, 'descending', required=False, default=False)

        def execute(self, data: 'DataFrame', **kwargs):
            """

            :return:
            """
            super().execute(data, **kwargs)

            sort_direc_list = [] # True for ascending
                          # False for descending

            clean_columns_list = [] 
                # getting rid of "+" and"-" in front of the column name
                # when the user inputs the columns in the second format

            for col in self.columns_list:

                if col[0] == "-":
                    clean_columns_list.append(col[1:])
                    sort_direc_list.append(False)
                else:
                    clean_columns_list.append(col[1:] if col.startswith("+") else col)
                    sort_direc_list.append(True)

            
            # overwrites any of the "+" that could have been provided
            # and sets all the directions to descending
            if self.descending is True:
                sort_direc_list = [False] * len(sort_direc_list)


            if not set(clean_columns_list).issubset(data.columns):
                self.error_handler.throw(
                    "one or more columns for sorting are undefined in the dataset"
                )
                raise 

            return data.sort_values(by=clean_columns_list, ascending=sort_direc_list)
                # where clean_columns_list is a list of strings 
                # and sort_direc_list is a list of booleans

class LimitRowsOperation(Operation):
        """

        """
        allowed_configs: Tuple[str] = (
            'operation', 'count',
            'offset',
        )

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.count = self.error_handler.assert_get_key(self.config, 'count', dtype=int, required=True)
            self.offset = self.error_handler.assert_get_key(self.config, 'offset', dtype=int, required=False, default=0)

        def execute(self, data: 'DataFrame', **kwargs):
            """

            :return:
            """
            super().execute(data, **kwargs)

            if self.count < 1:
                self.error_handler.throw(
                    "count for a limit operation must be a positive integer"
                )
                raise
            
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", message="Insufficient elements for `head`")
                return data.head(self.count + self.offset, npartitions=-1, compute=False).tail(self.count, compute=False)


class FlattenOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 
        'flatten_column', 'left_wrapper', 'right_wrapper', 'separator', 'value_column', 'trim_whitespace'
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.flatten_column  = self.error_handler.assert_get_key(self.config, 'flatten_column', dtype=str, required=True)
        self.left_wrapper = self.error_handler.assert_get_key(self.config, 'left_wrapper', dtype=str, required=False, default="[\"'")
        self.right_wrapper = self.error_handler.assert_get_key(self.config, 'right_wrapper', dtype=str, required=False, default="\"']")
        self.separator = self.error_handler.assert_get_key(self.config, 'separator', dtype=str, required=False, default=',')
        self.value_column = self.error_handler.assert_get_key(self.config, 'value_column', dtype=str, required=True)
        self.trim_whitespace = self.error_handler.assert_get_key(self.config, 'trim_whitespace', dtype=str, required=False, default=" \t\r\n\"'")

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, **kwargs)

        # Update the meta to reflect the flattened column.
        target_dtypes = data.dtypes.to_dict()
        target_dtypes.update({self.value_column: target_dtypes[self.flatten_column]})
        del target_dtypes[self.flatten_column]

        return data.map_partitions(self.flatten_partition, meta=target_dtypes)

    def flatten_partition(self, df):

        flattened_values_df = (df[self.flatten_column]
            # force to a string before splitting
            .astype("string")

            # trim off `left_wrapper` and `right_wrapper` characters
            .str.lstrip(self.left_wrapper)  
            .str.rstrip(self.right_wrapper)

            # split by `separator` and explode rows
            .str.split(self.separator, expand=True)
            .stack()

            # trim off `trim_whitespace` characters from each of the split values
            .str.strip(self.trim_whitespace)

            # remove the hierarchical index and set the `value_column` name
            .reset_index(level=1)
            .drop('level_1', axis=1)
            .rename(columns={0: self.value_column})
        )

        # join the exploded df to the original and drop `flatten_column` which is no longer needed
        return (df
            .join(flattened_values_df)
            .drop(self.flatten_column, axis=1)
            .reset_index(drop=True)
        )
