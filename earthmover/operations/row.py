import numpy as np
import pandas as pd
import polars as pl

from earthmover.operations.operation import Operation

from typing import Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from polars import LazyFrame as DataFrame


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

        data_columns = data.collect_schema().names()
        if not set(self.columns_list).issubset(data_columns):
            self.error_handler.throw(
                "one or more columns for checking for distinctness are undefined in the dataset"
            )
            raise

        # An empty subset means "distinct over all columns".
        subset = self.columns_list or None
        return data.unique(subset=subset, keep='first', maintain_order=True)


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

        # `filter_rows` uses pandas' `query` mini-language (e.g. `col.str.contains(...)`),
        # which has no direct Polars equivalent. We preserve it exactly by evaluating each
        # streaming batch with pandas. Filtering is row-independent, so `streamable=True`
        # lets Polars run this per-batch in the streaming engine (memory stays bounded).
        def _filter(batch: 'pl.DataFrame') -> 'pl.DataFrame':
            return pl.from_pandas(batch.to_pandas().query(_query, engine='python'))

        try:
            data = data.map_batches(_filter, streamable=True)

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


            if not set(clean_columns_list).issubset(data.collect_schema().names()):
                self.error_handler.throw(
                    "one or more columns for sorting are undefined in the dataset"
                )
                raise

            # Polars `descending` is the inverse of the ascending flags. `nulls_last=True`
            # matches pandas' default of sorting NaN/null values to the end.
            descending = [not ascending for ascending in sort_direc_list]
            return data.sort(by=clean_columns_list, descending=descending, nulls_last=True)
                # where clean_columns_list is a list of strings
                # and descending is a list of booleans

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

            # Equivalent to the previous `head(count+offset).tail(count)`: take `count`
            # rows starting at `offset`.
            return data.slice(self.offset, self.count)


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

        # Output schema: original columns minus the flattened column, plus the new value column.
        cols = data.collect_schema().names()
        out_cols = [c for c in cols if c != self.flatten_column] + [self.value_column]
        out_schema = {c: pl.Utf8 for c in out_cols}

        # Exploding a delimited cell into many rows is row-independent, so we run the exact
        # pandas implementation per streaming batch (`streamable=True`).
        def _flatten(batch: 'pl.DataFrame') -> 'pl.DataFrame':
            return pl.from_pandas(self.flatten_partition(batch.to_pandas()))

        return data.map_batches(_flatten, streamable=True, schema=out_schema)

    @staticmethod
    def _stringify_cell(value):
        """
        Render a cell as the string the splitter expects. Plain strings pass through; JSON
        list/array values (e.g. from a JSONL source read as a Polars List) are rendered with
        their Python list repr (comma-separated), matching the previous backend's behavior.
        """
        if isinstance(value, str):
            return value
        if isinstance(value, np.ndarray):
            return str(value.tolist())  # `.tolist()` yields plain Python scalars (e.g. int, not np.int64)
        if isinstance(value, (list, tuple)):
            return str([v.item() if hasattr(v, "item") else v for v in value])
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (ValueError, TypeError):
            pass
        return str(value)

    def flatten_partition(self, df):

        flattened_values = (df[self.flatten_column]
            # force to a string before splitting (handles JSON list cells too)
            .map(self._stringify_cell)
            .astype("string")

            # trim off `left_wrapper` and `right_wrapper` characters
            .str.lstrip(self.left_wrapper)
            .str.rstrip(self.right_wrapper)

            # split by `separator` and explode into one row per value (the index is repeated,
            # which drives the join below). `explode` avoids the NaN-padding that
            # `split(expand=True).stack()` produces.
            .str.split(self.separator)
            .explode()

            # trim off `trim_whitespace` characters from each of the split values
            .str.strip(self.trim_whitespace)

            # name the resulting column
            .rename(self.value_column)
        )

        # join the exploded values back to the original (on index) and drop the now-unneeded
        # `flatten_column`.
        return (df
            .drop(self.flatten_column, axis=1)
            .join(flattened_values)
            .reset_index(drop=True)
        )
