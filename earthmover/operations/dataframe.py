import polars as pl
import pandas as pd

from earthmover.nodes.node import Node
from earthmover.operations.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from polars import LazyFrame as DataFrame


# Map Earthmover's join-type names onto Polars' `how` values.
_JOIN_HOW = {"inner": "inner", "left": "left", "right": "right", "outer": "full"}


class JoinOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition',
        'sources', 'join_type',
        'left_keys', 'left_key', 'right_keys', 'right_key',
        'left_keep_columns', 'left_drop_columns', 'right_keep_columns', 'right_drop_columns',
    )

    INDEX_COL = "__join_index__"
    JOIN_TYPES = ["inner", "left", "right", "outer"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Check joined node
        self.sources: List[str] = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)

        # Check left keys
        _key  = self.error_handler.assert_get_key(self.config, 'left_key', dtype=str, required=False)
        _keys = self.error_handler.assert_get_key(self.config, 'left_keys', dtype=list, required=False)

        if bool(_key) == bool(_keys):  # Fail if both or neither are populated.
            self.error_handler.throw("must define `left_key` or `left_keys`")
            raise

        self.left_keys = _keys or [_key]  # `[None]` evaluates to True

        # Check right keys
        _key  = self.error_handler.assert_get_key(self.config, 'right_key', dtype=str, required=False)
        _keys = self.error_handler.assert_get_key(self.config, 'right_keys', dtype=list, required=False)

        if bool(_key) == bool(_keys):  # Fail if both or neither are populated.
            self.error_handler.throw("must define `right_key` or `right_keys`")
            raise

        self.right_keys = _keys or [_key]  # `[None]` evaluates to True

        # Check join type
        self.join_type = self.config.get('join_type')
        if not self.join_type:
            self.error_handler.throw("must define `join_type`")
            raise

        if self.join_type not in self.JOIN_TYPES:
            self.error_handler.throw(
                f"`join_type` must be one of [inner, left, right, outer], not `{self.join_type}`"
            )
            raise

        # Collect columns
        #   - There is a "if keep - elif drop" block in verify, so doesn't matter if both are populated.
        self.left_keep_cols  = self.error_handler.assert_get_key(self.config, 'left_keep_columns', dtype=list, required=False)
        self.left_drop_cols  = self.error_handler.assert_get_key(self.config, 'left_drop_columns', dtype=list, required=False)
        self.right_keep_cols = self.error_handler.assert_get_key(self.config, 'right_keep_columns', dtype=list, required=False)
        self.right_drop_cols = self.error_handler.assert_get_key(self.config, 'right_drop_columns', dtype=list, required=False)

    def execute(self, data: 'DataFrame', data_mapping: Dict[str, Node], **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, data_mapping=data_mapping, **kwargs)

        # Build left dataset
        left_cols = data.collect_schema().names()

        if self.left_keep_cols:
            if not set(self.left_keep_cols).issubset(left_cols):
                self.error_handler.throw(
                    "columns in `left_keep_columns` are not defined in the dataset"
                )
                raise

            left_cols = list(set(self.left_keep_cols).union(self.left_keys))

        elif self.left_drop_cols:
            if any(col in self.left_keys for col in self.left_drop_cols):
                self.error_handler.throw(
                    "you may not `left_drop_columns` that are part of the `left_key(s)`"
                )
                raise

            left_cols = list(set(left_cols).difference(self.left_drop_cols))

        left_data = data.select(left_cols)

        # Iterate each right dataset
        for source in self.sources:
            right_data = data_mapping[source].data
            right_cols = right_data.collect_schema().names()

            if self.right_keep_cols:
                if not set(self.right_keep_cols).issubset(right_cols):
                    self.error_handler.throw(
                        "columns in `right_keep_columns` are not defined in the dataset"
                    )
                    raise

                right_cols = list(set(self.right_keep_cols).union(self.right_keys))

            elif self.right_drop_cols:
                if any(col in self.right_keys for col in self.right_drop_cols):
                    self.error_handler.throw(
                        "you may not `right_drop_columns` that are part of the `right_key(s)`"
                    )
                    raise

                right_cols = list(set(right_cols).difference(self.right_drop_cols))

            right_data = right_data.select(right_cols)

            # Complete the merge. To match pandas' `merge` semantics: when the left/right key
            # names are identical we join `on` them (a single key column is kept); when they
            # differ we keep both key columns (`coalesce=False`).
            try:
                how = _JOIN_HOW[self.join_type]
                if self.left_keys == self.right_keys:
                    left_data = left_data.join(right_data, how=how, on=self.left_keys)
                else:
                    left_data = left_data.join(
                        right_data, how=how,
                        left_on=self.left_keys, right_on=self.right_keys,
                        coalesce=False,
                    )

            except Exception as _:
                self.error_handler.throw(
                    "error during `join` operation. Check your join keys?"
                )
                raise

        return left_data


class UnionOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'repartition', 'sources', 'fill_missing_columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sources = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)
        self.fill_missing_columns = self.error_handler.assert_get_key(self.config, 'fill_missing_columns', dtype=bool, required=False, default=False)

    def execute(self, data: 'DataFrame', data_mapping: Dict[str, Node], **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, data_mapping=data_mapping, **kwargs)

        for source in self.sources:
            source_data = data_mapping[source].data

            data_cols = data.collect_schema().names()
            source_cols = source_data.collect_schema().names()

            if set(source_cols) != set(data_cols):
                if self.fill_missing_columns:
                    self.logger.debug('Dataframes to union do not share identical columns. Missing columns will be filled with nulls.')
                else:
                    self.error_handler.throw('dataframes to union do not share identical columns')
                    raise

            # Raise an error if duplicate columns are found in either data source.
            # These can cause issues because a DataFrame is returned during union instead of a column.
            if len(source_cols) != len(set(source_cols)) or len(data_cols) != len(set(data_cols)):
                self.error_handler.throw("One or more columns in either dataframe are duplicated. Union cannot be performed consistently.")
                raise

            try:
                # `diagonal_relaxed` aligns columns by name (regardless of order), fills any
                # missing columns with null, and tolerates differing-but-compatible dtypes.
                data = pl.concat([data, source_data], how="diagonal_relaxed")

            except Exception as _:
                self.error_handler.throw(
                    "error during `union` operation... are sources same shape?"
                )
                raise

        return data


class DebugOperation(Operation):
    """
    """
    allowed_configs: Tuple[str] = (
        'operation', 'function', 'rows', 'transpose', 'skip_columns', 'keep_columns'
    )

    DEBUG_FUNCTIONS = ['head', 'tail', 'describe', 'columns']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.func = self.error_handler.assert_get_key(self.config, 'function', dtype=str, required=False, default="head")
        self.rows = self.error_handler.assert_get_key(self.config, 'rows', dtype=int, required=False, default=5)
        self.skip_columns = self.error_handler.assert_get_key(self.config, 'skip_columns', dtype=list, required=False, default=[])
        self.keep_columns = self.error_handler.assert_get_key(self.config, 'keep_columns', dtype=list, required=False, default=None)
        self.transpose = self.error_handler.assert_get_key(self.config, 'transpose', dtype=bool, required=False, default=False)

        if self.func not in self.DEBUG_FUNCTIONS:
            self.error_handler.throw(f"debug type `{self.func}` not defined")

    def execute(self, data: 'DataFrame', data_mapping: Dict[str, Node], **kwargs) -> 'DataFrame':
        """
        :return:
        """
        super().execute(data, data_mapping=data_mapping, **kwargs)

        # construct log message, removing reference to the debug operation
        transformation_name = self.full_name.replace('.operations:debug', '')
        rows_str = ' ' + str(self.rows) if self.func in ['head', 'tail'] else ''
        transpose_str = ', Transpose' if self.transpose else ''
        self.logger.info(f"debug ({self.func}{rows_str}{transpose_str}) for {transformation_name}:")

        data_columns = data.collect_schema().names()

        # `columns` debug does not require column selection or compute
        if self.func == 'columns':
            print(list(data_columns))
            return data  # do not actually transform the data

        # otherwise, subset to desired columns
        keep_columns = self.keep_columns if self.keep_columns else list(data_columns)
        selected_columns = [col for col in list(data_columns) if col in keep_columns and col not in self.skip_columns]
        debug_data = data.select(selected_columns)

        # call function, and display debug info (materialized to pandas for familiar formatting)
        if self.func == 'head':
            debug_pdf = debug_data.head(self.rows).collect().to_pandas()
        elif self.func == 'tail':
            debug_pdf = debug_data.tail(self.rows).collect().to_pandas()
        elif self.func == 'describe':
            debug_pdf = debug_data.collect().to_pandas().describe()

        if self.transpose:
            debug_pdf = debug_pdf.transpose().reset_index(names="column")

        print(debug_pdf.to_string(index=False))
        return data  # do not actually transform the data


class MeltOperation(Operation):
    allowed_configs: Tuple[str] = (
        'operation', 'repartition',
        'id_vars', 'value_vars', 'var_name', 'value_name'
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # columns to keep as identifier variables (can be single column or list)
        self.id_vars = self.config.get('id_vars')
        if isinstance(self.id_vars, str):
            self.id_vars = [self.id_vars]
        elif self.id_vars is not None and not isinstance(self.id_vars, list):
            self.error_handler.throw(f"`id_vars` must be a string or list, got {type(self.id_vars)}")

        # columns to unpivot (can be single column or list)
        self.value_vars = self.config.get('value_vars')
        if isinstance(self.value_vars, str):
            self.value_vars = [self.value_vars]
        elif self.value_vars is not None and not isinstance(self.value_vars, list):
            self.error_handler.throw(f"`value_vars` must be a string or list, got {type(self.value_vars)}")

        # name for the new column that will hold the unpivoted column names
        self.var_name = self.error_handler.assert_get_key(self.config, 'var_name', dtype=str, required=False, default='melt_variable')
        # name for the new column that will hold the values
        self.value_name = self.error_handler.assert_get_key(self.config, 'value_name', dtype=str, required=False, default='melt_value')

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        super().execute(data, **kwargs)

        data_columns = data.collect_schema().names()
        if self.id_vars and not set(self.id_vars).issubset(data_columns):
            missing_cols = set(self.id_vars) - set(data_columns)
            self.error_handler.throw(
                f"columns in `id_vars` are not defined in the dataset: {missing_cols}"
            )

        if self.value_vars and not set(self.value_vars).issubset(data_columns):
            missing_cols = set(self.value_vars) - set(data_columns)
            self.error_handler.throw(
                f"columns in `value_vars` are not defined in the dataset: {missing_cols}"
            )

        try:
            # Polars' `unpivot` is the equivalent of pandas/Dask `melt`. It streams (the
            # destination's streaming sink keeps peak memory bounded even when the melt
            # explodes row count), so no manual column-chunking is needed.
            return data.unpivot(
                index=self.id_vars,
                on=self.value_vars,
                variable_name=self.var_name,
                value_name=self.value_name,
            )
        except Exception as e:
            self.error_handler.throw(
                f"error during `melt` operation: {str(e)}"
            )

class PivotOperation(Operation):
    allowed_configs: Tuple[str] = (
        'operation', 'repartition',
        'rows_by', 'cols_by', 'values'
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # column(s) to use as the new index - what pandas calls "index"
        self.rows_by = self.config.get('rows_by')
        if isinstance(self.rows_by, str):
            self.rows_by = [self.rows_by]
        elif self.rows_by is not None and not isinstance(self.rows_by, list):
            self.error_handler.throw(f"`rows_by` must be a string or list, got {type(self.rows_by)}")

        # column whose unique values will become the new columns - what pandas calls "columns"
        self.cols_by = self.error_handler.assert_get_key(self.config, 'cols_by', dtype=str)
        # column to pivot
        self.values = self.error_handler.assert_get_key(self.config, 'values', dtype=str)

    def execute(self, data: 'DataFrame', **kwargs) -> 'DataFrame':
        super().execute(data, **kwargs)

        data_columns = data.collect_schema().names()
        required_cols = [self.cols_by, self.values]
        if self.rows_by:
            required_cols.extend(self.rows_by)
        if not set(required_cols).issubset(data_columns):
            missing_cols = set(required_cols) - set(data_columns)
            self.error_handler.throw(
                f"required columns for pivot are not defined in the dataset: {missing_cols}"
            )

        try:
            # Pivot is an inherently whole-frame reshape (it must see every row to know the
            # output columns), so it materializes — but we use Polars' native, Arrow-backed
            # `pivot` rather than a pandas `pivot_table` round-trip.
            df = data.collect()

            # Check for uniqueness: index + columns should uniquely identify values
            # This is required for a pivot without aggregation.
            key_cols = (self.rows_by + [self.cols_by]) if self.rows_by else [self.cols_by]
            total_rows = df.height
            unique_rows = df.select(key_cols).unique().height

            if total_rows != unique_rows:
                self.error_handler.throw(
                    f"pivot operation requires unique combinations of index and columns. "
                    f"Found {total_rows} rows but only {unique_rows} unique combinations. "
                    f"Consider using group_by to aggregate the data instead."
                )

            # When no `rows_by` is given, every non-(cols_by/values) column acts as the index
            # (matches the previous behavior of leaving the remaining columns intact).
            index = self.rows_by if self.rows_by else [
                c for c in df.columns if c not in (self.cols_by, self.values)
            ]

            pivoted = df.pivot(
                on=self.cols_by,
                index=index,
                values=self.values,
                aggregate_function="first",  # combinations are unique (checked above)
                sort_columns=True,  # match pandas `pivot_table`'s sorted output columns
            )

            return pivoted.lazy()

        except Exception as e:
            self.error_handler.throw(
                f"error during `pivot` operation: {str(e)}"
            )
