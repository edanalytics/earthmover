import dask.dataframe as dd
import numpy as np
import pandas as pd

from earthmover.nodes.node import Node
from earthmover.operations.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


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
        left_cols = data.columns

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

        left_data = data[left_cols]

        # Iterate each right dataset
        for source in self.sources:
            right_data = data_mapping[source].data
            right_cols = right_data.columns

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

            right_data = right_data[right_cols]

            # Complete the merge, using different logic depending on the partitions of the datasets.
            try:
                left_data = dd.merge(
                    left_data, right_data, how=self.join_type,
                    left_on=self.left_keys, right_on=self.right_keys
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

            if set(source_data.columns) != set(data.columns):
                if self.fill_missing_columns:
                    self.logger.debug('Dataframes to union do not share identical columns. Missing columns will be filled with nulls.')
                else:
                    self.error_handler.throw('dataframes to union do not share identical columns')
                    raise

            # Raise an error if duplicate columns are found in either data source.
            # These can cause `AttributeError: 'DataFrame' object has no attribute 'dtype'` because a DataFrame is returned during union instead of a column.
            if len(source_data.columns) != len(set(source_data.columns)) or len(data.columns) != len(set(data.columns)):
                self.error_handler.throw("One or more columns in either dataframe are duplicated. Union cannot be performed consistently.")
                raise

            try:
                data = dd.concat([data, source_data], ignore_index=True)
            
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
        
        # `columns` debug does not require column selection or compute
        if self.func == 'columns':
            print(list(data.columns))
            return data  # do not actually transform the data

        # otherwise, subset to desired columns
        if not self.keep_columns:
            self.keep_columns = list(data.columns)
        
        selected_columns = [col for col in list(data.columns) if col in self.keep_columns and col not in self.skip_columns]
        debug_data = data[selected_columns]

        # call function, and display debug info
        if self.func == 'head':
            debug_data = debug_data.head(self.rows)
        elif self.func == 'tail':
            debug_data = debug_data.tail(self.rows)
        elif self.func == 'describe':
            debug_data = debug_data.compute().describe()

        if self.transpose:
            debug_data = debug_data.transpose().reset_index(names="column")
        
        print(debug_data.to_string(index=False))
        return data  # do not actually transform the data
