import dask.dataframe as dd
import pandas as pd

from earthmover.node import Node
from earthmover.nodes.operation import Operation

from typing import Dict, List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


class JoinOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'chunksize',
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

        self.join_type: str = None

        self.left_keys: List[str] = None
        self.left_keep_cols: List[str] = None
        self.left_drop_cols: List[str] = None
        self.left_cols: List[str] = None  # The final column list built of cols and keys

        self.right_keys: List[str] = None
        self.right_keep_cols: List[str] = None
        self.right_drop_cols: List[str] = None
        self.right_cols: List[str] = None  # The final column list built of cols and keys

    def compile(self):
        """

        :return:
        """
        super().compile()

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
        self.left_cols = data.columns

        if self.left_keep_cols:
            if not set(self.left_keep_cols).issubset(self.left_cols):
                self.error_handler.throw(
                    "columns in `left_keep_columns` are not defined in the dataset"
                )
                raise

            self.left_cols = list(set(self.left_keep_cols).union(self.left_keys))

        elif self.left_drop_cols:
            if any(col in self.left_keys for col in self.left_drop_cols):
                self.error_handler.throw(
                    "you may not `left_drop_columns` that are part of the `left_key(s)`"
                )
                raise

            self.left_cols = list(set(self.left_cols).difference(self.left_drop_cols))

        left_data = data[self.left_cols]

        # Keep track of whether a concatted-index was built during any of the merges.
        use_concat_index = False

        # Iterate each right dataset
        for source in self.sources:
            right_data = data_mapping[source].data
            self.right_cols = right_data.columns

            if self.right_keep_cols:
                if not set(self.right_keep_cols).issubset(self.right_cols):
                    self.error_handler.throw(
                        "columns in `right_keep_columns` are not defined in the dataset"
                    )
                    raise

                self.right_cols = list(set(self.right_keep_cols).union(self.right_keys))

            elif self.right_drop_cols:
                if any(col in self.right_keys for col in self.right_drop_cols):
                    self.error_handler.throw(
                        "you may not `right_drop_columns` that are part of the `right_key(s)`"
                    )
                    raise

                self.right_cols = list(set(self.right_cols).difference(self.right_drop_cols))

            right_data = right_data[self.right_cols]

            # Complete the merge, using different logic depending on the partitions of the datasets.
            try:
                if left_data.npartitions == 1 or right_data.npartitions == 1:
                    left_data = dd.merge(
                        left_data, right_data, how=self.join_type,
                        left_on=self.left_keys, right_on=self.right_keys
                    )

                else:
                    self.logger.debug(
                        f"data at {self.type} `{self.name}` has {left_data.npartitions} (left) and {right_data.npartitions} (right) partitions..."
                    )

                    # If the left index has not been already set, set it now.
                    if not use_concat_index:
                        left_data = self.set_concat_index(left_data, self.left_keys)
                        use_concat_index = True

                    # Concatenate key columns into an index to allow merging by index.
                    right_data = self.set_concat_index(right_data, self.right_keys)

                    left_data = dd.merge(
                        left_data, right_data, how=self.join_type,
                        left_index=True, right_index=True,
                    )

            except Exception as _:
                self.error_handler.throw(
                    "error during `join` operation. Check your join keys?"
                )
                raise

        # Remove the generated index column.
        if use_concat_index:
            left_data = left_data.reset_index(drop=True)

        return left_data.repartition(partition_size=self.chunksize)

    def set_concat_index(self, data: 'DataFrame', keys: List[str]) -> 'DataFrame':
        """
        Add a concatenated column to use as an index.
        Fix the divisions in the case of an empty dataframe.
        :param data:
        :param keys:
        :return:
        """
        if len(keys) == 1:
            data = data.set_index(keys[0], drop=False)

        else:
            data[self.INDEX_COL] = data[keys].apply(
                lambda row: row.str.cat(sep='_', na_rep=''),
                axis=1,
                meta=pd.Series(dtype='str', name=self.INDEX_COL)
            )

            data = data.set_index(self.INDEX_COL, drop=True)

        # Empty dataframes create divisions that cannot be compared.
        if data.divisions == (pd.np.nan, pd.np.nan):
            data.divisions = (None, None)

        return data.repartition(partition_size=self.chunksize)


class UnionOperation(Operation):
    """

    """
    allowed_configs: Tuple[str] = (
        'operation', 'chunksize', 'sources',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sources = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)

    def execute(self, data: 'DataFrame', data_mapping: Dict[str, Node], **kwargs) -> 'DataFrame':
        """

        :return:
        """
        super().execute(data, data_mapping=data_mapping, **kwargs)

        for source in self.sources:
            source_data = data_mapping[source].data

            if set(source_data.columns) != set(data.columns):
                self.error_handler.throw('dataframes to union do not share identical columns')
                raise

            try:
                data = dd.concat([data, source_data], ignore_index=True)
            
            except Exception as _:
                self.error_handler.throw(
                    "error during `union` operation... are sources same shape?"
                )
                raise

        return data.repartition(partition_size=self.chunksize)
