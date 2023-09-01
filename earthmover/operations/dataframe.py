import dask.dataframe as dd
import numpy as np
import pandas as pd

from typing import List

from earthmover.nodes.operation import Operation


class JoinOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'operation',
        'sources', 'join_type',
        'left_keys', 'left_key', 'right_keys', 'right_key',
        'left_keep_columns', 'left_drop_columns', 'right_keep_columns', 'right_drop_columns',
    )

    INDEX_COL = "__join_index__"
    JOIN_TYPES = ["inner", "left", "right", "outer"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Check joined node
        self.sources = self.get_config('sources', dtype=list)

        self.join_type: str = None

        self.left_keys: list = None
        self.left_keep_cols: list = None
        self.left_drop_cols: list = None
        self.left_cols: list = None  # The final column list built of cols and keys

        self.right_keys: list = None
        self.right_keep_cols: list = None
        self.right_drop_cols: list = None
        self.right_cols: list = None  # The final column list built of cols and keys

    def compile(self):
        """

        :return:
        """
        super().compile()

        # Check left keys
        _key  = self.get_config('left_key', "", dtype=str)
        _keys = self.get_config('left_keys', [], dtype=list)

        if bool(_key) == bool(_keys):  # Fail if both or neither are populated.
            self.logger.critical("must define `left_key` or `left_keys`")
            raise

        self.left_keys = _keys or [_key]  # `[None]` evaluates to True

        # Check right keys
        _key  = self.get_config('right_key', "", dtype=str)
        _keys = self.get_config('right_keys', [], dtype=list)

        if bool(_key) == bool(_keys):  # Fail if both or neither are populated.
            self.logger.critical("must define `right_key` or `right_keys`")
            raise

        self.right_keys = _keys or [_key]  # `[None]` evaluates to True

        # Check join type
        self.join_type = self.get_config('join_type', dtype=str)
        if not self.join_type:
            self.logger.critical("must define `join_type`")
            raise

        if self.join_type not in self.JOIN_TYPES:
            self.logger.critical(
                f"`join_type` must be one of [inner, left, right, outer], not `{self.join_type}`"
            )
            raise

        # Collect columns
        #   - There is a "if keep - elif drop" block in verify, so doesn't matter if both are populated.
        self.left_keep_cols  = self.get_config('left_keep_columns' , [], dtype=list)
        self.left_drop_cols  = self.get_config('left_drop_columns' , [], dtype=list)
        self.right_keep_cols = self.get_config('right_keep_columns', [], dtype=list)
        self.right_drop_cols = self.get_config('right_drop_columns', [], dtype=list)

    def execute(self, data: 'DataFrame', data_mapping: dict, **kwargs):
        """

        :return:
        """
        super().execute(data, data_mapping=data_mapping, **kwargs)

        # Build left dataset
        self.left_cols = data.columns

        if self.left_keep_cols:
            if not set(self.left_keep_cols).issubset(self.left_cols):
                self.logger.critical(
                    "columns in `left_keep_columns` are not defined in the dataset"
                )
                raise

            self.left_cols = list(set(self.left_keep_cols).union(self.left_keys))

        elif self.left_drop_cols:
            if any(col in self.left_keys for col in self.left_drop_cols):
                self.logger.critical(
                    "you may not `left_drop_columns` that are part of the `left_key(s)`"
                )
                raise

            self.left_cols = list(set(self.left_cols).difference(self.left_drop_cols))

        left_data = data[self.left_cols]

        # Iterate each right dataset
        for source in self.sources:
            right_data = data_mapping[source].data
            self.right_cols = right_data.columns

            if self.right_keep_cols:
                if not set(self.right_keep_cols).issubset(self.right_cols):
                    self.logger.critical(
                        "columns in `right_keep_columns` are not defined in the dataset"
                    )
                    raise

                self.right_cols = list(set(self.right_keep_cols).union(self.right_keys))

            elif self.right_drop_cols:
                if any(col in self.right_keys for col in self.right_drop_cols):
                    self.logger.critical(
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

                    # Concatenate key columns into an index to allow merging by index.
                    left_data = self.set_concat_index(left_data, self.left_keys)
                    right_data = self.set_concat_index(right_data, self.right_keys)

                    left_data = dd.merge(
                        left_data, right_data, how=self.join_type,
                        left_index=True, right_index=True,
                    )

                    # Remove the generated index column.
                    left_data = left_data.reset_index(drop=True).repartition(partition_size=self.chunksize)

            except Exception as _:
                self.logger.critical(
                    "error during `join` operation. Check your join keys?"
                )
                raise

        return left_data

    def set_concat_index(self, data: 'DataFrame', keys: List[str]):
        """
        Add a concatenated column to use as an index.
        Fix the divisions in the case of an empty dataframe.
        :param data:
        :param keys:
        :return:
        """
        data[self.INDEX_COL] = data[keys].apply(
            lambda row: row.str.cat(sep='_', na_rep=''),
            axis=1,
            meta=pd.Series(dtype='str', name=self.INDEX_COL)
        )

        data = data.set_index(self.INDEX_COL, drop=True)

        # Empty dataframes create divisions that cannot be compared.
        if data.divisions == (np.nan, np.nan):
            data.divisions = (None, None)

        return data.repartition(partition_size=self.chunksize)


class UnionOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'operation', 'sources',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.sources = self.get_config('sources', dtype=list)

    def execute(self, data: 'DataFrame', data_mapping: dict, **kwargs):
        """

        :return:
        """
        super().execute(data, data_mapping=data_mapping, **kwargs)

        for source in self.sources:
            source_data = data_mapping[source].data

            if set(source_data.columns) != set(data.columns):
                self.logger.critical('dataframes to union do not share identical columns')
                raise

            try:
                data = dd.concat([data, source_data], ignore_index=True)
            
            except Exception as _:
                self.logger.critical(
                    "error during `union` operation... are sources same shape?"
                )
                raise

        return data
