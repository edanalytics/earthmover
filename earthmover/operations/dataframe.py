import dask.dataframe as dd

from earthmover.nodes.operation import Operation


class JoinOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation', 'sources', 'join_type',
        'left_keys', 'left_key', 'right_keys', 'right_key',
        'left_keep_columns', 'left_drop_columns', 'right_keep_columns', 'right_drop_columns',
    )

    join_type: str = None

    left_keys: list = None
    left_keep_cols: list = None
    left_drop_cols: list = None
    left_cols: list = None  # The final column list built of cols and keys

    right_keys: list = None
    right_keep_cols: list = None
    right_drop_cols: list = None
    right_cols: list = None  # The final column list built of cols and keys

    JOIN_TYPES = ["inner", "left", "right", "outer"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Check joined node
        self.sources = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)
        for source in self.sources:
            self.upstream_sources[source] = None


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


    def execute(self):
        """

        :return:
        """
        super().execute()

        # Build left dataset
        self.left_cols = self.data.columns

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

        self.data = self.data[self.left_cols]

        # Iterate each right dataset
        for source in self.sources:
            right_data = self.upstream_sources[source].data.copy()
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

            try:
                self.data = dd.merge(
                    self.data, right_data, how=self.join_type,
                    left_on=self.left_keys, right_on=self.right_keys
                )

            except Exception as _:
                self.error_handler.throw(
                    "error during `join` operation. Check your join keys?"
                )
                raise

        return self.data



class UnionOperation(Operation):
    """

    """
    allowed_configs: tuple = ('debug', 'expect', 'operation', 'sources',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.sources = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)
        for source in self.sources:
            self.upstream_sources[source] = None


    def execute(self):
        """

        :return:
        """
        super().execute()

        for source in self.sources:
            source_data = self.upstream_sources[source].data.copy()

            if set(source_data.columns) != set(self.data.columns):
                self.error_handler.throw('dataframes to union do not share identical columns')
                raise

            try:
                self.data = dd.concat([self.data, source_data], ignore_index=True)
            
            except Exception as _:
                self.error_handler.throw(
                    "error during `union` operation... are sources same shape?"
                )
                raise

        return self.data
