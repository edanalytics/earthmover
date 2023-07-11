import dask.dataframe as dd

from earthmover.nodes.operation import Operation


class JoinOperation(Operation):
    """

    """
    sources_data: list = []

    JOIN_TYPES = ["inner", "left", "right", "outer"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update([
            'left_keys', 'left_key', 'right_keys', 'right_key',
            'left_keep_columns', 'left_drop_columns', 'right_keep_columns', 'right_drop_columns',
            'join_type', 'sources'
        ])
        self.join_type = None

        self.left_keys = None
        self.left_keep_cols = None
        self.left_drop_cols = None
        self.left_cols  = None # The final column list built of cols and keys

        self.right_keys = None
        self.right_keep_cols = None
        self.right_drop_cols = None
        self.right_cols = None  # The final column list built of cols and keys

        # Check joined node
        self.sources = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)


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


    def verify(self):
        """

        :return:
        """
        super().verify()

        # Build left dataset columns
        self.left_cols = self.data.columns

        if self.left_keep_cols:
            if not set(self.left_keep_cols).issubset(self.data.columns):
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

        # Build right dataset columns
        for right_data in self.sources_data:
            self.right_cols = right_data.columns

            if self.right_keep_cols:
                if not set(self.right_keep_cols).issubset(right_data.columns):
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


    def execute(self):
        """

        :return:
        """
        self.sources_data = list(map(self.get_source_node, self.sources))

        super().execute()

        left_data = self.data[ self.left_cols ]
        for right_data in self.sources:
            print(self.right_cols)
            right_data = right_data[ self.right_cols ]

            try:
                self.data = dd.merge(
                    left_data, right_data, how=self.join_type,
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
    sources_data: list = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['sources',])

        self.header = None

        self.sources = self.error_handler.assert_get_key(self.config, 'sources', dtype=list)


    def compile(self):
        """

        :return:
        """
        super().compile()


    def verify(self):
        """

        :return:
        """
        super().verify()

        _data_columns = set( self.data.columns )

        for data in self.sources_data:
            if set(data.columns) != _data_columns:
                self.error_handler.throw('dataframes to union do not share identical columns')
                raise
        else:
            self.header = list(_data_columns)


    def execute(self):
        """

        :return:
        """
        self.sources_data = list(map(self.get_source_node, self.sources))

        super().execute()

        for _data in self.sources_data:
            try:
                self.data = dd.concat([self.data, _data], ignore_index=True)
            
            except Exception as _:
                self.error_handler.throw(
                    "error during `union` operation... are sources same shape?"
                )
                raise

        return self.data
