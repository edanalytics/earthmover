from earthmover.nodes.operation import Operation


class DistinctRowsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'operation',
        'column', 'columns',
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.columns_list: list = None

    def compile(self):
        """

        :return:
        """
        super().compile()

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

        if data.npartitions > 1:
            self.logger.debug(
                f"data at {self.type} `{self.name}` has {data.npartitions} partitions... indexing on first uniqueness column `{self.columns_list[0]}`, then each partition will be deduped"
            )

            # see https://stackoverflow.com/questions/68019990
            data = (
                data
                .set_index(self.columns_list[0], drop=False).repartition(partition_size=self.chunksize)
                .map_partitions(lambda x: x.drop_duplicates(self.columns_list))
            )

        else:
            data = data.drop_duplicates(subset=self.columns_list)

        return data


class FilterRowsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'operation',
        'query', 'behavior',
    )

    BEHAVIORS = ["include", "exclude"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query: str = None
        self.behavior: str = None

    def compile(self):
        """

        :return:
        """
        super().compile()

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
