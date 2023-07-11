from earthmover.nodes.operation import Operation


class DistinctRowsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'column', 'columns',
    )

    columns_list: list = None


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


    def execute(self):
        """

        :return:
        """
        super().execute()

        if not set(self.columns_list).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more columns for checking for distinctness are undefined in the dataset"
            )
            raise

        if not self.columns_list:
            self.columns_list = self.data.columns

        self.data = self.data.drop_duplicates(subset=self.columns_list)

        return self.data


class FilterRowsOperation(Operation):
    """

    """
    allowed_configs: tuple = (
        'debug', 'expect', 'operation',
        'query', 'behavior',
    )

    query: str = None
    behavior: str = None

    BEHAVIORS = ["include", "exclude"]


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


    def execute(self):
        """

        :return:
        """
        super().execute()

        #
        if self.behavior == 'exclude':
            _query = f"not( {self.query} )"
        else:
            _query = self.query

        try:
            self.data = self.data.query(_query, engine='python')  #`numexpr` is used by default if installed.

        except Exception as _:
            self.error_handler.throw(
                "error during `filter_rows` operation... check query format?"
            )
            raise

        return self.data
