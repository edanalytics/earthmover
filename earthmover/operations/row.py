from earthmover.nodes.operation import Operation


class DistinctRowsOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['column', 'columns'])

        self.columns_list = None


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


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.columns_list).issubset(self.data.columns):
            self.error_handler.throw(
                "one or more columns for checking for distinctness are undefined in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        if not self.columns_list:
            self.columns_list = self.data.columns

        self.data = self.data.drop_duplicates(subset=self.columns_list)

        return self.data


class FilterRowsOperation(Operation):
    """

    """
    BEHAVIORS = ["include", "exclude"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.allowed_configs.update(['query', 'behavior'])

        self.query = None
        self.behavior = None


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
