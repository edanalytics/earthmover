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
        _column  = self.get_config('column' , "", dtype=str)
        _columns = self.get_config('columns', [], dtype=list)

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
            self.logger.critical(
                "one or more columns for checking for distinctness are undefined in the dataset"
            )
            raise

        if not self.columns_list:
            self.columns_list = data.columns

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

        self.query    = self.get_config('query', dtype=str)
        self.behavior = self.get_config('behavior', dtype=str)

        if self.behavior not in self.BEHAVIORS:
            self.logger.critical(
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
            self.logger.critical(
                "error during `filter_rows` operation... check query format?"
            )
            raise

        return data
