import abc

from earthmover.refactor.operations.operation import Operation


class GenericRowOperation(Operation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.source = None


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'source', str)
        self.source = self.config['source']

        pass


    @abc.abstractmethod
    def verify(self):
        """

        :return:
        """
        super().verify()
        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        super().execute()
        pass



class DistinctRowsOperation(GenericRowOperation):
    """

    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.columns_list = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        #
        if _column := self.config.get('column'):
            self.error_handler.assert_key_type_is(self.config, 'column', str)
            self.columns_list = [_column]

        elif _columns := self.config.get('columns'):
            self.error_handler.assert_key_type_is(self.config, 'columns', list)
            self.columns_list = _columns

        else:  # Do not subset the columns.
            self.columns_list = []  # TODO: Check whether this logic works.

        pass


    def verify(self):
        """

        :return:
        """
        super().verify()

        if not set(self.columns_list).issubset(self.source.data.columns):
            self.error_handler.throw(
                "one or more columns for checking for distinctness are undefined in the dataset"
            )
            raise


    def execute(self):
        """

        :return:
        """
        super().execute()

        return self.source.data.drop_duplicates(subset=self.columns_list)



class FilterRowsOperation(GenericRowOperation):
    """

    """
    BEHAVIORS = ["include", "exclude"]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.query = None
        self.behavior = None


    def compile(self):
        """

        :return:
        """
        super().compile()

        self.error_handler.assert_key_exists_and_type_is(self.config, "query", str)
        self.query = self.config['query']

        #
        self.error_handler.assert_key_exists_and_type_is(self.config, "behavior", str)
        self.behavior = self.config['behavior']

        if self.behavior not in self.BEHAVIORS:
            self.error_handler.throw(
                "`behavior` must be one of [include, exclude]"
            )
            raise


    def verify(self):
        """

        :return:
        """
        super().verify()
        pass


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
            return self.source.data.query(_query)
        except Exception as err:
            self.error_handler.throw(
                "error during `filter_rows` operation... check query format?"
            )
            raise
