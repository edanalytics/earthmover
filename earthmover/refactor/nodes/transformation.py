from dask.dataframe.core import DataFrame as DaskDataFrame

from earthmover.refactor.nodes.node import Node
from earthmover.refactor.operations.operation import Operation


class Transformation(Node):
    """

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.type = 'transformation'

        self.sources = None
        self.operations = None


    def compile(self):
        """

        :return:
        """
        # super().compile()

        self.sources = set()
        self.operations = []

        for idx, operation_config in enumerate(self.config, start=1):
            operation = Operation(operation_config, earthmover=self.earthmover)
            operation.compile()
            self.operations.append(operation)

            # Sources are defined in a 'source_list' or 'source' class attribute, but never both.
            if _source_list := operation.__dict__.get('source_list'):
                self.sources.update(_source_list)
            elif _source := operation.__dict__.get('source'):
                self.sources.add(_source)

        # Sources are saved as an attribute to build network edges in `Earthmover.graph`.
        if self.sources:
            self.sources = list(self.sources)

        else:
            self.error_handler.throw(
                "no source(s) defined for transformation operation"
            )
            raise


    def execute(self):
        """

        :return:
        """
        # super().execute()

        for operation in self.operations:
            self.data = operation.execute()