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
        self.sources = set()
        self.operations = []

        for idx, operation_config in enumerate(self.config, start=1):
            self.error_handler.ctx.update(
                file=self.earthmover.config_file, line=operation_config["__line__"], node=self, operation=operation_config
            )

            _operation = Operation(operation_config, earthmover=self.earthmover)
            self.operations.append(_operation)

            # Sources are defined in a list of 'sources', or a single 'source', but never both.
            # These are saved as an attribute to build network edges in `Earthmover.graph`.
            if 'sources' in operation_config:
                self.sources.update(operation_config['sources'])
            elif 'source' in operation_config:
                self.sources.add(operation_config['source'])

        if not self.sources:
            self.error_handler.throw(
                "no source(s) defined for transformation operation"
            )
        else:
            self.sources = list(self.sources)


    def execute(self):
        """

        :return:
        """
        for operation in self.operations:
            self.data = operation.execute()

