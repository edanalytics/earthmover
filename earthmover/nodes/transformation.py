from earthmover.node import Node
from earthmover.nodes.operation import Operation


class Transformation(Node):
    """

    """
    type: str = 'transformation'
    allowed_configs: tuple = ('debug', 'expect', 'operations', 'source',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operations: list = []

        # Load in the operation configs and save each under operations.
        # Verify all specified sources exist in the global config.
        self.source = self.get_config('source', dtype=str)
        self.upstream_sources[self.source] = None

        for operation_config in self.get_config('operations', dtype=list):

            operation = Operation(self.name, operation_config)
            self.operations.append(operation)

            if hasattr(operation, 'sources'):
                for source in operation.sources:
                    self.upstream_sources[source] = None

    def compile(self):
        """

        :return:
        """
        super().compile()

        for operation in self.operations:
            operation.compile()

    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.upstream_sources[self.source].data.copy()

        for operation in self.operations:
            self.data = operation.run(self.data, self.upstream_sources)

        self.post_execute()
