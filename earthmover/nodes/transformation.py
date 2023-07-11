from earthmover.node import Node
from earthmover.nodes.operation import Operation


class Transformation(Node):
    """

    """
    type: str = 'transformation'

    allowed_configs: tuple = ('debug', 'expect', 'operations', 'source',)

    operations: list = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Load in the operation configs and save each under operations.
        # Verify all specified sources exist in the global config.
        self.source = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None

        _operations = self.error_handler.assert_get_key(self.config, 'operations', dtype=list)
        for idx, operation_config in enumerate(_operations, start=1):

            operation = Operation(self.name, operation_config, earthmover=self.earthmover)
            self.operations.append(operation)

            for source in operation.upstream_sources:
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
