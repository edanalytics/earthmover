from earthmover.node import Node
from earthmover.nodes.operation import Operation


class Transformation(Node):
    """

    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.type = 'transformation'

        self.allowed_configs.update(['operations'])

        # Load in the operation configs and save each under operations.
        # Verify all specified sources exist in the global config.
        self.operations = []
        self.sources = set()

        _operations = self.error_handler.assert_get_key(self.config, 'operations', dtype=list)
        for idx, operation_config in enumerate(_operations, start=1):

            operation = Operation(self.name, operation_config, earthmover=self.earthmover)
            self.operations.append(operation)
            self.sources.update(operation.sources)


    def compile(self):
        """

        :return:
        """
        super().compile()

        for operation in self.operations:
            operation.source = self
            operation.compile()


    def execute(self):
        """

        :return:
        """
        super().execute()

        for operation in self.operations:
            self.data = operation.execute()
            operation.post_execute()
