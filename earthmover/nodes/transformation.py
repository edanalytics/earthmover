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

            operation = Operation(operation_config, earthmover=self.earthmover)
            self.operations.append(operation)

            # Sources are defined in a 'source_list' or 'source' class attribute, but never both.
            _source_list = operation.__dict__.get('source_list')
            _source = operation.__dict__.get('source')

            if _source_list:
                self.sources.update(_source_list)
            elif _source:
                self.sources.add(_source)

        # Sources are saved as an attribute to build network edges in `Earthmover.graph`.
        if not self.sources:
            self.error_handler.throw(
                "no source(s) defined for transformation operation"
            )
            raise


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

        for operation in self.operations:
            self.data = operation.execute()
            operation.post_execute()
