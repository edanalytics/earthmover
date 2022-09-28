from earthmover.node import Node
from earthmover.nodes.operation import Operation

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover

class Transformation(Node):
    """

    """
    CUSTOM_NODE_KEY = 'transformations'

    @classmethod
    def select_class(cls, config: dict) -> 'Node':
        """
        Logic for assigning transformations to their respective classes.

        :param config:
        :return:
        """
        # Only one Transformation has been defined in Earthmover thus far.
        return cls


    def __init__(self, name: str, config: dict, *, earthmover: 'Earthmover'):
        super().__init__(name, config, earthmover=earthmover)

        self.type = 'transformation'

        self.allowed_configs.update(['operations'])

        # Load in the operation configs and save each under operations.
        # Verify all specified sources exist in the global config.
        self.operations = []
        self.sources = set()

        self.error_handler.assert_key_exists_and_type_is(self.config, 'operations', list)
        for idx, operation_config in enumerate(self.config.get('operations'), start=1):

            operation = Operation(name=None, config=operation_config, earthmover=self.earthmover)
            self.operations.append(operation)

            # Sources are defined in a 'source_list' or 'source' class attribute, but never both.
            if _source_list := operation.__dict__.get('source_list'):
                self.sources.update(_source_list)
            elif _source := operation.__dict__.get('source'):
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
