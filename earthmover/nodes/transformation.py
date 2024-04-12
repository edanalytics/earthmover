from earthmover.nodes.node import Node
from earthmover.operations.operation import Operation

from typing import List, Tuple


class Transformation(Node):
    """

    """
    type: str = 'transformation'
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'repartition', 'operations', 'source',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operations: List[Operation] = []

        # Load in the operation configs and save each under operations.
        # Verify all specified sources exist in the global config.
        self.source: str = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None

        for operation_config in self.error_handler.assert_get_key(self.config, 'operations', dtype=list):

            operation = Operation(self.name, operation_config, earthmover=self.earthmover)
            self.operations.append(operation)

            # Only used by DataFrame Operations. Consider moving into those inits.
            if hasattr(operation, 'sources'):
                for source in operation.sources:
                    self.upstream_sources[source] = None

        # Force error-handler reset before graph is built.
        self.error_handler.ctx.update(
            file=self.config.__file__, line=self.config.__line__, node=self, operation=None
        )

    def execute(self):
        """

        :return:
        """
        super().execute()

        self.data = self.upstream_sources[self.source].data.copy()

        for operation in self.operations:
            self.data = operation.execute(self.data, data_mapping=self.upstream_sources)
            self.data = operation.post_execute(self.data)

        self.data = self.opt_repartition(self.data)  # Repartition if specified.

        self.post_execute()
