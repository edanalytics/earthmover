from earthmover.node import Node
from earthmover.nodes.operation import Operation

from typing import List, Tuple
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame


class Transformation(Node):
    """

    """
    type: str = 'transformation'
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'operations', 'source',)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.operations: List[Node] = []

        # Load in the operation configs and save each under operations.
        # Verify all specified sources exist in the global config.
        self.source: str = self.error_handler.assert_get_key(self.config, 'source', dtype=str)
        self.upstream_sources[self.source] = None

        for operation_config in self.error_handler.assert_get_key(self.config, 'operations', dtype=list):

            operation = Operation(self.name, operation_config, earthmover=self.earthmover)
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

    def execute(self) -> 'DataFrame':
        """

        :return:
        """
        super().execute()

        self.data = self.upstream_sources[self.source].data.copy()

        for operation in self.operations:
            self.data = operation.execute(self.data, data_mapping=self.upstream_sources)

        self.post_execute()

        return self.data
