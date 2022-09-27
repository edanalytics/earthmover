import abc
import dask
import jinja2
import pandas as pd

from typing import List

from earthmover import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Node:
    """

    """
    CUSTOM_NODE_KEY: str

    CHUNKSIZE = 1024 * 1024 * 100  # 100 MB

    @classmethod
    @abc.abstractmethod
    def select_class(cls, *args, **kwargs) -> 'Node':
        raise NotImplementedError(
            "Method `select_class` must be defined within child classes, not Node parent class."
        )

    def __new__(cls, name: str, config: dict, *, earthmover: 'Earthmover'):
        """

        :param name:
        :param config:
        :param earthmover:
        """
        # First check for custom nodes, if provided in the YAML file.
        custom_node_superclasses = earthmover.custom_nodes.get(cls.CUSTOM_NODE_KEY, [])

        for _node_superclass in custom_node_superclasses:
            if custom_node_class := _node_superclass.select_class(config):
                return object.__new__(custom_node_class)

        # Otherwise, assume the node is a default Node.
        else:
            if node_class := cls.select_class(config):
                return object.__new__(node_class)
            else:
                earthmover.error_handler.throw(
                    f"Node type has not been defined!"
                )


    def __init__(self, name: str, config: dict, *, earthmover: 'Earthmover'):
        self.name = name
        self.config = config
        self.type = None

        self.earthmover = earthmover
        self.logger = earthmover.logger
        self.error_handler = earthmover.error_handler

        self.data = None
        self.size = None
        self.num_rows = None
        self.num_cols = None

        self.expectations = None
        self.allowed_configs = {'__line__', 'debug', 'expect'}
        self.debug = self.config.get('debug', False)


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config['__line__'], node=self, operation=None
        )

        # Verify all configs provided by the user are specified for the node.
        # (This ensures the user doesn't pass in unexpected or misspelled configs.)
        for _config in self.config:
            if _config not in self.allowed_configs:
                self.logger.warning(
                    f"Config `{_config}` not defined for node `{self.name}`."
                )

        # Always check for expectations
        if 'expect' in self.config:
            self.error_handler.assert_key_type_is(self.config, 'expect', list)
            self.expectations = self.config['expect']

        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config["__line__"], node=self, operation=None
        )
        pass


    def post_execute(self):
        """
        Function to run generic logic following execute.

        1. Check the dataframe aligns with expectations
        2. Prepare row and column counts for graphing

        :return:
        """
        self.check_expectations(self.expectations)

        self.num_rows, self.num_cols = self.data.shape

        if self.debug:
            self.num_rows = dask.compute(self.num_rows)[0]
            self.logger.debug(
                f"Node {self.name}: {self.num_rows} rows; {self.num_cols} columns\n"
                f"Header: {self.data.columns}"
            )


    def get_source_node(self, source) -> 'Node':
        """

        :return:
        """
        return self.earthmover.graph.ref(source)


    def check_expectations(self, expectations: List[str]):
        """

        :return:
        """
        expectation_result_col = "__expectation_result__"

        if expectations:
            result = self.data.copy()

            for expectation in expectations:
                template = jinja2.Template("{{" + expectation + "}}")

                result[expectation_result_col] = result.apply(
                    util.render_jinja_template, axis=1,
                    meta=pd.Series(dtype='str', name=expectation_result_col),
                    template=template,
                    error_handler = self.error_handler
                )

                num_failed = len(result.query(f"{expectation_result_col}=='False'").index)
                if num_failed > 0:
                    self.error_handler.throw(
                        f"Source `${self.type}s.{self.name}` failed expectation `{expectation}` ({num_failed} rows fail)"
                    )
                else:
                    self.logger.info(
                        f"Assertion passed! {self.name}: {expectation}"
                    )
