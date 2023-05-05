import abc
import dask
import jinja2
import pandas as pd

from typing import List

from earthmover.yaml_parser import YamlMapping
from earthmover import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Node:
    """

    """
    CHUNKSIZE = 1024 * 1024 * 100  # 100 MB

    def __init__(self, name: str, config: YamlMapping, *, earthmover: 'Earthmover'):
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
        self.allowed_configs = {'debug', 'expect'}
        self.debug = self.config.get('debug', False)


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config.__line__, node=self, operation=None
        )

        # Verify all configs provided by the user are specified for the node.
        # (This ensures the user doesn't pass in unexpected or misspelled configs.)
        for _config in self.config:
            if _config not in self.allowed_configs:
                self.logger.warning(
                    f"Config `{_config}` not defined for node `{self.name}`."
                )

        # Always check for expectations
        self.expectations = self.error_handler.assert_get_key(self.config, 'expect', dtype=list, required=False)

        pass


    @abc.abstractmethod
    def execute(self):
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config.__line__, node=self, operation=None
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
                    template_str="{{" + expectation + "}}",
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


    def ensure_dask_dataframe(self):
        """
        Converts a Pandas DataFrame to a Dask DataFrame.
        """
        if isinstance(self.data, pd.DataFrame):
            self.logger.debug(
                f"Casting data in {self.type} node `{self.name}` to a Dask dataframe."
            )
            self.data = dask.dataframe.from_pandas(
                self.data,
                chunksize=self.CHUNKSIZE
            )