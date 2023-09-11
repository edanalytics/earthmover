import abc
import dask
import jinja2
import pandas as pd

from dask.diagnostics import ProgressBar

from earthmover import util

from typing import Dict, List, Tuple, Optional, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from dask.dataframe.core import DataFrame
    from earthmover.earthmover import Earthmover
    from earthmover.error_handler import ErrorHandler
    from earthmover.yaml_parser import YamlMapping
    from logging import Logger


class Node:
    """

    """
    type: str = None
    allowed_configs: Tuple[str] = ('debug', 'expect', 'show_progress', 'repartition',)

    def __init__(self, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        self.name: str = name
        self.config: 'YamlMapping' = config

        self.earthmover: 'Earthmover' = earthmover
        self.logger: 'Logger' = earthmover.logger
        self.error_handler: 'ErrorHandler' = earthmover.error_handler

        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config.__line__, node=self, operation=None
        )

        self.upstream_sources: Dict[str, Optional['Node']] = {}

        self.data: 'DataFrame' = None

        self.size: int = None
        self.num_rows: int = None
        self.num_cols: int = None

        self.expectations: List[str] = None
        self.debug: bool = False

        # Internal Dask configs
        self.partition_size: Union[str, int] = self.config.get('repartition')

        # Optional variables for displaying progress and diagnostics.
        self.show_progress: bool = self.config.get('show_progress', self.earthmover.state_configs["show_progress"])
        self.progress_bar: ProgressBar = ProgressBar(minimum=10, dt=5.0)  # Always instantiate, but only use if `show_progress is True`.

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

        # Always check for debug and expectations
        self.debug = self.config.get('debug', False)
        self.expectations = self.error_handler.assert_get_key(self.config, 'expect', dtype=list, required=False)

        pass

    @abc.abstractmethod
    def execute(self, **kwargs):
        """
        Node.execute()          :: Saves data into memory
        Operation.execute(data) :: Does NOT save data into memory

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config.__line__, node=self, operation=None
        )

        # Turn on the progress bar manually.
        if self.show_progress:
            self.logger.info(f"Displaying progress for {self.type} node: {self.name}")
            self.progress_bar.__enter__()  # Open context manager manually to avoid with-clause

        pass

    @abc.abstractmethod
    def post_execute(self, **kwargs):
        """
        Function to run generic logic following execute.

        1. Complete any post-transformations to self.data (currently unused).
        2. Check the dataframe aligns with expectations.
        3. Prepare row and column counts for graphing.
        4. Display row and column counts if debug is True.

        :return:
        """
        # Close context manager manually to avoid with-clause.
        if self.show_progress:
            self.progress_bar.__exit__(None, None, None)

        self.check_expectations(self.expectations)

        # Get lazy row and column counts to display in graph.png.
        if isinstance(self.data, (pd.Series, dask.dataframe.Series)):
            self.num_rows, self.num_cols = self.data.size, 1
        else:
            self.num_rows, self.num_cols = self.data.shape

        if self.debug:
            self.num_rows = dask.compute(self.num_rows)[0]
            self.logger.debug(
                f"Node {self.name}: {self.num_rows} rows; {self.num_cols} columns\n"
                f"Header: {self.data.columns if hasattr(self.data, 'columns') else 'No header'}"
            )

        pass

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

    def opt_repartition(self, data: 'DataFrame'):
        if self.partition_size:
            data = data.repartition(partition_size=self.partition_size)
        return data
