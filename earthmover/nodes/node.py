import abc
import fnmatch
import jinja2
import logging
import pandas as pd
import polars as pl
import warnings

from earthmover import util

from typing import Dict, List, Tuple, Optional, Union
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from polars import LazyFrame
    from earthmover.earthmover import Earthmover
    from earthmover.error_handler import ErrorHandler
    from earthmover.yaml_parser import YamlMapping
    from logging import Logger


class Node:
    """

    """
    type: str = None
    allowed_configs: Tuple[str] = ('debug', 'expect', 'require_rows', 'show_progress', 'repartition')

    def __init__(self, name: str, config: 'YamlMapping', *, earthmover: 'Earthmover'):
        self.name: str = name
        self.config: 'YamlMapping' = config
        self.full_name: str = f"${self.type}s.{self.name}"

        self.earthmover: 'Earthmover' = earthmover
        self.logger: 'Logger' = earthmover.logger
        self.error_handler: 'ErrorHandler' = earthmover.error_handler

        self.error_handler.ctx.update(
            file=self.config.__file__, line=self.config.__line__, node=self, operation=None
        )

        self.upstream_sources: Dict[str, Optional['Node']] = {}

        self.data: 'LazyFrame' = None

        self.size: int = None
        self.num_rows: int = None
        self.num_cols: int = None

        self.expectations: List[str] = None
        self.require_rows: bool = False
        self.debug: bool = (self.logger.level <= logging.DEBUG)  # Default to Logger's level.

        # `repartition` was a Dask partitioning concept; it has no effect under the Polars
        # backend (Polars manages memory/parallelism internally). We still accept it for
        # backward-compatibility and warn that it is a no-op (see `opt_repartition`).
        self.partition_size: Union[str, int] = self.config.get('repartition')

        # Progress bars were provided by Dask's diagnostics; Polars has no equivalent global
        # progress bar, so `show_progress` is accepted but currently a no-op.
        self.show_progress: bool = self.config.get('show_progress', self.earthmover.state_configs["show_progress"])
        self.head_was_displayed: bool = False  # Workaround to prevent displaying the head twice when debugging.

        # Verify all configs provided by the user are specified for the node.
        # (This ensures the user doesn't pass in unexpected or misspelled configs.)
        for _config in self.config:
            if _config not in self.allowed_configs:
                self.logger.warning(
                    f"Config `{_config}` not defined for node `{self.name}`."
                )

        # Always check for debug and expectations
        self.debug = self.debug or self.config.get('debug', False)
        self.expectations = self.error_handler.assert_get_key(self.config, 'expect', dtype=list, required=False)
        self.require_rows = int(self.require_rows or self.config.get('require_rows', False))
        if self.require_rows < 0:
            self.error_handler.throw(
                f"Source `{self.full_name}` require_rows cannot be negative"
            )


    @abc.abstractmethod
    def execute(self, **kwargs):
        """
        Node.execute()          :: Saves data into memory
        Operation.execute(data) :: Does NOT save data into memory

        :return:
        """
        self.error_handler.ctx.update(
            file=self.config.__file__, line=self.config.__line__, node=self, operation=None
        )

        if self.show_progress:
            self.logger.info(f"Processing {self.type} node: {self.name}")

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
        self.check_expectations(self.expectations)

        # Column count is available cheaply from the (lazy) schema; row count is deferred
        # because it would force a full materialization of the LazyFrame.
        self.num_cols = len(self.data.collect_schema().names())
        self.num_rows = None

        # Only actually count the rows if `require_rows` was defined for this node.
        if self.require_rows > 0:
            self.check_require_rows(self.require_rows)

        # Display row-count and dataframe shape if debug is enabled.
        if self.debug:
            self.display_head()

        pass

    def check_require_rows(self, num_required_rows):
        if self.num_rows is None:
            self.num_rows = self.data.select(pl.len()).collect().item()
        if self.num_rows < num_required_rows:
            self.error_handler.throw(
                f"Source `{self.full_name}` failed require_rows >= {num_required_rows}` (only {self.num_rows} rows found)"
            )
        else:
            self.logger.info(
                f"Assertion passed! {self.name}: require_rows >= {num_required_rows}"
            )

    def display_head(self, nrows: int = 5):
        """
        Materialize just the first `nrows` rows for display.
        """
        if self.head_was_displayed:
            return None

        # Collect the head and (cheaply, alongside) the total row count.
        data_head = self.data.head(nrows).collect()
        if self.num_rows is None:
            self.num_rows = self.data.select(pl.len()).collect().item()

        self.logger.info(f"Node {self.name}: {int(self.num_rows)} rows; {self.num_cols} columns")
        # Render pandas-style (no index) to keep the familiar debug output format.
        with pd.option_context('display.max_columns', None, 'display.width', None):
            print(f"\n{data_head.to_pandas().to_string(index=False)}\n")

        self.head_was_displayed = True  # Mark that state was shown to avoid double-logging.

    def check_expectations(self, expectations: List[str]):
        """
        Evaluate Jinja boolean `expect` expressions row-by-row and fail if any row is False.
        """
        expectation_result_col = "__expectation_result__"

        if expectations:
            # Expectations are opt-in and evaluated eagerly via pandas to preserve the exact
            # rendering/`query` semantics of the previous backend.
            result = self.data.collect().to_pandas()

            for expectation in expectations:
                template = jinja2.Template("{{" + expectation + "}}")

                result[expectation_result_col] = result.apply(
                    util.render_jinja_template, axis=1,
                    template=template,
                    template_str="{{" + expectation + "}}",
                    error_handler=self.error_handler
                )

                num_failed = len(result.query(f"{expectation_result_col}=='False'").index)
                if num_failed > 0:
                    self.error_handler.throw(
                        f"Source `{self.full_name}` failed expectation `{expectation}` ({num_failed} rows fail)"
                    )
                else:
                    self.logger.info(
                        f"Assertion passed! {self.name}: {expectation}"
                    )

    def opt_repartition(self, data: 'LazyFrame'):
        """
        `repartition` was a Dask-specific tuning knob. Under Polars it is a no-op; we warn
        once per node that sets it so existing project YAML keeps working but users know it
        no longer has any effect.
        """
        if self.partition_size:
            self.logger.warning(
                f"Config `repartition` on node `{self.name}` is deprecated and has no effect "
                f"under the Polars backend (Polars manages partitioning/memory internally)."
            )
        return data

    def set_upstream_source(self, source_name: str, node: 'Node'):
        """ Upstream sources initialize as strings and are replaced during Earthmover.build_graph(). """
        if source_name not in self.upstream_sources:
            self.error_handler.throw(f"Source {source_name} not found in Node sources list.")
        self.upstream_sources[source_name] = node

    def match_wildcard_columns(self,
        columns_list: List[str],
        wildcard_list: List[str],
        raise_on_unmatched: bool = False
    ) -> List[str]:
        """
        Determine which column names match against unix filename wildcard patterns.
        If no column matches a wildcard, raise an error if `raise_on_unmatched` is flagged.

        See the fnmatch library for more info: https://docs.python.org/3/library/fnmatch.html
        """
        matched_cols: List[str] = []
        unmatched_wildcards: List[str] = []

        # Iterate the wildcards and attempt to match against all columns in the dataframe.
        for wildcard in wildcard_list:

            # Track whether the wildcard matched any columns.
            match_found = False

            for column in columns_list:
                if fnmatch.fnmatch(column, wildcard):
                    matched_cols.append(column)
                    match_found = True

            if not match_found:
                unmatched_wildcards.append(wildcard)

        # Raise an error if one or more columns specified could not be mapped to the columns list.
        if raise_on_unmatched and unmatched_wildcards:
            self.error_handler.throw(
                f"One or more columns specified are not present in the dataset: {unmatched_wildcards}"
            )

        return list(dict.fromkeys(matched_cols))  # Return as an ordered set
