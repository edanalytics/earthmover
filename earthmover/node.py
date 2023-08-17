import abc
import dask
import jinja2
import pandas as pd

from typing import Any, List, Optional

from earthmover.logging_mixin import LoggingMixin
from earthmover.yaml_parser import YamlMapping
from earthmover import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover


class Node(LoggingMixin):
    """

    """
    type: str = None
    allowed_configs: tuple = ('debug', 'expect',)

    CHUNKSIZE = 1024 * 1024 * 100  # 100 MB

    def __init__(self, name: str, config: YamlMapping, **kwargs):
        self.name = name
        self.config = config

        self.data: 'DataFrame' = None
        self.upstream_sources: dict = {}

        self.size: int = None
        self.num_rows: int = None
        self.num_cols: int = None

        self.expectations: list = None
        self.debug: bool = False

    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        # Verify all configs provided by the user are specified for the node.
        # (This ensures the user doesn't pass in unexpected or misspelled configs.)
        for _config in self.config:
            if _config not in self.allowed_configs:
                self.logger.warning(
                    f"Config `{_config}` not defined for node `{self.name}`."
                )

        # Always check for debug and expectations
        self.debug = self.config.get('debug', False)
        self.expectations = self.get_config('expect', [], dtype=list)

        pass

    @abc.abstractmethod
    def execute(self, **kwargs) -> 'DataFrame':
        """
        Node.execute()      :: Saves data into memory
        Operation.execute() :: Does NOT save data into memory

        :return:
        """
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

    def get_config(self, key: str, default: Optional[Any] = "[[UNDEFINED]]", *, dtype: Any = object):
        value = self.config.get(key, default)

        if value == "[[UNDEFINED]]":
            self.logger.critical(
                f"YAML parse error: Field not defined: {key}."
            )

        if value and not isinstance(value, dtype):
            self.logger.critical(
                f"YAML parse error: Field does not match expected datatype: {key}\n"
                f"    Expected: {dtype}\n"
                f"    Received: {value}"
            )

        return value

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
                    template_str="{{" + expectation + "}}"
                )

                num_failed = len(result.query(f"{expectation_result_col}=='False'").index)
                if num_failed > 0:
                    self.logger.critical(
                        f"Source `${self.type}s.{self.name}` failed expectation `{expectation}` ({num_failed} rows fail)"
                    )
                else:
                    self.logger.info(
                        f"Assertion passed! {self.name}: {expectation}"
                    )
