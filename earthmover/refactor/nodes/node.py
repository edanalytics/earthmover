import abc
import jinja2
import time

from typing import Optional

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.refactor.earthmover import Earthmover


class Node:
    """

    """
    def __init__(self, name: str, config: Optional[dict] = None, *, earthmover: Earthmover):
        self.name = name
        self.config = config
        self.type = None
        self.age = time.time()

        self.earthmover = earthmover
        self.logger = earthmover.logger
        self.error_handler = earthmover.error_handler

        self.data = None

        self.size = None
        self.rows = None
        self.cols = None

        self.expectations = []
        self.is_done = False


    @abc.abstractmethod
    def compile(self):
        raise NotImplementedError

    @abc.abstractmethod
    def execute(self):
        raise NotImplementedError


    def check_expectations(self):
        if not self.is_done:
            self.logger.debug("skipping checking expectations (not yet loaded)")
            return

        if self.expectations:
            result = self.data

            for expectation in self.expectations:
                _template = jinja2.Template("{{" + expectation + "}}")
                result = result.apply(
                    self.earthmover.apply_jinja,
                    axis=1,
                    args=(_template, "__expectation_result__", "expectations")
                )

                num_failed = len(result.query("__expectation_result__=='False'"))
                if num_failed > 0:
                    self.error_handler.throw(
                        f"Source `${self.type}s.{self.name}` failed expectation `{expectation}` ({num_failed} rows fail)"
                    )

            result.drop(columns="__expectation_result__")



