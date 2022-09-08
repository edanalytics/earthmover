import abc
import jinja2

from earthmover.refactor import util

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.refactor.earthmover import Earthmover


class Node:
    """

    """
    def __init__(self, name: str, config: dict, *, earthmover: 'Earthmover'):
        self.name = name
        self.config = config
        self.type = None

        self.earthmover = earthmover
        self.logger = earthmover.logger
        self.error_handler = earthmover.error_handler

        self.data = None
        self.size = None
        self.rows = None
        self.cols = None

        self.expectations = []


    @abc.abstractmethod
    def compile(self):
        """

        :return:
        """
        self.error_handler.ctx.update(
            file=self.earthmover.config_file, line=self.config['__line__'], node=self, operation=None
        )
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


    def check_expectations(self):
        """

        :return:
        """
        if not self.data:
            self.logger.debug("skipping checking expectations (not yet loaded)")
            return

        if self.expectations:
            result = self.data.copy()

            for expectation in self.expectations:
                _template = jinja2.Template("{{" + expectation + "}}")
                result = result.apply(
                    util.apply_jinja_template_to_row,
                    axis=1,
                    kwargs={
                        'template': _template,
                        'col': "__expectation_result__",
                        'error_handler': self.error_handler,
                    }
                )

                num_failed = len(result.query("__expectation_result__=='False'"))
                if num_failed > 0:
                    self.error_handler.throw(
                        f"Source `${self.type}s.{self.name}` failed expectation `{expectation}` ({num_failed} rows fail)"
                    )

            result.drop(columns="__expectation_result__")
