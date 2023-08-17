import logging

from typing import Optional


###
class ContextFilter(logging.Filter):
    """
    This filter injects contextual information while parsing a YAML file into the log.

    Warning: `file` and `line` are similar to built-ins `filename` and `lineno`.
    """
    def filter(self, record: logging.LogRecord):
        record.file = LoggingMixin.ctx.get('file')
        record.line = LoggingMixin.ctx.get('line')
        record.node = LoggingMixin.ctx.get('node')
        record.operation = LoggingMixin.ctx.get('operation')
        return record


###
class ExitOnExceptionHandler(logging.StreamHandler):
    """

    """
    def emit(self, record: logging.LogRecord):
        super().emit(record)
        if record.levelno in (logging.ERROR, logging.CRITICAL):
            raise SystemExit(-1)


###
class YamlParserFormatter(logging.Formatter):
    """

    """
    def format(self, record: logging.LogRecord):
        if record.line:
            log = f"near line {record.line} of "
        elif record.file:
            log = "at "
        else:
            log = ""

        if record.file:
            log += f"`{record.file}` "

        if record.node:
            log += f"in `${record.node.type}s.{record.node.name}` "

        if record.operation:
            log += f"operation `{record.operation.type}` "

        if log and record.levelno in (logging.ERROR, logging.CRITICAL):
            return f"({log.strip()})\n{super().format(record)}"
        else:
            return super().format(record)


class LoggingMixin:
    """

    """
    ctx: dict = {
        'file': None,
        'line': None,
        'node': None,
        'operation': None,
    }

    logger: Optional[logging.Logger] = None

    @classmethod
    def set_logger(cls):
        if cls.logger:
            return cls.logger

        handler = ExitOnExceptionHandler()

        formatter = YamlParserFormatter(
            "[%(asctime)s.%(msecs)03d] %(levelname)-8s :: %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

        filter = ContextFilter()
        handler.addFilter(filter)

        logger = logging.getLogger('earthmover')
        logger.addHandler(handler)

        LoggingMixin.logger = logger
        return logger

    @classmethod
    def set_logging_level(cls, level: str = "INFO"):
        if not cls.logger:
            cls.set_logger()

        LoggingMixin.logger.setLevel(logging.getLevelName(level.upper()))

    @classmethod
    def update_ctx(cls, **kwargs):
        LoggingMixin.ctx.update(kwargs)

    @classmethod
    def reset_ctx(cls, *args):
        if not args:
            args = cls.ctx.keys()

        for arg in args:
            LoggingMixin.ctx[arg] = None
