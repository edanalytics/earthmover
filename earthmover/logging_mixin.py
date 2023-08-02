import logging

from typing import Optional


###
class ContextFilter(logging.Filter):
    """
    This filter injects contextual information while parsing a YAML file into the log.

    Warning: `file` and `line` are similar to built-ins `filename` and `lineno`.
    """
    def filter(self, record: logging.LogRecord):
        record.file = LoggingMixin.ctx_file
        record.line = LoggingMixin.ctx_line
        record.node = LoggingMixin.ctx_node
        record.operation = LoggingMixin.ctx_operation
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

        if log and record.levelname in (logging.ERROR, logging.CRITICAL):
            return f"({log.strip()})\n{super().format(record)}"
        else:
            return super().format(record)


class LoggingMixin:
    """

    """
    ctx_file     : Optional[str ]        = None
    ctx_line     : Optional[int ]        = None
    ctx_node     : Optional['Node']      = None
    ctx_operation: Optional['Operation'] = None

    logger: Optional[logging.Logger] = None

    @classmethod
    def set_logger(cls, level: str = "INFO"):
        level = level.upper()

        handler = ExitOnExceptionHandler()

        formatter = YamlParserFormatter(
            "[%(asctime)s.%(msecs)03d] %(levelname)s :: %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

        filter = ContextFilter()
        handler.addFilter(filter)

        logger = logging.getLogger()
        logger.setLevel(level)
        logger.addHandler(handler)

        cls.logger = logger
        return logger

    @classmethod
    def update_ctx(cls,
        file: Optional[str] = None,
        line: Optional[int] = None,
        node: Optional['Node'] = None,
        operation: Optional['Operation'] = None,
    ):
        cls.ctx_file = file or cls.ctx_file
        cls.ctx_line = line or cls.ctx_line
        cls.ctx_node = node or cls.ctx_node
        cls.ctx_operation = operation or cls.ctx_operation

    @classmethod
    def reset_ctx(cls, removes: Optional[list] = None):
        if not removes:
            removes = ['file', 'line', 'node', 'operation']

        if 'file' in removes: cls.ctx_file = None
        if 'line' in removes: cls.ctx_line = None
        if 'node' in removes: cls.ctx_node = None
        if 'operation' in removes: cls.ctx_operation = None
