import inspect
import logging

from typing import Optional


###
class ExitOnExceptionHandler(logging.StreamHandler):
    """
    Automatically exit Earthmover if an error or higher event is passed.
    """
    def emit(self, record: logging.LogRecord):
        super().emit(record)
        if record.levelno >= logging.ERROR:
            raise SystemExit(-1)


###
class DynamicLoggingFormatter(logging.Formatter):
    """
    Override Formatter to retrieve the calling_class extra from LogRecord.
    Check for extended-logging attributes, and add them to the record.
    Dynamically build out an error-location message from the attributes.

    Warning: `line` is similar to built-in `lineno`.
    """
    def format(self, record: logging.LogRecord):
        # Retrieve the calling-class (i.e., Node, Graph, Operation, etc.) and dynamically-infer context.
        calling_class = getattr(record, 'calling_class')
        if calling_class:

            # Use duck-typing to check whether the calling class is a Node (or Node child-class).
            if hasattr(calling_class, 'name') and hasattr(calling_class, 'type'):
                record.node = calling_class

            if hasattr(calling_class, 'config'):
                record.line = calling_class.config.__line__

        # Format the record into a location-string to make debugging errors easier.
        log_string = self.to_formatted_string(record)

        if log_string and record.levelno >= logging.ERROR:
            return f"{super().format(record)} ({log_string})"
        else:
            return super().format(record)

    @staticmethod
    def to_formatted_string(record: logging.LogRecord):
        """
        e.g. (near line 257 in `$transformations.total_of_each_species.operations:add_columns`)

        :param record:
        :return:
        """
        log_string = ""

        if hasattr(record, 'line'):
            log_string += f"near line {record.line} "

        if hasattr(record, 'node'):
            log_string += f"in `${record.node.type}s.{record.node.name}` "

        return log_string.strip()


class ClassConsciousLogger(logging.Logger):
    """

    """
    show_stacktrace: bool = False  # Default to False, and turn on using `LoggingMixin.set_logging_level()`

    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):
        """
        Override Logger._log() to automatically infer calling-class.
        """
        # Automatically add the 'calling_class' attribute to the extra dictionary
        if extra is None:
            extra = {}
        extra['calling_class'] = self._get_calling_class()

        super()._log(
            level, msg, args,
            exc_info=(exc_info or self.show_stacktrace),
            extra=extra, stack_info=stack_info, stacklevel=stacklevel
        )

    @classmethod
    def _get_calling_class(cls):
        # Iterate the stack until the first object that is not the logger is found.
        for frame_info in inspect.stack():
            calling_class = frame_info[0].f_locals.get('self', None)
            if calling_class and not isinstance(calling_class, cls):
                return calling_class
        else:
            return None


class LoggingMixin:
    """

    """

    logger: Optional[logging.Logger] = None

    @classmethod
    def set_logger(cls):
        if cls.logger:
            return cls.logger

        handler = ExitOnExceptionHandler()

        formatter = DynamicLoggingFormatter(
            "[%(asctime)s.%(msecs)03d] %(levelname)-8s :: %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

        logger = ClassConsciousLogger('earthmover')
        logger.addHandler(handler)

        LoggingMixin.logger = logger
        return logger

    @classmethod
    def set_logging_level(cls, level: str = "INFO", show_stacktrace: bool = False):
        if not cls.logger:
            cls.set_logger()

        LoggingMixin.logger.setLevel(logging.getLevelName(level.upper()))
        LoggingMixin.logger.show_stacktrace = show_stacktrace
