import inspect
import logging

from typing import Any


class ExitOnExceptionHandler(logging.StreamHandler):
    """
    Automatically exit Earthmover if an error or higher event is passed.
    """
    def emit(self, record: logging.LogRecord):
        super().emit(record)
        if record.levelno >= logging.ERROR:
            raise SystemExit(-1)


class DynamicLoggingFormatter(logging.Formatter):
    """
    Override Formatter to retrieve the calling_class extra from LogRecord.
    Check for extended-logging attributes, and add them to the record.
    Dynamically build out an error-location message from the attributes.
    Warning: `line` is similar to built-in `lineno`.
    """
    def format(self, record: logging.LogRecord):
        """
        e.g. (near line 257 in `$transformations.total_of_each_species.operations:add_columns`)

        :param record:
        :return:
        """
        # Only use extended format if an error.
        if record.levelno < logging.ERROR:
            return super().format(record)

        # Retrieve the parent class kwargs (i.e., YamlMapping, Node, etc.) and use duck-typing to build a message.
        calling_class = getattr(record, 'calling_class')
        kwargs = getattr(record, 'kwargs')

        # Format the record into a location-string to make debugging errors easier.
        log_string = ""

        if 'config' in kwargs:
            log_string += f"near line {kwargs['config'].__line__} "

        # Name and Type are Class attributes, so they are not collected in vars().
        if hasattr(calling_class, 'name') and hasattr(calling_class, 'type'):
            log_string += f"in `${calling_class.type}s.{calling_class.name}` "

        log_string = f"({log_string.strip()})" if log_string else ""
        return f"{super().format(record)} {log_string}"


class ClassConsciousLogger(logging.Logger):
    """
    """
    # Override using `ClassConsciousLogger.set_logging_config()`.
    log_level: int = logging.getLevelName("DEBUG")
    show_stacktrace: bool = True

    # Store latest context as a class and kwargs object
    calling_class: Any = None
    kwargs: dict = dict()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        handler = ExitOnExceptionHandler()
        formatter = DynamicLoggingFormatter(
            "[%(asctime)s.%(msecs)03d] %(levelname)-8s :: %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)
        self.addHandler(handler)
        self.setLevel(self.log_level)

    def __repr__(self):
        return f"<ClassConsciousLogger earthmover ({logging.getLevelName(self.log_level)}: {self.show_stacktrace})>"

    def set_logging_config(self, level: str = log_level, show_stacktrace: bool = False):
        ClassConsciousLogger.log_level = logging.getLevelName(level.upper())
        ClassConsciousLogger.show_stacktrace = show_stacktrace

    def _log(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1):
        """
        Override Logger._log() to automatically infer calling-class.
        """
        if extra is None:
            extra = {}

        # Automatically add the 'calling_class' attribute to the extra dictionary
        self.calling_class = self.get_calling_class()
        if self.calling_class:
            self.kwargs = {**self.kwargs, **vars(self.calling_class)}

        extra['calling_class'] = self.calling_class
        extra['kwargs'] = self.kwargs

        super()._log(
            level, msg, args,
            exc_info=(exc_info or self.show_stacktrace),
            extra=extra, stack_info=stack_info, stacklevel=stacklevel
        )

    @classmethod
    def get_calling_class(cls):
        # Iterate the stack until the first object that is not the logger is found.
        for frame_info in inspect.stack():
            calling_class = frame_info[0].f_locals.get('self', None)
            if calling_class and not isinstance(calling_class, cls):
                return calling_class
        else:
            return None
