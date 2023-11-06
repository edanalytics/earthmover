import inspect
import logging

from typing import Union


class UniversalLogger(logging.Logger):
    """
    logging.Formatter: https://docs.python.org/3/library/logging.html#formatter-objects
    """
    logging_format : str = "[%(asctime)s.%(msecs)03d] %(levelname)-5s: %(message)s"
    logging_datefmt: str = "%Y-%m-%d %H:%M:%S"

    # Override using `UniversalLogger.set_logging_config()`.
    log_level: int = logging.getLevelName("INFO")
    show_stacktrace: bool = False

    def __repr__(self):
        return f"{type(self)} ({logging.getLevelName(self.log_level)}: {self.show_stacktrace})"

    @classmethod
    def initialize(cls):
        root_logger = logging.getLogger()  # Force the root logger to initialize (maybe unnecessary)
        root_logger.propagate = False  # Turn off propagation to prevent multiple logging (maybe unnecessary)

        output_earthmover = ContextFormatter()
        exit_on_exception = ExitOnExceptionHandler()  # Must come last because of system exit

        logging.basicConfig(
            format=cls.logging_format,
            datefmt=cls.logging_datefmt,
            handlers=[output_earthmover, exit_on_exception]
        )
        logging.setLoggerClass(cls)  # Force all child loggers to use this class.

    @classmethod
    def set_logging_config(cls, level: Union[int, str], show_stacktrace: bool):
        """

        :param level:
        :param show_stacktrace:
        :return:
        """
        if isinstance(level, str):
            level = logging.getLevelName(level.upper())

        cls.log_level = level
        cls.show_stacktrace = show_stacktrace

    def isEnabledFor(self, level):
        """
        All logging output methods use this method, so force log-level override.
        """
        # Force session to match universal log level.
        if self.level != self.log_level:
            self.setLevel(self.log_level)

        return super().isEnabledFor(level)

    def exception(self, *args, exc_info=None, **kwargs):
        """
        Apply stacktrace on error only if globally-specified.
        """
        return super().exception(*args, exc_info=self.show_stacktrace, **kwargs)


class ExitOnExceptionHandler(logging.StreamHandler):
    """
    Automatically exit Earthmover if an error or higher event is passed.
    Turn off `super().emit()` to prevent double-logging.
    """
    def emit(self, record: logging.LogRecord):
        if record.levelno >= logging.ERROR:
            raise SystemExit(-1)


class ContextFormatter(logging.StreamHandler):
    """
    logging.Filter: https://docs.python.org/3/library/logging.html#filter-objects

    Override Formatter to retrieve the calling_class extra from LogRecord.
    Check for extended-logging attributes, and add them to the record.
    Dynamically build out an error-location message from the attributes.
    Warning: `line` is similar to built-in `lineno`.
    """
    context: dict = dict()  # Update context over the run

    def handle(self, record):
        """
        Add the calling class' metadata to the context.

        :param record:
        :return:
        """
        calling_class = self.get_calling_class()

        if not calling_class:
            return super().handle(record)

        # Merge global context with latest calling class
        self.context = {**self.context, **vars(calling_class)}

        # Class attributes cannot be accessed in vars().
        for var in dir(calling_class):
            self.context[var] = getattr(calling_class, var)  # Avoid `hasattr()` to prevent recursion-loop

        return super().handle(record)

    def format(self, record: logging.LogRecord):
        """
        e.g. (near line 257 in `$transformations.total_of_each_species.operations:add_columns`)

        :param record:
        :return:
        """
        # Only use extended format if an error.
        if record.levelno < logging.ERROR:
            return super().format(record)

        # Format the record into a location-string to make debugging errors easier.
        log_string = ""

        if 'config' in self.context:
            log_string += "near line {} ".format(self.context['config'].__line__)

        if 'name' in self.context and 'type' in self.context:
            log_string += "in `${}s.{}` ".format(self.context['type'], self.context['name'])

        log_string = f"({log_string.strip()})" if log_string else ""
        return f"{super().format(record)} {log_string}"

    @classmethod
    def get_calling_class(cls):
        # Iterate the stack until the first object that is not itself or logger is found.
        for frame_info in inspect.stack():
            calling_class = frame_info[0].f_locals.get('self', None)
            if (
                calling_class
                and not isinstance(calling_class, cls)
                and not issubclass(type(calling_class), logging.Logger)
            ):
                return calling_class
        else:
            return None
