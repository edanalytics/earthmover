import inspect
import logging


class UniversalLogger(logging.Logger):
    """
    logging.Formatter: https://docs.python.org/3/library/logging.html#formatter-objects
    """
    # TODO: logging.{fmt, datefmt} must be defined in-line, not as class attributes???
    # # Override using `UniversalLogger.set_logging_config()`.
    # logging_fmt  : str = "[%(asctime)s.%(msecs)03d] %(levelname)-5s: %(message)s",
    # logging_datefmt: str = "%Y-%m-%d %H:%M:%S"

    log_level: int = logging.getLevelName("INFO")
    show_stacktrace: bool = False

    def __init__(self, *args, level: int = log_level, **kwargs):
        self.log_level = level or self.log_level
        super().__init__(*args, level=self.log_level, **kwargs)

        handler = ExitOnExceptionHandler()

        formatter = DynamicLoggingFormatter(
            "[%(asctime)s.%(msecs)03d] %(levelname)-5s: %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(formatter)

        filter_ = ClassConsciousFilter()
        handler.addFilter(filter_)

        self.addHandler(handler)

    def __repr__(self):
        return f"<{self.__name__} ({logging.getLevelName(self.log_level)}: {self.show_stacktrace})>"

    @staticmethod
    def set_logging_config(level: str = log_level, show_stacktrace: bool = False):
        """

        :param level:
        :param show_stacktrace:
        :return:
        """
        if isinstance(level, str):
            level = logging.getLevelName(level.upper())

        UniversalLogger.log_level = level
        UniversalLogger.show_stacktrace = show_stacktrace

    def _log(self, *args, **kwargs):
        print("@@@", self.log_level)
        print("@@@", self.show_stacktrace)
        super()._log(*args, **kwargs)

    def exception(self, *args, exc_info=show_stacktrace, **kwargs):
        return super().exception(*args, exc_info=exc_info, **kwargs)

    @property
    def level(self):
        return self.log_level

    @level.setter
    def level(self, value):
        print(self.log_level, ' :: ', value)
        self.log_level = value


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
        kwargs = getattr(record, 'kwargs')

        # Format the record into a location-string to make debugging errors easier.
        log_string = ""

        if 'config' in kwargs:
            log_string += "near line {} ".format(kwargs['config'].__line__)

        if 'name' in kwargs and 'type' in kwargs:
            log_string += "in `${}s.{}` ".format(kwargs['type'], kwargs['name'])

        log_string = f"({log_string.strip()})" if log_string else ""
        return f"{super().format(record)} {log_string}"


class ClassConsciousFilter(logging.Filter):
    """
    logging.Filter: https://docs.python.org/3/library/logging.html#filter-objects
    """
    kwargs: dict = dict()  # Update context over the run

    def filter(self, record):
        """

        :param record:
        :return:
        """
        default_kwargs = {}
        record.kwargs = getattr(record, 'kwargs', default_kwargs)

        calling_class = self.get_calling_class()
        if calling_class:
            record.kwargs = {**record.kwargs, **vars(calling_class)}

            # Class attributes cannot be accessed in vars().
            if hasattr(calling_class, 'type'):
                record.kwargs['type'] = calling_class.type

        return super().filter(record)

    @classmethod
    def get_calling_class(cls):
        # Iterate the stack until the first object that is not the logger is found.
        for frame_info in inspect.stack():
            calling_class = frame_info[0].f_locals.get('self', None)
            if calling_class and not isinstance(calling_class, cls):
                return calling_class
        else:
            return None
