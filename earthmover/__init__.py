import logging

from earthmover.logger import UniversalLogger

UniversalLogger.set_logging_config(level="INFO", show_stacktrace=False)
logging.setLoggerClass(UniversalLogger)
