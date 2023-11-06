import logging

from earthmover.logger import UniversalLogger

UniversalLogger.initialize()
logging.setLoggerClass(UniversalLogger)
