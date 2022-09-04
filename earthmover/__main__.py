import argparse
import filecmp
import logging
import os
import sys

from earthmover.refactor.earthmover import Earthmover  # TODO: Undo. Main import change to test refactor.


class ExitOnExceptionHandler(logging.StreamHandler):
    """

    """
    def emit(self, record):
        super().emit(record)
        if record.levelno in (logging.ERROR, logging.CRITICAL):
            raise SystemExit(-1)


# Set up logging
handler = ExitOnExceptionHandler()

_formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(_formatter)

logger = logging.getLogger("earthmover")
logger.setLevel(logging.getLevelName('INFO'))
logger.addHandler(handler)


def main(argv=None):
    """

    :param argv:
    :return:
    """
    if argv is None:
        argv = sys.argv
    
    description = """Efficient data transformer: converts tablular data from files
        or database connections into JSON, XML, and other text-based formats via
        instructions from a YAML configuration file. Supports joins, unions,
        filtering, value mapping, and value conversions and computations via the
        Jinja templating language. Data sources larger than memory are supported
        via chunked processing."""
    
    parser = argparse.ArgumentParser(
        prog="earthmover",
        description=description,
        epilog="Full documentation at https://github.com/edanalytics/earthmover"
    )
    parser.add_argument('config_file',
        nargs="?",
        type=str,
        help='Specify YAML config file',
        metavar="FILE"
    )
    parser.add_argument("-v", "--version",
        action='store_true',
        help='view tool version'
    )
    parser.add_argument("-t", "--test",
        action='store_true',
        help="Run transformation tests"
    )
    parser.add_argument("-s", "--selector",
        nargs='?',
        help='subgraph selector to run'
    )
    parser.add_argument("-f", "--force",
        action='store_true',
        help='force regenerating data - even if nothing has changed'
    )
    parser.add_argument("-k", "--skip-hashing",
        action='store_true',
        help='skips computing input hashes for change detection, and prevents write to run log'
    )
    parser.add_argument("-p", "--params",
        type=str,
        help='specify parameters as a JSON object via CLI (overrides environment variables)'
    )
    parser.add_argument("-c", "--compile",
        action='store_true',
        help='only compile earthmover (does not process any actual data)'
    )
    
    _defaults = { "selector":"*", "params": "" }
    parser.set_defaults(**_defaults)

    #
    args, remaining_argv = parser.parse_known_args()
    
    if args.version:
        logger.info(f"earthmover, version {Earthmover.version}")
        exit(0)

    if args.test:
        em = Earthmover(
            config_file="earthmover/tests/config.yaml",
            logger=logger,
            params="",
            force=True,
            skip_hashing=True
        )
        em.logger.info("running tests...")
        em.generate(selector='*')

        # compare tests/outputs/* against tests/expected/*
        for filename in os.listdir('earthmover/tests/expected/'):
            if not filecmp.cmp(
                    os.path.join('earthmover', 'tests', 'expected', filename),
                    os.path.join('earthmover', 'tests', 'outputs', filename)
            ):
                file = os.path.join('earthmover', 'tests', 'outputs', filename)
                em.logger.critical(f"Test output `{file}` does not match expected output.")
                exit(1)

        em.logger.info('tests passed successfully.')
        exit(0)

    if not args.config_file:
        logger.exception("please pass a config YAML file as a command line argument (try the -h flag for help)")


    # Main run
    try:
        em = Earthmover(
            config_file=args.config_file,
            logger=logger,
            params=args.params,
            force=args.force,
            skip_hashing=args.skip_hashing
        )
    except Exception as err:
        logger.exception(err, exc_info=False)
        raise  # Avoids linting error

    #
    if args.compile:
        em.logger.info(f"compiling earthmover")
        try:
            em.compile()
        except Exception as e:
            logger.exception(e, exc_info=em.config['show_stacktrace'])

    #
    try:
        em.logger.info("starting...")
        em.generate(selector=args.selector)
        em.logger.info("done!")
    except Exception as e:
        logger.exception(e, exc_info=em.config['show_stacktrace'])


if __name__ == "__main__":
    sys.exit(main())
