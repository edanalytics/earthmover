import argparse
import logging
import os
import sys

from earthmover.earthmover import Earthmover


class ExitOnExceptionHandler(logging.StreamHandler):
    """

    """
    def emit(self, record):
        super().emit(record)
        if record.levelno in (logging.ERROR, logging.CRITICAL):
            raise SystemExit(-1)

DEFAULT_CONFIG_FILES = ['earthmover.yaml', 'earthmover.yml']

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
    ### Prepare and initialize the parser with defaults.
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
    parser.add_argument('command',
        nargs="?",
        type=str,
        help='the command to run: `run`, `compile`, `visualize`'
        )
    parser.add_argument("-c", "--config-file",
        nargs="?",
        type=str,
        help='Specify YAML config file',
        metavar="CONFIG_FILE"
    )
    parser.add_argument("-v", "--version",
        action='store_true',
        help='view tool version and exit'
    )
    parser.add_argument("-t", "--test",
        action='store_true',
        help="run transformation test suite"
    )
    parser.add_argument("-s", "--selector",
        nargs='?',
        help='select a subset of nodes to run [node1,node2,subnode*]'
    )
    parser.add_argument("-f", "--force",
        action='store_true',
        help='force regeneration (even if data and configs have not changed)'
    )
    parser.add_argument("-k", "--skip-hashing",
        action='store_true',
        help='skips computing input hashes for change detection, and prevents write to run log'
    )
    parser.add_argument("-p", "--params",
        type=str,
        help='specify parameters as a JSON object via CLI (overrides environment variables)'
    )
    parser.add_argument("-g", "--show-graph",
        action='store_true',
        help='overwrites `show_graph` config in the config file to true'
    )
    parser.add_argument("-e", "--show-stacktrace",
        action='store_true',
        help='overwrites `show_stacktrace` config in the config file to true; sets `log_level` to DEBUG'
    )
    parser.add_argument("--results-file",
        type=str,
        help='produces a JSON output file with structured information about run results'
    )

    # Set empty defaults in case they've not been populated by the user.
    parser.set_defaults(**{
        "selector": "*",
        "params": "",
        "results_file": "",
    })

    ### Parse the user-inputs and run Earthmover, depending on the command and subcommand passed.
    args, remaining_argv = parser.parse_known_args()

    # Command: Version
    if args.version:
        em_dir = os.path.dirname(os.path.abspath(__file__))
        version_file = os.path.join(em_dir, 'VERSION.txt')
        with open(version_file, 'r', encoding='utf-8') as f:
            VERSION = f.read().strip()
            print(f"earthmover, version {VERSION}")
        exit(0)

    # Command: Test
    if args.test:
        tests_dir = os.path.join( os.path.realpath(os.path.dirname(__file__)), "tests" )
        
        em = Earthmover(
            config_file=os.path.join(tests_dir, "earthmover.yaml"),
            logger=logger,
            params='{"BASE_DIR": "' + tests_dir + '"}',
            force=True,
            skip_hashing=True
        )
        em.logger.info("running tests...")
        em.test(tests_dir)
        em.logger.info('tests passed successfully.')
        exit(0)

    ### Otherwise, initialize Earthmover for a main run.
    if not args.config_file:
        for file in DEFAULT_CONFIG_FILES:
            test_file = os.path.join(".", file)
            if os.path.isfile(test_file):
                args.config_file = test_file
                logger.info(f"config file not specified with `-c` flag... but found and using ./{file}")
                break

    if not args.config_file:
        logger.error("config file not specified with `-c` flag, and no default {" + ", ".join(DEFAULT_CONFIG_FILES) + "} found")

    # Update state configs with those forced via the command line.
    cli_state_configs = {}

    if args.show_graph:
        cli_state_configs['show_graph'] = True

    if args.show_stacktrace:
        cli_state_configs['show_stacktrace'] = True
        cli_state_configs['log_level'] = 'DEBUG'


    # Main run
    try:
        em = Earthmover(
            config_file=args.config_file,
            logger=logger,
            params=args.params,
            force=args.force,
            skip_hashing=args.skip_hashing,
            cli_state_configs=cli_state_configs,
            results_file=args.results_file
        )

    except Exception as err:
        logger.exception(err, exc_info=True)
        raise  # Avoids linting error

    # Subcommand: deps (parse Earthmover YAML and compile listed packages)
    if args.command == 'deps':
        em.logger.info(f"installing packages...")
        if args.selector != '*':
            em.logger.info("selector is ignored for package install.")

        try:
            em.deps()
            em.logger.info("done!")
        except Exception as e:
            logger.exception(e, exc_info=em.state_configs['show_stacktrace'])
            raise

    # Subcommand: compile (parse Earthmover YAML and build graph)
    elif args.command == 'compile':
        em.logger.info(f"compiling project...")
        if args.selector != '*':
            em.logger.info("selector is ignored for compile-only run.")

        try:
            em.compile()
            em.logger.info("looks ok")

        except Exception as e:
            logger.exception(e, exc_info=em.state_configs['show_stacktrace'])
            raise

    # Subcommand: run (compile + execute)
    # This is the default if none is specified.
    elif args.command == 'run' or not args.command:
        if not args.command:
            em.logger.warning("[no command specified; proceeding with `run` but we recommend explicitly giving a command]")

        try:
            em.logger.info("starting...")
            em.generate(selector=args.selector)
            em.logger.info("done!")

        except Exception as err:
            logger.exception(err, exc_info=em.state_configs['show_stacktrace'])
            raise

    else:
        logger.exception("unknown command, use -h flag for help")
        raise


if __name__ == "__main__":
    sys.exit(main())
