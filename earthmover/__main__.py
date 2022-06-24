import os
import sys
import filecmp
import argparse
import traceback
from earthmover import Earthmover


def main(argv=None):
    if argv is None: argv = sys.argv
    
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
    
    defaults = { "selector":"*", "params": "" }
    parser.set_defaults(**defaults)
    args, remaining_argv = parser.parse_known_args()
    
    if args.version: exit("earthmover, version {0}".format(Earthmover.version))
    if args.test:
        print("Running tests...")
        em = Earthmover(
            config_file="earthmover/tests/config.yaml",
            params="",
            force=True,
            skip_hashing=True
            )
        em.generate(selector='*')
        # compare tests/outputs/* against tests/expected/*
        for filename in os.listdir('earthmover/tests/expected/'):
            if not filecmp.cmp(os.path.join('earthmover', 'tests', 'expected', filename), os.path.join('earthmover', 'tests', 'outputs', filename)):
                exit(f"FATAL: Test output {filename} does not match expected output.")
        exit('Tests passed successfully.')
    if not args.config_file: exit("FATAL: Please pass a config YAML file as a command line argument. (Try the -h flag for help.)")
    try:
        em = Earthmover(
            config_file=args.config_file,
            params=args.params,
            force=args.force,
            skip_hashing=args.skip_hashing
            )
        em.generate(selector=args.selector)
    except Exception as e:
        print('FATAL: ' + str(e))
        if(em.config.show_stacktrace): traceback.print_exc()
        exit

if __name__ == "__main__":
    sys.exit(main())