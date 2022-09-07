import csv
import hashlib
import json
import os
import time

from typing import Dict, Optional

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.refactor.earthmover import Earthmover
    from earthmover.refactor.nodes import Node


class RunsFile:
    """

    """
    BUF_SIZE = 65536  # 64kb chunks
    ROW_COUNT_TO_WARN = 10000

    HEADER = [
        'run_timestamp',
        'config_hash',
        'sources_hash',
        'templates_hash',
        'mappings_hash',
        'params_hash',
        'selector',
    ]

    def __init__(self, file: str, *, earthmover: 'Earthmover'):
        self.file = file

        self.earthmover = earthmover
        self.logger = self.earthmover.logger
        self.hashes = self.build_hashes()

        # Force the existence of the runs file.
        if not os.path.isfile( self.file ):
            self._write_header()

        self.runs = self.read_runs()


    def read_runs(self):
        """

        :return:
        """
        with open(self.file, 'r') as fp:
            runs = list(csv.DictReader(fp, delimiter=','))

        # Raise a warning for the user to manually reset or select a new log-runs file.
        if len(runs) > self.ROW_COUNT_TO_WARN:
            self.logger.warning(
                f"run log file {self.file} is getting long, consider truncating it for better performance.",
                True
            )

        return runs


    def _write_header(self):
        """
        The 'x' open-mode fails if the file already exists.
        Excellent safeguard, Tom!

        :return:
        """
        with open(self.file, 'x') as fp:
            writer = csv.writer(fp)
            writer.writerow(self.HEADER)


    def write_row(self, selector: Optional[str] = None):
        """

        :parameter selector:
        :return:
        """
        row_dict = self.hashes

        # Add missing columns to the row
        row_dict['run_timestamp'] = time.time()

        if selector:
            row_dict['selector'] = selector

        with open(self.file, 'a') as fp:
            writer = csv.DictWriter(fp, fieldnames=self.HEADER)
            writer.writerow(row_dict)


    def build_hashes(self) -> dict:
        """
        This tool maintains state about prior runs. If no inputs have changed, there's no need to re-run,
        so for each run, we log hashes of
        - config.yaml
        - any CSV/TSV files from sources
        - any template files from destinations
        - any CSV/TSV files from map_values transformation operations
        - any parameters passed via CLI
        Only if any of these have changed since the last run do we actually re-process the DAG.
        We also need to make sure to handle the selector...  data since a prior run may not have changed,
        but the selector may be "wider" this time, necessitating a (re)run.

        :return:
        """
        node_data = self.earthmover.graph.get_node_data()

        ### Store all hashes into a dictionary to merge with the rest of the row.
        row = {}

        # Hash the configs
        config_hash = self.get_string_hash(json.dumps(self.earthmover.state_configs))
        row['config_hash'] = config_hash


        # Hash the params
        if self.earthmover.params != "":
            params_hash = self.get_string_hash(self.earthmover.params)
        else:
            params_hash = ""

        row['params_hash'] = params_hash


        # Hash the sources
        sources_hash = ""
        for name, source in self.earthmover.sources.items():

            if f"$sources.{name}" not in node_data.keys():
                continue
            if name == "__line__":
                continue

            if "file" in source.keys():
                if "://" in source["file"]:
                    continue

                sources_hash += self.get_file_hash(source['file'])

        if sources_hash:
            sources_hash = self.get_string_hash(sources_hash)

        row['sources_hash'] = sources_hash


        # Hash the templates
        templates_hash = ""
        for name, destination in self.earthmover.destinations.items():

            if f"$destinations.{name}" not in node_data.keys():
                continue
            if name == "__line__":
                continue

            if "template" in destination.keys():
                templates_hash += self.get_file_hash(destination["template"])

        if templates_hash != "":
            templates_hash = self.get_string_hash(templates_hash)

        row['templates_hash'] = templates_hash


        # Hash the transformation mapping files
        mappings_hash = ""
        for name, transformation in self.earthmover.transformations.items():

            if name == "__line__":
                continue

            for op in transformation:
                if "operation" in op.keys() and op["operation"] == "map_values" and "map_file" in op.keys():
                    mappings_hash += self.get_file_hash(op["map_file"])

        if mappings_hash != "":
            mappings_hash = self.get_string_hash(mappings_hash)
        else:
            mappings_hash = ""
        row['mappings_hash'] = mappings_hash

        return row


    def get_file_hash(self, file: str, hash_algorithm: str = 'md5'):
        """
        Compute the hash of a (potentially large) file by streaming it in from disk

        :param file:
        :param hash_algorithm:
        :return:
        """
        if hash_algorithm == "md5":
            hashed = hashlib.md5()
        elif hash_algorithm == "sha1":
            hashed = hashlib.sha1()
        else:
            raise Exception("invalid hash algorithm, must be md5 or sha1")

        with open(file, 'rb') as fp:
            while True:
                data = fp.read(self.BUF_SIZE)
                if not data:
                    break
                hashed.update(data)

        return hashed.hexdigest()

    @staticmethod
    def get_string_hash(string: str, hash_algorithm: str = 'md5'):
        """
        :param string:
        :param hash_algorithm:
        :return:
        """

        if hash_algorithm == "md5":
            hashed = hashlib.md5()
        elif hash_algorithm == "sha1":
            hashed = hashlib.sha1()
        else:
            raise Exception("invalid hash algorithm, must be md5 or sha1")

        hashed.update(str(string).encode('utf-8'))
        return hashed.hexdigest()


    def get_newest_compatible_destination_run(self, active_nodes: Dict[str, 'Node']):
        """
        Find most-recent (i.e., last) line where this run’s destinations are a subset of the line’s destinations

        :return:
        """
        runs = self.read_runs()[::-1]  # Get the newest runs first.

        for run in runs:
            # (possibly) same project... see if this run’s destinations are a subset of row’s destinations
            # Configs are stable between runs. If they don't match, don't attempt a partial rerun.
            if run['config_hash'] != self.hashes['config_hash']:
                continue

            # Note that row[6] is either (a) "*", (b) a list like "dest1|dest2|dest3", or (c) missing.
            # (Tom, I am ignoring older cases where the selector is missing.)
            if run['selector'] == '*':  # All models are run
                return run
            else:
                _run_destinations = run['selector'].split('|')
                if set(active_nodes.keys()).issubset(set(_run_destinations)):
                    return run
        else:
            return None


    def find_hash_differences(self, run):
        """

        :param run:
        :return:
        """
        differences = []

        if self.hashes['sources_hash'] != run['sources_hash']:
            differences.append(
                "one or more sources"
            )
        if self.hashes['templates_hash'] != run['templates_hash']:
            differences.append(
                "one or more destination templates"
            )
        if self.hashes['mappings_hash'] != run['mappings_hash']:
            differences.append(
                "one or more map_values transformations' map_file"
            )
        if self.hashes['params_hash'] != run['params_hash']:
            differences.append(
                "CLI parameter(s)"
            )

        return differences
