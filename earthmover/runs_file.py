import csv
import hashlib
import json
import os
import time

from typing import Dict, List, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.earthmover import Earthmover
    from earthmover.nodes.node import Node
    from logging import Logger


class RunsFile:
    """

    """
    BUF_SIZE = 65536  # 64kb chunks
    ROW_COUNT_TO_WARN = 10000
    HASH_ALGORITHM = 'md5'

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
        self.file: str = file

        self.earthmover: 'Earthmover' = earthmover
        self.logger: 'Logger' = self.earthmover.logger
        self.hashes: Dict[str, str] = self._build_hashes()

        # Force the existence of the runs file.
        if not os.path.isfile( self.file ):
            self._write_header()

        self.runs: List[Dict[str, str]] = self._read_runs()


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

        with open(self.file, 'a', encoding='utf-8') as fp:
            writer = csv.DictWriter(fp, fieldnames=self.HEADER)
            writer.writerow(row_dict)


    def get_newest_compatible_run(self, active_nodes: Dict[str, 'Node']) -> Optional[Dict[str, str]]:
        """
        Find most-recent (i.e., last) line where this run’s destinations are a subset of the line’s destinations

        :return:
        """
        runs = self._read_runs()[::-1]  # Get the newest runs first.

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


    def find_hash_differences(self, run: Dict[str, str]) -> List[str]:
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


    def _build_hashes(self) -> Dict[str, str]:
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
        ### Store all hashes into a dictionary to merge with the rest of the row.
        row = {}

        # Hash the configs
        config_hash = self._get_string_hash(json.dumps(self.earthmover.state_configs))
        row['config_hash'] = config_hash


        # Hash the params
        if self.earthmover.params:
            params_hash = self._get_string_hash(self.earthmover.params)
        else:
            params_hash = ""

        row['params_hash'] = params_hash


        # Hash the sources
        node_data = self.earthmover.graph.get_node_data()

        sources_hash = ""
        for source in self.earthmover.sources:

            if f"$sources.{source.name}" not in node_data.keys():
                continue

            if not source.is_remote and source.file and not os.path.isdir(source.file):
                sources_hash += self._get_file_hash(source.file)

        if sources_hash:
            sources_hash = self._get_string_hash(sources_hash)

        row['sources_hash'] = sources_hash


        # Hash the templates
        templates_hash = ""
        for destination in self.earthmover.destinations:

            if f"$destinations.{destination.name}" not in node_data.keys():
                continue

            templates_hash += self._get_file_hash(destination.template)

        if templates_hash != "":
            templates_hash = self._get_string_hash(templates_hash)

        row['templates_hash'] = templates_hash


        # Hash the transformation mapping files
        mappings_hash = ""
        for transformation in self.earthmover.transformations:

            for op in transformation.operations:
                if op.type == "map_values" and op.map_file:
                    mappings_hash += self._get_file_hash(op.map_file)

        if mappings_hash != "":
            mappings_hash = self._get_string_hash(mappings_hash)

        row['mappings_hash'] = mappings_hash

        return row


    def _write_header(self):
        """
        The 'x' open-mode fails if the file already exists.
        Excellent safeguard, Tom!

        :return:
        """
        with open(self.file, 'x', encoding='utf-8') as fp:
            writer = csv.writer(fp)
            writer.writerow(self.HEADER)


    def _read_runs(self) -> List[Dict[str, str]]:
        """

        :return:
        """
        with open(self.file, 'r', encoding='utf-8') as fp:
            runs = list(csv.DictReader(fp, delimiter=','))

        # Raise a warning for the user to manually reset or select a new log-runs file.
        if len(runs) > self.ROW_COUNT_TO_WARN:
            self.logger.warning(
                f"run log file {self.file} is getting long, consider truncating it for better performance.",
                True
            )

        return runs


    def _get_file_hash(self, file: str) -> str:
        """
        Compute the hash of a (potentially large) file by streaming it in from disk

        :param file:
        :return:
        """
        if self.HASH_ALGORITHM == "md5":
            hashed = hashlib.md5()
        elif self.HASH_ALGORITHM == "sha1":
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


    def _get_string_hash(self, string: str) -> str:
        """
        :param string:
        :return:
        """

        if self.HASH_ALGORITHM == "md5":
            hashed = hashlib.md5()
        elif self.HASH_ALGORITHM == "sha1":
            hashed = hashlib.sha1()
        else:
            raise Exception("invalid hash algorithm, must be md5 or sha1")

        hashed.update(str(string).encode('utf-8'))
        return hashed.hexdigest()
