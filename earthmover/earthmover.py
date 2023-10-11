import dask
import json
import logging
import tempfile
import networkx as nx
import os
import time
import datetime
import pandas as pd

from earthmover.error_handler import ErrorHandler
from earthmover.graph import Graph
from earthmover.runs_file import RunsFile
from earthmover.nodes.destination import Destination
from earthmover.nodes.source import Source
from earthmover.nodes.transformation import Transformation
from earthmover.yaml_parser import JinjaEnvironmentYamlLoader
from earthmover import util

from typing import List, Optional


class Earthmover:
    """

    """
    start_timestamp: datetime.datetime  = datetime.datetime.now()
    end_timestamp: Optional[datetime.datetime] = None

    config_defaults = {
        "output_dir": "./",
        "macros": "",
        "show_graph": False,
        "log_level": "INFO",
        "show_stacktrace": False,
        "tmp_dir": tempfile.gettempdir(),
        "show_progress": False,
    }

    sources: List[Source] = []
    transformations: List[Transformation] = []
    destinations: List[Destination] = []

    def __init__(self,
        config_file: str,
        logger: logging.Logger,
        params: str = "",
        force: bool = False,
        skip_hashing: bool = False,
        cli_state_configs: Optional[dict] = None,
        results_file: str = "",
    ):
        self.do_generate = True
        self.force = force
        self.skip_hashing = skip_hashing

        self.results_file = results_file
        self.config_file = config_file
        self.error_handler = ErrorHandler(file=self.config_file)

        # Parse the user-provided config file and retrieve project-configs, macros, and parameter defaults.
        # Merge the optional user configs into the defaults.
        self.params = json.loads(params) if params else {}

        project_configs = JinjaEnvironmentYamlLoader.load_project_configs(self.config_file, params=self.params)
        self.macros = project_configs.get("macros", "").strip()

        # Update parameter defaults, if any.
        for key, val in project_configs.get("parameter_defaults", {}).items():
            self.params.setdefault(key, val)

        # Complete a full-parse of the user config file.
        self.user_configs = JinjaEnvironmentYamlLoader.load_config_file(self.config_file, params=self.params, macros=self.macros)

        self.state_configs = {
            **self.config_defaults,
            **project_configs,
            **(cli_state_configs or {})
        }

        # Set up the logger
        self.logger = logger
        self.logger.setLevel(
            logging.getLevelName( self.state_configs['log_level'].upper() )
        )

        # Prepare the output directory for destinations.
        self.state_configs['output_dir'] = os.path.expanduser(self.state_configs['output_dir'])
        if not os.path.isdir(self.state_configs['output_dir']):
            self.logger.info(
                f"creating output directory {self.state_configs['output_dir']}"
            )
            os.makedirs(self.state_configs['output_dir'], exist_ok=True)

        # Set the temporary directory in cases of disk-spillage.
        dask.config.set({'temporary_directory': self.state_configs['tmp_dir']})

        # Initialize the NetworkX DiGraph
        self.graph = Graph(error_handler=self.error_handler)

        # Initialize a dictionary for tracking run metadata (for structured output)
        self.metadata = {
            "started_at": self.start_timestamp.isoformat(timespec='microseconds'),
            "working_dir": os.getcwd(),
            "config_file": self.config_file,
            "output_dir": self.state_configs["output_dir"],
            "row_counts": {}
        }

    
    def build_graph(self):
        """

        :return:
        """
        self.logger.debug("building dataflow graph")

        node_types = {
            'sources': Source,
            'transformations': Transformation,
            'destinations': Destination,
        }

        ### Build the graph type-by-type
        for node_type, node_class in node_types.items():
            nodes = self.error_handler.assert_get_key(self.user_configs, node_type, dtype=dict, required=False, default={})

            # Place the nodes
            for name, config in nodes.items():
                node = node_class(name, config, earthmover=self)
                self.graph.add_node(f"${node_type}.{name}", data=node)

                # Place edges for transformations and destinations
                for source in node.upstream_sources:
                    try:
                        node.upstream_sources[source] = self.graph.ref(source)
                        self.graph.add_edge(source, f"${node_type}.{name}")
                    except KeyError:
                        self.error_handler.throw(f"invalid source {source}")

        ### Confirm that the graph is a DAG
        self.logger.debug("checking dataflow graph")
        if not nx.is_directed_acyclic_graph(self.graph):
            _cycle = nx.find_cycle(self.graph)
            self.error_handler.throw(
                f"the graph is not a DAG! it has the cycle {_cycle}"
            )
            raise

        ### Delete all nodes not connected to a destination.
        while True:  # Iterate until no nodes are removed.
            terminal_nodes = self.graph.get_terminal_nodes()

            for node_name in terminal_nodes:
                node = self.graph.ref(node_name)

                if node.type != 'destination':
                    self.graph.remove_node(node_name)
                    self.logger.warning(
                        f"{node.type} node `{node.name}` will not be generated because it is not connected to a destination"
                    )

            # Iterate until no nodes are removed.
            if set(terminal_nodes) == set(self.graph.get_terminal_nodes()):
                break


    def hash_graph_to_runs_file(self, subgraph: Graph):
        """

        :return:
        """
        ### Hashing requires an entire class mixin and multiple additional steps.
        if not self.skip_hashing and self.state_configs.get('state_file', False):
            _runs_path = os.path.expanduser(self.state_configs['state_file'])

            self.logger.info(f"computing input hashes for run log at {_runs_path}")

            runs_file = RunsFile(_runs_path, earthmover=self)

            # Remote sources cannot be hashed; no hashed runs contain remote sources.
            if any(source.is_remote for source in self.sources):
                self.logger.info(
                    "forcing regenerate, since some sources are remote (and we cannot know if they changed)"
                )

            elif self.force:
                self.logger.info("forcing regenerate")

            else:
                self.logger.info("checking for prior runs...")

                # Find the latest run that matched our selector(s)...
                most_recent_run = runs_file.get_newest_compatible_run(
                    active_nodes=subgraph.get_node_data()
                )

                if most_recent_run is None:
                    self.logger.info("regenerating (no prior runs found, or config.yaml has changed since last run)")

                else:
                    _run_differences = runs_file.find_hash_differences(most_recent_run)
                    if _run_differences:
                        self.logger.info("regenerating (changes since last run: ")
                        self.logger.info("   [{0}])".format(", ".join(_run_differences)))
                    else:
                        _last_run_string = util.human_time(
                            int(time.time()) - int(float(most_recent_run['run_timestamp'])))
                        self.logger.info(
                            f"skipping (no changes since the last run {_last_run_string} ago)"
                        )
                        self.do_generate = False

        elif not self.state_configs.get('state_file', False):
            self.logger.info("skipping hashing and run-logging (no `state_file` defined in config)")
            runs_file = None  # This instantiation will never be used, but this avoids linter alerts.

        else:  # Skip hashing
            self.logger.info("skipping hashing and run-logging (run initiated with `--skip-hashing` flag)")
            runs_file = None  # This instantiation will never be used, but this avoids linter alerts.

        return runs_file


    def compile(self, subgraph: Optional[Graph] = None):
        """

        :param subgraph:
        :return:
        """
        if subgraph is None:
            subgraph = self.graph

        for layer in list(nx.topological_generations(subgraph)):

            for node_name in layer:
                node = self.graph.ref(node_name)
                node.compile()

                # Add the active nodes to the class attribute lists for the hashing file.
                if node.type == 'source':
                    self.sources.append(node)

                elif node.type == 'transformation':
                    self.transformations.append(node)

                elif node.type == 'destination':
                    self.destinations.append(node)

        ### Confirm that at least one source is defined.
        if not self.sources:
            self.error_handler.throw("No sources have been defined!")


    def execute(self, subgraph: Graph):
        """

        :param subgraph:
        :return:
        """
        for layer in list(nx.topological_generations(subgraph)):
            for node_name in layer:
                node = self.graph.ref(node_name)
                if not node.data:
                    node.execute()  # Sets self.data in each node.
                    node.post_execute()
                    if self.results_file:
                        self.metadata["row_counts"].update({f"${node.type}s.{node.name}": len(node.data)})


    def generate(self, selector: str):
        """
        Build DAG from YAML configs

        Build subgraph to process based on the selector. We always run through from sources to destinations
        (so all ancestors and descendants of selected nodes are also selected) but here we allow processing
        only parts/paths of the graph. Selectors may select just one node ("node_1") or several
        ("node_1,node_2,node_3"). Selectors may also contain wildcards ("node_*"), and these operations may
        be composed ("node_*_cheeses,node_*_fruits").

        :param selector:
        :return:
        """
        self.build_graph()

        if selector != "*":
            self.logger.info(f"filtering dataflow graph using selector `{selector}`")

        active_graph = self.graph.select_subgraph(selector)
        self.compile(active_graph)


        ### Hashing requires an entire class mixin and multiple additional steps.
        runs_file = self.hash_graph_to_runs_file(active_graph)


        ### Draw the graph, regardless of whether a run is completed.
        if self.state_configs['show_graph']:
            active_graph.draw()

        # Unchanged runs are avoided unless the user forces the run.
        if not self.do_generate:
            exit(99) # Operation canceled


        ### Process the graph
        for idx, component in enumerate( nx.weakly_connected_components(active_graph) ):
            self.logger.debug(f"processing component {idx}")

            # load all sources! (in topological sort order)
            _subgraph = active_graph.subgraph(component)
            self.execute(_subgraph)


        ### Save run log only after a successful run! (in case of errors)
        # Note: `runs_file` is only defined in certain circumstances.
        if not self.skip_hashing and runs_file:
            self.logger.debug("saving details to run log")

            # Build selector information
            if selector == "*":
                destinations = "*"
            else:
                _active_destinations = active_graph.get_node_data().keys()
                destinations = "|".join(_active_destinations)

            runs_file.write_row(selector=destinations)


        ### Draw the graph again, this time add metadata about rows/cols/size at each node
        if self.state_configs['show_graph']:
            self.logger.info("saving dataflow graph image to `graph.png` and `graph.svg`")

            # Compute all row number values at once for performance, then update the nodes.
            computed_node_rows = dask.compute(
                {node_name: node.num_rows for node_name, node in self.graph.get_node_data().items()}
            )[0]

            for node_name, num_rows in computed_node_rows.items():
                node = self.graph.ref(node_name)
                node.num_rows = num_rows

            active_graph.draw()
        
        ### Create structured output results_file if necessary
        if self.results_file:

            # create directory if not exists
            os.makedirs(os.path.dirname(self.results_file), exist_ok=True)

            self.end_timestamp = datetime.datetime.now()
            self.metadata.update({"completed_at": self.end_timestamp.isoformat(timespec='microseconds')})
            self.metadata.update({"runtime_sec": (self.end_timestamp - self.start_timestamp).total_seconds()})
            with open(self.results_file, 'w') as fp:
                fp.write(json.dumps(self.metadata, indent=4))


    def test(self, tests_dir: str):
        # delete files in tests/output/
        output_dir = os.path.join(tests_dir, "outputs")
        for f in os.listdir(output_dir):
            os.remove(os.path.join(output_dir, f))

        # run earthmover!
        self.generate(selector="*")

        # compare tests/outputs/* against tests/expected/*
        for filename in os.listdir( os.path.join(tests_dir, 'expected') ):

            # load expected and outputted content as dataframes, and sort them
            # because dask may shuffle output order
            _expected_file  = os.path.join(tests_dir, 'expected', filename)
            with open(_expected_file, "r", encoding='utf-8') as f:
                _expected_df = pd.DataFrame([l.strip() for l in f.readlines()])
                _expected_df = _expected_df.sort_values(by=_expected_df.columns.tolist()).reset_index(drop=True)

            _outputted_file = os.path.join(tests_dir, 'outputs', filename)
            with open(_outputted_file, "r", encoding='utf-8') as f:
                _outputted_df = pd.DataFrame([l.strip() for l in f.readlines()])
                _outputted_df = _outputted_df.sort_values(by=_outputted_df.columns.tolist()).reset_index(drop=True)
            
            # compare sorted contents
            if not _expected_df.equals(_outputted_df):
                self.logger.critical(f"Test output `{_outputted_file}` does not match expected output.")
                exit(1)
