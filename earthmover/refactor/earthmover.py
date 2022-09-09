import json
import logging
import networkx as nx
import os
import time
import yaml

from yaml import SafeLoader

from earthmover.refactor.error_handler import ErrorHandler
from earthmover.refactor.graph import Graph
from earthmover.refactor.runs_file import RunsFile
from earthmover.refactor.nodes.destination import Destination
from earthmover.refactor.nodes.source import Source
from earthmover.refactor.nodes.transformation import Transformation
from earthmover.refactor.util import human_time


class Earthmover:

    version = "0.2.0"

    config_defaults = {
        "state_file": os.path.join(os.path.expanduser("~"), ".earthmover.csv"),
        "output_dir": "./",
        "macros": "",
        "show_graph": False,
        "log_level": "INFO",
        "show_stacktrace": True,
    }

    def __init__(self,
        config_file: str,
        logger: logging.Logger,
        params: str = "",
        force: bool = False,
        skip_hashing: bool = False
    ):
        self.config_file = config_file
        self.params = json.loads(params) if params else {}
        self.force = force
        self.skip_hashing = skip_hashing

        self.error_handler = ErrorHandler(file=config_file)
        self.do_generate = True

        # Parse the user-provided config file and retrieve state-configs.
        # Merge the optional user state configs into the defaults, then clean as necessary.
        self.user_configs = self.load_config_file()

        _state_configs = {**self.config_defaults, **self.user_configs.get('config', {})}
        self.state_configs = {
            'state_file': os.path.expanduser(_state_configs['state_file']),
            'output_dir': os.path.expanduser(_state_configs['output_dir']),
            'macros': _state_configs['macros'].strip(),
            'show_graph': _state_configs['show_graph'],
            'log_level': _state_configs['log_level'].upper(),
            'show_stacktrace': _state_configs['show_stacktrace'],
        }

        # Set up the logger
        self.logger = logger
        self.logger.setLevel(
            logging.getLevelName( self.state_configs['log_level'] )
        )

        # Prepare the output directory for destinations.
        _output_dir = self.state_configs['output_dir']
        if not os.path.isdir(_output_dir):
            self.logger.info(f"creating output directory {_output_dir}")
            os.makedirs(_output_dir, exist_ok=True)

        # Initialize the sources, transformations, and destinations
        self.sources = []
        self.transformations = []
        self.destinations = []

        # Initialize the NetworkX DiGraph
        self.graph = Graph(error_handler=self.error_handler)


    def load_config_file(self):
        """

        :return:
        """
        _env_backup = os.environ.copy()

        # Load & parse config YAML (using modified environment vars)
        os.environ.update(self.params)

        with open(self.config_file, "r") as stream:
            try:
                configs = yaml.load(stream, Loader=SafeLineEnvVarLoader)
            except yaml.YAMLError as err:
                raise Exception(self.error_handler.ctx + f"YAML could not be parsed: {err}")

        # Return environment to original backup
        os.environ = _env_backup

        return configs


    def compile(self):
        self.logger.debug("building dataflow graph")

        ### Build all nodes into a graph
        # sources:
        self.error_handler.assert_key_exists_and_type_is(self.user_configs, 'sources', dict)
        for name, config in self.user_configs['sources'].items():
            if name == "__line__":
                continue  # skip YAML line annotations

            node = Source(name, config, earthmover=self)
            self.sources.append(node)
            self.graph.add_node(f"$sources.{name}", data=node)

            if not node.skip:
                node.compile()


        # transformations:
        if 'transformations' in self.user_configs:
            self.error_handler.assert_key_type_is(self.user_configs, 'transformations', dict)

            for name, config in self.user_configs['transformations'].items():
                if name == "__line__":
                    continue  # skip YAML line annotations

                node = Transformation(name, config, earthmover=self)
                node.compile()
                self.transformations.append(node)
                self.graph.add_node(f"$transformations.{name}", data=node)

                for source in node.sources:
                    if not self.graph.lookup_node(source):
                        self.error_handler.throw(
                            f"invalid source {source}"
                        )
                        raise

                    if source != f"$transformations.{name}":
                        self.graph.add_edge(source, f"$transformations.{name}")


        # destinations:
        self.error_handler.assert_key_exists_and_type_is(self.user_configs, 'destinations', dict)
        for name, config in self.user_configs['destinations'].items():
            if name == "__line__":
                continue  # skip YAML line annotations

            node = Destination(name, config, earthmover=self)
            node.compile()
            self.destinations.append(node)
            self.graph.add_node(f"$destinations.{name}", data=node)

            if not self.graph.lookup_node(node.source):
                self.error_handler.throw(
                    f"invalid source {node.source}"
                )
                raise

            self.graph.add_edge(node.source, f"$destinations.{name}")


        ### Confirm that the graph is a DAG
        self.logger.debug("checking dataflow graph")
        if not nx.is_directed_acyclic_graph(self.graph):
            _cycle = nx.find_cycle(self.graph)
            self.error_handler.throw(
                f"the graph is not a DAG! it has the cycle {_cycle}"
            )
            raise


        ### Delete all trees emanating from a missing/blank source
        node_data = self.graph.get_node_data()
        nodes_to_remove = []

        for component in nx.weakly_connected_components(self.graph):
            skip_nodes = []

            for node in component:
                if node_data[node].type == "source" and node_data[node].skip:
                    skip_nodes.append(node.replace("$sources.", ""))

            #
            if skip_nodes:
                _missing_sources = ", ".join(skip_nodes)

                for skip_node in skip_nodes:
                    for node in nx.dfs_tree(self.graph, f"$sources.{skip_node}"):
                        if node_data[node].type == "destination":
                            _dest_node = node.replace("$destinations.", "")
                            self.logger.info(
                                f"destination {_dest_node} will not be generated because it depends on missing source(s) [{_missing_sources}]"
                            )

                        nodes_to_remove.append(node)

        for node in nodes_to_remove:
            self.graph.remove_node(node)


    def execute(self, subgraph, start_nodes=(), exclude_nodes=()):
        """
        # TODO: `start_nodes` is not used.

        :param subgraph:
        :param start_nodes:
        :param exclude_nodes:
        :return:
        """
        for layer in list(nx.topological_generations(subgraph)):

            for node_name in layer:
                if node_name in exclude_nodes:
                    continue

                nodes_dict = self.graph.get_node_data()

                if not nodes_dict[node_name].data:
                    nodes_dict[node_name].execute()  # Sets self.data in each node.



    def generate(self, selector):
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
        self.compile()

        if selector != "*":
            self.logger.info(f"filtering dataflow graph using selector `{selector}`")

        active_graph = self.graph.select_subgraph(selector)


        ### Hashing requires an entire class mixin and multiple additional steps.
        if not self.skip_hashing:
            _runs_path = self.state_configs['state_file']
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
                most_recent_run = runs_file.get_newest_compatible_destination_run(
                    active_nodes=active_graph.get_node_data()
                )

                if most_recent_run is None:
                    self.logger.info("regenerating (no prior runs found, or config.yaml has changed since last run)")

                else:
                    _run_differences = runs_file.find_hash_differences(most_recent_run)
                    if _run_differences:
                        self.logger.info("regenerating (changes since last run: ")
                        self.logger.info("   [{0}])".format(", ".join(_run_differences)))
                    else:
                        _last_run_string = human_time(int(time.time()) - int(float(most_recent_run['run_timestamp'])))
                        self.logger.info(
                            f"skipping (no changes since the last run {_last_run_string} ago)"
                        )
                        self.do_generate = False

        else:  # Skip hashing
            self.logger.info("skipping hashing and run logging")
            runs_file = None  # This instantiation will never be used, but this avoids linter alerts.


        ### Draw the graph, regardless of whether a run is completed.
        if self.state_configs['show_graph']:
            active_graph.draw()

        # Unchanged runs are avoided unless the user forces the run.
        if not self.do_generate:
            return


        ### Process the graph
        for idx, component in enumerate( nx.weakly_connected_components(active_graph) ):
            self.logger.debug(f"processing component {idx}")

            # load all sources! (in topological sort order)
            subgraph = active_graph.subgraph(component)

            start_nodes = [
                node for node in subgraph.nodes(data=True)
                if node[1]["data"].type == "source"
            ]
            start_node_names = [node[0] for node in start_nodes]
            self.execute(subgraph, start_nodes=start_node_names, exclude_nodes=[])


        ### Save run log only after a successful run! (in case of errors)
        if not self.skip_hashing:
            self.logger.debug("saving details to run log")

            # Build selector information
            if selector == "*":
                destinations = "*"
            else:
                _active_destinations = active_graph.get_node_data().keys()
                destinations = "|".join(_active_destinations)

            runs_file.write_row(selector=destinations)


        ### (Draw the graph again, this time we can add metadata about rows/cols/size at each node)
        # TODO: Re-incorporate metadata.
        if self.state_configs['show_graph']:
            self.logger.info("saving dataflow graph image to `graph.png` and `graph.svg`")
            active_graph.draw()



# This allows us to determine the YAML file line number for any element loaded from YAML
# (very useful for debugging and giving meaningful error messages)
# (derived from https://stackoverflow.com/a/53647080)
# Also added env var interpolation based on
# https://stackoverflow.com/questions/52412297/how-to-replace-environment-variable-value-in-yaml-file-to-be-parsed-using-python#answer-55301129
class SafeLineEnvVarLoader(SafeLoader):

    def construct_mapping(self, node, deep=False):
        mapping = super().construct_mapping(node, deep=deep)

        # expand env vars:
        for k, v in mapping.items():
            if isinstance(v, str):
                mapping[k] = os.path.expandvars(v)

        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping