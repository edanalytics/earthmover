import json
import logging
import os
import time

import networkx as nx

from earthmover.refactor.configs import UserConfigs
from earthmover.refactor.error_handler import ErrorHandler
from earthmover.refactor.graph import Graph
from earthmover.refactor.runs_file import RunsFile
from earthmover.refactor.nodes.destination import Destination
from earthmover.refactor.nodes.source import Source
from earthmover.refactor.nodes.transformation import Transformation
from earthmover.refactor.util import human_time


class Earthmover:

    version = "0.2.0"

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
        _user_configs = UserConfigs(self.config_file, params=self.params, error_handler=self.error_handler)
        self.config = _user_configs.get_state_configs()

        # Set up the logger
        self.logger = logger
        self.logger.setLevel(
            logging.getLevelName( self.config['log_level'] )
        )

        # Prepare the output directory for destinations.
        _output_dir = self.config['output_dir']
        if not os.path.isdir(_output_dir):
            self.logger.info(f"creating output directory {_output_dir}")
            os.makedirs(_output_dir, exist_ok=True)

        # Retrieve the sources, transformations, and destinations
        self.sources         = _user_configs.get_sources()
        self.transformations = _user_configs.get_transformations()
        self.destinations    = _user_configs.get_destinations()

        # Finally, prepare the NetworkX DiGraph
        self.graph = Graph(error_handler=self.error_handler)


    def apply_jinja(self, row, template, col, func):
        """
        # TODO: Where should this go?

        :param row:
        :param template:
        :param col:
        :param func:
        :return:
        """
        row["___row_id___"] = row.name

        if func == "modify":
            row["value"] = row[col]

        try:
            value = template.render(row)
        except Exception as err:
            self.error_handler.throw(
                f"Error rendering Jinja template for column `{col}` of `{func}_columns` operation ({err})"
            )
            raise

        row[col] = value
        del row["___row_id___"]

        if func == "modify":
            del row["value"]

        return row


    def compile(self):
        self.logger.debug("building dataflow graph")

        ### Build all nodes into a graph
        # sources:
        for name, config in self.sources.items():
            if name == "__line__":
                continue  # skip YAML line annotations

            _node = Source(name, config, earthmover=self)
            _node.compile()
            self.graph.add_node(f"$sources.{name}", data=_node)


        # transformations:
        for name, operations_config in self.transformations.items():
            if name == "__line__":
                continue  # skip YAML line annotations

            _node = Transformation(name, operations_config, earthmover=self)
            _node.compile()
            self.graph.add_node(f"$transformations.{name}", data=_node)

            for source in _node.sources:
                if not self.graph.lookup_node(source):
                    self.error_handler.throw(
                        f"invalid source {source}"
                    )
                    raise

                if source != f"$transformations.{name}":
                    self.graph.add_edge(source, f"$transformations.{name}")


        # destinations:
        for name, config in self.destinations.items():
            if name == "__line__":
                continue  # skip YAML line annotations

            _node = Destination(name, config, earthmover=self)
            _node.compile()
            self.graph.add_node(f"$destinations.{name}", data=_node)

            if not self.graph.lookup_node(_node.source):
                self.error_handler.throw(
                    f"invalid source {_node.source}"
                )
                raise

            self.graph.add_edge(_node.source, f"$destinations.{name}")


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

            if skip_nodes:
                _missing_sources = ", ".join(skip_nodes)

                for skip_node in skip_nodes:

                    #
                    for node in nx.dfs_tree(self.graph, f"$sources.{skip_node}"):
                        if node_data[node].type == "destination":
                            _dest_node = node.replace("$destinations.", "")
                            self.logger.info(
                                f"destination {_dest_node} will not be generated because it depends on missing source(s) [{_missing_sources}]"
                            )

                        nodes_to_remove.append(node)

        for node in nodes_to_remove:
            self.graph.remove_node(node)


    def execute(self, subgraph, start_nodes=(), exclude_nodes=(), ignore_done=False):
        """
        # TODO: `start_nodes` is not used.

        :param subgraph:
        :param start_nodes:
        :param exclude_nodes:
        :param ignore_done:
        :return:
        """
        for layer in list(nx.topological_generations(subgraph)):

            for node in layer:
                if node in exclude_nodes:
                    continue

                node_data = self.graph.get_node_data()

                if not node_data[node].is_done or ignore_done:
                    node_data[node].execute()  # Sets self.data in each node.


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
            _runs_path = self.config['state_file']
            self.logger.info(f"computing input hashes for run log at {_runs_path}")

            runs_file = RunsFile(_runs_path, earthmover=self)

            # Remote sources cannot be hashed.  # TODO: Use Source.is_remote to complete this check.
            _has_remote_sources = False

            for name, source in self.sources.items():
                if name == "__line__":
                    continue
                if "connection" in source.keys():
                    _has_remote_sources = True
                if "file" in source.keys() and "://" in source["file"]:
                    _has_remote_sources = True

            # Remote sources cannot be hashed; no hashed runs contain remote sources.
            if _has_remote_sources:
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
                        _last_run_string = human_time(int(time.time()) - int(most_recent_run['run_timestamp']))
                        self.logger.info(
                            f"skipping (no changes since the last run {_last_run_string} ago)"
                        )
                        self.do_generate = False

        else:  # Skip hashing
            self.logger.info("skipping hashing and run logging")
            runs_file = None  # This instantiation will never be used, but this avoids linter alerts.


        ### Draw the graph, regardless of whether a run is completed.
        if self.config['show_graph']:
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
        if self.config['show_graph']:
            self.logger.info("saving dataflow graph image to `graph.png` and `graph.svg`")
            active_graph.draw()
