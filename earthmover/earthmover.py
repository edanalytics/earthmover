import dask
import json
import logging
import networkx as nx
import os
import string
import time
import yaml
import jinja2
import pandas as pd

from string import Template
from typing import Optional

from earthmover.error_handler import ErrorHandler
from earthmover.graph import Graph
from earthmover.runs_file import RunsFile
from earthmover.nodes.destination import Destination
from earthmover.nodes.source import Source
from earthmover.nodes.transformation import Transformation
from earthmover.yaml_parser import SafeLineEnvVarLoader
from earthmover import util


class Earthmover:

    config_defaults = {
        "output_dir": "./",
        "macros": "",
        "show_graph": False,
        "log_level": "INFO",
        "show_stacktrace": False,
    }

    def __init__(self,
        config_file: str,
        logger: logging.Logger,
        params: str = "",
        force: bool = False,
        skip_hashing: bool = False,
        cli_state_configs: Optional[dict] = None
    ):
        self.do_generate = True
        self.force = force
        self.skip_hashing = skip_hashing
        self.macros = ""
        self.macros_lines = 0

        self.config_file = config_file
        self.config_template_string = ""
        self.config_template = None
        self.config_yaml = ""
        self.error_handler = ErrorHandler(file=self.config_file)

        # Parse the user-provided config file and retrieve state-configs.
        # Merge the optional user state configs into the defaults, then clean as necessary.
        self.params = json.loads(params) if params else {}
        self.user_configs = self.load_config_file()

        if cli_state_configs is None:
            cli_state_configs = {}

        _state_configs = {**self.config_defaults, **self.user_configs.get('config', {}), **cli_state_configs}
        self.state_configs = {
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


    def load_config_file(self) -> dict:
        """

        :param: params
        :return:
        """

        # pass 1: grab config.macros (if any) so Jinja in the YAML can be rendered with macros
        with open(self.config_file, "r") as stream:
            # cannot just yaml.load() here, since Jinja in the YAML may make it invalid...
            # instead, pull out just the `config` section, which must not contain Jinja (except for `macros`)
            # then we yaml.load() just the config section to grab any `macros`
            start = None
            end = None

            lines = stream.readlines()
            for idx, line in enumerate(lines):

                # Find the start of the config block.
                if line.startswith("config:"):
                    start = idx
                    continue

                # Find the end of the config block (i.e., the next top-level field)
                if start is not None and not line.startswith(tuple(string.whitespace+"#")):
                    end = idx
                    break

            # Read the configs block and extract the (optional) macros field.
            if start is not None and end is not None:
                configs_pass1 = yaml.safe_load("".join(lines[start:end]))
                self.macros = configs_pass1.get("config", {}).get("macros", "")
            else:
                configs_pass1 = {}

            # Figure out lines range of macro definitions, to skip (re)reading/parsing them later
            self.macros_lines = self.macros.count("\n")
            macros_definitions = [i for i, x in enumerate(lines) if x.strip().startswith('macros:')]


        # pass 2:
        #   (a) load template YAML minus macros (which were already loaded in pass 1)
        #   (b) replace envvars
        #   (c) render Jinja in YAML template
        #   (d) load YAML to config Dict

        # (a)
        if len(macros_definitions)>0:
            self.config_template_string = "".join(lines[:macros_definitions[0]] + lines[macros_definitions[0] + self.macros_lines + 1:])
        else:
            self.config_template_string = "".join(lines)

        # (b)
        _env_backup = os.environ.copy() # backup envvars
        os.environ.update(self.params) # override with CLI params

        for k, v in configs_pass1.get("config", {}).get("parameter_defaults", {}).items():
            if isinstance(v, str):
                os.environ.setdefault(k, v)  # set defaults, if any
            else:
                self.error_handler.throw(
                    f"YAML config.parameter_defaults.{k} must be a string"
                )
                raise

        self.config_template_string = Template(self.config_template_string).safe_substitute(os.environ)
        os.environ = _env_backup # restore envvars

        # Uncomment the following to view original template yaml and parsed yaml:
        # with open("./earthmover_template.yml", "w") as f:
        #     f.write(self.config_template_string)
        
        # (c)
        try:
            self.config_template = jinja2.Environment(
                loader=jinja2.FileSystemLoader(os.path.dirname('./'))
            ).from_string(self.macros + self.config_template_string)

            self.config_yaml = self.config_template.render()
            # Uncomment the following to view original template yaml and parsed yaml:
            # with open("./earthmover_yaml.yml", "w") as f:
            #     f.write(self.config_yaml)
    
        except Exception as err:
            lineno = util.jinja2_template_error_lineno()
            if lineno:
                lineno = ", near line " + str(lineno - self.macros_lines - 1)
            self.error_handler.throw(
                f"Jinja syntax error in YAML configuration template{lineno} ({err})"
            )
            raise

        # (d)
        try:
            configs_pass2 = yaml.load(self.config_yaml, Loader=SafeLineEnvVarLoader)
            configs_pass2.get("config", {}).update({"macros": self.macros})
        except yaml.YAMLError as err:
            linear_err = " ".join([line.replace("^", "").strip() for line in str(err).split("\n")])
            self.error_handler.throw(
                f"YAML could not be parsed: {linear_err}"
            )
            raise

        return configs_pass2

    
    def build_graph(self):
        """

        :return:
        """
        self.logger.debug("building dataflow graph")

        ### Build all nodes into a graph
        # sources:
        _sources = self.error_handler.assert_get_key(self.user_configs, 'sources', dtype=dict)
        for name, config in _sources.items():

            node = Source(name, config, earthmover=self)
            self.graph.add_node(f"$sources.{name}", data=node)

        # transformations:
        _transformations = self.error_handler.assert_get_key(
            self.user_configs, 'transformations',
            dtype=dict, required=False, default={}
        )

        for name, config in _transformations.items():

            node = Transformation(name, config, earthmover=self)
            self.graph.add_node(f"$transformations.{name}", data=node)

            for source in node.sources:
                if not self.graph.ref(source):
                    self.error_handler.throw(
                        f"invalid source {source}"
                    )
                    raise

                if source != f"$transformations.{name}":
                    self.graph.add_edge(source, f"$transformations.{name}")

        # destinations:
        _destinations = self.error_handler.assert_get_key(self.user_configs, 'destinations', dtype=dict)
        for name, config in _destinations.items():

            node = Destination(name, config, earthmover=self)
            self.graph.add_node(f"$destinations.{name}", data=node)

            if not self.graph.ref(node.source):
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


    def compile(self, subgraph = None):
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

                node.compile()


    def execute(self, subgraph):
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
        self.build_graph()

        if selector != "*":
            self.logger.info(f"filtering dataflow graph using selector `{selector}`")

        active_graph = self.graph.select_subgraph(selector)
        self.compile(active_graph)


        ### Hashing requires an entire class mixin and multiple additional steps.
        if not self.skip_hashing and 'state_file' in self.state_configs:
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
                        _last_run_string = util.human_time(int(time.time()) - int(float(most_recent_run['run_timestamp'])))
                        self.logger.info(
                            f"skipping (no changes since the last run {_last_run_string} ago)"
                        )
                        self.do_generate = False

        elif 'state_file' not in self.state_configs:
            self.logger.info("skipping hashing and run-logging (no `state_file` defined in config)")
            runs_file = None  # This instantiation will never be used, but this avoids linter alerts.
         
        else:  # Skip hashing
            self.logger.info("skipping hashing and run-logging (run initiated with `--skip-hashing` flag)")
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


    def test(self, tests_dir):
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
            with open(_expected_file, "r") as f:
                _expected_df = pd.DataFrame([l.strip() for l in f.readlines()])
                _expected_df = _expected_df.sort_values(by=_expected_df.columns.tolist()).reset_index(drop=True)

            _outputted_file = os.path.join(tests_dir, 'outputs', filename)
            with open(_outputted_file, "r") as f:
                _outputted_df = pd.DataFrame([l.strip() for l in f.readlines()])
                _outputted_df = _outputted_df.sort_values(by=_outputted_df.columns.tolist()).reset_index(drop=True)
            
            # compare sorted contents
            if not _expected_df.equals(_outputted_df):
                self.logger.critical(f"Test output `{_outputted_file}` does not match expected output.")
                exit(1)
