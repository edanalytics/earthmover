import os
import re
import csv
import json
import math
import time
import yaml
import logging
from pathlib import Path
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from yaml.loader import SafeLoader
import hashlib

from earthmover.earthmover_errorhandler import ErrorHandler
from earthmover.earthmover_source import Source
from earthmover.earthmover_transformation import Transformation
from earthmover.earthmover_destination import Destination

parameters = {}

# from https://gist.github.com/miku/dc6d06ed894bc23dfd5a364b7def5ed8
class dotdict(dict):
    __getattr__ = dict.get
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        for k, v in self.items():
            if isinstance(v, dict): self[k] = dotdict(v)
    def lookup(self, dotkey):
        path = list(reversed(dotkey.split(".")))
        v = self
        while path:
            key = path.pop()
            if isinstance(v, dict): v = v[key]
            elif isinstance(v, list): v = v[int(key)]
            else: raise KeyError(key)
        return v


# This allows us to determine the YAML file line number for any element loaded from YAML
# (very useful for debugging and giving meaningful error messages)
# (derived from https://stackoverflow.com/a/53647080)
# Also added env var interpolation based on
# https://stackoverflow.com/questions/52412297/how-to-replace-environment-variable-value-in-yaml-file-to-be-parsed-using-python#answer-55301129
class SafeLineEnvVarLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineEnvVarLoader, self).construct_mapping(node, deep=deep)

        # expand env vars:
        for k,v in mapping.items():
            if isinstance(v, str):
                mapping[k] = os.path.expandvars(v)
        
        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping


class Earthmover:

    version = "0.0.1"
    config_defaults = {
        "state_file": os.path.join(os.path.expanduser("~"), ".earthmover.csv"),
        "output_dir": "./",
        "macros": "",
        "memory_limit": "1GB",
        "show_graph": False,
        "log_level": "INFO",
        "verbose": False,
        "show_stacktrace": False
    }
    
    def __init__(self, config_file, params="", force=False, skip_hashing=False):
        self.config_file = config_file
        self.error_handler = ErrorHandler(file=config_file)
        self.logger = logging.getLogger('earthmover')
        self.graph = nx.DiGraph() # dependency graph
        self.t0 = time.time()
        self.memory_usage = 0
        self.dispatcher_status_counts = {}
        self.dispatcher_errors = 0
        self.do_generate = True
        self.params = params
        self.force=force
        self.skip_hashing=skip_hashing

        # Load any CLI params
        parameters = {}
        if params!="": parameters = json.loads(params)
        
        # Make a backup copy of the current environment variables before we start messing with them
        env_backup = os.environ.copy()

        # and make a copy we can start to modify
        env_copy = os.environ.copy()

        # Add in (overwrite) any CLI params
        if isinstance(parameters, dict):
            for k,v in parameters.items():
                env_copy[k] = v
        
        # (Temporarily) set environment vars to modified version
        os.environ = env_copy
        
        # Load & parse config YAML (using modified environment vars):
        with open(config_file, "r") as stream:
            try:
                user_config = yaml.load(stream, Loader=SafeLineEnvVarLoader)
            except yaml.YAMLError as e:
                raise Exception(self.error_handler.ctx + "YAML could not be parsed: {0}".format(e))
        
        # Return environment to original backup
        os.environ = env_backup
        
        # Process configs, merge with defaults to fill in any missing values
        if "config" in user_config.keys() and isinstance(user_config["config"], dict):
            self.config = dotdict(self.merge_config(user_config["config"], self.config_defaults))
        else: self.config = dotdict(self.config_defaults)
        self.config.memory_limit = self.string_to_bytes(self.config.memory_limit)
        self.config.macros = self.config.macros.strip()

        # Set up logging
        handler = logging.StreamHandler()
        handler.setLevel(logging.getLevelName(self.config.log_level))
        formatter = logging.Formatter("%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s",
            "%Y-%m-%d %H:%M:%S"
            )
        handler.setFormatter(formatter)
        self.logger.setLevel(logging.getLevelName(self.config.log_level))
        self.logger.addHandler(handler)
        
        # Check if output_dir exists, if not, create it:
        self.config.output_dir = os.path.expanduser(self.config.output_dir)
        if not os.path.isdir(self.config.output_dir):
            self.logger.info(f'Creating output directory {self.config.output_dir}')
            Path(self.config.output_dir).mkdir(parents=True, exist_ok=True)

        # Expand state_file if necessary:
        self.config.state_file = os.path.expanduser(self.config.state_file)
        
        # sources and destinations are required; transformations are optional
        self.error_handler.assert_key_exists_and_type_is(user_config, "sources", dict)
        self.sources = user_config["sources"]
        if "transformations" in user_config.keys() and len(user_config["transformations"])>0:
            self.transformations = user_config["transformations"]
        else: self.transformations = {}
        self.error_handler.assert_key_exists_and_type_is(user_config, "destinations", dict)
        self.destinations = user_config["destinations"]

    # Compute the hash of a (potentially large) file by streaming it in from disk
    def get_file_hash(self, file, hash_algorithm):
        BUF_SIZE = 65536  # 64kb chunks
        if hash_algorithm=="md5": hashed = hashlib.md5()
        elif hash_algorithm=="sha1": hashed = hashlib.sha1()
        else: raise Exception("invalid hash algorithm, must be md5 or sha1")
        with open(file, 'rb') as f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                hashed.update(data)
        return hashed.hexdigest()
    
    def get_string_hash(self, string, hash_algorithm):
        BUF_SIZE = 65536  # 64kb chunks
        if hash_algorithm=="md5": hashed = hashlib.md5()
        elif hash_algorithm=="sha1": hashed = hashlib.sha1()
        else: raise Exception("invalid hash algorithm, must be md5 or sha1")
        hashed.update(string.encode('utf-8'))
        return hashed.hexdigest()
    
    # Turns a human-readable bytes string to an integer
    def string_to_bytes(self, bytes_str):
        bytes_str = bytes_str.replace("B","")
        if "K" in bytes_str:
            return int(bytes_str.replace("K","")) * 1024
        if "M" in bytes_str:
            return int(bytes_str.replace("M","")) * 1024 * 1024
        if "G" in bytes_str:
            return int(bytes_str.replace("G","")) * 1024 * 1024 * 1024
        return int(bytes_str)

    # Turns a raw bytes integer into human-readable format like 322MB
    def human_size(self, bytes, units=['B','KB','MB','GB','TB', 'PB', 'EB']):
        return str(bytes) + units[0] if bytes < 1024 else self.human_size(bytes>>10, units[1:])

    # Expose the dotdict class to other classes
    def to_dotdict(self, a_dict):
        return dotdict(a_dict)
    
    def merge_config(self, user, default):
        if isinstance(user, dict) and isinstance(default, dict):
            for k, v in default.items():
                if k not in user:
                    user[k] = v
                else:
                    user[k] = self.merge_config(user[k], v)
        return user

    # Determine field separator from file extension
    def get_sep(self, file_name):
        if ".csv" in file_name: return ","
        elif ".tsv" in file_name: return "\t"
        else: raise Exception("file format of {0} not recognized, must be .tsv or .csv".format(file_name))

    # Turns a raw duration (seconds) integer into a human-readable approximation like "42 minutes"
    def human_time(self, seconds):
        if seconds<60: return "less than a minute"
        if seconds<90: return "about a minute"
        if seconds<150: return "a couple minutes"
        if seconds<3600: return str(round(seconds/60))+" minutes"
        if round(seconds/60)<80: return "about an hour"
        if round(seconds/60)<150: return "a couple hours"
        if seconds<86400: return str(round(seconds/3600))+" hours"
        if seconds<129600: return "about a day"
        if seconds<216000: return "a couple of days"
        return str(round(seconds/86400))+" days"

    def profile(self, msg, force=False):
        t = time.time()
        if self.config.verbose or force: print(str(t-self.t0) + "\t" + msg)

    def profile_memory(self):
        self.update_memory_usage()
        usage = self.memory_usage
        limit = self.config.memory_limit
        self.profile("   [using {0}/{1} ({2}) memory bytes]".format(self.human_size(usage), self.human_size(limit), "{:.2%}".format(usage/limit)))

    def update_memory_usage(self):
        self.memory_usage = 0
        for node in self.graph.nodes(data=True):
            self.memory_usage += node[1]["data"].memory_usage
    
    # Destinations can reference either sources directly, or an intermediate transformation.
    # This function determines which a reference refers to, and returns the appropriate target.
    def ref(self, ref):
        # look up the node by name in the graph:
        return self.graph.nodes[ref]["data"]

    # Convenience function, packs columns with small number of unique values using 'category' dtype
    def pack_dataframe(self, df):
        for column in df.columns.values:
            if len(pd.unique(df[column]))<500:
                df[column] = df[column].fillna('').astype('category')
                if '' not in df[column].cat.categories:
                    df[column] = df[column].cat.add_categories('')
        return df

    # draws the dependency graph
    def draw_graph(self, graph):
        # Set image size:
        image_width = 20
        image_height = 14
        f = plt.figure(figsize=(image_width,image_height))

        # Pre-build lists of source, transformation, and destination nodes
        sources = []
        destinations = []
        transformations = []
        node_labels = {}
        node_sizes = {}
        for node in graph.nodes(data=True):
            node_labels[node[0]] = node[1]["data"].name
            
            size_pieces = []
            if node[1]["data"].size > 0: size_pieces.append(self.human_size(node[1]["data"].size))
            if node[1]["data"].rows > 0 and node[1]["data"].cols > 0:
                size_pieces.append("{0}r x {1}c".format(node[1]["data"].rows, node[1]["data"].cols))
            elif node[1]["data"].rows > 0:
                size_pieces.append("{0}r".format(node[1]["data"].rows))
            if len(size_pieces)>0: 
                node_sizes[node[0]] = ", ".join(size_pieces)
            else: node_sizes[node[0]] = ""
            
            if node[1]["data"].type=="source":
                sources.append(node[0])
            elif node[1]["data"].type=="destination":
                destinations.append(node[0])
            else: transformations.append(node[0])
        
        # Position nodes using PyGraphViz (needs to be apt/pip installed separately):
        node_positions = nx.drawing.nx_agraph.graphviz_layout(graph, prog='dot', args='-Grankdir=LR')

        # Calculate label positions: sources to left of node, destinations to right of node, transformations centered
        label_positions = {}
        size_positions = {}
        label_off = round(7*math.sqrt(len(graph.nodes)))  # offset on the x axis
        size_off = max(1, len(graph.nodes)-4)
        for k, v in node_positions.items():
            if k in sources: label_positions[k] = (v[0]-label_off, v[1])
            elif k in destinations: label_positions[k] = (v[0]+label_off, v[1])
            else: label_positions[k] = (v[0], v[1]+size_off+1)

            if k in sources: size_positions[k] = (v[0]-label_off, v[1]-size_off)
            elif k in destinations: size_positions[k] = (v[0]+label_off, v[1]-size_off)
            else: size_positions[k] = (v[0]+size_off, v[1]+1)

        # Some configs:
        label_options = { "font_size": 12, "font_color": "whitesmoke" }
        size_options = { "font_size": 8, "font_color": "black" }
        boxstyle = 'round,pad=0.3'

        # Draw sources:
        nx.draw_networkx_nodes(graph, pos=node_positions, nodelist=sources, node_color="tab:green")
        nx.draw_networkx_labels(graph, pos=label_positions, horizontalalignment="right", **label_options,
            labels={ k: v for k, v in node_labels.items() if k in sources },
            bbox=dict(facecolor="tab:green", edgecolor="black", boxstyle=boxstyle, zorder=-1.0))
        nx.draw_networkx_labels(graph, pos=size_positions, horizontalalignment="right", **size_options,
            labels={ k: v for k, v in node_sizes.items() if k in sources })
        
        # Draw transformations:
        nx.draw_networkx_nodes(graph, pos=node_positions, nodelist=transformations, node_color="tab:blue")
        nx.draw_networkx_labels(graph, pos=label_positions, horizontalalignment="center", **label_options,
            labels={ k: v for k, v in node_labels.items() if k in transformations },
            bbox=dict(facecolor="tab:blue", edgecolor="black", boxstyle=boxstyle, zorder=-1.0))
        nx.draw_networkx_labels(graph, pos=size_positions, horizontalalignment="left", **size_options,
            labels={ k: v for k, v in node_sizes.items() if k in transformations })
        
        # Draw destinations:
        nx.draw_networkx_nodes(graph, pos=node_positions, nodelist=destinations, node_color="tab:red")
        nx.draw_networkx_labels(graph, pos=label_positions, horizontalalignment="left", **label_options,
            labels={ k: v for k, v in node_labels.items() if k in destinations },
            bbox=dict(facecolor="tab:red", edgecolor="black", boxstyle=boxstyle, zorder=-1.0))
        nx.draw_networkx_labels(graph, pos=size_positions, horizontalalignment="left", **size_options,
            labels={ k: v for k, v in node_sizes.items() if k in destinations })
        
        # Draw edges:
        nx.draw_networkx_edges(graph, pos=node_positions, arrowsize=20)

        # Add legend:
        legend = [
            mpatches.Patch(color='tab:green', label='sources'),
            mpatches.Patch(color='tab:blue', label='transformations'),
            mpatches.Patch(color='tab:red', label='destinations')
        ]
        plt.legend(handles=legend, loc='lower center', ncol=3)

        # Save graph image
        plt.margins(0.3)
        plt.savefig("graph.svg")
        plt.savefig("graph.png")
        plt.clf()
        plt.close()
    
    def lookup_node(self, name):
        for node in self.graph.nodes(data=True):
            if node[0]==name:
                return node[1]["data"]
        return False

    def process(self, graph, start_nodes=[], exclude_nodes=[], ignore_done=False):
        layers = list(nx.topological_generations(graph))
        node_data = { node[0]: node[1]["data"] for node in graph.nodes(data=True) }
        prev_layer = []
        for layer in layers:
            for node in layer:
                if node in exclude_nodes:
                    continue
                if not node_data[node].is_done or ignore_done:
                    node_data[node].do()
        for node in prev_layer:
                node_data[node].clear()

    def select_subgraph(self, selector):
        if selector=="*": graph = self.graph
        else:
            if "," in selector:
                selectors = selector.split(",")
            else: selectors = [selector]
            all_nodes = self.graph.nodes
            all_selected_nodes = []
            for selector in selectors:
                selected_nodes = []
                pattern = re.compile(selector.replace("*",".*"))
                for node in all_nodes:
                    if(pattern.search(node)): selected_nodes.append(node)
                ancestor_nodes = []
                for node in selected_nodes:
                    ancestor_nodes += list(nx.ancestors(self.graph, node))
                descendant_nodes = []
                for node in selected_nodes:
                    descendant_nodes += list(nx.descendants(self.graph, node))
                selected_nodes += descendant_nodes + ancestor_nodes
                all_selected_nodes += selected_nodes
            graph = nx.subgraph(self.graph, all_selected_nodes)
        return graph

    def build_graph(self):
        self.logger.debug("building dataflow graph")
        # sources:
        for name, config in self.sources.items():
            if name=="__line__": continue # skip YAML line annotations
            node = Source(name, dotdict(config), self)
            self.graph.add_node("$sources."+name, data=node)
        
        # transformations:
        for name, ops in self.transformations.items():
            if name=="__line__": continue # skip YAML line annotations
            node = Transformation(name, ops, self)
            self.graph.add_node("$transformations."+name, data=node)
            has_chunked_source = False
            for i, op in enumerate(ops, start=1):
                self.error_handler.ctx.update(file=self.config_file, line=op["__line__"], node=node, operation=op)
                if "sources" in op.keys():
                    for source in op["sources"]:
                        source_node = self.lookup_node(source)
                        if not source_node:
                            self.error_handler.throw(f"invalid source {source}")
                        if source_node.is_chunked: has_chunked_source = True
                        if source!="$transformations."+name:
                            self.graph.add_edge(source, "$transformations."+name)
                elif "source" in op.keys():
                    source_node = self.lookup_node(op["source"])
                    if not source_node:
                        self.error_handler.throw("invalid source {0}".format(op["source"]))
                    if source_node.is_chunked: has_chunked_source = True
                    if op["source"]!="$transformations."+name:
                        self.graph.add_edge(op["source"], "$transformations."+name)
                else:
                    self.error_handler.throw(f"no source(s) defined for transformation operation")
            if has_chunked_source: node.is_chunked = True
        
        # destinations:
        for name, config in self.destinations.items():
            if name=="__line__": continue # skip YAML line annotations
            node = Destination(name, config, self)
            self.error_handler.ctx.update(file=self.config_file, line=config["__line__"], node=node, operation=None)
            self.graph.add_node("$destinations."+name, data=node)
            self.error_handler.assert_key_exists_and_type_is(config, "source", str)
            self.error_handler.assert_key_exists_and_type_is(config, "extension", str)
            source_node = self.lookup_node(config["source"])
            if not source_node:
                self.error_handler.throw("invalid source {0}".format(config["source"]))
            self.graph.add_edge(config["source"], "$destinations."+name)
            if source_node.is_chunked: node.is_chunked = True

        # Confirm that the graph is a DAG
        self.logger.debug("checking dataflow graph")
        if not nx.is_directed_acyclic_graph(self.graph):
            self.error_handler.throw("the graph is not a DAG! it has the cycle {0}".format(nx.find_cycle(self.graph)))

        # Delete any components that have a blank source
        node_data = { node[0]: node[1]["data"] for node in self.graph.nodes(data=True) }
        nodes_to_remove = []
        for component in nx.weakly_connected_components(self.graph):
            skip_nodes = []
            for node in component:
                node_data = { node[0]: node[1]["data"] for node in self.graph.nodes(data=True) }
                if node_data[node].type=="source" and node_data[node].skip:
                    skip_nodes.append(node.replace("$sources.", ""))
            if len(skip_nodes)>0:
                missing_sources = ", ".join(skip_nodes)
                for node in component:
                    if node_data[node].type=="destination":
                        dest_node = node.replace("$destinations.","")
                        self.logger.warn(f"destination {dest_node} will not be generated because it depends on missing source(s) [{missing_sources}]")
                    nodes_to_remove.append(node)
        for node in nodes_to_remove:
            self.graph.remove_node(node)


    def generate(self, selector):

        # Build DAG from YAML configs
        self.build_graph()
        
        # Build subgraph to process based on the selector. We always run through from sources to destinations
        # (so all ancestors and descendants of selected nodes are also selected) but here we allow processing
        # only parts/paths of the graph. Selectors may select just one node ("node_1") or several
        # ("node_1,node_2,node_3"). Selectors may also contain wildcards ("node_*"), and these operations may
        # be composed ("node_*_cheeses,node_*_fruits").
        graph = self.select_subgraph(selector)
        active_destinations = [node[0] for node in graph.nodes(data=True)]
            
        if not self.skip_hashing:
            node_data = { node[0]: node[1]["data"] for node in self.graph.nodes(data=True) }
            # This tool maintains state about prior runs. If no inputs have changed, there's no need to re-run,
            # so for each run, we log hashes of
            # - config.yaml
            # - any CSV/TSV files from sources
            # - any template files from destinations
            # - any CSV/TSV files from map_values transformation operations
            # - any parameters passed via CLI
            # Only if any of these have changed since the last run do we actually re-process the DAG.
            # 
            # We also need to make sure to handle the selector...  data since a prior run may not have changed,
            # but the selector may be "wider" this time, necessitating a (re)run.
            has_remote_sources = False
            for name, source in self.sources.items():
                if name=="__line__": continue
                if "connection" in source.keys():
                    has_remote_sources = True
            
            runs_file = self.config.state_file
            self.profile("INFO: computing input hashes for run log at {0}".format(runs_file))
            self.logger.info("computing input hashes for run log at {0}".format(runs_file))
            hash_algorithm = "md5" # or "sha1"
            
            config_hash = self.get_string_hash(json.dumps(self.config), hash_algorithm)
            
            source_hashes = ""
            for name, source in self.sources.items():
                if "$sources."+name not in node_data.keys(): continue
                if name=="__line__": continue
                if "file" in source.keys():
                    source_hashes += self.get_file_hash(source["file"], hash_algorithm)
            if source_hashes!="": sources_hash = self.get_string_hash(source_hashes, hash_algorithm)
            else: sources_hash = ""
            
            template_hashes = ""
            for name, destination in self.destinations.items():
                if "$destinations."+name not in node_data.keys(): continue
                if name=="__line__": continue
                if "template" in destination.keys():
                    template_hashes += self.get_file_hash(destination["template"], hash_algorithm)
            if template_hashes!="": templates_hash = self.get_string_hash(template_hashes, hash_algorithm)
            else: templates_hash = ""

            mapping_hashes = ""
            for name, transformation in self.transformations.items():
                if name=="__line__": continue
                for op in transformation:
                    if "operation" in op.keys() and op["operation"]=="map_values" and "map_file" in op.keys():
                        mapping_hashes += self.get_file_hash(op["map_file"], hash_algorithm)
            if mapping_hashes!="": mappings_hash = self.get_string_hash(mapping_hashes, hash_algorithm)
            else: mappings_hash = ""

            if self.params!="": params_hash = self.get_string_hash(self.params, hash_algorithm)
            else: params_hash = ""

            if not os.path.isfile(runs_file):
                f = open(runs_file, "x")
                f.write("run_timestamp,config_hash,sources_hash,templates_hash,mappings_hash,params_hash,selector")
                f.close()
        else: self.profile("INFO: skipping hashing and run logging")
        
        if not self.force and not has_remote_sources:
            self.profile("INFO: checking for changes since last run")
            # Find the latest run that matched our selector(s)...
            with open(runs_file) as runs_handle:
                runs_reader = csv.reader(runs_handle, delimiter=',')
                num_lines = 0
                most_recent_run = False
                for row in runs_reader:
                    num_lines += 1
                    if row[1]==config_hash:
                        # (possibly) same project... see if this run’s destinations are a subset of row’s destinations
                        # Note that row[6] is either (a) "*", (b) a list like "dest1|dest2|dest3", or (c) missing.
                        if len(row)>6 and row[6]=="*":
                            most_recent_run = row
                        elif len(row)>6 and row[6]!="*":
                            run_destinations = row[6].split("|")
                            if set(active_destinations).issubset(set(run_destinations)):
                                most_recent_run = row
                        else: pass # older versions of the run log didn't log selected destinations
                    # find most-recent (farthest down) line where this run’s destinations are a subset of the line’s destinations
                if most_recent_run:
                    # row now contains the last (most recent) run
                    differences = []
                    if most_recent_run[2]!=sources_hash: differences.append("one or more sources")
                    if most_recent_run[3]!=templates_hash: differences.append("one or more destination templates")
                    if most_recent_run[4]!=mappings_hash: differences.append("one or more map_values transformations' map_file")
                    if most_recent_run[5]!=params_hash: differences.append("CLI parameter(s)")
                    if len(differences)==0:
                        self.profile("INFO: skipping (no changes since the last run {0} ago)".format(self.human_time(time.time() - float(most_recent_run[0]))))
                        self.do_generate = False
                    else:
                        self.profile("INFO: regenerating (changes since last run: ")
                        self.profile("      [{0}]".format(", ".join(differences)))
                else: self.profile("INFO: regenerating (no prior runs found, or config.yaml has changed since last run")
                
                if num_lines>10000:
                    self.profile(f"WARN: run log file {runs_file} is getting long, consider truncating it for better performance.", True)

        elif self.force:
            self.profile("INFO: forcing regenerate")
        
        elif has_remote_sources:
            self.profile("INFO: forcing regenerate, since some sources are remote (FTP/database)")
        
        # (Draw the graph)
        if self.config.show_graph: self.draw_graph(graph)

        if not self.do_generate: return

        # Process the graph
        for component in nx.weakly_connected_components(graph):
            subgraph = graph.subgraph(component)

            # Validate chunked sources
            num_chunked_sources = sum([1 if node[1]["data"].is_chunked and node[1]["data"].type=="source" else 0 for node in subgraph.nodes(data=True) ])
            if num_chunked_sources > 1:
                raise Exception("no dataflow graph component may contain multiple chunked (large) sources.")
            elif num_chunked_sources==0:
                # load all sources! (in topological sort order)
                start_nodes = [node for node in subgraph.nodes(data=True) if node[1]["data"].type=="source"]
                start_node_names = [node[0] for node in start_nodes]
                self.process(subgraph, start_nodes=start_node_names, exclude_nodes=[])
            else: # exactly 1 chunked source
                # load all but chunked source, stream chunked data down through tree emanating from it
                chunked_source = [node[0] for node in subgraph.nodes(data=True) if node[1]["data"].is_chunked and node[1]["data"].type=="source"]
                chunked_source_name = chunked_source[0]
                chunked_tree = nx.dfs_tree(self.graph, chunked_source_name)
                non_chunked_nodes = [node for node in subgraph.nodes(data=True) if node[0] not in chunked_tree.nodes]
                non_chunked_node_names = [node[0] for node in non_chunked_nodes]
                non_chunked_sources = [node[0] for node in non_chunked_nodes if node[1]["data"].type=="source"]
                
                # process non_chunked_sources, up to (but not including) chunked_tree
                self.process(subgraph, start_nodes=non_chunked_sources, exclude_nodes=chunked_tree)
                
                # set up reader for the chunked data source
                subgraph.nodes[chunked_source[0]]["data"].do()
                
                # clear destination files for an destinations in chunk tree (for append)
                chunked_destinations = [node for node in subgraph.nodes(data=True) if node[0] in chunked_tree.nodes and node[1]["data"].type=="destination"]
                for chunked_destination in chunked_destinations:
                    chunked_destination[1]["data"].wipe()
                    chunked_destination[1]["data"].mode = "a"

                # repeatedly process (in topological order) from chunked source, through the subtree, with each chunk
                chunked_source_node = subgraph.nodes[chunked_source[0]]["data"]
                next_chunk = False
                while not chunked_source_node.is_done:
                    self.profile("   source {0} chunk loaded ({1} rows)".format(chunked_source_node.file, len(chunked_source_node.data)))
                    self.profile_memory()
                    try:
                        next_chunk = chunked_source_node.reader.get_chunk()
                    except StopIteration:
                        chunked_source_node.is_done = True
                    # process chunked_source[1]["data"].data through tree
                    start_node_names = list(subgraph.successors(chunked_source[0]))
                    start_nodes = [node for node in subgraph.nodes(data=True) if node[0] in start_node_names]
                    for start_node in start_nodes:
                        start_node[1]["data"].is_done = False
                    self.process(subgraph, start_nodes=start_node_names, exclude_nodes=non_chunked_node_names, ignore_done=True)
                    if len(next_chunk)>0: chunked_source_node.data = self.pack_dataframe(next_chunk)

            # done with this component, clear out its data to reduce memory usage:
            for node in subgraph.nodes(data=True):
                node[1]["data"].clear()

        # Save run log only after a successful run! (in case of errors)
        if not self.skip_hashing:
            # save to run details to run log (which now certainly exists):
            f = open(runs_file, "a") # <-- append!
            if selector=="*": destinations = "*"
            else: destinations = "|".join(active_destinations)
            f.write("\n{0},{1},{2},{3},{4},{5},{6}".format(time.time(), config_hash, sources_hash, templates_hash, mappings_hash, params_hash, destinations))
            f.close()


        # (Draw the graph again, this time we can add metadata about rows/cols/size at each node)
        if self.config.show_graph: self.draw_graph(graph)