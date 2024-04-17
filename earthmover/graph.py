import math
import re
import networkx as nx

from earthmover import util

from typing import Dict, Iterable, Optional
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from earthmover.error_handler import ErrorHandler
    from earthmover.nodes.node import Node


class Graph(nx.DiGraph):
    """

    """
    BOXSTYLE = 'round,pad=0.3'
    LABEL_OPTIONS = {"font_size": 12, "font_color": "whitesmoke"}
    SIZE_OPTIONS  = {"font_size": 8 , "font_color": "black"}

    def __init__(self, error_handler: Optional['ErrorHandler'] = None, graph: Optional['Graph'] = None):
        """
        Note: Defining `error_handler` as optional is a hack
        to allow networkx methods to still act on `Graph`.

        :param error_handler:
        :param graph:
        """
        # Logic to convert subgraphs into Earthmover `Graph`s.
        if graph:
            super().__init__(graph)
        else:
            super().__init__()  # Empty init for an empty graph

        self.error_handler: 'ErrorHandler' = error_handler


    def get_node_data(self) -> Dict[str, 'Node']:
        return {node[0]: node[1]["data"] for node in self.nodes(data=True)}


    def get_terminal_nodes(self) -> Iterable['Node']:
        """
        Helper function to remove terminal source and transformation nodes during Earthmover.build_graph().
        :return:
        """
        return [node for node, degree in self.out_degree() if degree == 0]


    def ref(self, ref) -> 'Node':
        """
        Destinations can reference either sources directly, or an intermediate transformation.
        This function determines which a reference refers to, and returns the appropriate target.

        :param ref:
        :return:
        """
        node = self.nodes.get(ref)
        if not node:
            raise KeyError(
                f"Node not found in the graph: {ref}"
            )
        return node['data']


    def select_subgraph(self, selector: str) -> 'Graph':
        """

        :param selector:
        :return:
        """
        if selector == '*':
            return self

        else:
            if "," in selector:
                selectors = selector.split(",")
            else:
                selectors = [selector]

            all_nodes = self.nodes
            all_selected_nodes = []

            for selector in selectors:
                selected_nodes = []
                pattern = re.compile(selector.replace("*",".*"))

                for node in all_nodes:
                    if pattern.search(node):
                        selected_nodes.append(node)

                ancestor_nodes = []
                for node in selected_nodes:
                    ancestor_nodes += list(nx.ancestors(self, node))

                descendant_nodes = []
                for node in selected_nodes:
                    descendant_nodes += list(nx.descendants(self, node))

                selected_nodes += descendant_nodes + ancestor_nodes
                all_selected_nodes += selected_nodes

            _graph = nx.subgraph(self, all_selected_nodes)
            return Graph(graph=_graph, error_handler=self.error_handler)


    def draw(self, image_width: int = 20, image_height: int = 14):
        """

        :param image_width:
        :param image_height:
        :return:
        """
        try:
            import matplotlib.patches as mpatches
            import matplotlib.pyplot as plt
            import pygraphviz
            _ = plt.figure(figsize=(image_width, image_height))
        except ImportError:
            self.error_handler.ctx.remove('node', 'line', 'file')
            self.error_handler.throw(
                "drawing the graph requires additional libraries... please install using `pip install earthmover[graph]`"
            )
            raise  # Never called; avoids linting errors

        # Pre-build lists of source, transformation, and destination nodes
        sources = []
        destinations = []
        transformations = []

        node_labels = {}
        node_sizes = {}
        for node_id, node in self.get_node_data().items():

            node_labels[node_id] = node.name

            # Default row and column counts to 0 during compile.
            _node_size_label = f"{int(node.num_rows or 0)} rows; {node.num_cols or 0} cols"
            if node.size:
                _node_size_label += f"; {util.human_size(node.size)}"

            node_sizes[node_id] =  _node_size_label

            # Classify node as source, transformation, or destination
            _type = node.type
            if _type == "source":
                sources.append(node_id)
            elif _type == "destination":
                destinations.append(node_id)
            else:
                transformations.append(node_id)

        # Position nodes using PyGraphViz (needs to be apt/pip installed separately):
        try:
            node_positions = nx.drawing.nx_agraph.graphviz_layout(self, prog='dot', args='-Grankdir=LR')
        except ValueError:
            self.error_handler.ctx.remove('node', 'line', 'file')
            self.error_handler.throw(
                "drawing the graph requires the GraphViz package... please install it with `sudo apt-get install graphviz graphviz-dev` or similar"
            )
            raise  # Never called; avoids linting errors

        # Calculate label positions: sources to left of node, destinations to right of node, transformations centered
        label_positions = {}
        size_positions = {}

        label_off = round(7 * math.sqrt(len(self.nodes)))  # offset on the x axis
        size_off = max(1, len(self.nodes) - 4)

        for key, val in node_positions.items():
            if key in sources:
                label_positions[key] = (val[0] - label_off, val[1])
            elif key in destinations:
                label_positions[key] = (val[0] + label_off, val[1])
            else:
                label_positions[key] = (val[0], val[1] + size_off + 1)

            if key in sources:
                size_positions[key] = (val[0] - label_off, val[1] - size_off)
            elif key in destinations:
                size_positions[key] = (val[0] + label_off, val[1] - size_off)
            else:
                size_positions[key] = (val[0] + size_off, val[1] + 1)

        # Draw sources:
        nx.draw_networkx_nodes(self, pos=node_positions, nodelist=sources, node_color="tab:green")
        nx.draw_networkx_labels(self,
            pos=label_positions, horizontalalignment="right", **self.LABEL_OPTIONS,
            labels={k: v for k, v in node_labels.items() if k in sources},
            bbox=dict(facecolor="tab:green", edgecolor="black", boxstyle=self.BOXSTYLE, zorder=-1.0)
        )
        nx.draw_networkx_labels(self,
            pos=size_positions, horizontalalignment="right", **self.SIZE_OPTIONS,
            labels={k: v for k, v in node_sizes.items() if k in sources}
        )

        # Draw transformations:
        nx.draw_networkx_nodes(self, pos=node_positions, nodelist=transformations, node_color="tab:blue")
        nx.draw_networkx_labels(self,
            pos=label_positions, horizontalalignment="center", **self.LABEL_OPTIONS,
            labels={k: v for k, v in node_labels.items() if k in transformations},
            bbox=dict(facecolor="tab:blue", edgecolor="black", boxstyle=self.BOXSTYLE, zorder=-1.0)
        )
        nx.draw_networkx_labels(self,
            pos=size_positions, horizontalalignment="left", **self.SIZE_OPTIONS,
            labels={k: v for k, v in node_sizes.items() if k in transformations}
        )

        # Draw destinations:
        nx.draw_networkx_nodes(self, pos=node_positions, nodelist=destinations, node_color="tab:red")
        nx.draw_networkx_labels(self,
            pos=label_positions, horizontalalignment="left", **self.LABEL_OPTIONS,
            labels={k: v for k, v in node_labels.items() if k in destinations},
            bbox=dict(facecolor="tab:red", edgecolor="black", boxstyle=self.BOXSTYLE, zorder=-1.0)
        )
        nx.draw_networkx_labels(self,
            pos=size_positions, horizontalalignment="left", **self.SIZE_OPTIONS,
            labels={k: v for k, v in node_sizes.items() if k in destinations}
        )

        # Draw edges:
        nx.draw_networkx_edges(self, pos=node_positions, arrowsize=20)

        # Add legend:
        legend = [
            mpatches.Patch(color='tab:green', label='sources'),
            mpatches.Patch(color='tab:blue', label='transformations'),
            mpatches.Patch(color='tab:red', label='destinations')
        ]
        plt.legend(handles=legend, loc='lower center', ncol=3)

        # If the graph is just a single line, the y-range is tiny and so the size labels don't appear.
        # This is a silly hack to artificially increase the y-range if it's small, so the labels show up.
        plt.margins(0.3)
        axes = plt.gca()
        y_min, y_max = axes.get_ylim()
        plt.ylim([min(0, y_min), max(40, y_max)])
        
        # Save graph image
        plt.savefig("graph.svg")
        plt.savefig("graph.png")
        plt.clf()
        plt.close()
