import math
import re

import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

from typing import Optional

from earthmover.refactor.error_handler import ErrorHandler


class Graph(nx.DiGraph):
    """

    """
    BOXSTYLE = 'round,pad=0.3'
    LABEL_OPTIONS = {"font_size": 12, "font_color": "whitesmoke"}
    SIZE_OPTIONS  = {"font_size": 8 , "font_color": "black"}


    def __init__(self, error_handler: Optional[ErrorHandler] = None, graph=None):
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

        self.error_handler = error_handler


    def get_node_data(self) -> dict:
        return {node[0]: node[1]["data"] for node in self.nodes(data=True)}


    def lookup_node(self, name):
        for node in self.nodes(data=True):
            if node[0] == name:
                return node[1]["data"]
        else:
            return False


    def ref(self, ref):
        """
        Destinations can reference either sources directly, or an intermediate transformation.
        This function determines which a reference refers to, and returns the appropriate target.

        :param ref:
        :return:
        """
        print(self.nodes[ref])
        return self.nodes[ref]["data"]


    def select_subgraph(self, selector) -> 'Graph':
        """

        :param selector:
        :return:
        """
        _graph = self

        if selector != '*':
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

        # Return as an Earthmover Graph to give better flexibility
        return Graph(graph=_graph, error_handler=self.error_handler)


    def draw(self, image_width=20, image_height=14):
        """

        :param image_width:
        :param image_height:
        :return:
        """
        _ = plt.figure(figsize=(image_width, image_height))

        # Pre-build lists of source, transformation, and destination nodes
        sources = []
        destinations = []
        transformations = []

        node_labels = {}
        node_sizes = {}
        for node in self.nodes(data=True):
            node_labels[node[0]] = node[1]["data"].name

            # Extract sizing data about the node (i.e., num rows, num cols, size on disk)
            # TODO: Make sizing-parsing function.

            # Classify node as source, transformation, or destination
            _type = node[1]["data"].type
            if _type == "source":
                sources.append(node[0])
            elif _type == "destination":
                destinations.append(node[0])
            else:
                transformations.append(node[0])

        # Position nodes using PyGraphViz (needs to be apt/pip installed separately):
        try:
            node_positions = nx.drawing.nx_agraph.graphviz_layout(self, prog='dot', args='-Grankdir=LR')
        except ImportError:
            self.error_handler.throw(
                "drawing the graph requires the PyGraphViz library... please install it with `sudo apt-get install graphviz graphviz-dev && pip install pygraphviz` or similar"
            )
            raise  # Never called; avoids linting errors

        # Calculate label positions: sources to left of node, destinations to right of node, transformations centered
        label_positions = {}
        size_positions = {}

        label_off = round(7 * math.sqrt(len(self.nodes)))  # offset on the x axis
        size_off = max(1, len(self.nodes) - 4)

        for k, v in node_positions.items():
            if k in sources:
                label_positions[k] = (v[0] - label_off, v[1])
            elif k in destinations:
                label_positions[k] = (v[0] + label_off, v[1])
            else:
                label_positions[k] = (v[0], v[1] + size_off + 1)

            if k in sources:
                size_positions[k] = (v[0] - label_off, v[1] - size_off)
            elif k in destinations:
                size_positions[k] = (v[0] + label_off, v[1] - size_off)
            else:
                size_positions[k] = (v[0] + size_off, v[1] + 1)

        # Draw sources:
        nx.draw_networkx_nodes(self, pos=node_positions, nodelist=sources, node_color="tab:green")
        nx.draw_networkx_labels(self, pos=label_positions, horizontalalignment="right", **self.LABEL_OPTIONS,
                                labels={k: v for k, v in node_labels.items() if k in sources},
                                bbox=dict(facecolor="tab:green", edgecolor="black", boxstyle=self.BOXSTYLE, zorder=-1.0))
        nx.draw_networkx_labels(self, pos=size_positions, horizontalalignment="right", **self.SIZE_OPTIONS)

        # Draw transformations:
        nx.draw_networkx_nodes(self, pos=node_positions, nodelist=transformations, node_color="tab:blue")
        nx.draw_networkx_labels(self, pos=label_positions, horizontalalignment="center", **self.LABEL_OPTIONS,
                                labels={k: v for k, v in node_labels.items() if k in transformations},
                                bbox=dict(facecolor="tab:blue", edgecolor="black", boxstyle=self.BOXSTYLE, zorder=-1.0))
        nx.draw_networkx_labels(self, pos=size_positions, horizontalalignment="left", **self.SIZE_OPTIONS)

        # Draw destinations:
        nx.draw_networkx_nodes(self, pos=node_positions, nodelist=destinations, node_color="tab:red")
        nx.draw_networkx_labels(self, pos=label_positions, horizontalalignment="left", **self.LABEL_OPTIONS,
                                labels={k: v for k, v in node_labels.items() if k in destinations},
                                bbox=dict(facecolor="tab:red", edgecolor="black", boxstyle=self.BOXSTYLE, zorder=-1.0))
        nx.draw_networkx_labels(self, pos=size_positions, horizontalalignment="left", **self.SIZE_OPTIONS)

        # Draw edges:
        nx.draw_networkx_edges(self, pos=node_positions, arrowsize=20)

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
