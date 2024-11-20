import json
import typing
from typing import Generic, List, Set, Tuple, TypeVar

import networkx as nx # type: ignore[import-untyped]

T = TypeVar("T")


class ExecutionGraph(Generic[T]):

    def __init__(self, start_node: T, end_node: T, edges: Set[Tuple[T, T]]):
        self._graph = nx.DiGraph()
        self._graph.add_edges_from(edges)
        self._graph.add_node(start_node)
        self._graph.add_node(end_node)
        self._end_node = end_node
        self._start_node = start_node
        if not nx.is_directed_acyclic_graph(self._graph):
            raise Exception("Graph not directed acyclic")
        nodes = set(self._graph)
        descendants_plus_start_node = nx.descendants(self._graph, self._start_node) | {
            self._start_node
        }
        if not descendants_plus_start_node == nodes:
            raise Exception("Not all Nodes are reachable from start node")
        ancestors_plus_end_node = nx.ancestors(self._graph, self._end_node) | {
            self._end_node
        }
        if not ancestors_plus_end_node == nodes:
            raise Exception("End node not reachable by all nodes")

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            result = (
                self._start_node == other._start_node
                and self._end_node == other._end_node
                and self._graph.edges == other._graph.edges
            )
            return result
        else:
            return False

    def __repr__(self) -> str:
        sorted_edges = list(sorted([str(edge) for edge in self.edges()]))
        result = {
            "start_node": str(self._start_node),
            "end_node": str(self._end_node),
            "edges": sorted_edges,
        }
        # return f"ExecutionGraph(start_node={self._start_node},end_node={self._end_node},edges={sorted_edges})"
        return json.dumps(result, indent=2)

    @property
    def start_node(self) -> T:
        return self._start_node

    @property
    def end_node(self) -> T:
        return self._end_node

    def predecessors(self, node: T) -> List[T]:
        if not node in self._graph:
            raise Exception(f"The node {node} is not in the graph.")
        return list(self._graph.predecessors(node))

    def successors(self, node: T) -> List[T]:
        if not node in self._graph:
            raise Exception(f"The node {node} is not in the graph.")
        return list(self._graph.successors(node))

    def nodes(self) -> Set[T]:
        return set(self._graph)

    def edges(self) -> Set[Tuple[T, T]]:
        return set(self._graph.edges)

    def compute_reverse_dependency_order(self) -> List[T]:
        reversed_graph = self._graph.reverse()
        post_order_of_reversed_graph = list(
            nx.traversal.dfs_postorder_nodes(reversed_graph, self._end_node)
        )
        reversed_post_order_of_reversed_graph = list(
            reversed(post_order_of_reversed_graph)
        )
        return reversed_post_order_of_reversed_graph

    def compute_dependency_order(self) -> List[T]:
        post_order_of__graph = list(
            nx.traversal.dfs_postorder_nodes(self._graph, self._start_node)
        )
        reversed_post_order_of_graph = list(reversed(post_order_of__graph))
        return reversed_post_order_of_graph
