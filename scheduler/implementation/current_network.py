from typing import List, Dict

from scheduler.abstract.abstract_network import AbstractNetwork

# ІМПОРТИ АЛГОРИТМІВ
from scheduler.implementation.wave_node import WaveNode
from scheduler.implementation.echo_node import EchoNode
from scheduler.implementation.tree_node import TreeNode


class CurrentNetwork(AbstractNetwork):
    NUMBER_OF_NODES = 8

    def __init__(self, algorithm: str = "wave") -> None:
        self.nodes = []

        ids = [f"Node-{i}" for i in range(self.NUMBER_OF_NODES)]
        self.__get_edges(ids)

        #  ВИБІР АЛГОРИТМУ
        NodeClass = self.__get_node_class(algorithm)

        for node_id in ids:
            self.nodes.append(NodeClass(node_id, self.edges[node_id]))

        super().__init__(self.nodes)

    def __get_node_class(self, algorithm: str):
        if algorithm == "wave":
            return WaveNode
        elif algorithm == "echo":
            return EchoNode
        elif algorithm == "tree":
            return TreeNode
        else:
            raise ValueError(f"Unknown algorithm: {algorithm}")

    def __get_edges(self, ids: List[str]) -> Dict[str, List[str]]:
        self.edges = {
            ids[0]: [ids[1], ids[2]],
            ids[1]: [ids[0], ids[3], ids[4]],
            ids[2]: [ids[0], ids[5], ids[6], ids[7]],
            ids[3]: [ids[1]],
            ids[4]: [ids[1]],
            ids[5]: [ids[2]],
            ids[6]: [ids[2]],
            ids[7]: [ids[2]]
        }
        return self.edges