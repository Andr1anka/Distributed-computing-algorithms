from typing import List, Dict

from scheduler.abstract.abstract_network import AbstractNetwork
from scheduler.implementation.safra_node import SafraNode


class SafraNetwork(AbstractNetwork):

    NUMBER_OF_NODES = 8

    def __init__(self):

        self.nodes = []

        ids = [f"Node-{i}" for i in range(self.NUMBER_OF_NODES)]

        self.__get_edges(ids)

        for node_id in ids:
            self.nodes.append(
                SafraNode(node_id, self.edges[node_id])
            )

        for i in range(len(self.nodes)):
            self.nodes[i].next_node = self.nodes[(i + 1) % len(self.nodes)].node_id

        super().__init__(self.nodes)

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