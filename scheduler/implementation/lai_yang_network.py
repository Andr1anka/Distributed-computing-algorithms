from typing import List, Dict
from pprint import pprint

from scheduler.abstract.abstract_network import AbstractNetwork
from scheduler.implementation.lai_yang_node import LaiYangNode


class LaiYangNetwork(AbstractNetwork):
    NUMBER_OF_NODES = 5

    def __init__(self):
        self.nodes = []
        ids = [f"Node-{i}" for i in range(self.NUMBER_OF_NODES)]

        self.edges = self.__get_edges(ids)

        for node_id in ids:
            self.nodes.append(LaiYangNode(node_id, self.edges[node_id]))

        super().__init__(self.nodes)

    def __get_edges(self, ids: List[str]) -> Dict[str, List[str]]:
        edges = {
            ids[0]: [ids[1], ids[2]],
            ids[1]: [ids[0], ids[3]],
            ids[2]: [ids[0], ids[4]],
            ids[3]: [ids[1]],
            ids[4]: [ids[2]]
        }
        pprint(edges)
        return edges