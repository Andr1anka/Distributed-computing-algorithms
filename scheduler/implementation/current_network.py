import uuid
from typing import List, Dict

from scheduler.abstract.abstract_network import AbstractNetwork
from scheduler.implementation.averbuch_sidon_node import AverbuchSidonNode
from scheduler.core.action import Action


class CurrentNetwork(AbstractNetwork):
    NUMBER_OF_NODES = 8

    def __init__(self) -> None:
        self.nodes = []
        ids = [f"Node-{i}" for i in range(self.NUMBER_OF_NODES)]
        self.__get_edges(ids)

        for node_id in ids:
            self.nodes.append(AverbuchSidonNode(node_id, self.edges[node_id]))

        super().__init__(self.nodes)

        # одноразовий алгоритм авербиха
        if self.nodes:
            start_node = self.nodes[0]                    # Node-0 буде коренем
            start_node.father = start_node.node_id
            start_node.is_root = True

            # Створюємо початкове повідомлення DISCOVER для кореня
            discover_msg = {
                'sender_id': start_node.node_id,
                'message_type': 'DISCOVER',
                'clock': 0,
                'vector_clock': start_node.vector_clock.copy()
            }

            action = Action(
                data=discover_msg,
                node_id=start_node.node_id,
                action_id=uuid.uuid4()
            )

            start_node.mailbox.add_inbox_action(action)

            print(f"Алгоритм Авербуха запущено з кореня: {start_node.node_id}")

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
        print("Network edges:", self.edges)
        return self.edges