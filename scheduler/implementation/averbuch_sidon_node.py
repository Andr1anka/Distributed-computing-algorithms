import uuid
from typing import List, Dict, Any

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class Colors:
    RESET = "\033[0m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"


class AverbuchSidonNode(AbstractNode):
    def __init__(self, node_id: str, neighbors: List[str]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = sorted(neighbors)          # для детермінованості

        # Змінні алгоритму Авербуха
        self.father = None
        self.is_root: bool = False
        self.unvisited = set(self.neighbors)
        self.flags = {nb: 0 for nb in self.neighbors}
        self.finished: bool = False

        # Годинники
        self.lamport_clock: int = 0
        self.vector_clock = {n: 0 for n in self.neighbors}
        self.vector_clock[self.node_id] = 0

        # Для сумісності з Observer
        self.visited: int = 0
        self.data: int = 0


    def log(self, text: str):
        color = Colors.RESET
        if "RECEIVED" in text:
            color = Colors.BLUE
        elif "SEND" in text:
            color = Colors.GREEN
        elif "FINISHED" in text:
            color = Colors.RED
        elif "Handling DISCOVER" in text or "STARTED" in text:
            color = Colors.MAGENTA
        elif "RETURN" in text:
            color = Colors.YELLOW

        print(
            f"{color}[{self.node_id} | "
            f"L={self.lamport_clock} | "
            f"V={self.vector_clock}] "
            f"{text}{Colors.RESET}"
        )

    def create_message(self, message_type: str) -> Dict[str, Any]:
        self.lamport_clock += 1
        self.vector_clock[self.node_id] += 1
        return {
            'sender_id': self.node_id,
            'message_type': message_type,
            'clock': self.lamport_clock,
            'vector_clock': self.vector_clock.copy()
        }

    def handle_discover(self, sender: str) -> List[tuple[str, Dict]]:
        if self.finished:
            return []

        if self.father is None:
            self.father = sender
            self.is_root = (sender == self.node_id)

        self.log(f"Handling DISCOVER from {sender}")

        outgoing: List[tuple[str, Dict]] = []

        # Відправляємо VISITED всім сусідам крім батька
        for q in [nb for nb in self.neighbors if nb != self.father]:
            if self.flags.get(q, 0) == 0:
                self.flags[q] = 1
                msg = self.create_message('VISITED')
                outgoing.append((q, msg))

        # Просуваємося далі
        if self.unvisited:
            k = min(self.unvisited)
            msg = self.create_message('DISCOVER')
            outgoing.append((k, msg))
            self.unvisited.remove(k)
        elif not self.is_root:
            msg = self.create_message('RETURN')
            outgoing.append((self.father, msg))
        else:
            self.log("FINISHED ALGORITHM (root)")
            self.finished = True
            self.unvisited.clear()
            for q in self.flags:
                self.flags[q] = 0

        return outgoing

    def handle_return(self, sender: str) -> List[tuple[str, Dict]]:
        if self.finished:
            return []

        outgoing: List[tuple[str, Dict]] = []

        if self.unvisited:
            k = min(self.unvisited)
            msg = self.create_message('DISCOVER')
            outgoing.append((k, msg))
            self.unvisited.remove(k)
        else:
            if self.father is not None and self.father != self.node_id:
                msg = self.create_message('RETURN')
                outgoing.append((self.father, msg))
            else:
                self.log("FINISHED ALGORITHM")
                self.finished = True
                self.unvisited.clear()
                for q in self.flags:
                    self.flags[q] = 0

        return outgoing

    def process_action(self, message: Action) -> NodeResponse:
        if self.finished:
            self.log("Node already finished - ignoring message")
            return NodeResponse([])

        # Оновлення годинників
        incoming_clock = message.data.get("clock", 0)
        self.lamport_clock = max(self.lamport_clock, incoming_clock) + 1

        incoming_vector = message.data.get("vector_clock", {})
        for node, val in incoming_vector.items():
            self.vector_clock[node] = max(self.vector_clock.get(node, 0), val)
        self.vector_clock[self.node_id] += 1

        self.visited += 1

        msg_type = message.data.get('message_type', 'UNKNOWN')
        sender = message.data.get('sender_id', 'self')
        self.log(f"RECEIVED ← {sender} | {msg_type}")

        message_type = message.data.get('message_type')
        sender_id = message.data.get('sender_id')

        if message_type == 'DISCOVER':
            outgoing = self.handle_discover(sender_id)
        elif message_type == 'RETURN':
            outgoing = self.handle_return(sender_id)
        elif message_type == 'VISITED':
            if sender_id in self.unvisited:
                self.unvisited.remove(sender_id)
            msg = self.create_message('ACK')
            outgoing = [(sender_id, msg)]
        elif message_type == 'ACK':
            self.flags[sender_id] = 0
            if all(f == 0 for f in self.flags.values()):
                outgoing = self.handle_return(sender_id)
            else:
                outgoing = []
        else:
            outgoing = []

        # === Формування дій для відправки ===
        actions = []
        for receiver, msg_data in outgoing:
            self.log(f"SEND → {receiver} | {msg_data.get('message_type')}")
            action = Action(msg_data, receiver, uuid.uuid4())
            actions.append(action)

        return NodeResponse(actions)