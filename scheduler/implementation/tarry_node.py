import random
from typing import List, Any, Dict

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

class TarryNode(AbstractNode):

    def __init__(self, node_id: str, neighbors: List[str]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        # Дані алгоритму Террі
        self.data = 0
        self.transactions = []
        self.parent = None
        self.visited_first_time = False
        self.visited = 0

        # Лемпорт
        self.lamport_clock = 0

        # Векторний годинник
        self.vector_clock = {neighbor: 0 for neighbor in neighbors}
        self.vector_clock[self.node_id] = 0

    # ЛОГ
    def log(self, text, event_type="INFO"):
        color = Colors.RESET

        if "RECEIVED" in text:
            color = Colors.BLUE
        elif "SEND" in text:
            color = Colors.GREEN
        elif "RETURN" in text:
            color = Colors.YELLOW
        elif "FINISHED" in text:
            color = Colors.RED
        elif "STARTED" in text:
            color = Colors.MAGENTA

        print(
            f"{color}[{self.node_id} | "
            f"L={self.lamport_clock} | "
            f"V={self.vector_clock}] "
            f"{text}{Colors.RESET}"
        )
    def process_action(self, message: Action) -> NodeResponse:
        # Лемпорт (отримання)
        incoming_clock = message.data.get("clock", 0)
        self.lamport_clock = max(self.lamport_clock, incoming_clock) + 1

        # Векторний (отримання)
        incoming_vector = message.data.get("vector_clock", {})
        for node in incoming_vector:
            self.vector_clock[node] = max(
                self.vector_clock.get(node, 0),
                incoming_vector[node]
            )
        self.vector_clock[self.node_id] += 1

        self.log(f"RECEIVED ← {message.data.get('sender_id')} | data={message.data}")
        self.visited += 1
        new_message = self.process_message(message)

        if new_message is None:
            return NodeResponse([])

        receiver = list(new_message.keys())[0]

        self.log(f"SEND → {receiver}")

        return NodeResponse([Action(new_message[receiver], receiver, '11111')])

    def process_message(self, message: Action):
        if message.data.get('message_type') == 'New':
            return self.start_wave(message.data)
        elif message.data.get('message_type') == 'Offer':
            return self.receive_offer(message.data)
        return {}

    def start_wave(self, message: dict[str, Any]) -> Dict[str, Any]:
        transaction_data = message.get("transaction_data")
        receiver = random.choice(self.neighbors)

        self.data = transaction_data
        self.transactions.append(receiver)
        self.visited_first_time = True

        # перед відправкою
        self.lamport_clock += 1
        self.vector_clock[self.node_id] += 1

        offer = {
            'sender_id': self.node_id,
            'transaction_data': transaction_data,
            'message_type': "Offer",
            'clock': self.lamport_clock,
            'vector_clock': self.vector_clock.copy()
        }

        self.log("STARTED ALGORITHM")

        return {receiver: offer}

    def receive_offer(self, message: Dict[Any, Any]) -> Dict[str, Any]:
        if not self.visited_first_time:
            self.visited_first_time = True
            self.parent = message.get("sender_id")
            self.data = message.get("transaction_data")

        # перед відправкою
        self.lamport_clock += 1
        self.vector_clock[self.node_id] += 1

        offer = {
            'sender_id': self.node_id,
            'transaction_data': message.get("transaction_data"),
            'message_type': "Offer",
            'clock': self.lamport_clock,
            'vector_clock': self.vector_clock.copy()
        }

        receiver = None

        for neighbor in self.neighbors:
            if neighbor != self.parent and neighbor not in self.transactions:
                receiver = neighbor
                self.transactions.append(receiver)
                break

        if receiver is None and self.parent:
            receiver = self.parent
            self.transactions.append(receiver)
            self.log("RETURN TO PARENT")

        if receiver is None:
            self.log("FINISHED ALGORITHM")
            return None

        return {receiver: offer}