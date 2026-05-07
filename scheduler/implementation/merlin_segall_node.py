import uuid
from typing import List, Dict, Any

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class Colors:
    RESET = "\033[0m"
    GREEN = "\033[92m"
    BLUE = "\033[94m"
    YELLOW = "\033[93m"
    RED = "\033[91m"


class MerlinSegallNode(AbstractNode):

    def __init__(self, node_id: str, neighbors: List[str]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        # Lamport clock
        self.clock = 0

        # Merlin-Segall state
        self.requesting_cs = False
        self.in_cs = False

        self.request_timestamp = None

        self.pending_replies = set()

        self.deferred_requests = []

    def log(self, text, color=Colors.RESET):
        print(
            f"{color}[{self.node_id} | clock={self.clock}] {text}{Colors.RESET}"
        )

    def increment_clock(self, received_clock=None):
        if received_clock is not None:
            self.clock = max(self.clock, received_clock)

        self.clock += 1

    def process_action(self, message: Action) -> NodeResponse:

        received_clock = message.data.get("clock", 0)
        self.increment_clock(received_clock)

        msg_type = message.data.get("message_type")

        self.log(f"RECEIVED {msg_type} from {message.data.get('sender_id')}", Colors.BLUE)

        actions = []

        if msg_type == "New":
            actions.extend(self.start_request())

        elif msg_type == "REQUEST":
            response = self.handle_request(message.data)

            if response:
                actions.append(response)

        elif msg_type == "ACK":
            actions.extend(self.handle_ack(message.data))

        elif msg_type == "RELEASE":
            response = self.handle_release(message.data)

            if response:
                actions.append(response)

        return NodeResponse(actions)

    # =========================================================
    # START REQUEST
    # =========================================================

    def start_request(self):

        self.requesting_cs = True

        self.increment_clock()

        self.request_timestamp = self.clock

        self.pending_replies = set(self.neighbors)

        self.log("REQUESTING CRITICAL SECTION", Colors.YELLOW)

        actions = []

        for neighbor in self.neighbors:

            request_message = {
                "message_type": "REQUEST",
                "sender_id": self.node_id,
                "timestamp": self.request_timestamp,
                "clock": self.clock
            }

            actions.append(
                Action(
                    request_message,
                    neighbor,
                    uuid.uuid4()
                )
            )

        return actions

    # =========================================================
    # HANDLE REQUEST
    # =========================================================

    def handle_request(self, data: Dict[Any, Any]):

        sender = data["sender_id"]
        sender_timestamp = data["timestamp"]

        should_defer = False

        if self.in_cs:
            should_defer = True

        elif self.requesting_cs:

            my_priority = (self.request_timestamp, self.node_id)
            sender_priority = (sender_timestamp, sender)

            if my_priority < sender_priority:
                should_defer = True

        if should_defer:

            self.log(f"DEFERRED reply to {sender}", Colors.YELLOW)

            self.deferred_requests.append(sender)

            return None

        self.increment_clock()

        ack_message = {
            "message_type": "ACK",
            "sender_id": self.node_id,
            "clock": self.clock
        }

        self.log(f"SEND ACK to {sender}", Colors.GREEN)

        return Action(
            ack_message,
            sender,
            uuid.uuid4()
        )

    # =========================================================
    # HANDLE ACK
    # =========================================================

    def handle_ack(self, data):

        sender = data["sender_id"]

        if sender in self.pending_replies:
            self.pending_replies.remove(sender)

        self.log(f"ACK from {sender}", Colors.GREEN)

        if len(self.pending_replies) == 0:
            return self.enter_critical_section()

        return []

    # =========================================================
    # ENTER CS
    # =========================================================

    def enter_critical_section(self):

        self.in_cs = True

        self.log("ENTERED CRITICAL SECTION", Colors.RED)

        # Симуляція завершення роботи

        self.in_cs = False
        self.requesting_cs = False

        self.log("EXITED CRITICAL SECTION", Colors.RED)

        actions = []

        for node in self.deferred_requests:

            self.increment_clock()

            release_message = {
                "message_type": "RELEASE",
                "sender_id": self.node_id,
                "clock": self.clock
            }

            actions.append(
                Action(
                    release_message,
                    node,
                    uuid.uuid4()
                )
            )

        self.deferred_requests.clear()

        return actions

    # =========================================================
    # HANDLE RELEASE
    # =========================================================

    def handle_release(self, data):

        sender = data["sender_id"]

        self.increment_clock()

        ack_message = {
            "message_type": "ACK",
            "sender_id": self.node_id,
            "clock": self.clock
        }

        self.log(f"RELEASE from {sender} -> ACK", Colors.GREEN)

        return Action(
            ack_message,
            sender,
            uuid.uuid4()
        )