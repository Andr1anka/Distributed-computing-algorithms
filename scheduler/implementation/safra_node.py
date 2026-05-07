from typing import List, Dict, Any

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class SafraNode(AbstractNode):

    def __init__(self, node_id: str, neighbors: List[str]):

        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        self.active = False
        self.color = "WHITE"

        # Safra counter = number of outstanding messages
        self.message_counter = 0

        self.next_node = None
        self.is_initiator = False

    def process_action(self, message: Action) -> NodeResponse:

        data = message.data
        msg_type = data.get("message_type")

        print(f"[{self.node_id}] RECEIVED {msg_type}")

        if msg_type == "New":
            return self.start(data)

        if msg_type == "WORK":
            return self.receive_work(data)

        if msg_type == "TOKEN":
            return self.receive_token(data)

        return NodeResponse([])

    # ---------------------------
    # INIT
    # ---------------------------
    def start(self, data: Dict[str, Any]) -> NodeResponse:

        self.is_initiator = True
        self.active = True

        actions = []

        print(f"[{self.node_id}] STARTED SAFRA")

        # send WORK
        for neighbor in self.neighbors:

            msg = {
                "message_type": "WORK",
                "sender_id": self.node_id
            }

            self.message_counter += 1   # ✔ outbound message count

            actions.append(
                Action(msg, neighbor, data["transaction_id"])
            )

        self.active = False  # після ініціації стає passive

        # start TOKEN
        token = {
            "message_type": "TOKEN",
            "color": "WHITE",
            "counter": 0
        }

        actions.append(
            Action(token, self.next_node, "token")
        )

        return NodeResponse(actions)

    # ---------------------------
    # WORK
    # ---------------------------
    def receive_work(self, data: Dict[str, Any]):

        self.active = True

        self.message_counter -= 1   # ✔ received work decreases outstanding

        # Safra rule: ANY new activity → BLACK
        self.color = "BLACK"

        print(
            f"[{self.node_id}] WORK "
            f"COUNTER={self.message_counter}"
        )

        self.active = False  # finish local execution

        return NodeResponse([])

    # ---------------------------
    # TOKEN
    # ---------------------------
    def receive_token(self, data: Dict[str, Any]):

        token_color = data["color"]
        token_counter = data["counter"]

        print(
            f"[{self.node_id}] TOKEN "
            f"color={token_color} "
            f"counter={token_counter} "
            f"local={self.message_counter}"
        )

        # accumulate local outstanding work
        token_counter += self.message_counter

        # if any node is BLACK → token becomes BLACK
        if self.color == "BLACK":
            token_color = "BLACK"

        # reset node color after participation
        self.color = "WHITE"

        # -------------------
        # INITIATOR CHECK
        # -------------------
        if self.is_initiator:

            if token_color == "WHITE" and token_counter == 0:

                print(f"\n[{self.node_id}] GLOBAL TERMINATION DETECTED (SAFRA)\n")
                return NodeResponse([])

            # restart round
            token_color = "WHITE"
            token_counter = 0

        # pass token forward
        token = {
            "message_type": "TOKEN",
            "color": token_color,
            "counter": token_counter
        }

        return NodeResponse([
            Action(token, self.next_node, "token")
        ])