from typing import List, Dict, Any

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class RanaNode(AbstractNode):

    def __init__(self, node_id: str, neighbors: List[str]):

        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        self.transactions = {}

    def process_action(self, message: Action) -> NodeResponse:

        data = message.data

        msg_type = data.get("message_type")

        print(f"[{self.node_id}] RECEIVED {msg_type} FROM {data.get('sender_id')}")

        if msg_type == "New":
            return self.start_algorithm(data)

        elif msg_type == "WORK":
            return self.receive_work(data)

        elif msg_type == "ACK":
            return self.receive_ack(data)

        return NodeResponse([])

    def init_transaction(self, transaction_id):

        if transaction_id not in self.transactions:

            self.transactions[transaction_id] = {
                "parent": None,
                "children": set(),
                "completed": set(),
                "active": False,
                "initiator": False
            }

    def start_algorithm(self, data):

        transaction_id = data["transaction_id"]

        self.init_transaction(transaction_id)

        tx = self.transactions[transaction_id]

        tx["initiator"] = True
        tx["active"] = True

        actions = []

        print(f"[{self.node_id}] STARTED RANA")

        for neighbor in self.neighbors:

            tx["children"].add(neighbor)

            msg = {
                "message_type": "WORK",
                "sender_id": self.node_id,
                "transaction_id": transaction_id
            }

            actions.append(
                Action(msg, neighbor, transaction_id)
            )

        return NodeResponse(actions)

    def receive_work(self, data):

        transaction_id = data["transaction_id"]

        self.init_transaction(transaction_id)

        tx = self.transactions[transaction_id]

        sender = data["sender_id"]

        first_visit = not tx["active"]

        if first_visit:
            tx["parent"] = sender
            tx["active"] = True

        actions = []

        forwarded = False

        for neighbor in self.neighbors:

            if neighbor != sender:

                forwarded = True

                tx["children"].add(neighbor)

                msg = {
                    "message_type": "WORK",
                    "sender_id": self.node_id,
                    "transaction_id": transaction_id
                }

                actions.append(
                    Action(msg, neighbor, transaction_id)
                )

        if not forwarded:

            ack = {
                "message_type": "ACK",
                "sender_id": self.node_id,
                "transaction_id": transaction_id
            }

            actions.append(
                Action(ack, sender, transaction_id)
            )

        return NodeResponse(actions)

    def receive_ack(self, data):

        transaction_id = data["transaction_id"]

        tx = self.transactions[transaction_id]

        sender = data["sender_id"]

        tx["completed"].add(sender)

        if tx["completed"] == tx["children"]:

            tx["active"] = False

            if tx["initiator"]:

                print(
                    f"\n[{self.node_id}] GLOBAL TERMINATION DETECTED "
                    f"FOR {transaction_id}\n"
                )

                return NodeResponse([])

            parent = tx["parent"]

            if parent:

                ack = {
                    "message_type": "ACK",
                    "sender_id": self.node_id,
                    "transaction_id": transaction_id
                }

                return NodeResponse([
                    Action(ack, parent, transaction_id)
                ])

        return NodeResponse([])