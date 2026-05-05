import uuid
from typing import List, Dict, Any

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class AverbuchSidenNode(AbstractNode):

    def __init__(self, node_id: uuid.UUID, neighbors: List[uuid.UUID]):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        # змінні з методички
        self.father = None
        self.unvisited = set(neighbors)
        self.flags = {n: 0 for n in neighbors}

        self.visited = 0
        self.started = False

    def process_action(self, message: Action) -> NodeResponse:
        self.visited += 1
        msg = message.data

        if msg["message_type"] == "DISCOVER":
            outputs = self.on_discover(msg["sender_id"])
        elif msg["message_type"] == "RETURN":
            outputs = self.on_return()
        elif msg["message_type"] == "VISITED":
            outputs = [self.on_visited(msg["sender_id"])]
        elif msg["message_type"] == "ACK":
            outputs = self.on_ack(msg["sender_id"])
        elif msg["message_type"] == "New":
            return self.start(msg)
        else:
            return NodeResponse([])

        if not outputs:
            return NodeResponse([])

        actions = []
        for out in outputs:
            if out:
                receiver = list(out.keys())[0]
                actions.append(
                    Action(out[receiver], receiver, uuid.uuid4())
                )

        return NodeResponse(actions)

    # -------------------------
    # START
    # -------------------------
    def start(self, msg):
        self.started = True
        self.father = self.node_id

        return NodeResponse([
            Action({
                "message_type": "DISCOVER",
                "sender_id": self.node_id
            }, self.node_id, uuid.uuid4())
        ])

    # -------------------------
    # DISCOVER
    # -------------------------
    def on_discover(self, sender):
        self.father = sender

        outputs = []

        for n in self.neighbors:
            if n != sender:
                self.flags[n] = 1
                outputs.append({
                    n: {
                        "message_type": "VISITED",
                        "sender_id": self.node_id
                    }
                })

        if not outputs:
            return [{
                sender: {
                    "message_type": "RETURN",
                    "sender_id": self.node_id
                }
            }]

        return outputs


    # -------------------------
    # VISITED
    # -------------------------
    def on_visited(self, sender):
        if sender in self.unvisited:
            self.unvisited.remove(sender)

        return {
            sender: {
                "message_type": "ACK",
                "sender_id": self.node_id
            }
        }

    # -------------------------
    # ACK
    # -------------------------
    def on_ack(self, sender):
        self.flags[sender] = 0

        if all(v == 0 for v in self.flags.values()):
            return self.on_return()

        return []

    # -------------------------
    # RETURN
    # -------------------------
    def on_return(self):
        if self.unvisited:
            k = self.unvisited.pop()
            return [{
                k: {
                    "message_type": "DISCOVER",
                    "sender_id": self.node_id
                }
            }]

        if self.father != self.node_id:
            return [{
                self.father: {
                    "message_type": "RETURN",
                    "sender_id": self.node_id
                }
            }]

        print(f"Node {self.node_id} FINISHED DFS")
        return []