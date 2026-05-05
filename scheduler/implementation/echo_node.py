from typing import List, Set

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse
from scheduler.core.action import Action

class EchoNode(AbstractNode):

    def __init__(self, node_id, neighbors):
        self.node_id = node_id
        self.neighbors = neighbors
        self.mailbox = Mailbox()

        self.parent = None
        self.received = set()
        self.visited = False

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data

        if data["message_type"] == "WAVE":
            return self.on_wave(data)

        if data["message_type"] == "ECHO":
            return self.on_echo(data)

        return NodeResponse([])

    def on_wave(self, data):
        sender = data["sender_id"]

        if not self.visited:
            self.visited = True
            self.parent = sender

            actions = []
            for neighbor in self.neighbors:
                if neighbor != sender:
                    actions.append(
                        Action(
                            {"message_type": "WAVE", "sender_id": self.node_id},
                            neighbor,
                            "wave"
                        )
                    )
            return NodeResponse(actions)

        return NodeResponse([])

    def on_echo(self, data):
        sender = data["sender_id"]
        self.received.add(sender)

        if len(self.received) == len(self.neighbors) - (1 if self.parent else 0):
            if self.parent:
                return NodeResponse([
                    Action(
                        {"message_type": "ECHO", "sender_id": self.node_id},
                        self.parent,
                        "echo"
                    )
                ])
            else:
                print(f"ROOT {self.node_id} FINISHED ECHO")

        return NodeResponse([])