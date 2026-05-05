from typing import List

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse
from scheduler.core.action import Action

class TreeNode(AbstractNode):

    def __init__(self, node_id, neighbors):
        self.node_id = node_id
        self.neighbors = neighbors
        self.mailbox = Mailbox()

        self.parent = None
        self.children = []
        self.visited = False

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data

        if data["message_type"] == "TREE":
            return self.on_tree(data)

        return NodeResponse([])

    def on_tree(self, data):
        sender = data["sender_id"]

        if not self.visited:
            self.visited = True
            self.parent = sender

            actions = []
            for neighbor in self.neighbors:
                if neighbor != sender:
                    actions.append(
                        Action(
                            {"message_type": "TREE", "sender_id": self.node_id},
                            neighbor,
                            "tree"
                        )
                    )
            return NodeResponse(actions)

        return NodeResponse([])