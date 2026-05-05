from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse
from scheduler.core.action import Action

class WaveNode(AbstractNode):

    def __init__(self, node_id, neighbors):
        self.node_id = node_id
        self.neighbors = neighbors
        self.mailbox = Mailbox()

        self.visited = False
        self.parent = None

    def process_action(self, message: Action) -> NodeResponse:
        data = message.data

        if data.get("message_type") == "WAVE":
            return self.on_wave(data)

        return NodeResponse([])

    def on_wave(self, data):
        if not self.visited:
            self.visited = True
            self.parent = data.get("sender_id")

            actions = []
            for neighbor in self.neighbors:
                if neighbor != self.parent:
                    actions.append(
                        Action(
                            {
                                "message_type": "WAVE",
                                "sender_id": self.node_id
                            },
                            neighbor,
                            "wave"
                        )
                    )
            return NodeResponse(actions)

        return NodeResponse([])