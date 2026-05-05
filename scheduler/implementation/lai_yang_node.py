import uuid
import random

from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.action import Action
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse


class LaiYangNode(AbstractNode):

    def __init__(self, node_id, neighbors):
        self.node_id = node_id
        self.mailbox = Mailbox()
        self.neighbors = neighbors

        # snapshot state
        self.snapshot_taken = False
        self.local_state = None

        # channel states
        self.channel_state = {n: [] for n in neighbors}

        # counters
        self.sent_false_count = {n: 0 for n in neighbors}
        self.received_false_count = {n: 0 for n in neighbors}

        # control info
        self.expected_false = {n: None for n in neighbors}
        self.channel_done = {n: False for n in neighbors}

    # -------------------------------
    def process_action(self, message) -> NodeResponse:
        data = message.data
        msg_type = data.get("message_type")

        actions = []

        # --- обробка повідомлення ---
        if msg_type == "New":
            actions += self.start_snapshot()

        elif msg_type == "BASIC":
            self.handle_basic(data)

        elif msg_type == "CONTROL":
            self.handle_control(data)

        if self.neighbors :
            receiver = random.choice(self.neighbors)
            actions.append(self.send_basic(receiver))

        return NodeResponse(actions)

    # -------------------------------
    def start_snapshot(self):
        if self.snapshot_taken:
            return []

        print(f"{self.node_id} START SNAPSHOT")
        self.take_snapshot()

        actions = []

        for n in self.neighbors:
            control = {
                "message_type": "CONTROL",
                "sender": self.node_id,
                "sent_false": self.sent_false_count[n]
            }
            actions.append(Action(control, n, uuid.uuid4()))

        return actions

    # -------------------------------
    def handle_basic(self, data):
        sender = data["sender"]
        tag = data["tag"]

        # якщо отримали TRUE і ще не зняли snapshot
        if not self.snapshot_taken and tag:
            self.take_snapshot()

        # якщо snapshot вже взятий і це "false" повідомлення
        if self.snapshot_taken and not tag:
            self.channel_state[sender].append(data)
            self.received_false_count[sender] += 1

            self.try_finish_channel(sender)

    # -------------------------------
    def handle_control(self, data):
        sender = data["sender"]
        sent_false = data["sent_false"]

        if not self.snapshot_taken:
            self.take_snapshot()

        self.expected_false[sender] = sent_false

        self.try_finish_channel(sender)

    # -------------------------------
    def try_finish_channel(self, sender):
        expected = self.expected_false[sender]
        received = self.received_false_count[sender]

        if expected is not None and received == expected:
            if not self.channel_done[sender]:
                self.channel_done[sender] = True
                print(f"{self.node_id} CHANNEL {sender} DONE: {self.channel_state[sender]}")

                self.check_snapshot_complete()

    # -------------------------------
    def check_snapshot_complete(self):
        if all(self.channel_done.values()):
            print(f"\n{self.node_id} SNAPSHOT COMPLETE")
            print(f"STATE: {self.local_state}")
            print(f"CHANNELS: {self.channel_state}\n")

    # -------------------------------
    def take_snapshot(self):
        self.snapshot_taken = True
        self.local_state = f"STATE({self.node_id})"
        print(f"{self.node_id} TOOK SNAPSHOT")

    # -------------------------------
    def send_basic(self, receiver):
        tag = self.snapshot_taken  # TRUE після snapshot

        if not tag:
            self.sent_false_count[receiver] += 1

        msg = {
            "message_type": "BASIC",
            "tag": tag,
            "sender": self.node_id
        }

        return Action(msg, receiver, uuid.uuid4())