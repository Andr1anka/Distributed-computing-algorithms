from scheduler.abstract.abstract_node import AbstractNode
from scheduler.core.mailbox import Mailbox
from scheduler.core.node_response import NodeResponse
from scheduler.core.action import Action


class ElectionEchoNode(AbstractNode):

    def __init__(self, node_id, neighbors):
        self.node_id = node_id
        self.neighbors = neighbors
        self.mailbox = Mailbox()

        # ID хвилі в якій зараз вузол
        self.wave_id = None

        # Батько в дереві хвилі
        self.parent = None

        # Від кого прийшли echo
        self.echo_received = set()

        # Чи вузол вже завершив
        self.decided = False

    def process_action(self, message: Action) -> NodeResponse:

        data = message.data
        message_type = data["message_type"]

        if message_type == "WAVE":
            return self.on_wave(data)

        elif message_type == "ECHO":
            return self.on_echo(data)

        return NodeResponse([])

    # ==========================================
    # WAVE
    # ==========================================

    def on_wave(self, data):

        sender = data["sender_id"]
        incoming_wave = data["wave_id"]

        actions = []

        # ======================================
        # Перша хвиля або більший wave_id
        # ======================================

        if self.wave_id is None or incoming_wave > self.wave_id:

            print(f"{self.node_id} switches to wave {incoming_wave}")

            # Погашення старої хвилі
            self.wave_id = incoming_wave

            # Якщо вузол сам ініціатор хвилі
            if sender == self.node_id:
                self.parent = self.node_id
            else:
                self.parent = sender

            self.echo_received = set()
            # Розсилка нової хвилі
            children = 0

            for neighbor in self.neighbors:

                if neighbor != sender:

                    children += 1

                    actions.append(
                        Action(
                            {
                                "message_type": "WAVE",
                                "sender_id": self.node_id,
                                "wave_id": incoming_wave
                            },
                            neighbor,
                            f"wave-{incoming_wave}"
                        )
                    )

            # Якщо лист
            if children == 0:

                actions.append(
                    Action(
                        {
                            "message_type": "ECHO",
                            "sender_id": self.node_id,
                            "wave_id": incoming_wave
                        },
                        self.parent,
                        f"echo-{incoming_wave}"
                    )
                )

        # ======================================
        # Та сама хвиля
        # ======================================

        elif incoming_wave == self.wave_id:

            # ігноруємо дублікати
            pass

        # ======================================
        # Менша хвиля → гасимо
        # ======================================

        else:
            print(f"{self.node_id} ignores smaller wave {incoming_wave}")

        return NodeResponse(actions)

    # ==========================================
    # ECHO
    # ==========================================

    def on_echo(self, data):

        sender = data["sender_id"]
        wave_id = data["wave_id"]

        actions = []

        # echo старої хвилі ігноруємо
        if wave_id != self.wave_id:
            return NodeResponse([])

        self.echo_received.add(sender)

        expected = len(self.neighbors)

        if self.parent is not None:
            expected -= 1

        # ======================================
        # Всі echo отримані
        # ======================================

        if len(self.echo_received) == expected:

            # ROOT
            if self.parent == self.node_id:

                if not self.decided:
                    self.decided = True

                    print()
                    print("===================================")
                    print(f"LEADER ELECTED: {self.node_id}")
                    print(f"WAVE ID: {self.wave_id}")
                    print("===================================")
                    print()

            else:

                actions.append(
                    Action(
                        {
                            "message_type": "ECHO",
                            "sender_id": self.node_id,
                            "wave_id": self.wave_id
                        },
                        self.parent,
                        f"echo-{self.wave_id}"
                    )
                )

        return NodeResponse(actions)