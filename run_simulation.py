from scheduler.implementation.current_network import CurrentNetwork
from scheduler.core.observer import Observer
from scheduler.core.action import Action


ALGORITHM = "election"

network = CurrentNetwork(algorithm=ALGORITHM)
observer = Observer(network)

# КОЖЕН ВУЗОЛ Є ІНІЦІАТОРОМ

for node in network.nodes:

    # стартове повідомлення
    node.mailbox.add_inbox_action(
        Action(
            {
                "message_type": "WAVE",
                "sender_id": node.node_id,
                "wave_id": node.node_id
            },
            node.node_id,
            f"start-{node.node_id}"
        )
    )

observer.run()