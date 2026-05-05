from scheduler.implementation.current_network import CurrentNetwork
from scheduler.core.observer import Observer
from scheduler.core.action import Action


ALGORITHM = "echo"   # "echo" / "tree"

network = CurrentNetwork(algorithm=ALGORITHM)
observer = Observer(network)

# СТАРТОВЕ ПОВІДОМЛЕННЯ (ДУЖЕ ВАЖЛИВО)
root = network.nodes[0]

start_message_type = {
    "wave": "WAVE",
    "echo": "WAVE",
    "tree": "TREE"
}[ALGORITHM]

root.mailbox.add_inbox_action(
    Action(
        {
            "message_type": start_message_type,
            "sender_id": root.node_id
        },
        root.node_id,
        "start"
    )
)

observer.run()