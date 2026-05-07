from scheduler.core.observer import Observer
from scheduler.implementation.rana_network import RanaNetwork
from scheduler.implementation.safra_network import SafraNetwork

# NETWORK_CLASS should be set to the current implementation network class of the AbstractNetwork class
NETWORK_CLASS = RanaNetwork
# NETWORK_CLASS = SafraNetwork

if __name__ == '__main__':
    # create the current object of the network implementation
    network = NETWORK_CLASS()
    observer = Observer(network)
    observer.run()
