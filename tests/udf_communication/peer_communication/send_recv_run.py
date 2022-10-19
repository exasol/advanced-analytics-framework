import random
import time
from typing import Set

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from tests.udf_communication.peer_communication.utils import BidirectionalQueue


def run(name: str, number_of_instances: int, queue: BidirectionalQueue):
    listen_ip = IPAddress(ip_address=f"127.1.0.1")
    com = PeerCommunicator(number_of_peers=number_of_instances,
                           listen_ip=listen_ip,
                           group_identifier="test")
    queue.put(com.my_connection_info)
    peer_connection_infos = queue.get()
    for index, connection_infos in peer_connection_infos.items():
        com.register_peer(connection_infos)
        time.sleep(random.random() / 10)
    com.wait_for_peers()
    queue.put("Wait finish")
    for peer in com.peers():
        com.send(peer, name.encode("utf8"))
    received_values: Set[str] = set()
    for peer in com.peers():
        value = com.recv(peer)
        received_values.add(value.decode("utf8"))
    queue.put(received_values)
