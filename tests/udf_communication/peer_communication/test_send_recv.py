from typing import Dict, Set

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from tests.udf_communication.peer_communication.utils import BidirectionalQueue, TestThread


def run(name: str, number_of_instances: int, queue: BidirectionalQueue):
    listen_ip = IPAddress(ip_address=f"127.1.0.1")
    com = PeerCommunicator(number_of_peers=number_of_instances,
                           listen_ip=listen_ip,
                           group_identifier="test")
    queue.put(com.my_connection_info)
    peer_connection_infos = queue.get()
    for index, connection_infos in peer_connection_infos.items():
        com.register_peer(connection_infos)
    for peer in com.peers():
        com.send(peer, name.encode("utf8"))
    received_values: Set[str] = set()
    for peer in com.peers():
        value = com.recv(peer)
        received_values.add(value.decode("utf8"))
    queue.put(received_values)


def test():
    number_of_instances = 5
    threads: Dict[int, TestThread] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        threads[i] = TestThread(f"t{i}", number_of_instances, run=run)
        threads[i].start()
        connection_infos[i] = threads[i].get()

    for i in range(number_of_instances):
        t = threads[i].put(connection_infos)

    received_values: Dict[int, Set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = threads[i].get()

    expected_received_values = {
        i: {
            thread.name
            for index, thread in threads.items()
            if index != i
        }
        for i in range(number_of_instances)
    }
    assert expected_received_values == received_values

    for i in range(number_of_instances):
        threads[i].join(timeout=10)
        assert not threads[i].is_alive()
