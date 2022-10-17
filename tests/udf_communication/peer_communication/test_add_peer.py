import time
import traceback
from typing import Dict, List

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator, key_for_peer
from tests.udf_communication.peer_communication.utils import BidirectionalQueue, TestThread


def run(name: str, number_of_instances: int, queue: BidirectionalQueue):
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        com = PeerCommunicator(number_of_peers=number_of_instances,
                               listen_ip=listen_ip,
                               group_identifier="test")
        queue.put(com.my_connection_info)
        peer_connection_infos = queue.get()
        for index, connection_infos in peer_connection_infos.items():
            # print("register_peer start", name, index)
            com.register_peer(connection_infos)
            # print("register_peer end", name, index)
        # print("peers start", name)
        peers = com.peers()
        # print("peers end", name)
        # print("put start", name)
        queue.put(peers)
        # print("put end", name)
    except Exception as e:
        print(e)
        traceback.print_exc()


def test():
    number_of_instances = 3
    threads: Dict[int, TestThread] = {}
    connection_infos: Dict[int, ConnectionInfo] = {}
    for i in range(number_of_instances):
        threads[i] = TestThread(f"t{i}", number_of_instances, run=run)
        threads[i].start()
        connection_infos[i] = threads[i].get()

    for i in range(number_of_instances):
        t = threads[i].put(connection_infos)

    peers_of_threads: Dict[int, List[ConnectionInfo]] = {}
    for i in range(number_of_instances):
        peers_of_threads[i] = threads[i].get()

    expected_peers_of_threads = {
        i: sorted([
            Peer(connection_info=connection_info)
            for index, connection_info in connection_infos.items()
            if index != i
        ], key=key_for_peer)
        for i in range(number_of_instances)
    }
    assert expected_peers_of_threads == peers_of_threads

    for i in range(number_of_instances):
        threads[i].join(timeout=10)
        assert not threads[i].is_alive()
