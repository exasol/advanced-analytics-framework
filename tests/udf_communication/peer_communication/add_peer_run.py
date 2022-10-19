import time
import traceback

import structlog
from structlog.types import FilteringBoundLogger

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from tests.udf_communication.peer_communication.utils import BidirectionalQueue

LOGGER: FilteringBoundLogger = structlog.get_logger(module_name=__name__)


def run(name: str, group_identifier: str, number_of_instances: int, queue: BidirectionalQueue):
    logger = LOGGER.bind(group_identifier=group_identifier, name=name)
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        com = PeerCommunicator(
            name=name,
            number_of_peers=number_of_instances,
            listen_ip=listen_ip,
            group_identifier=group_identifier)
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            for index, connection_info in peer_connection_infos.items():
                # logger.info("connection_info", connection_info=connection_info.dict())
                com.register_peer(connection_info)
            peers = com.peers(timeout_in_seconds=None)
            logger.info("peers", peers=peers)
            queue.put(peers)
        finally:
            com.close()
    except Exception as e:
        traceback.print_exc()
        logger.exception("Exception during test", exception=e)
