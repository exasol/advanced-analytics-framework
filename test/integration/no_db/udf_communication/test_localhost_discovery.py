import time
from test.integration.no_db.peer_com_runner import (
    PeerComSetupFactory,
    RepetitionRunner,
    expect_sorted_peers,
)
from test.integration.no_db.structlog.structlog_utils import configure_structlog
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    CommunicatorTestProcessParameter,
    PeerCommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)

import pytest
import structlog
import zmq
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.discovery import localhost
from exasol.analytics.udf.communication.discovery.localhost.communicator import (
    CommunicatorFactory,
)
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.peer_communicator import (
    key_for_peer,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)

configure_structlog(__file__)


def executor(
    logger: FilteringBoundLogger,
    setup_factory: PeerComSetupFactory,
    parameter: PeerCommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    discovery_port = Port(port=44444)
    listen_ip = IPAddress(ip_address="127.1.0.1")
    context = zmq.Context()
    socket_factory = ZMQSocketFactory(context)
    discovery_socket_factory = localhost.DiscoverySocketFactory()
    peer_communicator = CommunicatorFactory().create(
        group_identifier=parameter.group_identifier,
        name=parameter.instance_name,
        number_of_instances=parameter.number_of_instances,
        listen_ip=listen_ip,
        discovery_port=discovery_port,
        socket_factory=socket_factory,
        discovery_socket_factory=discovery_socket_factory,
    )
    queue.put(peer_communicator.my_connection_info)
    if peer_communicator.are_all_peers_connected():
        peers = peer_communicator.peers()
        queue.put(peers)
    else:
        queue.put([])


RUNNER = RepetitionRunner(
    __name__,
    setup_factory=None,
    executor=executor,
    expectation_generator=expect_sorted_peers,
)


@pytest.mark.parametrize("instances", [2, 10, 25])
def test_functionality(instances):
    RUNNER.run_multiple(instances, 1)


@pytest.mark.parametrize("instances, repetitions", [(2, 1000), (10, 100), (25, 10)])
def test_reliability(instances: int, repetitions: int):
    RUNNER.run_multiple(instances, repetitions)
