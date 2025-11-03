import os
import sys
import traceback
from test.integration.no_db.peer_com_runner import (
    PeerCommunicatorFactory,
    RepetitionRunner,
)
from test.integration.no_db.structlog.structlog_utils import configure_structlog
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    PeerCommunicatorTestProcessParameter,
    TestProcess,
)

import pytest
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.peer import Peer

configure_structlog(__file__)


def executor(
    logger: FilteringBoundLogger,
    communicator_factory: PeerCommunicatorFactory,
    parameter: PeerCommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    received_values: set[str] = set()
    try:
        setup = communicator_factory.create(parameter)
        com = setup.communicator
        try:
            queue.put(com.my_connection_info)
            for index, connection_infos in queue.get().items():
                com.register_peer(connection_infos)
            com.wait_for_peers()
            logger.info("Peer is ready", name=parameter.instance_name)
            for peer in com.peers():
                if peer != Peer(connection_info=com.my_connection_info):
                    payload = parameter.instance_name.encode("utf8")
                    com.send(peer, [setup.socket_factory.create_frame(payload)])
            for peer in com.peers():
                if peer != Peer(connection_info=com.my_connection_info):
                    value = com.recv(peer)
                    received_values.add(value[0].to_bytes().decode("utf8"))
        finally:
            try:
                com.stop()
            except Exception as e:
                logger.exception("Exception during stop")
                queue.put(f"Failed: {e}")
            setup.context.destroy(linger=0)
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        logger.exception("Exception during test")
        queue.put(f"Failed: {e}")
    queue.put(received_values)


def expectation_generator(
    number_of_instances: int,
    connection_infos: dict[int, ConnectionInfo],
    processes: list[TestProcess[PeerCommunicatorTestProcessParameter]],
) -> dict[int, set[str]]:
    return {
        i: {
            thread.parameter.instance_name
            for index, thread in enumerate(processes)
            if index != i
        }
        for i in range(number_of_instances)
    }


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=PeerCommunicatorFactory(inject_faults=True),
    executor=executor,
    transfer_connection_infos_to_processes=True,
    expectation_generator=expectation_generator,
)


@pytest.mark.parametrize("instances", [2, 5, 10, 25])
def test_functionality(instances):
    if instances > 20 and "GITHUB_ACTIONS" in os.environ:
        pytest.skip(
            "This test is unstable on Github Action, "
            "because of the limited number of cores on the default runners.",
        )
    RUNNER.run_multiple(instances, 1)


@pytest.mark.parametrize("instances, repetitions", [(2, 1000), (10, 100)])
def test_reliability(instances: int, repetitions: int):
    RUNNER.run_multiple(instances, repetitions)

