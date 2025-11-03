import sys
import traceback
from test.integration.no_db.peer_com_runner import (
    PeerCommunicatorFactory,
    RepetitionRunner,
    expect_sorted_peers,
)
from test.integration.no_db.structlog.structlog_utils import configure_structlog
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    PeerCommunicatorTestProcessParameter,
)

import pytest
from structlog.types import FilteringBoundLogger

configure_structlog(__file__)


def executor(
    logger: FilteringBoundLogger,
    communicator_factory: PeerCommunicatorFactory,
    parameter: PeerCommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    try:
        setup = communicator_factory.create(parameter)
        com = setup.communicator
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            for index, connection_info in peer_connection_infos.items():
                com.register_peer(connection_info)
            peers = com.peers(timeout_in_milliseconds=None)
            logger.info("peers", number_of_peers=len(peers))
            queue.put(peers)
        finally:
            com.stop()
            logger.info("after close")
            setup.context.destroy(linger=0)
            logger.info("after destroy")
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        queue.put([])
        logger.exception("Exception during test")


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=PeerCommunicatorFactory(inject_faults=True),
    executor=executor,
    expectation_generator=expect_sorted_peers,
)


@pytest.mark.parametrize("instances", [2, 3, 10, 25])
def test_functionality(instances):
    RUNNER.run_multiple(instances, 1)


@pytest.mark.parametrize("instances, repetitions", [(2, 1000), (10, 100), (25, 10)])
def test_reliability(instances: int, repetitions: int):
    RUNNER.run_multiple(instances, repetitions)
