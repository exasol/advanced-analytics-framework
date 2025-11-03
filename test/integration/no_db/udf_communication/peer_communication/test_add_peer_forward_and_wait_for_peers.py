import traceback
from datetime import timedelta
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
        com = communicator_factory.create(parameter).communicator
        try:
            queue.put(com.my_connection_info)
            if com.forward_register_peer_config.is_leader:
                for index, connection_info in queue.get().items():
                    com.register_peer(connection_info)
            peers = com.peers(timeout_in_milliseconds=None)
            logger.info("peers", peers=len(peers))
        finally:
            logger.info("com stop before")
            com.stop()
            logger.info("com stop after")
        queue.put(peers)
    except Exception as e:
        traceback.print_exc()
        logger.exception("Exception during test", exception=e)
        queue.put([])


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=PeerCommunicatorFactory(
        inject_faults=True,
        leader_name="i0",
        enable_forward=True,
    ),
    executor=executor,
    process_finish_timeout=timedelta(seconds=300),
    expectation_generator=expect_sorted_peers,
    transfer_connection_infos_to_processes=True,
)


@pytest.mark.parametrize("instances", [2, 3, 5, 10, 15, 25])
def test_functionality(instances):
    RUNNER.run_multiple(instances, 1)


@pytest.mark.parametrize("instances, repetitions", [(2, 1000), (10, 100), (25, 10)])
def test_reliability(instances: int, repetitions: int):
    RUNNER.run_multiple(instances, repetitions)
