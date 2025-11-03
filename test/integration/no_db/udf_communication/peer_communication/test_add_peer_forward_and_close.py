import sys
import traceback
from test.integration.no_db.peer_com_runner import (
    PeerComSetupFactory,
    RepetitionRunner,
    expect_success,
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
    setup_factory: PeerComSetupFactory,
    parameter: PeerCommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    try:
        setup = setup_factory.create(parameter)
        com = setup.communicator
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            if com.forward_register_peer_config.is_leader:
                for index, connection_info in peer_connection_infos.items():
                    com.register_peer(connection_info)
        finally:
            try:
                com.stop()
                queue.put("Success")
            except Exception as e:
                logger.exception("Exception during stop")
                queue.put(f"Failed to stop: {e}")
            setup.context.destroy(linger=0)
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        logger.exception("Exception during test")
        queue.put(f"Failed: {e}")


RUNNER = RepetitionRunner(
    __name__,
    setup_factory=PeerComSetupFactory(
        inject_faults=True,
        leader_name="i0",
        enable_forward=True,
    ),
    executor=executor,
    expectation_generator=expect_success,
)


@pytest.mark.parametrize("instances", [2, 3, 10, 25])
def test_functionality(instances):
    RUNNER.run_multiple(instances, 1)


@pytest.mark.parametrize("instances, repetitions", [(2, 1000), (10, 100), (25, 10)])
def test_reliability(instances: int, repetitions: int):
    RUNNER.run_multiple(instances, repetitions)
