import sys
import time
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

from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo

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
            logger.info("Peer is ready", name=com.peer.connection_info.name)
            if com.peer.connection_info.name != "i0":
                time.sleep(10)
                peer_i0 = next(
                    peer for peer in com.peers() if peer.connection_info.name == "i0"
                )
                payload = com.peer.connection_info.name.encode("utf8")
                com.send(peer_i0, [setup.socket_factory.create_frame(payload)])
            else:
                while len(received_values) < parameter.number_of_instances - 1:
                    poll_peers = com.poll_peers(timeout_in_milliseconds=100)
                    for peer in poll_peers:
                        message = com.recv(peer)
                        received_values.add(message[0].to_bytes().decode("utf8"))
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
            if index != i and i == 0
        }
        for i in range(number_of_instances)
    }


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=PeerCommunicatorFactory(),
    executor=executor,
    transfer_connection_infos_to_processes=True,
    expectation_generator=expectation_generator,
)


def test_functionality():
    run_test_with_assert(5)


def run_test_with_assert(number_of_instances: int):
    group = f"{time.monotonic_ns()}"
    expected, actual = RUNNER.run_single(group, number_of_instances, 0)
    assert expected == actual
