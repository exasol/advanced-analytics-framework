import sys
import time
import traceback
from pathlib import Path
from test.integration.without_db.udf_communication.peer_communication.conditional_method_dropper import (
    ConditionalMethodDropper,
)
from test.integration.without_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    PeerCommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)
from typing import (
    Dict,
    List,
    Set,
)

import structlog
import zmq
from structlog import WriteLoggerFactory
from structlog.tracebacks import ExceptionDictTransformer
from structlog.typing import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress
from exasol.analytics.udf.communication.peer_communicator import PeerCommunicator
from exasol.analytics.udf.communication.peer_communicator.forward_register_peer_config import (
    ForwardRegisterPeerConfig,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator_config import (
    PeerCommunicatorConfig,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)

structlog.configure(
    context_class=dict,
    logger_factory=WriteLoggerFactory(
        file=Path(__file__).with_suffix(".log").open("wt")
    ),
    processors=[
        structlog.contextvars.merge_contextvars,
        ConditionalMethodDropper(method_name="debug"),
        ConditionalMethodDropper(method_name="info"),
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(),
        structlog.processors.ExceptionRenderer(
            exception_formatter=ExceptionDictTransformer(locals_max_string=320)
        ),
        structlog.processors.CallsiteParameterAdder(),
        structlog.processors.JSONRenderer(),
    ],
)

LOGGER: FilteringBoundLogger = structlog.get_logger()


def run(parameter: PeerCommunicatorTestProcessParameter, queue: BidirectionalQueue):
    logger = LOGGER.bind(
        group_identifier=parameter.group_identifier, name=parameter.instance_name
    )
    received_values: set[str] = set()
    try:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        context = zmq.Context()
        socket_factory = ZMQSocketFactory(context)
        com = PeerCommunicator(
            name=parameter.instance_name,
            number_of_peers=parameter.number_of_instances,
            listen_ip=listen_ip,
            group_identifier=parameter.group_identifier,
            socket_factory=socket_factory,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=False, is_enabled=False
                ),
            ),
        )
        try:
            queue.put(com.my_connection_info)
            peer_connection_infos = queue.get()
            for index, connection_infos in peer_connection_infos.items():
                com.register_peer(connection_infos)
            com.wait_for_peers()
            LOGGER.info("Peer is ready", name=com.peer.connection_info.name)
            if com.peer.connection_info.name != "i0":
                time.sleep(10)
                peer_i0 = next(
                    peer for peer in com.peers() if peer.connection_info.name == "i0"
                )
                com.send(
                    peer_i0,
                    [
                        socket_factory.create_frame(
                            com.peer.connection_info.name.encode("utf8")
                        )
                    ],
                )
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
            context.destroy(linger=0)
            for frame in sys._current_frames().values():
                stacktrace = traceback.format_stack(frame)
                logger.info("Frame", stacktrace=stacktrace)
    except Exception as e:
        logger.exception("Exception during test")
        queue.put(f"Failed: {e}")
    queue.put(received_values)


def test_functionality_5():
    run_test_with_assert(5)


def run_test_with_assert(number_of_instances: int):
    group = f"{time.monotonic_ns()}"
    expected_peers_of_threads, peers_of_threads = run_test(
        group, number_of_instances, 0
    )
    assert expected_peers_of_threads == peers_of_threads


def run_test(group: str, number_of_instances: int, seed: int):
    connection_infos: dict[int, ConnectionInfo] = {}
    parameters = [
        PeerCommunicatorTestProcessParameter(
            instance_name=f"i{i}",
            group_identifier=group,
            number_of_instances=number_of_instances,
            seed=seed + i,
        )
        for i in range(number_of_instances)
    ]
    processes: list[TestProcess[PeerCommunicatorTestProcessParameter]] = [
        TestProcess(parameter, run=run) for parameter in parameters
    ]
    for i in range(number_of_instances):
        processes[i].start()
    for i in range(number_of_instances):
        connection_infos[i] = processes[i].get()
    for i in range(number_of_instances):
        t = processes[i].put(connection_infos)
    assert_processes_finish(processes, timeout_in_seconds=180)
    received_values: dict[int, set[str]] = {}
    for i in range(number_of_instances):
        received_values[i] = processes[i].get()
    expected_received_values = {
        i: {
            thread.parameter.instance_name
            for index, thread in enumerate(processes)
            if index != i and i == 0
        }
        for i in range(number_of_instances)
    }
    return expected_received_values, received_values
