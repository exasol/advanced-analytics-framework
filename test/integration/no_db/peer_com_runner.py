import time
from dataclasses import dataclass
from datetime import timedelta
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    PeerCommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)
from typing import (
    Any,
    Callable,
)

import structlog
import zmq
from numpy.random import RandomState
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.connection_info import ConnectionInfo
from exasol.analytics.udf.communication.ip_address import IPAddress
from exasol.analytics.udf.communication.peer import Peer
from exasol.analytics.udf.communication.peer_communicator.forward_register_peer_config import (
    ForwardRegisterPeerConfig,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator import (
    PeerCommunicator,
    key_for_peer,
)
from exasol.analytics.udf.communication.peer_communicator.peer_communicator_config import (
    PeerCommunicatorConfig,
)
from exasol.analytics.udf.communication.socket_factory.abstract import SocketFactory
from exasol.analytics.udf.communication.socket_factory.fault_injection import (
    FaultInjectionSocketFactory,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)


@dataclass
class PeerCommunicatorSetup:
    context: zmq.Context
    socket_factory: SocketFactory
    communicator: PeerCommunicator


@dataclass
class PeerCommunicatorFactory:
    inject_faults: bool = False
    leader_name: str = ""
    enable_forward: bool = False

    def context(self) -> zmq.Context:
        return zmq.Context()

    def socket_factory(
        self,
        parameter: PeerCommunicatorTestProcessParameter,
        context: zmq.Context | None = None,
    ) -> SocketFactory:
        context = context or self.context()
        socket_factory = ZMQSocketFactory(context)
        if not self.inject_faults:
            return socket_factory
        return FaultInjectionSocketFactory(
            socket_factory, 0.01, RandomState(parameter.seed)
        )

    def create(
        self,
        parameter: PeerCommunicatorTestProcessParameter,
        # context: zmq.Context | None = None,
        # socket_factory: SocketFactory | None = None,
    ) -> PeerCommunicator:
        listen_ip = IPAddress(ip_address=f"127.1.0.1")
        context = zmq.Context()
        # socket_factory = socket_factory or self.socket_factory(parameter, context)
        socket_factory = ZMQSocketFactory(context)
        if self.inject_faults:
            socket_factory = FaultInjectionSocketFactory(
                socket_factory, 0.01, RandomState(parameter.seed)
            )

        is_leader = False
        if self.leader_name and parameter.instance_name == self.leader_name:
            is_leader = True
        com = PeerCommunicator(
            name=parameter.instance_name,
            number_of_peers=parameter.number_of_instances,
            listen_ip=listen_ip,
            group_identifier=parameter.group_identifier,
            socket_factory=socket_factory,
            config=PeerCommunicatorConfig(
                forward_register_peer_config=ForwardRegisterPeerConfig(
                    is_leader=is_leader, is_enabled=self.enable_forward
                )
            ),
        )
        return PeerCommunicatorSetup(
            context=context,
            socket_factory=socket_factory,
            communicator=com,
        )


Executor = Callable[
    [
        FilteringBoundLogger,
        PeerCommunicatorFactory | None,
        PeerCommunicatorTestProcessParameter,
        BidirectionalQueue,
    ],
    None,
]


def expect_success(
    number_of_instances: int,
    connection_infos: dict[int, ConnectionInfo],
    processes: list[TestProcess[PeerCommunicatorTestProcessParameter]],
) -> dict[int, str]:
    return {i: "Success" for i in range(number_of_instances)}


def expect_sorted_peers(
    number_of_instances: int,
    connection_infos: dict[int, ConnectionInfo],
    processes: list[TestProcess[PeerCommunicatorTestProcessParameter]],
) -> dict[int, set[str]]:
    return {
        i: sorted(
            [
                Peer(connection_info=connection_info)
                for index, connection_info in connection_infos.items()
            ],
            key=key_for_peer,
        )
        for i in range(number_of_instances)
    }


ExpectationGenerator = Callable[
    [
        int,  # number_of_instances
        dict[int, ConnectionInfo],  # connection_infos
        list[TestProcess[PeerCommunicatorTestProcessParameter]],  # processes
    ],
    dict[int, Any],  # expected_thread_result
]
"""
Given the number_of_instances, the connection_infos, and the list of
processes, this function defines the expected results for each of the threads.
"""


class RepetitionRunner:
    def __init__(
        self,
        name: str,
        communicator_factory: PeerCommunicatorFactory | None,
        executor: Executor,
        expectation_generator: ExpectationGenerator,
        vary_seed: bool = True,
        transfer_connection_infos_to_processes: bool = True,
        process_finish_timeout: timedelta = timedelta(seconds=180),
    ):
        self.logger: FilteringBoundLogger = structlog.get_logger(name)
        self.communicator_factory = communicator_factory
        self.executor = executor
        self.vary_seed = vary_seed
        self.transfer_connection_infos = transfer_connection_infos_to_processes
        self.finish_timeout = process_finish_timeout
        self.expectation_generator = expectation_generator

    def run(
        self,
        parameter: PeerCommunicatorTestProcessParameter,
        queue: BidirectionalQueue,
    ):
        logger = self.logger.bind(
            group_identifier=parameter.group_identifier,
            name=parameter.instance_name,
        )
        self.executor(
            logger=logger,
            communicator_factory=self.communicator_factory,
            parameter=parameter,
            queue=queue,
        )

    def run_single(self, group: str, number_of_instances: int, seed: int):
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
            TestProcess(parameter, run=self.run) for parameter in parameters
        ]
        connection_infos: dict[int, ConnectionInfo] = {}
        for i in range(number_of_instances):
            processes[i].start()
        for i in range(number_of_instances):
            connection_infos[i] = processes[i].get()
        if self.transfer_connection_infos:
            for i in range(number_of_instances):
                t = processes[i].put(connection_infos)
        assert_processes_finish(processes, self.finish_timeout.seconds)
        actual: dict[int, list[ConnectionInfo]] = {
            i: processes[i].get() for i in range(number_of_instances)
        }
        expected = self.expectation_generator(
            number_of_instances,
            connection_infos,
            processes,
        )
        return expected, actual

    def run_multiple(self, number_of_instances: int, repetitions: int):
        for i in range(repetitions):
            self.logger.info(
                f"Start iteration",
                iteration=i + 1,
                repetitions=repetitions,
                number_of_instances=number_of_instances,
            )
            start_time = time.monotonic()
            group = f"{time.monotonic_ns()}"
            seed = i if self.vary_seed else -1
            expected, actual = self.run_single(group, number_of_instances, seed=seed)
            assert expected == actual, f"{expected} != {actual}"
            end_time = time.monotonic()
            self.logger.info(
                f"Finish iteration",
                iteration=i + 1,
                repetitions=repetitions,
                number_of_instances=number_of_instances,
                duration=end_time - start_time,
            )
