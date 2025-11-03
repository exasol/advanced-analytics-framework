import time
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    CommunicatorTestProcessParameter,
    TestProcess,
    assert_processes_finish,
)
from typing import Callable

import structlog
import zmq
from structlog.types import FilteringBoundLogger

from exasol.analytics.udf.communication.communicator import Communicator
from exasol.analytics.udf.communication.ip_address import (
    IPAddress,
    Port,
)
from exasol.analytics.udf.communication.socket_factory.zmq_wrapper import (
    ZMQSocketFactory,
)


class UdfCommunicatorFactory:
    def create(self, parameter: CommunicatorTestProcessParameter) -> Communicator:
        is_discovery_leader_node = parameter.node_name == "n0"
        context = zmq.Context()
        socket_factory = ZMQSocketFactory(context)
        return Communicator(
            multi_node_discovery_port=Port(port=44444),
            local_discovery_port=parameter.local_discovery_port,
            multi_node_discovery_ip=IPAddress(ip_address="127.0.0.1"),
            node_name=parameter.node_name,
            instance_name=parameter.instance_name,
            listen_ip=IPAddress(ip_address="127.0.0.1"),
            group_identifier=parameter.group_identifier,
            number_of_nodes=parameter.number_of_nodes,
            number_of_instances_per_node=parameter.number_of_instances_per_node,
            is_discovery_leader_node=is_discovery_leader_node,
            socket_factory=socket_factory,
        )


Executor = Callable[
    [
        FilteringBoundLogger,
        UdfCommunicatorFactory,
        CommunicatorTestProcessParameter,
        BidirectionalQueue,
    ],
    None,
]


class RepetitionRunner:
    def __init__(
        self,
        name: str,
        communicator_factory: UdfCommunicatorFactory,
        executor: Executor,
        expect: str,
    ):
        self.logger: FilteringBoundLogger = structlog.get_logger(name)
        self.communicator_factory = communicator_factory
        self.executor = executor
        self.expect = expect

    def run(
        self,
        parameter: CommunicatorTestProcessParameter,
        queue: BidirectionalQueue,
    ):
        self.executor(
            logger=self.logger,
            communicator_factory=self.communicator_factory,
            parameter=parameter,
            queue=queue,
        )

    def run_single(
        self,
        group_identifier: str,
        number_of_nodes: int,
        number_of_instances_per_node: int,
    ):
        parameters = [
            CommunicatorTestProcessParameter(
                node_name=f"n{n}",
                instance_name=f"i{i}",
                group_identifier=group_identifier,
                number_of_nodes=number_of_nodes,
                number_of_instances_per_node=number_of_instances_per_node,
                local_discovery_port=Port(port=44445 + n),
                seed=0,
            )
            for n in range(number_of_nodes)
            for i in range(number_of_instances_per_node)
        ]
        processes: list[TestProcess[CommunicatorTestProcessParameter]] = [
            TestProcess(parameter, run=self.run) for parameter in parameters
        ]
        for process in processes:
            process.start()
        assert_processes_finish(processes, timeout_in_seconds=180)
        actual: dict[tuple[str, str], str] = {}
        expected: dict[tuple[str, str], str] = {}
        for process in processes:
            result_key = (process.parameter.node_name, process.parameter.instance_name)
            actual[result_key] = process.get()
            expected[result_key] = self.expect
        return expected, actual

    def run_multiple(
        self,
        number_of_nodes: int,
        number_of_instances_per_node: int,
        repetitions: int,
    ):
        for i in range(repetitions):
            group = f"{time.monotonic_ns()}"
            self.logger.info(
                f"Start iteration",
                iteration=i + 1,
                repetitions=repetitions,
                group_identifier=group,
                number_of_nodes=number_of_nodes,
                number_of_instances_per_node=number_of_instances_per_node,
            )
            start_time = time.monotonic()
            expected, actual = self.run_single(
                group_identifier=group,
                number_of_nodes=number_of_nodes,
                number_of_instances_per_node=number_of_instances_per_node,
            )
            assert expected == actual, f"{expected} =! {actual}"
            end_time = time.monotonic()
            self.logger.info(
                f"Finish iteration",
                iteration=i + 1,
                repetitions=repetitions,
                group_identifier=group,
                number_of_nodes=number_of_nodes,
                number_of_instances_per_node=number_of_instances_per_node,
                duration=end_time - start_time,
            )
