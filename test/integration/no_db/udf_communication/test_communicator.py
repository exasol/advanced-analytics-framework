from test.integration.no_db.structlog.structlog_utils import configure_structlog
from test.integration.no_db.udf_com_runner import (
    RepetitionRunner,
    UdfCommunicatorFactory,
)
from test.integration.no_db.udf_communication.peer_communication.utils import (
    BidirectionalQueue,
    CommunicatorTestProcessParameter,
)

import pytest
from structlog.types import FilteringBoundLogger

configure_structlog(__file__)


def executor(
    logger: FilteringBoundLogger,
    communicator_factory: UdfCommunicatorFactory,
    parameter: CommunicatorTestProcessParameter,
    queue: BidirectionalQueue,
):
    communicator = communicator_factory.create(parameter)
    queue.put("Finished")


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=UdfCommunicatorFactory(),
    executor=executor,
    expect="Finished",
)

@pytest.mark.parametrize("nodes, instances_per_node", [
    (2,1), (1,2), (2, 2), (3,3),
])
def test_functionality(nodes, instances_per_node):
    RUNNER.run_multiple(nodes, instances_per_node, 1)


@pytest.mark.parametrize(
    "nodes, instances_per_node, repetitions",
    [
        (2, 2, 100),
        (3, 3, 20),
    ],
)
def test_reliability(nodes: int, instances_per_node: int, repetitions: int):
    RUNNER.run_multiple(
        number_of_nodes=nodes,
        number_of_instances_per_node=instances_per_node,
        repetitions=repetitions,
    )
