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
    try:
        value = f"{parameter.node_name}_{parameter.instance_name}"
        names = {
            f"n{node}_i{instance}".encode()
            for instance in range(parameter.number_of_instances_per_node)
            for node in range(parameter.number_of_nodes)
        }
        result = communicator.gather(value.encode("utf-8"))
        logger.info(
            "result",
            result=result,
            instance_name=parameter.instance_name,
            node_name=parameter.node_name,
        )
        if communicator.is_multi_node_leader():
            if isinstance(result, list):
                if names != set(result):
                    queue.put(f"Leader failed: {result} != {names}")
                    return
            else:
                queue.put(f"Leader failed: {result} != {names}")
                return
        else:
            if result is not None:
                queue.put(f"Non-Leader failed: {result} is not None")
                return
        queue.put("Success")
    except Exception as e:
        logger.exception("Exception during test")
        queue.put(f"Failed during test: {e}")


RUNNER = RepetitionRunner(
    __name__,
    communicator_factory=UdfCommunicatorFactory(),
    executor=executor,
    expect="Success",
)


@pytest.mark.parametrize("nodes, instance_per_node", [(2, 1), (1, 2), (2, 2), (3, 3)])
def test_functionality(nodes, instance_per_node):
    RUNNER.run_multiple(
        number_of_nodes=nodes,
        number_of_instances_per_node=instance_per_node,
        repetitions=1,
    )
