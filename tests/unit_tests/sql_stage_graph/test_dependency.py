from enum import Enum, auto

from exasol_machine_learning_library.execution.sql_stage_graph_execution.dependency import Dependency


class TestEnum(Enum):
    __test__ = False
    K1 = auto()
    K2 = auto()


def test_object():
    Dependency(object="test")


def test_object_none():
    Dependency(object=None)


def test_dependencies():
    Dependency(object="test", dependencies={TestEnum.K1: Dependency("dep")})
