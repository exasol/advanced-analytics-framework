from enum import Enum, auto

from exasol.analytics.query_handler.graph.stage.sql.execution.dependency import Dependency


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
