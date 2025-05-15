from enum import (
    Enum,
    auto,
)

import pytest

from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependency
from exasol.analytics.query_handler.graph.stage.sql.execution.find_object_proxies import (
    find_object_proxies,
)
from exasol.analytics.schema import (
    Column,
    ColumnName,
    DecimalType,
    TableBuilder,
    TableName,
    View,
    ViewName,
    decimal_column,
)

BUCKETFS_LOCATION = "BUCKETFS_LOCATION"

VIEW_NAME = "VIEW_NAME"

TABLE_NAME = "TABLE_NAME"


class TestEnum(Enum):
    __test__ = False
    K1 = auto()
    K2 = auto()


@pytest.fixture(params=[TABLE_NAME, VIEW_NAME, BUCKETFS_LOCATION])
def object_proxy(top_level_query_handler_context_mock, request):
    if request.param == TABLE_NAME:
        return top_level_query_handler_context_mock.get_temporary_table_name()
    elif request.param == VIEW_NAME:
        return top_level_query_handler_context_mock.get_temporary_view_name()
    elif request.param == BUCKETFS_LOCATION:
        return top_level_query_handler_context_mock.get_temporary_bucketfs_location()
    else:
        raise ValueError(f"Unknown parameter value {request.param}")


def test_object_proxy(object_proxy):
    result = find_object_proxies(object_proxy)
    assert result == [object_proxy]


def test_object_proxy_in_list(object_proxy):
    result = find_object_proxies([object_proxy])
    assert result == [object_proxy]


def test_object_proxy_in_set(object_proxy):
    result = find_object_proxies({object_proxy})
    assert result == [object_proxy]


def test_object_proxy_in_tuple(object_proxy):
    result = find_object_proxies((object_proxy,))
    assert result == [object_proxy]


def test_object_proxy_in_dict(object_proxy):
    result = find_object_proxies({"test": object_proxy})
    assert result == [object_proxy]


def test_object_proxy_in_dependency_object(object_proxy):
    dependency = Dependency(object=object_proxy)
    result = find_object_proxies(dependency)
    assert result == [object_proxy]


def test_object_proxy_in_sub_dependency(object_proxy):
    dependency = Dependency(
        object="test", dependencies={TestEnum.K1: Dependency(object=object_proxy)}
    )
    result = find_object_proxies(dependency)
    assert result == [object_proxy]


def test_object_proxy_in_table(object_proxy):
    if not isinstance(object_proxy, TableName):
        pytest.skip()
    column = decimal_column("test")
    table = TableBuilder().with_name(object_proxy).with_columns([column]).build()
    result = find_object_proxies(table)
    assert result == [object_proxy]


def test_object_proxy_in_view(object_proxy):
    if not isinstance(object_proxy, ViewName):
        pytest.skip()
    column = decimal_column("test")
    view = View(name=object_proxy, columns=[column])
    result = find_object_proxies(view)
    assert result == [object_proxy]


def test_object_proxy_in_column_name(object_proxy):
    if not isinstance(object_proxy, TableName):
        pytest.skip()
    column_name = ColumnName("test", table_like_name=object_proxy)
    result = find_object_proxies(column_name)
    assert result == [object_proxy]


def test_object_proxy_in_column(object_proxy):
    if not isinstance(object_proxy, TableName):
        pytest.skip()
    column_name = ColumnName("test", table_like_name=object_proxy)
    column = Column(column_name, DecimalType())
    result = find_object_proxies(column)
    assert result == [object_proxy]


# TODO DataPartition, Dataset, SQLStageInputOutput, arbitrary object
