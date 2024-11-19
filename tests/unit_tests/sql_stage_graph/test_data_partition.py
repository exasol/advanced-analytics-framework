from enum import Enum, auto

import pytest

from exasol.analytics.query_handler.graph.stage.sql.data_partition import DataPartition
from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependency
from exasol.analytics.schema import (
    Column,
    ColumnName,
    ColumnType,
    SchemaName,
    Table,
    TableNameBuilder,
    View,
    ViewNameBuilder,
)


class TestEnum(Enum):
    __test__ = False
    K1 = auto()
    K2 = auto()


@pytest.fixture
def table():
    table = Table(
        TableNameBuilder.create("table", SchemaName("TEST_SCHEMA")),
        columns=[Column(ColumnName("x1"), ColumnType("INTEGER"))],
    )
    return table


def test_with_table(table):
    DataPartition(table_like=table)


@pytest.fixture()
def view():
    view = View(
        ViewNameBuilder.create("view", SchemaName("TEST_SCHEMA")),
        columns=[Column(ColumnName("x1"), ColumnType("INTEGER"))],
    )
    return view


def test_with_view(view):
    DataPartition(table_like=view)


def test_dependencies(table, view):
    view = View(
        ViewNameBuilder.create("view", SchemaName("TEST_SCHEMA")),
        columns=[Column(ColumnName("x1"), ColumnType("INTEGER"))],
    )
    DataPartition(
        table_like=view,
        dependencies={TestEnum.K1: Dependency(DataPartition(table_like=table))},
    )
