from enum import Enum, auto

import pytest
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType
from exasol_data_science_utils_python.schema.schema_name import SchemaName
from exasol_data_science_utils_python.schema.table import Table
from exasol_data_science_utils_python.schema.table_name_builder import TableNameBuilder
from exasol_data_science_utils_python.schema.view import View
from exasol_data_science_utils_python.schema.view_name_builder import ViewNameBuilder

from exasol_machine_learning_library.execution.sql_stage_graph_execution.data_partition import DataPartition
from exasol_machine_learning_library.execution.sql_stage_graph_execution.dependency import Dependency


class TestEnum(Enum):
    K1 = auto()
    K2 = auto()


@pytest.fixture
def table():
    table = Table(
        TableNameBuilder.create(
            "table", SchemaName("TEST_SCHEMA")),
        columns=[Column(ColumnName("x1"), ColumnType("INTEGER"))])
    return table


def test_with_table(table):
    DataPartition(table_like=table)


@pytest.fixture()
def view():
    view = View(
        ViewNameBuilder.create(
            "view", SchemaName("TEST_SCHEMA")),
        columns=[Column(ColumnName("x1"), ColumnType("INTEGER"))])
    return view


def test_with_view(view):
    DataPartition(table_like=view)


def test_dependencies(table, view):
    view = View(
        ViewNameBuilder.create(
            "view", SchemaName("TEST_SCHEMA")),
        columns=[Column(ColumnName("x1"), ColumnType("INTEGER"))])
    DataPartition(table_like=view,
                  dependencies={TestEnum.K1: Dependency(
                      DataPartition(table_like=table))})
