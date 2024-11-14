import pytest

from exasol.analytics.schema import (
    Column,
    ColumnNameBuilder,
    ColumnType,
    TableNameImpl,
    Table,
)
from typeguard import TypeCheckError


def test_valid():
    table = Table(TableNameImpl("table"), [
        Column(ColumnNameBuilder.create("column1"), ColumnType("INTEGER")),
        Column(ColumnNameBuilder.create("column2"), ColumnType("VACHAR")),
    ])


def test_no_columns_fail():
    with pytest.raises(ValueError, match="At least one column needed.") as c:
        table = Table(TableNameImpl("table"), [])


def test_duplicate_column_names_fail():
    with pytest.raises(ValueError, match="Column names are not unique.") as c:
        table = Table(TableNameImpl("table"), [
            Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")),
            Column(ColumnNameBuilder.create("column"), ColumnType("VACHAR")),
        ])


def test_set_new_name_fail():
    table = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    with pytest.raises(AttributeError) as c:
        table.name = "edf"


def test_set_new_columns_fail():
    table = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    with pytest.raises(AttributeError) as c:
        table.columns = [Column(ColumnNameBuilder.create("column1"), ColumnType("INTEGER"))]


def test_wrong_types_in_constructor():
    with pytest.raises(TypeCheckError) as c:
        column = Table("abc", "INTEGER")


def test_columns_list_is_immutable():
    table = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    columns = table.columns
    columns.append(Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")))
    assert len(columns) == 2 and len(table.columns) == 1


def test_equality():
    table1 = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    table2 = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert table1 == table2


def test_inequality_name():
    table1 = Table(TableNameImpl("table1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    table2 = Table(TableNameImpl("table2"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert table1 != table2


def test_inequality_columns():
    table1 = Table(TableNameImpl("table1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    table2 = Table(TableNameImpl("table1"),
                   [
                       Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")),
                       Column(ColumnNameBuilder.create("column2"), ColumnType("INTEGER"))
                   ])
    assert table1 != table2


def test_hash_equality():
    table1 = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    table2 = Table(TableNameImpl("table"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert hash(table1) == hash(table2)


def test_hash_inequality_name():
    table1 = Table(TableNameImpl("table1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    table2 = Table(TableNameImpl("table2"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert hash(table1) != hash(table2)


def test_hash_inequality_columns():
    table1 = Table(TableNameImpl("table1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    table2 = Table(TableNameImpl("table1"),
                   [
                       Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")),
                       Column(ColumnNameBuilder.create("column2"), ColumnType("INTEGER"))
                   ])
    assert hash(table1) != hash(table2)
