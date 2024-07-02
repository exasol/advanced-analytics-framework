import pytest

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column import ColumnType
from exasol_data_science_utils_python.schema.column_name_builder import ColumnNameBuilder
from exasol_data_science_utils_python.schema.table import Table
from exasol_data_science_utils_python.schema.table_name_impl import TableNameImpl


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
    with pytest.raises(TypeError) as c:
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
