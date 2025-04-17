import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    DecimalColumn,
    VarCharColumn,
    Table,
    TableNameImpl,
)


def test_valid():
    table = Table(
        TableNameImpl("table"),
        [
            DecimalColumn.simple("column1"),
            VarCharColumn.simple("column2", size=1),
        ],
    )


def test_no_columns_fail():
    with pytest.raises(ValueError, match="At least one column needed.") as c:
        table = Table(TableNameImpl("table"), [])


def test_duplicate_column_names_fail():
    with pytest.raises(ValueError, match="Column names are not unique.") as c:
        table = Table(
            TableNameImpl("table"),
            [
                DecimalColumn.simple("column"),
                VarCharColumn.simple("column", size=1),
            ],
        )


def test_set_new_name_fail():
    table = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    with pytest.raises(AttributeError) as c:
        table.name = "edf"


def test_set_new_columns_fail():
    table = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    with pytest.raises(AttributeError) as c:
        table.columns = [DecimalColumn.simple("column1")]


def test_wrong_types_in_constructor():
    with pytest.raises(TypeCheckError) as c:
        column = Table("abc", "INTEGER")


def test_columns_list_is_immutable():
    table = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    columns = table.columns
    columns.append(DecimalColumn.simple("column"))
    assert len(columns) == 2 and len(table.columns) == 1


def test_equality():
    table1 = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    table2 = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    assert table1 == table2


def test_inequality_name():
    table1 = Table(TableNameImpl("table1"), [DecimalColumn.simple("column")])
    table2 = Table(TableNameImpl("table2"), [DecimalColumn.simple("column")])
    assert table1 != table2


def test_inequality_columns():
    table1 = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    table2 = Table(TableNameImpl("table"), [
        DecimalColumn.simple("column"),
        DecimalColumn.simple("column2"),
    ])
    assert table1 != table2


def test_hash_equality():
    table1 = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    table2 = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    assert hash(table1) == hash(table2)


def test_hash_inequality_name():
    table1 = Table(TableNameImpl("table1"), [DecimalColumn.simple("column")])
    table2 = Table(TableNameImpl("table2"), [DecimalColumn.simple("column")])
    assert hash(table1) != hash(table2)


def test_hash_inequality_columns():
    table1 = Table(TableNameImpl("table"), [DecimalColumn.simple("column")])
    table2 = Table(TableNameImpl("table"), [
        DecimalColumn.simple("column"),
        DecimalColumn.simple("column2"),
    ])
    assert hash(table1) != hash(table2)
