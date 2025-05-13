import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    DecimalColumn,
    VarCharColumn,
    View,
    ViewNameImpl,
)


def test_valid():
    table = View(
        ViewNameImpl("view_name"),
        [
            DecimalColumn.simple("column1"),
            VarCharColumn.simple("column2", size=20),
        ],
    )


def test_no_columns_fail():
    with pytest.raises(ValueError, match="At least one column needed.") as c:
        table = View(ViewNameImpl("table"), [])


def test_duplicate_column_names_fail():
    with pytest.raises(ValueError, match="Column names are not unique.") as c:
        table = View(
            ViewNameImpl("view_name"),
            [
                DecimalColumn.simple("column"),
                VarCharColumn.simple("column", size=20),
            ],
        )


def test_set_new_name_fail():
    view = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    with pytest.raises(AttributeError) as c:
        view.name = "edf"


def test_set_new_columns_fail():
    view = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    with pytest.raises(AttributeError) as c:
        view.columns = [DecimalColumn.simple("column1")]


def test_wrong_types_in_constructor():
    with pytest.raises(TypeCheckError) as c:
        column = View("abc", "INTEGER")


def test_columns_list_is_immutable():
    view = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    columns = view.columns
    columns.append(DecimalColumn.simple("column"))
    assert len(columns) == 2 and len(view.columns) == 1


def test_equality():
    view1 = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    view2 = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    assert view1 == view2


def test_inequality_name():
    view1 = View(ViewNameImpl("view1"), [DecimalColumn.simple("column")])
    view2 = View(ViewNameImpl("view2"), [DecimalColumn.simple("column")])
    assert view1 != view2


def test_inequality_columns():
    view1 = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    view2 = View(
        ViewNameImpl("view"),
        [
            DecimalColumn.simple("column"),
            DecimalColumn.simple("column2"),
        ],
    )
    assert view1 != view2


def test_hash_equality():
    view1 = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    view2 = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    assert hash(view1) == hash(view2)


def test_hash_inequality_name():
    view1 = View(ViewNameImpl("view1"), [DecimalColumn.simple("column")])
    view2 = View(ViewNameImpl("view2"), [DecimalColumn.simple("column")])
    assert hash(view1) != hash(view2)


def test_hash_inequality_columns():
    view1 = View(ViewNameImpl("view"), [DecimalColumn.simple("column")])
    view2 = View(
        ViewNameImpl("view"),
        [
            DecimalColumn.simple("column"),
            DecimalColumn.simple("column2"),
        ],
    )
    assert hash(view1) != hash(view2)
