import pytest
from typeguard import TypeCheckError

from exasol.analytics.schema import (
    View,
    ViewNameImpl,
    decimal_column,
    varchar_column,
)


def test_valid():
    table = View(
        ViewNameImpl("view_name"),
        [
            decimal_column("column1"),
            varchar_column("column2", size=20),
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
                decimal_column("column"),
                varchar_column("column", size=20),
            ],
        )


def test_set_new_name_fail():
    view = View(ViewNameImpl("view"), [decimal_column("column")])
    with pytest.raises(AttributeError) as c:
        view.name = "edf"


def test_set_new_columns_fail():
    view = View(ViewNameImpl("view"), [decimal_column("column")])
    with pytest.raises(AttributeError) as c:
        view.columns = [decimal_column("column1")]


def test_wrong_types_in_constructor():
    with pytest.raises(TypeCheckError) as c:
        column = View("abc", "INTEGER")


def test_columns_list_is_immutable():
    view = View(ViewNameImpl("view"), [decimal_column("column")])
    columns = view.columns
    columns.append(decimal_column("column"))
    assert len(columns) == 2 and len(view.columns) == 1


def test_equality():
    view1 = View(ViewNameImpl("view"), [decimal_column("column")])
    view2 = View(ViewNameImpl("view"), [decimal_column("column")])
    assert view1 == view2


def test_inequality_name():
    view1 = View(ViewNameImpl("view1"), [decimal_column("column")])
    view2 = View(ViewNameImpl("view2"), [decimal_column("column")])
    assert view1 != view2


def test_inequality_columns():
    view1 = View(ViewNameImpl("view"), [decimal_column("column")])
    view2 = View(
        ViewNameImpl("view"),
        [
            decimal_column("column"),
            decimal_column("column2"),
        ],
    )
    assert view1 != view2


def test_hash_equality():
    view1 = View(ViewNameImpl("view"), [decimal_column("column")])
    view2 = View(ViewNameImpl("view"), [decimal_column("column")])
    assert hash(view1) == hash(view2)


def test_hash_inequality_name():
    view1 = View(ViewNameImpl("view1"), [decimal_column("column")])
    view2 = View(ViewNameImpl("view2"), [decimal_column("column")])
    assert hash(view1) != hash(view2)


def test_hash_inequality_columns():
    view1 = View(ViewNameImpl("view"), [decimal_column("column")])
    view2 = View(
        ViewNameImpl("view"),
        [
            decimal_column("column"),
            decimal_column("column2"),
        ],
    )
    assert hash(view1) != hash(view2)
