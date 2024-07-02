import pytest

from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column import ColumnType
from exasol_data_science_utils_python.schema.column_name_builder import ColumnNameBuilder
from exasol_data_science_utils_python.schema.view import View
from exasol_data_science_utils_python.schema.view_name_impl import ViewNameImpl


def test_valid():
    table = View(ViewNameImpl("view_name"), [
        Column(ColumnNameBuilder.create("column1"), ColumnType("INTEGER")),
        Column(ColumnNameBuilder.create("column2"), ColumnType("VACHAR")),
    ])


def test_no_columns_fail():
    with pytest.raises(ValueError, match="At least one column needed.") as c:
        table = View(ViewNameImpl("table"), [])


def test_duplicate_column_names_fail():
    with pytest.raises(ValueError, match="Column names are not unique.") as c:
        table = View(ViewNameImpl("view_name"), [
            Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")),
            Column(ColumnNameBuilder.create("column"), ColumnType("VACHAR")),
        ])


def test_set_new_name_fail():
    view = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    with pytest.raises(AttributeError) as c:
        view.name = "edf"


def test_set_new_columns_fail():
    view = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    with pytest.raises(AttributeError) as c:
        view.columns = [Column(ColumnNameBuilder.create("column1"), ColumnType("INTEGER"))]


def test_wrong_types_in_constructor():
    with pytest.raises(TypeError) as c:
        column = View("abc", "INTEGER")


def test_columns_list_is_immutable():
    view = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    columns = view.columns
    columns.append(Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")))
    assert len(columns) == 2 and len(view.columns) == 1


def test_equality():
    view1 = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    view2 = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert view1 == view2


def test_inequality_name():
    view1 = View(ViewNameImpl("view1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    view2 = View(ViewNameImpl("view2"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert view1 != view2


def test_inequality_columns():
    view1 = View(ViewNameImpl("view1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    view2 = View(ViewNameImpl("view1"),
                 [
                     Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")),
                     Column(ColumnNameBuilder.create("column2"), ColumnType("INTEGER"))
                 ])
    assert view1 != view2


def test_hash_equality():
    view1 = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    view2 = View(ViewNameImpl("view"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert hash(view1) == hash(view2)


def test_hash_inequality_name():
    view1 = View(ViewNameImpl("view1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    view2 = View(ViewNameImpl("view2"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    assert hash(view1) != hash(view2)


def test_hash_inequality_columns():
    view1 = View(ViewNameImpl("view1"), [Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER"))])
    view2 = View(ViewNameImpl("view1"),
                 [
                     Column(ColumnNameBuilder.create("column"), ColumnType("INTEGER")),
                     Column(ColumnNameBuilder.create("column2"), ColumnType("INTEGER"))
                 ])
    assert hash(view1) != hash(view2)
