import pytest

from exasol.analytics.schema.column_types import (
    ColumnTypeSource,
    PyexasolMapping,
    PyexasolOption,
    SqlType,
)


@pytest.mark.parametrize(
    "spec, expected",
    [
        ("CHAR ASCII", SqlType("CHAR", [], "ASCII")),
        ("DECIMAL(10)", SqlType("DECIMAL", [10], "")),
        ("DECIMAL(10,2)", SqlType("DECIMAL", [10, 2], "")),
        ("HASHTYPE(10)", SqlType("HASHTYPE", [10], "")),
        ("HASHTYPE(8 BIT)", SqlType("HASHTYPE", [8], "BIT")),
        (
            "TIMESTAMP WITH LOCAL TIME ZONE",
            SqlType("TIMESTAMP", [], "WITH LOCAL TIME ZONE"),
        ),
        ("VARCHAR(10)", SqlType("VARCHAR", [10], "")),
    ],
)
def test_sql_type_from_string(spec, expected):
    assert SqlType.from_string(spec) == expected


def test_varchar_without_size_attribute():
    with pytest.raises(ValueError):
        SqlType.from_string("VARCHAR")
