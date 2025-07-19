from datetime import datetime

import pytest

from exasol.analytics.schema import quote_value


@pytest.mark.parametrize(
    "value, expected",
    [
        (1, "1"),
        (1.1, "1.1"),
        (None, "NULL"),
        (True, "TRUE"),
        (False, "FALSE"),
        ("abc", "'abc'"),
        (datetime(2022, 11, 30, 1, 59, 1), "'2022-11-30 01:59:01'"),
    ],
)
def test_quote_value(value, expected):
    assert quote_value(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        list(),
        dict(),
        set(),
        tuple(),
    ],
)
def test_quote_unsupported_datatype(value):
    with pytest.raises(ValueError):
        quote_value(value)
