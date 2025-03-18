import pytest

from exasol.analytics.schema import (
    ConnectionObjectNameImpl,
    DBObjectName,
    SchemaName,
    TableLikeNameImpl,
)


@pytest.mark.parametrize(
    "object, expected_fully_qualified",
    [
        (ConnectionObjectNameImpl("C1"), '"C1"'),
        (TableLikeNameImpl("T1", SchemaName("SSS")), '"SSS"."T1"'),
        (TableLikeNameImpl("T2"), '"T2"'),
    ],
)
def test_fully_qualified(object: DBObjectName, expected_fully_qualified):
    assert object.fully_qualified == expected_fully_qualified
