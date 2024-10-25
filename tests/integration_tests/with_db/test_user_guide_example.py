import importlib.resources
import pytest
import re

from contextlib import ExitStack
from exasol_advanced_analytics_framework.deployment import constants
from exasol.python_extension_common.deployment.temp_schema import temp_schema


@pytest.fixture
def example_db_schemas(pyexasol_connection):
    with ExitStack() as stack:
        s1 = stack.enter_context(temp_schema(example_connection))
        s2 = stack.enter_context(temp_schema(example_connection))
        yield (s1, s2)


def test_user_guide_example(database_with_slc, pyexasol_connection, example_db_schemas):
    """
    This test verifies the adhoc implementation of a QueryHandler as shown
    in the AAF user guide.  The adhoc implementation dynamically creates its
    own python module.
    """
    bucketfs_connection_name, schema_name = database_with_slc
    dir = importlib.resources.files(constants.BASE_DIR) \
        / ".." / "doc" / "user_guide" / "example-udf-script"

    statement = (
        (dir / "create.sql")
        .read_text()
        .replace("EXAMPLE_SCHEMA", example_db_schemas[0])
    )
    pyexasol_connection.execute(statement)
    statement = (
        (dir / "execute.sql")
        .read_text()
        .replace("EXAMPLE_BFS_CON", bucketfs_connection_name)
        .replace("AAF_DB_SCHEMA", schema_name)
        .replace("EXAMPLE_SCHEMA", example_db_schemas[0])
        .replace("EXAMPLE_TEMP_SCHEMA", example_db_schemas[1])
    )
    result = pyexasol_connection.execute(statement).fetchall()
    expected = (
        "Final result: from query '.* table-insert bla-bla', 4"
        " and bucketfs: '.* bucketfs bla-bla'"
    )
    assert re.match(expected, result[0][0])
