import re
import pytest
from exasol_advanced_analytics_framework.example \
    import generator as example_generator


def script_args(bfs_connection_name: str, schema_name: str):
    args = dict(example_generator.SCRIPT_ARGUMENTS)
    args["query_handler"]["udf"]["schema"] = schema_name
    args["temporary_output"]["bucketfs_location"]["connection_name"] = bfs_connection_name
    args["temporary_output"]["schema_name"] = schema_name
    return args


import pyexasol
@pytest.mark.skip("local")
def test_x2():
    pyexasol_connection = pyexasol.connect(
        dsn="192.168.124.221:8563",
        user="SYS",
        password="exasol",
    )
    bucketfs_connection_name, schema_name = ("BFS_CON", "MY_SCHEMA")
    args = script_args(bucketfs_connection_name, schema_name)
    statement = example_generator.create_script(args)
    # print(f'create_script:\n{statement}')
    pyexasol_connection.execute(statement)
    statement = example_generator.execute_script(args)
    # print(f'execute_script:\n{statement}')
    result = pyexasol_connection.execute(statement).fetchall()
    print(f'{result}')
    expected = (
        "Final result: from query '.* table-insert bla-bla', 4"
        " and bucketfs: '.* bucketfs bla-bla'"
    )
    assert re.match(expected, result[0][0])


def test_user_guide_example(database_with_slc, pyexasol_connection):
    """
    This test verifies the adhoc implementation of a QueryHandler as shown
    in the AAF user guide.  The adhoc implementation dynamically creates its
    own python module.
    """
    bucketfs_connection_name, schema_name = database_with_slc
    args = script_args(bucketfs_connection_name, schema_name)
    statement = example_generator.create_script(args)
    pyexasol_connection.execute(statement)
    statement = example_generator.execute_script(args)
    result = pyexasol_connection.execute(statement).fetchall()
    expected = (
        "Final result: from query '.* table-insert bla-bla', 4"
        " and bucketfs: '.* bucketfs bla-bla'"
    )
    assert re.match(expected, result[0][0])
