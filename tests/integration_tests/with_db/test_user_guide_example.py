import re
from exasol_advanced_analytics_framework.example \
    import generator as example_generator


def script_args(bfs_connection_name: str, schema_name: str):
    args = dict(example_generator.SCRIPT_ARGUMENTS)
    args["query_handler"]["udf"]["schema"] = schema_name
    args["temporary_output"]["bucketfs_location"]["connection_name"] = bfs_connection_name
    args["temporary_output"]["schema_name"] = schema_name
    return args


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
