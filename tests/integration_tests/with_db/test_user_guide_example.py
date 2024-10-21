import re
import pytest
from exasol_advanced_analytics_framework.example \
    import generator as example_generator


def generate_example(bfs_connection_name: str, schema_name: str):
    script = dict(example_generator.QUERY_HANDLER_SCRIPT)
    script["query_handler"]["udf"]["schema"] = schema_name
    script["temporary_output"]["bucketfs_location"]["connection_name"] = bfs_connection_name
    script["temporary_output"]["schema_name"] = schema_name
    return example_generator.generate(script)


def test_x1(request):
    # opt = request.config.getoption("--exasol-host")
    # print(f'{opt}')
    # return
    example_code = generate_example("BBB", "SSS")
    print(f'{example_code}')
    result = [[(
        "Final result: from query"
        " '2024-10-21 12:26:00 table-insert bla-bla', 4"
        " and bucketfs: '2024-10-21 12:26:00 bucketfs bla-bla'"
    )]]
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
    example_code = generate_example(bucketfs_connection_name, schema_name)
    result = pyexasol_connection.execute(example_code).fetchall()
    expected = (
        "Final result: from query '.* table-insert bla-bla', 4"
        " and bucketfs: '.* bucketfs bla-bla'"
    )
    assert re.match(expected, result[0][0])
