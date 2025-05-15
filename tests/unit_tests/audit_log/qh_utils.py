from exasol.analytics.query_handler.query.interface import Query
from exasol.analytics.query_handler.query.select import SelectQueryWithColumnDefinition
from exasol.analytics.query_handler.result import Continue
from exasol.analytics.schema import decimal_column


def continue_action(query_list: list[Query]) -> Continue:
    input_query = SelectQueryWithColumnDefinition(
        "SELECT 1 as CONTINUE_INPUT_COLUMN",
        [decimal_column("CONTINUE_INPUT_COLUMN", precision=1, scale=0)],
    )
    return Continue(query_list=query_list, input_query=input_query)
