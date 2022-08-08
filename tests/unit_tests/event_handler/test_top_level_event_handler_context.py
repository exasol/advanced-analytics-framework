from exasol_advanced_analytics_framework.event_handler.context.top_level_event_handler_context import \
    TopLevelEventHandlerContext
from exasol_advanced_analytics_framework.event_handler.query.drop_table_query import DropTableQuery


def test_cleanup_invalid_object_proxies_after_release(
        top_level_event_handler_context: TopLevelEventHandlerContext):
    proxy = top_level_event_handler_context.get_temporary_table()
    proxy_name = proxy.name()
    top_level_event_handler_context.release()
    queries = top_level_event_handler_context.cleanup_invalid_object_proxies()
    assert len(queries) == 1 and isinstance(queries[0], DropTableQuery) \
           and queries[0].get_query_str() == f"DROP TABLE {proxy_name};"
