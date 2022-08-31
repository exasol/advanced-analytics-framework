from exasol_advanced_analytics_framework.query_handler.query_handler import QueryHandler


class UDFQueryHandler(QueryHandler[str, str]):
    """Abstract class for QueryHandlers used in QueryHandlerRunnerUDF"""
