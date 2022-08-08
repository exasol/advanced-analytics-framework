from abc import abstractmethod

from exasol_advanced_analytics_framework.event_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.event_handler.query.query import Query


class DBObjectProxy(ObjectProxy):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def get_cleanup_query(self) -> Query:
        pass
