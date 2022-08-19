from abc import abstractmethod

from exasol_advanced_analytics_framework.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.query_handler.query.query import Query


class DBObjectProxy(ObjectProxy):
    def __init__(self, global_counter_value: int):
        super().__init__()
        self._global_counter_value = global_counter_value

    @abstractmethod
    def get_cleanup_query(self) -> Query:
        pass
