from exasol_advanced_analytics_framework.event_handler.context.proxy.db_object_proxy import DBObjectProxy


class TableLikeProxy(DBObjectProxy):

    def __init__(self, name: str):
        super().__init__()
        self._name = name

    def name(self) -> str:
        self._check_if_valid()
        return self._name
