from exasol_data_science_utils_python.preprocessing.sql.schema.table_name import TableName

from exasol_advanced_analytics_framework.event_handler.context.proxy.db_object_proxy import DBObjectProxy


class TableLikeProxy(DBObjectProxy):

    def __init__(self, table_name: TableName):
        super().__init__()
        self._table_name = table_name

    def name(self) -> TableName:
        self._check_if_valid()
        return self._table_name
