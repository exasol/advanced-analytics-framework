from exasol.analytics.schema import UDFName


from exasol.analytics.query_handler.query.drop_query import DropQuery


class DropUDFQuery(DropQuery):

    def __init__(self, udf_name: UDFName):
        self._udf_name = udf_name

    @property
    def query_string(self) -> str:
        return f"DROP SCRIPT IF EXISTS {self._udf_name.fully_qualified};"

    @property
    def table_name(self) -> UDFName:
        return self._udf_name
