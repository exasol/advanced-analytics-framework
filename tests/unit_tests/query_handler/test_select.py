from exasol.analytics.query_handler.query.select import (
    SelectQueryWithColumnDefinition,
    AuditQuery,
    ModifyQuery
)
from exasol.analytics.schema import (
    TableLikeNameImpl,
    DBObjectType,
    DBOperationType
)


def test_audit_query():
    query = 'my_query'
    ad_query = AuditQuery(SelectQueryWithColumnDefinition(query, []))
    assert ad_query.query_string == query


def test_modify_query():
    query = 'my_query'
    table_name = 'my_table'
    mod_query = ModifyQuery(query,
                            db_object_name=TableLikeNameImpl(table_name),
                            db_object_type=DBObjectType.TABLE.name,
                            db_operation_type=DBOperationType.CREATE.name)
    assert mod_query.query_string == query
    assert mod_query.db_object_name.name == table_name
    assert mod_query.db_object_type == DBObjectType.TABLE
    assert mod_query.db_operation_type == DBOperationType.CREATE
