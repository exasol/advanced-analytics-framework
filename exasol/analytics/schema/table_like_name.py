from abc import ABC

from exasol.analytics.schema.dbobject_name_with_schema import DBObjectNameWithSchema


class TableLikeName(DBObjectNameWithSchema, ABC):
    """Abstract DBObjectName for table like objects, such as Tables and Views"""
