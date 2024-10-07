from abc import ABC

from exasol_data_science_utils_python.schema.dbobject_name_with_schema import DBObjectNameWithSchema


class TableLikeName(DBObjectNameWithSchema, ABC):
    """Abstract DBObjectName for table like objects, such as Tables and Views"""
