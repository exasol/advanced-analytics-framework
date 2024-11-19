from typing import Optional

from typeguard import typechecked

from exasol.analytics.schema import SchemaName, TableLikeNameImpl, ViewName


class ViewNameImpl(TableLikeNameImpl, ViewName):

    @typechecked
    def __init__(self, view_name: str, schema: Optional[SchemaName] = None):
        super().__init__(view_name, schema)
