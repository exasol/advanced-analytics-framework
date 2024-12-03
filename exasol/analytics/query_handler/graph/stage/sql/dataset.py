from dataclasses import dataclass, field
from typing import List

from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependencies
from exasol.analytics.schema import Column, TableLike

@dataclass(frozen=True)
class Dataset:
    table_like: TableLike
    identifier_columns: List[Column]
    columns: List[Column]
    dependencies: Dependencies = field(default_factory=dict)
