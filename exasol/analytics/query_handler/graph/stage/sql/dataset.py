from dataclasses import dataclass, field
from typing import List

from exasol.analytics.query_handler.graph.stage.sql.dependency import Dependencies
from exasol.analytics.schema import Column, TableLike


@dataclass(frozen=True)
class Dataset:
    """
    A Dataset consists of a TableLike, column lists indicating the
    identifier and other columns, and optional dependencies.

    The TableLike refers to a database table containing the actual data that
    can be used for instance in training or testing.

    Q. A TableLike is basically a list of columns and a name. Why do we have
    a separate list of columns here?
    """
    table_like: TableLike
    identifier_columns: List[Column]
    columns: List[Column]
    dependencies: Dependencies = field(default_factory=dict)
