from __future__ import annotations

from typing import Any

from exasol.analytics.schema.column import Column
from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.values import quote_value


class InsertStatement:
    """
    Columns and values for creating an SQL INSERT statement.
    """

    def __init__(self, columns: list[Column], separator: str = ", "):
        self._lookup = {c.name.name: c.name for c in columns}
        self._separator = separator
        self._columns: list[ColumnName] = []
        self._values: list[str] = []

    def add(self, values: dict[str, Any], quote_values: bool = True) -> InsertStatement:
        """
        Add a list of columns and values specified as dict to the
        statement.
        Columns are sorted by name and looked up in attribute `_lookup`.
        Values are quoted according to parameter `quote_values` and wrt. their
        data type.

        Setting `quote_values` to `False` is required when using SQL scalar
        functions, e.g. `CURRENT_SESSION` or `CURRENT_TIMESTAMP`.
        """

        def col_val(name: str) -> str:
            val = values[name]
            if val is None or quote_values:
                return quote_value(val)
            return str(val)

        names = sorted(values)
        self._columns += [self._lookup[n] for n in names]
        self._values += [col_val(n) for n in names]
        return self

    def add_references(self, *references: ColumnName) -> InsertStatement:
        """
        Add columns and values both obtained from `references` parameters.

        The values are then references into other database tables using the
        fully_qualified name of each column, optionally including a table name
        and a database schema.
        """
        self._columns += [self._lookup[ref.name] for ref in references]
        self._values += [ref.fully_qualified for ref in references]
        return self

    @property
    def columns(self) -> str:
        """
        List of fully_qualified column names, separated using the
        separator provided to the constructor.
        """
        return self._separator.join(c.fully_qualified for c in self._columns)

    @property
    def values(self) -> str:
        """
        List of (quoted) values, separated using the separator provided to
        the constructor.
        """
        return self._separator.join(self._values)
