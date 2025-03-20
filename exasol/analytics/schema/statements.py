from __future__ import annotations

from typing import Any

from exasol.analytics.schema.column_name import ColumnName
from exasol.analytics.schema.values import quote_value


class UnknownColumnError(Exception):
    """
    In case of adding a value for a column not contained in the initial
    list of available columns.
    """


class InsertStatement:
    """
    Enables adding columns and values for creating an SQL INSERT statement.

    All available columns need to be specified up front when creating an
    instance of this class.

    Trying to add a value for a column missing in the initial list will raise
    an exception.

    Let's look at an example for an INSERT statement:

        INSERT INTO "T" ("C1", "C2", "C3", "C4")
        SELECT SYSTIMESTAMP(), 'Phase', SQ.B, SQ.A
        FROM VALUES (1)
        CROSS JOIN (SELECT "A", "B" FROM S.T2) as SQ

    Here we see 3 columns "C1", "C2", and "R" being inserted into table "T"
    while the values after `SELECT` have 3 different categories:

    * SYSTIMESTAMP() is an SQL scalar function that must not be quoted.
    * "Phase" is a string constant, that must be enclosed in single-quotes.
    * SQ.B and SQ.A are references to columns "B" and "A" in a subquery with the alias "SQ".

    Alias "SQ" is assigned to the subquery at the very end of the INSERT
    statement. Please note that the alias also needs to match the name of the
    column in the target table "T".

    columns = [
        ColumnName("C1"),
        ColumnName("C2"),
        ColumnName("C3"),
        ColumnName("C4"),
    ]
    insert_statement = (
        InsertStatement(columns)
        .add_scalar_functions({"C1": "SELECT SYSTIMESTAMP()"}
        .add_constants({"C2": "Phase"})
        .add_references({
          "C3": ColumnName("B", TableNameImpl("SQ")),
          "C4": ColumnName("A", TableNameImpl("SQ")),
        })

    Use properties `columns` and `values` to obtain comma-separated lists of
    all the columns and values, respectively with the columns being referred
    fully-qualified and the values properly quoted.
    """

    def __init__(self, columns: list[ColumnName], separator: str = ", "):
        self._lookup = {c.name: c for c in columns}
        self._separator = separator
        self._columns: list[ColumnName] = []
        self._values: list[str] = []

    def add_constants(self, values: dict[str, Any]) -> InsertStatement:
        return self._add(values, True)

    def add_scalar_functions(self, values: dict[str, str]) -> InsertStatement:
        return self._add(values, False)

    def add_references(self, values: dict[str, ColumnName]) -> InsertStatement:
        fully_qualified = {k: v.fully_qualified for k, v in values.items()}
        return self._add(fully_qualified, False)

    def _lookup_column(self, column_name: str) -> ColumnName:
        try:
            return self._lookup[column_name]
        except KeyError:
            raise UnknownColumnError(
                f'Can\'t add value for unknown column "{column_name}"'
            )

    def _add(self, values: dict[str, Any], quote_values: bool) -> InsertStatement:
        """
        Add a list of columns and values specified as dict to the
        statement.
        Columns are sorted by name and looked up in attribute `_lookup`.
        Values are quoted according to parameter `quote_values` and wrt. their
        data type.

        Setting `quote_values` to `False` is required when using SQL scalar
        functions, e.g. `CURRENT_SESSION` or `CURRENT_TIMESTAMP`.

        If adding values of different categories then method `add()` needs to
        be called multiple times, once for each category.
        """

        def col_val(name: str) -> str:
            val = values[name]
            if val is None or quote_values:
                return quote_value(val)
            return str(val)

        names = sorted(values)
        self._columns += [self._lookup_column(n) for n in names]
        self._values += [col_val(n) for n in names]
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
