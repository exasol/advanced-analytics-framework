from __future__ import annotations

from typing import Any

from exasol.analytics.schema.column import Column
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

        INSERT INTO "T" ("C1", "C2", "C3")
        SELECT SYSTIMESTAMP(), 'Phase', SQ.R
        FROM VALUES (1)
        CROSS JOIN (SELECT count(1) as R FROM S.T) as SQ

    Here we see 3 columns "C1", "C2", and "C3" being inserted into table "T"
    while the values after `SELECT` have 3 different categories:

    * SYSTIMESTAMP() is an SQL scalar function that must not be quoted.
    * "Phase" is a string constant, that must be enclosed in single-quotes.
    * SQ.R is a reference to column "R" in a subquery with the alias "SQ".

    The alias "R" can be found again inside the subquery `(SELECT count(1) as
    R FROM S.T)` as an alias for the `count(1)`. Alias "SQ" is assigned to the
    subquery at the very end of the INSERT statement.

    Use methods `add_constants()`, `add_scalar_functions()`, and
    `add_references()` to add constants, scalar functions, and reference to
    columns of the subquery, respectively.

    Each of the methods will add columns as well as values to the current
    instance of :class:`InsertStatement`.

    Use properties `columns` and `values` to obtain comma-separated lists of
    all the columns and values, respectively with the columns being referred
    fully-qualified and the values properly quoted.
    """

    def __init__(self, columns: list[Column], separator: str = ", "):
        self._lookup = {c.name.name: c.name for c in columns}
        self._separator = separator
        self._columns: list[ColumnName] = []
        self._values: list[str] = []

    def add_constants(self, values: dict[str, Any]) -> InsertStatement:
        return self._add(values, True)

    def add_scalar_functions(self, values: dict[str, str]) -> InsertStatement:
        return self._add(values, False)

    def add_references(self, *references: ColumnName) -> InsertStatement:
        """
        Add columns and values both obtained from `references` parameters.

        The values are then references into other database tables using the
        fully_qualified name of each column, optionally including a table name
        and a database schema.
        """
        self._columns += [self._lookup_column(ref.name) for ref in references]
        self._values += [ref.fully_qualified for ref in references]
        return self

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
