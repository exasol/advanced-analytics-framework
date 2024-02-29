from typing import Any
from collections.abc import Sequence
from itertools import chain
from abc import ABC, abstractmethod
from collections import deque, defaultdict
from pyexasol import ExaStatement


ExaRowFormat = tuple[str | tuple[str], ...]
ExaGroupFormat = str | tuple[str, ...] | None
ExaRow = tuple | None
ExaGroup = Any | None
ExaGroupRow = tuple[ExaGroup, ExaRow]


class ExaQueBase(ABC):

    @abstractmethod
    def __iter__(self):
        """Rows iterator"""

    @property
    @abstractmethod
    def row_format(self) -> ExaRowFormat:
        """
        Columns definition in the form ("column-name[:type]", ...). When the object is
        grouped this becomes ("group-name[:type]", ..., original-columns-definition).
        """

    @property
    @abstractmethod
    def group_by(self) -> ExaGroupFormat:
        """
        Grouping definition, that is either "group_name[:type]" in case of grouping by
        a single column, or ("group_name[:type]", ...) in case of grouping by multiple
        columns. None if grouping is not defined.

        When the object gets grouped, the grouping definition moves into the column
        definition and the group_by property becomes None.

        The grouping definition should be provided in the constructor. It cannot be changed
        other than becoming None after the object gets grouped.
        """

    @property
    @abstractmethod
    def grouped(self) -> bool:
        """An object can be in either grouped or ungrouped state."""

    @abstractmethod
    def group(self) -> None:
        """
        Groups the input data if the grouping is defined. The rows of a grouped object
        consist of the group values and a list of rows that belong to the group. For example,
        suppose the column definition was ["C1", "C2", "C3] and the grouping definition was
        ["G1", "G2"]. After grouping, the rows will look like this:
        (G1_value, G2_value, [(C1_value, C2_value, C3_value), ...]).

        Grouping is one way to enforce a compartmentalised processing of the data, that is
        required for making a SET UDF from a function. Well, there is no real enforcement
        here. It is still possible to make some kind of amalgamation of multiple groups.
        But in all fairness, in this case it can be considered an act of sabotage.

        It should be noted that grouping is not necessarily the most efficient way of
        compartmentalised data processing. It requires loading the whole dataset. In some
        cases it might be better to work with ungrouped object and maintain a state for
        each group. A trivial example is computing the group mean for some value. In this
        case we just need to keep the cumulative sum and the count.

        Logically, the grouping could have been done repeatedly, every time with a new
        "group_by". However, this would complicate the matter on the UDF side. So, for now
        at least, grouping can be done only once, using the pre-arranged "group_by".
        """

    @abstractmethod
    def fetch(self) -> ExaGroupRow:
        """
        Returns the next row in the form (group, data). If the group_by property is None,
        the returned tuple is (None, data).In case there are no more rows left the call
        returns (None, None).

        For an ungrouped object the data is a tuple in the form (C1_value, ...).
        For a grouped object the data is a tuple in the form (G1_value, ..., [(C1_value, ...), ...]).
        The group is either None, in case group_by is None, or G_value or (G1_value, ...).

        TODO: Implement the data type conversion to ensure the types of the output data match
        the types in the column definition and group definition if specified there.
        """

    @abstractmethod
    def fetch_many(self, num_rows: str | int = 1) -> list[ExaGroupRow]:
        """Returns multiple rows in a list."""

    @abstractmethod
    def emit(self, *args) -> None:
        """Emits one row of data. Not allowed if the object is grouped."""


def split_column_def(column_def: str) -> tuple[str, str | None]:
    if ':' in column_def:
        column_name, column_type = column_def.split(':', maxsplit=1)
    else:
        column_name, column_type = column_def, None
    return column_name, column_type


class ExaQue(ExaQueBase):

    def __init__(self, row_format: Sequence[str], group_by: str | Sequence[str] | None = None,
                 statement: ExaStatement | None = None) -> None:

        self._row_format = tuple(row_format)
        if (group_by is None) or isinstance(group_by, str):
            self._group_by = group_by
        elif len(group_by) == 1:
            self._group_by = group_by[0]
        else:
            self._group_by = tuple(group_by)
        self._statement = statement
        self._grouped = False
        self._columns = [split_column_def(column_def)[0] for column_def in row_format]
        self._n_cols = len(self._columns)
        self._buffer: deque[tuple] = deque()

    @property
    def row_format(self) -> ExaRowFormat:
        return self._row_format

    @property
    def group_by(self) -> ExaGroupFormat:
        return self._group_by

    @property
    def grouped(self):
        return self._grouped

    def _group_row_format(self) -> None:
        if not self._group_by:
            raise RuntimeError(f'Cannot group an {ExaQue.__name__} object where the '
                               'group_by property is not defined')
        if isinstance(self._group_by, str):
            self._row_format = (self._group_by, self._row_format)
        else:
            self._row_format = tuple(chain(self._group_by), [self._row_format])

        self._columns = self._row_format
        self._n_cols = len(self._row_format)
        self._group_by = None

    def group(self) -> None:

        group_dic = defaultdict(list)
        for group, row in self.fetch_many('all'):
            group_dic[group].append(row)

        group_is_str = isinstance(self._group_by, str)
        self._group_row_format()

        for group, row_list in group_dic.items():
            if group_is_str:
                grouped_row = (group, row_list)
            else:
                grouped_row = tuple(chain(group, [row_list]))
            self.emit(*grouped_row)

        self._grouped = True

    def _get_group_for_row(self, row: dict[str, Any]) -> ExaGroup:
        if not self._group_by:
            return None
        elif isinstance(self._group_by, str):
            return row[self._group_by]
        else:
            return tuple(row[col] for col in self._group_by)

    def _map_statement_row(self, row: tuple | dict[str, Any]) -> ExaGroupRow:
        if not isinstance(row, dict):
            row = {col: val for col, val in zip(self._statement.col_names, row)}
        return self._get_group_for_row(row), tuple(row[col] for col in self._columns)

    def _map_buffer_row(self, row: tuple) -> ExaGroupRow:
        if self._grouped:
            group = None
        else:
            row_dict = {col: val for col, val in zip(self._columns, row)}
            group = self._get_group_for_row(row_dict)
        return group, row

    def __iter__(self):
        return self

    def __next__(self) -> ExaGroupRow:
        group_id, row = self.fetch()
        if row is None:
            raise StopIteration
        return group_id, row

    def _fetch_from_statement(self) -> ExaGroupRow:
        row = next(self._statement)
        if row is None:
            return None, None
        return self._map_statement_row(row)

    def _fetch_from_buffer(self) -> ExaGroupRow:
        if len(self._buffer) > 0:
            return self._map_buffer_row(self._buffer.pop())
        return None, None

    def _fetch_many_from_statement(self, num_rows: str | int) -> list[ExaGroupRow]:
        if num_rows == 'all':
            return list(map(self._map_statement_row, self._statement.fetchall()))
        elif isinstance(num_rows, int) and num_rows > 0:
            return list(map(self._map_statement_row, self._statement.fetchmany(num_rows)))
        else:
            return []

    def _fetch_many_from_buffer(self, num_rows: str | int) -> list[ExaGroupRow]:
        if num_rows == 'all':
            num_rows = len(self._buffer)
        elif isinstance(num_rows, int) and num_rows > 0:
            num_rows = min(len(self._buffer), num_rows)
        else:
            return []
        return [self._map_buffer_row(self._buffer.pop()) for _ in range(num_rows)]

    def fetch(self) -> ExaGroupRow:
        if self._grouped or (self._statement is None):
            return self._fetch_from_buffer()
        else:
            return self._fetch_from_statement()

    def fetch_many(self, num_rows: str | int = 1) -> list[ExaGroupRow]:
        if self._grouped or (self._statement is None):
            return self._fetch_many_from_buffer(num_rows)
        else:
            return self._fetch_many_from_statement(num_rows)

    def emit(self, *args) -> None:
        if self._grouped:
            raise RuntimeError(f'Emitting to a grouped {ExaQue.__name__} object is not allowed.')
        elif len(args) != self._n_cols:
            raise ValueError(f'Trying to emit a row with {len(args)} elements, '
                             f'while {self._n_cols} are expected.')
        self._buffer.appendleft(tuple(args))
