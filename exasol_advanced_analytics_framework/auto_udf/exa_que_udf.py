from .exa_que import ExaQueBase


class ExaQue(ExaQueBase):
    def __init__(self, ctx, scalar, row_format, group_by=None,
                 first_group_index=0, first_input_index=0) -> None:
        self._ctx = ctx
        self._scalar = scalar
        self._row_format = tuple(row_format)
        if (group_by is None) or isinstance(group_by, str):
            self._group_by = group_by
        elif len(group_by) == 1:
            self._group_by = group_by[0]
        else:
            self._group_by = tuple(group_by)
        self._grouped = False
        self._first_group_index = first_group_index
        self._first_input_index = first_input_index
        self._n_cols = len(row_format)
        self._beyond_last_row = False

    @property
    def row_format(self):
        return self._row_format

    @property
    def group_by(self):
        return None if self._grouped else self._group_by

    @property
    def grouped(self) -> bool:
        return self._grouped

    def group(self) -> None:
        if (not self._group_by) or self._grouped:
            raise RuntimeError(f'Cannot group an {ExaQue.__name__} object where the '
                               'group_by property is not defined')
        if isinstance(self._group_by, str):
            self._row_format = (self._group_by, self._row_format)
        else:
            grouped_format = list(self._group_by)
            grouped_format.append(self._row_format)
            self._row_format = tuple(grouped_format)

        self._grouped = True

    def _read_context_range(self, first_index, num_inputs):
        return tuple(self._ctx[i] for i in range(first_index, first_index + num_inputs))

    def _read_row(self):
        return self._read_context_range(self._first_input_index, self._n_cols)

    def _read_group(self):
        if self._group_by is None:
            return None
        elif isinstance(self._group_by, str):
            return self._ctx[self._first_group_index]
        return self._read_context_range(self._first_group_index, len(self._group_by))

    def _read_group_row(self):
        return self._read_group(), self._read_row()

    def __iter__(self):
        return self

    def __next__(self):
        group_id, row = self.fetch()
        if row is None:
            raise StopIteration
        return group_id, row

    def _fetch_many(self, num_rows=-1):
        rows = []
        row_count = 0
        read_func = self._read_row if self._grouped else self._read_group_row
        while (not self._beyond_last_row) and (row_count != num_rows):
            rows.append(read_func())
            row_count += 1
            self._beyond_last_row = self._scalar or (not self._ctx.next())
        return rows

    def _fetch_group(self):
        group = self._read_group()
        row = [group] if isinstance(self._group_by, str) else list(group)
        row.append(self._fetch_many())
        return tuple(row)

    def fetch(self):
        if self._beyond_last_row:
            return None, None
        elif self._grouped:
            return None, self._fetch_group()
        row = self._read_group_row()
        self._beyond_last_row = self._scalar or (not self._ctx.next())
        return row

    def fetch_many(self, num_rows=1):
        if self._grouped:
            return [(None, self._fetch_group())]
        elif num_rows == 'all':
            return self._fetch_many()
        elif isinstance(num_rows, int) and (num_rows > 0):
            return self._fetch_many(num_rows)
        return []

    def emit(self, *args):
        if self._grouped:
            raise RuntimeError(f'Emitting to a grouped {ExaQue.__name__} object is not allowed.')
        elif len(args) != self._n_cols:
            raise ValueError(f'Trying to emit a row with {len(args)} elements, '
                             f'while {self._n_cols} are expected.')
        self._ctx.emit(*args)
