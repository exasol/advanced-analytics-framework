from collections import OrderedDict
from typing import Union, Mapping, List

from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column
from exasol_data_science_utils_python.preprocessing.sql.schema.column_name import \
    ColumnName
from exasol_data_science_utils_python.preprocessing.sql.schema.column_type import \
    ColumnType

from exasol_advanced_analytics_framework.event_context.event_context_base \
    import EventContextBase


class UDFEventContext(EventContextBase):
    def __init__(self, ctx, exa, column_mapping: Mapping[str, str],
                 start_col: int = 0):
        super().__init__(ctx)
        self.start_col = start_col
        if not isinstance(column_mapping, OrderedDict):
            raise ValueError(
                f"column_mapping needs to be a OrderedDict, "
                f"got {type(column_mapping)}")
        self.column_mapping = column_mapping
        self.original_columns = list(self.column_mapping.keys())
        self.new_columns = list(self.column_mapping.values())
        self.__ctx = ctx
        self.exa = exa

    def _get_mapped_column(self, original_name: str) -> str:
        if original_name in self.column_mapping:
            return self.column_mapping[original_name]
        raise ValueError(
            f"Column {original_name} does not exists "
            f"in mapping {self.column_mapping}")

    def __getattr__(self, name):
        return self.__ctx[self._get_mapped_column(name)]

    def __next__(self):
        return self.__ctx.next()

    def rowcount(self) -> int:
        return self.__ctx.size()

    def fetch_as_dataframe(self, num_rows: Union[str, int], start_col: int = 0):
        df = self.__ctx.get_dataframe(num_rows, start_col=self.start_col)
        filtered_df = df[self.original_columns]
        filtered_df.columns = [self._get_mapped_column(column)
                               for column in filtered_df.columns]
        filtered_df_from_start_col = filtered_df.iloc[:, start_col:]
        return filtered_df_from_start_col

    def columns(self) -> List[Column]:
        query_columns: List[Column] = []
        for i in range(len(self.exa.meta.input_columns)):
            col_name = self.exa.meta.input_columns[i].name
            col_type = self.exa.meta.input_columns[i].sql_type
            query_columns.append(
                Column(ColumnName(col_name), ColumnType(col_type)))
        return query_columns

    def column_names(self) -> List[str]:
        column_names: List[str] = []
        for i in range(len(self.exa.meta.input_columns)):
            column_names.append(self.exa.meta.input_columns[i].name)
        return column_names


