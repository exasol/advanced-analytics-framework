from collections import OrderedDict
from typing import Mapping, Optional, Union

import pandas as pd


class UDFContextWrapper:
    def __init__(self, ctx, column_mapping: Mapping[str, str], start_col: int = 0):
        self.start_col = start_col
        if not isinstance(column_mapping, OrderedDict):
            raise ValueError(
                f"column_mapping needs to be a OrderedDict, got {type(column_mapping)}"
            )
        self.column_mapping = column_mapping
        self.original_columns = list(self.column_mapping.keys())
        self.new_columns = list(self.column_mapping.values())
        self.ctx = ctx

    def _get_mapped_column(self, original_name: str) -> str:
        if original_name in self.column_mapping:
            return self.column_mapping[original_name]
        raise ValueError(
            f"Column {original_name} does not exists in mapping {self.column_mapping}"
        )

    def __getattr__(self, name):
        return self.ctx[self._get_mapped_column(name)]

    def get_dataframe(
        self, num_rows: Union[str, int], start_col: int = 0
    ) -> Optional[pd.DataFrame]:
        df = self.ctx.get_dataframe(num_rows, start_col=self.start_col)
        filtered_df = df[self.original_columns]
        filtered_df.columns = [
            self._get_mapped_column(column) for column in filtered_df.columns
        ]
        filtered_df_from_start_col = filtered_df.iloc[:, start_col:]
        return filtered_df_from_start_col

    def next(self, reset: bool = False) -> bool:
        return self.ctx.next(reset)

    def size(self) -> int:
        return self.ctx.size()

    def reset(self):
        self.ctx.reset()

    def emit(self, *args):
        self.ctx.emits(*args)
