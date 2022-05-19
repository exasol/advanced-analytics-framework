from abc import ABC, abstractmethod
from typing import Union, List
from exasol_data_science_utils_python.preprocessing.sql.schema.column import \
    Column


class EventContextBase(ABC):
    def __init__(self, ctx):
        self.__ctx = ctx

    @abstractmethod
    def __next__(self):
        pass

    @abstractmethod
    def rowcount(self) -> int:
        pass

    @abstractmethod
    def fetch_as_dataframe(
            self,  num_rows: Union[str, int], start_col: int = 0):
        pass

    @abstractmethod
    def columns(self) -> List[Column]:
        pass

    @abstractmethod
    def column_names(self) -> List[str]:
        pass
