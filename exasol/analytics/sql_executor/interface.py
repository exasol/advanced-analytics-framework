from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    List,
    Tuple,
)

from exasol.analytics.schema import Column


class ResultSet(ABC):
    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __next__(self) -> tuple:
        pass

    @abstractmethod
    def fetchone(self) -> tuple:
        pass

    @abstractmethod
    def fetchmany(self, size=None) -> list[tuple]:
        pass

    @abstractmethod
    def fetchall(self) -> list[tuple]:
        pass

    @abstractmethod
    def rowcount(self):
        pass

    @abstractmethod
    def columns(self) -> list[Column]:
        pass

    @abstractmethod
    def close(self):
        pass


class SQLExecutor(ABC):
    @abstractmethod
    def execute(self, sql: str) -> ResultSet:
        pass
