from abc import ABC, abstractmethod
from typing import List, Tuple

from exasol.analytics.schema import Column


class ResultSet(ABC):
    @abstractmethod
    def __iter__(self):
        pass

    @abstractmethod
    def __next__(self) -> Tuple:
        pass

    @abstractmethod
    def fetchone(self) -> Tuple:
        pass

    @abstractmethod
    def fetchmany(self, size=None) -> List[Tuple]:
        pass

    @abstractmethod
    def fetchall(self) -> List[Tuple]:
        pass

    @abstractmethod
    def rowcount(self):
        pass

    @abstractmethod
    def columns(self) -> List[Column]:
        pass

    @abstractmethod
    def close(self):
        pass


class SQLExecutor(ABC):
    @abstractmethod
    def execute(self, sql: str) -> ResultSet:
        pass
