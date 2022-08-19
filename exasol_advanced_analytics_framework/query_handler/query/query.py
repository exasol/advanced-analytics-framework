import abc
from abc import abstractmethod


class Query(abc.ABC):

    @property
    @abstractmethod
    def query_string(self) -> str:
        pass
