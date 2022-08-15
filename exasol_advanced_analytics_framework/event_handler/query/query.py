import abc
from abc import abstractmethod


class Query(abc.ABC):
    @abstractmethod
    def get_query_str(self) -> str:
        pass
