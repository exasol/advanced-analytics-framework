import abc
from abc import abstractmethod

from exasol.analytics.utils.repr_generation_for_object import generate_repr_for_object


class Query(abc.ABC):

    @property
    @abstractmethod
    def query_string(self) -> str:
        pass

    def __repr__(self):
        return generate_repr_for_object(self)

    @property
    def audit(self) -> bool:
        return False
