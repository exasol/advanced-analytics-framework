import abc
from abc import abstractmethod

from exasol_data_science_utils_python.utils.repr_generation_for_object import generate_repr_for_object


class Query(abc.ABC):

    @property
    @abstractmethod
    def query_string(self) -> str:
        pass

    def __repr__(self):
        return generate_repr_for_object(self)
