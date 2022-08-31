from abc import abstractmethod
from typing import TypeVar, Generic

from exasol_data_science_utils_python.schema.dbobject_name import DBObjectName
from exasol_data_science_utils_python.utils.repr_generation_for_object import generate_repr_for_object

from exasol_advanced_analytics_framework.query_handler.context.proxy.object_proxy import ObjectProxy
from exasol_advanced_analytics_framework.query_handler.query.query import Query

NameType = TypeVar('NameType', bound=DBObjectName)


class DBObjectNameProxy(ObjectProxy, DBObjectName, Generic[NameType]):

    def __init__(self, db_object_name: NameType, global_counter_value: int):
        super().__init__()
        self._db_object_name = db_object_name
        self._global_counter_value = global_counter_value

    @property
    def name(self) -> str:
        self._check_if_valid()
        return self._db_object_name.name

    def quoted_name(self) -> str:
        self._check_if_valid()
        return self._db_object_name.quoted_name()

    def fully_qualified(self) -> str:
        self._check_if_valid()
        return self._db_object_name.fully_qualified()

    def __eq__(self, other):
        self._check_if_valid()
        return id(self) == id(other)

    def __repr__(self):
        return generate_repr_for_object(self)

    @abstractmethod
    def get_cleanup_query(self) -> Query:
        pass

    def __hash__(self):
        self._check_if_valid()
        return id(self)
