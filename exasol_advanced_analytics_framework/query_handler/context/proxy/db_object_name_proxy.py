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

    @property
    def quoted_name(self) -> str:
        self._check_if_valid()
        return self._db_object_name.quoted_name

    @property
    def fully_qualified(self) -> str:
        self._check_if_valid()
        return self._db_object_name.fully_qualified

    def __eq__(self, other):
        """
        Compares the object id of this object and the others.
        We use the object ids, because we actually don't want
        to have two objects with the same name, because these
        object represent temporary DBObject which should be distinct
        from each other.
        """
        self._check_if_valid()
        return id(self) == id(other)

    def __repr__(self):
        return generate_repr_for_object(self)

    @abstractmethod
    def get_cleanup_query(self) -> Query:
        pass

    def __hash__(self):
        """
        Returns the hash (object id) of this proxy.
        We use the hash of the object id of this object,
        because we actually don't want to have two objects
        with the same name, because these object represent
        temporary DBObject which should be distinct from each other.
        Note: We need to implement this method,
        because without it, we get the error "unhashable type".
        The reason is likely the multiple inheritance of this object.
        """
        self._check_if_valid()
        return hash(id(self))
