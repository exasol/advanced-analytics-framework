from typing import List, Any, Iterable, Dict, Set

from exasol.analytics.query_handler.context.proxy.db_object_name_proxy import ObjectProxy


def find_object_proxies(obj: Any) -> List[ObjectProxy]:
    """
    This functions searches through the object tree of obj to find ObjectProxy objects.
    This functions can be used when you need to know which ObjectProxy objects are still in use.
    :param obj: Object to search
    :return: List of ObjectProxy objects
    """
    return _find_object_proxies(obj, set())


def _find_object_proxies(obj: Any, already_seen: Set[int]) -> List[ObjectProxy]:
    """
    This functions searches through the object tree of obj to find ObjectProxy objects
    and tracks the already seen object ids.
    :param obj: Object to search
    :param already_seen: a set of integers which records which object ids we already have seen
    :return: List of ObjectProxy objects
    """
    obj_identifier = id(obj)
    if obj_identifier in already_seen:
        return []
    else:
        already_seen.add(obj_identifier)
        if isinstance(obj, ObjectProxy):
            return [obj]
        elif isinstance(obj, Dict):
            return find_object_proxies_in_dict(obj, already_seen)
        elif isinstance(obj, Iterable) and not isinstance(obj, str):
            generator = (_find_object_proxies(o, already_seen) for o in obj)
            return sum(generator, [])  # Append all sublist
        elif hasattr(obj, "__dict__"):
            return find_object_proxies_in_dict(obj.__dict__, already_seen)
        else:
            return []


def find_object_proxies_in_dict(obj: Dict, already_seen: Set[int]) -> List[ObjectProxy]:
    # We can't create temporary lists for keys and values and then reuse the Iterable part of _find_object_proxies.
    # Because, the id of temporary lists could be reused and then we wouldn't trasverse all branches of the object graphs.
    result = []
    for key in obj.keys():
        result += _find_object_proxies(key, already_seen)
    for value in obj.values():
        result += _find_object_proxies(value, already_seen)
    return result
