from typing import Hashable, Dict, Any, Iterable, Set


def generate_hash_for_object(obj: Any) -> int:
    return hash(tuple(_hash_object(v, set())
                      for k, v in sorted(obj.__dict__.items())))


def _hash_object(obj: Any, already_seen: Set[int]) -> int:
    """
    This function generates a hash value for the object using either the objects __hash__ method,
    or by inspecting it, if it is a list or dict. For non-hashable objects it returns 0.
    It uses the already_seen set to detect potential cycles. However, as a side effect
    it only counts an object once, also if it finds it a second time in another branch.
    This should be not a problem, here, because for computing the hash of an object
    we only need the set of child objects.
    """
    object_id = id(obj)
    if object_id in already_seen:
        return 0
    else:
        already_seen.add(object_id)
        if isinstance(obj, Hashable):
            return hash(obj)
        elif isinstance(obj, Dict):
            return \
                hash(
                    (
                        _hash_object(obj.keys(), already_seen),
                        _hash_object(obj.values(), already_seen)
                    )
                )
        elif isinstance(obj, Iterable):
            return hash(tuple(_hash_object(item, already_seen) for item in obj))
        else:
            return 0
