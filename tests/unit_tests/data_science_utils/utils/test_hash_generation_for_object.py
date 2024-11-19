import dataclasses
from typing import Dict, List

from exasol.analytics.utils.hash_generation_for_object import generate_hash_for_object


class HashableTestObject:

    def __init__(self, i: int):
        self.i = i

    def __eq__(self, other):
        return isinstance(other, HashableTestObject) and self.i == other.i

    def __hash__(self):
        return self.i


@dataclasses.dataclass
class ObjectWithList:
    test: List[HashableTestObject]


def test_object_with_list_equal():
    test1 = ObjectWithList(test=[HashableTestObject(1), HashableTestObject(2)])
    test2 = ObjectWithList(test=[HashableTestObject(1), HashableTestObject(2)])
    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 == result2


def test_object_with_list_different_values_not_equal():
    test1 = ObjectWithList(
        test=[HashableTestObject(1), HashableTestObject(2), HashableTestObject(3)]
    )
    test2 = ObjectWithList(test=[HashableTestObject(1), HashableTestObject(2)])
    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 != result2


def test_object_with_list_different_order_not_equal():
    test1 = ObjectWithList(
        test=[HashableTestObject(1), HashableTestObject(2), HashableTestObject(3)]
    )
    test2 = ObjectWithList(
        test=[HashableTestObject(3), HashableTestObject(1), HashableTestObject(2)]
    )
    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 != result2


@dataclasses.dataclass
class ObjectWithMutlipleAttributes:
    test1: HashableTestObject
    test2: HashableTestObject


def test_object_with_multiple_attributes_equal():
    test1 = ObjectWithMutlipleAttributes(
        test1=HashableTestObject(1), test2=HashableTestObject(2)
    )
    test2 = ObjectWithMutlipleAttributes(
        test1=HashableTestObject(1), test2=HashableTestObject(2)
    )
    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 == result2


def test_object_with_multiple_attributes_not_equal():
    test1 = ObjectWithMutlipleAttributes(
        test1=HashableTestObject(1), test2=HashableTestObject(2)
    )
    test2 = ObjectWithMutlipleAttributes(
        test1=HashableTestObject(1), test2=HashableTestObject(3)
    )
    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 != result2


@dataclasses.dataclass
class ObjectWithDict:
    test: Dict[HashableTestObject, HashableTestObject]


def test_object_with_dict_equal():
    test1 = ObjectWithDict(
        test={
            HashableTestObject(1): HashableTestObject(1),
            HashableTestObject(2): HashableTestObject(2),
        }
    )
    test2 = ObjectWithDict(
        test={
            HashableTestObject(1): HashableTestObject(1),
            HashableTestObject(2): HashableTestObject(2),
        }
    )

    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 == result2


def test_object_with_dict_different_values_not_equal():
    test1 = ObjectWithDict(
        test={
            HashableTestObject(1): HashableTestObject(1),
            HashableTestObject(2): HashableTestObject(2),
        }
    )
    test2 = ObjectWithDict(test={HashableTestObject(1): HashableTestObject(1)})

    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 != result2


def test_object_with_dict_different_order_not_equal():
    test1 = ObjectWithDict(
        test={
            HashableTestObject(1): HashableTestObject(1),
            HashableTestObject(2): HashableTestObject(2),
        }
    )
    test2 = ObjectWithDict(
        test={
            HashableTestObject(2): HashableTestObject(2),
            HashableTestObject(1): HashableTestObject(1),
        }
    )

    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 != result2


class NotHashable:
    __hash__ = None

    def __init__(self, i: int):
        self.i = i


@dataclasses.dataclass
class ObjectWithNotHashable:
    test: NotHashable


def test_object_not_hashable_same_value_equal_hash():
    test1 = ObjectWithNotHashable(test=NotHashable(1))
    test2 = ObjectWithNotHashable(test=NotHashable(1))

    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 == result2


def test_object_not_hashable_different_values_equal_hash():
    test1 = ObjectWithNotHashable(test=NotHashable(1))
    test2 = ObjectWithNotHashable(test=NotHashable(2))

    result1 = generate_hash_for_object(test1)
    result2 = generate_hash_for_object(test2)
    assert result1 == result2


@dataclasses.dataclass
class ObjectWithCycle:
    cycle: List


def create_object_with_cycle(value: int) -> ObjectWithCycle:
    input_list = [HashableTestObject(value)]
    test = ObjectWithCycle(input_list)
    test.cycle.append(input_list)
    return test


def test_object_with_cycle_equal():
    result1 = generate_hash_for_object(create_object_with_cycle(1))
    result2 = generate_hash_for_object(create_object_with_cycle(1))
    assert result1 == result2


def test_object_with_cycle_not_equal():
    result1 = generate_hash_for_object(create_object_with_cycle(1))
    result2 = generate_hash_for_object(create_object_with_cycle(2))
    assert result1 != result2
