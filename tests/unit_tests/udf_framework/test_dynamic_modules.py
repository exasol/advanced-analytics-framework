import pytest

from exasol.analytics.utils.dynamic_modules import (
    ModuleExistsException,
    create_module,
)


class ExampleClass:
    pass


def example_function():
    return "example_function return value"


def test_create_module_with_class():
    mod = create_module("xx1")
    mod.add_to_module(ExampleClass)
    import xx1

    instance = xx1.ExampleClass()
    assert isinstance(instance, ExampleClass) and ExampleClass.__module__ == "xx1"


def test_add_function():
    mod = create_module("xx2")
    import xx2

    xx2.add_to_module(example_function)
    assert (
        xx2.example_function() == "example_function return value"
        and example_function.__module__ == "xx2"
    )


def test_add_function_to_existing_module():
    create_module("xx3")
    with pytest.raises(
        ModuleExistsException, match='Module "xx3" already exists'
    ) as ex:
        create_module("xx3")
