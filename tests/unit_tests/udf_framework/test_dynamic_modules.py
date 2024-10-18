from exasol_advanced_analytics_framework.udf_framework.dynamic_modules import create_module


class ExampleClass:
    pass


def example_function():
    return "example_function return value"


def test_create_module_with_class():
    mod = create_module("xx1")
    mod.add_to_module(ExampleClass)
    import xx1
    instance = xx1.ExampleClass()
    assert isinstance(instance, ExampleClass) and \
        ExampleClass.__module__ == "xx1"


def test_add_function():
    mod = create_module("xx2")
    mod.add_to_module(example_function)
    import xx2
    assert xx2.example_function() == "example_function return value" \
        and example_function.__module__ == "xx2"
