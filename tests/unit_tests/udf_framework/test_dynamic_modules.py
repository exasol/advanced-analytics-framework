from exasol_advanced_analytics_framework.udf_framework.dynamic_modules import create_module

class ExampleClass:
    pass


def test_create_module():
    new_module = create_module("xyz", [ExampleClass])
    instance = new_module.ExampleClass()
    assert isinstance(instance, ExampleClass)
