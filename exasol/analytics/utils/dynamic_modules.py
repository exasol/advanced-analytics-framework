import sys
from importlib.machinery import ModuleSpec
from importlib.util import module_from_spec
from types import ModuleType
from typing import Any


def _create_module(name: str) -> ModuleType:
    spec = ModuleSpec(name, None)
    return module_from_spec(spec)


def _register_module_for_import(name: str, mod: ModuleType):
    sys.modules[name] = mod


class ModuleExistsException(Exception):
    """
    When trying create a module that already exists.
    """


def create_module(name: str) -> ModuleType:
    """
    Dynamically create a python module using the specified name and
    register the module in sys.modules[] for import.

    Additionally add a function add_to_module() to the module enabling other
    code to add classes and functions to the module.
    """
    if name in sys.modules:
        raise ModuleExistsException(f'Module "{name}" already exists')

    mod = _create_module(name)
    _register_module_for_import(name, mod)

    def add_to_module(object: Any):
        object.__module__ = name
        setattr(mod, object.__name__, object)

    add_to_module(add_to_module)
    return mod
