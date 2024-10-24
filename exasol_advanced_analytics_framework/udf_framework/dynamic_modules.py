import sys
import importlib
from typing import Any
from types import ModuleType


def _create_module(name: str) -> ModuleType:
    spec = importlib.machinery.ModuleSpec(name, None)
    return importlib.util.module_from_spec(spec)


def _register_module_for_import(name: str, mod: ModuleType):
    sys.modules[name] = mod


def create_module(name: str) -> ModuleType:
    """
    Dynamically create a python module using the specified name and
    register the module in sys.modules[].

    Additionally add a function add_to_module() to the module enabling other
    code to add classes and functions to the module.
    """
    mod = sys.modules.get(name)
    if mod is None:
        mod = _create_module(name)
        _register_module_for_import(name, mod)

    def add_to_module(object: Any):
        object.__module__ = name
        setattr(mod, object.__name__, object)

    add_to_module(add_to_module)
    return mod
