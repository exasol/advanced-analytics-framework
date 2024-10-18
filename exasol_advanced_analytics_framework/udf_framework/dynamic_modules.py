import sys
import importlib
from typing import Any
from types import ModuleType


def create_module(name: str) -> ModuleType:
    """
    Dynamically create a python module using the specified name and
    register the module in sys.modules[].

    Additionally add a function add_to_module() to the module enabling other
    code to add classes and functions to the module.
    """
    spec = importlib.machinery.ModuleSpec(name, None)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod

    def add_to_module(object: Any):
        object.__module__ = name
        setattr(mod, object.__name__, object)

    add_to_module(add_to_module)
    return mod
