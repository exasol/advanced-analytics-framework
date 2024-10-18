import importlib
from typing import Any, List


def _new_module(mod_name):
    spec = importlib.machinery.ModuleSpec(mod_name, None)
    return importlib.util.module_from_spec(spec)


def create_module(name: str, objects: List[Any]):
    """
    Dynamically create a python module using the specified name and add
    the specified objects to the new module. Each object may be a class,
    variable, or function.
    """
    mod = _new_module(name)
    for obj in objects:
        setattr(mod, obj.__name__, obj)
    return mod
