from typing import (
    Any,
    Tuple,
    Type,
    TypeVar,
)


def _init(self):
    pass


def _setattr(self, key: str, value: Any):
    """
    With this __setattr__ implementation, we allow only to set attributes which are defined as class attributes
    and are available in self.__annotations__. Furthermore, an attributes can only be set once.
    If an attribute, already exists this functions raises a AttributeError.
    """
    if key not in self.__annotations__ and key != "result_id":
        raise AttributeError(
            f"Attribute '{key}' is not defined for class '{self.__class__.__name__}'."
        )
    if hasattr(self, key):
        raise AttributeError(f"Attribute '{key}' is already set.")
    object.__setattr__(self, key, value)


def _delattr(self, key: str):
    """
    With this __delattr__ implementation, we disallow the deletion of attributes.
    If an attribute, already exists this functions raises a AttributeError.
    """
    if key not in self.__annotations__ and key != "result_id":
        raise AttributeError(
            f"Attribute '{key}' is not defined for class '{self.__class__.__name__}'."
        )
    else:
        raise AttributeError(f"Attribute '{key}' cannnot be deleted.")


_T = TypeVar("_T")


def _new(cls: type[_T], parent_cls: type):
    """
    Create a new object from cls which is a subclass of parent_cls.
    where a unique id is set for each object regardless of the specific type.
    Furthermore, it checks if  __setattr__ and __init__ are set to _setattr and _init and otherwise raises TypeError.
    :param cls: Type of the subclass of the Result class
    :param parent_cls: Type of the Result class
    :return: Object of the type cls
    """
    if cls.__setattr__ != _setattr:
        raise TypeError(f"No custom __setattr__ allowed. Got {cls.__setattr__}.")
    if cls.__delattr__ != _delattr:
        raise TypeError(f"No custom __delattr__ allowed. Got {cls.__delattr__}.")
    if cls.__init__ != object.__init__ and cls.__init__ != _init:
        raise TypeError(f"No custom constructors allowed. Got {cls.__init__}.")
    result_object = super(parent_cls, cls).__new__(cls)  # type: ignore
    result_id = _Meta._result_counter.next()
    setattr(result_object, "result_id", result_id)
    return result_object


class _ResultCounter:
    _current_result_id: int = 0

    def next(self) -> int:
        result = self._current_result_id
        self._current_result_id += 1
        return result


class _Meta(type):
    _result_counter = _ResultCounter()

    def __new__(cls, name: str, bases: tuple[type, ...], attrs: Any):
        """
        Create a new type based on the parameters and overwrites the functions __new__, __init__ and __setattr__.
        This function is calles for types that declare this class as their metaclass.
        :param name:
        :param bases:
        :param attrs:
        """
        result_type = type(name, bases, attrs)

        def _configured_new(cls: type[_T]):
            """This function is called for subclasses of classes that declare _Meta as their metaclass."""
            return _new(cls, result_type)

        result_type.__new__ = _configured_new  # type: ignore
        result_type.__init__ = _init  # type: ignore
        result_type.__setattr__ = _setattr  # type: ignore
        result_type.__delattr__ = _delattr  # type: ignore
        return result_type


class Result(metaclass=_Meta):
    """
    This class implements the base class for all Result classes. Attributes can only be set once and are then frozen.
    This behavior guarantees that different components that compute different part of the result don't
    overwrite already existing information. This is important, because result objcts might get serialized and
    then used by multiple different processes compute some parts of the result on the bases of already existing parts.
    By freezing attributes after they were set, we guarantee alle processes have a consistent view.
    Furthermore, this class also implements a way to update a result object with another result object
    with additional set attributes. However, this is only allowed if bot objects originated
    from the same object via copy or pickle. With this update mechanism, TrainedEstimator's, Operation's and Stage's
    can share the same result object and gradually set the attributes of the result object, also if parts of the result
    where computed in a different process deserialized.
    """

    result_id: int

    def update(self, other: "Result"):
        if self.__class__ != other.__class__:
            raise TypeError(
                f"Incompatible classes for "
                f"self '{self.__class__.__name__}' and "
                f"other '{other.__class__.__name__}'."
            )
        if self.result_id != other.result_id:
            raise ValueError("Self and other have different result ids.")
        for key in self.__annotations__.keys():
            if not hasattr(other, key) and hasattr(self, key):
                raise AttributeError(
                    f"Attribute '{key}' is set in self, but not in other."
                )
            if not hasattr(other, key):
                continue
            other_value = getattr(other, key)
            if hasattr(self, key):
                self_value = getattr(self, key)
                if other_value == self_value:
                    continue
                raise AttributeError(
                    f"Values for attribute '{key}' are different in self '{self_value}' and other '{other_value}'"
                )
            setattr(self, key, other_value)
        return self

    def is_complete(self) -> bool:
        result = all(hasattr(self, key) for key in self.__annotations__.keys())
        return result
