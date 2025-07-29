from typing import (
    Type,
    TypeVar,
)

from pydantic import BaseModel


def serialize_message(obj: BaseModel) -> bytes:
    json_str = obj.model_dump_json()
    return json_str.encode("UTF-8")


T = TypeVar("T", bound=BaseModel)


def deserialize_message(message: bytes, base_model_class: type[T]) -> T:
    obj = base_model_class.parse_raw(message, encoding="UTF-8")
    return obj
