from typing import Type

from pydantic import BaseModel


def serialize(obj: BaseModel) -> bytes:
    json_str = obj.json()
    return json_str.encode("UTF-8")


def deserialize(message: bytes, base_model_class: Type[BaseModel]) -> BaseModel:
    obj = base_model_class.parse_raw(message, encoding="UTF-8")
    return obj
