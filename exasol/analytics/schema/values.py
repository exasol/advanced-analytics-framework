from datetime import datetime
from typing import Any


def format_timestamp(timestamp: datetime) -> str:
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")


def _enclose_in_single_quotes(value: Any) -> str:
    return f"'{value}'"


def quote_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return _enclose_in_single_quotes(value)
    if isinstance(value, bool):
        return str(value).upper()
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    if isinstance(value, datetime):
        return _enclose_in_single_quotes(format_timestamp(value))
    raise ValueError(f"Unexpected data type {type(value)}: '{value}'")
