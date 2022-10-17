import dataclasses

from pydantic import BaseModel


class IPAddress(BaseModel, frozen=True):
    ip_address: str


class Port(BaseModel, frozen=True):
    port: int
