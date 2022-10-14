import dataclasses

from pydantic import BaseModel


class IPAddress(BaseModel):
    ip_address: str


class Port(BaseModel):
    port: int
