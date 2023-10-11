from typing import Optional

from pydantic import BaseModel


class IPAddress(BaseModel, frozen=True):
    ip_address: str
    network_prefix: Optional[int] = None


class Port(BaseModel, frozen=True):
    port: int
