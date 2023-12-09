from ipaddress import ip_network
from typing import Optional

from pydantic import BaseModel


class IPAddress(BaseModel, frozen=True):
    ip_address: str
    network_prefix: Optional[int] = None


class Port(BaseModel, frozen=True):
    port: int


def are_ips_in_same_network(ip_address1: IPAddress, ip_address2: IPAddress):
    ip_network1 = ip_network(f"{ip_address1.ip_address}/{ip_address1.network_prefix}", strict=False)
    ip_network2 = ip_network(f"{ip_address2.ip_address}/{ip_address2.network_prefix}", strict=False)
    return ip_network1.network_address == ip_network2.network_address
