from typing import List, Callable, Iterable

import ifaddr
from ifaddr import Adapter

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress


class HostIPAddresses:

    def __init__(self, get_adapters: Callable[[], Iterable[Adapter]] = ifaddr.get_adapters):
        self._get_adapters = get_adapters

    def get_all_ip_addresses(self) -> List[IPAddress]:
        ip_adresses = [IPAddress(ip_address=ip.ip, network_prefix=ip.network_prefix)
                       for adapter in self._get_adapters()
                       for ip in adapter.ips
                       if ip.is_IPv4 and ip.ip is not None]
        return ip_adresses
