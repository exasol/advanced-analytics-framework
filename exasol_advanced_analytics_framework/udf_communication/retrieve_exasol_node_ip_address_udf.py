import re
from typing import List

from python_hosts import Hosts, HostsEntry

from exasol_advanced_analytics_framework.udf_communication.host_ip_addresses import HostIPAddresses
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress


class RetrieveExasolNodeIPAddressUDF:

    def __init__(self,
                 host_ip_addresses: HostIPAddresses = HostIPAddresses(),
                 hosts: Hosts = Hosts()):
        self._hosts = hosts
        self._host_ip_addresses = host_ip_addresses

    def run(self, ctx):
        hosts_entries: List[HostsEntry] = self._hosts.entries
        ip_addresses = self._host_ip_addresses.get_all_ip_addresses()

        def is_current_node_ip_address(ip_address: IPAddress, hosts_entry: HostsEntry) -> bool:
            if hosts_entry.entry_type != "ipv4":
                return False
            if hosts_entry.names is None:
                return False
            ip_address_matches = hosts_entry.address == ip_address.ip_address
            names_match = any(re.fullmatch(r"n\d\d", name) is not None for name in hosts_entry.names)
            return ip_address_matches and names_match

        current_node_ip_addresses = [
            ip_address
            for host_entry in hosts_entries
            for ip_address in ip_addresses
            if is_current_node_ip_address(ip_address=ip_address, hosts_entry=host_entry)
        ]
        if len(current_node_ip_addresses) != 1:
            raise RuntimeError(
                f"No or multiple possible IP addresses for current node found: {current_node_ip_addresses}\n"
                f"Hosts entries: {hosts_entries}\n"
                f"IP addresses: {ip_addresses}\n")
        current_node_ip_address = current_node_ip_addresses[0]
        ctx.emit(current_node_ip_address.ip_address, current_node_ip_address.network_prefix)
