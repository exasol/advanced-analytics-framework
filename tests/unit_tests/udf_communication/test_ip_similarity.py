import ifaddr
from ipaddress import ip_network

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress


def find_ip_in_same_network(ip_address: IPAddress):
    reference_ip_network = ip_network(f"{ip_address.ip_address}/{ip_address.network_prefix}", strict=False)
    ips = [ip for adapter in ifaddr.get_adapters() for ip in adapter.ips if ip.is_IPv4 and ip.ip]
    for ip in ips:
        network = ip_network(f"{ip.ip}/{ip.network_prefix}", strict=False)
        if reference_ip_network.network_address == network.network_address:
            return IPAddress(ip_address=ip.ip, network_prefix=ip.network_prefix)
    raise RuntimeError("No compatible IP adress found")


def test_find_ip_in_same_network():
    ips = [ip for adapter in ifaddr.get_adapters() for ip in adapter.ips if ip.is_IPv4 and ip.ip]
    print(ips)
    ip_address = IPAddress(ip_address="127.0.0.2",network_prefix=8)
    find_ip_in_same_network(ip_address)