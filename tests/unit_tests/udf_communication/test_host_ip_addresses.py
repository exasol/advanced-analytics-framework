import ifaddr
from ifaddr import Adapter, IP

from exasol_advanced_analytics_framework.udf_communication.host_ip_addresses import HostIPAddresses
from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress


def test_contains_localhost():
    host_ip_addresses = HostIPAddresses()
    assert IPAddress(ip_address='127.0.0.1', network_prefix=8) in host_ip_addresses.get_all_ip_addresses()


def test_multiple_adapters_ipv4_only():
    adapters = [
        Adapter(
            name="Test1", nice_name="Test1", index=0,
            ips=[
                IP(nice_name="Test1", network_prefix=8, ip="192.168.0.1")
            ]
        ),
        Adapter(
            name="Test2", nice_name="Test2", index=1,
            ips=[
                IP(nice_name="Test2", network_prefix=8, ip="192.168.1.1"),
            ]
        )
    ]
    host_ip_addresses = HostIPAddresses(get_adapters=lambda: adapters)
    result = host_ip_addresses.get_all_ip_addresses()
    assert result == [
        IPAddress(ip_address='192.168.0.1', network_prefix=8),
        IPAddress(ip_address='192.168.1.1', network_prefix=8),
    ]

def test_multiple_ips_per_adapter_ipv4_only():
    adapters = [
        Adapter(
            name="Test1", nice_name="Test1", index=0,
            ips=[
                IP(nice_name="Test1", network_prefix=8, ip="192.168.0.1"),
                IP(nice_name="Test1", network_prefix=8, ip="192.168.1.1")
            ]
        )
    ]
    host_ip_addresses = HostIPAddresses(get_adapters=lambda: adapters)
    result = host_ip_addresses.get_all_ip_addresses()
    assert result == [
        IPAddress(ip_address='192.168.0.1', network_prefix=8),
        IPAddress(ip_address='192.168.1.1', network_prefix=8),
    ]

def test_multiple_ips_per_adapter_ipv4_ipv6_mixed():
    adapters = [
        Adapter(
            name="Test1", nice_name="Test1", index=0,
            ips=[
                IP(nice_name="Test1", network_prefix=8, ip="192.168.0.1"),
                IP(nice_name="Test1", network_prefix=8, ip=('fe80::c624:335:aea:9c04', 0, 2))
            ]
        )
    ]
    host_ip_addresses = HostIPAddresses(get_adapters=lambda: adapters)
    result = host_ip_addresses.get_all_ip_addresses()
    assert result == [
        IPAddress(ip_address='192.168.0.1', network_prefix=8)
    ]
