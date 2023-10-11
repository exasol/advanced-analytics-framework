import ifaddr
import pytest

from exasol_advanced_analytics_framework.udf_communication.ip_address import IPAddress, Port
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import is_discovery_leader_node, \
    UDFCommunicatorConfig


def test_result_false():
    config = UDFCommunicatorConfig(
        multi_node_discovery_ip=IPAddress(ip_address=""),
        number_of_instances_per_node=1,
        group_identifier_suffix="test",
        listen_port=Port(port=6789)
    )
    assert is_discovery_leader_node(config) == False


ips = [ip.ip for adapter in ifaddr.get_adapters() for ip in adapter.ips if ip.is_IPv4]


@pytest.mark.parametrize("ip", ips)
def test_result_true(ip: str):
    config = UDFCommunicatorConfig(
        multi_node_discovery_ip=IPAddress(ip_address=ip),
        number_of_instances_per_node=1,
        group_identifier_suffix="test",
        listen_port=Port(port=6789)
    )
    assert is_discovery_leader_node(config) == True


def test_ips_not_emptry():
    assert ips != []
