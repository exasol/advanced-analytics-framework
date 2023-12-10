import re
from typing import Union, Callable
from unittest.mock import create_autospec, MagicMock, call, Mock

import pytest
from exasol_udf_mock_python.column import Column
from exasol_udf_mock_python.connection import Connection
from exasol_udf_mock_python.mock_exa_environment import MockExaEnvironment
from exasol_udf_mock_python.mock_meta_data import MockMetaData

from exasol_advanced_analytics_framework.udf_communication.communicator import CommunicatorFactory
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.host_ip_addresses import HostIPAddresses
from exasol_advanced_analytics_framework.udf_communication.udf_communicator import udf_communicator, \
    UDFCommunicatorConfig
from exasol_advanced_analytics_framework.udf_communication.socket_factory_context_manager_factory import \
    SocketFactoryContextManagerFactory, SocketFactoryContextManager
from tests.mock_cast import mock_cast

MY_CONN = "my_conn"


def dummy_udf_wrapper():
    pass


@pytest.fixture
def config() -> UDFCommunicatorConfig:
    config = UDFCommunicatorConfig(
        listen_port=Port(port=6789),
        multi_node_discovery_ip=IPAddress(ip_address="127.0.0.1", network_prefix=8),
        group_identifier_suffix="test",
        number_of_instances_per_node=1
    )
    return config


@pytest.fixture
def connection(config) -> Connection:
    return Connection(address=config.json())


@pytest.fixture
def node_id(config):
    return 1


@pytest.fixture
def exa_environment(connection, node_id) -> MockExaEnvironment:
    metadata = MockMetaData(
        script_code_wrapper_function=dummy_udf_wrapper,
        node_id=node_id,
        vm_id=222,
        session_id=3333,
        statement_id=1,
        node_count=1,
        input_type="SET",
        input_columns=[
            Column("test", int, "INTEGER"),
        ],
        output_type="EMITS",
        output_columns=[
            Column("test", int, "int"),
        ],
    )
    exa = MockExaEnvironment(
        metadata=metadata,
        connections={
            MY_CONN: connection
        }
    )
    return exa


def test_is_discovery_leader_node_true(exa_environment, node_id, config):
    communicator_factory_mock: Union[CommunicatorFactory, MagicMock] = create_autospec(CommunicatorFactory)

    host_ip_addresses_mock: Union[HostIPAddresses, MagicMock] = create_autospec(HostIPAddresses)
    mock_cast(host_ip_addresses_mock.get_all_ip_addresses).return_value = [config.multi_node_discovery_ip]

    socket_factory_context_manager_factory: Union[SocketFactoryContextManagerFactory, MagicMock] = \
        create_autospec(SocketFactoryContextManagerFactory)
    socket_factory_context_manager: Union[SocketFactoryContextManager, MagicMock] = \
        create_autospec(SocketFactoryContextManager)
    mock_cast(socket_factory_context_manager_factory.create).side_effect = [socket_factory_context_manager]
    with udf_communicator(
            connection_name=MY_CONN,
            exa=exa_environment,
            communicator_factory=communicator_factory_mock,
            socket_factory_context_manager_factory=socket_factory_context_manager_factory,
            host_ip_addresses=host_ip_addresses_mock):
        pass
    assert \
        communicator_factory_mock.mock_calls == [
            call.create(multi_node_discovery_ip=config.multi_node_discovery_ip,
                        socket_factory=mock_cast(socket_factory_context_manager.__enter__).return_value,
                        node_name=node_id,
                        instance_name=exa_environment.meta.vm_id,
                        listen_ip=config.multi_node_discovery_ip,
                        group_identifier='3333_1_test',
                        number_of_nodes=1,
                        number_of_instances_per_node=1,
                        is_discovery_leader_node=True,
                        multi_node_discovery_port=config.listen_port,
                        local_discovery_port=config.listen_port),
            call.create().stop(),
        ]
    assert host_ip_addresses_mock.mock_calls == [call.get_all_ip_addresses()]
    assert socket_factory_context_manager_factory.mock_calls == [call.create()]
    assert socket_factory_context_manager.mock_calls == [call.__enter__(), call.__exit__(None, None, None)]


def test_is_discovery_leader_node_false(exa_environment, node_id, config):
    communicator_factory_mock: Union[CommunicatorFactory, MagicMock] = create_autospec(CommunicatorFactory)

    host_ip_addresses_mock: Union[HostIPAddresses, MagicMock] = create_autospec(HostIPAddresses)
    ip_address = IPAddress(ip_address="127.0.0.2", network_prefix=8)
    mock_cast(host_ip_addresses_mock.get_all_ip_addresses).return_value = [ip_address]

    socket_factory_context_manager_factory: Union[SocketFactoryContextManagerFactory, MagicMock] = \
        create_autospec(SocketFactoryContextManagerFactory)
    socket_factory_context_manager: Union[SocketFactoryContextManager, MagicMock] = \
        create_autospec(SocketFactoryContextManager)
    mock_cast(socket_factory_context_manager_factory.create).side_effect = [socket_factory_context_manager]
    with udf_communicator(
            connection_name=MY_CONN,
            exa=exa_environment,
            communicator_factory=communicator_factory_mock,
            socket_factory_context_manager_factory=socket_factory_context_manager_factory,
            host_ip_addresses=host_ip_addresses_mock):
        pass
    assert \
        communicator_factory_mock.mock_calls == [
            call.create(multi_node_discovery_ip=config.multi_node_discovery_ip,
                        socket_factory=mock_cast(socket_factory_context_manager.__enter__).return_value,
                        node_name=node_id,
                        instance_name=exa_environment.meta.vm_id,
                        listen_ip=ip_address,
                        group_identifier='3333_1_test',
                        number_of_nodes=1,
                        number_of_instances_per_node=1,
                        is_discovery_leader_node=False,
                        multi_node_discovery_port=config.listen_port,
                        local_discovery_port=config.listen_port),
            call.create().stop(),
        ]
    assert host_ip_addresses_mock.mock_calls == [call.get_all_ip_addresses()]
    assert socket_factory_context_manager_factory.mock_calls == [call.create()]
    assert socket_factory_context_manager.mock_calls == [call.__enter__(), call.__exit__(None, None, None)]


def test_no_compatible_ip_address_found(exa_environment, node_id, config):
    communicator_factory_mock: Union[CommunicatorFactory, MagicMock] = create_autospec(CommunicatorFactory)

    host_ip_addresses_mock: Union[HostIPAddresses, MagicMock] = create_autospec(HostIPAddresses)
    mock_cast(host_ip_addresses_mock.get_all_ip_addresses).return_value = [
        IPAddress(ip_address="128.0.0.2", network_prefix=8)]

    socket_factory_context_manager_factory: Union[SocketFactoryContextManagerFactory, MagicMock] = \
        create_autospec(SocketFactoryContextManagerFactory)
    socket_factory_context_manager: Union[SocketFactoryContextManager, MagicMock] = \
        create_autospec(SocketFactoryContextManager)
    mock_cast(socket_factory_context_manager_factory.create).side_effect = [socket_factory_context_manager]
    with pytest.raises(RuntimeError, match=re.escape("No compatible IP address found")):
        with udf_communicator(
                connection_name=MY_CONN,
                exa=exa_environment,
                communicator_factory=communicator_factory_mock,
                socket_factory_context_manager_factory=socket_factory_context_manager_factory,
                host_ip_addresses=host_ip_addresses_mock):
            pass
