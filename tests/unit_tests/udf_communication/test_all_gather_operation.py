import dataclasses
from typing import Union, List, Optional
from unittest.mock import MagicMock, create_autospec, call, Mock

from polyfactory.factories.pydantic_factory import ModelFactory

from exasol_advanced_analytics_framework.udf_communication import messages
from exasol_advanced_analytics_framework.udf_communication.all_gather_operation import AllGatherOperation, \
    AllGatherResult
from exasol_advanced_analytics_framework.udf_communication.broadcast_operation import BroadcastOperation
from exasol_advanced_analytics_framework.udf_communication.communicator_protocol import CommunicatorProtocol
from exasol_advanced_analytics_framework.udf_communication.messages import Gather
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.peer_communicator import PeerCommunicator
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message
from exasol_advanced_analytics_framework.udf_communication.socket_factory.abstract import SocketFactory, Frame
from tests.mock_cast import mock_cast


def test_init():
    communicator_mock: Union[CommunicatorProtocol, MagicMock] = create_autospec(CommunicatorProtocol)
    value = b"test1"
    AllGatherOperation(communicator=communicator_mock, value=value)
    assert communicator_mock.mock_calls == []


def test_call_is_multi_node_leader():
    communicator_mock: Union[CommunicatorProtocol, MagicMock] = create_autospec(CommunicatorProtocol)
    value = b"test1"
    gather_result = [value, b"test2"]
    mock_cast(communicator_mock.gather).side_effect = [gather_result]
    broadcast_result = serialize_message(AllGatherResult(gather_result=gather_result))
    mock_cast(communicator_mock.broadcast).side_effect = [broadcast_result]
    mock_cast(communicator_mock.is_multi_node_leader).return_value = True
    operation = AllGatherOperation(communicator=communicator_mock, value=value)
    communicator_mock.reset_mock()
    result = operation()
    assert result == gather_result \
           and communicator_mock.mock_calls == [
               call.gather(value),
               call.is_multi_node_leader(),
               call.broadcast(broadcast_result)
           ]


def test_call_is_not_multi_node_leader():
    communicator_mock: Union[CommunicatorProtocol, MagicMock] = create_autospec(CommunicatorProtocol)
    value = b"test1"
    gather_result = [value, b"test2"]
    mock_cast(communicator_mock.gather).side_effect = [None]
    broadcast_result = serialize_message(AllGatherResult(gather_result=gather_result))
    mock_cast(communicator_mock.broadcast).side_effect = [broadcast_result]
    mock_cast(communicator_mock.is_multi_node_leader).return_value = False
    operation = AllGatherOperation(communicator=communicator_mock, value=value)
    communicator_mock.reset_mock()
    result = operation()
    assert result == gather_result \
           and communicator_mock.mock_calls == [
               call.gather(value),
               call.is_multi_node_leader(),
               call.broadcast(None)
           ]
