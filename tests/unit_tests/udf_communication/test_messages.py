import pytest

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import RegisterPeerMessage, Message, PingMessage, \
    StopMessage, PayloadMessage, MyConnectionInfoMessage, PeerIsReadyToReceiveMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message

connection_info = ConnectionInfo(port=Port(port=10000), group_identifier="test",
                                 ipaddress=IPAddress(ip_address="127.0.0.1"),
                                 name="test")
peer = Peer(connection_info=connection_info)

messages = [
    RegisterPeerMessage(peer=peer),
    PingMessage(source=connection_info),
    StopMessage(),
    PayloadMessage(source=connection_info),
    MyConnectionInfoMessage(my_connection_info=connection_info),
    PeerIsReadyToReceiveMessage(peer=peer)
]


@pytest.mark.parametrize(
    "message",
    [message for message in messages],
    ids=[message.__class__.__name__ for message in messages]
)
def test_message_serialization(message):
    byte_string = serialize_message(message)
    obj = deserialize_message(byte_string, Message)
    assert message == obj.__root__


@pytest.mark.parametrize(
    "message",
    [message for message in messages],
    ids=[message.__class__.__name__ for message in messages]
)
def test_message_has_message_type(message):
    assert "message_type" in message.__dict__ and message.message_type == message.__class__.__name__
