from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress
from exasol_advanced_analytics_framework.udf_communication.messages import Message, RegisterPeerMessage, AckMessage
from exasol_advanced_analytics_framework.udf_communication.peer import Peer
from exasol_advanced_analytics_framework.udf_communication.serialization import serialize_message, deserialize_message


def test_register_peer_message():
    message = RegisterPeerMessage(
        peer=Peer(
            connection_info=ConnectionInfo(
                port=Port(port=10000),
                group_identifier="test",
                ipaddress=IPAddress(ip_address="127.0.0.1")
            )
        )
    )
    byte_string = serialize_message(message)
    print(byte_string)
    obj = deserialize_message(byte_string, Message)
    print(obj)



def test_ack_register_peer_message():
    message = RegisterPeerMessage(
        peer=Peer(
            connection_info=ConnectionInfo(
                port=Port(port=10000),
                group_identifier="test",
                ipaddress=IPAddress(ip_address="127.0.0.1")
            )
        )
    )
    ack_message = AckMessage(wrapped_message=message)
    byte_string = serialize_message(ack_message)
    print(byte_string)
    obj = deserialize_message(byte_string, Message)
    print(obj)
