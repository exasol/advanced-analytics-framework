from typing import Literal, Union

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class RegisterPeerMessage(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerMessage"] = "RegisterPeerMessage"
    peer: Peer


class PeerIsReadyToReceiveMessage(BaseModel, frozen=True):
    message_type: Literal["PeerIsReadyToReceiveMessage"] = "PeerIsReadyToReceiveMessage"
    peer: Peer


class PingMessage(BaseModel, frozen=True):
    message_type: Literal["PingMessage"] = "PingMessage"
    source: ConnectionInfo


class StopMessage(BaseModel, frozen=True):
    message_type: Literal["StopMessage"] = "StopMessage"


class PayloadMessage(BaseModel, frozen=True):
    message_type: Literal["PayloadMessage"] = "PayloadMessage"
    source: ConnectionInfo


class MyConnectionInfoMessage(BaseModel, frozen=True):
    message_type: Literal["MyConnectionInfoMessage"] = "MyConnectionInfoMessage"
    my_connection_info: ConnectionInfo


class SynchronizeConnectionMessage(BaseModel, frozen=True):
    message_type: Literal["SynchronizeConnectionMessage"] = "SynchronizeConnectionMessage"
    source: ConnectionInfo


class AcknowledgeConnectionMessage(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeConnectionMessage"] = "AcknowledgeConnectionMessage"
    source: ConnectionInfo


class TimeoutMessage(BaseModel, frozen=True):
    message_type: Literal["TimeoutMessage"] = "TimeoutMessage"


class Message(BaseModel, frozen=True):
    __root__: Union[
        PingMessage,
        RegisterPeerMessage,
        StopMessage,
        PayloadMessage,
        MyConnectionInfoMessage,
        PeerIsReadyToReceiveMessage,
        SynchronizeConnectionMessage,
        AcknowledgeConnectionMessage,
        TimeoutMessage
    ]
