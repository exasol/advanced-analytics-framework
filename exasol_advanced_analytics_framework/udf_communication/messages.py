from typing import Literal, Union, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class RegisterPeerMessage(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerMessage"] = "RegisterPeerMessage"
    peer: Peer
    source: Optional["Peer"]


class AcknowledgeRegisterPeerMessage(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeRegisterPeerMessage"] = "AcknowledgeRegisterPeerMessage"
    peer: Peer
    source: Peer


class RegisterPeerCompleteMessage(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerCompleteMessage"] = "RegisterPeerCompleteMessage"
    peer: Peer
    source: Peer


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
        AcknowledgeRegisterPeerMessage,
        RegisterPeerCompleteMessage,
        StopMessage,
        PayloadMessage,
        MyConnectionInfoMessage,
        PeerIsReadyToReceiveMessage,
        SynchronizeConnectionMessage,
        AcknowledgeConnectionMessage,
        TimeoutMessage
    ]
