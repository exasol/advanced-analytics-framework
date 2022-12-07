from typing import Literal, Union, ForwardRef, List, Optional

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


class WeAreReadyToReceiveMessage(BaseModel, frozen=True):
    message_type: Literal["WeAreReadyToReceiveMessage"] = "WeAreReadyToReceiveMessage"
    source: ConnectionInfo


class AreYouReadyToReceiveMessage(BaseModel, frozen=True):
    message_type: Literal["AreYouReadyToReceiveMessage"] = "AreYouReadyToReceiveMessage"
    source: ConnectionInfo


class AckReadyToReceiveMessage(BaseModel, frozen=True):
    message_type: Literal["AckReadyToReceiveMessage"] = "AckReadyToReceiveMessage"
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
        WeAreReadyToReceiveMessage,
        AreYouReadyToReceiveMessage,
        PeerIsReadyToReceiveMessage,
        AckReadyToReceiveMessage,
        TimeoutMessage
    ]
