from typing import Literal, Union, ForwardRef, List, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class MyConnectionInfoMessage(BaseModel, frozen=True):
    message_type: Literal["MyConnectionInfoMessage"] = "MyConnectionInfoMessage"
    my_connection_info: ConnectionInfo


class PingMessage(BaseModel, frozen=True):
    message_type: Literal["PingMessage"] = "PingMessage"
    source: ConnectionInfo


class SynchronizeConnectionMessage(BaseModel, frozen=True):
    message_type: Literal["SynchronizeConnectionMessage"] = "SynchronizeConnectionMessage"
    source: ConnectionInfo


class AcknowledgeConnectionMessage(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeConnectionMessage"] = "AcknowledgeConnectionMessage"
    source: ConnectionInfo


class ConnectionIsReadyMessage(BaseModel, frozen=True):
    message_type: Literal["ConnectionIsReadyMessage"] = "ConnectionIsReadyMessage"
    peer: Peer


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


class PeerRegisterForwarderIsReadyMessage(BaseModel, frozen=True):
    message_type: Literal["PeerRegisterForwarderIsReadyMessage"] = \
        "PeerRegisterForwarderIsReadyMessage"
    peer: Peer


class CloseMessage(BaseModel, frozen=True):
    message_type: Literal["CloseMessage"] = "CloseMessage"


class PrepareToCloseMessage(BaseModel, frozen=True):
    message_type: Literal["PrepareToCloseMessage"] = "PrepareToCloseMessage"


class IsReadyToCloseMessage(BaseModel, frozen=True):
    message_type: Literal["IsReadyToCloseMessage"] = "IsReadyToCloseMessage"


class PayloadMessage(BaseModel, frozen=True):
    message_type: Literal["PayloadMessage"] = "PayloadMessage"
    source: ConnectionInfo


class TimeoutMessage(BaseModel, frozen=True):
    message_type: Literal["TimeoutMessage"] = "TimeoutMessage"
    reason: str


class Message(BaseModel, frozen=True):
    __root__: Union[
        MyConnectionInfoMessage,
        PingMessage,
        SynchronizeConnectionMessage,
        AcknowledgeConnectionMessage,
        ConnectionIsReadyMessage,
        RegisterPeerMessage,
        AcknowledgeRegisterPeerMessage,
        RegisterPeerCompleteMessage,
        PeerRegisterForwarderIsReadyMessage,
        CloseMessage,
        PrepareToCloseMessage,
        IsReadyToCloseMessage,
        PayloadMessage,
        TimeoutMessage,
    ]
