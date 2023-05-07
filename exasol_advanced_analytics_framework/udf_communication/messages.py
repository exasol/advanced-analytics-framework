from typing import Literal, Union, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class BaseMessage(BaseModel, frozen=True):
    pass


class MyConnectionInfoMessage(BaseMessage, frozen=True):
    message_type: Literal["MyConnectionInfoMessage"] = "MyConnectionInfoMessage"
    my_connection_info: ConnectionInfo


class PingMessage(BaseMessage, frozen=True):
    message_type: Literal["PingMessage"] = "PingMessage"
    source: ConnectionInfo


class SynchronizeConnectionMessage(BaseMessage, frozen=True):
    message_type: Literal["SynchronizeConnectionMessage"] = "SynchronizeConnectionMessage"
    source: ConnectionInfo


class AcknowledgeConnectionMessage(BaseMessage, frozen=True):
    message_type: Literal["AcknowledgeConnectionMessage"] = "AcknowledgeConnectionMessage"
    source: ConnectionInfo


class ConnectionIsReadyMessage(BaseMessage, frozen=True):
    message_type: Literal["ConnectionIsReadyMessage"] = "ConnectionIsReadyMessage"
    peer: Peer


class RegisterPeerMessage(BaseMessage, frozen=True):
    message_type: Literal["RegisterPeerMessage"] = "RegisterPeerMessage"
    peer: Peer
    source: Optional["Peer"]


class AcknowledgeRegisterPeerMessage(BaseMessage, frozen=True):
    message_type: Literal["AcknowledgeRegisterPeerMessage"] = "AcknowledgeRegisterPeerMessage"
    peer: Peer
    source: Peer


class RegisterPeerCompleteMessage(BaseMessage, frozen=True):
    message_type: Literal["RegisterPeerCompleteMessage"] = "RegisterPeerCompleteMessage"
    peer: Peer
    source: Peer


class PeerRegisterForwarderIsReadyMessage(BaseMessage, frozen=True):
    message_type: Literal["PeerRegisterForwarderIsReadyMessage"] = \
        "PeerRegisterForwarderIsReadyMessage"
    peer: Peer


class CloseMessage(BaseMessage, frozen=True):
    message_type: Literal["CloseMessage"] = "CloseMessage"


class PrepareToCloseMessage(BaseMessage, frozen=True):
    message_type: Literal["PrepareToCloseMessage"] = "PrepareToCloseMessage"


class IsReadyToCloseMessage(BaseMessage, frozen=True):
    message_type: Literal["IsReadyToCloseMessage"] = "IsReadyToCloseMessage"


class PayloadMessage(BaseMessage, frozen=True):
    message_type: Literal["PayloadMessage"] = "PayloadMessage"
    source: Peer
    destination: Peer
    sequence_number: int


class AcknowledgePayloadMessage(BaseMessage, frozen=True):
    message_type: Literal["AcknowledgePayloadMessage"] = "AcknowledgePayloadMessage"
    source: Peer
    sequence_number: int


class AbortPayloadMessage(BaseMessage, frozen=True):
    message_type: Literal["AbortPayloadMessage"] = "AbortPayloadMessage"
    payload_message: PayloadMessage
    reason: str


class TimeoutMessage(BaseMessage, frozen=True):
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
        AcknowledgePayloadMessage,
        AbortPayloadMessage,
        TimeoutMessage
    ]
