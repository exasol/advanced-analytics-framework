from typing import Literal, Union, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class RegisterPeer(BaseModel, frozen=True):
    message_type: Literal["RegisterPeer"] = "RegisterPeer"
    peer: Peer
    source: Optional["Peer"]


class AcknowledgeRegisterPeer(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeRegisterPeer"] = "AcknowledgeRegisterPeer"
    peer: Peer
    source: Peer


class RegisterPeerComplete(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerComplete"] = "RegisterPeerComplete"
    peer: Peer
    source: Peer


class PeerRegisterForwarderIsReady(BaseModel, frozen=True):
    message_type: Literal["PeerRegisterForwarderIsReady"] = \
        "PeerRegisterForwarderIsReady"
    peer: Peer


class Ping(BaseModel, frozen=True):
    message_type: Literal["Ping"] = "Ping"
    source: ConnectionInfo


class Stop(BaseModel, frozen=True):
    message_type: Literal["Stop"] = "Stop"


class PrepareToStop(BaseModel, frozen=True):
    message_type: Literal["PrepareToStop"] = "PrepareToStop"


class IsReadyToStop(BaseModel, frozen=True):
    message_type: Literal["IsReadyToStop"] = "IsReadyToStop"


class Payload(BaseModel, frozen=True):
    message_type: Literal["PayloadMessage"] = "PayloadMessage"
    source: Peer
    destination: Peer
    sequence_number: int


class AcknowledgePayload(BaseModel, frozen=True):
    message_type: Literal["AcknowledgePayloadMessage"] = "AcknowledgePayloadMessage"
    source: Peer
    sequence_number: int


class AbortPayload(BaseModel, frozen=True):
    message_type: Literal["AbortPayloadMessage"] = "AbortPayloadMessage"
    payload: Payload
    reason: str


class MyConnectionInfo(BaseModel, frozen=True):
    message_type: Literal["MyConnectionInfo"] = "MyConnectionInfo"
    my_connection_info: ConnectionInfo


class SynchronizeConnection(BaseModel, frozen=True):
    message_type: Literal["SynchronizeConnection"] = "SynchronizeConnection"
    source: ConnectionInfo


class AcknowledgeConnection(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeConnection"] = "AcknowledgeConnection"
    source: ConnectionInfo


class ConnectionIsReady(BaseModel, frozen=True):
    message_type: Literal["ConnectionIsReady"] = "ConnectionIsReady"
    peer: Peer


class Timeout(BaseModel, frozen=True):
    message_type: Literal["Timeout"] = "Timeout"
    reason: str


class Message(BaseModel, frozen=True):
    __root__: Union[
        Ping,
        RegisterPeer,
        AcknowledgeRegisterPeer,
        RegisterPeerComplete,
        PeerRegisterForwarderIsReady,
        Stop,
        PrepareToStop,
        IsReadyToStop,
        Payload,
        AcknowledgePayload,
        AbortPayload,
        MyConnectionInfo,
        ConnectionIsReady,
        SynchronizeConnection,
        AcknowledgeConnection,
        Timeout
    ]
