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


class PeerIsReadyToReceive(BaseModel, frozen=True):
    message_type: Literal["PeerIsReadyToReceive"] = "PeerIsReadyToReceive"
    peer: Peer


class Ping(BaseModel, frozen=True):
    message_type: Literal["Ping"] = "Ping"
    source: ConnectionInfo


class Stop(BaseModel, frozen=True):
    message_type: Literal["Stop"] = "Stop"


class Payload(BaseModel, frozen=True):
    message_type: Literal["Payload"] = "Payload"
    source: ConnectionInfo


class MyConnectionInfo(BaseModel, frozen=True):
    message_type: Literal["MyConnectionInfo"] = "MyConnectionInfo"
    my_connection_info: ConnectionInfo


class SynchronizeConnection(BaseModel, frozen=True):
    message_type: Literal["SynchronizeConnection"] = "SynchronizeConnection"
    source: ConnectionInfo


class AcknowledgeConnection(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeConnection"] = "AcknowledgeConnection"
    source: ConnectionInfo


class Timeout(BaseModel, frozen=True):
    message_type: Literal["Timeout"] = "Timeout"


class Message(BaseModel, frozen=True):
    __root__: Union[
        Ping,
        RegisterPeer,
        AcknowledgeRegisterPeer,
        RegisterPeerComplete,
        Stop,
        Payload,
        MyConnectionInfo,
        PeerIsReadyToReceive,
        SynchronizeConnection,
        AcknowledgeConnection,
        Timeout
    ]
