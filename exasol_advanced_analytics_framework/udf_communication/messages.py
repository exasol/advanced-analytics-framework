from typing import Literal, Union, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class RegisterPeer(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerMessage"] = "RegisterPeerMessage"
    peer: Peer
    source: Optional["Peer"]


class AcknowledgeRegisterPeer(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeRegisterPeerMessage"] = "AcknowledgeRegisterPeerMessage"
    peer: Peer
    source: Peer


class RegisterPeerComplete(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerCompleteMessage"] = "RegisterPeerCompleteMessage"
    peer: Peer
    source: Peer


class PeerIsReadyToReceive(BaseModel, frozen=True):
    message_type: Literal["PeerIsReadyToReceiveMessage"] = "PeerIsReadyToReceiveMessage"
    peer: Peer


class Ping(BaseModel, frozen=True):
    message_type: Literal["PingMessage"] = "PingMessage"
    source: ConnectionInfo


class Stop(BaseModel, frozen=True):
    message_type: Literal["StopMessage"] = "StopMessage"


class Payload(BaseModel, frozen=True):
    message_type: Literal["PayloadMessage"] = "PayloadMessage"
    source: ConnectionInfo


class MyConnectionInfo(BaseModel, frozen=True):
    message_type: Literal["MyConnectionInfoMessage"] = "MyConnectionInfoMessage"
    my_connection_info: ConnectionInfo


class SynchronizeConnection(BaseModel, frozen=True):
    message_type: Literal["SynchronizeConnectionMessage"] = "SynchronizeConnectionMessage"
    source: ConnectionInfo


class AcknowledgeConnection(BaseModel, frozen=True):
    message_type: Literal["AcknowledgeConnectionMessage"] = "AcknowledgeConnectionMessage"
    source: ConnectionInfo


class Timeout(BaseModel, frozen=True):
    message_type: Literal["TimeoutMessage"] = "TimeoutMessage"


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
