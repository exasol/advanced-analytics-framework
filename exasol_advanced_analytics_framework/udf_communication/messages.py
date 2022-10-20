from typing import Literal, Union, ForwardRef, List, Optional

from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo
from exasol_advanced_analytics_framework.udf_communication.peer import Peer


class RegisterPeerMessage(BaseModel, frozen=True):
    message_type: Literal["RegisterPeerMessage"] = "RegisterPeerMessage"
    peer: Peer


class PingMessage(BaseModel, frozen=True):
    message_type: Literal["PingMessage"] = "PingMessage"
    connection_info: ConnectionInfo


class PongMessage(BaseModel, frozen=True):
    message_type: Literal["PongMessage"] = "PongMessage"
    connection_info: ConnectionInfo


class StopMessage(BaseModel, frozen=True):
    message_type: Literal["StopMessage"] = "StopMessage"


class PayloadMessage(BaseModel, frozen=True):
    message_type: Literal["PayloadMessage"] = "PayloadMessage"
    connection_info: ConnectionInfo


class AckMessage(BaseModel, frozen=True):
    message_type: Literal["AckMessage"] = "AckMessage"
    connection_info: Optional[ConnectionInfo]
    wrapped_message: ForwardRef('Message')


class SendMessage(BaseModel, frozen=True):
    message_type: Literal["SendMessage"] = "SendMessage"


class MyConnectionInfoMessage(BaseModel, frozen=True):
    message_type: Literal["MyConnectionInfoMessage"] = "MyConnectionInfoMessage"
    connection_info: ConnectionInfo


class ReadyToReceiveMessage(BaseModel, frozen=True):
    message_type: Literal["ReadyToReceiveMessage"] = "ReadyToReceiveMessage"
    connection_info: ConnectionInfo


class Message(BaseModel, frozen=True):
    __root__: Union[
        PongMessage,
        PingMessage,
        RegisterPeerMessage,
        StopMessage,
        PayloadMessage,
        AckMessage,
        MyConnectionInfoMessage,
        ReadyToReceiveMessage
    ]


AckMessage.update_forward_refs()
