from pydantic import BaseModel

from exasol.analytics.udf.communication.connection_info import ConnectionInfo


class Peer(BaseModel, frozen=True):
    connection_info: ConnectionInfo
