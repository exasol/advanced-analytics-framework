from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.connection_info import ConnectionInfo


class Peer(BaseModel, frozen=True):
    connection_info: ConnectionInfo
