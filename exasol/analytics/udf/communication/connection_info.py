from pydantic import BaseModel

from exasol.analytics.udf.communication.ip_address import Port, IPAddress


class ConnectionInfo(BaseModel, frozen=True):
    name: str
    port: Port
    ipaddress: IPAddress
    group_identifier: str
