from pydantic import BaseModel

from exasol.analytics.udf.communication.ip_address import IPAddress, Port


class ConnectionInfo(BaseModel, frozen=True):
    name: str
    port: Port
    ipaddress: IPAddress
    group_identifier: str
