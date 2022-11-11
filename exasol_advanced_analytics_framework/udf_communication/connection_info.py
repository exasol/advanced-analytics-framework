from pydantic import BaseModel

from exasol_advanced_analytics_framework.udf_communication.ip_address import Port, IPAddress


class ConnectionInfo(BaseModel, frozen=True):
    name: str
    port: Port
    ipaddress: IPAddress
    group_identifier: str
