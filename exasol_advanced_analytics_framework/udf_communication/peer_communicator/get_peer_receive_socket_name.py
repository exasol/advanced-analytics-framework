import urllib.parse

from exasol_advanced_analytics_framework.udf_communication.peer import Peer


def get_peer_receive_socket_name(peer: Peer) -> str:
    quoted_ip_address = urllib.parse.quote_plus(peer.connection_info.ipaddress.ip_address)
    quoted_port = urllib.parse.quote_plus(str(peer.connection_info.port))
    quoted_group_identifier = peer.connection_info.group_identifier
    return f"inproc://peer/{quoted_group_identifier}/{quoted_ip_address}/{quoted_port}"
