import ipaddress
import logging
import os.path
import secrets

from typing import Union


logger = logging.getLogger(__name__)


def generate_fdb_cluster_file(
    base_directory: str,
    description: Union[str, None] = None,
    ip_address: Union[ipaddress.IPv4Address, ipaddress.IPv6Address, None] = None,
    port: Union[int, None] = None,
) -> str:
    """Generate a fdb.cluster file

    :param str base_directory: The place the fdb.cluster will be created
    :param Union[str, None] description: Cluster description
    :param Union[ipaddress.IPv4Address, None] ip_address: IP, defaults to None
    :param Union[int, None] port: Port, defaults to None
    :return str: Path to the cluster file
    """
    cluster_file_name = os.path.join(base_directory, "fdb.cluster")
    description = description or secrets.token_hex(10)
    ip_address = ip_address or ipaddress.ip_address("127.0.0.1")
    port = port or 4000
    content = f"{description}:{description}@{ip_address}:{port}"

    with open(cluster_file_name, "w") as stream:
        stream.write(content)
    logger.debug(f"Generated cluster file with content: {content}")

    return cluster_file_name
