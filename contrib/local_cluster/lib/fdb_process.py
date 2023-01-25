import asyncio
import glob
import ipaddress
import json
import logging
import os.path
import shutil

import lib.process

from typing import Dict, List, Union


logger = logging.getLogger(__name__)

FDBSERVER_TIMEOUT: float = 180.0

FDBCLI_TIMEOUT: float = 180.0
FDBCLI_RETRY_TIME: float = 1.0


class FileNotFoundError(OSError):
    """File not found error"""

    def __init__(self, strerror: str, filename: str, *args, **kwargs):
        """Constructor

        :param int errno: errno
        :param str strerror: error text
        :param str filename: file name
        """
        super().__init__(*args, **kwargs)

        self._strerror = strerror
        self._filename = filename

    @property
    def filename(self) -> str:
        """Name of the file"""
        return self._strerror

    @property
    def strerror(self) -> str:
        """Error context"""
        return self._strerror


class _ExecutablePath:
    """Path to executable"""

    def __init__(self, executable: str, overridden_path: str = None):
        """Constructor
        :param str executable:
        :param str overridden_path:
        :raises FileNotFoundError:
        """
        self._executable = executable
        self._path = None

        if overridden_path:
            self.set(overridden_path)

    def set(self, overridden_path: str = None):
        """Set and validate the path
        :param str overridden_path:
        """
        path = overridden_path
        if path is None:
            path = shutil.which(self._executable)

        if path is None or not os.path.exists(path):
            raise FileNotFoundError(
                0, f"{self._executable} not found in {path}", self._executable
            )

        logger.debug(f"Setting {self._executable} executable to {path}")
        self._path = path

    def __str__(self) -> str:
        return self._path


_fdbserver_path: _ExecutablePath = _ExecutablePath("fdbserver")
_fdbcli_path: _ExecutablePath = _ExecutablePath("fdbcli")


def set_fdbserver_path(path: Union[str, None] = None):
    """Set the path to fdbserver executable
    :raises RuntimeError: if fdbserver is not found
    """
    _fdbserver_path.set(path)


def get_fdbserver_path() -> str:
    """Gets the path to fdbserver executable"""
    return str(_fdbserver_path)


def set_fdbcli_path(path: Union[str, None] = None):
    """Set the path to fdbcli executable
    :raises RuntimeError: if fdbcli is not found
    """
    _fdbcli_path.set(path)


def get_fdbcli_path() -> str:
    """Get the path to fdbcli executable"""
    return str(_fdbcli_path)


class FDBServerProcess(lib.process.Process):
    """Maintain a FDB server process as coroutine"""

    def __init__(
        self,
        cluster_file: str,
        public_ip_address: Union[ipaddress.IPv4Address, None] = None,
        port: Union[int, None] = None,
        class_: str = None,
        data_path: str = None,
        log_path: str = None,
        fdbserver_overridden_path: str = None,
    ):
        """Constructor
        :param str cluster_file: Path to the cluster file
        :param ipaddress.IPv4Address public_ip_address: IP address
        :param int port: Port the FDB uses
        :param str class_: FDB process class
        :param str data_path: Path to the database files
        :param str log_path: Path to log files
        :param str fdbserver_overridden_path:
        """
        super().__init__(executable=str(fdbserver_overridden_path or _fdbserver_path))
        self._cluster_file = cluster_file
        self._public_address = f"{public_ip_address or '127.0.0.1'}:{port or 4000}"
        self._class_ = class_
        self._data_path = data_path
        self._log_path = log_path

    async def run(self):
        self._args = ["--cluster-file", self._cluster_file, "-p", self._public_address]
        if self._class_:
            self._args.extend("-c", self._class_)
        if self._data_path:
            self._args.extend(["--datadir", self._data_path])
        if self._log_path:
            self._args.extend(["--logdir", self._log_path])
        return await super().run()

    def _iterate_log_files(self) -> List[str]:
        """Iterate log files

        :return List[str]: _description_
        """
        pattern = os.path.join(self._log_path, "*.xml")
        for path in glob.glob(pattern, recursive=False):
            yield path

    def get_log_with_severity(self, severity=40) -> Dict[str, List[str]]:
        """Get the lines with given severity from XML log

        :return Dict[str, List[str]]:
        """
        result = {}
        severity_string = f'Severity="{severity}"'
        for log_file in self._iterate_log_files():
            result[log_file] = []
            with open(log_file) as stream:
                for line in stream:
                    if severity_string in line:
                        result[log_file].append(line)
        return result


class FDBCLIProcess(lib.process.Process):
    """Maintain a FDB CLI process as coroutine"""

    def __init__(self, cluster_file: str, commands: Union[List[str], str, None] = None):
        """Constructor
        :param str cluster_file: Path to the cluster file
        :param List[str] commands: Commands
        """
        super().__init__(executable=str(_fdbcli_path))
        self._cluster_file = cluster_file
        self._commands = commands

    async def run(self):
        self._args = ["-C", self._cluster_file]
        if isinstance(self._commands, list):
            self._args.extend(["--exec", ";".join(self._commands)])
        elif isinstance(self._commands, str):
            self._args.extend(["--exec", self._commands])
        return await super().run()


async def get_server_status(cluster_file: str) -> Union[Dict, None]:
    """Get the status of fdbserver via fdbcli

    :param str cluster_file: path to the cluster file
    :return Union[Dict, None]:
    """
    fdbcli_process = await lib.fdb_process.FDBCLIProcess(
        cluster_file=cluster_file, commands="status json"
    ).run()
    try:
        output = await asyncio.wait_for(fdbcli_process.stdout.read(-1), FDBCLI_TIMEOUT)
        await fdbcli_process.wait()
        return json.loads(output.decode())
    except TimeoutError:
        return None


async def wait_fdbserver_up(cluster_file: str):
    """Wait for the server that responds

    :param str cluster_file: path to the cluster file
    """

    async def impl():
        response = await get_server_status(cluster_file=cluster_file)
        while response is None:
            await asyncio.sleep(FDBCLI_RETRY_TIME)
            response = await get_server_status(cluster_file=cluster_file)

    await asyncio.wait_for(impl(), FDBSERVER_TIMEOUT)


async def wait_fdbserver_available(cluster_file: str):
    """Wait for the server gets available

    :param str cluster_file: path to the cluster file
    """

    async def impl():
        while True:
            status = await get_server_status(cluster_file)
            if status is not None and status.get("client", {}).get(
                "database_status", {}
            ).get("available", False):
                break
            await asyncio.sleep(FDBCLI_RETRY_TIME)

    await asyncio.wait_for(impl(), FDBSERVER_TIMEOUT)


try:
    set_fdbserver_path()
except FileNotFoundError:
    logger.warn("Cannot find fdbserver at default location")

try:
    set_fdbcli_path()
except FileNotFoundError:
    logger.warn("Cannot find fdbcli at default location")
