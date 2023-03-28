import asyncio
import logging
import os
import os.path

import lib.cluster_file
import lib.fdb_process
import lib.work_directory

from typing import List, Union

logger = logging.getLogger(__name__)

FDB_DEFAULT_PORT = 4000


async def configure_fdbserver(cluster_file: str):
    await lib.fdb_process.wait_fdbserver_up(cluster_file=cluster_file)
    await (
        await lib.fdb_process.FDBCLIProcess(
            cluster_file=cluster_file,
            commands="configure new single memory tenant_mode=optional_experimental",
        ).run()
    ).wait()
    await lib.fdb_process.wait_fdbserver_available(cluster_file=cluster_file)


async def spawn_fdbservers(
    num_processes: int,
    directory: lib.work_directory.WorkDirectory,
    cluster_file: Union[str, None],
    port: Union[int, None] = None,
):
    fdb_processes = {"handlers": [], "processes": []}
    initial_port = port or FDB_DEFAULT_PORT

    for i in range(num_processes):
        data_path = os.path.join(directory.data_directory, str(i))
        os.makedirs(data_path, exist_ok=True)

        log_path = os.path.join(directory.log_directory, str(i))
        os.makedirs(log_path, exist_ok=True)

        fdb_server_process = lib.fdb_process.FDBServerProcess(
            cluster_file=cluster_file,
            port=initial_port,
            data_path=data_path,
            log_path=log_path,
        )
        fdb_processes["handlers"].append(fdb_server_process)
        fdb_processes["processes"].append(await fdb_server_process.run())
        initial_port += 1

    return fdb_processes


class FDBServerLocalCluster:
    def __init__(
        self,
        num_processes: int,
        work_directory: Union[str, None] = None,
        cluster_file: Union[str, None] = None,
        port: Union[int, None] = None,
    ):
        """Constructor

        :param int num_processes: _description_
        :param Union[str, None] work_directory: _description_, defaults to None
        :param Union[str, None] cluster_file: _description_, defaults to None
        :param Union[int, None] port: _description_, defaults to None
        """
        self._num_processes: int = num_processes
        self._work_directory: Union[str, None] = work_directory
        self._cluster_file: Union[str, None] = cluster_file
        self._port: int = port or FDB_DEFAULT_PORT

        self._processes = {"processes": [], "handlers": []}

    @property
    def work_directory(self) -> Union[str, None]:
        """Work directory

        :return str: _description_
        """
        return self._work_directory

    @property
    def cluster_file(self) -> Union[str, None]:
        """Path to the cluster file

        :return str: _description_
        """
        return self._cluster_file

    @property
    def processes(self):
        """Processes"""
        return self._processes["processes"]

    @property
    def handlers(self):
        """Handlers"""
        return self._processes["handlers"]

    def terminate(self):
        """Terminate the cluster"""
        # Send SIGTERM
        logger.debug("Sending SIGTERM")
        for process in self.processes:
            process.terminate()

        # Send SIGKILL
        logger.debug("Sending SIGKILL")
        for process in self.processes:
            process.kill()

    async def run(self) -> List[asyncio.subprocess.Process]:
        directory = lib.work_directory.WorkDirectory(self.work_directory)
        directory.setup()

        self._work_directory = directory.base_directory
        logger.info(f"Work directory: {directory.base_directory}")
        if not self._cluster_file:
            self._cluster_file = lib.cluster_file.generate_fdb_cluster_file(
                directory.base_directory
            )

        self._processes = await spawn_fdbservers(
            self._num_processes, directory, self.cluster_file, self._port
        )

        await configure_fdbserver(self._cluster_file)
        logger.info("FoundationDB ready to use")

        return self.processes

    async def __aenter__(self):
        """Enter the context

        :return _type_: _description_
        """
        await self.run()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        """Exit the context

        :param _type_ exc_type: _description_
        :param _type_ exc: _description_
        :param _type_ tb: _description_
        """
        self.terminate()
