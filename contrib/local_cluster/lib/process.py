""" Process management """

import asyncio
import logging

from typing import Dict, List, Union

logger = logging.getLogger(__name__)


class Process:
    """Maintain a process as coroutine"""

    def __init__(
        self, executable: str, arguments: List[str] = None, env: Dict[str, str] = None
    ):
        """Constructor
        :param str executable: Path to the executable
        :param List[str] arguments: arguments
        """
        self._executable = executable
        self._args = arguments or []
        self._env = env

        self._process: asyncio.subprocess.Process = None

    async def run(self) -> asyncio.subprocess.Process:
        logger.debug(
            f"Spawning process [{self._executable} {' '.join(str(arg) for arg in self._args)}]"
        )
        self._process = await asyncio.subprocess.create_subprocess_exec(
            self._executable,
            *self._args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=self._env,
        )

        return self._process

    @property
    def pid(self) -> Union[int, None]:
        """Return the pid of the process
        :return: The PID, or None if not running
        :rtype: int | None
        """
        if self._process is None:
            return None
        return self._process.pid

    def kill(self):
        """Kill the process
        :raises RuntimeError: if not running
        """
        if self._process is None:
            raise RuntimeError("Not running")
        self._process.kill()

    def terminate(self):
        """Terminate the process
        :raises RuntimeError: if not running
        """
        if self._process is None:
            raise RuntimeError("Not running")
        self._process.terminate()

    def return_code(self) -> Union[int, None]:
        """Get the return code
        :return: The return code, if the process is terminated; otherwise None
        :rtype: int | None
        """
        if self._process is None:
            return None
        return self._process.returncode

    def is_running(self) -> bool:
        """Check if is running
        :return: True if still running
        :rtype: bool
        """
        return self.pid is not None and self.return_code is None
