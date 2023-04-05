"""Maintains work directories for FDB """

import logging
import os
import os.path
import shutil
import tempfile

from typing import Union


logger = logging.getLogger(__name__)


class WorkDirectory:
    def __init__(
        self,
        base_directory: Union[str, None] = None,
        data_directory: str = "data/",
        log_directory: str = "log/",
        auto_cleanup: bool = False,
    ):

        """Constructor

        :param str base_directory: Base directory, if None, uses a temporary directory
        :param str data_directory: Data directory, related to base_directory
        :param str log_directory: Log directory, related to base_directory
        :param bool auto_cleanup: Automatically deletes the file defaults to False
        """
        self._data_directory_rel = data_directory
        self._log_directory_rel = log_directory
        self._base_directory = base_directory

        self._data_directory = None
        self._log_directory = None

        self._pwd = os.getcwd()

        self._auto_cleanup = auto_cleanup

    @property
    def base_directory(self) -> Union[str, None]:
        """Base directory

        :return str:
        """
        return self._base_directory

    @property
    def data_directory(self) -> Union[str, None]:
        """Data directory

        :return str:
        """
        if self._base_directory is None:
            return None
        return self._data_directory

    @property
    def log_directory(self) -> Union[str, None]:
        """Log directory

        :return str:
        """
        if self._base_directory is None:
            return None
        return self._log_directory

    def setup(self):
        """Set up the directories"""
        if self._base_directory is None:
            self._base_directory = tempfile.mkdtemp()
        assert self.base_directory is not None
        logger.debug(f"Work directory {self.base_directory}")

        self._data_directory = os.path.join(
            self._base_directory, self._data_directory_rel
        )
        assert self.data_directory is not None
        os.makedirs(self.data_directory, exist_ok=True)
        logger.debug(f"Created data directory {self.data_directory}")

        self._log_directory = os.path.join(
            self._base_directory, self._log_directory_rel
        )
        assert self.log_directory is not None
        os.makedirs(self.log_directory, exist_ok=True)

        os.chdir(self.base_directory)
        logger.debug(f"Created log directory {self.log_directory}")

    def teardown(self):
        """Tear down the directories"""
        assert self.base_directory is not None

        shutil.rmtree(self.base_directory)
        logger.debug(f"Cleaned up directory {self.base_directory}")

    def __enter__(self):
        """Enter the context"""
        self.setup()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context"""
        os.chdir(self._pwd)
        if self._auto_cleanup:
            self.teardown()
