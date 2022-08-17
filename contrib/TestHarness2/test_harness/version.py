from functools import total_ordering
from pathlib import Path
from typing import Tuple


@total_ordering
class Version:
    def __init__(self):
        self.major: int = 0
        self.minor: int = 0
        self.patch: int = 0

    def version_tuple(self):
        return self.major, self.minor, self.patch

    def _compare(self, other) -> int:
        lhs: Tuple[int, int, int] = self.version_tuple()
        rhs: Tuple[int, int, int]
        if isinstance(other, Version):
            rhs = other.version_tuple()
        else:
            rhs = Version.parse(str(other)).version_tuple()
        if lhs < rhs:
            return -1
        elif lhs > rhs:
            return 1
        else:
            return 0

    def __eq__(self, other) -> bool:
        return self._compare(other) == 0

    def __lt__(self, other) -> bool:
        return self._compare(other) < 0

    def __hash__(self):
        return hash(self.version_tuple())

    def __str__(self):
        return format('{}.{}.{}'.format(self.major, self.minor, self.patch))

    @staticmethod
    def of_binary(binary: Path):
        parts = binary.name.split('-')
        if len(parts) != 2:
            return Version.max_version()
        return Version.parse(parts[1])

    @staticmethod
    def parse(version: str):
        version_tuple = version.split('.')
        self = Version()
        self.major = int(version_tuple[0])
        if len(version_tuple) > 1:
            self.minor = int(version_tuple[1])
            if len(version_tuple) > 2:
                self.patch = int(version_tuple[2])
        return self

    @staticmethod
    def max_version():
        self = Version()
        self.major = 2**32 - 1
        self.minor = 2**32 - 1
        self.patch = 2**32 - 1
        return self
