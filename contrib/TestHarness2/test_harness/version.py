class Version:
    def __init__(self):
        self.major: int = 0
        self.minor: int = 0
        self.patch: int = 0

    def version_tuple(self):
        return self.major, self.minor, self.patch

    def __eq__(self, other):
        return self.version_tuple() == other.version_tuple()

    def __lt__(self, other):
        return self.version_tuple() < other.version_tuple()

    def __le__(self, other):
        return self.version_tuple() <= other.version_tuple()

    def __gt__(self, other):
        return self.version_tuple() > other.version_tuple()

    def __ge__(self, other):
        return self.version_tuple() >= other.version_tuple()

    def __hash__(self):
        return hash(self.version_tuple())

    @staticmethod
    def parse(version: str):
        version_tuple = version.split('.')
        self = Version()
        self.major = int(version_tuple[0])
        if len(version_tuple) > 1:
            self.minor = int(version_tuple[1])
            if len(version_tuple) > 2:
                self.patch = int(version_tuple[2])

    @staticmethod
    def max_version():
        self = Version()
        self.major = 2**32 - 1
        self.minor = 2**32 - 1
        self.patch = 2**32 - 1
