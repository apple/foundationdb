from __future__ import annotations

import hashlib
import shutil
import sys
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Set

from cpackman import config, eprint


class FetchSource:
    def __init__(self, name: str, version_str: str):
        self.name = name
        self.version_str = version_str
        self.base_folder: Path = Path('cpackman')
        self.source_folders: Path = self.base_folder / 'sources'
        self.source_folders.mkdir(0o777, parents=True, exist_ok=True)

    def source_root(self) -> Path:
        return self.source_folders

    def version_string(self):
        return self.version_str

    def get_source(self) -> Path:
        raise NotImplemented()


class GitSource(FetchSource):
    def __init__(self, name: str, version_str: str, url: str, git_hash: str):
        super().__init__(name, version_str)
        self.url: str = url
        self.git_hash = git_hash

    def download(self):
        pass


class HTTPSource(FetchSource):
    def __init__(self, name: str, version_str: str, url: str, checksum: str, hash_function='sha1'):
        super().__init__(name, version_str)
        self.url = url
        self.checksum = checksum
        self.hash_function = hash_function
        self.file_path: Path | None = None

    def get_filepath(self) -> Path:
        if self.file_path is not None:
            return self.file_path
        req = urllib.request.Request(self.url, method='HEAD')
        with urllib.request.urlopen(req) as r:
            res = r.info().get_filename()
            if res is None:
                res = urllib.parse.urlsplit(self.url).path.split('/')[-1]
            assert res is not None
            self.file_path = self.source_folders / res
            return self.file_path

    def fetch(self):
        urllib.request.urlretrieve(self.url, self.get_filepath())

    def verify_checksum(self):
        with open(self.get_filepath(), 'rb') as file:
            data = file.read()
            hash_function = hashlib.new(self.hash_function)
            hash_function.update(data)
            if hash_function.hexdigest() != self.checksum:
                eprint('Hash for {} did not match. Expected "{}", got "{}"'.format(self.get_filepath(),
                                                                                   self.checksum,
                                                                                   hash_function.hexdigest()))
                sys.exit(1)

    def unpack(self) -> Path:
        dirname_file = Path('{}.dn'.format(self.get_filepath()))
        if dirname_file.exists():
            with open(dirname_file, 'r') as f:
                return Path(f.read())
        self.verify_checksum()
        dirs: Set[Path] = set()
        for f in self.source_folders.iterdir():
            if f.is_dir():
                dirs.add(f)
        shutil.unpack_archive(self.get_filepath(), self.source_folders)
        res: Path | None = None
        for f in self.source_folders.iterdir():
            if f.is_dir() and f not in dirs:
                assert res is None
                res = f
        with open(dirname_file, 'w') as f:
            f.write(str(res))
        return res

    def get_source(self) -> Path:
        if not self.get_filepath().exists():
            self.fetch()
        return self.unpack()


class Build:
    def __init__(self, fetch_source: FetchSource):
        self.fetch_source = fetch_source
        self.build_folder = Path('cpackman') / 'build' / fetch_source.name / self.build_id()
        self.install_folder = Path('cpackman') / 'install' / fetch_source.name / self.build_id()
        self.build_folder.mkdir(0o777, parents=True, exist_ok=True)

    def build_id(self) -> str:
        m = hashlib.sha1()
        m.update(config.c_compiler_id.encode())
        m.update(config.c_compiler_version.encode())
        m.update(config.cxx_compiler_id.encode())
        m.update(config.cxx_compiler_version.encode())
        m.update(self.fetch_source.name.encode())
        m.update(self.fetch_source.version_string().encode())
        return m.hexdigest()

    def build(self) -> None:
        raise NotImplemented()
