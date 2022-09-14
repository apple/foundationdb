from __future__ import annotations

import hashlib
import multiprocessing
import os
import shutil
import subprocess
import sys
import urllib.parse
import urllib.request
from copy import copy
from pathlib import Path
from typing import Set, List, TextIO

from cpackman import config, eprint


def run_command(cmd: List[str], env=os.environ, cwd=None):
    with subprocess.Popen(cmd, stdout=None, stderr=None, env=env, cwd=cwd) as process:
        process.wait()
        if process.returncode != 0:
            raise RuntimeError('Command Failed')


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
        self.install_folder.mkdir(0o777, parents=True, exist_ok=True)
        self.env = copy(os.environ)
        if config.c_compiler is not None:
            self.env['CC'] = config.c_compiler
        if config.cxx_compiler is not None:
            self.env['CXX'] = config.cxx_compiler

    def build_id(self) -> str:
        m = hashlib.sha1()
        m.update(config.c_compiler_id.encode())
        m.update(config.c_compiler_version.encode())
        m.update(config.cxx_compiler_id.encode())
        m.update(config.cxx_compiler_version.encode())
        m.update(self.fetch_source.name.encode())
        m.update(self.fetch_source.version_string().encode())
        return m.hexdigest()

    def configure(self) -> None:
        configure_done_file = self.build_folder / '.cpackman_configure_done'
        if configure_done_file.exists():
            return
        else:
            self.run_configure()
            with open(configure_done_file, 'w'):
                pass

    def build(self) -> None:
        build_done_file = self.build_folder / '.cpackman_build_done'
        if build_done_file.exists():
            return
        else:
            self.run_build()
            with open(build_done_file, 'w'):
                pass

    def install(self) -> Path:
        install_done_file = self.install_folder / '.cpackman_install_done'
        if install_done_file.exists():
            return self.install_folder
        else:
            self.configure()
            self.build()
            self.run_install()
            with open(install_done_file, 'w'):
                return self.install_folder

    def run_configure(self) -> None:
        raise NotImplemented()

    def run_build(self) -> None:
        raise NotImplemented()

    def run_install(self) -> None:
        raise NotImplemented()

    def print_target(self, out: TextIO):
        raise NotImplemented()


class ConfigureMake(Build):
    def __init__(self, fetch_source: FetchSource,
                 additional_configure_args: List[str] | None = None,
                 additional_make_args: List[str] | None = None,
                 additional_install_args: List[str] | None = None):
        super().__init__(fetch_source)
        self.additional_configure_args: List[str] = []
        if additional_configure_args is not None:
            self.additional_configure_args = additional_configure_args
        self.additional_make_args: List[str] = []
        if additional_make_args is not None:
            self.additional_make_args = additional_make_args
        self.additional_install_args: List[str] = []
        if additional_install_args is not None:
            self.additional_install_args = additional_install_args

    def build_id(self) -> str:
        res = super().build_id()
        m = hashlib.sha1()
        m.update(res)
        # we could add all args to the checksum which would be simpler. But we want to make sure that if an argument is
        # moved from one build step to another that they will be distinct builds
        for arg in self.additional_configure_args:
            m.update("config-arg: {}".format(arg).encode())
        for arg in self.additional_make_args:
            m.update("make-arg: {}".format(arg).encode())
        for arg in self.additional_install_args:
            m.update("install-arg: {}".format(arg).encode())
        return m.hexdigest()

    def run_configure(self) -> None:
        cmd = ["{}/configure".format(self.fetch_source.get_source().absolute()),
               '--prefix={}'.format(self.install_folder.absolute())]
        cmd += self.additional_configure_args if self.additional_configure_args is not None else []
        run_command(cmd, env=self.env, cwd=self.build_folder)

    def run_build(self) -> None:
        cmd = ['make', '-j{}'.format(multiprocessing.cpu_count())]
        cmd += self.additional_make_args if self.additional_make_args is not None else []
        run_command(cmd, env=self.env, cwd=self.build_folder)

    def run_install(self) -> None:
        cmd = ['make', 'install']
        cmd += self.additional_install_args if self.additional_install_args is not None else []
        run_command(cmd, env=self.env, cwd=self.build_folder)


def add_static_library(out: TextIO, target: str, include_dirs: List[Path], library_path: Path, link_language: str):
    assert link_language in ['C', 'CXX']
    include_list: List[str] = []
    for include in include_dirs:
        include_list.append(str(include.absolute()))
    include_cmake_list = ';'.join(include_list)
    print('add_library({} STATIC IMPORTED)'.format(target), file=out)
    print('set_target_properties({} PROPERTIES\n'
          '  INTERFACE_INCLUDE_DIRECTORIES {}\n'
          '  IMPORTED_LINK_INTERFACE_LANGUAGES "{}"\n'
          '  IMPORTED_LOCATION "{}")'.format(target, include_cmake_list, link_language, str(library_path.absolute())),
          file=out)
