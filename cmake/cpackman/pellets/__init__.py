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
from typing import Set, List, TextIO, Dict

from cpackman import config, eprint


def run_command(cmd: List[str], env=os.environ, cwd=None):
    print("Run Command: {}".format(' '.join(cmd)))
    sys.stdout.flush()
    with subprocess.Popen(cmd, stdout=None, stderr=None, env=env, cwd=cwd) as process:
        process.wait()
        if process.returncode != 0:
            print("Command: {}".format(' '.join(cmd)))
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

    def get_source_impl(self) -> Path:
        raise NotImplementedError()

    def get_source(self) -> Path:
        pathfile = self.source_folders / '{}-{}.cpackman'.format(self.name, self.version_str)
        if pathfile.exists():
            with open(pathfile, 'r') as f:
                data = f.read().rstrip()
                return Path(data)
        else:
            res = self.get_source_impl().absolute()
            data = str(res)
            with open(pathfile, 'w') as f:
                print(data, file=f, end='')
            return res


class GitSource(FetchSource):
    def __init__(self, name: str, version_str: str, url: str, git_hash: str, init_submodules: bool = False):
        super().__init__(name, version_str)
        self.url = url
        self.git_hash = git_hash
        self.init_submodules = init_submodules

    def get_source_impl(self) -> Path:
        folder_name = '{}-{}'.format(self.name, self.version_str)
        res = self.source_folders / folder_name
        clone_cmd = ['git', 'clone', self.url, folder_name]
        run_command(clone_cmd, cwd=self.source_folders)
        checkout_cmd = ['git', 'checkout', self.git_hash]
        run_command(checkout_cmd, cwd=res)
        if self.init_submodules:
            init_submodules_cmd = ['git', 'submodule', 'update', '--init', '--recursive']
            run_command(init_submodules_cmd, cwd=res)
        return res


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
            with open(dirname_file, 'r') as file:
                return Path(file.read())
        self.verify_checksum()
        dirs: Set[Path] = set()
        for path in self.source_folders.iterdir():
            if path.is_dir():
                dirs.add(path)
        shutil.unpack_archive(self.get_filepath(), self.source_folders)
        res: Path | None = None
        for path in self.source_folders.iterdir():
            if path.is_dir() and path not in dirs:
                assert res is None
                res = path
        with open(dirname_file, 'w') as file:
            file.write(str(res))
        assert res is not None
        return res

    def get_source_impl(self) -> Path:
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
        raise NotImplementedError()

    def run_build(self) -> None:
        raise NotImplementedError()

    def run_install(self) -> None:
        raise NotImplementedError()

    def print_target(self, out: TextIO):
        raise NotImplementedError()


class CMakeBuild(Build):
    def __init__(self, fetch_source: FetchSource,
                 build_type: str = os.getenv('CMAKE_BUILD_TYPE'),
                 cmake_vars: Dict[str, str] | None = None,
                 cmake_files_dir: Path | None = None):
        self.cmake_files_dir = cmake_files_dir
        self.cmake_command = os.getenv('CMAKE_COMMAND')
        assert self.cmake_command is not None
        self.cmake_version = os.getenv('CMAKE_VERSION')
        assert self.cmake_version is not None
        assert build_type is not None
        self.build_type = build_type
        self.cmake_vars = cmake_vars if cmake_vars is not None else {}
        super().__init__(fetch_source)

    def build_id(self) -> str:
        m = hashlib.sha1()
        m.update(super().build_id().encode())
        m.update(self.cmake_version.encode())
        m.update(self.cmake_command.encode())
        m.update(self.build_type.encode())
        if self.cmake_files_dir is not None:
            m.update(str(self.cmake_files_dir.absolute()).encode())
        for k, v in self.cmake_vars.items():
            m.update('-D{}={}'.format(k, v).encode())
        return m.hexdigest()

    def run_configure(self) -> None:
        src_dir = self.fetch_source.get_source().absolute()
        if self.cmake_files_dir is not None:
            src_dir = src_dir / self.cmake_files_dir
        cmd = [self.cmake_command,
               '-G',
               'Ninja',
               '-DCMAKE_BUILD_TYPE={}'.format(self.build_type),
               '-DCMAKE_INSTALL_PREFIX={}'.format(self.install_folder.absolute())]
        for k, v in self.cmake_vars.items():
            assert k not in ('CMAKE_INSTALL_PREFIX', 'CMAKE_BUILD_TYPE')
            cmd.append('-D{}={}'.format(k, v))
        cmd.append(str(src_dir.absolute()))
        run_command(cmd, env=self.env, cwd=self.build_folder)

    def run_build(self) -> None:
        cmd = ['ninja']
        run_command(cmd, env=self.env, cwd=self.build_folder)

    def run_install(self) -> None:
        cmd = ['ninja', 'install']
        run_command(cmd, env=self.env, cwd=self.build_folder)

    def print_target(self, out: TextIO):
        print('set({}_ROOT "{}")'.format(self.fetch_source.name, self.install().absolute()), file=out)
        print('find_package({} CONFIG BYPASS_PROVIDER)'.format(self.fetch_source.name), file=out)


class ConfigureMake(Build):
    def __init__(self, fetch_source: FetchSource,
                 additional_configure_args: List[str] | None = None,
                 additional_make_args: List[str] | None = None,
                 additional_install_args: List[str] | None = None):
        self.additional_configure_args: List[str] = []
        if additional_configure_args is not None:
            self.additional_configure_args = additional_configure_args
        self.additional_make_args: List[str] = []
        if additional_make_args is not None:
            self.additional_make_args = additional_make_args
        self.additional_install_args: List[str] = []
        if additional_install_args is not None:
            self.additional_install_args = additional_install_args
        super().__init__(fetch_source)

    def build_id(self) -> str:
        res = super().build_id()
        m = hashlib.sha1()
        m.update(res.encode())
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

    def print_target(self, out: TextIO):
        raise NotImplementedError()


def add_static_library(out: TextIO, target: str,
                       include_dirs: List[Path] | None,
                       library_path: Path,
                       link_language: str):
    assert link_language in ['C', 'CXX']
    properties: List[str] = ['IMPORTED_LINK_INTERFACE_LANGUAGES "{}"'.format(link_language),
                             'IMPORTED_LOCATION "{}"'.format(library_path.absolute())]
    if include_dirs is not None:
        include_str = ' '.join(map(lambda x: '"{}"'.format(x.absolute()), include_dirs))
        properties.append('INTERFACE_INCLUDE_DIRECTORIES {}'.format(include_str))
    print('add_library({} STATIC IMPORTED)'.format(target), file=out)
    print('set_target_properties({} PROPERTIES'.format(target), file=out, end='')
    for prop in properties:
        print('\n  {}'.format(prop), file=out, end='')
    print(')', file=out)
