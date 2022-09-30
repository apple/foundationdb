import hashlib
import multiprocessing
import os
from copy import copy
from pathlib import Path
from typing import List, TextIO
from cpackman.pellets import Build, HTTPSource, run_command, add_static_library
from cpackman import config


class OpenSSLBuild(Build):
    def __init__(self):
        source = HTTPSource(name='OpenSSL',
                            version_str='1.1.1q',
                            url='https://www.openssl.org/source/openssl-1.1.1q.tar.gz',
                            checksum='79511a8f46f267c533efd32f22ad3bf89a92d8e5')
        super().__init__(source)

    def build_id(self) -> str:
        m = hashlib.sha1()
        m.update(super().build_id().encode())
        if config.use_asan:
            m.update('enable-asan'.encode())
        if config.use_ubsan:
            m.update('enable-ubsan'.encode())
        return m.hexdigest()

    def run_configure(self) -> None:
        source_folder = self.fetch_source.get_source()
        openssl_dir = self.install_folder / 'ssl'
        configure_command = ['{}/config'.format(str(source_folder.absolute())),
                             '--prefix={}'.format(str(self.install_folder.absolute())),
                             '--openssldir={}'.format(str(openssl_dir.absolute())),
                             'no-shared',
                             'no-tests']
        if config.use_asan:
            configure_command += 'enable-asan'
        if config.use_ubsan:
            configure_command += 'enable-ubsan'
        run_command(configure_command, env=self.env, cwd=self.build_folder)

    def run_build(self) -> None:
        run_command(['make', '-j{}'.format(multiprocessing.cpu_count())], env=self.env, cwd=self.build_folder)

    def run_install(self) -> None:
        run_command(['make', 'install_sw'], env=self.env, cwd=self.build_folder)

    def print_target(self, out: TextIO):
        install_path = self.install()
        print('set(OPENSSL_ROOT_DIR "{}")'.format(install_path.absolute()), file=out)
        print('find_package(OpenSSL REQUIRED BYPASS_PROVIDER)', file=out)


def provide_module(out: TextIO, args: List[str]):
    build = OpenSSLBuild()
    build.print_target(out)
