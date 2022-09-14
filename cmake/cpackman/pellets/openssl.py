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
        self.env = copy(os.environ)
        if config.c_compiler is not None:
            self.env['CC'] = config.c_compiler
        super().__init__(source)

    def configure(self) -> None:
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

    def build(self) -> None:
        self.configure()
        run_command(['make', '-j{}'.format(multiprocessing.cpu_count())], env=self.env, cwd=self.build_folder)

    def install(self) -> Path:
        self.build()
        run_command(['make', 'install_sw'], env=self.env, cwd=self.build_folder)
        return self.install_folder

    def print_target(self, out: TextIO):
        install_path = self.install()
        include_path = install_path / 'include'
        ssl_path = install_path / 'lib' / 'libssl.a'
        crypto_path = install_path / 'lib' / 'libcrypto.a'
        add_static_library(out, 'OpenSSL::SSL',
                           include_dirs=[include_path],
                           library_path=ssl_path,
                           link_language='C')
        add_static_library(out, 'OpenSSL::Crypto',
                           include_dirs=[include_path],
                           library_path=crypto_path,
                           link_language='C')
        print('set(OpenSSL_FOUND ON PARENT_SCOPE)', file=out)


def provide_module(out: TextIO, args: List[str]):
    build = OpenSSLBuild()
    build.print_target(out)
