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
        print('set(OPENSSL_FOUND ON)', file=out)
        print('set(OPENSSL_VERSION, "{}")'.format(self.fetch_source.version_string()), file=out)
        print('set(OPENSSL_INCLUDE_DIR "{}")'.format(str(include_path.absolute())), file=out)
        print('set(OPENSSL_CRYPTO_LIBRARY "{}")'.format(str(crypto_path.absolute())), file=out)
        print('set(OPENSSL_CRYPTO_LIBRARIES "{}")'.format(str(crypto_path.absolute())), file=out)
        print('set(OPENSSL_SSL_LIBRARY "{}")'.format(str(ssl_path.absolute())), file=out)
        print('set(OPENSSL_SSL_LIBRARIES "{}")'.format(str(ssl_path.absolute())), file=out)
        print('set(OPENSSL_LIBRARIES "{}")'.format(';'.join([str(ssl_path.absolute()), str(crypto_path.absolute())])),
              file=out)


def provide_module(out: TextIO, args: List[str]):
    build = OpenSSLBuild()
    build.print_target(out)
