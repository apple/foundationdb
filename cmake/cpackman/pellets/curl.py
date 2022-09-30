import hashlib
from typing import List, TextIO

from cpackman.pellets import CMakeBuild, HTTPSource
from cpackman.pellets.openssl import OpenSSLBuild
from cpackman.pellets.zlib import ZLIB


class CurlBuild(CMakeBuild):
    def __init__(self):
        self.openssl = OpenSSLBuild()
        self.zlib = ZLIB()
        openssl = str(self.openssl.install().absolute())
        zlib = str(self.zlib.install().absolute())
        cmake_vars = {'OpenSSL_ROOT': openssl,
                      'ZLIB_ROOT': zlib,
                      'BUILD_SHARED_LIBS': 'OFF'}
        super().__init__(HTTPSource(
            name='CURL',
            version_str='7.85.0',
            url='https://curl.se/download/curl-7.85.0.tar.bz2',
            checksum='21a7e83628ee96164ac2b36ff6bf99d467c7b0b621c1f7e317d8f0d96011539c',
            hash_function='sha256'), cmake_vars=cmake_vars)

    def build_id(self) -> str:
        m = hashlib.sha1()
        m.update(super().build_id().encode())
        m.update(self.zlib.build_id().encode())
        m.update(self.openssl.build_id().encode())
        return m.hexdigest()


def provide_module(out: TextIO, args: List[str]):
    curl = CurlBuild().install().absolute()
    print('set(CURL_ROOT, "{}")'.format(curl), file=out)
    print('find_package(CURL CONFIG BYPASS_PROVIDER)', file=out)
