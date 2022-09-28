from typing import List, TextIO

from cpackman.pellets import CMakeBuild, HTTPSource
from cpackman.pellets.openssl import OpenSSLBuild
from cpackman.pellets.zlib import ZLIB


def provide_module(out: TextIO, args: List[str]):
    openssl = str(OpenSSLBuild().install().absolute())
    zlib = str(ZLIB().install().absolute())
    curl = CMakeBuild(HTTPSource(
        name='CURL',
        version_str='7.85.0',
        url='https://curl.se/download/curl-7.85.0.tar.bz2',
        checksum='21a7e83628ee96164ac2b36ff6bf99d467c7b0b621c1f7e317d8f0d96011539c',
        hash_function='sha256'),
        cmake_vars={'OpenSSL_ROOT': openssl, 'ZLIB_ROOT': zlib}).install().absolute()
    print('set(CURL_ROOT, "{}")'.format(curl), file=out)
    print('find_package(CURL CONFIG BYPASS_PROVIDER)', file=out)
