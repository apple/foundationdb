import hashlib
from typing import TextIO, List

from cpackman.pellets import CMakeBuild, GitSource
from cpackman.pellets.curl import CurlBuild
from cpackman.pellets.openssl import OpenSSLBuild
from cpackman.pellets.zlib import ZLIB


class AWSSDK(CMakeBuild):
    def __init__(self):
        self.openssl = OpenSSLBuild()
        self.zlib = ZLIB()
        self.curl = CurlBuild()
        openssl = str(self.openssl.install().absolute())
        zlib = str(self.zlib.install().absolute())
        curl = str(self.curl.install().absolute())
        cmake_vars = {
            'BUILD_SHARED_LIBS': 'OFF',
            'ENABLE_TESTING': 'OFF',
            'BUILD_ONLY': 'core',
            'OpenSSL_ROOT': openssl,
            'CMAKE_PREFIX_PATH': '{};{};{}'.format(zlib, curl, openssl)
        }
        super().__init__(GitSource(
            name='aws-sdk-cpp',
            version_str='1.9.331',
            git_hash='e4b4b310d8631bc7e9a797b6ac03a73c6f210bf6',
            url='https://github.com/aws/aws-sdk-cpp.git',
            init_submodules=True
        ), cmake_vars=cmake_vars)

    def build_id(self) -> str:
        m = hashlib.sha1()
        m.update(super().build_id().encode())
        # curl depends on zlib and openssl, so no need to add their build ids here
        m.update(self.curl.build_id().encode())
        return m.hexdigest()

    def print_target(self, out: TextIO):
        print('set(AWSSDK_ROOT "{}")'.format(self.install().absolute()), file=out)
        print('find_package(AWSSDK BYPASS_PROVIDER)', file=out)


def provide_module(out: TextIO, args: List[str]):
    return AWSSDK().print_target(out)