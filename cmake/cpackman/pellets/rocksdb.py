import os
from typing import TextIO, List

from cpackman.pellets import CMakeBuild, HTTPSource
from cpackman.pellets.lz4 import LZ4Build


class RocksDBBuild(CMakeBuild):
    def __init__(self):
        self.lz4 = LZ4Build()
        cmake_vars = {'WITH_GFLAGS': 'OFF',
                      'WITH_TESTS': 'OFF',
                      'WITH_TOOLS': 'OFF',
                      'WITH_CORE_TOOLS': 'OFF',
                      'WITH_BENCHMARK_TOOLS': 'OFF',
                      'WITH_BZ2': 'OFF',
                      'WITH_LZ4': 'ON',
                      'WITH_SNAPPY': 'OFF',
                      'WITH_ZLIB': 'OFF',
                      'WITH_ZSTD': 'OFF',
                      'CMAKE_PREFIX_PATH': str(self.lz4.install().absolute()),
                      'ROCKSDB_BUILD_SHARED': 'OFF',
                      'CMAKE_POSITION_INDEPENDENT_CODE': 'True',
                      'FAIL_ON_WARNINGS': 'OFF',
                      'WITH_LIBURING': os.getenv('WITH_LIBURING', 'OFF'),
                      'WITH_TSAN': os.getenv('USE_TSAN', 'OFF'),
                      'WITH_ASAN': os.getenv('USE_ASAN', 'OFF'),
                      'WITH_UBSAN': os.getenv('USE_UBSAN', 'OFF')}
        super().__init__(HTTPSource(
            name='RocksDB',
            version_str='6.27.3',
            url='https://github.com/facebook/rocksdb/archive/refs/tags/v6.27.3.tar.gz',
            checksum='ee29901749b9132692b26f0a6c1d693f47d1a9ed8e3771e60556afe80282bf58',
            hash_function='sha256'
        ), cmake_vars=cmake_vars)

    def print_target(self, out: TextIO):
        self.lz4.print_target(out)
        super().print_target(out)
        print('add_library(lz4::lz4 ALIAS LZ4::lz4_static)', file=out)


def provide_module(out: TextIO, args: List[str]):
    RocksDBBuild().print_target(out)
