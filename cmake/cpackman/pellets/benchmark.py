from typing import TextIO, List

from cpackman.pellets import CMakeBuild, HTTPSource
from cpackman.pellets.gtest import GTestBuild


class BenchmarkBuild(CMakeBuild):
    def __init__(self):
        self.gtest = GTestBuild()
        cmake_vars = {'GOOGLETEST_PATH': str(self.gtest.fetch_source.get_source().absolute())}
        super().__init__(HTTPSource(
            name='benchmark',
            version_str='1.6.0',
            url='https://github.com/google/benchmark/archive/refs/tags/v1.6.0.tar.gz',
            checksum='1f71c72ce08d2c1310011ea6436b31e39ccab8c2db94186d26657d41747c85d6',
            hash_function='sha256'
        ), cmake_vars=cmake_vars)


def provide_module(out: TextIO, args: List[str]):
    BenchmarkBuild().print_target(out)
