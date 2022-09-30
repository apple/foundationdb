from typing import TextIO, List

from cpackman.pellets import CMakeBuild, HTTPSource
from cpackman.pellets.gtest import GTestBuild


class Toml11Build(CMakeBuild):
    def __init__(self):
        cmake_vars = {'toml11_BUILD_TEST': 'OFF'}
        super().__init__(HTTPSource(
            name='toml11',
            version_str='3.4.0',
            url='https://github.com/ToruNiina/toml11/archive/v3.4.0.tar.gz',
            checksum='bc6d733efd9216af8c119d8ac64a805578c79cc82b813e4d1d880ca128bd154d',
            hash_function='sha256'
        ), cmake_vars=cmake_vars)


def provide_module(out: TextIO, args: List[str]):
    Toml11Build().print_target(out)
