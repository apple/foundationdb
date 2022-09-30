from typing import TextIO, List

from cpackman.pellets import CMakeBuild, HTTPSource


class GTestBuild(CMakeBuild):
    def __init__(self):
        super().__init__(HTTPSource(
            name='GTest',
            version_str='1.8.1',
            url='https://github.com/google/googletest/archive/refs/tags/release-1.8.1.tar.gz',
            checksum='9bf1fe5182a604b4135edc1a425ae356c9ad15e9b23f9f12a02e80184c3a249c',
            hash_function='sha256'
        ))


def provide_module(out: TextIO, args: List[str]):
    GTestBuild().print_target(out)
