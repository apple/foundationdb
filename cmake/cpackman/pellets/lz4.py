from pathlib import Path
from typing import List, TextIO

from cpackman.pellets import CMakeBuild, HTTPSource


class LZ4Build(CMakeBuild):
    def __init__(self):
        cmake_vars = {'BUILD_SHARED_LIBS': 'OFF', 'BUILD_STATIC_LIBS': 'ON'}
        super().__init__(HTTPSource(
            name='lz4',
            version_str='1.9.4',
            url='https://github.com/lz4/lz4/archive/refs/tags/v1.9.4.tar.gz',
            checksum='0b0e3aa07c8c063ddf40b082bdf7e37a1562bda40a0ff5272957f3e987e0e54b',
            hash_function='sha256'
        ), cmake_files_dir=Path('build') / 'cmake', cmake_vars=cmake_vars)


def provide_module(out: TextIO, args: List[str]):
    LZ4Build().print_target(out)
