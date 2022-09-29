from pathlib import Path
from typing import TextIO, List

from cpackman.pellets import CMakeBuild, HTTPSource


class ZSTDBuild(CMakeBuild):
    def __init__(self):
        super().__init__(HTTPSource(
            name='zstd',
            version_str='1.5.2',
            url='https://github.com/facebook/zstd/releases/download/v1.5.2/zstd-1.5.2.tar.gz',
            checksum='7c42d56fac126929a6a85dbc73ff1db2411d04f104fae9bdea51305663a83fd0',
            hash_function='sha256'),
            cmake_files_dir=Path('build') / 'cmake',
            cmake_vars={'ZSTD_BUILD_STATIC': 'ON', 'ZSTD_BUILD_SHARED': 'OFF'})


def provide_module(out: TextIO, args: List[str]):
    ZSTDBuild().print_target(out)
