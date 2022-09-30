from typing import List, TextIO

from cpackman.pellets import CMakeBuild, HTTPSource


class MsgPack(CMakeBuild):
    def __init__(self):
        super().__init__(HTTPSource(
            name='msgpack',
            version_str='3.3.0',
            url='https://github.com/msgpack/msgpack-c/releases/download/cpp-3.3.0/msgpack-3.3.0.tar.gz',
            checksum='6e114d12a5ddb8cb11f669f83f32246e484a8addd0ce93f274996f1941c1f07b',
            hash_function='sha256'
        ))

    def print_target(self, out: TextIO):
        print('set(msgpack_ROOT "{}")'.format(self.install().absolute()), file=out)
        print('find_package(msgpack CONFIG BYPASS_PROVIDER)', file=out)


def provide_module(out: TextIO, args: List[str]):
    MsgPack().print_target(out)
