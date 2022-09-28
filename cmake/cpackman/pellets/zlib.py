from typing import TextIO

from cpackman.pellets import ConfigureMake, HTTPSource


class ZLIB(ConfigureMake):
    def print_target(self, out: TextIO):
        print('set(ZLIB_USE_STATIC_LIBS ON)', file=out)
        print('set(ZLIB_ROOT "{}"'.format(self.install().absolute()), file=out)
        print('find_package(ZLIB)')

    def __init__(self):
        super().__init__(HTTPSource(
            name='ZLIB',
            version_str='1.2.12',
            url='https://zlib.net/zlib-1.2.12.tar.gz',
            checksum='91844808532e5ce316b3c010929493c0244f3d37593afd6de04f71821d5136d9',
            hash_function='sha256'
        ), additional_configure_args=['--static'])
