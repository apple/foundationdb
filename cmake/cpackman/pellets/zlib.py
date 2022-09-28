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
            checksum='173e89893dcb8b4a150d7731cd72f0602f1d6b45e60e2a54efdf7f3fc3325fd7'
        ), additional_configure_args=['--static'])
