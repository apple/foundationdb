from typing import List, TextIO

from cpackman.pellets import ConfigureMake, HTTPSource, add_static_library


class Jemalloc(ConfigureMake):
    def __init__(self):
        super().__init__(HTTPSource('jemalloc',
                                    version_str='5.3.0',
                                    url='https://github.com/jemalloc/jemalloc/releases/download/5.3.0/jemalloc-5.3.0.tar.bz2',
                                    hash_function='sha256',
                                    checksum='2db82d1e7119df3e71b7640219b6dfe84789bc0537983c3b7ac4f7189aecfeaa'),
                         additional_configure_args=['--enable-static', '--disable-cxx', '--enable-prof'])

    def print_target(self, out: TextIO):
        install_path = self.install()
        include_path = (install_path / 'include').absolute()
        jemalloc_path = (install_path / 'lib' / 'libjemalloc.a')
        jemalloc_pic_path = (install_path / 'lib' / 'libjemalloc_pic.a')
        print('set(jemalloc_FOUND ON)', file=out)
        add_static_library(out, 'jemalloc',
                           include_dirs=[include_path],
                           library_path=jemalloc_path,
                           link_language='C')
        add_static_library(out, 'jemalloc_pic',
                           include_dirs=[include_path],
                           library_path=jemalloc_pic_path,
                           link_language='C')
        print('target_link_libraries(jemalloc INTERFACE jemalloc_pic)')


def provide_module(out: TextIO, args: List[str]):
    build = Jemalloc()
    build.print_target(out)
