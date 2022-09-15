import sys
from typing import TextIO, List

from cpackman import FindPackageArgs, config
from cpackman.pellets import Build, HTTPSource, run_command, add_static_library


class BoostBuild(Build):
    def __init__(self, args: List[str]):
        super().__init__(
            HTTPSource(
                'Boost',
                version_str='1.78.0',
                url='https://boostorg.jfrog.io/artifactory/main/release/1.78.0/source/boost_1_78_0.tar.bz2',
                checksum='8681f175d4bdb26c52222665793eef08490d7758529330f98d3b29dd0735bccc',
                hash_function='sha256'))
        self.args = FindPackageArgs(args)
        self.toolset = 'clang' if config.cxx_compiler_id in ['Clang', 'AppleClang', 'Intel'] else 'gcc'
        self.compiler = config.cxx_compiler
        compiler_flags = ['-fvisibility=hidden', '-fPIC', '-std=c++17', '-w']
        linker_flags = []
        if sys.platform.startswith('darwin') or config.cxx_stdlib == 'libc++':
            compiler_flags.append("-stdlib=libc++")
            compiler_flags.append("-nostdlib++")
            linker_flags.append("-lc++")
            linker_flags.append('-lc++abi')
            if not sys.platform.startswith('darwin'):
                linker_flags.append('-static-libgcc')
        self.compiler_flags = ' '.join(map(lambda flag: '<cxxflags>{}'.format(flag), compiler_flags))
        self.linker_flags = ' '.join(map(lambda flag: '<linkflags>{}'.format(flag), linker_flags))

    def run_configure(self) -> None:
        if len(self.args.components) == 0:
            return
        source_folder = self.fetch_source.get_source().absolute()
        configure_command: List[str] = [
            '{}/bootstrap.sh'.format(source_folder),
            '--with-libraries={}'.format(','.join(self.args.components)),
            '--with-toolset={}'.format(self.toolset)]
        run_command(configure_command, env=self.env, cwd=self.build_folder.absolute())

    def run_build(self) -> None:
        if len(self.args.components) == 0:
            return
        jam_file = '{}/user-config.jam'.format(self.build_folder.absolute())
        with open(jam_file, 'w') as user_jam:
            print("using {} : "
                  ": {} :"
                  " {} {} ;".format(self.toolset, self.compiler, self.compiler_flags, self.linker_flags),
                  file=user_jam)
        build_command = ['./b2',
                         'link=static',
                         '--prefix={}'.format(self.install_folder.absolute()),
                         '--user-config={}'.format(jam_file),
                         'install']
        run_command(build_command, env=self.env, cwd=self.build_folder.absolute())

    def run_install(self) -> None:
        pass

    def print_target(self, out: TextIO):
        print('add_library(Boost::boost INTERFACE)', file=out)
        install_dir = self.install()
        if len(self.args.components) == 0:
            include_dir = self.fetch_source.source_root().absolute()
        else:
            include_dir = (install_dir / 'include').absolute()
        print('target_include(Boost::boost SYSTEM INTERFACE {})'.format(include_dir), file=out)
        for component in self.args.components:
            add_static_library(
                out,
                target='Boost::{}'.format(component),
                include_dirs=None,
                link_language='CXX',
                library_path=install_dir / 'lib' / 'libboost_{}.a'.format(component))
        print('set(Boost_FOUND ON)', file=out)


def provide_module(out: TextIO, args: List[str]):
    build = BoostBuild(args)
    build.print_target(out)
