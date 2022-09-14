import os
import sys


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


class Config:
    def __init__(self):
        self.c_compiler = os.getenv('CC')
        self.cxx_compiler = os.getenv('CXX')
        self.c_compiler_id = os.getenv('C_COMPILER_ID')
        self.c_compiler_version = os.getenv('C_COMPILER_VERSION')
        self.cxx_compiler_id = os.getenv('CXX_COMPILER_ID')
        self.cxx_compiler_version = os.getenv('CXX_COMPILER_VERSION')


config = Config()
