from __future__ import annotations

import atexit
import os
import sys


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def _env_bool(name: str, default: bool = False) -> bool:
    res: str | None = os.getenv(name)
    if res is None:
        return default
    else:
        r = res.lower()
        if r in ['true', '1', 'on']:
            return True
        else:
            return False


class Config:
    def __init__(self):
        if sys.platform.startswith('darwin'):
            self.c_compiler = 'clang'
            self.cxx_compiler = 'clang++'
        else:
            self.c_compiler = os.getenv('CC')
            self.cxx_compiler = os.getenv('CXX')
        self.c_compiler_id = os.getenv('C_COMPILER_ID')
        self.c_compiler_version = os.getenv('C_COMPILER_VERSION')
        self.cxx_compiler_id = os.getenv('CXX_COMPILER_ID')
        self.cxx_compiler_version = os.getenv('CXX_COMPILER_VERSION')
        self.cxx_stdlib = os.getenv('CXX_STDLIB')
        self.use_asan = _env_bool('USE_ASAN')
        self.use_tsan = _env_bool('USE_TSAN')
        self.use_msan = _env_bool('USE_MSAN')
        self.use_ubsan = _env_bool('USE_UBSAN')


config = Config()
