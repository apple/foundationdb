from __future__ import annotations

import os
import sys
from typing import List, Dict, Callable


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


# Parses the cmake arguments. The cmake signature is (as of cmake 3.24):
# find_package(<PackageName> [version] [EXACT] [QUIET]
#              [REQUIRED] [[COMPONENTS] [components...]]
#              [OPTIONAL_COMPONENTS components...]
#              [CONFIG|NO_MODULE]
#              [GLOBAL]
#              [NO_POLICY_SCOPE]
#              [BYPASS_PROVIDER]
#              [NAMES name1 [name2 ...]]
#              [CONFIGS config1 [config2 ...]]
#              [HINTS path1 [path2 ... ]]
#              [PATHS path1 [path2 ... ]]
#              [REGISTRY_VIEW  (64|32|64_32|32_64|HOST|TARGET|BOTH)]
#              [PATH_SUFFIXES suffix1 [suffix2 ...]]
#              [NO_DEFAULT_PATH]
#              [NO_PACKAGE_ROOT_PATH]
#              [NO_CMAKE_PATH]
#              [NO_CMAKE_ENVIRONMENT_PATH]
#              [NO_SYSTEM_ENVIRONMENT_PATH]
#              [NO_CMAKE_PACKAGE_REGISTRY]
#              [NO_CMAKE_BUILDS_PATH] # Deprecated; does nothing.
#              [NO_CMAKE_SYSTEM_PATH]
#              [NO_CMAKE_INSTALL_PREFIX]
#              [NO_CMAKE_SYSTEM_PACKAGE_REGISTRY]
#              [CMAKE_FIND_ROOT_PATH_BOTH |
#               ONLY_CMAKE_FIND_ROOT_PATH |
#               NO_CMAKE_FIND_ROOT_PATH])
class FindPackageArgs:
    def _exact(self, args: List[str]) -> int:
        self.exact = True
        return 1

    def _quiet(self, args: List[str]) -> int:
        self.quiet = True
        return 1

    def _required(self, args: List[str]) -> int:
        self.required = False
        return 1

    def _parse_list(self, args: List[str]) -> List[str]:
        res: List[str] = []
        for arg in args[1:]:
            if arg in self.keywords.keys():
                return res
            res.append(arg)
        return res

    def _list_args(self, result: List[str]) -> Callable[[List[str]], int]:
        def result_function(args: List[str]) -> int:
            old_len = len(result)
            result.extend(self._parse_list(args))
            return len(result) - old_len + 1
        return result_function

    def _config(self, args: List[str]) -> int:
        self.config = True
        return 1

    def _global(self, args: List[str]) -> int:
        self.is_gobal = True
        return 1

    def _no_policy_scope(self, args: List[str]) -> int:
        self.no_policy_scope = True
        return 1

    def _bypass_provider(self, args: List[str]) -> int:
        self.bypass_provider = True
        return 1

    def __init__(self, args: List[str]):
        self.components: List[str] = []
        self.optional_components: List[str] = []
        i = 0
        self.package_name = args[0]
        self.exact = False
        self.quiet = False
        self.required = False
        self.config = False
        self.is_gobal = False
        self.no_policy_scope = False
        self.bypass_provider = False
        self.names: List[str] = []
        self.hints: List[str] = []
        self.paths: List[str] = []
        self.keywords: Dict[str, Callable[[List[str]], int]] = {
            'EXACT': self._exact,
            'QUIET': self._quiet,
            'REQUIRED': self._required,
            'COMPONENTS': self._list_args(self.components),
            'OPTIONAL_COMPONENTS': self._list_args(self.optional_components),
            'CONFIG': self._config,
            'NO_MODULE': self._config,
            'GLOBAL': self._global,
            'NO_POLICY_SCOPE': self._no_policy_scope,
            'NAMES': self._list_args(self.names),
            'HINTS': self._list_args(self.hints),
            'PATHS': self._list_args(self.paths)
        }
        if len(args) == 1:
            return
        if args[i] not in self.keywords.keys():
            self.version = args[i]
            i += 1
        while i < len(args):
            assert args[i] in self.keywords.keys()
            i += self.keywords[args[i]](args[i:])


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
