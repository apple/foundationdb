#
# __init__.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# FoundationDB Python API

"""Documentation for this API can be found at
https://apple.github.io/foundationdb/api-python.html"""


def open(*args, **kwargs):
    raise RuntimeError('You must call api_version() before using any fdb methods')


init = open


def transactional(*args, **kwargs):
    raise RuntimeError('You must call api_version() before using fdb.transactional')


def _add_symbols(module, symbols):
    for symbol in symbols:
        globals()[symbol] = getattr(module, symbol)


def is_api_version_selected():
    return '_version' in globals()


def get_api_version():
    if is_api_version_selected():
        return globals()['_version']
    else:
        raise RuntimeError('API version is not set')


def api_version(ver):
    header_version = 630

    if '_version' in globals():
        if globals()['_version'] != ver:
            raise RuntimeError('FDB API already loaded at version %d' % _version)
        return

    if ver < 13:
        raise RuntimeError('FDB API versions before 13 are not supported')

    if ver > header_version:
        raise RuntimeError('Latest known FDB API version is %d' % header_version)

    import fdb.impl

    err = fdb.impl._capi.fdb_select_api_version_impl(ver, header_version)
    if err == 2203:  # api_version_not_supported, but that's not helpful to the user
        max_supported_ver = fdb.impl._capi.fdb_get_max_api_version()
        if header_version > max_supported_ver:
            raise RuntimeError("This version of the FoundationDB Python binding is not supported by the installed "
                               "FoundationDB C library. The binding requires a library that supports API version "
                               "%d, but the installed library supports a maximum version of %d." % (header_version, max_supported_ver))

        else:
            raise RuntimeError("API version %d is not supported by the installed FoundationDB C library." % ver)

    elif err != 0:
        raise RuntimeError('FoundationDB API error')

    fdb.impl.init_c_api()

    list = (
        'FDBError',
        'predicates',
        'Future',
        'Database',
        'Transaction',
        'KeyValue',
        'KeySelector',
        'open',
        'transactional',
        'options',
        'StreamingMode',
    )

    _add_symbols(fdb.impl, list)

    if ver < 610:
        globals()["init"] = getattr(fdb.impl, "init")
        globals()["open"] = getattr(fdb.impl, "open_v609")
        globals()["create_cluster"] = getattr(fdb.impl, "create_cluster")
        globals()["Cluster"] = getattr(fdb.impl, "Cluster")

    if ver > 22:
        import fdb.locality

    if ver == 13:
        globals()["open"] = getattr(fdb.impl, "open_v13")
        globals()["init"] = getattr(fdb.impl, "init_v13")

        # Future.get got renamed to Future.wait in v14 to make room for
        # Database.get, we have to undo that here
        for name in dir(fdb.impl):
            o = getattr(fdb.impl, name)
            try:
                if issubclass(o, fdb.impl.Future):
                    if hasattr(o, "wait"):
                        o.get = o.wait
            except TypeError:
                pass

        # FDBRange used to be called FDBRangeIter and was an iterator,
        # but it's now a container. In v13 we have to make it act like
        # an iterator again.
        def next(self):
            if not hasattr(self, "__iterating"):
                self.__iterating = iter(self)
            return next(self.__iterating)
        setattr(fdb.impl.FDBRange, "next", next)

    globals()['_version'] = ver

    import fdb.directory_impl
    directory_symbols = ('directory', 'DirectoryLayer',)
    _add_symbols(fdb.directory_impl, directory_symbols)

    import fdb.subspace_impl
    subspace_symbols = ('Subspace',)
    _add_symbols(fdb.subspace_impl, subspace_symbols)
