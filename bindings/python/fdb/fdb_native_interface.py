#
# fdb_native_interface.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

# FoundationDB Python Native Client Interface

import ctypes
import ctypes.util
import os
import platform
import sys

from fdb.fdb_client_interface import FdbClientInterface, FutureInterface, DatabaseInterface, TransactionInterface
from fdb.impl import FDBError, KeyValue

if platform.system() == 'Windows':
    capi_name = 'fdb_c.dll'
elif platform.system() == 'Linux':
    capi_name = 'libfdb_c.so'
elif platform.system() == 'FreeBSD':
    capi_name = 'libfdb_c.so'
elif platform.system() == 'Darwin':
    capi_name = 'libfdb_c.dylib'
elif sys.platform == 'win32':
    capi_name = 'fdb_c.dll'
elif sys.platform.startswith('cygwin'):
    capi_name = 'fdb_c.dll'
elif sys.platform.startswith('linux'):
    capi_name = 'libfdb_c.so'
elif sys.platform == 'darwin':
    capi_name = 'libfdb_c.dylib'
else:
    raise Exception("Platform (%s) %s is not supported by the FoundationDB API!" % (sys.platform, platform.system()))

this_dir = os.path.dirname(__file__)

_open_file = open

# Preferred installation: The C API library or a symbolic link to the
#    library should be in the same directory as this module.
# Failing that, a file named $(capi_name).pth should be in the same directory,
#    and a relative path to the library (including filename)
# Failing that, we try to load the C API library without qualification, and
#    the library should be on the platform's dynamic library search path
def read_pth_file():
    pth_file = os.path.join(this_dir, capi_name + '.pth')
    if not os.path.exists(pth_file):
        return None
    pth = _open_file(pth_file, "rt").read().strip()
    if pth[0] != '/':
        pth = os.path.join(this_dir, pth)
    return pth

for pth in [
    lambda: os.path.join(this_dir, capi_name),
    # lambda: os.path.join(this_dir, '../../lib', capi_name),  # For compatibility with existing unix installation process... should be removed
    read_pth_file
]:
    p = pth()
    if p and os.path.exists(p):
        _capi = ctypes.CDLL(os.path.abspath(p))
        break
else:
    try:
        _capi = ctypes.CDLL(capi_name)
    except:
        # The system python on OS X can't find the library installed to /usr/local/lib if SIP is enabled
        # find_library does find the location in /usr/local/lib, so if the above fails fallback to using it
        lib_path = ctypes.util.find_library(capi_name)
        if lib_path is not None:
            try:
                _capi = ctypes.CDLL(lib_path)
            except:
                raise Exception("Unable to locate the FoundationDB API shared library!")
        else:
            raise Exception("Unable to locate the FoundationDB API shared library!")


_CBFUNC = ctypes.CFUNCTYPE(None, ctypes.c_void_p)
if hasattr(ctypes.pythonapi, 'Py_IncRef'):
    def _pin_callback(cb):
        ctypes.pythonapi.Py_IncRef(ctypes.py_object(cb))

    def _unpin_callback(cb):
        ctypes.pythonapi.Py_DecRef(ctypes.py_object(cb))
else:
    _active_callbacks = set()
    _pin_callback = _active_callbacks.add
    _unpin_callback = _active_callbacks.remove


def check_error_code(code, func, arguments):
    if code:
        raise FDBError(code)
    return None


class KeyValueStruct(ctypes.Structure):
    _fields_ = [('key', ctypes.POINTER(ctypes.c_byte)),
                ('key_length', ctypes.c_int),
                ('value', ctypes.POINTER(ctypes.c_byte)),
                ('value_length', ctypes.c_int)]
    _pack_ = 4


class FdbNativeClient(FdbClientInterface):
    def init_c_api():
        _capi.fdb_select_api_version_impl.argtypes = [ctypes.c_int, ctypes.c_int]
        _capi.fdb_select_api_version_impl.restype = ctypes.c_int

        _capi.fdb_get_error.argtypes = [ctypes.c_int]
        _capi.fdb_get_error.restype = ctypes.c_char_p

        _capi.fdb_error_predicate.argtypes = [ctypes.c_int, ctypes.c_int]
        _capi.fdb_error_predicate.restype = ctypes.c_int

        _capi.fdb_setup_network.argtypes = []
        _capi.fdb_setup_network.restype = ctypes.c_int
        _capi.fdb_setup_network.errcheck = check_error_code

        _capi.fdb_network_set_option.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_network_set_option.restype = ctypes.c_int
        _capi.fdb_network_set_option.errcheck = check_error_code

        _capi.fdb_run_network.argtypes = []
        _capi.fdb_run_network.restype = ctypes.c_int
        _capi.fdb_run_network.errcheck = check_error_code

        _capi.fdb_stop_network.argtypes = []
        _capi.fdb_stop_network.restype = ctypes.c_int
        _capi.fdb_stop_network.errcheck = check_error_code

        _capi.fdb_future_destroy.argtypes = [ctypes.c_void_p]
        _capi.fdb_future_destroy.restype = None

        _capi.fdb_future_release_memory.argtypes = [ctypes.c_void_p]
        _capi.fdb_future_release_memory.restype = None

        _capi.fdb_future_cancel.argtypes = [ctypes.c_void_p]
        _capi.fdb_future_cancel.restype = None

        _capi.fdb_future_block_until_ready.argtypes = [ctypes.c_void_p]
        _capi.fdb_future_block_until_ready.restype = ctypes.c_int
        _capi.fdb_future_block_until_ready.errcheck = check_error_code

        _capi.fdb_future_is_ready.argtypes = [ctypes.c_void_p]
        _capi.fdb_future_is_ready.restype = ctypes.c_int

        _capi.fdb_future_set_callback.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p]
        _capi.fdb_future_set_callback.restype = int
        _capi.fdb_future_set_callback.errcheck = check_error_code

        _capi.fdb_future_get_error.argtypes = [ctypes.c_void_p]
        _capi.fdb_future_get_error.restype = int
        _capi.fdb_future_get_error.errcheck = check_error_code

        _capi.fdb_future_get_int64.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int64)]
        _capi.fdb_future_get_int64.restype = ctypes.c_int
        _capi.fdb_future_get_int64.errcheck = check_error_code

        _capi.fdb_future_get_key.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.POINTER(ctypes.c_byte)),
                                             ctypes.POINTER(ctypes.c_int)]
        _capi.fdb_future_get_key.restype = ctypes.c_int
        _capi.fdb_future_get_key.errcheck = check_error_code

        _capi.fdb_future_get_value.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int),
                                               ctypes.POINTER(ctypes.POINTER(ctypes.c_byte)), ctypes.POINTER(ctypes.c_int)]
        _capi.fdb_future_get_value.restype = ctypes.c_int
        _capi.fdb_future_get_value.errcheck = check_error_code

        _capi.fdb_future_get_keyvalue_array.argtypes = [ctypes.c_void_p, ctypes.POINTER(
            ctypes.POINTER(KeyValueStruct)), ctypes.POINTER(ctypes.c_int), ctypes.POINTER(ctypes.c_int)]
        _capi.fdb_future_get_keyvalue_array.restype = int
        _capi.fdb_future_get_keyvalue_array.errcheck = check_error_code

        _capi.fdb_future_get_string_array.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)), ctypes.POINTER(ctypes.c_int)]
        _capi.fdb_future_get_string_array.restype = int
        _capi.fdb_future_get_string_array.errcheck = check_error_code

        _capi.fdb_create_database.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
        _capi.fdb_create_database.restype = ctypes.c_int
        _capi.fdb_create_database.errcheck = check_error_code

        _capi.fdb_database_destroy.argtypes = [ctypes.c_void_p]
        _capi.fdb_database_destroy.restype = None

        _capi.fdb_database_create_transaction.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        _capi.fdb_database_create_transaction.restype = ctypes.c_int
        _capi.fdb_database_create_transaction.errcheck = check_error_code

        _capi.fdb_database_set_option.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_database_set_option.restype = ctypes.c_int
        _capi.fdb_database_set_option.errcheck = check_error_code

        _capi.fdb_transaction_destroy.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_destroy.restype = None

        _capi.fdb_transaction_cancel.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_cancel.restype = None

        _capi.fdb_transaction_set_read_version.argtypes = [ctypes.c_void_p, ctypes.c_int64]
        _capi.fdb_transaction_set_read_version.restype = None

        _capi.fdb_transaction_get_read_version.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_get_read_version.restype = ctypes.c_void_p

        _capi.fdb_transaction_get.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
        _capi.fdb_transaction_get.restype = ctypes.c_void_p

        _capi.fdb_transaction_get_key.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int]
        _capi.fdb_transaction_get_key.restype = ctypes.c_void_p

        _capi.fdb_transaction_get_range.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_void_p,
                                                    ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                                                    ctypes.c_int, ctypes.c_int]
        _capi.fdb_transaction_get_range.restype = ctypes.c_void_p

        _capi.fdb_transaction_get_estimated_range_size_bytes.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_get_estimated_range_size_bytes.restype = ctypes.c_void_p

        _capi.fdb_transaction_add_conflict_range.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
        _capi.fdb_transaction_add_conflict_range.restype = ctypes.c_int
        _capi.fdb_transaction_add_conflict_range.errcheck = check_error_code

        _capi.fdb_transaction_get_addresses_for_key.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_get_addresses_for_key.restype = ctypes.c_void_p

        _capi.fdb_transaction_set_option.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_set_option.restype = ctypes.c_int
        _capi.fdb_transaction_set_option.errcheck = check_error_code

        _capi.fdb_transaction_atomic_op.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
        _capi.fdb_transaction_atomic_op.restype = None

        _capi.fdb_transaction_set.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_set.restype = None

        _capi.fdb_transaction_clear.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_clear.restype = None

        _capi.fdb_transaction_clear_range.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_clear_range.restype = None

        _capi.fdb_transaction_watch.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_watch.restype = ctypes.c_void_p

        _capi.fdb_transaction_commit.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_commit.restype = ctypes.c_void_p

        _capi.fdb_transaction_get_committed_version.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_int64)]
        _capi.fdb_transaction_get_committed_version.restype = ctypes.c_int
        _capi.fdb_transaction_get_committed_version.errcheck = check_error_code

        _capi.fdb_transaction_get_approximate_size.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_get_approximate_size.restype = ctypes.c_void_p

        _capi.fdb_transaction_get_versionstamp.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_get_versionstamp.restype = ctypes.c_void_p

        _capi.fdb_transaction_on_error.argtypes = [ctypes.c_void_p, ctypes.c_int]
        _capi.fdb_transaction_on_error.restype = ctypes.c_void_p

        _capi.fdb_transaction_reset.argtypes = [ctypes.c_void_p]
        _capi.fdb_transaction_reset.restype = None

    def select_api_version(self, version, header_version):
        return _capi.fdb_select_api_version_impl(version, header_version)

    def get_max_api_version(self):
        return _capi.fdb_get_max_api_version()

    def get_error(self, error_code):
        return _capi.fdb_get_error(error_code)

    def error_predicate(self, predicate, error_code):
        return bool(_capi.fdb_error_predicate(predicate, error_code))

    def setup_network(self):
        _capi.fdb_setup_network()

    def network_set_option(self, option, param, length):
        _capi.fdb_network_set_option(option, param, length)

    def run_network(self):
        _capi.fdb_run_network()

    def stop_network(self):
        _capi.fdb_stop_network()
        
    def create_database(self, connection_string):
        pointer = ctypes.c_void_p()
        _capi.fdb_create_database(connection_string, pointer)
        return FdbNativeDatabase(pointer.value)


class FdbNativeFuture(FutureInterface):
    def __init__(self, fpointer):
        self.fpointer = fpointer

    def destroy(self):
        if self.fpointer:
            _capi.fdb_future_destroy(self.fpointer)
            self.fpointer = None

    def release_memory(self):
        _capi.fdb_future_release_memory(self.fpointer)

    def cancel(self):
        _capi.fdb_future_cancel(self.fpointer)

    def is_ready(self):
        return bool(_capi.fdb_future_is_ready(self.fpointer))

    def set_callback(self, callback):
        def cb_and_delref(ignore):
            _unpin_callback(cbfunc[0])
            del cbfunc[:]
            try:
                callback(self)
            except Exception as e:
                try:
                    sys.stderr.write("Discarding uncaught exception from user FDB callback:\n")
                    traceback.print_exception(*sys.exc_info(), file=sys.stderr, limit=1)
                except:
                    pass
        cbfunc = [_CBFUNC(cb_and_delref)]
        del cb_and_delref
        _pin_callback(cbfunc[0])
        _capi.fdb_future_set_callback(self.fpointer, cbfunc[0], None)

    def get_error(self):
        _capi.fdb_future_get_error(self.fpointer)

    def get_int64(self):
        value = ctypes.c_int64()
        _capi.fdb_future_get_int64(self.fpointer, ctypes.byref(value))
        return value.value

    def get_key(self):
        key = ctypes.pointer(ctypes.c_byte())
        key_length = ctypes.c_int()
        _capi.fdb_future_get_key(self.fpointer, ctypes.byref(key), ctypes.byref(key_length))
        return ctypes.string_at(key, key_length.value)

    def get_value(self):
        present = ctypes.c_int()
        value = ctypes.pointer(ctypes.c_byte())
        value_length = ctypes.c_int()
        _capi.fdb_future_get_value(self.fpointer, ctypes.byref(present),
                                   ctypes.byref(value), ctypes.byref(value_length))
        if present.value:
            return ctypes.string_at(value, value_length.value)
        else:
            return None

    def get_keyvalue_array(self):
        kvs = ctypes.pointer(KeyValueStruct())
        count = ctypes.c_int()
        more = ctypes.c_int()
        _capi.fdb_future_get_keyvalue_array(self.fpointer, ctypes.byref(kvs), ctypes.byref(count), ctypes.byref(more))
        return ([KeyValue(ctypes.string_at(x.key, x.key_length), ctypes.string_at(x.value, x.value_length))
                for x in kvs[0:count.value]], count.value, more.value)

    def get_string_array(self):
        strings = ctypes.pointer(ctypes.c_char_p())
        count = ctypes.c_int()
        _capi.fdb_future_get_string_array(self.fpointer, ctypes.byref(strings), ctypes.byref(count))
        return list(strings[0:count.value])


class FdbNativeDatabase(DatabaseInterface):
    def __init__(self, dpointer):
        self.dpointer = dpointer

    def destroy(self):
        if self.dpointer:
            _capi.fdb_database_destroy(self.dpointer)
            self.dpointer = None

    def create_transaction(self):
        pointer = ctypes.c_void_p()
        _capi.fdb_database_create_transaction(self.dpointer, ctypes.byref(pointer))
        return FdbNativeTransaction(pointer.value)

    def set_option(self, option, param, length):
        _capi.fdb_database_set_option(self.dpointer, option, param, length)


class FdbNativeTransaction(TransactionInterface):
    def __init__(self, tpointer):
        self.tpointer = tpointer

    def destroy(self):
        if self.tpointer:
            _capi.fdb_transaction_destroy(self.tpointer)
            self.tpointer = None

    def cancel(self):
        _capi.fdb_transaction_cancel(self.tpointer)

    def set_read_version(self, version):
        _capi.fdb_transaction_set_read_version(self.tpointer, version)

    def get_read_version(self):
        return FdbNativeFuture(_capi.fdb_transaction_get_read_version(self.tpointer))

    def get(self, key, snapshot):
        return FdbNativeFuture(_capi.fdb_transaction_get(self.tpointer, key, len(key), snapshot))

    def get_key(self, key_selector, snapshot):
        return FdbNativeFuture(
            _capi.fdb_transaction_get_key(
                self.tpointer, key_selector.key, len(key_selector.key), key_selector.or_equal, 
                key_selector.offset, snapshot))

    def get_range(self, begin, end, limit, target_bytes, streaming_mode, iteration, snapshot, reverse):
        return FdbNativeFuture(
            _capi.fdb_transaction_get_range(
                self.tpointer, begin.key, len(begin.key), begin.or_equal, begin.offset, end.key,
                len(end.key), end.or_equal,end.offset, limit, target_bytes, streaming_mode, iteration,
                snapshot, reverse))

    def get_estimated_range_size_bytes(self, begin, end):
        return FdbNativeFuture(
            _capi.fdb_transaction_get_estimated_range_size_bytes(
                self.tpointer, begin, len(begin), end, len(end)))

    def add_conflict_range(self, begin, end, type):
        _capi.fdb_transaction_add_conflict_range(self.tpointer, begin, len(begin), end, len(end), type)

    def get_addresses_for_key(self, key):
        return FdbNativeFuture(_capi.fdb_transaction_get_addresses_for_key(self.tpointer, key, len(key)))

    def set_option(self, option, param, length):
        _capi.fdb_transaction_set_option(self.tpointer, option, param, length)

    def atomic_op(self, key, param, opcode):
        _capi.fdb_transaction_atomic_op(self.tpointer, key, len(key), param, len(param), opcode)

    def set(self, key, value):
        _capi.fdb_transaction_set(self.tpointer, key, len(key), value, len(value))

    def clear(self, key):
        _capi.fdb_transaction_clear(self.tpointer, key, len(key))

    def clear_range(self, begin, end):
        _capi.fdb_transaction_clear_range(self.tpointer, begin, len(begin), end, len(end))

    def watch(self, key):
        return FdbNativeFuture(_capi.fdb_transaction_watch(self.tpointer, key, len(key)))

    def commit(self):
        return FdbNativeFuture(_capi.fdb_transaction_commit(self.tpointer))

    def get_committed_version(self):
        version = ctypes.c_int64()
        _capi.fdb_transaction_get_committed_version(self.tpointer, ctypes.byref(version))
        return version.value

    def get_approximate_size(self):
        return FdbNativeFuture(_capi.fdb_transaction_get_approximate_size(self.tpointer))

    def get_versionstamp(self):
        return FdbNativeFuture(_capi.fdb_transaction_get_versionstamp(self.tpointer))

    def on_error(self, error_code):
        return FdbNativeFuture(_capi.fdb_transaction_on_error(self.tpointer, error_code))

    def reset(self):
        _capi.fdb_transaction_reset(self.tpointer)
