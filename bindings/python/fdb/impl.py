#
# impl.py
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

import ctypes
import ctypes.util
import datetime
import functools
import inspect
import multiprocessing
import os
import platform
import sys
import threading
import traceback

import weakref
import fdb
from fdb import six
from fdb.tuple import pack, unpack

from fdb import fdboptions as _opts
import types
import struct

import atexit

_network_thread = None
_network_thread_reentrant_lock = threading.RLock()

_open_file = open

_thread_local_storage = threading.local()


class _NetworkOptions(object):
    def __init__(self, parent):
        self._parent = parent


class _ErrorPredicates(object):
    def __init__(self, parent):
        self._parent = parent


class _DatabaseOptions(object):
    def __init__(self, db):
        self._parent = weakref.proxy(db)


class _TransactionOptions(object):
    def __init__(self, tr):
        self._parent = weakref.proxy(tr)


def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def option_wrap(code):
    def setfunc(self):
        self._parent._set_option(code, None, 0)

    return setfunc


def option_wrap_string(code):
    def setfunc(self, param=None):
        param, length = optionalParamToBytes(param)
        self._parent._set_option(code, param, length)

    return setfunc


def option_wrap_bytes(code):
    def setfunc(self, param=None):
        if param is None:
            self._parent._set_option(code, None, 0)
        elif isinstance(param, bytes):
            self._parent._set_option(code, param, len(param))
        else:
            raise TypeError("Value must be of type " + bytes.__name__)

    return setfunc


def option_wrap_int(code):
    def setfunc(self, param):
        self._parent._set_option(code, struct.pack("<q", param), 8)

    return setfunc


def pred_wrap(code):
    def predfunc(self, error):
        return self._parent._error_predicate(code, error.code)

    return predfunc


def operation_wrap(code):
    def opfunc(self, key, param):
        self._atomic_operation(code, key, param)

    return opfunc


def fill_options(scope, predicates=False):
    _dict = getattr(_opts, scope)

    for k, v in _dict.items():
        fname = (predicates and "is_" or "set_") + k.lower()
        code, desc, paramType, paramDesc = v
        if predicates:
            f = pred_wrap(code)
        else:
            if paramType == type(None):
                f = option_wrap(code)
            elif paramType == type(""):
                f = option_wrap_string(code)
            elif paramType == type(b""):
                # This won't happen in Python 2 because type("") == type(b""), but it will happen in Python 3
                f = option_wrap_bytes(code)
            elif paramType == type(0):
                f = option_wrap_int(code)
            else:
                raise TypeError(
                    "Don't know how to set options of type %s" % paramType.__name__
                )
        f.__name__ = fname
        f.__doc__ = desc
        if paramDesc is not None:
            f.__doc__ += "\n\nArgument is " + paramDesc
        klass = globals()["_" + scope + "s"]
        setattr(klass, fname, f)


def add_operation(fname, v):
    code, desc, paramType, paramDesc = v
    f = operation_wrap(code)
    f.__name__ = fname
    f.__doc__ = (
        desc
        + "\n\nArguments are the key to which the operation is applied and the "
        + paramDesc
    )
    setattr(globals()["Database"], fname, f)
    setattr(globals()["Transaction"], fname, f)
    setattr(globals()["Tenant"], fname, f)


def fill_operations():
    _dict = getattr(_opts, "MutationType")

    for k, v in _dict.items():
        fname = k.lower()
        add_operation(fname, v)
        add_operation("bit_" + fname, v)


for scope in ["DatabaseOption", "TransactionOption", "NetworkOption"]:
    fill_options(scope)

fill_options("ErrorPredicate", True)

options = _NetworkOptions(sys.modules[__name__])
predicates = _ErrorPredicates(sys.modules[__name__])


def _set_option(option, param, length):
    _capi.fdb_network_set_option(option, param, length)


def _error_predicate(predicate, error_code):
    return bool(_capi.fdb_error_predicate(predicate, error_code))


def make_enum(scope):
    _dict = getattr(_opts, scope)

    x = type(scope, (), {})

    def makeprop(value, doc):
        return property(fget=lambda o: value, doc=doc)

    for k, v in _dict.items():
        setattr(x, k.lower(), makeprop(v[0], v[1]))

    globals()[scope] = x()


make_enum("StreamingMode")
make_enum("ConflictRangeType")


def transactional(*tr_args, **tr_kwargs):
    """Decorate a funcation as transactional.

    The decorator looks for a named argument (default "tr") and takes
    one of two actions, depending on the type of the parameter passed
    to the function at call time.

    If given a Database or Tenant, a Transaction will be created and
    passed into the wrapped code in place of the Database or Tenant.
    After the function is complete, the newly created transaction
    will be committed.

    It is important to note that the wrapped method may be called
    multiple times in the event of a commit failure, until the commit
    succeeds.  This restriction requires that the wrapped function
    may not be a generator, or a function that returns a closure that
    contains the `tr` object.

    If given a Transaction, the Transaction will be passed into the
    wrapped code, and WILL NOT be committed at completion of the
    function. This allows new transactional functions to be composed
    of other transactional methods.

    The transactional decorator may be used with or without
    arguments. The keyword argument "parameter" is recognized and
    specifies the name of the parameter to the wrapped function that
    will contain either a Database or Transaction.

    """

    def decorate(func):
        try:
            parameter = tr_kwargs["parameter"]
        except KeyError:
            parameter = "tr"

        wfunc = func
        while getattr(wfunc, "__wrapped__", None):
            wfunc = wfunc.__wrapped__
        if hasattr(inspect, "getfullargspec"):
            index = inspect.getfullargspec(wfunc).args.index(parameter)
        else:
            index = inspect.getargspec(wfunc).args.index(parameter)

        if getattr(func, "_is_coroutine", False):

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                if isinstance(args[index], TransactionRead):
                    raise asyncio.Return((yield asyncio.From(func(*args, **kwargs))))

                largs = list(args)
                tr = largs[index] = args[index].create_transaction()

                while True:
                    try:
                        ret = yield asyncio.From(func(*largs, **kwargs))
                        yield asyncio.From(tr.commit())
                        raise asyncio.Return(ret)
                    except FDBError as e:
                        yield asyncio.From(tr.on_error(e.code))

        else:

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                # We can't throw this from the decorator, as when a user runs
                # >>> import fdb ; fdb.api_version(fdb.LATEST_API_VERSION)
                # the code above uses @transactional before the API version is set
                if fdb.get_api_version() >= 630 and inspect.isgeneratorfunction(func):
                    raise ValueError(
                        "Generators can not be wrapped with fdb.transactional"
                    )

                if isinstance(args[index], TransactionRead):
                    return func(*args, **kwargs)

                largs = list(args)
                tr = largs[index] = args[index].create_transaction()

                committed = False
                # retries = 0
                # start = datetime.datetime.now()
                # last = start

                while not committed:
                    ret = None
                    try:
                        ret = func(*largs, **kwargs)
                        if fdb.get_api_version() >= 630 and inspect.isgenerator(ret):
                            raise ValueError(
                                "Generators can not be wrapped with fdb.transactional"
                            )
                        tr.commit().wait()
                        committed = True
                    except FDBError as e:
                        tr.on_error(e.code).wait()

                    # now = datetime.datetime.now()
                    # td = now - last
                    # elapsed = (td.microseconds + (td.seconds + td.days * 24 * 3600) * 10**6) / float(10**6)
                    # if elapsed >= 1:
                    #     td = now - start
                    #     print ("fdb WARNING: long transaction (%gs elapsed in transactional function \"%s\" (%d retries, %s))"
                    #            % (elapsed, func.__name__, retries, committed and "committed" or "not yet committed"))
                    #     last = now

                    # retries += 1
                return ret

        return wrapper

    if not tr_args:
        # Being called with parameters (possibly none); return a
        # decorator
        return decorate
    elif len(tr_args) == 1 and not tr_kwargs:
        # Being called as a decorator
        return decorate(tr_args[0])
    else:
        raise Exception("Invalid use of transactional decorator.")


class FDBError(Exception):
    """This exception is raised when an FDB API call returns an
    error. The error code will be stored in the code attribute, and a
    textual description of the error will be stored in the description
    attribute.

    """

    def __init__(self, code):
        self.code = code
        self._description = None

    @property
    def description(self):
        if not self._description:
            self._description = _capi.fdb_get_error(self.code)
        return self._description

    def __str__(self):
        return "%s (%d)" % (self.description, self.code)

    def __repr__(self):
        return "FDBError(%d)" % self.code


class _FDBBase(object):
    # By inheriting from _FDBBase, every class gets access to self.capi
    # (set below when we open libfdb_capi)
    pass


class FDBRange(object):
    """Iterates over the results of an FDB range query. Returns
    KeyValue objects.

    """

    def __init__(self, tr, begin, end, limit, reverse, streaming_mode):
        self._tr = tr

        self._bsel = begin
        self._esel = end

        self._limit = limit
        self._reverse = reverse
        self._mode = streaming_mode

        self._future = self._tr._get_range(
            begin, end, limit, streaming_mode, 1, reverse
        )

    def to_list(self):
        if self._mode == StreamingMode.iterator:
            if self._limit > 0:
                mode = StreamingMode.exact
            else:
                mode = StreamingMode.want_all
        else:
            mode = self._mode

        return list(self.__iter__(mode=mode))

    def __iter__(self, mode=None):
        if mode is None:
            mode = self._mode
        bsel = self._bsel
        esel = self._esel
        limit = self._limit

        iteration = 1  # the first read was fired off when the FDBRange was initialized
        future = self._future

        done = False

        while not done:
            if future:
                (kvs, count, more) = future.wait()
                index = 0
                future = None

                if not count:
                    return

            result = kvs[index]
            index += 1

            if index == count:
                if not more or limit == count:
                    done = True
                else:
                    iteration += 1
                    if limit > 0:
                        limit = limit - count
                    if self._reverse:
                        esel = KeySelector.first_greater_or_equal(kvs[-1].key)
                    else:
                        bsel = KeySelector.first_greater_than(kvs[-1].key)
                    future = self._tr._get_range(
                        bsel, esel, limit, mode, iteration, self._reverse
                    )

            yield result


class TransactionRead(_FDBBase):
    def __init__(self, tpointer, db, snapshot):
        self.tpointer = tpointer
        self.db = db
        self._snapshot = snapshot

    def __del__(self):
        # print("Destroying transactionread 0x%x" % self.tpointer)
        self.capi.fdb_transaction_destroy(self.tpointer)

    def get_read_version(self):
        """Get the read version of the transaction."""
        return FutureInt64(self.capi.fdb_transaction_get_read_version(self.tpointer))

    def get(self, key):
        key = keyToBytes(key)
        return Value(
            self.capi.fdb_transaction_get(self.tpointer, key, len(key), self._snapshot)
        )

    def get_key(self, key_selector):
        key = keyToBytes(key_selector.key)

        return Key(
            self.capi.fdb_transaction_get_key(
                self.tpointer,
                key,
                len(key),
                key_selector.or_equal,
                key_selector.offset,
                self._snapshot,
            )
        )

    def _get_range(self, begin, end, limit, streaming_mode, iteration, reverse):
        beginKey = keyToBytes(begin.key)
        endKey = keyToBytes(end.key)

        return FutureKeyValueArray(
            self.capi.fdb_transaction_get_range(
                self.tpointer,
                beginKey,
                len(beginKey),
                begin.or_equal,
                begin.offset,
                endKey,
                len(endKey),
                end.or_equal,
                end.offset,
                limit,
                0,
                streaming_mode,
                iteration,
                self._snapshot,
                reverse,
            )
        )

    def _to_selector(self, key_or_selector):
        if not isinstance(key_or_selector, KeySelector):
            key_or_selector = KeySelector.first_greater_or_equal(key_or_selector)
        return key_or_selector

    def get_range(
        self, begin, end, limit=0, reverse=False, streaming_mode=StreamingMode.iterator
    ):
        if begin is None:
            begin = b""
        if end is None:
            end = b"\xff"
        begin = self._to_selector(begin)
        end = self._to_selector(end)
        return FDBRange(self, begin, end, limit, reverse, streaming_mode)

    def get_range_startswith(self, prefix, *args, **kwargs):
        prefix = keyToBytes(prefix)
        return self.get_range(prefix, strinc(prefix), *args, **kwargs)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.get_range(key.start, key.stop, reverse=(key.step == -1))
        return self.get(key)

    def get_estimated_range_size_bytes(self, begin_key, end_key):
        if begin_key is None or end_key is None:
            if fdb.get_api_version() >= 700:
                raise Exception("Invalid begin key or end key")
            else:
                if begin_key is None:
                    begin_key = b""
                if end_key is None:
                    end_key = b"\xff"
        return FutureInt64(
            self.capi.fdb_transaction_get_estimated_range_size_bytes(
                self.tpointer, begin_key, len(begin_key), end_key, len(end_key)
            )
        )

    def get_range_split_points(self, begin_key, end_key, chunk_size):
        if begin_key is None or end_key is None or chunk_size <= 0:
            raise Exception("Invalid begin key, end key or chunk size")
        return FutureKeyArray(
            self.capi.fdb_transaction_get_range_split_points(
                self.tpointer,
                begin_key,
                len(begin_key),
                end_key,
                len(end_key),
                chunk_size,
            )
        )


class Transaction(TransactionRead):
    """A modifiable snapshot of a Database."""

    def __init__(self, tpointer, db):
        super(Transaction, self).__init__(tpointer, db, False)
        self.options = _TransactionOptions(self)
        self.__snapshot = self.snapshot = TransactionRead(tpointer, db, True)

    def __del__(self):
        pass

    def set_read_version(self, version):
        """Set the read version of the transaction."""
        self.capi.fdb_transaction_set_read_version(self.tpointer, version)

    def _set_option(self, option, param, length):
        self.capi.fdb_transaction_set_option(self.tpointer, option, param, length)

    def _atomic_operation(self, opcode, key, param):
        paramBytes = valueToBytes(param)
        paramLength = len(paramBytes)
        keyBytes = keyToBytes(key)
        keyLength = len(keyBytes)
        self.capi.fdb_transaction_atomic_op(
            self.tpointer, keyBytes, keyLength, paramBytes, paramLength, opcode
        )

    def set(self, key, value):
        key = keyToBytes(key)
        value = valueToBytes(value)
        self.capi.fdb_transaction_set(self.tpointer, key, len(key), value, len(value))

    def clear(self, key):
        if isinstance(key, KeySelector):
            key = self.get_key(key)

        key = keyToBytes(key)

        self.capi.fdb_transaction_clear(self.tpointer, key, len(key))

    def clear_range(self, begin, end):
        if begin is None:
            begin = b""
        if end is None:
            end = b"\xff"
        if isinstance(begin, KeySelector):
            begin = self.get_key(begin)
        if isinstance(end, KeySelector):
            end = self.get_key(end)

        begin = keyToBytes(begin)
        end = keyToBytes(end)

        self.capi.fdb_transaction_clear_range(
            self.tpointer, begin, len(begin), end, len(end)
        )

    def clear_range_startswith(self, prefix):
        prefix = keyToBytes(prefix)
        return self.clear_range(prefix, strinc(prefix))

    def watch(self, key):
        key = keyToBytes(key)
        return FutureVoid(self.capi.fdb_transaction_watch(self.tpointer, key, len(key)))

    def add_read_conflict_range(self, begin, end):
        begin = keyToBytes(begin)
        end = keyToBytes(end)
        self.capi.fdb_transaction_add_conflict_range(
            self.tpointer, begin, len(begin), end, len(end), ConflictRangeType.read
        )

    def add_read_conflict_key(self, key):
        key = keyToBytes(key)
        self.add_read_conflict_range(key, key + b"\x00")

    def add_write_conflict_range(self, begin, end):
        begin = keyToBytes(begin)
        end = keyToBytes(end)
        self.capi.fdb_transaction_add_conflict_range(
            self.tpointer, begin, len(begin), end, len(end), ConflictRangeType.write
        )

    def add_write_conflict_key(self, key):
        key = keyToBytes(key)
        self.add_write_conflict_range(key, key + b"\x00")

    def commit(self):
        return FutureVoid(self.capi.fdb_transaction_commit(self.tpointer))

    def get_committed_version(self):
        version = ctypes.c_int64()
        self.capi.fdb_transaction_get_committed_version(
            self.tpointer, ctypes.byref(version)
        )
        return version.value

    def get_approximate_size(self):
        """Get the approximate commit size of the transaction."""
        return FutureInt64(
            self.capi.fdb_transaction_get_approximate_size(self.tpointer)
        )

    def get_versionstamp(self):
        return Key(self.capi.fdb_transaction_get_versionstamp(self.tpointer))

    def on_error(self, error):
        if isinstance(error, FDBError):
            code = error.code
        elif isinstance(error, int):
            code = error
        else:
            raise error
        return FutureVoid(self.capi.fdb_transaction_on_error(self.tpointer, code))

    def reset(self):
        self.capi.fdb_transaction_reset(self.tpointer)

    def cancel(self):
        self.capi.fdb_transaction_cancel(self.tpointer)

    def __setitem__(self, key, value):
        self.set(key, value)

    def __delitem__(self, key):
        if isinstance(key, slice):
            self.clear_range(key.start, key.stop)
        else:
            self.clear(key)


class Future(_FDBBase):
    Event = threading.Event
    _state = None  # < Hack for trollius

    def __init__(self, fpointer):
        # print("Creating future 0x%x" % fpointer)
        self.fpointer = fpointer

    def __del__(self):
        if self.fpointer:
            # print("Destroying future 0x%x" % self.fpointer)
            self.capi.fdb_future_destroy(self.fpointer)
            self.fpointer = None

    def cancel(self):
        self.capi.fdb_future_cancel(self.fpointer)

    def _release_memory(self):
        self.capi.fdb_future_release_memory(self.fpointer)

    def wait(self):
        raise NotImplementedError

    def is_ready(self):
        return bool(self.capi.fdb_future_is_ready(self.fpointer))

    def block_until_ready(self):
        # Checking readiness is faster than using the callback, so it saves us time if we are already
        # ready. It also doesn't add much to the cost of this function
        if not self.is_ready():
            # Blocking in the native client from the main thread prevents Python from handling signals.
            # To avoid that behavior, we implement the blocking in Python using semaphores and on_ready.
            # Using a Semaphore is faster than an Event, and we create only one per thread to avoid the
            # cost of creating one every time.
            semaphore = getattr(_thread_local_storage, "future_block_semaphore", None)
            if semaphore is None:
                semaphore = multiprocessing.Semaphore(0)
                _thread_local_storage.future_block_semaphore = semaphore

            self.on_ready(lambda self: semaphore.release())

            try:
                semaphore.acquire()
            except Exception:
                # If this semaphore didn't actually get released, then we need to replace our thread-local
                # copy so that later callers still function correctly
                _thread_local_storage.future_block_semaphore = (
                    multiprocessing.Semaphore(0)
                )
                raise

    def on_ready(self, callback):
        def cb_and_delref(ignore):
            _unpin_callback(cbfunc[0])
            del cbfunc[:]
            try:
                callback(self)
            except Exception:
                try:
                    sys.stderr.write(
                        "Discarding uncaught exception from user FDB callback:\n"
                    )
                    traceback.print_exception(*sys.exc_info(), file=sys.stderr)
                except Exception:
                    pass

        cbfunc = [_CBFUNC(cb_and_delref)]
        del cb_and_delref
        _pin_callback(cbfunc[0])
        self.capi.fdb_future_set_callback(self.fpointer, cbfunc[0], None)

    @staticmethod
    def wait_for_any(*futures):
        """Does not return until at least one of the given futures is ready.
        Returns the index in the parameter list of a ready future."""
        if not futures:
            raise ValueError("wait_for_any requires at least one future")
        d = {}
        ev = futures[0].Event()
        for i, f in enumerate(futures):

            def cb(ignore, i=i):
                if d.setdefault("i", i) == i:
                    ev.set()

            f.on_ready(cb)
        ev.wait()
        return d["i"]

    # asyncio future protocol
    def cancelled(self):
        if not self.done():
            return False
        e = self.exception()
        return getattr(e, "code", 0) == 1101

    done = is_ready

    def result(self):
        if not self.done():
            raise Exception("Future result not available")
        return self.wait()

    def exception(self):
        if not self.done():
            raise Exception("Future result not available")
        try:
            self.wait()
            return None
        except BaseException as e:
            return e

    def add_done_callback(self, fn):
        self.on_ready(lambda f: self.call_soon_threadsafe(fn, f))

    def remove_done_callback(self, fn):
        raise NotImplementedError()


class FutureVoid(Future):
    def wait(self):
        self.block_until_ready()
        self.capi.fdb_future_get_error(self.fpointer)
        return None


class FutureInt64(Future):
    def wait(self):
        self.block_until_ready()
        value = ctypes.c_int64()
        self.capi.fdb_future_get_int64(self.fpointer, ctypes.byref(value))
        return value.value


class FutureUInt64(Future):
    def wait(self):
        self.block_until_ready()
        value = ctypes.c_uint64()
        self.capi.fdb_future_get_uint64(self.fpointer, ctypes.byref(value))
        return value.value


class FutureKeyValueArray(Future):
    def wait(self):
        self.block_until_ready()
        kvs = ctypes.pointer(KeyValueStruct())
        count = ctypes.c_int()
        more = ctypes.c_int()
        self.capi.fdb_future_get_keyvalue_array(
            self.fpointer, ctypes.byref(kvs), ctypes.byref(count), ctypes.byref(more)
        )
        return (
            [
                KeyValue(
                    ctypes.string_at(x.key, x.key_length),
                    ctypes.string_at(x.value, x.value_length),
                )
                for x in kvs[0 : count.value]
            ],
            count.value,
            more.value,
        )

        # Logically, we should self._release_memory() after extracting the
        # KVs but before returning, but then we would have to store
        # the KVs on the python side and in most cases we are about to
        # destroy the future anyway


class FutureKeyArray(Future):
    def wait(self):
        self.block_until_ready()
        ks = ctypes.pointer(KeyStruct())
        count = ctypes.c_int()
        self.capi.fdb_future_get_key_array(
            self.fpointer, ctypes.byref(ks), ctypes.byref(count)
        )
        return [ctypes.string_at(x.key, x.key_length) for x in ks[0 : count.value]]


class FutureStringArray(Future):
    def wait(self):
        self.block_until_ready()
        strings = ctypes.pointer(ctypes.c_char_p())
        count = ctypes.c_int()
        self.capi.fdb_future_get_string_array(
            self.fpointer, ctypes.byref(strings), ctypes.byref(count)
        )
        return list(strings[0 : count.value])


class replaceable_property(object):
    def __get__(self, obj, cls=None):
        return self.method(obj)

    def __init__(self, method):
        self.method = method


class LazyFuture(Future):
    def __init__(self, *args, **kwargs):
        super(LazyFuture, self).__init__(*args, **kwargs)

    def wait(self):
        self.value
        return self

    def _getter(self):
        raise NotImplementedError

    @replaceable_property
    def value(self):
        self.block_until_ready()

        try:
            self._getter()
            self._release_memory()

        except Exception:
            e = sys.exc_info()
            if not (
                isinstance(e[1], FDBError) and e[1].code == 1102
            ):  # future_released
                raise

        return self.value


# This is a workaround to avoid a compiler issue as described here:
# http://bugs.python.org/issue12370
_super = super


class FutureString(LazyFuture):
    def __init__(self, *args):
        self._error = None
        _super(FutureString, self).__init__(*args)

    def getclass(self):
        return bytes

    __class__ = property(getclass)

    def as_foundationdb_key(self):
        return self.value

    def as_foundationdb_value(self):
        return self.value

    def __str__(self):
        return self.value.__str__()

    def __bytes__(self):
        return self.value

    def __repr__(self):
        return self.value.__repr__()

    def __add__(self, rhs):
        if isinstance(rhs, FutureString):
            rhs = rhs.value
        return self.value + rhs

    def __radd__(self, lhs):
        if isinstance(lhs, FutureString):
            lhs = lhs.value
        return lhs + self.value

    def __mul__(self, rhs):
        return self.value * rhs

    def __rmul__(self, lhs):
        return lhs * self.value

    def __lt__(self, rhs):
        if isinstance(rhs, FutureString):
            rhs = rhs.value
        return self.value < rhs

    def __le__(self, rhs):
        if isinstance(rhs, FutureString):
            rhs = rhs.value
        return self.value <= rhs

    def __gt__(self, rhs):
        if isinstance(rhs, FutureString):
            rhs = rhs.value
        return self.value > rhs

    def __ge__(self, rhs):
        if isinstance(rhs, FutureString):
            rhs = rhs.value
        return self.value >= rhs

    def __eq__(self, rhs):
        if isinstance(rhs, FutureString):
            rhs = rhs.value
        return self.value == rhs

    def __ne__(self, rhs):
        return not self == rhs

    def __nonzero__(self):
        return bool(self.value)

    def __int__(self):
        return int(self.value)


def makewrapper(func):
    def tmpfunc(self, *args):
        return func(self.value, *args)

    return tmpfunc


for i in dir(bytes):
    if not i.startswith("_") or i in (
        "__getitem__",
        "__getslice__",
        "__hash__",
        "__len__",
    ):
        setattr(FutureString, i, makewrapper(getattr(bytes, i)))


class Value(FutureString):
    def _getter(self):
        present = ctypes.c_int()
        value = ctypes.pointer(ctypes.c_byte())
        value_length = ctypes.c_int()
        self.capi.fdb_future_get_value(
            self.fpointer,
            ctypes.byref(present),
            ctypes.byref(value),
            ctypes.byref(value_length),
        )
        if present.value:
            self.value = ctypes.string_at(value, value_length.value)
        else:
            self.value = None

    def present(self):
        return self.value is not None


class Key(FutureString):
    def _getter(self):
        key = ctypes.pointer(ctypes.c_byte())
        key_length = ctypes.c_int()
        self.capi.fdb_future_get_key(
            self.fpointer, ctypes.byref(key), ctypes.byref(key_length)
        )
        self.value = ctypes.string_at(key, key_length.value)


class FormerFuture(_FDBBase):
    def wait(self):
        return self

    def is_ready(self):
        return True

    def block_until_ready(self):
        pass

    def on_ready(self, callback):
        try:
            callback(self)
        except Exception:
            try:
                sys.stderr.write(
                    "Discarding uncaught exception from user FDB callback:\n"
                )
                traceback.print_exception(*sys.exc_info(), file=sys.stderr)
            except Exception:
                pass


class _TransactionCreator(_FDBBase):
    def get(self, key):
        return _TransactionCreator.__creator_getitem(self, key)

    def __getitem__(self, key):
        if isinstance(key, slice):
            return self.get_range(key.start, key.stop, reverse=(key.step == -1))
        return _TransactionCreator.__creator_getitem(self, key)

    def get_key(self, key_selector):
        return _TransactionCreator.__creator_get_key(self, key_selector)

    def get_range(
        self, begin, end, limit=0, reverse=False, streaming_mode=StreamingMode.want_all
    ):
        return _TransactionCreator.__creator_get_range(
            self, begin, end, limit, reverse, streaming_mode
        )

    def get_range_startswith(self, prefix, *args, **kwargs):
        return _TransactionCreator.__creator_get_range_startswith(
            self, prefix, *args, **kwargs
        )

    def set(self, key, value):
        _TransactionCreator.__creator_setitem(self, key, value)

    def __setitem__(self, key, value):
        _TransactionCreator.__creator_setitem(self, key, value)

    def clear(self, key):
        _TransactionCreator.__creator_delitem(self, key)

    def clear_range(self, begin, end):
        _TransactionCreator.__creator_delitem(self, slice(begin, end))

    def __delitem__(self, key_or_slice):
        _TransactionCreator.__creator_delitem(self, key_or_slice)

    def clear_range_startswith(self, prefix):
        _TransactionCreator.__creator_clear_range_startswith(self, prefix)

    def get_and_watch(self, key):
        return _TransactionCreator.__creator_get_and_watch(self, key)

    def set_and_watch(self, key, value):
        return _TransactionCreator.__creator_set_and_watch(self, key, value)

    def clear_and_watch(self, key):
        return _TransactionCreator.__creator_clear_and_watch(self, key)

    def create_transaction(self):
        pass

    def _atomic_operation(self, opcode, key, param):
        _TransactionCreator.__creator_atomic_operation(self, opcode, key, param)

    #### Transaction implementations ####
    @staticmethod
    @transactional
    def __creator_getitem(tr, key):
        return tr[key].value

    @staticmethod
    @transactional
    def __creator_get_key(tr, key_selector):
        return tr.get_key(key_selector).value

    @staticmethod
    @transactional
    def __creator_get_range(tr, begin, end, limit, reverse, streaming_mode):
        return tr.get_range(begin, end, limit, reverse, streaming_mode).to_list()

    @staticmethod
    @transactional
    def __creator_get_range_startswith(tr, prefix, *args, **kwargs):
        return tr.get_range_startswith(prefix, *args, **kwargs).to_list()

    @staticmethod
    @transactional
    def __creator_setitem(tr, key, value):
        tr[key] = value

    @staticmethod
    @transactional
    def __creator_clear_range_startswith(tr, prefix):
        tr.clear_range_startswith(prefix)

    @staticmethod
    @transactional
    def __creator_get_and_watch(tr, key):
        v = tr.get(key)
        return v, tr.watch(key)

    @staticmethod
    @transactional
    def __creator_set_and_watch(tr, key, value):
        tr.set(key, value)
        return tr.watch(key)

    @staticmethod
    @transactional
    def __creator_clear_and_watch(tr, key):
        del tr[key]
        return tr.watch(key)

    @staticmethod
    @transactional
    def __creator_delitem(tr, key_or_slice):
        del tr[key_or_slice]

    @staticmethod
    @transactional
    def __creator_atomic_operation(tr, opcode, key, param):
        tr._atomic_operation(opcode, key, param)

    # Asynchronous transactions
    @staticmethod
    def declare_asynchronous_transactions():
        Return = asyncio.Return
        From = asyncio.From
        coroutine = asyncio.coroutine

        class TransactionCreator:
            @staticmethod
            @transactional
            @coroutine
            def __creator_getitem(tr, key):
                # raise Return(( yield From( tr[key] ) ))
                raise Return(tr[key])
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_get_key(tr, key_selector):
                raise Return(tr.get_key(key_selector))
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_get_range(tr, begin, end, limit, reverse, streaming_mode):
                raise Return(
                    (
                        yield From(
                            tr.get_range(
                                begin, end, limit, reverse, streaming_mode
                            ).to_list()
                        )
                    )
                )

            @staticmethod
            @transactional
            @coroutine
            def __creator_get_range_startswith(tr, prefix, *args, **kwargs):
                raise Return(
                    (
                        yield From(
                            tr.get_range_startswith(prefix, *args, **kwargs).to_list()
                        )
                    )
                )

            @staticmethod
            @transactional
            @coroutine
            def __creator_setitem(tr, key, value):
                tr[key] = value
                raise Return()
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_clear_range_startswith(tr, prefix):
                tr.clear_range_startswith(prefix)
                raise Return()
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_get_and_watch(tr, key):
                v = tr.get(key)
                raise Return(v, tr.watch(key))
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_set_and_watch(tr, key, value):
                tr.set(key, value)
                raise Return(tr.watch(key))
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_clear_and_watch(tr, key):
                del tr[key]
                raise Return(tr.watch(key))
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_delitem(tr, key_or_slice):
                del tr[key_or_slice]
                raise Return()
                yield None

            @staticmethod
            @transactional
            @coroutine
            def __creator_atomic_operation(tr, opcode, key, param):
                tr._atomic_operation(opcode, key, param)
                raise Return()
                yield None

        return TransactionCreator


def process_tenant_name(name):
    if isinstance(name, tuple):
        return pack(name)
    elif isinstance(name, bytes):
        return name
    else:
        raise TypeError(
            "Tenant name must be of type "
            + bytes.__name__
            + " or of type "
            + tuple.__name__
        )


class Database(_TransactionCreator):
    def __init__(self, dpointer):
        self.dpointer = dpointer
        self.options = _DatabaseOptions(self)

    def __del__(self):
        # print("Destroying database 0x%x" % self.dpointer)
        self.capi.fdb_database_destroy(self.dpointer)

    def _set_option(self, option, param, length):
        self.capi.fdb_database_set_option(self.dpointer, option, param, length)

    def open_tenant(self, name):
        tname = process_tenant_name(name)
        pointer = ctypes.c_void_p()
        self.capi.fdb_database_open_tenant(
            self.dpointer, tname, len(tname), ctypes.byref(pointer)
        )
        return Tenant(pointer.value)

    def create_transaction(self):
        pointer = ctypes.c_void_p()
        self.capi.fdb_database_create_transaction(self.dpointer, ctypes.byref(pointer))
        return Transaction(pointer.value, self)

    def get_client_status(self):
        return Key(self.capi.fdb_database_get_client_status(self.dpointer))


class Tenant(_TransactionCreator):
    def __init__(self, tpointer):
        self.tpointer = tpointer

    def __del__(self):
        self.capi.fdb_tenant_destroy(self.tpointer)

    def create_transaction(self):
        pointer = ctypes.c_void_p()
        self.capi.fdb_tenant_create_transaction(self.tpointer, ctypes.byref(pointer))
        return Transaction(pointer.value, self)

    def get_id(self):
        return FutureInt64(self.capi.fdb_tenant_get_id(self.tpointer))


fill_operations()


class Cluster(_FDBBase):
    def __init__(self, cluster_file):
        self.cluster_file = cluster_file
        self.options = None

    def open_database(self, name):
        if name != b"DB":
            raise FDBError(2013)  # invalid_database_name

        return create_database(self.cluster_file)


def create_database(cluster_file=None):
    pointer = ctypes.c_void_p()
    _FDBBase.capi.fdb_create_database(
        optionalParamToBytes(cluster_file)[0], ctypes.byref(pointer)
    )
    return Database(pointer)


def create_cluster(cluster_file=None):
    return Cluster(cluster_file)


class KeySelector(object):
    def __init__(self, key, or_equal, offset):
        self.key = key
        self.or_equal = or_equal
        self.offset = offset

    def __add__(self, offset):
        return KeySelector(self.key, self.or_equal, self.offset + offset)

    def __sub__(self, offset):
        return KeySelector(self.key, self.or_equal, self.offset - offset)

    @classmethod
    def last_less_than(cls, key):
        return cls(key, False, 0)

    @classmethod
    def last_less_or_equal(cls, key):
        return cls(key, True, 0)

    @classmethod
    def first_greater_than(cls, key):
        return cls(key, True, 1)

    @classmethod
    def first_greater_or_equal(cls, key):
        return cls(key, False, 1)

    def __repr__(self):
        return "KeySelector(%r, %r, %r)" % (self.key, self.or_equal, self.offset)


class KVIter(object):
    def __init__(self, obj):
        self.obj = obj
        self.index = 0

    def __iter__(self):
        return self

    def next(self):
        self.index += 1
        if self.index == 1:
            return self.obj.key
        elif self.index == 2:
            return self.obj.value
        else:
            raise StopIteration

    def __next__(self):
        return self.next()


class KeyValueStruct(ctypes.Structure):
    _fields_ = [
        ("key", ctypes.POINTER(ctypes.c_byte)),
        ("key_length", ctypes.c_int),
        ("value", ctypes.POINTER(ctypes.c_byte)),
        ("value_length", ctypes.c_int),
    ]
    _pack_ = 4


class KeyStruct(ctypes.Structure):
    _fields_ = [("key", ctypes.POINTER(ctypes.c_byte)), ("key_length", ctypes.c_int)]
    _pack_ = 4


class KeyValue(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def __repr__(self):
        return "%s: %s" % (repr(self.key), repr(self.value))

    def __iter__(self):
        return KVIter(self)


def check_error_code(code, func, arguments):
    if code:
        raise FDBError(code)
    return None


if sys.maxsize <= 2**32:
    raise Exception("FoundationDB API requires a 64-bit python interpreter!")
if platform.system() == "Windows":
    capi_name = "fdb_c.dll"
elif platform.system() == "Linux":
    capi_name = "libfdb_c.so"
elif platform.system() == "FreeBSD":
    capi_name = "libfdb_c.so"
elif platform.system() == "Darwin":
    capi_name = "libfdb_c.dylib"
elif sys.platform == "win32":
    capi_name = "fdb_c.dll"
elif sys.platform.startswith("cygwin"):
    capi_name = "fdb_c.dll"
elif sys.platform.startswith("linux"):
    capi_name = "libfdb_c.so"
elif sys.platform == "darwin":
    capi_name = "libfdb_c.dylib"
else:
    raise Exception(
        "Platform (%s) %s is not supported by the FoundationDB API!"
        % (sys.platform, platform.system())
    )
this_dir = os.path.dirname(__file__)


# Preferred installation: The C API library or a symbolic link to the
#    library should be in the same directory as this module.
# Failing that, a file named $(capi_name).pth should be in the same directory,
#    and a relative path to the library (including filename)
# Failing that, we try to load the C API library without qualification, and
#    the library should be on the platform's dynamic library search path
def read_pth_file():
    pth_file = os.path.join(this_dir, capi_name + ".pth")
    if not os.path.exists(pth_file):
        return None
    pth = _open_file(pth_file, "rt").read().strip()
    if pth[0] != "/":
        pth = os.path.join(this_dir, pth)
    return pth


for pth in [
    lambda: os.path.join(this_dir, capi_name),
    # lambda: os.path.join(this_dir, "../../lib", capi_name),  # For compatibility with existing unix installation process... should be removed
    read_pth_file,
]:
    p = pth()
    if p and os.path.exists(p):
        _capi = ctypes.CDLL(os.path.abspath(p))
        break
else:
    try:
        _capi = ctypes.CDLL(capi_name)
    except Exception:
        # The system python on OS X can't find the library installed to /usr/local/lib if SIP is enabled
        # find_library does find the location in /usr/local/lib, so if the above fails fallback to using it
        lib_path = ctypes.util.find_library("fdb_c")
        if lib_path is not None:
            try:
                _capi = ctypes.CDLL(lib_path)
            except Exception:
                raise Exception("Unable to locate the FoundationDB API shared library!")
        else:
            raise Exception("Unable to locate the FoundationDB API shared library!")


def keyToBytes(k):
    if hasattr(k, "as_foundationdb_key"):
        k = k.as_foundationdb_key()
    if not isinstance(k, bytes):
        raise TypeError("Key must be of type " + bytes.__name__)
    return k


def valueToBytes(v):
    if hasattr(v, "as_foundationdb_value"):
        v = v.as_foundationdb_value()
    if not isinstance(v, bytes):
        raise TypeError("Value must be of type " + bytes.__name__)
    return v


def paramToBytes(v):
    if isinstance(v, FutureString):
        v = v.value
    if not isinstance(v, bytes) and hasattr(v, "encode"):
        v = v.encode("utf8")
    if not isinstance(v, bytes):
        raise TypeError("Parameter must be a string")
    return v


def optionalParamToBytes(v):
    if v is None:
        return (None, 0)
    else:
        v = paramToBytes(v)
        return (v, len(v))


_FDBBase.capi = _capi
_CBFUNC = ctypes.CFUNCTYPE(None, ctypes.c_void_p)


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

    _capi.fdb_network_set_option.argtypes = [
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
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

    _capi.fdb_future_set_callback.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_void_p,
    ]
    _capi.fdb_future_set_callback.restype = int
    _capi.fdb_future_set_callback.errcheck = check_error_code

    _capi.fdb_future_get_error.argtypes = [ctypes.c_void_p]
    _capi.fdb_future_get_error.restype = int
    _capi.fdb_future_get_error.errcheck = check_error_code

    _capi.fdb_future_get_int64.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_int64),
    ]
    _capi.fdb_future_get_int64.restype = ctypes.c_int
    _capi.fdb_future_get_int64.errcheck = check_error_code

    _capi.fdb_future_get_uint64.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_uint64),
    ]
    _capi.fdb_future_get_uint64.restype = ctypes.c_uint
    _capi.fdb_future_get_uint64.errcheck = check_error_code

    _capi.fdb_future_get_key.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.POINTER(ctypes.c_byte)),
        ctypes.POINTER(ctypes.c_int),
    ]
    _capi.fdb_future_get_key.restype = ctypes.c_int
    _capi.fdb_future_get_key.errcheck = check_error_code

    _capi.fdb_future_get_value.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.POINTER(ctypes.c_byte)),
        ctypes.POINTER(ctypes.c_int),
    ]
    _capi.fdb_future_get_value.restype = ctypes.c_int
    _capi.fdb_future_get_value.errcheck = check_error_code

    _capi.fdb_future_get_keyvalue_array.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.POINTER(KeyValueStruct)),
        ctypes.POINTER(ctypes.c_int),
        ctypes.POINTER(ctypes.c_int),
    ]
    _capi.fdb_future_get_keyvalue_array.restype = int
    _capi.fdb_future_get_keyvalue_array.errcheck = check_error_code

    _capi.fdb_future_get_key_array.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.POINTER(KeyStruct)),
        ctypes.POINTER(ctypes.c_int),
    ]
    _capi.fdb_future_get_key_array.restype = int
    _capi.fdb_future_get_key_array.errcheck = check_error_code

    _capi.fdb_future_get_string_array.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.POINTER(ctypes.c_char_p)),
        ctypes.POINTER(ctypes.c_int),
    ]
    _capi.fdb_future_get_string_array.restype = int
    _capi.fdb_future_get_string_array.errcheck = check_error_code

    _capi.fdb_create_database.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    _capi.fdb_create_database.restype = ctypes.c_int
    _capi.fdb_create_database.errcheck = check_error_code

    _capi.fdb_database_destroy.argtypes = [ctypes.c_void_p]
    _capi.fdb_database_destroy.restype = None

    _capi.fdb_database_open_tenant.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    _capi.fdb_database_open_tenant.restype = ctypes.c_int
    _capi.fdb_database_open_tenant.errcheck = check_error_code

    _capi.fdb_database_create_transaction.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    _capi.fdb_database_create_transaction.restype = ctypes.c_int
    _capi.fdb_database_create_transaction.errcheck = check_error_code

    _capi.fdb_database_set_option.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_database_set_option.restype = ctypes.c_int
    _capi.fdb_database_set_option.errcheck = check_error_code

    _capi.fdb_database_get_client_status.argtypes = [ctypes.c_void_p]
    _capi.fdb_database_get_client_status.restype = ctypes.c_void_p

    _capi.fdb_tenant_destroy.argtypes = [ctypes.c_void_p]
    _capi.fdb_tenant_destroy.restype = None

    _capi.fdb_tenant_get_id.argtypes = [ctypes.c_void_p]
    _capi.fdb_tenant_get_id.restype = ctypes.c_void_p

    _capi.fdb_tenant_create_transaction.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    _capi.fdb_tenant_create_transaction.restype = ctypes.c_int
    _capi.fdb_tenant_create_transaction.errcheck = check_error_code

    _capi.fdb_transaction_destroy.argtypes = [ctypes.c_void_p]
    _capi.fdb_transaction_destroy.restype = None

    _capi.fdb_transaction_cancel.argtypes = [ctypes.c_void_p]
    _capi.fdb_transaction_cancel.restype = None

    _capi.fdb_transaction_set_read_version.argtypes = [ctypes.c_void_p, ctypes.c_int64]
    _capi.fdb_transaction_set_read_version.restype = None

    _capi.fdb_transaction_get_read_version.argtypes = [ctypes.c_void_p]
    _capi.fdb_transaction_get_read_version.restype = ctypes.c_void_p

    _capi.fdb_transaction_get.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_get.restype = ctypes.c_void_p

    _capi.fdb_transaction_get_key.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_get_key.restype = ctypes.c_void_p

    _capi.fdb_transaction_get_range.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_get_range.restype = ctypes.c_void_p

    _capi.fdb_transaction_get_estimated_range_size_bytes.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_get_estimated_range_size_bytes.restype = ctypes.c_void_p

    _capi.fdb_transaction_get_range_split_points.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_get_range_split_points.restype = ctypes.c_void_p

    _capi.fdb_transaction_add_conflict_range.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_add_conflict_range.restype = ctypes.c_int
    _capi.fdb_transaction_add_conflict_range.errcheck = check_error_code

    _capi.fdb_transaction_get_addresses_for_key.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_get_addresses_for_key.restype = ctypes.c_void_p

    _capi.fdb_transaction_set_option.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_set_option.restype = ctypes.c_int
    _capi.fdb_transaction_set_option.errcheck = check_error_code

    _capi.fdb_transaction_atomic_op.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_atomic_op.restype = None

    _capi.fdb_transaction_set.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_set.restype = None

    _capi.fdb_transaction_clear.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_clear.restype = None

    _capi.fdb_transaction_clear_range.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_clear_range.restype = None

    _capi.fdb_transaction_watch.argtypes = [
        ctypes.c_void_p,
        ctypes.c_void_p,
        ctypes.c_int,
    ]
    _capi.fdb_transaction_watch.restype = ctypes.c_void_p

    _capi.fdb_transaction_commit.argtypes = [ctypes.c_void_p]
    _capi.fdb_transaction_commit.restype = ctypes.c_void_p

    _capi.fdb_transaction_get_committed_version.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_int64),
    ]
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


if hasattr(ctypes.pythonapi, "Py_IncRef"):

    def _pin_callback(cb):
        ctypes.pythonapi.Py_IncRef(ctypes.py_object(cb))

    def _unpin_callback(cb):
        ctypes.pythonapi.Py_DecRef(ctypes.py_object(cb))

else:
    _active_callbacks = set()
    _pin_callback = _active_callbacks.add
    _unpin_callback = _active_callbacks.remove


def init(event_model=None):
    """Initialize the FDB interface.

    Consider using open() as a higher-level interface.

    Keyword arguments:
    event_model -- the event model to support (default None, also "gevent")

    """
    with _network_thread_reentrant_lock:
        global _network_thread

        # init should only succeed once; if _network_thread is not
        # None, someone has already successfully called init
        if _network_thread:
            raise FDBError(2000)

        try:

            class NetworkThread(threading.Thread):
                def run(self):
                    try:
                        _capi.fdb_run_network()
                    except FDBError as e:
                        sys.stderr.write(
                            "Unhandled error in FoundationDB network thread: %s\n" % e
                        )
                    # print("Network stopped")

            _network_thread = NetworkThread()
            _network_thread.daemon = True
            # may not set actual underlying OS thread name
            _network_thread.name = "fdb-network-thread"

            if event_model is not None:
                if event_model == "gevent":
                    import gevent

                    if gevent.__version__[0] != "0":

                        def nullf():
                            pass

                        class ThreadEvent(object):
                            has_async_ = hasattr(gevent.get_hub().loop, "async_")

                            def __init__(self):
                                if ThreadEvent.has_async_:
                                    self.gevent_async = gevent.get_hub().loop.async_()
                                else:
                                    self.gevent_async = getattr(
                                        gevent.get_hub().loop, "async"
                                    )()

                                self.gevent_async.start(nullf)

                            def set(self):
                                self.gevent_async.send()

                            def wait(self):
                                gevent.get_hub().wait(self.gevent_async)

                    else:
                        # gevent 0.x doesn't have async, so use a pipe.  This doesn't work on Windows.
                        if platform.system() == "Windows":
                            raise Exception(
                                "The 'gevent' event_model requires gevent 1.0 on Windows."
                            )

                        import gevent.socket

                        class ThreadEvent(object):
                            def __init__(self):
                                self.pair = os.pipe()

                            def set(self):
                                os.write(self.pair[1], "!")

                            def wait(self):
                                gevent.socket.wait_read(self.pair[0])

                            def __del__(self):
                                os.close(self.pair[0])
                                os.close(self.pair[1])

                    Future.Event = ThreadEvent

                    def _gevent_block_until_ready(self):
                        e = self.Event()

                        def is_ready_cb(future):
                            e.set()

                        self.on_ready(is_ready_cb)
                        e.wait()

                    Future.block_until_ready = _gevent_block_until_ready
                elif event_model == "debug":
                    import time

                    class DebugEvent(object):
                        def __init__(self):
                            self.ev = threading.Event()

                        def set(self):
                            self.ev.set()

                        def wait(self):
                            while not self.ev.isSet():
                                self.ev.wait(0.001)

                    Future.Event = DebugEvent

                    def _debug_block_until_ready(self):
                        while not self.is_ready():
                            time.sleep(0.001)

                    Future.block_until_ready = _debug_block_until_ready
                elif event_model == "asyncio":
                    global asyncio
                    try:
                        import asyncio
                    except ImportError:
                        import trollius as asyncio

                    if isinstance(asyncio.futures._FUTURE_CLASSES, type):
                        asyncio.futures._FUTURE_CLASSES = (
                            asyncio.futures._FUTURE_CLASSES,
                        )
                    asyncio.futures._FUTURE_CLASSES += (Future,)

                    def _do_not_block(self):
                        if not self.is_ready():
                            raise Exception("Future not ready")

                    Future.block_until_ready = _do_not_block
                    Future.call_soon_threadsafe = (
                        asyncio.get_event_loop().call_soon_threadsafe
                    )
                    Future._loop = asyncio.get_event_loop()

                    def iterate(self):
                        """Usage:
                        fa = tr.get_range(...).iterate()
                        for k,v in (yield From(fa)):
                            print(k,v)
                            yield From(fa)"""

                        def it():
                            yield asyncio.From(self._future)
                            raise asyncio.Return(self)

                        return it()

                    FDBRange.iterate = iterate
                    AT = _TransactionCreator.declare_asynchronous_transactions()
                    for name in dir(AT):
                        if name.startswith("__TransactionCreator__creator_"):
                            setattr(_TransactionCreator, name, getattr(AT, name))

                    def to_list(self):
                        if self._mode == StreamingMode.iterator:
                            if self._limit > 0:
                                mode = StreamingMode.exact
                            else:
                                mode = StreamingMode.want_all
                        else:
                            mode = self._mode
                        yield asyncio.From(self._future)
                        out = []
                        for kv in self.__iter__(mode=mode):
                            out.append(kv)
                            yield asyncio.From(self._future)
                        raise asyncio.Return(out)

                    FDBRange.to_list = to_list
                else:
                    # Hard coded error
                    raise FDBError(2000)

            _capi.fdb_setup_network()

            # Sketchy... the only error returned by fdb_run_network
            # (invoked by _network_thread) is if the network hasn't
            # been setup, so if we get here without exception we know
            # it has been.
            _network_thread.start()
        except Exception:
            # We assigned _network_thread but didn't succeed in init,
            # so clear it out so the next caller has a chance
            _network_thread = None
            raise


def init_v13(local_address, event_model=None):
    return init(event_model)


open_databases = {}

cacheLock = threading.Lock()


def open(cluster_file=None, event_model=None):
    """Opens the given database (or the default database of the cluster indicated
    by the fdb.cluster file in a platform-specific location, if no cluster_file
    or database_name is provided).  Initializes the FDB interface as required."""

    with _network_thread_reentrant_lock:
        if not _network_thread:
            init(event_model=event_model)

    with cacheLock:
        if cluster_file not in open_databases:
            open_databases[cluster_file] = create_database(cluster_file)

        return open_databases[(cluster_file)]


def open_v609(cluster_file=None, database_name=b"DB", event_model=None):
    if database_name != b"DB":
        raise FDBError(2013)  # invalid_database_name

    return open(cluster_file, event_model)


def open_v13(cluster_id_path, database_name, local_address=None, event_model=None):
    return open_v609(cluster_id_path, database_name, event_model)


@atexit.register
def _stop_on_exit():
    if _network_thread:
        _capi.fdb_stop_network()
        _network_thread.join()


def strinc(key):
    key = key.rstrip(b"\xff")
    if len(key) == 0:
        raise ValueError("Key must contain at least one byte not equal to 0xFF.")

    return key[:-1] + six.int2byte(ord(key[-1:]) + 1)
