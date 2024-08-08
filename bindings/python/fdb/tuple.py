#
# tuple.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
import uuid
import struct
import math
import functools
from bisect import bisect_left

import fdb

_size_limits = tuple((1 << (i * 8)) - 1 for i in range(9))

int2byte = struct.Struct(">B").pack

# Define type codes:
NULL_CODE = 0x00
BYTES_CODE = 0x01
STRING_CODE = 0x02
NESTED_CODE = 0x05
INT_ZERO_CODE = 0x14
POS_INT_END = 0x1D
NEG_INT_START = 0x0B
FLOAT_CODE = 0x20
DOUBLE_CODE = 0x21
FALSE_CODE = 0x26
TRUE_CODE = 0x27
UUID_CODE = 0x30
VERSIONSTAMP_CODE = 0x33

# Reserved: Codes 0x03, 0x04, 0x23, and 0x24 are reserved for historical reasons.


def _find_terminator(v, pos):
    # Finds the start of the next terminator [\x00]![\xff] or the end of v
    while True:
        pos = v.find(b"\x00", pos)
        if pos < 0:
            return len(v)
        if pos + 1 == len(v) or v[pos + 1 : pos + 2] != b"\xff":
            return pos
        pos += 2


# If encoding and sign bit is 1 (negative), flip all of the bits. Otherwise, just flip sign.
# If decoding and sign bit is 0 (negative), flip all of the bits. Otherwise, just flip sign.
def _float_adjust(v, encode):
    if encode and v[0] & 0x80 != 0x00:
        return b"".join(map(lambda x: int2byte(x ^ 0xFF), iter(v)))
    elif not encode and v[0] & 0x80 != 0x80:
        return b"".join(map(lambda x: int2byte(x ^ 0xFF), iter(v)))
    else:
        return int2byte(v[0] ^ 0x80) + v[1:]


@functools.total_ordering
class SingleFloat(object):
    def __init__(self, value):
        if isinstance(value, float):
            # Restrict to the first 4 bytes (essentially)
            self.value = ctypes.c_float(value).value
        elif isinstance(value, ctypes.c_float):
            self.value = value.value
        elif isinstance(value, int):
            self.value = ctypes.c_float(value).value
        else:
            raise ValueError(
                "Incompatible type for single-precision float: " + repr(value)
            )

    # Comparisons
    def __eq__(self, other):
        if isinstance(other, SingleFloat):
            return _compare_floats(self.value, other.value) == 0
        else:
            return False

    def __lt__(self, other):
        return _compare_floats(self.value, other.value) < 0

    def __str__(self):
        return str(self.value)

    def __repr__(self):
        return "SingleFloat(" + str(self.value) + ")"

    def __hash__(self):
        # Left-circulate the child hash to make hash(self) != hash(self.value)
        v_hash = hash(self.value)
        if v_hash >= 0:
            return (v_hash >> 16) + ((v_hash & 0xFFFF) << 16)
        else:
            return ((v_hash >> 16) + 1) - ((abs(v_hash) & 0xFFFF) << 16)

    def __nonzero__(self):
        return bool(self.value)


@functools.total_ordering
class Versionstamp(object):
    LENGTH = 12
    _TR_VERSION_LEN = 10
    _MAX_USER_VERSION = (1 << 16) - 1
    _UNSET_TR_VERSION = 10 * int2byte(0xFF)
    _STRUCT_FORMAT_STRING = ">" + str(_TR_VERSION_LEN) + "sH"

    @classmethod
    def validate_tr_version(cls, tr_version):
        if tr_version is None:
            return
        if not isinstance(tr_version, bytes):
            raise TypeError(
                "Global version has illegal type "
                + str(type(tr_version))
                + " (requires bytes)"
            )
        elif len(tr_version) != cls._TR_VERSION_LEN:
            raise ValueError(
                "Global version has incorrect length "
                + str(len(tr_version))
                + " (requires "
                + str(cls._TR_VERSION_LEN)
                + ")"
            )

    @classmethod
    def validate_user_version(cls, user_version):
        if not isinstance(user_version, int):
            raise TypeError(
                "Local version has illegal type "
                + str(type(user_version))
                + " (requires int)"
            )
        elif user_version < 0 or user_version > cls._MAX_USER_VERSION:
            raise ValueError(
                "Local version has value "
                + str(user_version)
                + " which is out of range"
            )

    def __init__(self, tr_version=None, user_version=0):
        Versionstamp.validate_tr_version(tr_version)
        Versionstamp.validate_user_version(user_version)
        self.tr_version = tr_version
        self.user_version = user_version

    @staticmethod
    def incomplete(user_version=0):
        return Versionstamp(user_version=user_version)

    @classmethod
    def from_bytes(cls, v, start=0):
        if not isinstance(v, bytes):
            raise TypeError("Cannot parse versionstamp from non-byte string")
        elif len(v) - start < cls.LENGTH:
            raise ValueError(
                "Versionstamp byte string is too short (only "
                + str(len(v) - start)
                + " bytes to read from"
            )
        else:
            tr_version = v[start : start + cls._TR_VERSION_LEN]
            if tr_version == cls._UNSET_TR_VERSION:
                tr_version = None
            user_version = (
                v[start + cls._TR_VERSION_LEN] * (1 << 8)
                + v[start + cls._TR_VERSION_LEN + 1]
            )
            return Versionstamp(tr_version, user_version)

    def is_complete(self):
        return self.tr_version is not None

    def __repr__(self):
        return (
            "fdb.tuple.Versionstamp("
            + repr(self.tr_version)
            + ", "
            + repr(self.user_version)
            + ")"
        )

    def __str__(self):
        return (
            "Versionstamp("
            + repr(self.tr_version)
            + ", "
            + str(self.user_version)
            + ")"
        )

    def to_bytes(self):
        tr_version = self.tr_version
        if isinstance(tr_version, fdb.impl.Value):
            tr_version = tr_version.value
        return struct.pack(
            self._STRUCT_FORMAT_STRING,
            tr_version if self.is_complete() else self._UNSET_TR_VERSION,
            self.user_version,
        )

    def completed(self, new_tr_version):
        if self.is_complete():
            raise RuntimeError("Versionstamp already completed")
        else:
            return Versionstamp(new_tr_version, self.user_version)

    # Comparisons
    def __eq__(self, other):
        if isinstance(other, Versionstamp):
            return (
                self.tr_version == other.tr_version
                and self.user_version == other.user_version
            )
        else:
            return False

    def __lt__(self, other):
        if self.is_complete():
            if other.is_complete():
                if self.tr_version == other.tr_version:
                    return self.user_version < other.user_version
                else:
                    return self.tr_version < other.tr_version
            else:
                # All complete are less than all incomplete.
                return True
        else:
            if other.is_complete():
                # All incomplete are greater than all complete
                return False
            else:
                return self.user_version < other.user_version

    def __hash__(self):
        if self.tr_version is None:
            return hash(self.user_version)
        else:
            return hash(self.tr_version) * 37 ^ hash(self.user_version)

    def __nonzero__(self):
        return self.is_complete()


def _decode(v, pos):
    code = v[pos]
    if code == NULL_CODE:
        return None, pos + 1
    elif code == BYTES_CODE:
        end = _find_terminator(v, pos + 1)
        return v[pos + 1 : end].replace(b"\x00\xFF", b"\x00"), end + 1
    elif code == STRING_CODE:
        end = _find_terminator(v, pos + 1)
        return v[pos + 1 : end].replace(b"\x00\xFF", b"\x00").decode("utf-8"), end + 1
    elif code >= INT_ZERO_CODE and code < POS_INT_END:
        n = code - 20
        end = pos + 1 + n
        return struct.unpack(">Q", b"\x00" * (8 - n) + v[pos + 1 : end])[0], end
    elif code > NEG_INT_START and code < INT_ZERO_CODE:
        n = 20 - code
        end = pos + 1 + n
        return (
            struct.unpack(">Q", b"\x00" * (8 - n) + v[pos + 1 : end])[0]
            - _size_limits[n],
            end,
        )
    elif code == POS_INT_END:  # 0x1d; Positive 9-255 byte integer
        length = v[pos + 1]
        val = 0
        for i in _range(length):
            val = val << 8
            val += v[pos + 2 + i]
        return val, pos + 2 + length
    elif code == NEG_INT_START:  # 0x0b; Negative 9-255 byte integer
        length = v[pos + 1] ^ 0xFF
        val = 0
        for i in _range(length):
            val = val << 8
            val += v[pos + 2 + i]
        return val - (1 << (length * 8)) + 1, pos + 2 + length
    elif code == FLOAT_CODE:
        return (
            SingleFloat(
                struct.unpack(">f", _float_adjust(v[pos + 1 : pos + 5], False))[0]
            ),
            pos + 5,
        )
    elif code == DOUBLE_CODE:
        return (
            struct.unpack(">d", _float_adjust(v[pos + 1 : pos + 9], False))[0],
            pos + 9,
        )
    elif code == UUID_CODE:
        return uuid.UUID(bytes=v[pos + 1 : pos + 17]), pos + 17
    elif code == FALSE_CODE:
        if fdb.is_api_version_selected() and fdb.get_api_version() < 500:
            raise ValueError(
                "Invalid API version " + str(fdb._version) + " for boolean types"
            )
        return False, pos + 1
    elif code == TRUE_CODE:
        if fdb.is_api_version_selected() and fdb.get_api_version() < 500:
            raise ValueError(
                "Invalid API version " + str(fdb._version) + " for boolean types"
            )
        return True, pos + 1
    elif code == VERSIONSTAMP_CODE:
        return Versionstamp.from_bytes(v, pos + 1), pos + 1 + Versionstamp.LENGTH
    elif code == NESTED_CODE:
        ret = []
        end_pos = pos + 1
        while end_pos < len(v):
            if v[end_pos] == 0x00:
                if end_pos + 1 < len(v) and v[end_pos + 1] == 0xFF:
                    ret.append(None)
                    end_pos += 2
                else:
                    break
            else:
                val, end_pos = _decode(v, end_pos)
                ret.append(val)
        return tuple(ret), end_pos + 1
    else:
        raise ValueError("Unknown data type in DB: " + repr(v))


def _reduce_children(child_values):
    version_pos = -1
    len_so_far = 0
    bytes_list = []
    for child_bytes, child_pos in child_values:
        if child_pos >= 0:
            if version_pos >= 0:
                raise ValueError("Multiple incomplete versionstamps included in tuple")
            version_pos = len_so_far + child_pos
        len_so_far += len(child_bytes)
        bytes_list.append(child_bytes)
    return bytes_list, version_pos


def _bit_length(x):
    return x.bit_length()


def _encode(value, nested=False):
    # returns [code][data] (code != 0xFF)
    # encoded values are self-terminating
    # sorting need to work too!
    if value == None:  # ==, not is, because some fdb.impl.Value are equal to None
        if nested:
            return b"".join([int2byte(NULL_CODE), int2byte(0xFF)]), -1
        else:
            return b"".join([int2byte(NULL_CODE)]), -1
    elif isinstance(value, bytes):  # also gets non-None fdb.impl.Value
        return (
            int2byte(BYTES_CODE) + value.replace(b"\x00", b"\x00\xFF") + b"\x00",
            -1,
        )
    elif isinstance(value, str):
        return (
            int2byte(STRING_CODE)
            + value.encode("utf-8").replace(b"\x00", b"\x00\xFF")
            + b"\x00",
            -1,
        )
    elif isinstance(value, int) and (
        not isinstance(value, bool) or (hasattr(fdb, "_version") and fdb._version < 500)
    ):
        if value == 0:
            return b"".join([int2byte(INT_ZERO_CODE)]), -1
        elif value > 0:
            if value >= _size_limits[-1]:
                length = (_bit_length(value) + 7) // 8
                data = [int2byte(POS_INT_END), int2byte(length)]
                for i in _range(length - 1, -1, -1):
                    data.append(int2byte((value >> (8 * i)) & 0xFF))
                return b"".join(data), -1

            n = bisect_left(_size_limits, value)
            return int2byte(INT_ZERO_CODE + n) + struct.pack(">Q", value)[-n:], -1
        else:
            if -value >= _size_limits[-1]:
                length = (_bit_length(value) + 7) // 8
                value += (1 << (length * 8)) - 1
                data = [int2byte(NEG_INT_START), int2byte(length ^ 0xFF)]
                for i in _range(length - 1, -1, -1):
                    data.append(int2byte((value >> (8 * i)) & 0xFF))
                return b"".join(data), -1

            n = bisect_left(_size_limits, -value)
            maxv = _size_limits[n]
            return (
                int2byte(INT_ZERO_CODE - n) + struct.pack(">Q", maxv + value)[-n:],
                -1,
            )
    elif isinstance(value, ctypes.c_float) or isinstance(value, SingleFloat):
        return (
            int2byte(FLOAT_CODE) + _float_adjust(struct.pack(">f", value.value), True),
            -1,
        )
    elif isinstance(value, ctypes.c_double):
        return (
            int2byte(DOUBLE_CODE) + _float_adjust(struct.pack(">d", value.value), True),
            -1,
        )
    elif isinstance(value, float):
        return (
            int2byte(DOUBLE_CODE) + _float_adjust(struct.pack(">d", value), True),
            -1,
        )
    elif isinstance(value, uuid.UUID):
        return int2byte(UUID_CODE) + value.bytes, -1
    elif isinstance(value, bool):
        if value:
            return b"".join([int2byte(TRUE_CODE)]), -1
        else:
            return b"".join([int2byte(FALSE_CODE)]), -1
    elif isinstance(value, Versionstamp):
        version_pos = -1 if value.is_complete() else 1
        return int2byte(VERSIONSTAMP_CODE) + value.to_bytes(), version_pos
    elif isinstance(value, tuple) or isinstance(value, list):
        child_bytes, version_pos = _reduce_children(
            map(lambda x: _encode(x, True), value)
        )
        new_version_pos = -1 if version_pos < 0 else version_pos + 1
        return (
            b"".join([int2byte(NESTED_CODE)] + child_bytes + [int2byte(0x00)]),
            new_version_pos,
        )
    else:
        raise ValueError("Unsupported data type: " + str(type(value)))


# packs the tuple possibly for versionstamp operations and returns the position of the
# incomplete versionstamp
#  * if there are no incomplete versionstamp members, this returns the packed tuple and -1
#  * if there is exactly one incomplete versionstamp member, it returns the tuple with the
#    two extra version bytes and the position of the version start
#  * if there is more than one incomplete versionstamp member, it throws an error
def _pack_maybe_with_versionstamp(t, prefix=None):
    if not isinstance(t, tuple):
        raise Exception("fdbtuple pack() expects a tuple, got a " + str(type(t)))

    bytes_list = [prefix] if prefix is not None else []

    child_bytes, version_pos = _reduce_children(map(_encode, t))
    if version_pos >= 0:
        version_pos += len(prefix) if prefix is not None else 0
        bytes_list.extend(child_bytes)
        if fdb.is_api_version_selected() and fdb.get_api_version() < 520:
            bytes_list.append(struct.pack("<H", version_pos))
        else:
            bytes_list.append(struct.pack("<L", version_pos))
    else:
        bytes_list.extend(child_bytes)

    return b"".join(bytes_list), version_pos


# packs the specified tuple into a key
def pack(t, prefix=None):
    res, version_pos = _pack_maybe_with_versionstamp(t, prefix)
    if version_pos >= 0:
        raise ValueError("Incomplete versionstamp included in vanilla tuple pack")
    return res


# packs the specified tuple into a key for versionstamp operations
def pack_with_versionstamp(t, prefix=None):
    res, version_pos = _pack_maybe_with_versionstamp(t, prefix)
    if version_pos < 0:
        raise ValueError(
            "No incomplete versionstamp included in tuple pack with versionstamp"
        )
    return res


# unpacks the specified key into a tuple
def unpack(key, prefix_len=0):
    pos = prefix_len
    res = []
    while pos < len(key):
        r, pos = _decode(key, pos)
        res.append(r)
    return tuple(res)


# determines if there is at least one incomplete versionstamp in a tuple
def has_incomplete_versionstamp(t):
    def _elem_has_incomplete(item):
        if item is None:
            return False
        elif isinstance(item, Versionstamp):
            return not item.is_complete()
        elif isinstance(item, tuple) or isinstance(item, list):
            return has_incomplete_versionstamp(item)
        else:
            return False

    return any(map(_elem_has_incomplete, t))


_range = range


def range(t):
    """Returns a slice of keys that includes all tuples of greater
    length than the specified tuple that that start with the
    specified elements.

    e.g. range(('a', 'b')) includes all tuples ('a', 'b', ...)"""

    if not isinstance(t, tuple):
        raise Exception("fdbtuple range() expects a tuple, got a " + str(type(t)))

    p = pack(t)
    return slice(p + b"\x00", p + b"\xff")


def _code_for(value):
    if value == None:
        return NULL_CODE
    elif isinstance(value, bytes):
        return BYTES_CODE
    elif isinstance(value, str):
        return STRING_CODE
    elif (not hasattr(fdb, "_version") or fdb._version >= 500) and isinstance(
        value, bool
    ):
        return FALSE_CODE
    elif isinstance(value, int):
        return INT_ZERO_CODE
    elif isinstance(value, ctypes.c_float) or isinstance(value, SingleFloat):
        return FLOAT_CODE
    elif isinstance(value, ctypes.c_double) or isinstance(value, float):
        return DOUBLE_CODE
    elif isinstance(value, uuid.UUID):
        return UUID_CODE
    elif isinstance(value, Versionstamp):
        return VERSIONSTAMP_CODE
    elif isinstance(value, tuple) or isinstance(value, list):
        return NESTED_CODE
    else:
        raise ValueError("Unsupported data type: " + str(type(value)))


def _compare_floats(f1, f2):
    sign1 = int(math.copysign(1, f1))
    sign2 = int(math.copysign(1, f2))

    # This business with signs is to deal with negative zero, NaN, and infinity.
    if sign1 < sign2:
        # f1 is negative and f2 is positive.
        return -1
    elif sign1 > sign2:
        # f1 is positive and f2 is negative.
        return 1

    if not math.isnan(f1) and not math.isnan(f2):
        return -1 if f1 < f2 else 0 if f1 == f2 else 1

    # There are enough edge cases that bit comparison is safer.
    bytes1 = struct.pack(">d", f1)
    bytes2 = struct.pack(">d", f2)
    return sign1 * (-1 if bytes1 < bytes2 else 0 if bytes1 == bytes2 else 1)


def _compare_values(value1, value2):
    code1 = _code_for(value1)
    code2 = _code_for(value2)

    if code1 < code2:
        return -1
    elif code1 > code2:
        return 1

    # Compatible types.
    if code1 == NULL_CODE:
        return 0
    elif code1 == STRING_CODE:
        encoded1 = value1.encode("utf-8")
        encoded2 = value2.encode("utf-8")
        return -1 if encoded1 < encoded2 else 0 if encoded1 == encoded2 else 1
    elif code1 == FLOAT_CODE:
        f1 = value1 if isinstance(value1, SingleFloat) else SingleFloat(value1.value)
        f2 = value2 if isinstance(value2, SingleFloat) else SingleFloat(value2.value)
        return -1 if f1 < f2 else 0 if f1 == f2 else 1
    elif code1 == DOUBLE_CODE:
        d1 = value1.value if isinstance(value1, ctypes.c_double) else value1
        d2 = value2.value if isinstance(value2, ctypes.c_double) else value2
        return _compare_floats(d1, d2)
    elif code1 == NESTED_CODE:
        return compare(value1, value2)
    else:
        # Booleans, UUIDs, integers, and Versionstamps can just use standard comparison.
        return -1 if value1 < value2 else 0 if value1 == value2 else 1


# compare element by element and return -1 if t1 < t2 or 1 if t1 > t2 or 0 if t1 == t2
def compare(t1, t2):
    i = 0
    while i < len(t1) and i < len(t2):
        c = _compare_values(t1[i], t2[i])
        if c != 0:
            return c
        i += 1

    if i < len(t1):
        return 1
    elif i < len(t2):
        return -1
    else:
        return 0
