#
# tuple.py
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

import ctypes, uuid, struct, math
from bisect import bisect_left

from fdb import six
import fdb

_size_limits = tuple( (1 << (i*8))-1 for i in range(9) )

# Define type codes:
NULL_CODE       = 0x00
BYTES_CODE      = 0x01
STRING_CODE     = 0x02
NESTED_CODE     = 0x05
INT_ZERO_CODE   = 0x14
POS_INT_END     = 0x1d
NEG_INT_START   = 0x0b
FLOAT_CODE      = 0x20
DOUBLE_CODE     = 0x21
FALSE_CODE      = 0x26
TRUE_CODE       = 0x27
UUID_CODE       = 0x30

# Reserved: Codes 0x03, 0x04, 0x23, and 0x24 are reserved for historical reasons.

def _find_terminator( v, pos ):
    # Finds the start of the next terminator [\x00]![\xff] or the end of v
    while True:
        pos = v.find(b'\x00', pos)
        if pos < 0:
            return len(v)
        if pos+1 == len(v) or v[pos+1:pos+2] != b'\xff':
            return pos
        pos += 2

# If encoding and sign bit is 1 (negative), flip all of the bits. Otherwise, just flip sign.
# If decoding and sign bit is 0 (negative), flip all of the bits. Otherwise, just flip sign.
def _float_adjust( v, encode ):
    if encode and six.indexbytes(v, 0) & 0x80 != 0x00:
        return b''.join(map(lambda x: six.int2byte(x ^ 0xff), six.iterbytes(v)))
    elif not encode and six.indexbytes(v, 0) & 0x80 != 0x80:
        return b''.join(map(lambda x: six.int2byte(x ^ 0xff), six.iterbytes(v)))
    else:
        return six.int2byte(six.indexbytes(v, 0) ^ 0x80) + v[1:]

class SingleFloat(object):
    def __init__(self, value):
        if isinstance(value, float):
            # Restrict to the first 4 bytes (essentially)
            self.value = ctypes.c_float(value).value
        elif isinstance(value, ctypes.c_float):
            self.value = value.value
        elif isinstance(value, six.integertypes):
            self.value = ctypes.c_float(value).value
        else:
	        raise ValueError("Incompatible type for single-precision float: " + repr(value))

    # Comparisons
    def __eq__(self, other):
        if isinstance(other, SingleFloat):
            return _compare_floats(self.value, other.value) == 0
        else:
            return False

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return _compare_floats(self.value, other.value) < 0

    def __le__(self, other):
        return _compare_floats(self.value, other.value) <= 0

    def __gt__(self, other):
        return not (self <= other)

    def __ge__(self, other):
        return not (self < other)

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

def _decode(v, pos):
    code = six.indexbytes(v, pos)
    if code == NULL_CODE:
        return None, pos+1
    elif code == BYTES_CODE:
        end = _find_terminator(v, pos+1)
        return v[pos+1:end].replace(b"\x00\xFF", b"\x00"), end+1
    elif code == STRING_CODE:
        end = _find_terminator(v, pos+1)
        return v[pos+1:end].replace(b"\x00\xFF", b"\x00").decode("utf-8"), end+1
    elif code >= INT_ZERO_CODE and code < POS_INT_END:
        n = code - 20
        end = pos + 1 + n
        return struct.unpack(">Q", b'\x00'*(8-n) + v[pos+1:end])[0], end
    elif code > NEG_INT_START and code < INT_ZERO_CODE:
        n = 20 - code
        end = pos + 1 + n
        return struct.unpack(">Q", b'\x00'*(8-n) + v[pos+1:end])[0]-_size_limits[n], end
    elif code == POS_INT_END: # 0x1d; Positive 9-255 byte integer
        length = six.indexbytes(v, pos+1)
        val = 0
        for i in _range(length):
            val = val << 8
            val += six.indexbytes(v, pos+2+i)
        return val, pos+2+length
    elif code == NEG_INT_START: # 0x0b; Negative 9-255 byte integer
        length = six.indexbytes(v, pos+1)^0xff
        val = 0
        for i in _range(length):
            val = val << 8
            val += six.indexbytes(v, pos+2+i)
        return val - (1<<(length*8)) + 1, pos+2+length
    elif code == FLOAT_CODE:
        return SingleFloat(struct.unpack(">f", _float_adjust(v[pos+1:pos+5], False))[0]), pos+5
    elif code == DOUBLE_CODE:
        return struct.unpack(">d", _float_adjust(v[pos+1:pos+9], False))[0], pos+9
    elif code == UUID_CODE:
        return uuid.UUID(bytes=v[pos+1:pos+17]), pos+17
    elif code == FALSE_CODE:
        if hasattr(fdb, "_version") and fdb._version < 500:
            raise ValueError("Invalid API version " + str(fdb._version) + " for boolean types")
        return False, pos+1
    elif code == TRUE_CODE:
        if hasattr(fdb, "_version") and fdb._version < 500:
            raise ValueError("Invalid API version " + str(fdb._version) + " for boolean types")
        return True, pos+1
    elif code == NESTED_CODE:
        ret = []
        end_pos = pos+1
        while end_pos < len(v):
            if six.indexbytes(v, end_pos) == 0x00:
                if end_pos+1 < len(v) and six.indexbytes(v, end_pos+1) == 0xff:
                    ret.append(None)
                    end_pos += 2
                else:
                    break
            else:
                val, end_pos = _decode(v, end_pos)
                ret.append(val)
        return tuple(ret), end_pos+1
    else:
        raise ValueError("Unknown data type in DB: " + repr(v))

def _encode(value, nested=False):
    # returns [code][data] (code != 0xFF)
    # encoded values are self-terminating
    # sorting need to work too!
    if value == None:  # ==, not is, because some fdb.impl.Value are equal to None
        if nested:
            return b''.join([six.int2byte(NULL_CODE), six.int2byte(0xff)])
        else:
            return b''.join([six.int2byte(NULL_CODE)])
    elif isinstance(value, bytes): # also gets non-None fdb.impl.Value
        return six.int2byte(BYTES_CODE) + value.replace(b'\x00', b'\x00\xFF') + b'\x00'
    elif isinstance(value, six.text_type):
        return six.int2byte(STRING_CODE) + value.encode('utf-8').replace(b'\x00', b'\x00\xFF') + b'\x00'
    elif isinstance(value, six.integer_types) and (not isinstance(value, bool) or (hasattr(fdb, '_version') and fdb._version < 500)):
        if value == 0:
            return b''.join([six.int2byte(INT_ZERO_CODE)])
        elif value > 0:
            if value >= _size_limits[-1]:
                length = (value.bit_length()+7)//8
                data = [six.int2byte(POS_INT_END), six.int2byte(length)]
                for i in _range(length-1,-1,-1):
                    data.append(six.int2byte( (value>>(8*i))&0xff ))
                return b''.join(data)

            n = bisect_left( _size_limits, value )
            return six.int2byte(INT_ZERO_CODE + n) + struct.pack( ">Q", value )[-n:]
        else:
            if -value >= _size_limits[-1]:
                length = (value.bit_length()+7)//8
                value += (1<<(length*8)) - 1
                data = [six.int2byte(NEG_INT_START), six.int2byte(length^0xff)]
                for i in _range(length-1,-1,-1):
                    data.append(six.int2byte( (value>>(8*i))&0xff ))
                return b''.join(data)

            n = bisect_left( _size_limits, -value )
            maxv = _size_limits[n]
            return six.int2byte(INT_ZERO_CODE - n) + struct.pack( ">Q", maxv+value)[-n:]
    elif isinstance(value, ctypes.c_float) or isinstance(value, SingleFloat):
        return six.int2byte(FLOAT_CODE) + _float_adjust(struct.pack(">f", value.value), True)
    elif isinstance(value, ctypes.c_double):
        return six.int2byte(DOUBLE_CODE) + _float_adjust(struct.pack(">d", value.value), True)
    elif isinstance(value, float):
        return six.int2byte(DOUBLE_CODE) + _float_adjust(struct.pack(">d", value), True)
    elif isinstance(value, uuid.UUID):
        return six.int2byte(UUID_CODE) + value.bytes
    elif isinstance(value, bool):
        if value:
            return b''.join([six.int2byte(TRUE_CODE)])
        else:
            return b''.join([six.int2byte(FALSE_CODE)])
    elif isinstance(value, tuple) or isinstance(value, list):
       return b''.join([six.int2byte(NESTED_CODE)] + list(map(lambda x: _encode(x, True), value)) + [six.int2byte(0x00)])
    else:
        raise ValueError("Unsupported data type: " + str(type(value)))

# packs the specified tuple into a key
def pack(t):
    if not isinstance(t, tuple):
        raise Exception("fdbtuple pack() expects a tuple, got a " + str(type(t)))
    return b''.join([_encode(x) for x in t])

# unpacks the specified key into a tuple
def unpack(key):
    pos = 0
    res = []
    while pos < len(key):
        r, pos = _decode(key, pos)
        res.append(r)
    return tuple(res)

_range = range
def range(t):
    """Returns a slice of keys that includes all tuples of greater
    length than the specified tuple that that start with the
    specified elements.

    e.g. range(('a', 'b')) includes all tuples ('a', 'b', ...)"""

    if not isinstance(t, tuple):
        raise Exception("fdbtuple range() expects a tuple, got a " + str(type(t)))

    p = pack(t)
    return slice(
        p+b'\x00',
        p+b'\xff')

def _code_for(value):
    if value == None:
        return NULL_CODE
    elif isinstance(value, bytes):
        return BYTES_CODE
    elif isinstance(value, six.text_type):
        return STRING_CODE
    elif (not hasattr(fdb, '_version') or fdb._version >= 500) and isinstance(value, bool):
        return FALSE_CODE
    elif isinstance(value, six.integer_types):
        return INT_ZERO_CODE
    elif isinstance(value, ctypes.c_float) or isinstance(value, SingleFloat):
        return FLOAT_CODE
    elif isinstance(value, ctypes.c_double) or isinstance(value, float):
        return DOUBLE_CODE
    elif isinstance(value, uuid.UUID):
        return UUID_CODE
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
    return sign1*(-1 if bytes1 < bytes2 else 0 if bytes1 == bytes2 else 1)

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
        encoded1 = value1.encode('utf-8')
        encoded2 = value2.encode('utf-8')
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
        # Booleans, UUIDs, and integers can just use standard comparison.
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
