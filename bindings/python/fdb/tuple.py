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

import struct, math
from bisect import bisect_left

from fdb import six

_size_limits = tuple( (1 << (i*8))-1 for i in range(9) )

def _find_terminator( v, pos ):
    # Finds the start of the next terminator [\x00]![\xff] or the end of v
    while True:
        pos = v.find(b'\x00', pos)
        if pos < 0:
            return len(v)
        if pos+1 == len(v) or v[pos+1:pos+2] != b'\xff':
            return pos
        pos += 2

def _decode(v, pos):
    code = six.indexbytes(v, pos)
    if code == 0:
        return None, pos+1
    elif code == 1:
        end = _find_terminator(v, pos+1)
        return v[pos+1:end].replace(b"\x00\xFF", b"\x00"), end+1
    elif code == 2:
        end = _find_terminator(v, pos+1)
        return v[pos+1:end].replace(b"\x00\xFF", b"\x00").decode("utf-8"), end+1
    elif code >= 20 and code <= 28:
        n = code - 20
        end = pos + 1 + n
        return struct.unpack(">Q", b'\x00'*(8-n) + v[pos+1:end])[0], end
    elif code >= 12 and code < 20:
        n = 20 - code
        end = pos + 1 + n
        return struct.unpack(">Q", b'\x00'*(8-n) + v[pos+1:end])[0]-_size_limits[n], end
    elif code == 29: # 0x1d; Positive 9-255 byte integer
        length = six.indexbytes(v, pos+1)
        val = 0
        for i in _range(length):
            val = val << 8
            val += six.indexbytes(v, pos+2+i)
        return val, pos+2+length
    elif code == 11: # 0x0b; Negative 9-255 byte integer
        length = six.indexbytes(v, pos+1)^0xff
        val = 0
        for i in _range(length):
            val = val << 8
            val += six.indexbytes(v, pos+2+i)
        return val - (1<<(length*8)) + 1, pos+2+length
    else:
        raise ValueError("Unknown data type in DB: " + repr(v))

def _encode(value):
    # returns [code][data] (code != 0xFF)
    # encoded values are self-terminating
    # sorting need to work too!
    if value == None:  # ==, not is, because some fdb.impl.Value are equal to None
        return b'\x00'
    elif isinstance(value, bytes): # also gets non-None fdb.impl.Value
        return b'\x01' + value.replace(b'\x00', b'\x00\xFF') + b'\x00'
    elif isinstance(value, six.text_type):
        return b'\x02' + value.encode('utf-8').replace(b'\x00', b'\x00\xFF') + b'\x00'
    elif isinstance(value, six.integer_types):
        if value == 0:
            return b'\x14'
        elif value > 0:
            if value >= _size_limits[-1]:
                length = (value.bit_length()+7)//8
                data = [b'\x1d', six.int2byte(length)]
                for i in _range(length-1,-1,-1):
                    data.append(six.int2byte( (value>>(8*i))&0xff ))
                return b''.join(data)

            n = bisect_left( _size_limits, value )
            return six.int2byte(20 + n) + struct.pack( ">Q", value )[-n:]
        else:
            if -value >= _size_limits[-1]:
                length = (value.bit_length()+7)//8
                value += (1<<(length*8)) - 1
                data = [b'\x0b', six.int2byte(length^0xff)]
                for i in _range(length-1,-1,-1):
                    data.append(six.int2byte( (value>>(8*i))&0xff ))
                return b''.join(data)

            n = bisect_left( _size_limits, -value )
            maxv = _size_limits[n]
            return six.int2byte(20 - n) + struct.pack( ">Q", maxv+value)[-n:]
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
