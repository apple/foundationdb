#!/usr/bin/python
#
# tuple_tests.py
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


import ctypes
import sys
import random
import struct
import unicodedata
import math
import uuid

_range = range

from fdb.tuple import pack, unpack, range, compare, SingleFloat
from fdb import six

from fdb.six import u


def randomUnicode():
    while True:
        c = random.randint(0, 0xffff)
        if unicodedata.category(unichr(c))[0] in 'LMNPSZ':
            return unichr(c)


def randomElement():
    r = random.randint(0, 9)
    if r == 0:
        if random.random() < 0.5:
            chars = [b'\x00', b'\x01', b'a', b'7', b'\xfe', b'\ff']
            return b''.join([random.choice(chars) for c in _range(random.randint(0, 5))])
        else:
            return b''.join([six.int2byte(random.randint(0, 255)) for _ in _range(random.randint(0, 10))])
    elif r == 1:
        if random.random() < 0.5:
            chars = [u('\x00'), u('\x01'), u('a'), u('7'), u('\xfe'), u('\ff'), u('\u0000'), u('\u0001'), u('\uffff'), u('\uff00'), u('\U0001f4a9')]
            return u('').join([random.choice(chars) for c in _range(random.randint(0, 10))])
        else:
            return u('').join([randomUnicode() for _ in _range(random.randint(0, 10))])
    elif r == 2:
        return random.choice([-1, 1]) * min(2**random.randint(0, 2040) + random.randint(-10, 10), 2**2040 - 1)
    elif r == 3:
        return random.choice([-1, 1]) * 2**random.randint(0, 64) + random.randint(-10, 10)
    elif r == 4:
        return None
    elif r == 5:
        ret = random.choice([float('-nan'), float('-inf'), -0.0, 0.0, float('inf'), float('nan')])
        if random.random() < 0.5:
            return SingleFloat(ret)
        else:
            return ret
    elif r == 6:
        is_double = random.random() < 0.5
        byte_str = b''.join([six.int2byte(random.randint(0, 255)) for _ in _range(8 if is_double else 4)])
        if is_double:
            return struct.unpack(">d", byte_str)[0]
        else:
            return SingleFloat(struct.unpack(">f", byte_str)[0])
    elif r == 7:
        return random.random() < 0.5
    elif r == 8:
        return uuid.uuid4()
    elif r == 9:
        return [randomElement() for _ in _range(random.randint(0, 5))]


def randomTuple():
    return tuple(randomElement() for x in _range(random.randint(0, 4)))


def isprefix(a, b):
    return compare(a, b[:len(a)]) == 0


def find_bad_sort(a, b):
    for x1 in a:
        for x2 in b:
            if compare(x1, x2) < 0 and pack(x1) >= pack(x2):
                return (x1, x2)
    return None


def equalEnough(t1, t2):
    if len(t1) != len(t2):
        return False

    for i in _range(len(t1)):
        e1 = t1[i]
        e2 = t2[i]

        if isinstance(e1, SingleFloat):
            if not isinstance(e2, SingleFloat):
                return False
            return ctypes.c_float(e1.value).value == ctypes.c_float(e2.value).value
        elif isinstance(e1, list) or isinstance(e2, tuple):
            if not (isinstance(e2, list) or isinstance(e2, tuple)):
                return False
            if not equalEnough(e1, e2):
                return False
        else:
            if e1 != e2:
                return False

    return True


def tupleTest(N=10000):
    someTuples = [randomTuple() for i in _range(N)]
    a = sorted(someTuples, cmp=compare)
    b = sorted(someTuples, key=pack)

    if not a == b:
        problem = find_bad_sort(a, b)
        if problem:
            print("Bad sort:\n    %s\n    %s" % (problem[0], problem[1]))
            print("Bytes:\n    %s\n    %s" % (repr(pack(problem[0])), repr(pack(problem[1]))))
            # print("Tuple order:\n    %s\n    %s" % (tupleorder(problem[0]), tupleorder(problem[1])))
            return False
        else:
            print("Sorts unequal but every pair correct")
            return False

    print("Sort %d OK" % N)

    for i in _range(N):
        t = randomTuple()
        t2 = t + (randomElement(),)
        t3 = randomTuple()

        if not compare(unpack(pack(t)), t) == 0:
            print("unpack . pack /= identity:\n    Orig:  %s\n    Bytes: %s\n    New:   %s" % (t, repr(pack(t)), unpack(pack(t))))
            return False

        r = range(t)
        if r.start <= pack(t) < r.stop:
            print("element within own range:\n    Tuple: %s\n    Bytes: %s\n    Start: %s\n    Stop:  %s" %
                  (t, repr(pack(t)), repr(r.start), repr(r.stop)))
        if not r.start <= pack(t2) < r.stop:
            print("prefixed element not in range:\n    Tuple:    %s\n    Bytes:    %s\n    Prefixed: %s\n    Bytes:   %s" %
                  (t, repr(pack(t)), t2, repr(pack(t2))))
            return False

        if not isprefix(t, t3):
            if r.start <= pack(t3) <= r.stop:
                print("non-prefixed element in range:\n    Tuple: %s\n    Bytes: %s\n    Other: %s\n    Bytes: %s"
                      % (t, repr(pack(t)), t3, repr(pack(t3))))
                return False

        if (compare(t, t3) < 0) != (pack(t) < pack(t3)):
            print("Bad comparison:\n    Tuple: %s\n    Bytes: %s\n    Other: %s\n    Bytes: %s" % (t, repr(pack(t)), t3, repr(pack(t3))))
            return False
        if not pack(t) < pack(t2):
            print("Prefix not before prefixed:\n    Tuple: %s\n    Bytes: %s\n    Other: %s\n    Bytes: %s" % (t, repr(pack(t)), t2, repr(pack(t2))))
            return False

    print ("Tuple check %d OK" % N)
    return True

# test:
# a = ('\x00a', -2, 'b\x01', 12345, '')
# assert(a==fdbtuple.unpack(fdbtuple.pack(a)))


if __name__ == '__main__':
    assert tupleTest(10000)
