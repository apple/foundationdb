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


import sys, random

_range = range

from fdb.tuple import pack, unpack, range
from fdb import six

from fdb.six import u

def randomElement():
    r = random.randint(0,4)
    if r == 0:
        chars = [b'\x00', b'\x01', b'a', b'7', b'\xfe', b'\ff']
        return b''.join([random.choice(chars) for c in _range(random.randint(0, 5))])
    elif r == 1:
        chars = [u('\x00'), u('\x01'), u('a'), u('7'), u('\xfe'), u('\ff'), u('\u0000'), u('\u0001'), u('\uffff'), u('\uff00')]
        return u('').join([random.choice(chars) for c in _range(random.randint(0, 10))])
    elif r == 2:
        return random.choice([-1, 1]) * min(2**random.randint(0, 2040) + random.randint(-10, 10), 2**2040 - 1)
    elif r == 3:
        return random.choice([-1, 1]) * 2**random.randint(0, 64) + random.randint(-10, 10)
    elif r == 4:
        return None

def randomTuple():
    return tuple( randomElement() for x in _range(random.randint(0,4)) )

def isprefix(a,b):
    return tupleorder(a) == tupleorder(b[:len(a)])

def torder(x):
    if x == None:
        return 0
    elif type(x) == type(b''):
        return 1
    elif isinstance(x, six.text_type):
        return 2
    elif isinstance(x, six.integer_types):
        return 3
    raise Exception("Unknown type")
def tupleorder(t):
    return tuple( (torder(e),e) for e in t )

def tupleTest(N=10000):
    someTuples = [ randomTuple() for i in _range(N) ]
    a = sorted(someTuples, key=tupleorder)
    b = sorted(someTuples, key=pack)
    assert a == b

    print("Sort %d OK" % N)

    for i in _range(N):
        t = randomTuple()
        t2 = t + (randomElement(),)
        t3 = randomTuple()
        try:
            assert(unpack(pack(t)) == t)

            r = range(t)
            assert not (r.start <= pack(t) < r.stop)
            assert (r.start <= pack(t2) < r.stop)

            if not isprefix(t, t3):
                assert not (r.start <= pack(t3) <= r.stop)

            assert (tupleorder(t) < tupleorder(t3)) == (pack(t) < pack(t3))
        except:
            print (repr(t), repr(t2), repr(t3))
            raise

    print ("Tuple check %d OK" % N)

# test:
# a = ('\x00a', -2, 'b\x01', 12345, '')
# assert(a==fdbtuple.unpack(fdbtuple.pack(a)))

if __name__=='__main__':
    tupleTest()
