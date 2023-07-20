#
# vector.py
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

"""FoundationDB Vector Layer.

Provides the Vector() class for storing and manipulating arrays
in FoundationDB.
"""

import fdb
import fdb.tuple

import threading

fdb.api_version(22)


###################################
# This defines a Subspace of keys #
###################################


class Subspace (object):
    def __init__(self, prefixTuple, rawPrefix=""):
        self.rawPrefix = rawPrefix + fdb.tuple.pack(prefixTuple)

    def __getitem__(self, name):
        return Subspace((name,), self.rawPrefix)

    def key(self):
        return self.rawPrefix

    def pack(self, tuple):
        return self.rawPrefix + fdb.tuple.pack(tuple)

    def unpack(self, key):
        assert key.startswith(self.rawPrefix)
        return fdb.tuple.unpack(key[len(self.rawPrefix):])

    def range(self, tuple=()):
        p = fdb.tuple.range(tuple)
        return slice(self.rawPrefix + p.start, self.rawPrefix + p.stop)


########################
# _ImplicitTransaction #
########################

# A local class which is used to allow vector operations to be performed without
# explicitly passing a transaction. It is created by vector.use_transaction
# and is used as follows:
#
# with vector.use_transaction(tr):
#     vector[0] = 1
#     vector.push(1)
#     ...


class _ImplicitTransaction:
    def __init__(self, vector, tr):
        self.vector = vector
        self.tr = tr
        self.initialValue = self.vector.local.tr

    def __enter__(self):
        if self.initialValue is not None and self.vector.local.tr != self.tr:
            raise Exception('use_transaction cannot be nested')

        self.vector.local.tr = self.tr

    def __exit__(self, type, value, traceback):
        self.vector.local.tr = self.initialValue


##########
# Vector #
##########

# Vector stores each of its values using its index as the key.
# The size of a vector is equal to the index of its last key + 1.
##
# For indexes smaller than the vector's size that have no associated key
# in the database, the value will be the specified defaultValue.
##
# If the last value in the vector has the default value, its key will
# always be set so that size can be determined.
##
# By creating Vector with a Subspace, all kv pairs modified by the
# layer will have keys that start within that Subspace.


class Vector:
    """Represents a potentially sparse array in FoundationDB."""

    # Public functions

    def __init__(self, subspace, defaultValue=''):
        self.subspace = subspace
        self.defaultValue = defaultValue
        self.local = threading.local()
        self.local.tr = None

    def use_transaction(self, tr):
        """
        Get an object that can be used in a with statement to perform operations
        on this vector without supplying a transaction as an argument to each operation.

        For example:

        with vector.use_transaction(tr):
            vector[0] = 1
            vector.push(1)
            ...
        """
        return _ImplicitTransaction(self, tr)

    def size(self, tr=None):
        """Get the number of items in the Vector. This number includes the sparsely represented items."""
        return self._size(self._to_transaction(tr))

    def push(self, val, tr=None):
        """Push a single item onto the end of the Vector."""
        self._push(val, self._to_transaction(tr))

    def back(self, tr=None):
        """Get the value of the last item in the Vector."""
        return self._back(self._to_transaction(tr))

    def front(self, tr=None):
        """Get the value of the first item in the Vector."""
        return self._get(0, self._to_transaction(tr))

    def pop(self, tr=None):
        """Get and pops the last item off the Vector."""
        return self._pop(self._to_transaction(tr))

    def swap(self, i1, i2, tr=None):
        """Swap the items at positions i1 and i2."""
        self._swap(i1, i2, self._to_transaction(tr))

    def get(self, index, tr=None):
        """Get the item at the specified index."""
        return self._get(index, self._to_transaction(tr))

    def get_range(self, startIndex=None, endIndex=None, step=None, tr=None):
        """Get a range of items in the Vector, returned as a generator."""
        return self._get_range(startIndex, endIndex, step, self._to_transaction(tr))

    def set(self, index, val, tr=None):
        """Set the value at a particular index in the Vector."""
        self._set(index, val, self._to_transaction(tr))

    def empty(self, tr=None):
        """Test whether the Vector is empty."""
        return self._size(self._to_transaction(tr)) == 0

    def resize(self, length, tr=None):
        """Grow or shrink the size of the Vector."""
        self._resize(length, self._to_transaction(tr))

    def clear(self, tr=None):
        """Remove all items from the Vector."""
        self._clear(self._to_transaction(tr))

    # Vector supports array notation when combined with use_transaction
    def __setitem__(self, index, val):
        """Set the item at the specified index. Can only be used with use_transaction."""
        self.set(index, val)

    def __getitem__(self, index):
        """
        Get the item(s) at the specified index or range. Ranges are returned as a generator. Can only
        be used with use_transaction()
        """
        if isinstance(index, slice):
            return self.get_range(index.start, index.stop, index.step)
        else:
            return self.get(index)

    # Private functions

    @fdb.transactional
    def _push(self, val, tr):
        tr[self._key_at(self.size())] = fdb.tuple.pack((val,))

    @fdb.transactional
    def _back(self, tr):
        keyRange = self.subspace.range()
        last = tr.get_range(keyRange.start, keyRange.stop, 1, True)
        for k, v in last:
            return fdb.tuple.unpack(v)[0]
        return None

    @fdb.transactional
    def _pop(self, tr):
        keyRange = self.subspace.range()

        # Read the last two entries so we can check if the second to last item
        # is being represented sparsely. If so, we will be required to set it
        # to the default value
        lastTwo = list(tr.get_range(keyRange.start, keyRange.stop, 2, True))
        indices = [self.subspace.unpack(kv.key)[0] for kv in lastTwo]

        # Vector was empty
        if len(lastTwo) == 0:
            return None

        # Vector has size one:
        elif indices[0] == 0:
            pass

        # Second to last item is being represented sparsely
        elif len(lastTwo) == 1 or indices[0] > indices[1] + 1:
            tr[self._key_at(indices[0] - 1)] = fdb.tuple.pack((self.defaultValue,))

        del tr[lastTwo[0].key]
        return fdb.tuple.unpack(lastTwo[0].value)[0]

    @fdb.transactional
    def _swap(self, i1, i2, tr):
        k1 = self._key_at(i1)
        k2 = self._key_at(i2)

        currentSize = self._size(tr)
        v1 = tr[k1]
        v2 = tr[k2]

        if i1 > currentSize or i2 > currentSize or i1 < 0 or i2 < 0:
            raise IndexError('vector.swap: indices (%d, %d) out of range' % (i1, i2))

        if v2.present():
            tr[k1] = v2
        elif v1.present() and i1 < currentSize - 1:
            del tr[k1]

        if v1.present():
            tr[k2] = v1
        elif v2.present() and i2 < currentSize - 1:
            del tr[k2]

    @fdb.transactional
    def _get(self, index, tr):
        if index < 0:
            raise IndexError('vector.get: index \'%d\' out of range' % index)

        start = self._key_at(index)
        end = self.subspace.range().stop

        output = tr.get_range(start, end, 1)
        for k, v in output:
            # The requested index had an associated key
            if(start == k):
                return fdb.tuple.unpack(v)[0]

            # The requested index is sparsely represented
            return self.defaultValue

        # We requested a value past the end of the vector
        raise IndexError('vector.get: index \'%d\' out of range' % index)

    def _get_range(self, startIndex, endIndex, step, tr):
        size = self._size(tr)
        if startIndex is not None and startIndex < 0:
            startIndex = max(0, size + startIndex)
        if endIndex is not None and endIndex < 0:
            endIndex = max(0, size + endIndex)

        if step is None:
            if startIndex is None or endIndex is None or startIndex <= endIndex:
                step = 1
            else:
                step = -1
        elif step == 0:
            raise ValueError('vector.get_range: step cannot be zero')

        if startIndex is None:
            if step > 0:
                start = self.subspace.range().start
            else:
                end = self.subspace.range().stop
        else:
            if step > 0:
                start = self._key_at(startIndex)
            else:
                end = self._key_at(startIndex + 1)

        if endIndex is None:
            if step > 0:
                end = self.subspace.range().stop
            else:
                start = self.subspace.range().start

        else:
            if step > 0:
                end = self._key_at(endIndex)
            else:
                start = self._key_at(endIndex + 1)

        result = tr.get_range(start, end, 0, step < 0)

        currentIndex = startIndex
        if currentIndex is None:
            if step > 0:
                currentIndex = 0
            else:
                currentIndex = size - 1
        elif currentIndex >= size:
            currentIndex = size - 1

        for k, v in result:
            keyIndex = self.subspace.unpack(k)[0]
            while (step > 0 and currentIndex < keyIndex) or (step < 0 and currentIndex > keyIndex):
                currentIndex = currentIndex + step
                yield self.defaultValue

            if currentIndex == keyIndex:
                currentIndex = currentIndex + step
                yield fdb.tuple.unpack(v)[0]

    @fdb.transactional
    def _size(self, tr):
        keyRange = self.subspace.range()
        lastKey = tr.get_key(fdb.KeySelector.last_less_or_equal(keyRange.stop))
        if lastKey < keyRange.start:
            return 0

        return self.subspace.unpack(lastKey)[0] + 1

    @fdb.transactional
    def _set(self, index, val, tr):
        tr[self._key_at(index)] = fdb.tuple.pack((val,))

    @fdb.transactional
    def _resize(self, length, tr):
        currentSize = self.size()
        if(length == currentSize):
            return
        if(length < currentSize):
            self._shrink(tr, length, currentSize)
        else:
            self._expand(tr, length, currentSize)

    @fdb.transactional
    def _shrink(self, tr, length, currentSize):
        tr.clear_range(self._key_at(length), self.subspace.range().stop)

        # Check if the new end of the vector was being sparsely represented
        if self._size(tr) < length:
            tr[self._key_at(length - 1)] = fdb.tuple.pack((self.defaultValue,))

    @fdb.transactional
    def _expand(self, tr, length, currentSize):
        tr[self._key_at(length - 1)] = fdb.tuple.pack((self.defaultValue,))

    @fdb.transactional
    def _clear(self, tr):
        del tr[self.subspace.range()]

    def _to_transaction(self, tr):
        if tr is None:
            if self.local.tr is None:
                raise Exception('No transaction specified and use_transaction has not been called')
            else:
                return self.local.tr
        else:
            return tr

    def _key_at(self, index):
        return self.subspace.pack((index,))


##################
# internal tests #
##################


# caution: modifies the database!
@fdb.transactional
def vector_test(tr):
    vector = Vector(Subspace(('test_vector',)), 0)

    with vector.use_transaction(tr):
        print 'Clearing any previous values in the vector'
        vector.clear()

        print '\nMODIFIERS'

        # Set + Push
        vector[0] = 1
        vector[1] = 2
        vector.push(3)
        _print_vector(vector, tr)

        # Swap
        vector.swap(0, 2)
        _print_vector(vector, tr)

        # Pop
        print 'Popped:', vector.pop()
        _print_vector(vector, tr)

        # Clear
        vector.clear()

        print 'Pop empty:', vector.pop()
        _print_vector(vector, tr)

        vector.push('Foo')
        print 'Pop size 1:', vector.pop()
        _print_vector(vector, tr)

        print '\nCAPACITY OPERATIONS'

        # Capacity
        print 'Size:', vector.size()
        print 'Empty:', vector.empty()

        print 'Resizing to length 5'
        vector.resize(5)
        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Setting values'
        vector[0] = 'The'
        vector[1] = 'Quick'
        vector[2] = 'Brown'
        vector[3] = 'Fox'
        vector[4] = 'Jumps'
        vector[5] = 'Over'
        _print_vector(vector, tr)

        print '\nFRONT'
        print vector.front()

        print '\nBACK'
        print vector.back()

        print '\nELEMENT ACCESS'
        print 'Index 0:', vector[0]
        print 'Index 5:', vector[5]

        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print '\nRESIZING'
        print 'Resizing to 3'
        vector.resize(3)
        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Resizing to 3 again'
        vector.resize(3)
        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Resizing to 6'
        vector.resize(6)
        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print '\nSPARSE TESTS'
        print 'Popping sparse vector'
        vector.pop()

        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Resizing to 4'
        vector.resize(4)

        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Adding "word" to index 10, resize to 25'
        vector[10] = 'word'
        vector.resize(25)

        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Popping sparse vector'
        vector.pop()

        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Swapping with sparse element'
        vector.swap(10, 15)

        _print_vector(vector, tr)
        print 'Size:', vector.size()

        print 'Swapping sparse elements'
        vector.swap(12, 13)

        _print_vector(vector, tr)
        print 'Size:', vector.size()


##############################
# Vector sample usage #
##############################


import sys


# caution: modifies the database!
@fdb.transactional
def vector_example(tr):
    vector = Vector(Subspace(('my_vector',)), 0)

    with vector.use_transaction(tr):
        vector.clear()

        for i in range(0, 5):
            vector.push(i)

        _print_vector(vector, tr)

        print 'Pop last item: %d' % vector.pop()
        _print_vector(vector, tr)

        vector[1] = 10
        vector.resize(11)
        _print_vector(vector, tr)

        vector.swap(1, 10)
        _print_vector(vector, tr)


def _print_vector(vector, tr):
    first = True
    with vector.use_transaction(tr):
        for v in vector:
            if not first:
                sys.stdout.write(',')

            first = False
            sys.stdout.write(repr(v))

    print


# caution: modifies the database!
if __name__ == '__main__':
    db = fdb.open()
    vector_example(db)
    # vector_test(db)
