#
# test_util.py
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

import random
import unicodedata

import fdb

from bindingtester import util

class RandomGenerator(object):
    def __init__(self, max_int_bits=64):
        self.max_int_bits = max_int_bits

    def random_unicode_str(self, length):
        return u''.join(self.random_unicode_char() for i in range(0, length))

    def random_int(self):
        num_bits = random.randint(0, self.max_int_bits) # This way, we test small numbers with higher probability

        max_value = (1 << num_bits) - 1
        min_value = -max_value - 1
        num = random.randint(min_value, max_value)

        #util.get_logger().debug('generating int (%d): %d - %s' % (num_bits, num, repr(fdb.tuple.pack((num,)))))
        return num

    def random_tuple(self, max_size):
        size = random.randint(1, max_size)
        tup = []

        for i in range(size):
            choice = random.randint(0, 3)
            if choice == 0:
                tup.append(self.random_int())
            elif choice == 1:
                tup.append(None)
            elif choice == 2:
                tup.append(self.random_string(random.randint(0, 100)))
            elif choice == 3:
                tup.append(self.random_unicode_str(random.randint(0, 100)))
            else:
                assert false

        return tuple(tup)

    def random_range_params(self):
        if random.random() < 0.75:
            limit = random.randint(1, 1e3)
        elif random.random() < 0.75:
            limit = 0
        else:
            limit = random.randint(1e8, (1<<31)-1)

        return (limit, random.randint(0, 1), random.randint(-2, 4))

    def random_selector_params(self):
        if random.random() < 0.9:
            offset = random.randint(-20, 20)
        else:
            offset = random.randint(-1000, 1000)

        return (random.randint(0, 1), offset)

    def random_string(self, length):
        if length == 0:
            return ''

        return chr(random.randint(0, 254)) + ''.join(chr(random.randint(0, 255)) for i in range(0, length-1))

    def random_unicode_char(self):
        while True:
            c = random.randint(0, 0xffff)
            if unicodedata.category(unichr(c))[0] in 'LMNPSZ':
                return unichr(c)


def error_string(error_code):
    return fdb.tuple.pack(('ERROR', str(error_code)))

def blocking_commit(instructions):
    instructions.append('COMMIT')
    instructions.append('WAIT_FUTURE')
    instructions.append('RESET')

def to_front(instructions, index):
    if index == 0:
        pass
    elif index == 1:
        instructions.push_args(1)
        instructions.append('SWAP')
    elif index == 2:
        instructions.push_args(index-1)
        instructions.append('SWAP')
        instructions.push_args(index)
        instructions.append('SWAP')
    else:
        instructions.push_args(index-1)
        instructions.append('SWAP')
        instructions.push_args(index)
        instructions.append('SWAP')
        instructions.push_args(index-1)
        instructions.append('SWAP')
        to_front(instructions, index-1)

def with_length(tup):
    return (len(tup),) + tup

