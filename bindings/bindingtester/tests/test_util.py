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
import uuid
import unicodedata
import ctypes
import math

import fdb
import fdb.tuple

from bindingtester import util
from bindingtester import FDB_API_VERSION
from bindingtester.known_testers import COMMON_TYPES


class RandomGenerator(object):
    def __init__(self, max_int_bits=64, api_version=FDB_API_VERSION, types=COMMON_TYPES):
        self.max_int_bits = max_int_bits
        self.api_version = api_version
        self.types = list(types)

    def random_unicode_str(self, length):
        return ''.join(self.random_unicode_char() for i in range(0, length))

    def random_int(self):
        num_bits = random.randint(0, self.max_int_bits)  # This way, we test small numbers with higher probability

        max_value = (1 << num_bits) - 1
        min_value = -max_value - 1
        num = random.randint(min_value, max_value)

        # util.get_logger().debug('generating int (%d): %d - %s' % (num_bits, num, repr(fdb.tuple.pack((num,)))))
        return num

    def random_float(self, exp_bits):
        if random.random() < 0.05:
            # Choose a special value.
            return random.choice([float('-nan'), float('-inf'), -0.0, 0.0, float('inf'), float('nan')])
        else:
            # Choose a value from all over the range of acceptable floats for this precision.
            sign = -1 if random.random() < 0.5 else 1
            exponent = random.randint(-(1 << (exp_bits - 1)) - 10, (1 << (exp_bits - 1) - 1))
            mantissa = random.random()

            result = sign * math.pow(2, exponent) * mantissa
            if random.random() < 0.05:
                result = float(int(result))

            return result

    def random_tuple(self, max_size, incomplete_versionstamps=False):
        size = random.randint(1, max_size)
        tup = []

        for i in range(size):
            choice = random.choice(self.types)
            if choice == 'int':
                tup.append(self.random_int())
            elif choice == 'null':
                tup.append(None)
            elif choice == 'bytes':
                tup.append(self.random_string(random.randint(0, 100)))
            elif choice == 'string':
                tup.append(self.random_unicode_str(random.randint(0, 100)))
            elif choice == 'uuid':
                tup.append(uuid.uuid4())
            elif choice == 'bool':
                b = random.random() < 0.5
                if self.api_version < 500:
                    tup.append(int(b))
                else:
                    tup.append(b)
            elif choice == 'float':
                tup.append(fdb.tuple.SingleFloat(self.random_float(8)))
            elif choice == 'double':
                tup.append(self.random_float(11))
            elif choice == 'tuple':
                length = random.randint(0, max_size - size)
                if length == 0:
                    tup.append(())
                else:
                    tup.append(self.random_tuple(length))
            elif choice == 'versionstamp':
                if incomplete_versionstamps and random.random() < 0.5:
                    tr_version = fdb.tuple.Versionstamp._UNSET_TR_VERSION
                else:
                    tr_version = self.random_string(10)
                user_version = random.randint(0, 0xffff)
                tup.append(fdb.tuple.Versionstamp(tr_version, user_version))
            else:
                assert False

        return tuple(tup)

    def random_tuple_list(self, max_size, max_list_size):
        size = random.randint(1, max_list_size)
        tuples = []

        for i in range(size):
            to_add = self.random_tuple(max_size)
            tuples.append(to_add)
            if len(to_add) > 1 and random.random() < 0.25:
                # Add a smaller one to test prefixes.
                smaller_size = random.randint(1, len(to_add))
                tuples.append(to_add[:smaller_size])
            else:
                non_empty = [x for x in enumerate(to_add) if (isinstance(x[1], list) or isinstance(x[1], tuple)) and len(x[1]) > 0]
                if len(non_empty) > 0 and random.random() < 0.25:
                    # Add a smaller list to test prefixes of nested structures.
                    idx, choice = random.choice(non_empty)
                    smaller_size = random.randint(0, len(to_add[idx]))
                    tuples.append(to_add[:idx] + (choice[:smaller_size],) + to_add[idx + 1:])

        random.shuffle(tuples)
        return tuples

    def random_range_params(self):
        if random.random() < 0.75:
            limit = random.randint(1, 1e3)
        elif random.random() < 0.75:
            limit = 0
        else:
            limit = random.randint(1e8, (1 << 31) - 1)

        return (limit, random.randint(0, 1), random.randint(-2, 4))

    def random_selector_params(self):
        if random.random() < 0.9:
            offset = random.randint(-20, 20)
        else:
            offset = random.randint(-1000, 1000)

        return (random.randint(0, 1), offset)

    def random_string(self, length):
        if length == 0:
            return b''

        return bytes([random.randint(0, 254)] + [random.randint(0, 255) for i in range(0, length - 1)])

    def random_unicode_char(self):
        while True:
            if random.random() < 0.05:
                # Choose one of these special character sequences.
                specials = ['\U0001f4a9', '\U0001f63c', '\U0001f3f3\ufe0f\u200d\U0001f308', '\U0001f1f5\U0001f1f2', '\uf8ff',
                            '\U0002a2b2', '\u05e9\u05dc\u05d5\u05dd']
                return random.choice(specials)
            c = random.randint(0, 0xffff)
            if unicodedata.category(chr(c))[0] in 'LMNPSZ':
                return chr(c)


def error_string(error_code):
    return fdb.tuple.pack((b'ERROR', bytes(str(error_code), 'utf-8')))


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
        instructions.push_args(index - 1)
        instructions.append('SWAP')
        instructions.push_args(index)
        instructions.append('SWAP')
    else:
        instructions.push_args(index - 1)
        instructions.append('SWAP')
        instructions.push_args(index)
        instructions.append('SWAP')
        instructions.push_args(index - 1)
        instructions.append('SWAP')
        to_front(instructions, index - 1)


def with_length(tup):
    return (len(tup),) + tup
