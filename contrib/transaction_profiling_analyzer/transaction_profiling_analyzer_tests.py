#!/usr/bin/env python3
#
# transaction_profiling_analyzer_tests.py
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

from transaction_profiling_analyzer import RangeCounter
from sortedcontainers import SortedDict

import random
import string
import unittest


class RangeCounterTest(unittest.TestCase):
    def test_one_range(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "b")
        assert rc.ranges == SortedDict({"a": ("b", 1)}), rc.ranges

    def test_two_non_overlapping_desc(self):
        rc = RangeCounter(1)
        rc._insert_range("c", "d")
        rc._insert_range("a", "b")
        assert rc.ranges == SortedDict({"a": ("b", 1), "c": ("d", 1)}), rc.ranges

    def test_two_non_overlapping_asc(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "b")
        rc._insert_range("c", "d")
        assert rc.ranges == SortedDict({"a": ("b", 1), "c": ("d", 1)}), rc.ranges

    def test_two_touching(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "b")
        rc._insert_range("b", "c")
        assert rc.ranges == SortedDict({"a": ("b", 1), "b": ("c", 1)}), rc.ranges
        assert rc.get_count_for_key("a") == 1
        assert rc.get_count_for_key("b") == 1
        assert rc.get_count_for_key("c") == 0

    def test_two_duplicates(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "b")
        rc._insert_range("a", "b")
        assert rc.ranges == SortedDict({"a": ("b", 2)}), rc.ranges

    def test_wholly_outside(self):
        rc = RangeCounter(1)
        rc._insert_range("b", "c")
        rc._insert_range("a", "d")
        assert rc.ranges == SortedDict(
            {"a": ("b", 1), "b": ("c", 2), "c": ("d", 1)}
        ), rc.ranges

    def test_wholly_inside(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "d")
        rc._insert_range("b", "c")
        assert rc.ranges == SortedDict(
            {"a": ("b", 1), "b": ("c", 2), "c": ("d", 1)}
        ), rc.ranges

    def test_intersect_before(self):
        rc = RangeCounter(1)
        rc._insert_range("b", "d")
        rc._insert_range("a", "c")
        assert rc.ranges == SortedDict(
            {"a": ("b", 1), "b": ("c", 2), "c": ("d", 1)}
        ), rc.ranges

    def test_intersect_after(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "c")
        rc._insert_range("b", "d")
        assert rc.ranges == SortedDict(
            {"a": ("b", 1), "b": ("c", 2), "c": ("d", 1)}
        ), rc.ranges

    def test_wide(self):
        rc = RangeCounter(1)
        rc._insert_range("a", "c")
        rc._insert_range("e", "g")
        rc._insert_range("i", "k")
        rc._insert_range("b", "j")
        assert rc.ranges == SortedDict(
            {
                "a": ("b", 1),
                "b": ("c", 2),
                "c": ("e", 1),
                "e": ("g", 2),
                "g": ("i", 1),
                "i": ("j", 2),
                "j": ("k", 1),
            }
        ), rc.ranges

    def test_random(self):
        letters = string.ascii_lowercase

        for _ in range(0, 100):
            rc = RangeCounter(1)
            count_dict = {}

            def test_correct():
                for (k, v) in count_dict.items():
                    rc_count = rc.get_count_for_key(k)
                assert rc_count == v, "Counts for %s mismatch. Expected %d got %d" % (
                    k,
                    v,
                    rc_count,
                )

            for _ in range(0, 100):
                i = random.randint(0, len(letters) - 1)
                j = random.randint(0, len(letters) - 2)
                if i == j:
                    j += 1
                start_index = min(i, j)
                end_index = max(i, j)
                start_key = letters[start_index]
                end_key = letters[end_index]
                rc._insert_range(start_key, end_key)
                for letter in letters[start_index:end_index]:
                    if letter not in count_dict:
                        count_dict[letter] = 0
                    count_dict[letter] = count_dict[letter] + 1

                test_correct()


if __name__ == "__main__":
    unittest.main()  # run all tests
