#
# priority.py
#
# This source file is part of the FoundationDB open source project
#
# Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

import functools

@functools.total_ordering
class Priority:
    def __init__(self, priority_value, label):
        self.priority_value = priority_value
        self.label = label

    def __lt__(self, other):
        return self.priority_value < other.priority_value

    def __str__(self):
        return self.label

    def __repr__(self):
        return repr(self.label)

Priority.SYSTEM = Priority(0, "System")
Priority.DEFAULT = Priority(1, "Default")
Priority.BATCH = Priority(2, "Batch")
