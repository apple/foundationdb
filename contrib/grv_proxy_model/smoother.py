#
# smoother.py
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

import math


class Smoother:
    def __init__(self, folding_time):
        self.folding_time = folding_time
        self.reset(0)

    def reset(self, value):
        self.time = 0
        self.total = value
        self.estimate = value

    def set_total(self, time, total):
        self.add_delta(time, total - self.total)

    def add_delta(self, time, delta):
        self.update(time)
        self.total += delta

    def smooth_total(self, time):
        self.update(time)
        return self.estimate

    def smooth_rate(self, time):
        self.update(time)
        return (self.total - self.estimate) / self.folding_time

    def update(self, time):
        elapsed = time - self.time
        if elapsed > 0:
            self.time = time
            self.estimate += (self.total - self.estimate) * (1 - math.exp(-elapsed / self.folding_time))
