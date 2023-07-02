#
# rate_model.py
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

import numpy


class RateModel:
    def __init__(self):
        pass

    def get_rate(self, time):
        pass


class FixedRateModel(RateModel):
    def __init__(self, rate):
        RateModel.__init__(self)
        self.rate = rate

    def get_rate(self, time):
        return self.rate


class UnlimitedRateModel(FixedRateModel):
    def __init__(self):
        self.rate = 1e9


class IntervalRateModel(RateModel):
    def __init__(self, intervals):
        self.intervals = sorted(intervals)

    def get_rate(self, time):
        if len(self.intervals) == 0 or time < self.intervals[0][0]:
            return 0

        target_interval = len(self.intervals) - 1
        for i in range(1, len(self.intervals)):
            if time < self.intervals[i][0]:
                target_interval = i - 1
                break

        self.intervals = self.intervals[target_interval:]
        return self.intervals[0][1]


class SawtoothRateModel(RateModel):
    def __init__(self, low, high, frequency):
        self.low = low
        self.high = high
        self.frequency = frequency

    def get_rate(self, time):
        if int(2 * time / self.frequency) % 2 == 0:
            return self.low
        else:
            return self.high


class DistributionRateModel(RateModel):
    def __init__(self, distribution, frequency):
        self.distribution = distribution
        self.frequency = frequency
        self.last_change = 0
        self.rate = None

    def get_rate(self, time):
        if (
            self.frequency == 0
            or int((time - self.last_change) / self.frequency)
            > int(self.last_change / self.frequency)
            or self.rate is None
        ):
            self.last_change = time
            self.rate = self.distribution()

        return self.rate
