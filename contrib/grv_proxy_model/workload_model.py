#
# workload_model.py
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
import numpy
import math

import rate_model
from priority import Priority


@functools.total_ordering
class Request:
    def __init__(self, time, count, priority):
        self.time = time
        self.count = count
        self.priority = priority

    def __lt__(self, other):
        return self.priority < other.priority


class PriorityWorkloadModel:
    def __init__(
        self, priority, rate_model, batch_model, generator, max_outstanding=1e9
    ):
        self.priority = priority
        self.rate_model = rate_model
        self.batch_model = batch_model
        self.generator = generator
        self.max_outstanding = max_outstanding
        self.outstanding = 0

    def next_request(self, time):
        if self.outstanding >= self.max_outstanding:
            return None

        batch_size = self.batch_model.next_batch()
        self.outstanding += batch_size
        interval = self.generator.next_request_interval(self.rate_model.get_rate(time))
        return Request(time + interval, batch_size, self.priority)

    def request_completed(self, request):
        was_full = self.max_outstanding <= self.outstanding
        self.outstanding -= request.count

        return was_full and self.outstanding < self.max_outstanding


class WorkloadModel:
    def __init__(self, workload_models):
        self.workload_models = workload_models

    def priorities(self):
        return list(self.workload_models.keys())

    def next_request(self, time, priority):
        return self.workload_models[priority].next_request(time)

    def request_completed(self, request):
        return self.workload_models[request.priority].request_completed(request)


class Distribution:
    def exponential(x):
        return numpy.random.exponential(x)

    def uniform(x):
        return numpy.random.uniform(0, 2.0 * x)

    def fixed(x):
        return x


class BatchGenerator:
    def __init__(self):
        pass

    def next_batch(self):
        pass


class DistributionBatchGenerator(BatchGenerator):
    def __init__(self, distribution, size):
        BatchGenerator.__init__(self)
        self.distribution = distribution
        self.size = size

    def next_batch(self):
        return math.ceil(self.distribution(self.size))


class RequestGenerator:
    def __init__(self):
        pass

    def next_request_interval(self, rate):
        pass


class DistributionRequestGenerator(RequestGenerator):
    def __init__(self, distribution):
        RequestGenerator.__init__(self)
        self.distribution = distribution

    def next_request_interval(self, rate):
        if rate == 0:
            return 1e9

        return self.distribution(1.0 / rate)


predefined_workloads = {}

predefined_workloads["slow_exponential"] = WorkloadModel(
    {
        Priority.DEFAULT: PriorityWorkloadModel(
            Priority.DEFAULT,
            rate_model.FixedRateModel(100),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.exponential),
            max_outstanding=100,
        )
    }
)

predefined_workloads["fixed_uniform"] = WorkloadModel(
    {
        Priority.SYSTEM: PriorityWorkloadModel(
            Priority.SYSTEM,
            rate_model.FixedRateModel(0),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=10,
        ),
        Priority.DEFAULT: PriorityWorkloadModel(
            Priority.DEFAULT,
            rate_model.FixedRateModel(95),
            DistributionBatchGenerator(Distribution.fixed, 10),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=200,
        ),
        Priority.BATCH: PriorityWorkloadModel(
            Priority.BATCH,
            rate_model.FixedRateModel(1),
            DistributionBatchGenerator(Distribution.uniform, 500),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=200,
        ),
    }
)

predefined_workloads["batch_starvation"] = WorkloadModel(
    {
        Priority.SYSTEM: PriorityWorkloadModel(
            Priority.SYSTEM,
            rate_model.FixedRateModel(1),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=10,
        ),
        Priority.DEFAULT: PriorityWorkloadModel(
            Priority.DEFAULT,
            rate_model.IntervalRateModel([(0, 50), (60, 150), (120, 90)]),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=200,
        ),
        Priority.BATCH: PriorityWorkloadModel(
            Priority.BATCH,
            rate_model.FixedRateModel(100),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=200,
        ),
    }
)

predefined_workloads["default_low_high_low"] = WorkloadModel(
    {
        Priority.SYSTEM: PriorityWorkloadModel(
            Priority.SYSTEM,
            rate_model.FixedRateModel(0),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=10,
        ),
        Priority.DEFAULT: PriorityWorkloadModel(
            Priority.DEFAULT,
            rate_model.IntervalRateModel([(0, 100), (60, 300), (120, 100)]),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=200,
        ),
        Priority.BATCH: PriorityWorkloadModel(
            Priority.BATCH,
            rate_model.FixedRateModel(0),
            DistributionBatchGenerator(Distribution.fixed, 1),
            DistributionRequestGenerator(Distribution.uniform),
            max_outstanding=200,
        ),
    }
)

for rate in [83, 100, 180, 190, 200]:
    predefined_workloads["default%d" % rate] = WorkloadModel(
        {
            Priority.DEFAULT: PriorityWorkloadModel(
                Priority.DEFAULT,
                rate_model.FixedRateModel(rate),
                DistributionBatchGenerator(Distribution.fixed, 1),
                DistributionRequestGenerator(Distribution.exponential),
                max_outstanding=1000,
            )
        }
    )
