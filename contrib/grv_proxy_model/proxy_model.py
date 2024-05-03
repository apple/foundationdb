#
# proxy_model.py
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

import copy
import functools
import heapq

from priority import Priority
from smoother import Smoother


@functools.total_ordering
class Task:
    def __init__(self, time, fxn):
        self.time = time
        self.fxn = fxn

    def __lt__(self, other):
        return self.time < other.time


class Limiter:
    class UpdateRateParams:
        def __init__(self, time):
            self.time = time

    class UpdateLimitParams:
        def __init__(self, time, elapsed):
            self.time = time
            self.elapsed = elapsed

    class CanStartParams:
        def __init__(self, time, num_started, count):
            self.time = time
            self.num_started = num_started
            self.count = count

    class UpdateBudgetParams:
        def __init__(
            self,
            time,
            num_started,
            num_started_at_priority,
            min_priority,
            last_batch,
            queue_empty,
            elapsed,
        ):
            self.time = time
            self.num_started = num_started
            self.num_started_at_priority = num_started_at_priority
            self.min_priority = min_priority
            self.last_batch = last_batch
            self.queue_empty = queue_empty
            self.elapsed = elapsed

    def __init__(self, priority, ratekeeper_model, proxy_model):
        self.priority = priority
        self.ratekeeper_model = ratekeeper_model
        self.proxy_model = proxy_model
        self.limit = 0
        self.rate = self.ratekeeper_model.get_limit(0, self.priority)

    def update_rate(self, params):
        pass

    def update_limit(self, params):
        pass

    def can_start(self, params):
        pass

    def update_budget(self, params):
        pass


class OriginalLimiter(Limiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        Limiter.__init__(self, priority, limit_rate_model, proxy_model)

    def update_rate(self, params):
        self.rate = self.ratekeeper_model.get_limit(params.time, self.priority)

    def update_limit(self, params):
        self.limit = min(0, self.limit) + params.elapsed * self.rate
        self.limit = min(self.limit, self.rate * 0.01)
        self.limit = min(self.limit, 100000)

        self.proxy_model.results.rate[self.priority][params.time] = self.rate
        self.proxy_model.results.limit[self.priority][params.time] = self.limit

    def can_start(self, params):
        return params.num_started < self.limit

    def update_budget(self, params):
        self.limit -= params.num_started


class PositiveBudgetLimiter(OriginalLimiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        OriginalLimiter.__init__(self, priority, limit_rate_model, proxy_model)

    def update_limit(self, params):
        self.limit += params.elapsed * self.rate
        self.limit = min(self.limit, 2.0 * self.rate)


class ClampedBudgetLimiter(PositiveBudgetLimiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        PositiveBudgetLimiter.__init__(self, priority, limit_rate_model, proxy_model)

    def update_budget(self, params):
        min_budget = -self.rate * 5.0
        if self.limit > min_budget:
            self.limit = max(self.limit - params.num_started, min_budget)


class TimeLimiter(PositiveBudgetLimiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        PositiveBudgetLimiter.__init__(self, priority, limit_rate_model, proxy_model)
        self.locked_until = 0

    def can_start(self, params):
        return params.time >= self.locked_until and PositiveBudgetLimiter.can_start(
            self, params
        )

    def update_budget(self, params):
        # print('Start update budget: time=%f, limit=%f, locked_until=%f, num_started=%d, priority=%s, min_priority=%s, last_batch=%d' % (params.time, self.limit, self.locked_until, params.num_started, self.priority, params.min_priority, params.last_batch))

        if params.min_priority >= self.priority or params.num_started < self.limit:
            self.limit -= params.num_started
        else:
            self.limit = min(
                self.limit, max(self.limit - params.num_started, -params.last_batch)
            )
            self.locked_until = min(
                params.time + 2.0,
                max(params.time, self.locked_until)
                + (params.num_started - self.limit) / self.rate,
            )

        # print('End update budget: time=%f, limit=%f, locked_until=%f, num_started=%d, priority=%s, min_priority=%s' % (params.time, self.limit, self.locked_until, params.num_started, self.priority, params.min_priority))


class TimePositiveBudgetLimiter(PositiveBudgetLimiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        PositiveBudgetLimiter.__init__(self, priority, limit_rate_model, proxy_model)
        self.locked_until = 0

    def update_limit(self, params):
        if params.time >= self.locked_until:
            PositiveBudgetLimiter.update_limit(self, params)

    def can_start(self, params):
        return params.num_started + params.count <= self.limit

    def update_budget(self, params):
        # if params.num_started > 0:
        # print('Start update budget: time=%f, limit=%f, locked_until=%f, num_started=%d, priority=%s, min_priority=%s, last_batch=%d' % (params.time, self.limit, self.locked_until, params.num_started, self.priority, params.min_priority, params.last_batch))

        if params.num_started > self.limit:
            self.locked_until = min(
                params.time + 2.0,
                max(params.time, self.locked_until)
                + (params.num_started - self.limit) / self.rate,
            )
            self.limit = 0
        else:
            self.limit -= params.num_started

        # if params.num_started > 0:
        # print('End update budget: time=%f, limit=%f, locked_until=%f, num_started=%d, priority=%s, min_priority=%s' % (params.time, self.limit, self.locked_until, params.num_started, self.priority, params.min_priority))


class SmoothingLimiter(OriginalLimiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        OriginalLimiter.__init__(self, priority, limit_rate_model, proxy_model)
        self.smooth_released = Smoother(2)
        self.smooth_rate_limit = Smoother(2)
        self.rate_set = False

    def update_rate(self, params):
        OriginalLimiter.update_rate(self, params)
        if not self.rate_set:
            self.rate_set = True
            self.smooth_rate_limit.reset(self.rate)
        else:
            self.smooth_rate_limit.set_total(params.time, self.rate)

    def update_limit(self, params):
        self.limit = 2.0 * (
            self.smooth_rate_limit.smooth_total(params.time)
            - self.smooth_released.smooth_rate(params.time)
        )

    def can_start(self, params):
        return params.num_started + params.count <= self.limit

    def update_budget(self, params):
        self.smooth_released.add_delta(params.time, params.num_started)


class SmoothingBudgetLimiter(SmoothingLimiter):
    def __init__(self, priority, limit_rate_model, proxy_model):
        SmoothingLimiter.__init__(self, priority, limit_rate_model, proxy_model)
        # self.smooth_filled = Smoother(2)
        self.budget = 0

    def update_limit(self, params):
        release_rate = self.smooth_rate_limit.smooth_total(
            params.time
        ) - self.smooth_released.smooth_rate(params.time)
        # self.smooth_filled.set_total(params.time, 1 if release_rate > 0 else 0)
        self.limit = 2.0 * release_rate

        self.proxy_model.results.rate[self.priority][
            params.time
        ] = self.smooth_rate_limit.smooth_total(params.time)
        self.proxy_model.results.released[self.priority][
            params.time
        ] = self.smooth_released.smooth_rate(params.time)
        self.proxy_model.results.limit[self.priority][params.time] = self.limit
        self.proxy_model.results.limit_and_budget[self.priority][params.time] = (
            self.limit + self.budget
        )
        self.proxy_model.results.budget[self.priority][params.time] = self.budget

        # self.budget = max(0, self.budget + params.elapsed * self.smooth_rate_limit.smooth_total(params.time))

        # if self.smooth_filled.smooth_total(params.time) >= 0.1:
        # self.budget += params.elapsed * self.smooth_rate_limit.smooth_total(params.time)

        # print('Update limit: time=%f, priority=%s, limit=%f, rate=%f, released=%f, budget=%f' % (params.time, self.priority, self.limit, self.smooth_rate_limit.smooth_total(params.time), self.smooth_released.smooth_rate(params.time), self.budget))

    def can_start(self, params):
        return (
            params.num_started + params.count <= self.limit + self.budget
        )  # or params.num_started + params.count <= self.budget

    def update_budget(self, params):
        self.budget = max(
            0,
            self.budget
            + (self.limit - params.num_started_at_priority) / 2 * params.elapsed,
        )

        if params.queue_empty:
            self.budget = min(10, self.budget)

        self.smooth_released.add_delta(params.time, params.num_started_at_priority)


class ProxyModel:
    class Results:
        def __init__(self, priorities, duration):
            self.started = self.init_result(priorities, 0, duration)
            self.queued = self.init_result(priorities, 0, duration)
            self.latencies = self.init_result(priorities, [], duration)
            self.unprocessed_queue_sizes = self.init_result(priorities, [], duration)

            self.rate = {p: {} for p in priorities}
            self.released = {p: {} for p in priorities}
            self.limit = {p: {} for p in priorities}
            self.limit_and_budget = {p: {} for p in priorities}
            self.budget = {p: {} for p in priorities}

        def init_result(self, priorities, starting_value, duration):
            return {
                p: {s: copy.copy(starting_value) for s in range(0, duration)}
                for p in priorities
            }

    def __init__(self, duration, ratekeeper_model, workload_model, Limiter):
        self.time = 0
        self.log_time = 0
        self.duration = duration
        self.priority_limiters = {
            priority: Limiter(priority, ratekeeper_model, self)
            for priority in workload_model.priorities()
        }
        self.workload_model = workload_model
        self.request_scheduled = {p: False for p in self.workload_model.priorities()}

        self.tasks = []
        self.request_queue = []
        self.results = ProxyModel.Results(self.workload_model.priorities(), duration)

    def run(self):
        self.update_rate()
        self.process_requests(self.time)

        for priority in self.workload_model.priorities():
            next_request = self.workload_model.next_request(self.time, priority)
            assert next_request is not None
            heapq.heappush(
                self.tasks,
                Task(
                    next_request.time,
                    lambda next_request=next_request: self.receive_request(
                        next_request
                    ),
                ),
            )
            self.request_scheduled[priority] = True

        while True:  # or len(self.request_queue) > 0:
            if int(self.time) > self.log_time:
                self.log_time = int(self.time)
                # print(self.log_time)

            task = heapq.heappop(self.tasks)
            self.time = task.time
            if self.time >= self.duration:
                break

            task.fxn()

    def update_rate(self):
        for limiter in self.priority_limiters.values():
            limiter.update_rate(Limiter.UpdateRateParams(self.time))

        heapq.heappush(self.tasks, Task(self.time + 0.01, lambda: self.update_rate()))

    def receive_request(self, request):
        heapq.heappush(self.request_queue, request)

        self.results.queued[request.priority][int(self.time)] += request.count

        next_request = self.workload_model.next_request(self.time, request.priority)
        if next_request is not None and next_request.time < self.duration:
            heapq.heappush(
                self.tasks,
                Task(next_request.time, lambda: self.receive_request(next_request)),
            )
        else:
            self.request_scheduled[request.priority] = False

    def process_requests(self, last_time):
        elapsed = self.time - last_time
        for limiter in self.priority_limiters.values():
            limiter.update_limit(Limiter.UpdateLimitParams(self.time, elapsed))

        current_started = 0
        started = {p: 0 for p in self.workload_model.priorities()}

        min_priority = Priority.SYSTEM
        last_batch = 0
        while len(self.request_queue) > 0:
            request = self.request_queue[0]

            if not self.priority_limiters[request.priority].can_start(
                Limiter.CanStartParams(self.time, current_started, request.count)
            ):
                break

            min_priority = request.priority
            last_batch = request.count

            if (
                self.workload_model.request_completed(request)
                and not self.request_scheduled[request.priority]
            ):
                next_request = self.workload_model.next_request(
                    self.time, request.priority
                )
                assert next_request is not None
                heapq.heappush(
                    self.tasks,
                    Task(
                        next_request.time,
                        lambda next_request=next_request: self.receive_request(
                            next_request
                        ),
                    ),
                )
                self.request_scheduled[request.priority] = True

            current_started += request.count
            started[request.priority] += request.count

            heapq.heappop(self.request_queue)
            self.results.started[request.priority][int(self.time)] += request.count
            self.results.latencies[request.priority][int(self.time)].append(
                self.time - request.time
            )

        if len(self.request_queue) == 0:
            min_priority = Priority.BATCH

        for priority, limiter in self.priority_limiters.items():
            started_at_priority = sum([v for p, v in started.items() if p <= priority])
            limiter.update_budget(
                Limiter.UpdateBudgetParams(
                    self.time,
                    current_started,
                    started_at_priority,
                    min_priority,
                    last_batch,
                    len(self.request_queue) == 0
                    or self.request_queue[0].priority > priority,
                    elapsed,
                )
            )

        for priority in self.workload_model.priorities():
            self.results.unprocessed_queue_sizes[priority][int(self.time)].append(
                self.workload_model.workload_models[priority].outstanding
            )

        current_time = self.time

        delay = 0.001
        heapq.heappush(
            self.tasks,
            Task(self.time + delay, lambda: self.process_requests(current_time)),
        )
