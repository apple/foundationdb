/*
 * MetricSample.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef METRIC_SAMPLE_H
#define METRIC_SAMPLE_H
#pragma once

#include <tuple>

template <class T>
struct MetricSample {
	IndexedSet<T, int64_t> sample;
	int64_t metricUnitsPerSample = 0;

	explicit MetricSample(int64_t metricUnitsPerSample) : metricUnitsPerSample(metricUnitsPerSample) {}

	int64_t getMetric(const T& Key) const {
		auto i = sample.find(Key);
		if (i == sample.end())
			return 0;
		else
			return sample.getMetric(i);
	}
};

template <class T>
struct TransientMetricSample : MetricSample<T> {
	Deque<std::tuple<double, T, int64_t>> queue;

	explicit TransientMetricSample(int64_t metricUnitsPerSample) : MetricSample<T>(metricUnitsPerSample) {}

	// Returns the sampled metric value (possibly 0, possibly increased by the sampling factor)
	int64_t addAndExpire(const T& key, int64_t metric, double expiration) {
		int64_t x = add(key, metric);
		if (x)
			queue.emplace_back(expiration, *this->sample.find(key), -x);
		return x;
	}

	void poll() {
		double now = ::now();
		while (queue.size() && std::get<0>(queue.front()) <= now) {
			const T& key = std::get<1>(queue.front());
			int64_t delta = std::get<2>(queue.front());
			ASSERT(delta != 0);

			if (this->sample.addMetric(T(key), delta) == 0)
				this->sample.erase(key);

			queue.pop_front();
		}
	}

private:
	bool roll(int64_t metric) const {
		return nondeterministicRandom()->random01() <
		       (double)metric / this->metricUnitsPerSample; //< SOMEDAY: Better randomInt64?
	}

	int64_t add(const T& key, int64_t metric) {
		if (!metric)
			return 0;
		int64_t mag = std::abs(metric);

		if (mag < this->metricUnitsPerSample) {
			if (!roll(mag))
				return 0;

			metric = metric < 0 ? -this->metricUnitsPerSample : this->metricUnitsPerSample;
		}

		if (this->sample.addMetric(T(key), metric) == 0)
			this->sample.erase(key);

		return metric;
	}
};

template <class T>
struct TransientThresholdMetricSample : MetricSample<T> {
	Deque<std::tuple<double, T, int64_t>> queue;
	IndexedSet<T, int64_t> thresholdCrossedSet;
	int64_t thresholdLimit;

	TransientThresholdMetricSample(int64_t metricUnitsPerSample, int64_t threshold)
	  : MetricSample<T>(metricUnitsPerSample), thresholdLimit(threshold) {}

	template <class U>
	bool isAboveThreshold(const U& key) const {
		auto i = thresholdCrossedSet.find(key);
		if (i == thresholdCrossedSet.end())
			return false;
		else
			return true;
	}

	// Returns the sampled metric value (possibly 0, possibly increased by the sampling factor)
	template <class T_>
	int64_t addAndExpire(T_&& key, int64_t metric, double expiration) {
		int64_t x = add(std::forward<T_>(key), metric);
		if (x)
			queue.emplace_back(expiration, *this->sample.find(key), -x);
		return x;
	}

	void poll() {
		double now = ::now();
		while (queue.size() && std::get<0>(queue.front()) <= now) {
			const T& key = std::get<1>(queue.front());
			int64_t delta = std::get<2>(queue.front());
			ASSERT(delta != 0);

			int64_t val = this->sample.addMetric(T(key), delta);
			if (val < thresholdLimit && (val + std::abs(delta)) >= thresholdLimit) {
				auto iter = thresholdCrossedSet.find(key);
				ASSERT(iter != thresholdCrossedSet.end());
				thresholdCrossedSet.erase(iter);
			}
			if (val == 0)
				this->sample.erase(key);

			queue.pop_front();
		}
	}

private:
	bool roll(int64_t metric) const {
		return nondeterministicRandom()->random01() <
		       (double)metric / this->metricUnitsPerSample; //< SOMEDAY: Better randomInt64?
	}

	template <class T_>
	int64_t add(T_&& key, int64_t metric) {
		if (!metric)
			return 0;
		int64_t mag = std::abs(metric);

		if (mag < this->metricUnitsPerSample) {
			if (!roll(mag))
				return 0;

			metric = metric < 0 ? -this->metricUnitsPerSample : this->metricUnitsPerSample;
		}

		int64_t val = this->sample.addMetric(T(key), metric);
		if (val >= thresholdLimit) {
			ASSERT((val - metric) < thresholdLimit ? thresholdCrossedSet.find(key) == thresholdCrossedSet.end()
			                                       : thresholdCrossedSet.find(key) != thresholdCrossedSet.end());
			thresholdCrossedSet.insert(key, val);
		}

		if (val == 0)
			this->sample.erase(key);

		return metric;
	}
};
#endif
