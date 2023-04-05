/*
 * Stats.h
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

#ifndef FDBRPC_STATS_H
#define FDBRPC_STATS_H
#include <type_traits>
#pragma once

// Yet another performance statistics interface
/*

struct MyCounters {
CounterCollection cc;
Counter foo, bar, baz;
MyCounters() : foo("foo", cc), bar("bar", cc), baz("baz", cc) {}
};

*/

#include <cstdint>
#include <cstddef>
#include "flow/flow.h"
#include "flow/TDMetric.actor.h"
#include "fdbrpc/ContinuousSample.h"

struct ICounter {
	// All counters have a name and value
	virtual std::string const& getName() const = 0;
	virtual int64_t getValue() const = 0;

	// Counters may also have rate and roughness
	virtual bool hasRate() const = 0;
	virtual double getRate() const = 0;
	virtual bool hasRoughness() const = 0;
	virtual double getRoughness() const = 0;

	virtual void resetInterval() = 0;

	virtual void remove() {}
};

template <>
struct Traceable<ICounter*> : std::true_type {
	static std::string toString(ICounter const* counter) {
		if (counter->hasRate() && counter->hasRoughness()) {
			return format("%g %g %lld", counter->getRate(), counter->getRoughness(), (long long)counter->getValue());
		} else {
			return format("%lld", (long long)counter->getValue());
		}
	}
};

struct CounterCollection {
	CounterCollection(std::string name, std::string id = std::string()) : name(name), id(id) {}
	std::vector<struct ICounter*> counters, counters_to_remove;
	~CounterCollection() {
		for (auto c : counters_to_remove)
			c->remove();
	}
	std::string name;
	std::string id;

	void logToTraceEvent(TraceEvent& te) const;
};

struct Counter final : ICounter, NonCopyable {
public:
	typedef int64_t Value;

	Counter(std::string const& name, CounterCollection& collection);

	void operator+=(Value delta);
	void operator++() { *this += 1; }
	void clear();
	void resetInterval() override;

	std::string const& getName() const override { return name; }

	Value getIntervalDelta() const { return interval_delta; }
	Value getValue() const override { return interval_start_value + interval_delta; }

	// dValue / dt
	double getRate() const override;

	// Measures the clumpiness or dispersion of the counter.
	// Computed as a normalized variance of the time between each incrementation of the value.
	// A delta of N is treated as N distinct increments, with N-1 increments having time span 0.
	// Normalization is performed by dividing each time sample by the mean time before taking variance.
	//
	// roughness = Variance(t/mean(T)) for time interval samples t in T
	//
	// A uniformly periodic counter will have roughness of 0
	// A uniformly periodic counter that increases in clumps of N will have roughness of N-1
	// A counter with exponentially distributed incrementations will have roughness of 1
	double getRoughness() const override;

	bool hasRate() const override { return true; }
	bool hasRoughness() const override { return true; }

private:
	std::string name;
	double interval_start, last_event, interval_sq_time, roughness_interval_start;
	Value interval_delta, interval_start_value;
	Int64MetricHandle metric;
};

template <>
struct Traceable<Counter> : std::true_type {
	static std::string toString(Counter const& counter) {
		return Traceable<ICounter*>::toString((ICounter const*)&counter);
	}
};

template <class F>
struct SpecialCounter final : ICounter, FastAllocated<SpecialCounter<F>>, NonCopyable {
	SpecialCounter(CounterCollection& collection, std::string const& name, F&& f) : name(name), f(f) {
		collection.counters.push_back(this);
		collection.counters_to_remove.push_back(this);
	}
	void remove() override { delete this; }

	std::string const& getName() const override { return name; }
	int64_t getValue() const override {
		auto result = f();
		// Disallow conversion from floating point to int64_t, since this has
		// been a source of confusion - e.g. a percentage represented as a
		// fraction between 0 and 1 is not meaningful after conversion to
		// int64_t.
		static_assert(!std::is_floating_point_v<decltype(result)>);
		return result;
	}

	void resetInterval() override {}

	bool hasRate() const override { return false; }
	double getRate() const override { throw internal_error(); }
	bool hasRoughness() const override { return false; }
	double getRoughness() const override { throw internal_error(); }

	std::string name;
	F f;
};
template <class F>
static void specialCounter(CounterCollection& collection, std::string const& name, F&& f) {
	new SpecialCounter<F>(collection, name, std::move(f));
}

Future<Void> traceCounters(
    std::string const& traceEventName,
    UID const& traceEventID,
    double const& interval,
    CounterCollection* const& counters,
    std::string const& trackLatestName = std::string(),
    std::function<void(TraceEvent&)> const& decorator = [](TraceEvent& te) {});

class LatencyBands {
public:
	LatencyBands(std::string name, UID id, double loggingInterval)
	  : name(name), id(id), loggingInterval(loggingInterval) {}

	void addThreshold(double value) {
		if (value > 0 && bands.count(value) == 0) {
			if (bands.size() == 0) {
				ASSERT(!cc && !filteredCount);
				cc = std::make_unique<CounterCollection>(name, id.toString());
				logger = traceCounters(name, id, loggingInterval, cc.get(), id.toString() + "/" + name);
				filteredCount = std::make_unique<Counter>("Filtered", *cc);
				insertBand(std::numeric_limits<double>::infinity());
			}

			insertBand(value);
		}
	}

	void addMeasurement(double measurement, bool filtered = false) {
		if (filtered && filteredCount) {
			++(*filteredCount);
		} else if (bands.size() > 0) {
			auto itr = bands.upper_bound(measurement);
			ASSERT(itr != bands.end());
			++(*itr->second);
		}
	}

	void clearBands() {
		logger = Void();
		bands.clear();
		filteredCount.reset();
		cc.reset();
	}

	~LatencyBands() { clearBands(); }

private:
	std::map<double, std::unique_ptr<Counter>> bands;
	std::unique_ptr<Counter> filteredCount;

	std::string name;
	UID id;
	double loggingInterval;

	std::unique_ptr<CounterCollection> cc;
	Future<Void> logger;

	void insertBand(double value) {
		bands.emplace(std::make_pair(value, std::make_unique<Counter>(format("Band%f", value), *cc)));
	}
};

class LatencySample {
public:
	LatencySample(std::string name, UID id, double loggingInterval, int sampleSize)
	  : name(name), id(id), sampleStart(now()), sample(sampleSize),
	    latencySampleEventHolder(makeReference<EventCacheHolder>(id.toString() + "/" + name)) {
		logger = recurring([this]() { logSample(); }, loggingInterval);
	}

	void addMeasurement(double measurement) { sample.addSample(measurement); }

private:
	std::string name;
	UID id;
	double sampleStart;

	ContinuousSample<double> sample;
	Future<Void> logger;

	Reference<EventCacheHolder> latencySampleEventHolder;

	void logSample() {
		TraceEvent(name.c_str(), id)
		    .detail("Count", sample.getPopulationSize())
		    .detail("Elapsed", now() - sampleStart)
		    .detail("Min", sample.min())
		    .detail("Max", sample.max())
		    .detail("Mean", sample.mean())
		    .detail("Median", sample.median())
		    .detail("P25", sample.percentile(0.25))
		    .detail("P90", sample.percentile(0.9))
		    .detail("P95", sample.percentile(0.95))
		    .detail("P99", sample.percentile(0.99))
		    .detail("P99.9", sample.percentile(0.999))
		    .trackLatest(latencySampleEventHolder->trackingKey);

		sample.clear();
		sampleStart = now();
	}
};

#endif
