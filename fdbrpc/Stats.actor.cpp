/*
 * Stats.actor.cpp
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

#include "fdbrpc/Stats.h"
#include "flow/Knobs.h"
#include "flow/OTELMetrics.h"
#include "flow/TDMetric.actor.h"
#include "flow/actorcompiler.h" // has to be last include
#include "flow/network.h"
#include <string>

Counter::Counter(std::string const& name, CounterCollection& collection)
  : name(name), IMetric(knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL)), interval_start(0), last_event(0),
    interval_sq_time(0), roughness_interval_start(0), interval_delta(0), interval_start_value(0) {
	metric.init(collection.getName() + "." + (char)toupper(name.at(0)) + name.substr(1), collection.getId());
	collection.addCounter(this);
}

void Counter::operator+=(Value delta) {
	if (!delta)
		return; //< Otherwise last_event will be reset
	interval_delta += delta;
	auto t = now();
	auto elapsed = t - last_event;
	interval_sq_time += elapsed * elapsed;
	last_event = t;

	metric += delta;
}

double Counter::getRate() const {
	double elapsed = now() - interval_start;
	return elapsed > 0 ? interval_delta / elapsed : 0;
}

double Counter::getRoughness() const {
	double elapsed = last_event - roughness_interval_start;
	if (elapsed == 0) {
		return -1;
	}

	// If we have time interval samples t in T, and let:
	// n = size(T) = interval_delta
	// m = mean(T) = elapsed / interval_delta
	// v = sum(t^2) for t in T = interval_sq_time
	//
	// The formula below is: (v/(m*n)) / m - 1
	// This is equivalent to (v/n - m^2) / m^2 = Variance(T)/m^2
	// Variance(T)/m^2 is equal to Variance(t/m) for t in T
	double delay = interval_sq_time / elapsed;
	return delay * interval_delta / elapsed - 1;
}

void Counter::resetInterval() {
	interval_start_value += interval_delta;
	interval_delta = 0;
	interval_sq_time = 0;
	interval_start = now();
	if (last_event == 0) {
		last_event = interval_start;
	}
	roughness_interval_start = last_event;
}

void Counter::clear() {
	resetInterval();
	interval_start_value = 0;

	metric = 0;
}

void Counter::flush(MetricBatch& batch) {
	Value val = getValue();
	switch (model) {
	case MetricsDataModel::STATSD: {
		std::string msg = createStatsdMessage(name, StatsDMetric::COUNTER, std::to_string(val));
		if (!batch.statsd_message.empty()) {
			batch.statsd_message += "\n";
		}
		batch.statsd_message += msg;
		break;
	}
	case MetricsDataModel::OTEL: {
		NetworkAddress addr = g_network->getLocalAddress();
		batch.counters.emplace_back(name, val);
		batch.counters.back().point.startTime = last_event;
		batch.counters.back().point.addAttribute("ip", addr.ip.toString());
		batch.counters.back().point.addAttribute("port", std::to_string(addr.port));
		break;
	}
	default:
		break;
	}
}

void CounterCollection::logToTraceEvent(TraceEvent& te) const {
	for (ICounter* c : counters) {
		te.detail(c->getName().c_str(), c);
		c->resetInterval();
	}
}

class CounterCollectionImpl {
public:
	ACTOR static Future<Void> traceCounters(CounterCollection* counters,
	                                        std::string traceEventName,
	                                        UID traceEventID,
	                                        double interval,
	                                        std::string trackLatestName,
	                                        std::function<void(TraceEvent&)> decorator) {
		wait(delay(0)); // Give an opportunity for all members used in special counters to be initialized

		for (ICounter* c : counters->counters)
			c->resetInterval();

		state Reference<EventCacheHolder> traceEventHolder;
		if (!trackLatestName.empty()) {
			traceEventHolder = makeReference<EventCacheHolder>(trackLatestName);
		}

		state double last_interval = now();

		loop {
			TraceEvent te(traceEventName.c_str(), traceEventID);
			te.detail("Elapsed", now() - last_interval);

			counters->logToTraceEvent(te);
			decorator(te);

			if (!trackLatestName.empty()) {
				te.trackLatest(traceEventHolder->trackingKey);
			}

			last_interval = now();
			wait(delay(interval, TaskPriority::FlushTrace));
		}
	}
};

Future<Void> CounterCollection::traceCounters(std::string const& traceEventName,
                                              UID traceEventID,
                                              double interval,
                                              std::string const& trackLatestName,
                                              std::function<void(TraceEvent&)> const& decorator) {
	return CounterCollectionImpl::traceCounters(
	    this, traceEventName, traceEventID, interval, trackLatestName, decorator);
}

void LatencyBands::insertBand(double value) {
	bands.emplace(std::make_pair(value, std::make_unique<Counter>(format("Band%f", value), *cc)));
}

FDB_DEFINE_BOOLEAN_PARAM(Filtered);

LatencyBands::LatencyBands(std::string const& name,
                           UID id,
                           double loggingInterval,
                           std::function<void(TraceEvent&)> const& decorator)
  : name(name), id(id), loggingInterval(loggingInterval), decorator(decorator) {}

void LatencyBands::addThreshold(double value) {
	if (value > 0 && bands.count(value) == 0) {
		if (bands.size() == 0) {
			ASSERT(!cc && !filteredCount);
			cc = std::make_unique<CounterCollection>(name, id.toString());
			logger = cc->traceCounters(name, id, loggingInterval, id.toString() + "/" + name, decorator);
			filteredCount = std::make_unique<Counter>("Filtered", *cc);
			insertBand(std::numeric_limits<double>::infinity());
		}

		insertBand(value);
	}
}

void LatencyBands::addMeasurement(double measurement, int count, Filtered filtered) {
	if (filtered && filteredCount) {
		(*filteredCount) += count;
	} else if (bands.size() > 0) {
		auto itr = bands.upper_bound(measurement);
		ASSERT(itr != bands.end());
		(*itr->second) += count;
	}
}

void LatencyBands::clearBands() {
	logger = Void();
	bands.clear();
	filteredCount.reset();
	cc.reset();
}

LatencyBands::~LatencyBands() {
	clearBands();
}

LatencySample::LatencySample(std::string name, UID id, double loggingInterval, int sampleSize)
  : name(name), IMetric(knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL)), id(id), sampleEmit(now()),
    sampleTrace(now()), sample(sampleSize), prev(sampleSize),
    latencySampleEventHolder(makeReference<EventCacheHolder>(id.toString() + "/" + name)) {
	logger = recurring([this]() { logSample(); }, loggingInterval);
}

void LatencySample::addMeasurement(double measurement) {
	sample.addSample(measurement);
}

void LatencySample::flush(MetricBatch& batch) {
	std::string msg;
	switch (model) {
	case MetricsDataModel::STATSD: {
		auto median_gauge = createStatsdMessage(name + "p50", StatsDMetric::GAUGE, std::to_string(sample.median()));
		auto p90_gauge = createStatsdMessage(name + "p90", StatsDMetric::GAUGE, std::to_string(sample.percentile(0.9)));
		auto p95_gauge =
		    createStatsdMessage(name + "p95", StatsDMetric::GAUGE, std::to_string(sample.percentile(0.95)));
		auto p99_gauge =
		    createStatsdMessage(name + "p99", StatsDMetric::GAUGE, std::to_string(sample.percentile(0.99)));
		auto p999_gauge =
		    createStatsdMessage(name + "p99.9", StatsDMetric::GAUGE, std::to_string(sample.percentile(0.999)));

		if (!batch.statsd_message.empty()) {
			msg += "\n";
		}
		msg += median_gauge;
		msg += "\n";
		msg += p90_gauge;
		msg += "\n";
		msg += p95_gauge;
		msg += "\n";
		msg += p99_gauge;
		msg += "\n";
		msg += p999_gauge;
		batch.statsd_message += msg;
		break;
	}

	case MetricsDataModel::OTEL: {
		NetworkAddress addr = g_network->getLocalAddress();
		const ContinuousSample<double> emitSample = (sample.getPopulationSize() == 0) ? prev : sample;
		batch.hists.push_back(
		    OTELHistogram(name, emitSample.getSamples(), emitSample.min(), emitSample.max(), emitSample.sum()));
		batch.hists.back().point.startTime = sampleTrace;
		batch.hists.back().point.addAttribute("ip", addr.ip.toString());
		batch.hists.back().point.addAttribute("port", std::to_string(addr.port));
		batch.hists.back().point.startTime = sampleTrace;
		sampleEmit = now();
		break;
	}

	default:
		break;
	}
}

void LatencySample::logSample() {
	TraceEvent(name.c_str(), id)
	    .detail("Count", sample.getPopulationSize())
	    .detail("Elapsed", now() - sampleTrace)
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
	prev.swap(sample);
	sample.clear();
	sampleTrace = now();
}
