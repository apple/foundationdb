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
#include "flow/IRandom.h"
#include "flow/Knobs.h"
#include "flow/OTELMetrics.h"
#include "flow/TDMetric.actor.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/network.h"
#include <string>
#include "flow/actorcompiler.h" // has to be last include

Counter::Counter(std::string const& name, CounterCollection& collection, bool skipTraceOnSilentInterval)
  : name(name), interval_start(0), last_event(0), interval_sq_time(0), roughness_interval_start(0), interval_delta(0),
    interval_start_value(0), skip_trace_on_silent_interval(skipTraceOnSilentInterval) {
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

void CounterCollection::logToTraceEvent(TraceEvent& te) {
	NetworkAddress addr = g_network->getLocalAddress();
	for (ICounter* c : counters) {
		MetricCollection* metrics = MetricCollection::getMetricCollection();
		if (metrics != nullptr) {
			std::string ip_str = addr.ip.toString();
			std::string port_str = std::to_string(addr.port);
			uint64_t val = c->getValue();
			switch (c->model) {
			case MetricsDataModel::OTLP: {
				if (metrics->sumMap.find(c->id) != metrics->sumMap.end()) {
					metrics->sumMap[c->id].points.emplace_back(static_cast<int64_t>(val));
				} else {
					metrics->sumMap[c->id] = OTEL::OTELSum(name + "." + c->getName(), val);
				}
				metrics->sumMap[c->id].points.back().addAttribute("ip", ip_str);
				metrics->sumMap[c->id].points.back().addAttribute("port", port_str);
				metrics->sumMap[c->id].points.back().startTime = logTime;
			}
			case MetricsDataModel::STATSD: {
				std::vector<std::pair<std::string, std::string>> statsd_attributes{ { "ip", ip_str },
					                                                                { "port", port_str } };
				metrics->statsd_message.push_back(createStatsdMessage(
				    c->getName(), StatsDMetric::COUNTER, std::to_string(val) /*, statsd_attributes*/));
			}
			case MetricsDataModel::NONE:
			default: {
			}
			}
		}
		if (!c->suppressTrace()) {
			te.detail(c->getName().c_str(), c);
		}
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

LatencySample::LatencySample(std::string name,
                             UID id,
                             double loggingInterval,
                             double accuracy,
                             bool skipTraceOnSilentInterval)
  : name(name), IMetric(knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL)), id(id), sampleEmit(now()), sketch(accuracy),
    latencySampleEventHolder(makeReference<EventCacheHolder>(id.toString() + "/" + name)),
    skipTraceOnSilentInterval(skipTraceOnSilentInterval) {
	logger = recurring([this]() { logSample(); }, loggingInterval);
	p50id = deterministicRandom()->randomUniqueID();
	p90id = deterministicRandom()->randomUniqueID();
	p95id = deterministicRandom()->randomUniqueID();
	p99id = deterministicRandom()->randomUniqueID();
	p999id = deterministicRandom()->randomUniqueID();
}

void LatencySample::addMeasurement(double measurement) {
	sketch.addSample(measurement);
}

void LatencySample::logSample() {
	if (skipTraceOnSilentInterval && sketch.getPopulationSize() == 0) {
		return;
	}
	double p25 = sketch.percentile(0.25);
	double p50 = sketch.mean();
	double p90 = sketch.percentile(0.9);
	double p95 = sketch.percentile(0.95);
	double p99 = sketch.percentile(0.99);
	double p99_9 = sketch.percentile(0.999);
	TraceEvent(name.c_str(), id)
	    .detail("Count", sketch.getPopulationSize())
	    .detail("Elapsed", now() - sampleEmit)
	    .detail("Min", sketch.min())
	    .detail("Max", sketch.max())
	    .detail("Mean", sketch.mean())
	    .detail("Median", p50)
	    .detail("P25", p25)
	    .detail("P90", p90)
	    .detail("P95", p95)
	    .detail("P99", p99)
	    .detail("P99.9", p99_9)
	    .trackLatest(latencySampleEventHolder->trackingKey);
	MetricCollection* metrics = MetricCollection::getMetricCollection();
	if (metrics != nullptr) {
		NetworkAddress addr = g_network->getLocalAddress();
		std::string ip_str = addr.ip.toString();
		std::string port_str = std::to_string(addr.port);
		switch (model) {
		case MetricsDataModel::OTLP: {
			// We only want to emit the entire DDSketch if the knob is set
			if (FLOW_KNOBS->METRICS_EMIT_DDSKETCH) {
				if (metrics->histMap.find(IMetric::id) != metrics->histMap.end()) {
					metrics->histMap[IMetric::id].points.emplace_back(
					    sketch.getErrorGuarantee(), sketch.getSamples(), sketch.min(), sketch.max(), sketch.getSum());
				} else {
					metrics->histMap[IMetric::id] = OTEL::OTELHistogram(name,
					                                                    sketch.getErrorGuarantee(),
					                                                    sketch.getSamples(),
					                                                    sketch.min(),
					                                                    sketch.max(),
					                                                    sketch.getSum());
				}
				metrics->histMap[IMetric::id].points.back().addAttribute("ip", ip_str);
				metrics->histMap[IMetric::id].points.back().addAttribute("port", port_str);
				metrics->histMap[IMetric::id].points.back().startTime = sampleEmit;
			}
			createOtelGauge(p50id, name + "p50", p50);
			createOtelGauge(p90id, name + "p90", p90);
			createOtelGauge(p95id, name + "p95", p95);
			createOtelGauge(p99id, name + "p99", p99);
			createOtelGauge(p999id, name + "p99_9", p99_9);
		}
		case MetricsDataModel::STATSD: {
			std::vector<std::pair<std::string, std::string>> statsd_attributes{ { "ip", ip_str },
				                                                                { "port", port_str } };
			auto median_gauge =
			    createStatsdMessage(name + "p50", StatsDMetric::GAUGE, std::to_string(p50) /*, statsd_attributes*/);
			auto p90_gauge =
			    createStatsdMessage(name + "p90", StatsDMetric::GAUGE, std::to_string(p90) /*, statsd_attributes*/);
			auto p95_gauge =
			    createStatsdMessage(name + "p95", StatsDMetric::GAUGE, std::to_string(p95) /*, statsd_attributes*/);
			auto p99_gauge =
			    createStatsdMessage(name + "p99", StatsDMetric::GAUGE, std::to_string(p99) /*, statsd_attributes*/);
			auto p999_gauge =
			    createStatsdMessage(name + "p99.9", StatsDMetric::GAUGE, std::to_string(p99_9) /*, statsd_attributes*/);
		}
		case MetricsDataModel::NONE:
		default: {
		}
		}
	}
	sketch.clear();
	sampleEmit = now();
}
