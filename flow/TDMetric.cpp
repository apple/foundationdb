/*
 * TDMetric.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "flow/Error.h"
#include "flow/OTELMetrics.h"
#include "flow/TDMetric.actor.h"
#include "flow/flow.h"
#include <cctype>
#include <cstddef>
#include <string>

alignas(8) const StringRef BaseEventMetric::metricType = "Event"_sr;
template <>
alignas(8) const StringRef Int64Metric::metricType = "Int64"_sr;
template <>
alignas(8) const StringRef DoubleMetric::metricType = "Double"_sr;
template <>
alignas(8) const StringRef BoolMetric::metricType = "Bool"_sr;
template <>
alignas(8) const StringRef StringMetric::metricType = "String"_sr;

std::string reduceFilename(std::string const& filename) {
	std::string r = filename;

	// Remove any path prefix
	size_t trunc = r.find_last_of("/\\");
	if (trunc != r.npos)
		r = r.substr(trunc + 1);

	// Look for sequences of 8 or more hex chars and remove them if found
	size_t pos = 0;
	static const char* hexchars = "0123456789abcdef";
	while (pos < r.size()) {
		size_t first = r.find_first_of(hexchars, pos);
		if (first == r.npos)
			break;
		size_t last = r.find_first_not_of(hexchars, first);
		if (last == r.npos)
			last = r.size();
		int runLen = last - first;
		if (runLen >= 8)
			r.erase(first, runLen);
		else
			pos = last + 1;
	}

	return r;
}

void MetricKeyRef::writeField(BinaryWriter& wr) const {
	wr.serializeBytes("\x01"_sr);
	wr.serializeBytes(fieldName);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(fieldType);
	wr.serializeBytes("\x00"_sr);
}

void MetricKeyRef::writeMetricName(BinaryWriter& wr) const {
	wr.serializeBytes("\x01"_sr);
	wr.serializeBytes(name.name);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(name.type);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(address);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(name.id);
	wr.serializeBytes("\x00"_sr);
}

const Standalone<StringRef> MetricKeyRef::packLatestKey() const {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(prefix);
	wr.serializeBytes("\x01TDMetricsLastValue\x00"_sr);
	writeMetricName(wr);
	return wr.toValue();
}

const Standalone<StringRef> MetricKeyRef::packDataKey(int64_t time) const {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(prefix);
	if (isField())
		wr.serializeBytes("\x01TDFieldData\x00"_sr);
	else
		wr.serializeBytes("\x01TDMetricData\x00"_sr);
	writeMetricName(wr);
	if (isField())
		writeField(wr);
	wr.serializeAsTuple(level);
	if (time >= 0)
		wr.serializeAsTuple(time);
	return wr.toValue();
}

const Standalone<StringRef> MetricKeyRef::packFieldRegKey() const {
	ASSERT(isField());
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(prefix);
	wr.serializeBytes("\x01TDFields\x00\x01"_sr);
	wr.serializeBytes(name.name);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(name.type);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(fieldName);
	wr.serializeBytes("\x00\x01"_sr);
	wr.serializeBytes(fieldType);
	wr.serializeBytes("\x00"_sr);
	return wr.toValue();
}

bool TDMetricCollection::canLog(int level) const {
	// Whether a given level can be logged or not depends on the length of the rollTimes queue.

	// No restriction until queue size reaches METRIC_LIMIT_START_QUEUE_SIZE
	if (rollTimes.size() < FLOW_KNOBS->METRIC_LIMIT_START_QUEUE_SIZE)
		return true;

	int extraQueueItems = rollTimes.size() - FLOW_KNOBS->METRIC_LIMIT_START_QUEUE_SIZE;
	// Level must be greater than the number of responseFactor-sized groups of additional items in the queue.
	return level > extraQueueItems / FLOW_KNOBS->METRIC_LIMIT_RESPONSE_FACTOR;
}

void TDMetricCollection::checkRoll(uint64_t t, int64_t usedBytes) {
	currentTimeBytes += usedBytes;
	if (currentTimeBytes > 1e6) {
		CODE_PROBE(true, "metrics were rolled", probe::decoration::rare);
		currentTimeBytes = 0;
		rollTimes.push_back(t);
		for (auto& it : metricMap)
			it.value->rollMetric(t);
		metricEnabled.trigger();
	}
}

DynamicEventMetric::DynamicEventMetric(MetricNameRef const& name, Void)
  : BaseEventMetric(name), latestRecorded(false), newFields(false) {}

uint64_t DynamicEventMetric::log(uint64_t explicitTime) {
	if (!enabled)
		return 0;

	uint64_t t = explicitTime ? explicitTime : timer_int();
	double x = deterministicRandom()->random01();

	int64_t l = 0;
	if (x == 0.0)
		l = FLOW_KNOBS->MAX_METRIC_LEVEL - 1;
	else
		l = std::min(FLOW_KNOBS->MAX_METRIC_LEVEL - 1, (int64_t)(::log(1.0 / x) / FLOW_KNOBS->METRIC_LEVEL_DIVISOR));

	if (!TDMetricCollection::getTDMetrics()->canLog(l))
		return 0;

	// fprintf(stderr, "Logging %s with %d fields (other than Time)\n", name.name.toString().c_str(), fields.size());

	if (newFields) {
		// New fields were added so go to new key for all fields (at all levels) so the field parallel data series line
		// up correctly.
		time.nextKeyAllLevels(t);
		for (auto& [name, field] : fields)
			field->nextKeyAllLevels(t);
		newFields = false;
	}

	bool overflow = false;
	int64_t bytes = 0;

	time.log(t, t, l, overflow, bytes);

	for (auto& [name, field] : fields)
		field->log(t, l, overflow, bytes);

	if (overflow) {
		time.nextKey(t, l);
		for (auto& [name, field] : fields)
			field->nextKey(t, l);
	}

	latestRecorded = false;
	TDMetricCollection::getTDMetrics()->checkRoll(t, bytes);
	clearFields();
	return t;
}

void DynamicEventMetric::flushData(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) {
	time.flushField(mk, rollTime, batch);
	for (auto& [name, field] : fields)
		field->flushField(mk, rollTime, batch);
	if (!latestRecorded) {
		batch.scope.updates.emplace_back(mk.packLatestKey(), StringRef());
		latestRecorded = true;
	}
}

void DynamicEventMetric::rollMetric(uint64_t t) {
	time.rollMetric(t);
	for (auto& f : fields)
		f.second->rollMetric(t);
}

void DynamicEventMetric::registerFields(MetricKeyRef const& mk, std::vector<Standalone<StringRef>>& fieldKeys) {
	// This is actually redundant on update registrations but it's not a big deal
	time.registerField(mk, fieldKeys);

	// Register the new fields
	for (auto& f : fieldsToRegister)
		fields[f]->registerField(mk, fieldKeys);

	// Clear the to-register set.
	fieldsToRegister.clear();
}

std::string MetricData::toString() const {
	return format("MetricData(addr=%p start=%llu appendStart=%llu rollTime=%llu writerLen=%d)",
	              this,
	              start,
	              appendStart,
	              rollTime,
	              writer.getLength());
}

std::string createStatsdMessage(const std::string& name, StatsDMetric type, const std::string& val) {
	return createStatsdMessage(name, type, val, {});
}

std::string createStatsdMessage(const std::string& name,
                                StatsDMetric type,
                                const std::string& val,
                                const std::vector<std::pair<std::string, std::string>>& tags) {
	ASSERT(!name.empty());
	std::string msg = name + ":" + val;
	switch (type) {
	case StatsDMetric::GAUGE:
		msg += "|g";
		break;

	case StatsDMetric::COUNTER:
		msg += "|c";
		break;
	}

	if (!tags.empty()) {
		msg += "|";
		for (size_t i = 0; i < tags.size(); i++) {
			msg = msg + "#" + tags[i].first + ":" + tags[i].second;
			// If we know there is another tag coming, we should add a comma in the message
			if (i != tags.size() - 1) {
				msg += ",";
			}
		}
	}

	return msg;
}

MetricsDataModel knobToMetricModel(const std::string& knob) {
	if (knob == "statsd") {
		return MetricsDataModel::STATSD;
	} else if (knob == "otel") {
		return MetricsDataModel::OTLP;
	} else if (knob == "none") {
		return MetricsDataModel::NONE;
	}
	ASSERT(false);
	return MetricsDataModel::NONE;
}

std::vector<std::string> splitString(const std::string& str, const std::string& delimit) {
	std::vector<std::string> splitted;
	size_t pos = 0;
	std::string s = str;

	while ((pos = s.find(delimit)) != std::string::npos) {
		splitted.push_back(s.substr(0, pos));
		s.erase(0, pos + delimit.length());
	}
	splitted.push_back(s);
	return splitted;
}

/*
    Returns true if num is exactly a string representation of a number
    Ex: "123", "123.65" both return true
    "124.532.13", "t4fr", "102g" all return false
*/
bool isNumber(const std::string& num) {
	if (num.empty()) {
		return false;
	}

	size_t start = 0;
	// We could have a negative number, if the first character isn't a digit
	// but it's a "-", then we start from position 1. Otherwise it's not a valid number
	if (!std::isdigit(num[0])) {
		if (num[0] == '-') {
			start = 1;
		} else {
			return false;
		}
	}

	// Iterate through the string and make sure every char is a digit and there is only one occurrence of "."
	int dot_count = 0;
	for (size_t i = start; i < num.size(); i++) {
		if (!std::isdigit(num[i])) {
			if (num[i] == '.') {
				if (dot_count > 0) {
					return false;
				}
				++dot_count;
			} else {
				return false;
			}
		}
	}
	return true;
}

/*
    Returns true if msg is a valid statsd string. Valid statsd strings are of the form
    <name>:<value>|<type>|#<tag1-key>:<tag1-value>,<tag2-k/v>

    Where name consists of only upper or lowercase letters (no symbols),
    value is numeric (positive or negative, integer or decimal),
    type is one of "g", "c",

*/
bool verifyStatsdMessage(const std::string& msg) {
	auto tokens = splitString(msg, "|");
	std::vector<std::string> statsdTypes{ "c", "g" };

	// We can't have more than three "|" in our string based on above format
	if (tokens.size() > 3) {
		return false;
	}

	// First check if <name>:<value> is valid, this should be in tokens[0]
	auto nameVal = splitString(tokens[0], ":");
	if (nameVal.size() != 2) {
		return false;
	}
	// nameVal[1] should be a numeric value
	if (!isNumber(nameVal[1])) {
		return false;
	}

	// The 2nd token should always represent a valid statsd type
	if (std::find(statsdTypes.begin(), statsdTypes.end(), tokens[1]) == statsdTypes.end()) {
		return false;
	}

	// It is optional to have tags but the tags section must be non-empty and begin
	// with a "#"
	if (tokens.size() > 2) {
		if (tokens[2].empty()) {
			return false;
		}
		if (tokens[2][0] != '#') {
			return false;
		}
	}
	return true;
}

void createOtelGauge(UID id, const std::string& name, double value) {
	MetricCollection* metrics = MetricCollection::getMetricCollection();
	if (metrics != nullptr) {
		NetworkAddress addr = g_network->getLocalAddress();
		std::string ip_str = addr.ip.toString();
		std::string port_str = std::to_string(addr.port);
		if (metrics->gaugeMap.find(id) != metrics->gaugeMap.end()) {
			metrics->gaugeMap[id].points.emplace_back(value);
		} else {
			metrics->gaugeMap[id] = OTEL::OTELGauge(name, value);
		}
		metrics->gaugeMap[id].points.back().addAttribute("ip", ip_str);
		metrics->gaugeMap[id].points.back().addAttribute("port", port_str);
	}
}

void createOtelGauge(UID id, const std::string& name, double value, const std::vector<OTEL::Attribute>& attrs) {
	MetricCollection* metrics = MetricCollection::getMetricCollection();
	createOtelGauge(id, name, value);
	if (metrics != nullptr) {
		for (const auto& attr : attrs) {
			metrics->gaugeMap[id].points.back().addAttribute(attr.key, attr.value);
		}
	}
}
