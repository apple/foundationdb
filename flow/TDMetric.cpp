/*
 * TDMetric.cpp
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

#include "flow/TDMetric.actor.h"
#include "flow/flow.h"

const StringRef BaseEventMetric::metricType = LiteralStringRef("Event");
template <>
const StringRef Int64Metric::metricType = LiteralStringRef("Int64");
template <>
const StringRef DoubleMetric::metricType = LiteralStringRef("Double");
template <>
const StringRef BoolMetric::metricType = LiteralStringRef("Bool");
template <>
const StringRef StringMetric::metricType = LiteralStringRef("String");

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
	wr.serializeBytes(LiteralStringRef("\x01"));
	wr.serializeBytes(fieldName);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(fieldType);
	wr.serializeBytes(LiteralStringRef("\x00"));
}

void MetricKeyRef::writeMetricName(BinaryWriter& wr) const {
	wr.serializeBytes(LiteralStringRef("\x01"));
	wr.serializeBytes(name.name);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(name.type);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(address);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(name.id);
	wr.serializeBytes(LiteralStringRef("\x00"));
}

const Standalone<StringRef> MetricKeyRef::packLatestKey() const {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(prefix);
	wr.serializeBytes(LiteralStringRef("\x01TDMetricsLastValue\x00"));
	writeMetricName(wr);
	return wr.toValue();
}

const Standalone<StringRef> MetricKeyRef::packDataKey(int64_t time) const {
	BinaryWriter wr(Unversioned());
	wr.serializeBytes(prefix);
	if (isField())
		wr.serializeBytes(LiteralStringRef("\x01TDFieldData\x00"));
	else
		wr.serializeBytes(LiteralStringRef("\x01TDMetricData\x00"));
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
	wr.serializeBytes(LiteralStringRef("\x01TDFields\x00\x01"));
	wr.serializeBytes(name.name);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(name.type);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(fieldName);
	wr.serializeBytes(LiteralStringRef("\x00\x01"));
	wr.serializeBytes(fieldType);
	wr.serializeBytes(LiteralStringRef("\x00"));
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
		TEST(true); // metrics were rolled
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

void DynamicEventMetric::flushData(MetricKeyRef const& mk, uint64_t rollTime, MetricUpdateBatch& batch) {
	time.flushField(mk, rollTime, batch);
	for (auto& [name, field] : fields)
		field->flushField(mk, rollTime, batch);
	if (!latestRecorded) {
		batch.updates.emplace_back(mk.packLatestKey(), StringRef());
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
