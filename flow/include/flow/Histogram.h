/*
 * Histogram.h
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

#ifndef FLOW_HISTOGRAM_H
#define FLOW_HISTOGRAM_H
#pragma once

#include <flow/Arena.h>
#include <string>
#include <map>
#include <unordered_map>
#include <iomanip>
#ifdef _WIN32
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#endif

class Histogram;

class HistogramRegistry : public ReferenceCounted<HistogramRegistry> {
public:
	void registerHistogram(Histogram* h);
	void unregisterHistogram(Histogram* h);
	Histogram* lookupHistogram(std::string const& name);
	void logReport(double elapsed = -1.0);
	void clear();

private:
	// This map is ordered by key so that ops within the same group end up
	// next to each other in the trace log.
	std::map<std::string, Histogram*> histograms;
};

HistogramRegistry& GetHistogramRegistry();

/*
 * A fast histogram with power-of-two spaced buckets.
 *
 * For more information about this technique, see:
 * https://www.fsl.cs.stonybrook.edu/project-osprof.html
 */
class Histogram final : public ReferenceCounted<Histogram> {
public:
	enum class Unit { microseconds = 0, bytes, bytes_per_second, percentageLinear, countLinear, MAXHISTOGRAMUNIT };
	static const char* const UnitToStringMapper[];

	Histogram(Reference<HistogramRegistry> regis,
	          std::string const& group = "",
	          std::string const& op = "",
	          Unit unit = Unit::MAXHISTOGRAMUNIT,
	          uint32_t lower = 0,
	          uint32_t upper = UINT32_MAX)
	  : group(group), op(op), unit(unit), registry(regis), lowerBound(lower), upperBound(upper) {

		ASSERT(unit <= Unit::MAXHISTOGRAMUNIT);
		ASSERT(upperBound >= lowerBound);
		clear();
	}

private:
	static std::string generateName(std::string const& group, std::string const& op) { return group + ":" + op; }

public:
	~Histogram() {
		if (registry.isValid() && unit != Unit::MAXHISTOGRAMUNIT) {
			registry->unregisterHistogram(this);
		}
		registry.clear();
	}

	static Reference<Histogram> getHistogram(StringRef group,
	                                         StringRef op,
	                                         Unit unit,
	                                         uint32_t lower = 0,
	                                         uint32_t upper = UINT32_MAX) {
		std::string group_str = group.toString();
		std::string op_str = op.toString();
		std::string name = generateName(group_str, op_str);
		HistogramRegistry& registry = GetHistogramRegistry();
		Histogram* h = registry.lookupHistogram(name);
		if (!h) {
			h = new Histogram(Reference<HistogramRegistry>::addRef(&registry), group_str, op_str, unit, lower, upper);
			registry.registerHistogram(h);
			return Reference<Histogram>(h);
		} else {
			return Reference<Histogram>::addRef(h);
		}
	}

	// This histogram buckets samples into powers of two.
	inline void sample(uint32_t sample) {
		size_t idx;
#ifdef _WIN32
		unsigned long index;
		// _BitScanReverse sets index to the position of the first non-zero bit, so
		// _BitScanReverse(sample) ~= log_2(sample).  _BitScanReverse returns false if
		// sample is zero.
		idx = _BitScanReverse(&index, sample) ? index : 0;
#else
		// __builtin_clz counts the leading zeros in its uint32_t argument.  So, 31-clz ~= log_2(sample).
		// __builtin_clz(0) is undefined.
		idx = sample ? (31 - __builtin_clz(sample)) : 0;
#endif
		ASSERT(idx < 32);
		buckets[idx]++;
	}

	inline void sampleSeconds(double delta) {
		uint64_t delta_usec = (delta * 1000000);
		if (delta_usec > UINT32_MAX) {
			sample(UINT32_MAX);
		} else {
			sample((uint32_t)(delta * 1000000)); // convert to microseconds and truncate to integer
		}
	}
	// Histogram buckets samples into linear interval of size 4 percent.
	inline void samplePercentage(double pct) {
		ASSERT(pct >= 0.0);
		if (pct >= 1.28) {
			pct = 1.24;
		}
		size_t idx = (pct * 100) / 4;
		ASSERT(idx < 32 && idx >= 0);
		buckets[idx]++;
	}

	// Histogram buckets samples into one of the same sized buckets
	// This is used when the distance b/t upperBound and lowerBound are relativly small
	inline void sampleRecordCounter(uint32_t sample) {
		if (sample > upperBound) {
			sample = upperBound;
		}
		size_t idx = ((sample - lowerBound) * 31.0) / (upperBound - lowerBound);
		ASSERT(idx < 32 && idx >= 0);
		buckets[idx]++;
	}

	void updateUpperBound(uint32_t upperBound) {
		this->upperBound = upperBound;
		clear();
	}

	void clear() {
		for (uint32_t& i : buckets) {
			i = 0;
		}
	}
	void writeToLog(double elapsed = -1.0);

	std::string name() const { return generateName(this->group, this->op); }

	std::string drawHistogram();

	std::string const group;
	std::string const op;
	Unit const unit;
	Reference<HistogramRegistry> registry;
	uint32_t buckets[32];
	uint32_t lowerBound;
	uint32_t upperBound;
};

#endif // FLOW_HISTOGRAM_H
