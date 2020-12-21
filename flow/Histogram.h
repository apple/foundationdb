/*
 * Histogram.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifdef _WIN32
#include <intrin.h>
#pragma intrinsic(_BitScanReverse)
#endif

class Histogram;

class HistogramRegistry {
public:
	void registerHistogram(Histogram* h);
	void unregisterHistogram(Histogram* h);
	Histogram* lookupHistogram(std::string name);
	void logReport();

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
class Histogram sealed : public ReferenceCounted<Histogram> {
public:
	enum class Unit { microseconds, bytes, bytes_per_second };

private:
	static const std::unordered_map<Unit, std::string> UnitToStringMapper;

	Histogram(std::string group, std::string op, Unit unit, HistogramRegistry& registry)
	  : group(group), op(op), unit(unit), registry(registry), ReferenceCounted<Histogram>() {

		ASSERT(UnitToStringMapper.find(unit) != UnitToStringMapper.end());

		clear();
	}

	static std::string generateName(std::string group, std::string op) { return group + ":" + op; }

public:
	~Histogram() { registry.unregisterHistogram(this); }

	static Reference<Histogram> getHistogram(StringRef group, StringRef op, Unit unit) {
		std::string group_str = group.toString();
		std::string op_str = op.toString();
		std::string name = generateName(group_str, op_str);
		HistogramRegistry& registry = GetHistogramRegistry();
		Histogram* h = registry.lookupHistogram(name);
		if (!h) {
			h = new Histogram(group_str, op_str, unit, registry);
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

	void clear() {
		for (uint32_t& i : buckets) {
			i = 0;
		}
	}
	void writeToLog();

	std::string name() { return generateName(this->group, this->op); }

	std::string const group;
	std::string const op;
	Unit const unit;
	HistogramRegistry& registry;
	uint32_t buckets[32];
};

#endif // FLOW_HISTOGRAM_H