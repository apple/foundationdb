/*
 * OTELMetrics.h
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

#include "flow/flow.h"
#include <sys/_types/_int64_t.h>
#include <vector>

class Attribute {
private:
	std::string key;
	std::string value;

public:
	Attribute(const std::string& k, const std::string& v) : key{ k }, value{ v } {}
	Attribute(std::string&& k, std::string&& v) : key{ std::move(k) }, value{ std::move(v) } {}
};

enum AggregationTemporality {
	AGGREGATION_TEMPORALITY_UNSPECIFIED = 0,
	AGGREGATION_TEMPORALITY_DELTA,
	AGGREGATION_TEMPORALITY_CUMULATIVE
};

enum DataPointFlags { FLAG_NONE = 0, FLAG_NO_RECORDED_VALUE };

// TODO: Enforce that T should be one of (double, uint).
//       This is possible in C++20 with concepts
class NumberDataPoint {
public:
	double startTime;
	double recordTime;
	std::vector<Attribute> attributes;
	int64_t val;
	DataPointFlags flags;

	NumberDataPoint() : startTime{ now() }, flags{ DataPointFlags::FLAG_NONE } {}

	NumberDataPoint& addAttribute(const std::string& key, const std::string& value) {
		attributes.push_back(Attribute(key, value));
		return *this;
	}
};

class OTELSum {
public:
	std::vector<NumberDataPoint> points;
	AggregationTemporality aggregation;
	bool isMonotonic;
};

class OTELGauge {
public:
	std::vector<NumberDataPoint> points;
	OTELGauge() {}
};

class HistogramDataPoint {
public:
	std::vector<Attribute> attributes;
	double startTime;
	double recordTime;
	uint64_t count;
	double sum;
	double min;
	double max;
	std::vector<double> samples;
	DataPointFlags flags;
	HistogramDataPoint()
	  : startTime{ now() }, count{ 0 }, sum{ 0 }, min{ 0 }, max{ 0 }, flags{ DataPointFlags::FLAG_NONE } {}
};

class OTELHistogram {
public:
	std::vector<HistogramDataPoint> points;
	AggregationTemporality aggregation;
	OTELHistogram() {}
};
