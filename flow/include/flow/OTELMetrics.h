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
#ifndef FLOW_OTELMETRIC_H
#define FLOW_OTELMETRIC_H
#include "flow/flow.h"
#include <vector>

class Attribute {
public:
	std::string key;
	std::string value;

	Attribute(const std::string& k, const std::string& v) : key{ k }, value{ v } {}
	Attribute(std::string&& k, std::string&& v) : key{ std::move(k) }, value{ std::move(v) } {}
};

enum AggregationTemporality {
	AGGREGATION_TEMPORALITY_UNSPECIFIED = 0,
	AGGREGATION_TEMPORALITY_DELTA,
	AGGREGATION_TEMPORALITY_CUMULATIVE
};

enum DataPointFlags { FLAG_NONE = 0, FLAG_NO_RECORDED_VALUE };

class NumberDataPoint {
public:
	double startTime;
	double recordTime;
	std::vector<Attribute> attributes;
	int64_t val;
	DataPointFlags flags;

	NumberDataPoint() : recordTime{ now() }, val{ 0 }, flags{ DataPointFlags::FLAG_NONE } {}

	NumberDataPoint(int64_t v) : recordTime{ now() }, val{ v }, flags{ DataPointFlags::FLAG_NONE } {}

	NumberDataPoint& addAttribute(const std::string& key, const std::string& value) {
		attributes.emplace_back(Attribute(key, value));
		return *this;
	}
};

enum OTELMetricType { Gauge = 0, Sum, Hist };

class OTELSum {
public:
	std::string name;
	NumberDataPoint point;
	AggregationTemporality aggregation;
	bool isMonotonic;
	OTELSum() : aggregation{ AGGREGATION_TEMPORALITY_DELTA }, isMonotonic{ true } {}
	OTELSum(const std::string& n) : name{ n }, aggregation{ AGGREGATION_TEMPORALITY_DELTA }, isMonotonic{ true } {}
	OTELSum(const std::string& n, int64_t v)
	  : name{ n }, point{ v }, aggregation{ AGGREGATION_TEMPORALITY_DELTA }, isMonotonic{ true } {}
};

class OTELGauge {
public:
	std::string name;
	NumberDataPoint point;
	OTELGauge() {}
	OTELGauge(const std::string& n) : name{ n } {}
};

class HistogramDataPoint {
public:
	std::vector<Attribute> attributes;
	double startTime;
	const std::vector<double> samples;
	double recordTime;
	uint64_t count;
	double sum;
	double min;
	double max;
	DataPointFlags flags;
	HistogramDataPoint(const std::vector<double>& s, double _min, double _max, double _sum)
	  : recordTime{ now() }, samples{ s }, count{ samples.size() }, min{ _min }, max{ _max }, sum{ _sum }, flags{
		    DataPointFlags::FLAG_NONE
	    } {}
	HistogramDataPoint& addAttribute(const std::string& key, const std::string& value) {
		attributes.emplace_back(Attribute(key, value));
		return *this;
	}
};

class OTELHistogram {
public:
	std::string name;
	HistogramDataPoint point;
	AggregationTemporality aggregation;
	OTELHistogram(const std::string& n, const std::vector<double>& s, double min, double max, double sum)
	  : name{ n }, point{ s, min, max, sum }, aggregation{ AGGREGATION_TEMPORALITY_DELTA } {}
};
#endif
