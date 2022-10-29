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
#include "flow/Msgpack.h"
#include <vector>

/*
    The following classes are based off of the OTEL protobuf definitions for metrics:
    NumberDataPoint
    HistogramDataPoint
    OTELSum
    OTELGauge
    OTELHistogram

    Since Counters in FDB always use int64_t as the underlying type (see ICounter impl)
    we choose to not cover the version of OTELSum which uses double

    Furthermore, we also diverge from the protobuf definition of HistogramDataPoint by using DDSketch.
    This means that that there is an additional field for storing the errorGuarantee (a double). Also, to save some
   space the buckets are uint32_t instead of uint64_t. The reason for this is due to the fact that it is highly unlikely
   that a single bucket would hit it's threshold with the default error guarantee of 1%.

    The receiver will sign extend the buckets to uint64_t upon receiving a HistogramDataPoint.

    See https://github.com/open-telemetry/opentelemetry-proto/blob/main/opentelemetry/proto/metrics/v1/metrics.proto
    for more details on the protobuf definitions
*/

namespace OTEL {
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
	std::vector<NumberDataPoint> points;
	AggregationTemporality aggregation;
	bool isMonotonic;
	OTELSum() : aggregation{ AGGREGATION_TEMPORALITY_DELTA }, isMonotonic{ true } {}
	OTELSum(const std::string& n) : name{ n }, aggregation{ AGGREGATION_TEMPORALITY_DELTA }, isMonotonic{ true } {}
	OTELSum(const std::string& n, int64_t v)
	  : name{ n }, aggregation{ AGGREGATION_TEMPORALITY_DELTA }, isMonotonic{ true } {
		points.emplace_back(v);
	}
};

class OTELGauge {
public:
	std::string name;
	std::vector<NumberDataPoint> points;
	OTELGauge() {}
	OTELGauge(const std::string& n) : name{ n } {}
};

class HistogramDataPoint {
public:
	double errorGuarantee;
	std::vector<Attribute> attributes;
	double startTime;
	const std::vector<uint64_t> buckets;
	double recordTime;
	uint64_t count;
	double sum;
	double min;
	double max;
	DataPointFlags flags;
	HistogramDataPoint(double error, const std::vector<uint64_t>& s, double _min, double _max, double _sum)
	  : errorGuarantee(error), recordTime{ now() }, buckets{ s }, count{ buckets.size() }, min{ _min }, max{ _max },
	    sum{ _sum }, flags{ DataPointFlags::FLAG_NONE } {}
	HistogramDataPoint& addAttribute(const std::string& key, const std::string& value) {
		attributes.emplace_back(Attribute(key, value));
		return *this;
	}
};

class OTELHistogram {
public:
	std::string name;
	std::vector<HistogramDataPoint> points;
	AggregationTemporality aggregation;
	OTELHistogram() {}
	OTELHistogram(const std::string& n,
	              double error,
	              const std::vector<uint64_t>& s,
	              double min,
	              double max,
	              double sum)
	  : name{ n }, aggregation{ AGGREGATION_TEMPORALITY_DELTA } {
		points.emplace_back(error, s, min, max, sum);
	}
};

inline void serialize(const Attribute& attr, MsgpackBuffer& buf) {
	serialize_string(attr.key, buf);
	serialize_string(attr.value, buf);
}

inline void serialize(const NumberDataPoint& point, MsgpackBuffer& buf) {
	serialize_value(point.startTime, buf, 0xcb);
	serialize_value(point.recordTime, buf, 0xcb);
	typedef void (*func_ptr)(const Attribute&, MsgpackBuffer&);
	func_ptr f = serialize;
	serialize_vector(point.attributes, buf, f);
	serialize_value(point.val, buf, 0xd3);
	serialize_value<uint8_t>(point.flags, buf, 0xcc);
}

inline void serialize(const OTELSum& sum, MsgpackBuffer& buf) {
	serialize_string(sum.name, buf);
	typedef void (*func_ptr)(const NumberDataPoint&, MsgpackBuffer&);
	func_ptr f = OTEL::serialize;
	serialize_vector(sum.points, buf, f);
	serialize_value<uint8_t>(sum.aggregation, buf, 0xcc);
	serialize_bool(sum.isMonotonic, buf);
}

inline void serialize(const OTELGauge& g, MsgpackBuffer& buf) {
	serialize_string(g.name, buf);
	typedef void (*func_ptr)(const NumberDataPoint&, MsgpackBuffer&);
	func_ptr f = OTEL::serialize;
	serialize_vector(g.points, buf, f);
}

inline void serialize(const HistogramDataPoint& point, MsgpackBuffer& buf) {
	typedef void (*func_ptr)(const Attribute&, MsgpackBuffer&);
	func_ptr f = serialize;
	serialize_value(point.errorGuarantee, buf, 0xcb);
	serialize_vector(point.attributes, buf, f);
	serialize_value(point.startTime, buf, 0xcb);
	serialize_value(point.recordTime, buf, 0xcb);
	serialize_value(point.count, buf, 0xcf);
	serialize_value(point.sum, buf, 0xcb);
	serialize_value(point.min, buf, 0xcb);
	serialize_value(point.max, buf, 0xcb);
	auto f_Bucket = [](const uint64_t& d, MsgpackBuffer& buf) { serialize_value(d, buf, 0xcf); };
	serialize_vector(point.buckets, buf, f_Bucket);
	serialize_value<uint8_t>(point.flags, buf, 0xcc);
}

inline void serialize(const OTELHistogram& h, MsgpackBuffer& buf) {
	serialize_string(h.name, buf);
	typedef void (*func_ptr)(const HistogramDataPoint&, MsgpackBuffer&);
	func_ptr f = OTEL::serialize;
	serialize_vector(h.points, buf, f);
	serialize_value<uint8_t>(h.aggregation, buf, 0xcc);
}
} // namespace OTEL
#endif
