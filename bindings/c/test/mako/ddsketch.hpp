#ifndef MAKO_DDSKETCH_HPP
#define MAKO_DDSKETCH_HPP

#include <array>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <limits>
#include <list>
#include <new>
#include <utility>
#include "fdbclient/rapidjson/document.h"
#include "fdbclient/rapidjson/rapidjson.h"
#include "fdbclient/rapidjson/stringbuffer.h"
#include "fdbclient/rapidjson/writer.h"
#include "operations.hpp"
namespace mako {
/*
    Collect sampled latencies according to DDSketch paper:
    https://arxiv.org/pdf/1908.10693.pdf
 */
class DDSketch {
private:
	double errorGuarantee;
	std::vector<uint64_t> buckets;
	uint64_t minValue, maxValue, populationSize, zeroPopulationSize;
	double gamma;
	int offset;
	constexpr static double EPSILON = 1e-18;

	int getIdx(uint64_t sample) const noexcept { return ceil(log(sample) / log(gamma)); }

	double getVal(int idx) const noexcept { return (2.0 * pow(gamma, idx)) / (1 + gamma); }

public:
	DDSketch(double err = 0.05)
	  : errorGuarantee(err), minValue(std::numeric_limits<uint64_t>::max()),
	    maxValue(std::numeric_limits<uint64_t>::min()), populationSize(0), zeroPopulationSize(0),
	    gamma((1.0 + errorGuarantee) / (1.0 - errorGuarantee)), offset(getIdx(1.0 / EPSILON)) {
		buckets.resize(2 * offset, 0);
	}

	uint64_t getPopulationSize() { return populationSize; }

	void add(uint64_t sample) {
		if (sample <= EPSILON) {
			zeroPopulationSize++;
		} else {
			int idx = getIdx(sample);
			assert(idx >= 0 && idx < int(buckets.size()));
			buckets[idx]++;
		}
		populationSize++;
		maxValue = std::max(maxValue, sample);
		minValue = std::min(minValue, sample);
	}

	double percentile(double percentile) {
		assert(percentile >= 0 && percentile <= 1);

		if (populationSize == 0) {
			return 0;
		}
		uint64_t targetPercentilePopulation = percentile * (populationSize - 1);
		// Now find the tPP-th (0-indexed) element
		if (targetPercentilePopulation < zeroPopulationSize) {
			return 0;
		}

		int index = -1;
		bool found = false;
		if (percentile <= 0.5) { // count up
			uint64_t count = zeroPopulationSize;
			for (size_t i = 0; i < buckets.size(); i++) {
				if (targetPercentilePopulation < count + buckets[i]) {
					// count + buckets[i] = # of numbers so far (from the rightmost to
					// this bucket, inclusive), so if target is in this bucket, it should
					// means tPP < cnt + bck[i]
					found = true;
					index = i;
					break;
				}
				count += buckets[i];
			}
		} else { // and count down
			uint64_t count = 0;
			for (size_t i = buckets.size() - 1; i >= 0; i--) {
				if (targetPercentilePopulation + count + buckets[i] >= populationSize) {
					// cnt + bkt[i] is # of numbers to the right of this bucket (incl.),
					// so if target is not in this bucket (i.e., to the left of this
					// bucket), it would be as right as the left bucket's rightmost
					// number, so we would have tPP + cnt + bkt[i] < total population (tPP
					// is 0-indexed), that means target is in this bucket if this
					// condition is not satisfied.
					found = true;
					index = i;
					break;
				}
				count += buckets[i];
			}
		}
		assert(found);
		return getVal(index);
	}

	uint64_t min() const { return minValue; }
	uint64_t max() const { return maxValue; }

	void serialize(rapidjson::Writer<rapidjson::StringBuffer>& writer) const {
		writer.StartObject();

		writer.String("errorGuarantee");
		writer.Double(errorGuarantee);
		writer.String("minValue");
		writer.Uint64(minValue);
		writer.String("maxValue");
		writer.Uint64(maxValue);
		writer.String("populationSize");
		writer.Uint64(populationSize);
		writer.String("zeroPopulationSize");
		writer.Uint64(zeroPopulationSize);
		writer.String("gamma");
		writer.Double(gamma);
		writer.String("offset");
		writer.Int(offset);
		writer.String("buckets");
		writer.StartArray();
		for (auto b : buckets) {
			writer.Uint64(b);
		}
		writer.EndArray();

		writer.EndObject();
	}

	void deserialize(const rapidjson::Value& obj) {
		errorGuarantee = obj["errorGuarantee"].GetDouble();
		minValue = obj["minValue"].GetUint64();
		maxValue = obj["maxValue"].GetUint64();
		populationSize = obj["populationSize"].GetUint64();
		zeroPopulationSize = obj["zeroPopulationSize"].GetUint64();
		gamma = obj["gamma"].GetDouble();
		offset = obj["offset"].GetInt();
		auto jsonBuckets = obj["buckets"].GetArray();
		uint64_t idx = 0;
		for (auto it = jsonBuckets.Begin(); it != jsonBuckets.End(); it++) {
			buckets[idx] = it->GetUint64();
			idx++;
		}
	}

	void merge(const DDSketch& other) {
		// what to do if we have different errorGurantees?
		maxValue = std::max(maxValue, other.maxValue);
		minValue = std::min(minValue, other.minValue);
		populationSize += other.populationSize;
		zeroPopulationSize += other.zeroPopulationSize;
		for (uint32_t i = 0; i < buckets.size(); i++) {
			buckets[i] += other.buckets[i];
		}
	}
};
} // namespace mako

#endif