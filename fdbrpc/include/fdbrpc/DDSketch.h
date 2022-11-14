/*
 * DDSketch.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef DDSKETCH_H
#define DDSKETCH_H
#include <iterator>
#include <limits>
#include <type_traits>
#pragma once

#include <vector>
#include <algorithm>
#include <cassert>
#include <cmath>
#include "flow/Error.h"
#include "flow/UnitTest.h"

// A namespace for fast log() computation.
namespace fastLogger {
// Basically, the goal is to compute log(x)/log(r).
// For double, it is represented as 2^e*(1+s) (0<=s<1), so our goal becomes
// e*log(2)/log(r)*log(1+s), and we approximate log(1+s) with a cubic function.
// See more details on Datadog's paper, or CubicallyInterpolatedMapping.java in
// https://github.com/DataDog/sketches-java/
inline const double correctingFactor = 1.00988652862227438516; // = 7 / (10 * log(2));
constexpr inline const double A = 6.0 / 35.0, B = -3.0 / 5.0, C = 10.0 / 7.0;

inline double fastlog(double value) {
	int e;
	double s = frexp(value, &e);
	s = s * 2 - 1;
	return ((A * s + B) * s + C) * s + e - 1;
}

inline double reverseLog(double index) {
	long exponent = floor(index);
	// Derived from Cardano's formula
	double d0 = B * B - 3 * A * C;
	double d1 = 2 * B * B * B - 9 * A * B * C - 27 * A * A * (index - exponent);
	double p = cbrt((d1 - sqrt(d1 * d1 - 4 * d0 * d0 * d0)) / 2);
	double significandPlusOne = -(B + p + d0 / p) / (3 * A) + 1;
	return ldexp(significandPlusOne / 2, exponent + 1);
}
}; // namespace fastLogger

// DDSketch for non-negative numbers (those < EPS = 10^-18 are
// treated as 0, and huge numbers (>1/EPS) fail ASSERT). This is the base
// class without a concrete log() implementation.
template <class Impl, class T>
class DDSketchBase {

	static constexpr T defaultMin() { return std::numeric_limits<T>::max(); }

	static constexpr T defaultMax() {
		if constexpr (std::is_floating_point_v<T>) {
			return -std::numeric_limits<T>::max();
		} else {
			return std::numeric_limits<T>::min();
		}
	}

public:
	explicit DDSketchBase(double errorGuarantee)
	  : errorGuarantee(errorGuarantee), populationSize(0), zeroPopulationSize(0), minValue(defaultMin()),
	    maxValue(defaultMax()), sum(T()) {}

	DDSketchBase<Impl, T>& addSample(T sample) {
		// Call it addSample for now, while it is not a sample anymore
		if (!populationSize)
			minValue = maxValue = sample;

		if (sample <= EPS) {
			zeroPopulationSize++;
		} else {
			size_t index = static_cast<Impl*>(this)->getIndex(sample);
			assert(index >= 0 && index < buckets.size());
			try {
				buckets.at(index)++;
			} catch (std::out_of_range const& e) {
				fmt::print(stderr, "ERROR: Invalid DDSketch bucket index ({}) at {}/{} for sample: {}\n", e.what(), index, buckets.size(), sample);
			}
		}

		populationSize++;
		sum += sample;
		maxValue = std::max(maxValue, sample);
		minValue = std::min(minValue, sample);
		return *this;
	}

	double mean() const {
		if (populationSize == 0)
			return 0;
		return (double)sum / populationSize;
	}

	T median() { return percentile(0.5); }

	T percentile(double percentile) {
		assert(percentile >= 0 && percentile <= 1);

		if (populationSize == 0)
			return T();
		uint64_t targetPercentilePopulation = percentile * (populationSize - 1);
		// Now find the tPP-th (0-indexed) element
		if (targetPercentilePopulation < zeroPopulationSize)
			return T(0);

		size_t index = 0;
		[[maybe_unused]] bool found = false;
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
			for (auto rit = buckets.rbegin(); rit != buckets.rend(); rit++) {
				if (targetPercentilePopulation + count + *rit >= populationSize) {
					// cnt + bkt[i] is # of numbers to the right of this bucket (incl.),
					// so if target is not in this bucket (i.e., to the left of this
					// bucket), it would be as right as the left bucket's rightmost
					// number, so we would have tPP + cnt + bkt[i] < total population (tPP
					// is 0-indexed), that means target is in this bucket if this
					// condition is not satisfied.
					found = true;
					index = std::distance(rit, buckets.rend()) - 1;
					break;
				}
				count += *rit;
			}
		}
		assert(found);
		if (!found) return -1;
		return static_cast<Impl*>(this)->getValue(index);
	}

	T min() const { return minValue; }
	T max() const { return maxValue; }

	void clear() {
		std::fill(buckets.begin(), buckets.end(), 0);
		populationSize = zeroPopulationSize = 0;
		sum = 0;
		minValue = defaultMin();
		maxValue = defaultMax();
	}

	uint64_t getPopulationSize() const { return populationSize; }

	double getErrorGuarantee() const { return errorGuarantee; }

	size_t getBucketSize() const { return buckets.size(); }

	DDSketchBase<Impl, T>& mergeWith(const DDSketchBase<Impl, T>& anotherSketch) {
		// Must have the same guarantee
		assert(fabs(errorGuarantee - anotherSketch.errorGuarantee) < EPS &&
		       anotherSketch.buckets.size() == buckets.size());
		for (size_t i = 0; i < anotherSketch.buckets.size(); i++) {
			buckets[i] += anotherSketch.buckets[i];
		}
		populationSize += anotherSketch.populationSize;
		zeroPopulationSize += anotherSketch.zeroPopulationSize;
		minValue = std::min(minValue, anotherSketch.minValue);
		maxValue = std::max(maxValue, anotherSketch.maxValue);
		sum += anotherSketch.sum;
		return *this;
	}

	constexpr static double EPS = 1e-18; // smaller numbers are considered as 0
protected:
	double errorGuarantee; // As defined in the paper

	uint64_t populationSize, zeroPopulationSize; // we need to separately count 0s
	std::vector<uint64_t> buckets;
	T minValue, maxValue, sum;
	void setBucketSize(size_t capacity) { buckets.resize(capacity, 0); }
};

// DDSketch with fast log implementation for float numbers
template <class T>
class DDSketch : public DDSketchBase<DDSketch<T>, T> {
public:
	explicit DDSketch(double errorGuarantee = 0.01)
	  : DDSketchBase<DDSketch<T>, T>(errorGuarantee), gamma((1.0 + errorGuarantee) / (1.0 - errorGuarantee)),
	    multiplier(fastLogger::correctingFactor * log(2) / log(gamma)) {
		assert(errorGuarantee > 0);
		offset = getIndex(1.0 / DDSketchBase<DDSketch<T>, T>::EPS);
		this->setBucketSize(2 * offset);
	}

	size_t getIndex(T sample) {
		static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "Do not support non-little-endian systems");
		return ceil(fastLogger::fastlog(sample) * multiplier) + offset;
	}

	T getValue(size_t index) { return fastLogger::reverseLog((index - offset) / multiplier) * 2.0 / (1 + gamma); }

private:
	double gamma, multiplier;
	size_t offset = 0;
};

// DDSketch with <cmath> log. Slow and only use this when others doesn't work.
template <class T>
class DDSketchSlow : public DDSketchBase<DDSketchSlow<T>, T> {
public:
	DDSketchSlow(double errorGuarantee = 0.1)
	  : DDSketchBase<DDSketchSlow<T>, T>(errorGuarantee), gamma((1.0 + errorGuarantee) / (1.0 - errorGuarantee)),
	    logGamma(log(gamma)) {
		offset = getIndex(1.0 / DDSketchBase<DDSketch<T>, T>::EPS) + 5;
		this->setBucketSize(2 * offset);
	}

	size_t getIndex(T sample) { return ceil(log(sample) / logGamma) + offset; }

	T getValue(size_t index) { return (T)(2.0 * pow(gamma, (index - offset)) / (1 + gamma)); }

private:
	double gamma, logGamma;
	size_t offset = 0;
};

// DDSketch for unsigned int. Faster than the float version. Fixed accuracy.
class DDSketchFastUnsigned : public DDSketchBase<DDSketchFastUnsigned, unsigned> {
public:
	DDSketchFastUnsigned() : DDSketchBase<DDSketchFastUnsigned, unsigned>(errorGuarantee) { this->setBucketSize(129); }

	size_t getIndex(unsigned sample) {
		__uint128_t v = sample;
		v *= v;
		v *= v; // sample^4
		uint64_t low = (uint64_t)v, high = (uint64_t)(v >> 64);

		return 128 - (high == 0 ? ((low == 0 ? 64 : __builtin_clzll(low)) + 64) : __builtin_clzll(high));
	}

	unsigned getValue(size_t index) {
		double r = 1, g = gamma;
		while (index) { // quick power method for power(gamma, index)
			if (index & 1)
				r *= g;
			g *= g;
			index >>= 1;
		}
		// 2.0 * pow(gamma, index) / (1 + gamma) is what we need
		return (unsigned)(2.0 * r / (1 + gamma) + 0.5); // round to nearest int
	}

private:
	constexpr static double errorGuarantee = 0.08642723372;
	// getIndex basically calc floor(log_2(x^4)) + 1,
	// which is almost ceil(log_2(x^4)) as it only matters when x is a power of 2,
	// and it does not change the error bound. Original sketch asks for
	// ceil(log_r(x)), so we know r = pow(2, 1/4) = 1.189207115. And r = (1 + eG)
	// / (1 - eG) so eG = 0.08642723372.
	constexpr static double gamma = 1.189207115;
};

#endif

TEST_CASE("/fdbrpc/ddsketch/accuracy") {

	int TRY = 100, SIZE = 1e6;
	const int totalPercentiles = 7;
	double targetPercentiles[totalPercentiles] = { .0001, .01, .1, .50, .90, .99, .9999 };
	double stat[totalPercentiles] = { 0 };
	for (int t = 0; t < TRY; t++) {
		DDSketch<double> dd;
		std::vector<double> nums;
		for (int i = 0; i < SIZE; i++) {
			static double a = 1, b = 1; // a skewed distribution
			auto y = deterministicRandom()->random01();
			auto num = b / pow(1 - y, 1 / a);
			nums.push_back(num);
			dd.addSample(num);
		}
		std::sort(nums.begin(), nums.end());
		for (int percentID = 0; percentID < totalPercentiles; percentID++) {
			double percentile = targetPercentiles[percentID];
			double ground = nums[percentile * (SIZE - 1)], ddvalue = dd.percentile(percentile);
			double relativeError = fabs(ground - ddvalue) / ground;
			stat[percentID] += relativeError;
		}
	}

	for (int percentID = 0; percentID < totalPercentiles; percentID++) {
		printf("%.4lf per, relative error %.4lf\n", targetPercentiles[percentID], stat[percentID] / TRY);
	}

	return Void();
}
