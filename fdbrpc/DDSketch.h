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
#pragma once

#include <vector>
#include <algorithm>
#include <cmath>

#if (!(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__))
#error Do not support non-little-endian systems
#endif

#define FASTLOG 1
// This significantly reduces the time for log

#if FASTLOG
namespace fastLogger {
// Basically, the goal is to compute log(x)/log(r).
// For double, it is represented as 2^e*(1+s) (0<=s<1), so our goal becomes e*log(2)/log(r)*log(1+s),
// and we approximate log(1+s) with a cubic function. See more details on Datadog's paper, or
// CubicallyInterpolatedMapping.java in https://github.com/DataDog/sketches-java/
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
#endif

// A DDSketch for non-negative numbers
// (those < 10^-18 are treated as 0, and huge numbers (>>10^18) fail ASSERT)
template <class T>
class DDSketch {
public:
	explicit DDSketch(double errorGuarantee = 0.1)
	  : errorGuarantee(errorGuarantee), gamma((1.0 + errorGuarantee) / (1.0 - errorGuarantee)), logGamma(log(gamma)),
	    multiplier(FASTLOG ? fastLogger::correctingFactor * log(2) / log(1 + 2 * errorGuarantee / (1 - errorGuarantee))
	                       : 1),
	    offset((FASTLOG ? fastLogger::fastlog(1 / EPS) * multiplier : log(1 / EPS)) + 5), populationSize(0),
	    zeroPopulationSize(0), minValue(T()), maxValue(T()), sum(T()) {
		buckets.resize(2 * offset, 0);
	}

	DDSketch<T>& addSample(T sample) {
		// Call it addSample for now, while it is not a sample anymore
		if (!populationSize) minValue = maxValue = sample;

		if (sample <= EPS) {
			zeroPopulationSize++;
		} else {
#if FASTLOG
			int index = ceil(fastLogger::fastlog(sample) * multiplier);
#else
			int index = ceil(log(sample) / logGamma);
#endif
			ASSERT(index + offset >= 0 && index + offset < buckets.size());
			buckets[index + offset]++;
		}

		populationSize++;
		sum += sample;
		maxValue = std::max(maxValue, sample);
		minValue = std::min(minValue, sample);
		return *this;
	}

	double mean() const {
		if (populationSize == 0) return 0;
		return (double)sum / populationSize;
	}

	T median() { return percentile(0.5); }

	T percentile(double percentile) { // Find the tPP-th (0-indexed) element
		ASSERT(percentile >= 0 && percentile <= 1);

		if (populationSize == 0) return T();
		uint64_t targetPercentilePopulation = percentile * (populationSize - 1);
		if (targetPercentilePopulation < zeroPopulationSize) return T(0);

		int index = -1;
		bool found = false;
		if (percentile <= 0.5) { // count up
			uint64_t count = zeroPopulationSize;
			for (size_t i = 0; i < buckets.size(); i++) {
				if (targetPercentilePopulation < count + buckets[i]) {
					// count + buckets[i] = # of numbers so far (from the rightmost to this
					// bucket, inclusive), so if target is in this bucket, it should means tPP < cnt + bck[i]
					found = true;
					index = i;
					break;
				}
				count += buckets[i];
			}
		} else { // and count down
			uint64_t count = 0;
			for (size_t i = buckets.size(); i >= 0; i--) {
				if (targetPercentilePopulation + count + buckets[i] >= populationSize) {
					// cnt + bkt[i] is # of numbers to the right of this bucket (incl.),
					// so if target is not in this bucket (i.e., to the left of this bucket),
					// it would be as right as the left bucket's rightmost number, so
					// we would have tPP + cnt + bkt[i] < total population (tPP is 0-indexed),
					// that means target is in this bucket if this condition is not satisfied.
					found = true;
					index = i;
					break;
				}
				count += buckets[i];
			}
		}
		ASSERT(found);
#if FASTLOG
		return fastLogger::reverseLog((index - offset) / multiplier) * 2.0 / (1 + gamma);
#else
		return (T)(2.0 * pow(gamma, (index - offset)) / (gamma + 1));
#endif
	}

	T min() const { return minValue; }
	T max() const { return maxValue; }

	void clear() {
		std::fill(buckets.begin(), buckets.end(), 0);
		populationSize = zeroPopulationSize = 0;
		sum = minValue = maxValue = 0; // Doesn't work for all T
	}

	uint64_t getPopulationSize() const { return populationSize; }

	double getErrorGurantee() const { return errorGuarantee; }

	DDSketch<T>& mergeWith(const DDSketch<T>& anotherSketch) {
		// Does not work if one use FASTLOG and the other did not
		// Must have the same guarantee
		ASSERT(errorGuarantee - anotherSketch.gerrorGuarantee < EPS &&
		       anotherSketch.gerrorGuarantee - errorGuarantee < EPS && anotherSketch.buckets.size() == buckets.size());
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

	// Need to implement a ser/deser method before we can send this over network and merge

private:
	double EPS = 1e-18; // incoming data smaller than this number are considered as 0
	double errorGuarantee, gamma, logGamma; // As defined in the paper
	double multiplier; // Calibration for FASTLOG
	int offset; // For buckets[i] we use buckets[i + offset] instead since i coule be negative

	uint64_t populationSize, zeroPopulationSize; // we need to separately count 0s
	std::vector<uint64_t> buckets;
	T minValue, maxValue, sum;
};

#endif

TEST_CASE("/fdbrpc/ddsketch/accuracy") {

	int TRY = 100, SIZE = 1e6;
	const int totalPercentiles = 7;
	double targetPercentiles[totalPercentiles] = { .0001, .01, .1, .50, .90, .99, .9999 };
	double stat[totalPercentiles];
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
