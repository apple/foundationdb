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
#include "flow/Error.h"

#if (!(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__))
#error Do not support non-little-endian systems
#endif

#define FASTLOG 1
// This significantly reduces the time for log

#if FASTLOG
struct fastLogger {
	// Basically, the goal is to compute log(x)/log(r).
	// For double, it is represented as 2^e(1+s) (0<=s<1), so our goal becomes e*log(2)/log(r)*log(1+s),
	// and we approximate log(1+s) with a cubic function. See more details on DataDog's paper, or
	// CubicallyInterpolatedMapping.java in https://github.com/DataDog/sketches-java/
	constexpr const static double correctingFactor = 1.00988652862227438516; // = 7 / (10 * log(2));
	constexpr static const double A = 6.0 / 35.0, B = -3.0 / 5.0, C = 10.0 / 7.0;
	static const int SIGNIFICAND_WIDTH = 53;
	static const uint64_t SIGNIFICAND_MASK = 0x000fffffffffffffuLL;
	static const uint64_t EXPONENT_MASK = 0x7FF0000000000000uLL;
	constexpr static const int EXPONENT_SHIFT = SIGNIFICAND_WIDTH - 1;
	static const int EXPONENT_BIAS = 1023;
	static const uint64_t ONE = 0x3ff0000000000000uLL;
	static inline uint64_t doubleToLongBits(double x) {
		uint64_t u;
		uint64_t* pu = reinterpret_cast<uint64_t*>(&x);
		u = *pu;
		return u;
	}
	static inline double longBitsToDouble(uint64_t x) {
		double u;
		double* pu = reinterpret_cast<double*>(&x);
		u = *pu;
		return u;
	}
	static int getExponent(uint64_t longBits) {
		return (int)((longBits & EXPONENT_MASK) >> EXPONENT_SHIFT) - EXPONENT_BIAS;
	}

	static double getSignificandPlusOne(uint64_t longBits) {
		return longBitsToDouble((longBits & SIGNIFICAND_MASK) | ONE);
	}
	static double fastlog(double value) {
		uint64_t longBits = doubleToLongBits(value);
		double s = getSignificandPlusOne(longBits) - 1;
		double e = getExponent(longBits);
		return ((A * s + B) * s + C) * s + e;
	}
};
#endif

// A DDSketch for non-negative numbers
// (those < 10^-18 are treated as 0, and huge numbers (>>10^18) may degrade the performance)
template <class T>
class DDSketch {
public:
	explicit DDSketch(double errorGuarantee = 0.1)
	  : errorGuarantee(errorGuarantee), gamma((1.0 + errorGuarantee) / (1.0 - errorGuarantee)), logGamma(log(gamma)),
	    multiplier(FASTLOG ? fastLogger::correctingFactor * log(2) / log(1 + 2 * errorGuarantee / (1 - errorGuarantee))
	                       : 1),
	    offset((FASTLOG ? fastLogger::fastlog(1 / EPS) * multiplier : log(1 / EPS)) + 5), populationSize(0),
	    zeroPopulationSize(0), _min(T()), _max(T()), sum(T()) {

		buckets.resize(2 * offset, 0);
#if FASTLOG
		ASSERT(sizeof(double) == sizeof(uint64_t));
#endif
	}

	DDSketch<T>& addSample(T sample) {
		// Call it addSample for now, while it is not a sample anymore
		if (!populationSize) _min = _max = sample;

		if (sample <= EPS) {
			zeroPopulationSize++;
		} else {
#if FASTLOG
			int index = ceil(fastLogger::fastlog(sample) * multiplier);
#else
			int index = ceil(log(sample) / logGamma);
#endif
			ASSERT(index + offset >= 0);
			if (index + offset >= buckets.size()) {
				buckets.resize(index + offset + 1);
			}
			buckets[index + offset]++;
		}

		populationSize++;
		sum += sample;
		_max = std::max(_max, sample);
		_min = std::min(_min, sample);
		return *this;
	}

	double mean() const {
		if (populationSize == 0) return 0;
		return (double)sum / populationSize;
	}

	T median() { return percentile(0.5); }

	T percentile(double percentile) { // Find the targetPercentilePopulation-th element
		if (percentile < 0.0 || percentile > 1.0 || populationSize == 0) return T();
		uint64_t count = zeroPopulationSize;
		uint64_t targetPercentilePopulation = percentile * (populationSize - 1);
		if (targetPercentilePopulation < zeroPopulationSize) {
			// Zeros are enough
			return (T)EPS;
		}
		for (size_t i = 0; i < buckets.size(); i++) {
			if (targetPercentilePopulation < count + buckets[i]) {
#if FASTLOG
				return (T)(2.0 * pow(gamma, (i - offset) / fastLogger::correctingFactor) / (gamma + 1));
#else
				return (T)(2.0 * pow(gamma, (i - offset)) / (gamma + 1));
#endif
			}
			count += buckets[i];
		}
		UNREACHABLE(); // Should not be here
		return T();
	}

	T min() const { return _min; }
	T max() const { return _max; }

	void clear() {
		std::fill(buckets.begin(), buckets.end(), 0);
		buckets.resize(2 * offset);
		populationSize = zeroPopulationSize = 0;
		sum = _min = _max = 0; // Doesn't work for all T
	}

	uint64_t getPopulationSize() const { return populationSize; }

	double getErrorGurantee() const { return errorGuarantee; }

	DDSketch<T>& mergeWith(const DDSketch<T>& anotherSketch) {
		// Does not work if one use FASTLOG and the other did not
		// Must have the same guarantee
		ASSERT(fabs(errorGuarantee - anotherSketch.gerrorGuarantee) < EPS);
		buckets.resize(std::max(buckets.size(), anotherSketch.buckets.size()));
		for (size_t i = 0; i < anotherSketch.buckets.size(); i++) {
			buckets[i] += anotherSketch.buckets[i];
		}
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
	T _min, _max, sum;
};

#endif
