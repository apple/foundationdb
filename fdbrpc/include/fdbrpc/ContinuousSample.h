/*
 * ContinuousSample.h
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

#ifndef CONTINUOUSSAMPLE_H
#define CONTINUOUSSAMPLE_H
#pragma once

#include "flow/Platform.h"
#include "flow/IRandom.h"
#include <vector>
#include <algorithm>
#include <cmath>

template <class T>
class ContinuousSample {
public:
	explicit ContinuousSample(int sampleSize)
	  : sampleSize(sampleSize), populationSize(0), sorted(true), _min(T()), _max(T()) {}

	ContinuousSample<T>& addSample(T sample) {
		if (!populationSize)
			_min = _max = sample;
		populationSize++;
		sorted = false;

		if (populationSize <= sampleSize) {
			samples.push_back(sample);
		} else if (deterministicRandom()->random01() < ((double)sampleSize / populationSize)) {
			samples[deterministicRandom()->randomInt(0, sampleSize)] = sample;
		}

		_max = std::max(_max, sample);
		_min = std::min(_min, sample);
		return *this;
	}

	double mean() const {
		if (!samples.size())
			return 0;
		T sum = 0;
		for (int c = 0; c < samples.size(); c++)
			sum += samples[c];
		return (double)sum / samples.size();
	}

	T median() { return percentile(0.5); }

	// Percentile (X) is the smallest element in the sample set at least as large as X% of the samples.
	T percentile(double percentile) {
		if (!samples.size() || percentile < 0.0 || percentile > 1.0)
			return T();
		sort();
		int idx = std::max<int>(0, std::ceil(samples.size() * percentile) - 1);
		return samples[idx];
	}

	T min() const { return _min; }
	T max() const { return _max; }

	void clear() {
		samples.clear();
		populationSize = 0;
		sorted = true;
		_min = _max = 0; // Doesn't work for all T
	}

	uint64_t getPopulationSize() const { return populationSize; }

private:
	int sampleSize;
	uint64_t populationSize;
	bool sorted;
	std::vector<T> samples;
	T _min, _max;

	void sort() {
		if (!sorted && samples.size() > 1)
			std::sort(samples.begin(), samples.end());
		sorted = true;
	}
};

#endif
