/*
 * YCSBCommon.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#pragma once

#include <algorithm>
#include <cmath>
#include <memory>
#include <string>

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/Stats.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "flow/IRandom.h"

// singleCF: one KV per row, value is all fields concatenated (best case for an LSM)
// multiCF: one KV per (row, field), so reads are small range reads (best case for a btree)
enum YCSBVariant { YCSB_SINGLE_CF, YCSB_MULTI_CF };
enum YCSBDistribution { YCSB_UNIFORM_RANDOM, YCSB_ZIPFIAN };
enum YCSBOperation { YCSB_READ, YCSB_UPDATE };

// Zipfian distribution over [min, max], using the algorithm from YCSB's ZipfianGenerator (Gray et al., "Quickly
// Generating Billion-Record Synthetic Databases"). Draws from deterministicRandom() so simulation stays deterministic,
// and holds no per-draw state, so one generator can be shared by any number of actors.
class YCSBZipfianGenerator {
public:
	YCSBZipfianGenerator(int64_t min, int64_t max, double theta)
	  : base_(min), items_(max - min + 1), theta_(theta), alpha_(1.0 / (1.0 - theta)) {
		ASSERT(items_ > 0);
		ASSERT(theta > 0 && theta < 1);
		double const zeta2Theta = zeta(2, theta);
		zetaN_ = zeta(items_, theta);
		eta_ = (1 - std::pow(2.0 / items_, 1 - theta)) / (1 - zeta2Theta / zetaN_);
	}

	int64_t next() const {
		double const u = deterministicRandom()->random01();
		double const uz = u * zetaN_;
		if (uz < 1.0) {
			return base_;
		}
		if (uz < 1.0 + std::pow(0.5, theta_)) {
			return base_ + 1;
		}
		auto const offset = static_cast<int64_t>(items_ * std::pow(eta_ * u - eta_ + 1, alpha_));
		return base_ + std::min(offset, items_ - 1);
	}

private:
	static double zeta(int64_t n, double theta) {
		double sum = 0;
		for (int64_t i = 0; i < n; ++i) {
			sum += 1 / std::pow(i + 1, theta);
		}
		return sum;
	}

	int64_t base_;
	int64_t items_;
	double theta_;
	double alpha_;
	double zetaN_;
	double eta_;
};

// Shared options and key/value layout for the YCSB workloads. Rows are KVWorkload keys, so datasets that must not
// overlap (e.g. one per layout) are kept apart with distinct nodePrefix values.
struct YCSBBase : KVWorkload {
	YCSBVariant variant{ YCSB_SINGLE_CF };
	YCSBDistribution distribution{ YCSB_UNIFORM_RANDOM };
	int fieldCount{ 0 };
	int readWeight{ 0 };
	int updateWeight{ 0 };
	double zipfConstant{ 0.0 };
	double latencyWindowSeconds{ 0.0 };

	// Per-window distributions so warmup and interference events show up over time. Only live while the measured
	// phase runs, so setup and teardown don't emit empty windows.
	std::unique_ptr<LatencySample> windowLatencies, windowReadLatencies, windowUpdateLatencies;

	explicit YCSBBase(WorkloadContext const& wcx) : KVWorkload(wcx) {
		std::string const variantStr = getOption(options, "variant"_sr, "singleCF"_sr).toString();
		ASSERT(variantStr == "singleCF" || variantStr == "multiCF");
		variant = variantStr == "multiCF" ? YCSB_MULTI_CF : YCSB_SINGLE_CF;
		fieldCount = getOption(options, "fieldCount"_sr, 10);
		// field suffix is two decimal digits
		ASSERT(fieldCount >= 1 && fieldCount <= 100);
		// valueBytes is the total row size in both variants so they store comparable data
		ASSERT(variant == YCSB_SINGLE_CF || minValueBytes >= fieldCount);

		std::string const distStr = getOption(options, "distribution"_sr, "zipfian"_sr).toString();
		ASSERT(distStr == "zipfian" || distStr == "random");
		distribution = distStr == "zipfian" ? YCSB_ZIPFIAN : YCSB_UNIFORM_RANDOM;
		zipfConstant = getOption(options, "zipfConstant"_sr, 0.99);

		readWeight = getOption(options, "readWeight"_sr, 80);
		updateWeight = getOption(options, "updateWeight"_sr, 20);
		ASSERT_EQ(readWeight + updateWeight, 100);

		latencyWindowSeconds = getOption(options, "latencyWindowSeconds"_sr, 5.0);
		ASSERT(latencyWindowSeconds > 0);
	}

	std::unique_ptr<LatencySample> makeWindowSample(std::string const& name, UID const& id) const {
		return std::make_unique<LatencySample>(name, id, latencyWindowSeconds, SERVER_KNOBS->LATENCY_SKETCH_ACCURACY);
	}

	void startLatencyWindows(std::string const& prefix, UID const& id) {
		windowLatencies = makeWindowSample(prefix + "TotalWindowLatency", id);
		if (readWeight > 0) {
			windowReadLatencies = makeWindowSample(prefix + "ReadWindowLatency", id);
		}
		if (updateWeight > 0) {
			windowUpdateLatencies = makeWindowSample(prefix + "UpdateWindowLatency", id);
		}
	}

	void stopLatencyWindows() {
		windowLatencies.reset();
		windowReadLatencies.reset();
		windowUpdateLatencies.reset();
	}

	void addWindowLatency(YCSBOperation op, double latency) {
		windowLatencies->addMeasurement(latency);
		(op == YCSB_READ ? windowReadLatencies : windowUpdateLatencies)->addMeasurement(latency);
	}

	// Number of KVs in the dataset: each multiCF row is fieldCount KVs
	uint64_t kvCount() const { return variant == YCSB_MULTI_CF ? nodeCount * fieldCount : nodeCount; }

	Key rowKey(uint64_t row) const { return KVWorkload::keyForIndex(row, false); }
	Key fieldKey(uint64_t row, int field) const { return rowKey(row).withSuffix(format("%02d", field)); }

	// Key of the n-th of kvCount() KVs; hides KVWorkload::keyForIndex so bulkSetup sees the YCSB layout
	Key keyForIndex(uint64_t n) const {
		if (variant == YCSB_MULTI_CF) {
			return fieldKey(n / fieldCount, n % fieldCount);
		}
		return rowKey(n);
	}

	// For bulkSetup
	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n), randomValue()); }

	Value randomValue() const override {
		int size = deterministicRandom()->randomInt(minValueBytes, maxValueBytes + 1);
		if (variant == YCSB_MULTI_CF) {
			size /= fieldCount;
		}
		return Value(deterministicRandom()->randomAlphaNumeric(size));
	}
};
