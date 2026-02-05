/*
 * ChaosMetrics.h
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

#ifndef FLOW_CHAOSMETRICS_H
#define FLOW_CHAOSMETRICS_H

struct TraceEvent;

// FIXME: simulation testing involves injection of a lot of chaos
// (e.g. from the description here: https://apple.github.io/foundationdb/testing.html).
// Explain how the specifics in this file differ from normal, background chaos
// that simulation already puts into the simulated environment.

// Chaos Metrics - We periodically log chaosMetrics to make sure that chaos events are happening
// Only includes DiskDelays which encapsulates all type delays and BitFlips for now
// Expand as per need
struct ChaosMetrics {

	ChaosMetrics();

	void clear();
	unsigned int diskDelays;
	unsigned int bitFlips;
	unsigned int s3Errors;
	unsigned int s3Throttles;
	unsigned int s3Delays;
	unsigned int s3Corruptions;
	double startTime;

	void getFields(TraceEvent* e);
};

// This class supports injecting two type of disk failures
// 1. Stalls: Every interval seconds, the disk will stall and no IO will complete for x seconds, where x is a randomly
// chosen interval
// 2. Slowdown: Random slowdown is injected to each disk operation for specified period of time
struct DiskFailureInjector {
	static DiskFailureInjector* injector();
	void setDiskFailure(double interval, double stallFor, double throttleFor);
	double getStallDelay() const;
	double getThrottleDelay() const;
	double getDiskDelay() const;

private: // members
	double stallInterval = 0.0; // how often should the disk be stalled (0 meaning once, 10 meaning every 10 secs)
	double stallPeriod; // Period of time disk stalls will be injected for
	double stallUntil; // End of disk stall period
	double stallDuration; // Duration of each stall
	double throttlePeriod; // Period of time the disk will be slowed down for
	double throttleUntil; // End of disk slowdown period

private: // construction
	DiskFailureInjector() = default;
	DiskFailureInjector(DiskFailureInjector const&) = delete;
};

struct BitFlipper {
	static BitFlipper* flipper();
	double getBitFlipPercentage() { return bitFlipPercentage; }

	void setBitFlipPercentage(double percentage) { bitFlipPercentage = percentage; }

private: // members
	double bitFlipPercentage = 0.0;

private: // construction
	BitFlipper() = default;
	BitFlipper(BitFlipper const&) = delete;
};

// S3 Fault Injector - Controls chaos injection rates for S3 operations
struct S3FaultInjector {
	static S3FaultInjector* injector();

	// Basic chaos rates (0.0-1.0)
	void setErrorRate(double rate) { errorRate = rate; }
	void setThrottleRate(double rate) { throttleRate = rate; }
	void setDelayRate(double rate) { delayRate = rate; }
	void setCorruptionRate(double rate) { corruptionRate = rate; }
	void setMaxDelay(double seconds) { maxDelay = seconds; }

	double getErrorRate() const { return errorRate; }
	double getThrottleRate() const { return throttleRate; }
	double getDelayRate() const { return delayRate; }
	double getCorruptionRate() const { return corruptionRate; }
	double getMaxDelay() const { return maxDelay; }

	// Operation-specific multipliers
	void setReadMultiplier(double mult) { readMultiplier = mult; }
	void setWriteMultiplier(double mult) { writeMultiplier = mult; }
	void setDeleteMultiplier(double mult) { deleteMultiplier = mult; }
	void setListMultiplier(double mult) { listMultiplier = mult; }

	double getReadMultiplier() const { return readMultiplier; }
	double getWriteMultiplier() const { return writeMultiplier; }
	double getDeleteMultiplier() const { return deleteMultiplier; }
	double getListMultiplier() const { return listMultiplier; }

private: // members
	double errorRate = 0.0;
	double throttleRate = 0.0;
	double delayRate = 0.0;
	double corruptionRate = 0.0;
	double maxDelay = 5.0;
	double readMultiplier = 1.0;
	double writeMultiplier = 1.0;
	double deleteMultiplier = 1.0;
	double listMultiplier = 1.0;

private: // construction
	S3FaultInjector() = default;
	S3FaultInjector(S3FaultInjector const&) = delete;
};

#endif // FLOW_CHAOSMETRICS_H
