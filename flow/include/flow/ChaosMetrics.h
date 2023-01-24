/*
 * ChaosMetrics.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

// Chaos Metrics - We periodically log chaosMetrics to make sure that chaos events are happening
// Only includes DiskDelays which encapsulates all type delays and BitFlips for now
// Expand as per need
struct ChaosMetrics {

	ChaosMetrics();

	void clear();
	unsigned int diskDelays;
	unsigned int bitFlips;
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

#endif // FLOW_CHAOSMETRICS_H
