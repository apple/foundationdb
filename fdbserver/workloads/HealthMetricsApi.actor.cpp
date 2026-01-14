/*
 * HealthMetricsApi.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// NOTE: it might be simpler to test health metrics via something
// other than simulation. Testing equivalent to what this workload does can
// seemingly be obtained by a straight line test case that does
// the following:
// a) start a cluster
// b) do a few transactions
// c) call getHealthMetrics()
// d) ensure the returned metrics are non-zero.

// This workload can be attached to other workload to collect health information about the FDB cluster.
struct HealthMetricsApiWorkload : TestWorkload {
	// Performance Metrics
	int64_t worstStorageQueue = 0;
	int64_t worstLimitingStorageQueue = 0;
	int64_t worstStorageDurabilityLag = 0;
	int64_t worstLimitingStorageDurabilityLag = 0;
	int64_t worstTLogQueue = 0;
	int64_t detailedWorstStorageQueue = 0;
	int64_t detailedWorstStorageDurabilityLag = 0;
	int64_t detailedWorstTLogQueue = 0;
	double detailedWorstCpuUsage = 0;
	double detailedWorstDiskUsage = 0;

	// Test configuration
	double testDuration;
	double healthMetricsCheckInterval;
	double maxAllowedStaleness;
	bool sendDetailedHealthMetrics;

	// internal states
	bool healthMetricsStoppedUpdating = false;
	bool gotMetrics = false;
	static constexpr auto NAME = "HealthMetricsApi";

	HealthMetricsApiWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		healthMetricsCheckInterval = getOption(options, "healthMetricsCheckInterval"_sr, 1.0);
		sendDetailedHealthMetrics = getOption(options, "sendDetailedHealthMetrics"_sr, true);
		maxAllowedStaleness = getOption(options, "maxAllowedStaleness"_sr, 60.0);
	}

	ACTOR static Future<Void> _setup(Database cx, HealthMetricsApiWorkload* self) {
		if (!self->sendDetailedHealthMetrics) {
			// Internally cached health metrics time out after this knob.  Wait
			// an extra second to avoid any off-by-1 ">" vs ">=" type issues.
			wait(delay(1 + CLIENT_KNOBS->DETAILED_HEALTH_METRICS_MAX_STALENESS));
			cx->healthMetrics.storageStats.clear();
			cx->healthMetrics.tLogQueue.clear();
		}
		return Void();
	}
	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	ACTOR static Future<Void> _start(Database cx, HealthMetricsApiWorkload* self) {
		wait(timeout(healthMetricsChecker(cx, self), self->testDuration, Void()));
		return Void();
	}
	Future<Void> start(Database const& cx) override { return _start(cx, this); }

	Future<bool> check(Database const& cx) override {
		if (!gotMetrics) {
			// It's not valid to fail a sanity check of metrics which have never been received.
			// Yes, this encodes a blatant "got" vs "have" usage error.  The intent is to show
			// up on any case insensitive search for "gotmetrics".
			TraceEvent("HealthMetricsCheckPassedBecauseWeDontGotMetrics");
			return true;
		}
		if (healthMetricsStoppedUpdating) {
			TraceEvent(SevError, "HealthMetricsStoppedUpdating").log();
			return false;
		}
		bool valid = true;
		if (worstStorageQueue == 0 || worstStorageDurabilityLag == 0 || worstTLogQueue == 0) {
			valid = false;
			TraceEvent("HealthMetrics:valid_false_case1");
		}
		if (sendDetailedHealthMetrics) {
			if (detailedWorstStorageQueue == 0 || detailedWorstStorageDurabilityLag == 0 ||
			    detailedWorstTLogQueue == 0 || detailedWorstCpuUsage == 0.0 || detailedWorstDiskUsage == 0.0) {
				valid = false;
				TraceEvent("HealthMetrics:valid_false_case2");
			}
		} else {
			if (detailedWorstStorageQueue != 0 || detailedWorstStorageDurabilityLag != 0 ||
			    detailedWorstTLogQueue != 0 || detailedWorstCpuUsage != 0.0 || detailedWorstDiskUsage != 0.0) {
				valid = false;
				TraceEvent("HealthMetrics:valid_false_case3");
			}
		}
		if (!valid) {
			TraceEvent(SevError, "IncorrectHealthMetricsState")
			    .detail("WorstStorageQueue", worstStorageQueue)
			    .detail("WorstLimitingStorageQueue", worstLimitingStorageQueue)
			    .detail("WorstStorageDurabilityLag", worstStorageDurabilityLag)
			    .detail("WorstLimitingStorageDurabilityLag", worstLimitingStorageDurabilityLag)
			    .detail("WorstTLogQueue", worstTLogQueue)
			    .detail("DetailedWorstStorageQueue", detailedWorstStorageQueue)
			    .detail("DetailedWorstStorageDurabilityLag", detailedWorstStorageDurabilityLag)
			    .detail("DetailedWorstTLogQueue", detailedWorstTLogQueue)
			    .detail("DetailedWorstCpuUsage", detailedWorstCpuUsage)
			    .detail("DetailedWorstDiskUsage", detailedWorstDiskUsage)
			    .detail("SendingDetailedHealthMetrics", sendDetailedHealthMetrics);
		}
		return valid;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.emplace_back("WorstStorageQueue", worstStorageQueue, Averaged::True);
		m.emplace_back("DetailedWorstStorageQueue", detailedWorstStorageQueue, Averaged::True);
		m.emplace_back("WorstStorageDurabilityLag", worstStorageDurabilityLag, Averaged::True);
		m.emplace_back("DetailedWorstStorageDurabilityLag", detailedWorstStorageDurabilityLag, Averaged::True);
		m.emplace_back("WorstTLogQueue", worstTLogQueue, Averaged::True);
		m.emplace_back("DetailedWorstTLogQueue", detailedWorstTLogQueue, Averaged::True);
		m.emplace_back("DetailedWorstCpuUsage", detailedWorstCpuUsage, Averaged::True);
		m.emplace_back("DetailedWorstDiskUsage", detailedWorstDiskUsage, Averaged::True);
	}

	ACTOR static Future<Void> healthMetricsChecker(Database cx, HealthMetricsApiWorkload* self) {
		state int repeated = 0;
		state HealthMetrics healthMetrics;
		loop {
			wait(delay(self->healthMetricsCheckInterval));
			HealthMetrics newHealthMetrics = wait(cx->getHealthMetrics(self->sendDetailedHealthMetrics));
			if (healthMetrics == newHealthMetrics) {
				if (++repeated > self->maxAllowedStaleness / self->healthMetricsCheckInterval)
					self->healthMetricsStoppedUpdating = true;
			} else
				repeated = 0;
			healthMetrics = newHealthMetrics;

			self->worstStorageQueue = std::max(self->worstStorageQueue, healthMetrics.worstStorageQueue);
			self->worstLimitingStorageQueue =
			    std::max(self->worstLimitingStorageQueue, healthMetrics.limitingStorageQueue);
			self->worstStorageDurabilityLag =
			    std::max(self->worstStorageDurabilityLag, healthMetrics.worstStorageDurabilityLag);
			self->worstLimitingStorageDurabilityLag =
			    std::max(self->worstLimitingStorageDurabilityLag, healthMetrics.limitingStorageDurabilityLag);
			self->worstTLogQueue = std::max(self->worstTLogQueue, healthMetrics.worstTLogQueue);

			TraceEvent("HealthMetrics")
			    .detail("WorstStorageQueue", healthMetrics.worstStorageQueue)
			    .detail("LimitingStorageQueue", healthMetrics.limitingStorageQueue)
			    .detail("WorstStorageDurabilityLag", healthMetrics.worstStorageDurabilityLag)
			    .detail("LimitingStorageDurabilityLag", healthMetrics.limitingStorageDurabilityLag)
			    .detail("WorstTLogQueue", healthMetrics.worstTLogQueue)
			    .detail("TpsLimit", healthMetrics.tpsLimit);

			TraceEvent traceStorageQueue("StorageQueue");
			TraceEvent traceStorageDurabilityLag("StorageDurabilityLag");
			TraceEvent traceCpuUsage("CpuUsage");
			TraceEvent traceDiskUsage("DiskUsage");

			bool gotStorageStats = false;
			for (const auto& ss : healthMetrics.storageStats) {
				gotStorageStats = true;
				auto storageStats = ss.second;
				self->detailedWorstStorageQueue = std::max(self->detailedWorstStorageQueue, storageStats.storageQueue);
				traceStorageQueue.detail(format("Storage-%s", ss.first.toString().c_str()), storageStats.storageQueue);
				self->detailedWorstStorageDurabilityLag =
				    std::max(self->detailedWorstStorageDurabilityLag, storageStats.storageDurabilityLag);
				traceStorageDurabilityLag.detail(format("Storage-%s", ss.first.toString().c_str()),
				                                 storageStats.storageDurabilityLag);
				self->detailedWorstCpuUsage = std::max(self->detailedWorstCpuUsage, storageStats.cpuUsage);
				traceCpuUsage.detail(format("Storage-%s", ss.first.toString().c_str()), storageStats.cpuUsage);
				self->detailedWorstDiskUsage = std::max(self->detailedWorstDiskUsage, storageStats.diskUsage);
				traceDiskUsage.detail(format("Storage-%s", ss.first.toString().c_str()), storageStats.diskUsage);
			}
			TraceEvent traceTLogQueue("TLogQueue");
			traceTLogQueue.setMaxEventLength(10000);
			bool gotTLogQueue = false;
			for (const auto& ss : healthMetrics.tLogQueue) {
				gotTLogQueue = true;
				self->detailedWorstTLogQueue = std::max(self->detailedWorstTLogQueue, ss.second);
				traceTLogQueue.detail(format("TLog-%s", ss.first.toString().c_str()), ss.second);
			}
			if (!self->gotMetrics && gotStorageStats && gotTLogQueue) {
				TraceEvent("HealthMetricsGotFullResult");
				self->gotMetrics = true;
			}
		};
	}
};
WorkloadFactory<HealthMetricsApiWorkload> HealthMetricsApiWorkloadFactory;
