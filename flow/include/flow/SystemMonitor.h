/*
 * SystemMonitor.h
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

#ifndef FLOW_SYSTEM_MONITOR_H
#define FLOW_SYSTEM_MONITOR_H
#pragma once

#include "flow/Platform.h"
#include "flow/TDMetric.actor.h"

struct SystemMonitorMachineState {
	Optional<std::string> folder;
	Optional<Standalone<StringRef>> dcId;
	Optional<Standalone<StringRef>> zoneId;
	Optional<Standalone<StringRef>> machineId;
	Optional<Standalone<StringRef>> datahallId;
	Optional<IPAddress> ip;
	Optional<std::string> fdbVersion;

	double monitorStartTime;

	SystemMonitorMachineState() : monitorStartTime(0) {}
	explicit SystemMonitorMachineState(const IPAddress& ip) : ip(ip), monitorStartTime(0) {}
	SystemMonitorMachineState(std::string const& folder,
	                          Optional<Standalone<StringRef>> const& dcId,
	                          Optional<Standalone<StringRef>> const& zoneId,
	                          Optional<Standalone<StringRef>> const& machineId,
	                          Optional<Standalone<StringRef>> const& datahallId,
	                          IPAddress const& ip,
	                          std::string const& fdbVersion)
	  : folder(folder), dcId(dcId), zoneId(zoneId), machineId(machineId), datahallId(datahallId), ip(ip),
	    monitorStartTime(0), fdbVersion(fdbVersion) {}
};

void initializeSystemMonitorMachineState(SystemMonitorMachineState machineState);

// Returns the machine start time. 0 if the system monitor is not initialized.
double machineStartTime();

struct NetworkData {
	int64_t bytesSent;
	int64_t countPacketsReceived;
	int64_t countPacketsGenerated;
	int64_t bytesReceived;
	int64_t countWriteProbes;
	int64_t countReadProbes;
	int64_t countReads;
	int64_t countWouldBlock;
	int64_t countWrites;
	int64_t countRunLoop;
	int64_t countCantSleep;
	int64_t countWontSleep;
	int64_t countTimers;
	int64_t countTasks;
	int64_t countYields;
	int64_t countYieldBigStack;
	int64_t countYieldCalls;
	int64_t countASIOEvents;
	int64_t countYieldCallsTrue;
	int64_t countRunLoopProfilingSignals;
	int64_t countFileLogicalWrites;
	int64_t countFileLogicalReads;
	int64_t countAIOSubmit;
	int64_t countAIOCollect;
	int64_t countFileCacheWrites;
	int64_t countFileCacheReads;
	int64_t countFileCacheWritesBlocked;
	int64_t countFileCacheReadsBlocked;
	int64_t countFileCachePageReadsMerged;
	int64_t countFileCacheFinds;
	int64_t countFileCacheReadBytes;
	int64_t countFilePageCacheHits;
	int64_t countFilePageCacheMisses;
	int64_t countFilePageCacheEvictions;
	int64_t countConnEstablished;
	int64_t countConnClosedWithError;
	int64_t countConnClosedWithoutError;
	int64_t countTLSPolicyFailures;
	double countLaunchTime;
	double countReactTime;

	void init();
};

struct StatisticsState {
	SystemStatisticsState* systemState;
	NetworkData networkState;
	NetworkMetrics networkMetricsState;

	StatisticsState() : systemState(nullptr) {}
};

void systemMonitor();
SystemStatistics customSystemMonitor(std::string const& eventName,
                                     StatisticsState* statState,
                                     bool machineMetrics = false);
SystemStatistics getSystemStatistics();

Future<Void> startMemoryUsageMonitor(uint64_t memLimit);

#endif /* FLOW_SYSTEM_MONITOR_H */
