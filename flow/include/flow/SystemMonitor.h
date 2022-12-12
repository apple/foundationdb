/*
 * SystemMonitor.h
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
	Optional<IPAddress> ip;
	Optional<std::string> fdbVersion;

	double monitorStartTime;

	SystemMonitorMachineState() : monitorStartTime(0) {}
	explicit SystemMonitorMachineState(const IPAddress& ip) : ip(ip), monitorStartTime(0) {}
	SystemMonitorMachineState(std::string const& folder,
	                          Optional<Standalone<StringRef>> const& dcId,
	                          Optional<Standalone<StringRef>> const& zoneId,
	                          Optional<Standalone<StringRef>> const& machineId,
	                          IPAddress const& ip,
	                          std::string const& fdbVersion)
	  : folder(folder), dcId(dcId), zoneId(zoneId), machineId(machineId), ip(ip), monitorStartTime(0),
	    fdbVersion(fdbVersion) {}
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

	void init() {
		bytesSent = Int64Metric::getValueOrDefault("Net2.BytesSent"_sr);
		countPacketsReceived = Int64Metric::getValueOrDefault("Net2.CountPacketsReceived"_sr);
		countPacketsGenerated = Int64Metric::getValueOrDefault("Net2.CountPacketsGenerated"_sr);
		bytesReceived = Int64Metric::getValueOrDefault("Net2.BytesReceived"_sr);
		countWriteProbes = Int64Metric::getValueOrDefault("Net2.CountWriteProbes"_sr);
		countReadProbes = Int64Metric::getValueOrDefault("Net2.CountReadProbes"_sr);
		countReads = Int64Metric::getValueOrDefault("Net2.CountReads"_sr);
		countWouldBlock = Int64Metric::getValueOrDefault("Net2.CountWouldBlock"_sr);
		countWrites = Int64Metric::getValueOrDefault("Net2.CountWrites"_sr);
		countRunLoop = Int64Metric::getValueOrDefault("Net2.CountRunLoop"_sr);
		countCantSleep = Int64Metric::getValueOrDefault("Net2.CountCantSleep"_sr);
		countWontSleep = Int64Metric::getValueOrDefault("Net2.CountWontSleep"_sr);
		countTimers = Int64Metric::getValueOrDefault("Net2.CountTimers"_sr);
		countTasks = Int64Metric::getValueOrDefault("Net2.CountTasks"_sr);
		countYields = Int64Metric::getValueOrDefault("Net2.CountYields"_sr);
		countYieldBigStack = Int64Metric::getValueOrDefault("Net2.CountYieldBigStack"_sr);
		countYieldCalls = Int64Metric::getValueOrDefault("Net2.CountYieldCalls"_sr);
		countASIOEvents = Int64Metric::getValueOrDefault("Net2.CountASIOEvents"_sr);
		countYieldCallsTrue = Int64Metric::getValueOrDefault("Net2.CountYieldCallsTrue"_sr);
		countRunLoopProfilingSignals = Int64Metric::getValueOrDefault("Net2.CountRunLoopProfilingSignals"_sr);
		countConnEstablished = Int64Metric::getValueOrDefault("Net2.CountConnEstablished"_sr);
		countConnClosedWithError = Int64Metric::getValueOrDefault("Net2.CountConnClosedWithError"_sr);
		countConnClosedWithoutError = Int64Metric::getValueOrDefault("Net2.CountConnClosedWithoutError"_sr);
		countTLSPolicyFailures = Int64Metric::getValueOrDefault("Net2.CountTLSPolicyFailures"_sr);
		countLaunchTime = DoubleMetric::getValueOrDefault("Net2.CountLaunchTime"_sr);
		countReactTime = DoubleMetric::getValueOrDefault("Net2.CountReactTime"_sr);
		countFileLogicalWrites = Int64Metric::getValueOrDefault("AsyncFile.CountLogicalWrites"_sr);
		countFileLogicalReads = Int64Metric::getValueOrDefault("AsyncFile.CountLogicalReads"_sr);
		countAIOSubmit = Int64Metric::getValueOrDefault("AsyncFile.CountAIOSubmit"_sr);
		countAIOCollect = Int64Metric::getValueOrDefault("AsyncFile.CountAIOCollect"_sr);
		countFileCacheWrites = Int64Metric::getValueOrDefault("AsyncFile.CountCacheWrites"_sr);
		countFileCacheReads = Int64Metric::getValueOrDefault("AsyncFile.CountCacheReads"_sr);
		countFileCacheWritesBlocked = Int64Metric::getValueOrDefault("AsyncFile.CountCacheWritesBlocked"_sr);
		countFileCacheReadsBlocked = Int64Metric::getValueOrDefault("AsyncFile.CountCacheReadsBlocked"_sr);
		countFileCachePageReadsMerged = Int64Metric::getValueOrDefault("AsyncFile.CountCachePageReadsMerged"_sr);
		countFileCacheFinds = Int64Metric::getValueOrDefault("AsyncFile.CountCacheFinds"_sr);
		countFileCacheReadBytes = Int64Metric::getValueOrDefault("AsyncFile.CountCacheReadBytes"_sr);
		countFilePageCacheHits = Int64Metric::getValueOrDefault("AsyncFile.CountCachePageReadsHit"_sr);
		countFilePageCacheMisses = Int64Metric::getValueOrDefault("AsyncFile.CountCachePageReadsMissed"_sr);
		countFilePageCacheEvictions = Int64Metric::getValueOrDefault("EvictablePageCache.CacheEvictions"_sr);
	}
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
