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

	double monitorStartTime;

	SystemMonitorMachineState() : monitorStartTime(0) {}
	explicit SystemMonitorMachineState(const IPAddress& ip) : ip(ip), monitorStartTime(0) {}
	SystemMonitorMachineState(std::string const& folder,
	                          Optional<Standalone<StringRef>> const& dcId,
	                          Optional<Standalone<StringRef>> const& zoneId,
	                          Optional<Standalone<StringRef>> const& machineId,
	                          IPAddress const& ip)
	  : folder(folder), dcId(dcId), zoneId(zoneId), machineId(machineId), ip(ip), monitorStartTime(0) {}
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
		bytesSent = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.BytesSent"));
		countPacketsReceived = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountPacketsReceived"));
		countPacketsGenerated = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountPacketsGenerated"));
		bytesReceived = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.BytesReceived"));
		countWriteProbes = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountWriteProbes"));
		countReadProbes = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountReadProbes"));
		countReads = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountReads"));
		countWouldBlock = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountWouldBlock"));
		countWrites = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountWrites"));
		countRunLoop = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountRunLoop"));
		countCantSleep = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountCantSleep"));
		countWontSleep = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountWontSleep"));
		countTimers = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountTimers"));
		countTasks = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountTasks"));
		countYields = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountYields"));
		countYieldBigStack = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountYieldBigStack"));
		countYieldCalls = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountYieldCalls"));
		countASIOEvents = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountASIOEvents"));
		countYieldCallsTrue = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountYieldCallsTrue"));
		countRunLoopProfilingSignals =
		    Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountRunLoopProfilingSignals"));
		countConnEstablished = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountConnEstablished"));
		countConnClosedWithError = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountConnClosedWithError"));
		countConnClosedWithoutError =
		    Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountConnClosedWithoutError"));
		countTLSPolicyFailures = Int64Metric::getValueOrDefault(LiteralStringRef("Net2.CountTLSPolicyFailures"));
		countLaunchTime = DoubleMetric::getValueOrDefault(LiteralStringRef("Net2.CountLaunchTime"));
		countReactTime = DoubleMetric::getValueOrDefault(LiteralStringRef("Net2.CountReactTime"));
		countFileLogicalWrites = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountLogicalWrites"));
		countFileLogicalReads = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountLogicalReads"));
		countAIOSubmit = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountAIOSubmit"));
		countAIOCollect = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountAIOCollect"));
		countFileCacheWrites = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCacheWrites"));
		countFileCacheReads = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCacheReads"));
		countFileCacheWritesBlocked =
		    Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCacheWritesBlocked"));
		countFileCacheReadsBlocked =
		    Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCacheReadsBlocked"));
		countFileCachePageReadsMerged =
		    Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCachePageReadsMerged"));
		countFileCacheFinds = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCacheFinds"));
		countFileCacheReadBytes = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCacheReadBytes"));
		countFilePageCacheHits = Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCachePageReadsHit"));
		countFilePageCacheMisses =
		    Int64Metric::getValueOrDefault(LiteralStringRef("AsyncFile.CountCachePageReadsMissed"));
		countFilePageCacheEvictions =
		    Int64Metric::getValueOrDefault(LiteralStringRef("EvictablePageCache.CacheEvictions"));
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

#endif /* FLOW_SYSTEM_MONITOR_H */
