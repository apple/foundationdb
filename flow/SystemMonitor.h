/*
 * SystemMonitor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "Platform.h"
#include "TDMetric.actor.h"

struct SystemMonitorMachineState {
	Optional<std::string> folder;
	Optional<Standalone<StringRef>> zoneId;
	Optional<Standalone<StringRef>> machineId;
	Optional<uint32_t> ip;

	double monitorStartTime;

	SystemMonitorMachineState() : monitorStartTime(0) {}
	SystemMonitorMachineState(uint32_t ip) : ip(ip), monitorStartTime(0) {}
	SystemMonitorMachineState(std::string folder, Optional<Standalone<StringRef>> zoneId, Optional<Standalone<StringRef>> machineId, uint32_t ip) 
		: folder(folder), zoneId(zoneId), machineId(machineId), ip(ip), monitorStartTime(0) {}
};

void initializeSystemMonitorMachineState(SystemMonitorMachineState machineState);

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
	int64_t countSlowTaskSignals;
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
	int64_t countConnEstablished;
	int64_t countConnClosedWithError;
	int64_t countConnClosedWithoutError;

	void init() {
		auto getValue = [] (StringRef name) -> int64_t {
			Reference<Int64Metric> r = Int64Metric::getOrCreateInstance(name);
			int64_t v = 0;
			if(r)
				v = r->getValue();
			return v;
		};

		bytesSent = getValue(LiteralStringRef("Net2.BytesSent"));
		countPacketsReceived = getValue(LiteralStringRef("Net2.CountPacketsReceived"));
		countPacketsGenerated = getValue(LiteralStringRef("Net2.CountPacketsGenerated"));
		bytesReceived = getValue(LiteralStringRef("Net2.BytesReceived"));
		countWriteProbes = getValue(LiteralStringRef("Net2.CountWriteProbes"));
		countReadProbes = getValue(LiteralStringRef("Net2.CountReadProbes"));
		countReads = getValue(LiteralStringRef("Net2.CountReads"));
		countWouldBlock = getValue(LiteralStringRef("Net2.CountWouldBlock"));
		countWrites = getValue(LiteralStringRef("Net2.CountWrites"));
		countRunLoop = getValue(LiteralStringRef("Net2.CountRunLoop"));
		countCantSleep = getValue(LiteralStringRef("Net2.CountCantSleep"));
		countWontSleep = getValue(LiteralStringRef("Net2.CountWontSleep"));
		countTimers = getValue(LiteralStringRef("Net2.CountTimers"));
		countTasks = getValue(LiteralStringRef("Net2.CountTasks"));
		countYields = getValue(LiteralStringRef("Net2.CountYields"));
		countYieldBigStack = getValue(LiteralStringRef("Net2.CountYieldBigStack"));
		countYieldCalls = getValue(LiteralStringRef("Net2.CountYieldCalls"));
		countASIOEvents = getValue(LiteralStringRef("Net2.CountASIOEvents"));
		countYieldCallsTrue = getValue(LiteralStringRef("Net2.CountYieldCallsTrue"));
		countSlowTaskSignals = getValue(LiteralStringRef("Net2.CountSlowTaskSignals"));
		countConnEstablished = getValue(LiteralStringRef("Net2.CountConnEstablished"));
		countConnClosedWithError = getValue(LiteralStringRef("Net2.CountConnClosedWithError"));
		countConnClosedWithoutError = getValue(LiteralStringRef("Net2.CountConnClosedWithoutError"));
		countFileLogicalWrites = getValue(LiteralStringRef("AsyncFile.CountLogicalWrites"));
		countFileLogicalReads = getValue(LiteralStringRef("AsyncFile.CountLogicalReads"));
		countAIOSubmit = getValue(LiteralStringRef("AsyncFile.CountAIOSubmit"));
		countAIOCollect = getValue(LiteralStringRef("AsyncFile.CountAIOCollect"));
		countFileCacheWrites = getValue(LiteralStringRef("AsyncFile.CountCacheWrites"));
		countFileCacheReads = getValue(LiteralStringRef("AsyncFile.CountCacheReads"));
		countFileCacheWritesBlocked = getValue(LiteralStringRef("AsyncFile.CountCacheWritesBlocked"));
		countFileCacheReadsBlocked = getValue(LiteralStringRef("AsyncFile.CountCacheReadsBlocked"));
		countFileCachePageReadsMerged = getValue(LiteralStringRef("AsyncFile.CountCachePageReadsMerged"));
		countFileCacheFinds = getValue(LiteralStringRef("AsyncFile.CountCacheFinds"));
		countFileCacheReadBytes = getValue(LiteralStringRef("AsyncFile.CountCacheReadBytes"));
	}
};

struct StatisticsState {
	SystemStatisticsState *systemState;
	NetworkData networkState;
	NetworkMetrics networkMetricsState;

	StatisticsState() : systemState(NULL) {}
};

void systemMonitor();
SystemStatistics customSystemMonitor(std::string eventName, StatisticsState *statState, bool machineMetrics = false);

#endif /* FLOW_SYSTEM_MONITOR_H */
