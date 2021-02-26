/*
 * SystemMonitor.cpp
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

#include "flow/flow.h"
#include "flow/Histogram.h"
#include "flow/Platform.h"
#include "flow/TDMetric.actor.h"
#include "flow/SystemMonitor.h"

SystemMonitorMachineState machineState;

void initializeSystemMonitorMachineState(SystemMonitorMachineState machineState) {
	::machineState = machineState;

	ASSERT(g_network);
	::machineState.monitorStartTime = now();
}

void systemMonitor() {
	static StatisticsState statState = StatisticsState();
	customSystemMonitor("ProcessMetrics", &statState, true );
}

SystemStatistics getSystemStatistics() {
	static StatisticsState statState = StatisticsState();
	const IPAddress ipAddr = machineState.ip.present() ? machineState.ip.get() : IPAddress();
	return getSystemStatistics(
		machineState.folder.present() ? machineState.folder.get() : "", &ipAddr, &statState.systemState, false);
}

#define TRACEALLOCATOR( size ) TraceEvent("MemSample").detail("Count", FastAllocator<size>::getApproximateMemoryUnused()/size).detail("TotalSize", FastAllocator<size>::getApproximateMemoryUnused()).detail("SampleCount", 1).detail("Hash", "FastAllocatedUnused" #size ).detail("Bt", "na")
#define DETAILALLOCATORMEMUSAGE( size ) detail("TotalMemory"#size, FastAllocator<size>::getTotalMemory()).detail("ApproximateUnusedMemory"#size, FastAllocator<size>::getApproximateMemoryUnused()).detail("ActiveThreads"#size, FastAllocator<size>::getActiveThreads())

void traceHeapStats();
SystemStatistics customSystemMonitor(std::string eventName, StatisticsState *statState, bool machineMetrics) {
	const IPAddress ipAddr = machineState.ip.present() ? machineState.ip.get() : IPAddress();
	SystemStatistics currentStats = getSystemStatistics(machineState.folder.present() ? machineState.folder.get() : "",
	                                                    &ipAddr, &statState->systemState, true);
	NetworkData netData;
	netData.init();
	if (!g_network->isSimulated() && currentStats.initialized) {
		{
			TraceEvent(eventName.c_str())
			    .detail("Elapsed", currentStats.elapsed)
			    .detail("CPUSeconds", currentStats.processCPUSeconds)
			    .detail("MainThreadCPUSeconds", currentStats.mainThreadCPUSeconds)
			    .detail("UptimeSeconds", now() - machineState.monitorStartTime)
			    .detail("Memory", currentStats.processMemory)
			    .detail("ResidentMemory", currentStats.processResidentMemory)
			    .detail("MbpsSent",
			            ((netData.bytesSent - statState->networkState.bytesSent) * 8e-6) / currentStats.elapsed)
			    .detail("MbpsReceived",
			            ((netData.bytesReceived - statState->networkState.bytesReceived) * 8e-6) / currentStats.elapsed)
			    .detail("DiskTotalBytes", currentStats.processDiskTotalBytes)
			    .detail("DiskFreeBytes", currentStats.processDiskFreeBytes)
			    .detail("DiskQueueDepth", currentStats.processDiskQueueDepth)
			    .detail("DiskIdleSeconds", currentStats.processDiskIdleSeconds)
			    .detail("DiskReads", currentStats.processDiskRead)
			    .detail("DiskWrites", currentStats.processDiskWrite)
			    .detail("DiskReadsCount", currentStats.processDiskReadCount)
			    .detail("DiskWritesCount", currentStats.processDiskWriteCount)
			    .detail("DiskWriteSectors", currentStats.processDiskWriteSectors)
			    .detail("DiskReadSectors", currentStats.processDiskReadSectors)
			    .detail("FileWrites", netData.countFileLogicalWrites - statState->networkState.countFileLogicalWrites)
			    .detail("FileReads", netData.countFileLogicalReads - statState->networkState.countFileLogicalReads)
			    .detail("CacheReadBytes",
			            netData.countFileCacheReadBytes - statState->networkState.countFileCacheReadBytes)
			    .detail("CacheFinds", netData.countFileCacheFinds - statState->networkState.countFileCacheFinds)
			    .detail("CacheWritesBlocked",
			            netData.countFileCacheWritesBlocked - statState->networkState.countFileCacheWritesBlocked)
			    .detail("CacheReadsBlocked",
			            netData.countFileCacheReadsBlocked - statState->networkState.countFileCacheReadsBlocked)
			    .detail("CachePageReadsMerged",
			            netData.countFileCachePageReadsMerged - statState->networkState.countFileCachePageReadsMerged)
			    .detail("CacheWrites", netData.countFileCacheWrites - statState->networkState.countFileCacheWrites)
			    .detail("CacheReads", netData.countFileCacheReads - statState->networkState.countFileCacheReads)
			    .detail("CacheHits", netData.countFilePageCacheHits - statState->networkState.countFilePageCacheHits)
			    .detail("CacheMisses",
			            netData.countFilePageCacheMisses - statState->networkState.countFilePageCacheMisses)
			    .detail("CacheEvictions",
			            netData.countFilePageCacheEvictions - statState->networkState.countFilePageCacheEvictions)
			    .detail("DCID", machineState.dcId)
			    .detail("ZoneID", machineState.zoneId)
			    .detail("MachineID", machineState.machineId)
			    .detail("AIOSubmitCount", netData.countAIOSubmit - statState->networkState.countAIOSubmit)
			    .detail("AIOCollectCount", netData.countAIOCollect - statState->networkState.countAIOCollect)
			    .detail("AIOSubmitLag", (g_network->networkInfo.metrics.secSquaredSubmit -
			                             statState->networkMetricsState.secSquaredSubmit) /
			                                currentStats.elapsed)
			    .detail("AIODiskStall", (g_network->networkInfo.metrics.secSquaredDiskStall -
			                             statState->networkMetricsState.secSquaredDiskStall) /
			                                currentStats.elapsed)
			    .detail("CurrentConnections", netData.countConnEstablished - netData.countConnClosedWithError -
			                                      netData.countConnClosedWithoutError)
			    .detail("ConnectionsEstablished",
			            (double)(netData.countConnEstablished - statState->networkState.countConnEstablished) /
			                currentStats.elapsed)
			    .detail("ConnectionsClosed",
			            ((netData.countConnClosedWithError - statState->networkState.countConnClosedWithError) +
			             (netData.countConnClosedWithoutError - statState->networkState.countConnClosedWithoutError)) /
			                currentStats.elapsed)
			    .detail("ConnectionErrors",
			            (netData.countConnClosedWithError - statState->networkState.countConnClosedWithError) /
			                currentStats.elapsed)
			    .detail("TLSPolicyFailures",
			            (netData.countTLSPolicyFailures - statState->networkState.countTLSPolicyFailures) /
			                currentStats.elapsed)
			    .trackLatest(eventName);

			TraceEvent n("NetworkMetrics");
			n.detail("Elapsed", currentStats.elapsed)
			    .detail("CantSleep", netData.countCantSleep - statState->networkState.countCantSleep)
			    .detail("WontSleep", netData.countWontSleep - statState->networkState.countWontSleep)
			    .detail("Yields", netData.countYields - statState->networkState.countYields)
			    .detail("YieldCalls", netData.countYieldCalls - statState->networkState.countYieldCalls)
			    .detail("YieldCallsTrue", netData.countYieldCallsTrue - statState->networkState.countYieldCallsTrue)
			    .detail("RunLoopProfilingSignals",
			            netData.countRunLoopProfilingSignals - statState->networkState.countRunLoopProfilingSignals)
			    .detail("YieldBigStack", netData.countYieldBigStack - statState->networkState.countYieldBigStack)
			    .detail("RunLoopIterations", netData.countRunLoop - statState->networkState.countRunLoop)
			    .detail("TimersExecuted", netData.countTimers - statState->networkState.countTimers)
			    .detail("TasksExecuted", netData.countTasks - statState->networkState.countTasks)
			    .detail("ASIOEventsProcessed", netData.countASIOEvents - statState->networkState.countASIOEvents)
			    .detail("ReadCalls", netData.countReads - statState->networkState.countReads)
			    .detail("WriteCalls", netData.countWrites - statState->networkState.countWrites)
			    .detail("ReadProbes", netData.countReadProbes - statState->networkState.countReadProbes)
			    .detail("WriteProbes", netData.countWriteProbes - statState->networkState.countWriteProbes)
			    .detail("PacketsRead", netData.countPacketsReceived - statState->networkState.countPacketsReceived)
			    .detail("PacketsGenerated",
			            netData.countPacketsGenerated - statState->networkState.countPacketsGenerated)
			    .detail("WouldBlock", netData.countWouldBlock - statState->networkState.countWouldBlock)
			    .detail("LaunchTime", netData.countLaunchTime - statState->networkState.countLaunchTime)
			    .detail("ReactTime", netData.countReactTime - statState->networkState.countReactTime)
			    .detail("DCID", machineState.dcId)
			    .detail("ZoneID", machineState.zoneId)
			    .detail("MachineID", machineState.machineId);

			for (int i = 0; i<NetworkMetrics::SLOW_EVENT_BINS; i++) {
				if (int c = g_network->networkInfo.metrics.countSlowEvents[i] - statState->networkMetricsState.countSlowEvents[i]) {
					n.detail(format("SlowTask%dM", 1 << i).c_str(), c);
				}
			}

			traceHeapStats();

			std::map<TaskPriority, double> loggedDurations;
			for (auto &itr : g_network->networkInfo.metrics.activeTrackers) {
				if(itr.second.active) {
					itr.second.duration += now() - itr.second.windowedTimer;
					itr.second.windowedTimer = now();
				}

				if(itr.second.duration / currentStats.elapsed >= FLOW_KNOBS->MIN_LOGGED_PRIORITY_BUSY_FRACTION) {
					loggedDurations[itr.first] = std::min(currentStats.elapsed, itr.second.duration);
				}

				itr.second.duration = 0;
			}

			for (auto const& itr : loggedDurations) {
				// PriorityBusyX measures the amount of time spent busy at exactly priority X
				n.detail(format("PriorityBusy%d", itr.first).c_str(), itr.second);
			}

			bool firstTracker = true;
			for (auto &itr : g_network->networkInfo.metrics.starvationTrackers) {
				if(itr.active) {
					itr.duration += now() - itr.windowedTimer;
					itr.maxDuration = std::max(itr.maxDuration, now() - itr.timer);
					itr.windowedTimer = now();
				}

				// PriorityStarvedBelowX: how much of the elapsed time we were running tasks at a priority at or above X
				// PriorityMaxStarvedBelowX: The longest single span of time that you were starved below that priority,
				// which could tell you if you are doing work in bursts.
				n.detail(format("PriorityStarvedBelow%d", itr.priority).c_str(), std::min(currentStats.elapsed, itr.duration));
				n.detail(format("PriorityMaxStarvedBelow%d", itr.priority).c_str(), itr.maxDuration);

				if(firstTracker) {
					g_network->networkInfo.metrics.lastRunLoopBusyness = std::min(currentStats.elapsed, itr.duration)/currentStats.elapsed;
					firstTracker = false;
				}

				itr.duration = 0;
				itr.maxDuration = 0;
			}

			n.trackLatest("NetworkMetrics");
		}

		if(machineMetrics) {
			TraceEvent("MachineMetrics")
			    .detail("Elapsed", currentStats.elapsed)
			    .detail("MbpsSent", currentStats.machineMegabitsSent / currentStats.elapsed)
			    .detail("MbpsReceived", currentStats.machineMegabitsReceived / currentStats.elapsed)
			    .detail("OutSegs", currentStats.machineOutSegs)
			    .detail("RetransSegs", currentStats.machineRetransSegs)
			    .detail("CPUSeconds", currentStats.machineCPUSeconds)
			    .detail("TotalMemory", currentStats.machineTotalRAM)
			    .detail("CommittedMemory", currentStats.machineCommittedRAM)
			    .detail("AvailableMemory", currentStats.machineAvailableRAM)
			    .detail("DCID", machineState.dcId)
			    .detail("ZoneID", machineState.zoneId)
			    .detail("MachineID", machineState.machineId)
			    .trackLatest("MachineMetrics");
		}
	}

	statState->networkMetricsState = g_network->networkInfo.metrics;
	statState->networkState = netData;
	return currentStats;
}
