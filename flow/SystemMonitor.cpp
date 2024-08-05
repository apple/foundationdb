/*
 * SystemMonitor.cpp
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

#include <fstream>

#include "flow/flow.h"
#include "flow/Histogram.h"
#include "flow/Platform.h"
#include "flow/TDMetric.actor.h"
#include "flow/SystemMonitor.h"

#if defined(ALLOC_INSTRUMENTATION) && defined(__linux__)
#include <cxxabi.h>
#endif

#ifdef ADDRESS_SANITIZER
#include <sanitizer/asan_interface.h>
#endif

SystemMonitorMachineState machineState;

void initializeSystemMonitorMachineState(SystemMonitorMachineState machineState) {
	::machineState = machineState;

	ASSERT(g_network);
	::machineState.monitorStartTime = now();
}

double machineStartTime() {
	return ::machineState.monitorStartTime;
}

void systemMonitor() {
	static StatisticsState statState = StatisticsState();
#if !DEBUG_DETERMINISM
	customSystemMonitor("ProcessMetrics", &statState, true);
#endif
}

SystemStatistics getSystemStatistics() {
	static StatisticsState statState = StatisticsState();
	const IPAddress ipAddr = machineState.ip.present() ? machineState.ip.get() : IPAddress();
	return getSystemStatistics(
	    machineState.folder.present() ? machineState.folder.get() : "", &ipAddr, &statState.systemState, false);
}

#define TRACEALLOCATOR(size)                                                                                           \
	TraceEvent("MemSample")                                                                                            \
	    .detail("Count", FastAllocator<size>::getApproximateMemoryUnused() / size)                                     \
	    .detail("TotalSize", FastAllocator<size>::getApproximateMemoryUnused())                                        \
	    .detail("SampleCount", 1)                                                                                      \
	    .detail("Hash", "FastAllocatedUnused" #size)                                                                   \
	    .detail("Bt", "na")
#define DETAILALLOCATORMEMUSAGE(size)                                                                                  \
	detail("TotalMemory" #size, FastAllocator<size>::getTotalMemory())                                                 \
	    .detail("ApproximateUnusedMemory" #size, FastAllocator<size>::getApproximateMemoryUnused())                    \
	    .detail("ActiveThreads" #size, FastAllocator<size>::getActiveThreads())

namespace {

#ifdef __linux__
// Converts cgroup key, e.g. nr_periods, to NrPeriods
std::string capitalizeCgroupKey(const std::string& key) {
	bool wordStart = true;
	std::string result;
	result.reserve(key.size());

	for (const char ch : key) {
		if (std::isalnum(ch)) {
			if (wordStart) {
				result.push_back(std::toupper(ch));
				wordStart = false;
			} else {
				result.push_back(ch);
			}
		} else {
			// Skip non-alnum characters
			wordStart = true;
		}
	}

	return result;
}
#endif // __linux__

} // anonymous namespace

SystemStatistics customSystemMonitor(std::string const& eventName, StatisticsState* statState, bool machineMetrics) {
	const IPAddress ipAddr = machineState.ip.present() ? machineState.ip.get() : IPAddress();
	SystemStatistics currentStats = getSystemStatistics(
	    machineState.folder.present() ? machineState.folder.get() : "", &ipAddr, &statState->systemState, true);
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
			    .detail("UnusedAllocatedMemory", getTotalUnusedAllocatedMemory())
			    .detail("MbpsSent",
			            ((netData.bytesSent - statState->networkState.bytesSent) * 8e-6) / currentStats.elapsed)
			    .detail("MbpsReceived",
			            ((netData.bytesReceived - statState->networkState.bytesReceived) * 8e-6) / currentStats.elapsed)
			    .detail("DiskTotalBytes", currentStats.processDiskTotalBytes)
			    .detail("DiskFreeBytes", currentStats.processDiskFreeBytes)
			    .detail("DiskQueueDepth", currentStats.processDiskQueueDepth)
			    .detail("DiskIdleSeconds", currentStats.processDiskIdleSeconds)
			    .detail("DiskReads", currentStats.processDiskRead)
			    .detail("DiskReadSeconds", currentStats.processDiskReadSeconds)
			    .detail("DiskWrites", currentStats.processDiskWrite)
			    .detail("DiskWriteSeconds", currentStats.processDiskWriteSeconds)
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
			    .detail("Version", machineState.fdbVersion)
			    .detail("AIOSubmitCount", netData.countAIOSubmit - statState->networkState.countAIOSubmit)
			    .detail("AIOCollectCount", netData.countAIOCollect - statState->networkState.countAIOCollect)
			    .detail("AIOSubmitLag",
			            (g_network->networkInfo.metrics.secSquaredSubmit -
			             statState->networkMetricsState.secSquaredSubmit) /
			                currentStats.elapsed)
			    .detail("AIODiskStall",
			            (g_network->networkInfo.metrics.secSquaredDiskStall -
			             statState->networkMetricsState.secSquaredDiskStall) /
			                currentStats.elapsed)
			    .detail("CurrentConnections",
			            netData.countConnEstablished - netData.countConnClosedWithError -
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

			TraceEvent("MemoryMetrics")
			    .DETAILALLOCATORMEMUSAGE(16)
			    .DETAILALLOCATORMEMUSAGE(32)
			    .DETAILALLOCATORMEMUSAGE(64)
			    .DETAILALLOCATORMEMUSAGE(96)
			    .DETAILALLOCATORMEMUSAGE(128)
			    .DETAILALLOCATORMEMUSAGE(256)
			    .DETAILALLOCATORMEMUSAGE(512)
			    .DETAILALLOCATORMEMUSAGE(1024)
			    .DETAILALLOCATORMEMUSAGE(2048)
			    .DETAILALLOCATORMEMUSAGE(4096)
			    .DETAILALLOCATORMEMUSAGE(8192)
			    .DETAILALLOCATORMEMUSAGE(16384)
			    .detail("HugeArenaMemory", g_hugeArenaMemory.load())
			    .detail("DCID", machineState.dcId)
			    .detail("ZoneID", machineState.zoneId)
			    .detail("MachineID", machineState.machineId);

			uint64_t total_memory = 0;
			total_memory += FastAllocator<16>::getTotalMemory();
			total_memory += FastAllocator<32>::getTotalMemory();
			total_memory += FastAllocator<64>::getTotalMemory();
			total_memory += FastAllocator<96>::getTotalMemory();
			total_memory += FastAllocator<128>::getTotalMemory();
			total_memory += FastAllocator<256>::getTotalMemory();
			total_memory += FastAllocator<512>::getTotalMemory();
			total_memory += FastAllocator<1024>::getTotalMemory();
			total_memory += FastAllocator<2048>::getTotalMemory();
			total_memory += FastAllocator<4096>::getTotalMemory();
			total_memory += FastAllocator<8192>::getTotalMemory();
			total_memory += FastAllocator<16384>::getTotalMemory();

			uint64_t unused_memory = 0;
			unused_memory += FastAllocator<16>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<32>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<64>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<96>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<128>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<256>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<512>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<1024>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<2048>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<4096>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<8192>::getApproximateMemoryUnused();
			unused_memory += FastAllocator<16384>::getApproximateMemoryUnused();

			if (total_memory > 0) {
				TraceEvent("FastAllocMemoryUsage")
				    .detail("TotalMemory", total_memory)
				    .detail("UnusedMemory", unused_memory)
				    .detail("Utilization", format("%f%%", (total_memory - unused_memory) * 100.0 / total_memory));
			}

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

			for (int i = 0; i < NetworkMetrics::SLOW_EVENT_BINS; i++) {
				if (int c = g_network->networkInfo.metrics.countSlowEvents[i] -
				            statState->networkMetricsState.countSlowEvents[i]) {
					n.detail(format("SlowTask%dM", 1 << i).c_str(), c);
				}
			}

			std::map<TaskPriority, double> loggedDurations;
			for (auto& itr : g_network->networkInfo.metrics.activeTrackers) {
				if (itr.second.active) {
					itr.second.duration += now() - itr.second.windowedTimer;
					itr.second.windowedTimer = now();
				}

				if (itr.second.duration / currentStats.elapsed >= FLOW_KNOBS->MIN_LOGGED_PRIORITY_BUSY_FRACTION) {
					loggedDurations[itr.first] = std::min(currentStats.elapsed, itr.second.duration);
				}

				itr.second.duration = 0;
			}

			for (auto const& itr : loggedDurations) {
				// PriorityBusyX measures the amount of time spent busy at exactly priority X
				n.detail(format("PriorityBusy%d", itr.first).c_str(), itr.second);
			}

			bool firstTracker = true;
			for (auto& itr : g_network->networkInfo.metrics.starvationTrackers) {
				if (itr.active) {
					itr.duration += now() - itr.windowedTimer;
					itr.maxDuration = std::max(itr.maxDuration, now() - itr.timer);
					itr.windowedTimer = now();
				}

				// PriorityStarvedBelowX: how much of the elapsed time we were running tasks at a priority at or above X
				// PriorityMaxStarvedBelowX: The longest single span of time that you were starved below that priority,
				// which could tell you if you are doing work in bursts.
				n.detail(format("PriorityStarvedBelow%d", itr.priority).c_str(),
				         std::min(currentStats.elapsed, itr.duration));
				n.detail(format("PriorityMaxStarvedBelow%d", itr.priority).c_str(), itr.maxDuration);

				if (firstTracker) {
					g_network->networkInfo.metrics.lastRunLoopBusyness =
					    std::min(currentStats.elapsed, itr.duration) / currentStats.elapsed;
					firstTracker = false;
				}

				itr.duration = 0;
				itr.maxDuration = 0;
			}

			n.trackLatest("NetworkMetrics");
		}

		if (machineMetrics) {
			auto traceEvent = TraceEvent("MachineMetrics");
			traceEvent.detail("Elapsed", currentStats.elapsed)
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
			    .detail("DatahallID", machineState.datahallId)
			    .trackLatest("MachineMetrics");
#ifdef __linux__
			for (const auto& [k, v] : linux_os::reportCGroupCpuStat()) {
				traceEvent.detail(capitalizeCgroupKey(k).c_str(), v);
			}
#endif // __linux__
		}
	}

#ifdef ALLOC_INSTRUMENTATION
	{
		static double firstTime = 0.0;
		if (firstTime == 0.0)
			firstTime = now();
		if (now() - firstTime > 10 || g_network->isSimulated()) {
			firstTime = now();
			std::vector<std::pair<std::string, const char*>> typeNames;
			for (auto i = allocInstr.begin(); i != allocInstr.end(); ++i) {
				std::string s;
#ifdef __linux__
				char* demangled = abi::__cxa_demangle(i->first, nullptr, nullptr, nullptr);
				if (demangled) {
					s = demangled;
					if (StringRef(s).startsWith("(anonymous namespace)::"_sr))
						s = s.substr("(anonymous namespace)::"_sr.size());
					free(demangled);
				} else
					s = i->first;
#else
				s = i->first;
				if (StringRef(s).startsWith("class `anonymous namespace'::"_sr))
					s = s.substr("class `anonymous namespace'::"_sr.size());
				else if (StringRef(s).startsWith("class "_sr))
					s = s.substr("class "_sr.size());
				else if (StringRef(s).startsWith("struct "_sr))
					s = s.substr("struct "_sr.size());
#endif
				typeNames.emplace_back(s, i->first);
			}
			std::sort(typeNames.begin(), typeNames.end());
			for (int i = 0; i < typeNames.size(); i++) {
				const char* n = typeNames[i].second;
				auto& f = allocInstr[n];
				if (f.maxAllocated > 10000)
					TraceEvent("AllocInstrument")
					    .detail("CurrentAlloc", f.allocCount - f.deallocCount)
					    .detail("Name", typeNames[i].first.c_str());
			}

			std::unordered_map<uint32_t, BackTraceAccount> traceCounts;
			size_t memSampleSize;
			memSample_entered = true;
			{
				ThreadSpinLockHolder holder(memLock);
				traceCounts = backTraceLookup;
				memSampleSize = memSample.size();
			}
			memSample_entered = false;

			uint64_t totalSize = 0;
			uint64_t totalCount = 0;
			for (auto i = traceCounts.begin(); i != traceCounts.end(); ++i) {
				std::vector<void*>* frames = i->second.backTrace;
				std::string backTraceStr;
#if defined(_WIN32)
				char buf[1024];
				for (int j = 1; j < frames->size(); j++) {
					_snprintf(buf, 1024, "%p ", frames->at(j));
					backTraceStr += buf;
				}
#else
				backTraceStr = platform::format_backtrace(&(*frames)[0], frames->size());
#endif

				TraceEvent("MemSample")
				    .detail("Count", (int64_t)i->second.count)
				    .detail("TotalSize", i->second.totalSize)
				    .detail("SampleCount", i->second.sampleCount)
				    .detail("Hash", format("%lld", i->first))
				    .detail("Bt", backTraceStr);

				totalSize += i->second.totalSize;
				totalCount += i->second.count;
			}

			TraceEvent("MemSampleSummary")
			    .detail("InverseByteSampleRatio", SAMPLE_BYTES)
			    .detail("MemorySamples", memSampleSize)
			    .detail("BackTraces", traceCounts.size())
			    .detail("TotalSize", totalSize)
			    .detail("TotalCount", totalCount);

			TraceEvent("MemSample")
			    .detail("Count", traceCounts.size())
			    .detail("TotalSize", traceCounts.size() * ((int)(sizeof(uint32_t) + sizeof(size_t) + sizeof(size_t))))
			    .detail("SampleCount", traceCounts.size())
			    .detail("Hash", "backTraces")
			    .detail("Bt", "na");

			TraceEvent("MemSample")
			    .detail("Count", memSampleSize)
			    .detail("TotalSize", memSampleSize * ((int)(sizeof(void*) + sizeof(uint32_t) + sizeof(size_t))))
			    .detail("SampleCount", memSampleSize)
			    .detail("Hash", "memSamples")
			    .detail("Bt", "na");
			TRACEALLOCATOR(16);
			TRACEALLOCATOR(32);
			TRACEALLOCATOR(64);
			TRACEALLOCATOR(96);
			TRACEALLOCATOR(128);
			TRACEALLOCATOR(256);
			TRACEALLOCATOR(512);
			TRACEALLOCATOR(1024);
			TRACEALLOCATOR(2048);
			TRACEALLOCATOR(4096);
			TRACEALLOCATOR(8192);
		}
	}
#endif
	statState->networkMetricsState = g_network->networkInfo.metrics;
	statState->networkState = netData;
	return currentStats;
}

Future<Void> startMemoryUsageMonitor(uint64_t memLimit) {
	if (memLimit == 0) {
		return Void();
	}
	auto checkMemoryUsage = [=]() {
		if (getResidentMemoryUsage() > memLimit) {
#if defined(ADDRESS_SANITIZER) && defined(__linux__)
			__sanitizer_print_memory_profile(/*top percent*/ 100, /*max contexts*/ 10);
#endif
			platform::outOfMemory();
		}
	};
	return recurring(checkMemoryUsage, FLOW_KNOBS->MEMORY_USAGE_CHECK_INTERVAL);
}
