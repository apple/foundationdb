/*
 * shm.hpp
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

#ifndef MAKO_SHM_HPP
#define MAKO_SHM_HPP

#include <atomic>
#include <cassert>
#include <cstdint>
#include "stats.hpp"

/* shared memory */
constexpr const int SIGNAL_RED = 0;
constexpr const int SIGNAL_GREEN = 1;
constexpr const int SIGNAL_OFF = 2;

// controlled, safer access to shared memory
namespace mako::shared_memory {

struct Header {
	std::atomic<int> signal = ATOMIC_VAR_INIT(SIGNAL_OFF);
	std::atomic<int> readycount = ATOMIC_VAR_INIT(0);
	std::atomic<double> throttle_factor = ATOMIC_VAR_INIT(1.0);
	std::atomic<int> stopcount = ATOMIC_VAR_INIT(0);
};

struct LayoutHelper {
	Header hdr;
	WorkflowStatistics stats;
};

inline size_t storageSize(int num_processes, int num_threads, int num_workers) noexcept {
	assert(num_processes >= 1 && num_threads >= 1);
	return sizeof(LayoutHelper) + sizeof(WorkflowStatistics) * ((num_processes * num_workers) - 1) +
	       sizeof(ThreadStatistics) * (num_threads * num_processes) + sizeof(ProcessStatistics) * num_processes;
}

// class Access memory layout:
// Header | WorkflowStatistics | WorkflowStatistics * (num_processes * num_workers - 1) | ThreadStatistics *
// (num_processes * num_threads) | ProcessStatistics * (num_processes)
// all Statistics classes have alignas(64)

class Access {
	void* base;
	int num_processes;
	int num_threads;
	int num_workers;

	static inline WorkflowStatistics& workerStatsSlot(void* shm_base,
	                                                  int num_workers,
	                                                  int process_idx,
	                                                  int worker_idx) noexcept {
		return (&static_cast<LayoutHelper*>(shm_base)->stats)[process_idx * num_workers + worker_idx];
	}

	static inline ThreadStatistics& threadStatsSlot(void* shm_base,
	                                                int num_processes,
	                                                int num_threads,
	                                                int num_workers,
	                                                int process_idx,
	                                                int thread_idx) noexcept {
		ThreadStatistics* thread_stat_base =
		    reinterpret_cast<ThreadStatistics*>(static_cast<char*>(shm_base) + sizeof(LayoutHelper) +
		                                        sizeof(WorkflowStatistics) * num_processes * num_workers);

		return thread_stat_base[process_idx * num_threads + thread_idx];
	}

	static inline ProcessStatistics& processStatsSlot(void* shm_base,
	                                                  int num_processes,
	                                                  int num_threads,
	                                                  int num_workers,
	                                                  int process_idx) noexcept {

		ProcessStatistics* proc_stat_base =
		    reinterpret_cast<ProcessStatistics*>(static_cast<char*>(shm_base) + sizeof(LayoutHelper) +
		                                         sizeof(WorkflowStatistics) * num_processes * num_workers +
		                                         sizeof(ThreadStatistics) * num_processes * num_threads);
		return proc_stat_base[process_idx];
	}

public:
	Access(void* shm, int num_processes, int num_threads, int num_workers) noexcept
	  : base(shm), num_processes(num_processes), num_threads(num_threads), num_workers(num_workers) {}

	Access() noexcept : Access(nullptr, 0, 0, 0) {}

	Access(const Access&) noexcept = default;

	Access& operator=(const Access&) noexcept = default;

	size_t size() const noexcept { return storageSize(num_processes, num_threads, num_workers); }

	void initMemory() noexcept {
		new (&header()) Header{};
		for (auto i = 0; i < num_processes; i++)
			for (auto j = 0; j < num_workers; j++) {
				new (&workerStatsSlot(i, j)) WorkflowStatistics();
			}
		for (auto i = 0; i < num_processes; i++)
			for (auto j = 0; j < num_threads; j++) {
				new (&threadStatsSlot(i, j)) ThreadStatistics();
			}
		for (auto i = 0; i < num_processes; i++) {
			new (&processStatsSlot(i)) ProcessStatistics();
		}
	}

	Header const& headerConst() const noexcept { return *static_cast<Header const*>(base); }

	Header& header() const noexcept { return *static_cast<Header*>(base); }

	WorkflowStatistics const* workerStatsConstArray() const noexcept {
		return &workerStatsSlot(base, num_workers, 0 /*process_idx*/, 0 /*worker_idx*/);
	}

	WorkflowStatistics* workerStatsArray() const noexcept {
		return &workerStatsSlot(base, num_workers, 0 /*process_idx*/, 0 /*worker_idx*/);
	}

	WorkflowStatistics const& workerStatsConstSlot(int process_idx, int worker_idx) const noexcept {
		return workerStatsSlot(base, num_workers, process_idx, worker_idx);
	}

	WorkflowStatistics& workerStatsSlot(int process_idx, int worker_idx) const noexcept {
		return workerStatsSlot(base, num_workers, process_idx, worker_idx);
	}

	ThreadStatistics const* threadStatsConstArray() const noexcept {
		return &threadStatsSlot(base, num_processes, num_threads, num_workers, 0 /*process_idx*/, 0 /*thread_idx*/);
	}

	ThreadStatistics* threadStatsArray() const noexcept {
		return &threadStatsSlot(base, num_processes, num_threads, num_workers, 0 /*process_idx*/, 0 /*thread_idx*/);
	}

	ThreadStatistics const& threadStatsConstSlot(int process_idx, int thread_idx) const noexcept {
		return threadStatsSlot(base, num_processes, num_threads, num_workers, process_idx, thread_idx);
	}

	ThreadStatistics& threadStatsSlot(int process_idx, int thread_idx) const noexcept {
		return threadStatsSlot(base, num_processes, num_threads, num_workers, process_idx, thread_idx);
	}

	ProcessStatistics const* processStatsConstArray() const noexcept {
		return &processStatsSlot(base, num_processes, num_threads, num_workers, 0 /*process_idx*/);
	}

	ProcessStatistics* processStatsArray() const noexcept {
		return &processStatsSlot(base, num_processes, num_threads, num_workers, 0 /*process_idx*/);
	}

	ProcessStatistics const& processStatsConstSlot(int process_idx) const noexcept {
		return processStatsSlot(base, num_processes, num_threads, num_workers, process_idx);
	}

	ProcessStatistics& processStatsSlot(int process_idx) const noexcept {
		return processStatsSlot(base, num_processes, num_threads, num_workers, process_idx);
	}
};

} // namespace mako::shared_memory

#endif /* MAKO_SHM_HPP */
