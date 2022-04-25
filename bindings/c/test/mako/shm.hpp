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
	ThreadStatistics stats;
};

inline size_t storageSize(int num_processes, int num_threads) noexcept {
	assert(num_processes >= 1 && num_threads >= 1);
	return sizeof(LayoutHelper) + sizeof(ThreadStatistics) * ((num_processes * num_threads) - 1);
}

class Access {
	void* base;
	int num_processes;
	int num_threads;

	static inline ThreadStatistics& statsSlot(void* shm_base,
	                                          int num_threads,
	                                          int process_idx,
	                                          int thread_idx) noexcept {
		return (&static_cast<LayoutHelper*>(shm_base)->stats)[process_idx * num_threads + thread_idx];
	}

public:
	Access(void* shm, int num_processes, int num_threads) noexcept
	  : base(shm), num_processes(num_processes), num_threads(num_threads) {}

	Access() noexcept : Access(nullptr, 0, 0) {}

	Access(const Access&) noexcept = default;

	Access& operator=(const Access&) noexcept = default;

	size_t size() const noexcept { return storageSize(num_processes, num_threads); }

	void initMemory() noexcept {
		new (&header()) Header{};
		for (auto i = 0; i < num_processes; i++)
			for (auto j = 0; j < num_threads; j++)
				new (&statsSlot(i, j)) ThreadStatistics();
	}

	Header const& headerConst() const noexcept { return *static_cast<Header const*>(base); }

	Header& header() const noexcept { return *static_cast<Header*>(base); }

	ThreadStatistics const* statsConstArray() const noexcept {
		return &statsSlot(base, num_threads, 0 /*process_id*/, 0 /*thread_id*/);
	}

	ThreadStatistics* statsArray() const noexcept {
		return &statsSlot(base, num_threads, 0 /*process_id*/, 0 /*thread_id*/);
	}

	ThreadStatistics const& statsConstSlot(int process_idx, int thread_idx) const noexcept {
		return statsSlot(base, num_threads, process_idx, thread_idx);
	}

	ThreadStatistics& statsSlot(int process_idx, int thread_idx) const noexcept {
		return statsSlot(base, num_threads, process_idx, thread_idx);
	}
};

} // namespace mako::shared_memory

#endif /* MAKO_SHM_HPP */
