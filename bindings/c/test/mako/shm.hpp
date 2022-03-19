#ifndef MAKO_SHM_HPP
#define MAKO_SHM_HPP

#include <atomic>
#include <cassert>
#include <cstdint>
#include "stats.hpp"

// controlled, safer access to shared memory
namespace mako::shared_memory {

struct Header {
	std::atomic<int> signal;
	std::atomic<int> readycount;
	std::atomic<double> throttle_factor;
	std::atomic<int> stopcount;
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

	void reset() noexcept { memset(base, 0, size()); }

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
