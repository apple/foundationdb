#ifndef MAKO_SHM_HPP
#define MAKO_SHM_HPP

#include <atomic>
#include <cassert>
#include <cstdint>
#include "stats.hpp"

namespace mako {

struct shmhdr_t {
	std::atomic<int> signal;
	std::atomic<int> readycount;
	std::atomic<double> throttle_factor;
	std::atomic<int> stopcount;
};

struct shm_layout_helper {
	shmhdr_t hdr;
	stats_t stats;
};

inline size_t shm_storage_size(int num_processes, int num_threads) noexcept {
	assert(num_processes >= 1 && num_threads >= 1);
	return sizeof(shm_layout_helper) + sizeof(stats_t) * ((num_processes * num_threads) - 1);
}

inline stats_t& shm_stats_slot(void* shm_base, int num_threads, int process_idx, int thread_idx) noexcept {
	return (&static_cast<shm_layout_helper*>(shm_base)->stats)[process_idx * num_threads + thread_idx];
}

class shm_access_t {
protected:
	void* base;
	int num_processes;
	int num_threads;

public:
	shm_access_t(void* shm, int num_processes, int num_threads) noexcept
	  : base(shm), num_processes(num_processes), num_threads(num_threads) {}

	shm_access_t() noexcept : shm_access_t(nullptr, 0, 0) {}

	shm_access_t(const shm_access_t&) noexcept = default;

	shm_access_t& operator=(const shm_access_t&) noexcept = default;

	size_t storage_size() const noexcept { return shm_storage_size(num_processes, num_threads); }

	void reset() noexcept { memset(base, 0, storage_size()); }

	shmhdr_t const& header_const() const noexcept { return *static_cast<shmhdr_t const*>(base); }

	shmhdr_t& header() const noexcept { return *static_cast<shmhdr_t*>(base); }

	stats_t const* stats_const_array() const noexcept {
		return &shm_stats_slot(base, num_threads, 0 /*process_id*/, 0 /*thread_id*/);
	}

	stats_t* stats_array() const noexcept {
		return &shm_stats_slot(base, num_threads, 0 /*process_id*/, 0 /*thread_id*/);
	}

	stats_t const& stats_const_slot(int process_idx, int thread_idx) const noexcept {
		return shm_stats_slot(base, num_threads, process_idx, thread_idx);
	}

	stats_t& stats_slot(int process_idx, int thread_idx) const noexcept {
		return shm_stats_slot(base, num_threads, process_idx, thread_idx);
	}
};

} // namespace mako

#endif /* MAKO_SHM_HPP */
