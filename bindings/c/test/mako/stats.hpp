#ifndef MAKO_STATS_HPP
#define MAKO_STATS_HPP

#include <cstdint>
#include <cstring>
#include <list>
#include <utility>
#include "operations.hpp"
#include "time.hpp"

namespace mako {

/* size of each block to get detailed latency for each operation */
constexpr const size_t LAT_BLOCK_SIZE = 4095;

/* memory block allocated to each operation when collecting detailed latency */
class lat_block_t {
	uint64_t samples[LAT_BLOCK_SIZE]{
		0,
	};
	uint32_t index{ 0 };

public:
	lat_block_t() noexcept = default;
	bool full() const noexcept { return index >= LAT_BLOCK_SIZE; }
	void put(timediff_t td) {
		assert(!full());
		samples[index++] = to_integer_microseconds(td);
	}
	// return {data block, number of samples}
	std::pair<uint64_t const*, size_t> data() const noexcept { return { samples, index }; }
};

/* collect sampled latencies */
class sample_bin {
	std::list<lat_block_t> blocks;

public:
	void reserve_one() {
		if (blocks.empty())
			blocks.emplace_back();
	}

	void put(timediff_t td) {
		if (blocks.empty() || blocks.back().full())
			blocks.emplace_back();
		blocks.back().put(td);
	}

	// iterate & apply for each block user function void(uint64_t const*, size_t)
	template <typename Func>
	void for_each_block(Func&& fn) const {
		for (const auto& block : blocks) {
			auto [ptr, cnt] = block.data();
			fn(ptr, cnt);
		}
	}
};

class alignas(64) stats_t {
	uint64_t xacts;
	uint64_t conflicts;
	uint64_t total_errors;
	uint64_t ops[MAX_OP];
	uint64_t errors[MAX_OP];
	uint64_t latency_samples[MAX_OP];
	uint64_t latency_us_total[MAX_OP];
	uint64_t latency_us_min[MAX_OP];
	uint64_t latency_us_max[MAX_OP];

public:
	stats_t() noexcept {
		memset(this, 0, sizeof(stats_t));
		memset(latency_us_min, 0xff, sizeof(latency_us_min));
	}

	stats_t(const stats_t& other) noexcept = default;
	stats_t& operator=(const stats_t& other) noexcept = default;

	uint64_t get_tx_count() const noexcept { return xacts; }

	uint64_t get_conflict_count() const noexcept { return conflicts; }

	uint64_t get_op_count(int op) const noexcept { return ops[op]; }

	uint64_t get_error_count(int op) const noexcept { return errors[op]; }

	uint64_t get_total_error_count() const noexcept { return total_errors; }

	uint64_t get_latency_sample_count(int op) const noexcept { return latency_samples[op]; }

	uint64_t get_latency_us_total(int op) const noexcept { return latency_us_total[op]; }

	uint64_t get_latency_us_min(int op) const noexcept { return latency_us_min[op]; }

	uint64_t get_latency_us_max(int op) const noexcept { return latency_us_max[op]; }

	// with 'this' as final aggregation, factor in 'other'
	void combine(const stats_t& other) {
		xacts += other.xacts;
		conflicts += other.conflicts;
		for (auto op = 0; op < MAX_OP; op++) {
			ops[op] += other.ops[op];
			errors[op] += other.errors[op];
			total_errors += other.errors[op];
			latency_samples[op] += other.latency_samples[op];
			latency_us_total[op] += other.latency_us_total[op];
			if (latency_us_min[op] > other.latency_us_min[op])
				latency_us_min[op] = other.latency_us_min[op];
			if (latency_us_max[op] < other.latency_us_max[op])
				latency_us_max[op] = other.latency_us_max[op];
		}
	}

	void incr_tx_count() noexcept { xacts++; }
	void incr_conflict_count() noexcept { conflicts++; }

	// non-commit write operations aren't measured for time.
	void incr_op_count(int op) noexcept { ops[op]++; }

	void incr_error_count(int op) noexcept {
		total_errors++;
		errors[op]++;
	}

	void add_latency(int op, timediff_t diff) noexcept {
		const auto latency_us = to_integer_microseconds(diff);
		latency_samples[op]++;
		latency_us_total[op] += latency_us;
		if (latency_us_min[op] > latency_us)
			latency_us_min[op] = latency_us;
		if (latency_us_max[op] < latency_us)
			latency_us_max[op] = latency_us;
	}
};

} // namespace mako

#endif /* MAKO_STATS_HPP */
