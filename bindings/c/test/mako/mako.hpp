#ifndef MAKO_HPP
#define MAKO_HPP
#pragma once

#ifndef FDB_API_VERSION
#define FDB_API_VERSION 710
#endif

#include <array>
#include <atomic>
#include <list>
#include <vector>
#include <string_view>
#include <fdb.hpp>
#include <pthread.h>
#include <sys/types.h>
#include <stdbool.h>
#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__APPLE__)
#include <sys/syslimits.h>
#else
#include <limits.h>
#endif

constexpr const int VERBOSE_NONE = 0;
constexpr const int VERBOSE_DEFAULT = 1;
constexpr const int VERBOSE_ANNOYING = 2;
constexpr const int VERBOSE_DEBUG = 3;

constexpr const int MODE_INVALID = -1;
constexpr const int MODE_CLEAN = 0;
constexpr const int MODE_BUILD = 1;
constexpr const int MODE_RUN = 2;

/* size of each block to get detailed latency for each operation */
constexpr const size_t LAT_BLOCK_SIZE = 511;

/* transaction specification */
enum Operations {
	OP_GETREADVERSION,
	OP_GET,
	OP_GETRANGE,
	OP_SGET,
	OP_SGETRANGE,
	OP_UPDATE,
	OP_INSERT,
	OP_INSERTRANGE,
	OP_OVERWRITE,
	OP_CLEAR,
	OP_SETCLEAR,
	OP_CLEARRANGE,
	OP_SETCLEARRANGE,
	OP_COMMIT,
	OP_TRANSACTION, /* pseudo-operation - cumulative time for the operation + commit */
	OP_READ_BG,
	MAX_OP /* must be the last item */
};

constexpr const int OP_COUNT = 0;
constexpr const int OP_RANGE = 1;
constexpr const int OP_REVERSE = 2;

/* for long arguments */
enum Arguments {
	ARG_KEYLEN,
	ARG_VALLEN,
	ARG_TPS,
	ARG_COMMITGET,
	ARG_SAMPLING,
	ARG_VERSION,
	ARG_KNOBS,
	ARG_FLATBUFFERS,
	ARG_LOGGROUP,
	ARG_PREFIXPADDING,
	ARG_TRACE,
	ARG_TRACEPATH,
	ARG_TRACEFORMAT,
	ARG_TPSMAX,
	ARG_TPSMIN,
	ARG_TPSINTERVAL,
	ARG_TPSCHANGE,
	ARG_TXNTRACE,
	ARG_TXNTAGGING,
	ARG_TXNTAGGINGPREFIX,
	ARG_STREAMING_MODE,
	ARG_DISABLE_RYW,
	ARG_CLIENT_THREADS_PER_VERSION,
	ARG_JSON_REPORT,
	ARG_BG_FILE_PATH // if blob granule files are stored locally, mako will read and materialize them if this is set
};

enum TPSChangeTypes { TPS_SIN, TPS_SQUARE, TPS_PULSE };

/* we set mako_txnspec_t and mako_args_t only once in the master process,
 * and won't be touched by child processes.
 */

typedef struct {
	/* for each operation, it stores "count", "range" and "reverse" */
	int ops[MAX_OP][3];
} mako_txnspec_t;

constexpr const int LOGGROUP_MAX = 256;
constexpr const int KNOB_MAX = 256;
constexpr const int TAGPREFIXLENGTH_MAX = 8;
constexpr const int NUM_CLUSTERS_MAX = 3;
constexpr const int NUM_DATABASES_MAX = 10;
constexpr const int MAX_BG_IDS = 1000;

/* benchmark parameters */
struct mako_args_t {
	int api_version;
	int json;
	int num_processes;
	int num_threads;
	int mode;
	int rows; /* is 2 billion enough? */
	int row_digits;
	int seconds;
	int iteration;
	int tpsmax;
	int tpsmin;
	int tpsinterval;
	int tpschange;
	int sampling;
	int key_length;
	int value_length;
	int zipf;
	int commit_get;
	int verbose;
	mako_txnspec_t txnspec;
	char cluster_files[NUM_CLUSTERS_MAX][PATH_MAX];
	int num_fdb_clusters;
	int num_databases;
	char log_group[LOGGROUP_MAX];
	int prefixpadding;
	int trace;
	char tracepath[PATH_MAX];
	int traceformat; /* 0 - XML, 1 - JSON */
	char knobs[KNOB_MAX];
	uint8_t flatbuffers;
	int txntrace;
	int txntagging;
	char txntagging_prefix[TAGPREFIXLENGTH_MAX];
	FDBStreamingMode streaming_mode;
	int64_t client_threads_per_version;
	int disable_ryw;
	char json_output_path[PATH_MAX];
	bool bg_materialize_files;
	char bg_file_path[PATH_MAX];
};

/* shared memory */
constexpr const int SIGNAL_RED = 0;
constexpr const int SIGNAL_GREEN = 1;
constexpr const int SIGNAL_OFF = 2;

struct mako_shmhdr_t {
	std::atomic<int> signal;
	std::atomic<int> readycount;
	std::atomic<double> throttle_factor;
	std::atomic<int> stopcount;
};

class alignas(64) mako_stats_t {
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
	mako_stats_t() noexcept {
		memset(this, 0, sizeof(mako_stats_t));
		memset(latency_us_min, 0xff, sizeof(latency_us_min));
	}

	mako_stats_t(const mako_stats_t& other) noexcept = default;
	mako_stats_t& operator=(const mako_stats_t& other) noexcept = default;

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
	void combine(const mako_stats_t& other) {
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
	void incr_count_immediate(int op) noexcept { ops[op]++; }

	void incr_error_count(int op) noexcept {
		total_errors++;
		errors[op]++;
	}

	void add_latency(int op, uint64_t latency_us) noexcept {
		latency_samples[op]++;
		latency_us_total[op] += latency_us;
		if (latency_us_min[op] > latency_us)
			latency_us_min[op] = latency_us;
		if (latency_us_max[op] < latency_us)
			latency_us_max[op] = latency_us;
		ops[op]++;
	}
};

/* per-process information */
typedef struct {
	int worker_id;
	pid_t parent_id;
	mako_args_t* args;
	mako_shmhdr_t* shm;
	std::vector<fdb::Database> databases;
} process_info_t;

/* memory block allocated to each operation when collecting detailed latency */
class lat_block_t {
	uint64_t samples[LAT_BLOCK_SIZE]{
		0,
	};
	uint32_t index{ 0 };

public:
	lat_block_t() noexcept = default;
	bool full() const noexcept { return index >= LAT_BLOCK_SIZE; }
	void put(uint64_t point) {
		assert(!full());
		samples[index++] = point;
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

	void put(uint64_t latency_us) {
		if (blocks.empty() || blocks.back().full())
			blocks.emplace_back();
		blocks.back().put(latency_us);
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

using sample_bin_array_t = std::array<sample_bin, MAX_OP>;
/* args for threads */
struct alignas(64) thread_args_t {
	int thread_id;
	int database_index; // index of the database to do work to
	sample_bin_array_t sample_bins;
	process_info_t* process;
};

/* process type */
typedef enum { proc_master = 0, proc_worker, proc_stats } proc_type_t;

// determines how resultant future will be handled
enum class StepKind {
	NONE, ///< not part of the table: OP_TRANSACTION, OP_COMMIT
	IMM, ///< non-future ops that return immediately: e.g. set, clear_range
	READ, ///< blockable reads: get(), get_range(), get_read_version, ...
	COMMIT, ///< self-explanatory
	ON_ERROR ///< future is a result of tx.on_error()
};

class OpDesc {
	std::string_view name_;
	std::vector<StepKind> steps_;
	bool needs_commit_;

public:
	OpDesc(std::string_view name, std::vector<StepKind>&& steps, bool needs_commit)
	  : name_(name), steps_(std::move(steps)), needs_commit_(needs_commit) {}

	std::string_view name() const noexcept { return name_; }
	// what
	StepKind step_kind(int step) const noexcept {
		assert(step < steps());
		return steps_[step];
	}
	// how many steps in this op?
	int steps() const noexcept { return static_cast<int>(steps_.size()); }
	// does the op needs to commit some time after its final step?
	bool needs_commit() const noexcept { return needs_commit_; }
};

#endif /* MAKO_HPP */
