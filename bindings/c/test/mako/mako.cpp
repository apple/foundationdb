#include <cmath>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <new>
#include <string>
#include <string_view>
#include <thread>

#include <fcntl.h>
#include <getopt.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#if defined(__linux__)
#include <linux/limits.h>
#elif defined(__FreeBSD__)
#include <sys/stat.h>
#elif defined(__APPLE__)
#include <sys/syslimits.h>
#else
#include <limits.h>
#endif

#include <fmt/format.h>
#include <fmt/printf.h>
#include <fdb.hpp>
#include "fdbclient/zipf.h"
#include "mako.hpp"
#include "utils.hpp"

using namespace fdb;

const std::string KEY_PREFIX{ "mako" };
const std::string TEMP_DATA_STORE{ "/tmp/makoTemp" };

/* global variables */
FILE* printme; /* descriptor used for default messages */
FILE* annoyme; /* descriptor used for annoying messages */
FILE* debugme; /* descriptor used for debug messages */

enum class FutureRC { OK, RETRY, CONFLICT, ABORT };

template <class FutureType>
FutureRC wait_and_handle_for_on_error(TX tx, FutureType f, std::string_view operation) {
	assert(f);
	auto err = Error{};
	if ((err = f.block_until_ready())) {
		fmt::print(stderr, "ERROR: Error while on_error() blocking for {}: {}\n", operation, err.what());
		return FutureRC::ABORT;
	}
	if ((err = f.error())) {
		fmt::print(stderr, "ERROR: Unretryable on_error() error from {}: {}\n", operation, err.what());
		return FutureRC::ABORT;
	} else {
		return FutureRC::OK;
	}
}

// wait on any non-immediate tx-related operation to complete. Follow up with on_error().
template <class FutureType>
FutureRC wait_and_handle_error(TX tx, FutureType f, std::string_view operation) {
	assert(f);
	auto err = Error{};
	if ((err = f.block_until_ready())) {
		const auto retry = err.retryable();
		auto fp = retry ? annoyme : stderr;
		fmt::print(fp, "ERROR: {} error in {}: {}\n", (retry ? "Retryable" : "Unretryable"), operation, err.what());
		return retry ? FutureRC::RETRY : FutureRC::ABORT;
	}
	err = f.error();
	if (!err)
		return FutureRC::OK;
	auto fp = err.retryable() ? annoyme : stderr;
	fmt::print(fp, "ERROR: Error in {}: {}\n", operation, err.what());
	// implicit backoff
	auto follow_up = tx.on_error(err);
	auto rc = wait_and_handle_for_on_error(tx, f, operation);
	if (rc == FutureRC::OK) {
		if (err.is(1020 /*not_committed*/))
			return FutureRC::CONFLICT;
		else
			return FutureRC::RETRY;
	} else {
		tx.reset();
		return rc;
	}
}

/* cleanup database */
int cleanup(TX tx, mako_args_t const& args) {
	auto beginstr = ByteString{};
	beginstr.reserve(args.key_length);
	genkeyprefix(beginstr, KEY_PREFIX, args);
	beginstr.push_back(0);

	auto endstr = ByteString{};
	endstr.reserve(args.key_length);
	genkeyprefix(endstr, KEY_PREFIX, args);
	endstr.push_back(0xff);

	const auto time_start = steady_clock::now();

	while (true) {
		tx.clear_range(beginstr, endstr);
		auto future_commit = tx.commit();
		const auto rc = wait_and_handle_error(tx, future_commit, "COMMIT_CLEANUP");
		if (rc == FutureRC::OK) {
			break;
		} else if (rc == FutureRC::RETRY || rc == FutureRC::CONFLICT) {
			// tx already reset
			continue;
		} else {
			return -1;
		}
	}

	tx.reset();
	const auto time_end = steady_clock::now();
	fmt::print(printme, "INFO: Clear range: {:6.3f} sec\n", to_double_seconds(time_end - time_start));
	return 0;
}

/* populate database */
int populate(TX tx,
             mako_args_t const& args,
             int worker_id,
             int thread_id,
             int thread_tps,
             mako_stats_t& stats,
             sample_bin_array_t& sample_bins) {
	const auto key_begin = insert_begin(args.rows, worker_id, thread_id, args.num_processes, args.num_threads);
	const auto key_end = insert_end(args.rows, worker_id, thread_id, args.num_processes, args.num_threads);
	auto xacts = 0;

	auto keystr = ByteString{};
	auto valstr = ByteString{};
	keystr.reserve(args.key_length);
	valstr.reserve(args.value_length);
	const auto time_start = steady_clock::now();
	const auto num_commit_every = args.txnspec.ops[OP_INSERT][OP_COUNT];
	auto time_prev = time_start; // for throttling
	auto time_tx_start = time_start;
	auto time_last_traced = time_start;
	auto key_checkpoint = key_begin; // in case of commit failure, restart from this key

	for (auto i = key_begin; i <= key_end; i++) {
		/* sequential keys */
		genkey(keystr, KEY_PREFIX, args, i);
		/* random values */
		randstr(valstr, args.value_length);

		while (thread_tps > 0 && xacts >= thread_tps /* throttle */) {
			const auto time_now = steady_clock::now();
			if (to_integer_seconds(time_now - time_prev) >= 1) {
				xacts = 0;
				time_prev = time_now;
			} else {
				usleep(1000);
			}
		}
		if (args.txntrace) {
			const auto time_now = steady_clock::now();
			if (to_integer_seconds(time_now - time_last_traced) >= args.txntrace) {
				time_last_traced = time_now;
				fmt::print(debugme, "DEBUG: txn tracing {}\n", to_chars_ref(keystr));
				auto err = Error{};
				err = tx.set_option_nothrow(FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, keystr);
				if (err) {
					fmt::print(stderr, "ERROR: set_option(TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER): {}\n", err.what());
				}
				err = tx.set_option_nothrow(FDB_TR_OPTION_LOG_TRANSACTION, BytesRef());
				if (err) {
					fmt::print(stderr, "ERROR: set_option(TR_OPTION_LOG_TRANSACTION): {}\n", err.what());
				}
			}
		}

		/* insert (SET) */
		tx.set(keystr, valstr);
		stats.incr_count_immediate(OP_INSERT);

		/* commit every 100 inserts (default) or if this is the last key */
		if (i == key_end || (i - key_begin + 1) % num_commit_every == 0) {
			const auto is_sample_target = (stats.get_tx_count() % args.sampling) == 0;
			auto time_commit_start = steady_clock::now();
			auto future_commit = tx.commit();
			const auto rc = wait_and_handle_error(tx, future_commit, "COMMIT_POPULATE_INSERT");
			if (rc == FutureRC::OK) {
				key_checkpoint = i + 1; // restart on failures from next key
			} else if (rc == FutureRC::ABORT) {
				return -1;
			} else {
				i = key_checkpoint - 1; // restart from last committed
				time_tx_start = steady_clock::now(); // tx shall restart
				continue;
			}

			const auto time_commit_end = steady_clock::now();
			/* xact latency stats */
			const auto commit_latency_us = to_integer_microseconds(time_commit_end - time_commit_start);
			const auto tx_duration_us = to_integer_microseconds(time_commit_end - time_tx_start);
			stats.add_latency(OP_COMMIT, commit_latency_us);
			stats.add_latency(OP_TRANSACTION, tx_duration_us);
			if (is_sample_target) {
				sample_bins[OP_COMMIT].put(commit_latency_us);
				sample_bins[OP_TRANSACTION].put(tx_duration_us);
			}

			time_tx_start = steady_clock::now();

			tx.reset();
			stats.incr_tx_count();
			xacts++; /* for throttling */
		}
	}
	const auto time_end = steady_clock::now();

	fmt::print(debugme,
	           "DEBUG: Populated {} rows [{}, {}]: {:6.3f} sec\n",
	           key_end - key_begin + 1,
	           key_begin,
	           key_end,
	           to_double_seconds(time_end - time_start));

	return 0;
}

// TODO: could always abstract this into something more generically usable by something other than mako.
// But outside of testing there are likely few use cases for local granules
typedef struct {
	char const* bgFilePath;
	int nextId;
	uint8_t** data_by_id;
} BGLocalFileContext;

int64_t granule_start_load(const char* filename,
                           int filenameLength,
                           int64_t offset,
                           int64_t length,
                           void* userContext) {
	FILE* fp;
	char full_fname[PATH_MAX]{
		0,
	};
	int loadId;
	uint8_t* data;
	size_t readSize;

	BGLocalFileContext* context = (BGLocalFileContext*)userContext;

	loadId = context->nextId;
	if (context->data_by_id[loadId] != 0) {
		fmt::print(stderr, "ERROR: too many granule file loads at once: {}\n", MAX_BG_IDS);
		return -1;
	}
	context->nextId = (context->nextId + 1) % MAX_BG_IDS;

	int ret = snprintf(full_fname, PATH_MAX, "%s%s", context->bgFilePath, filename);
	if (ret < 0 || ret >= PATH_MAX) {
		fmt::print(stderr, "ERROR: BG filename too long: {}{}\n", context->bgFilePath, filename);
		return -1;
	}

	fp = fopen(full_fname, "r");
	if (!fp) {
		fmt::print(stderr, "ERROR: BG could not open file: {}\n", full_fname);
		return -1;
	}

	// don't seek if offset == 0
	if (offset && fseek(fp, offset, SEEK_SET)) {
		// if fseek was non-zero, it failed
		fmt::print(stderr, "ERROR: BG could not seek to %{} in file {}\n", offset, full_fname);
		fclose(fp);
		return -1;
	}

	data = new uint8_t[length];
	readSize = fread(data, sizeof(uint8_t), length, fp);
	fclose(fp);

	if (readSize != length) {
		fmt::print(stderr, "ERROR: BG could not read {} bytes from file: {}\n", length, full_fname);
		return -1;
	}

	context->data_by_id[loadId] = data;
	return loadId;
}

uint8_t* granule_get_load(int64_t loadId, void* userContext) {
	BGLocalFileContext* context = (BGLocalFileContext*)userContext;
	if (context->data_by_id[loadId] == 0) {
		fmt::print(stderr, "ERROR: BG loadId invalid for get_load: {}\n", loadId);
		return 0;
	}
	return context->data_by_id[loadId];
}

void granule_free_load(int64_t loadId, void* userContext) {
	BGLocalFileContext* context = (BGLocalFileContext*)userContext;
	if (context->data_by_id[loadId] == 0) {
		fmt::print(stderr, "ERROR: BG loadId invalid for free_load: {}\n", loadId);
	}
	delete[] context->data_by_id[loadId];
	context->data_by_id[loadId] = 0;
}

inline int next_key(mako_args_t const& args) {
	if (args.zipf)
		return zipfian_next();
	return urand(0, args.rows - 1);
}

const std::array<OpDesc, MAX_OP> op_desc{ { { "GRV", { StepKind::READ }, false },
	                                        { "GET", { StepKind::READ }, false },
	                                        { "GETRANGE", { StepKind::READ }, false },
	                                        { "SGET", { StepKind::READ }, false },
	                                        { "SGETRANGE", { StepKind::READ }, false },
	                                        { "UPDATE", { StepKind::COMMIT, StepKind::IMM }, true },
	                                        { "INSERT", { StepKind::IMM }, true },
	                                        { "INSERTRANGE", { StepKind::IMM }, true },
	                                        { "OVERWRITE", { StepKind::IMM }, true },
	                                        { "CLEAR", { StepKind::IMM }, true },
	                                        { "SETCLEAR", { StepKind::COMMIT, StepKind::IMM }, true },
	                                        { "CLEARRANGE", { StepKind::IMM }, true },
	                                        { "SETCLEARRANGE", { StepKind::COMMIT, StepKind::IMM }, true },
	                                        { "COMMIT", { StepKind::NONE }, false },
	                                        { "TRANSACTION", { StepKind::NONE }, false },
	                                        { "READBLOBGRANULE", { StepKind::ON_ERROR }, false } } };

const std::map<std::pair<int /*op*/, int /*sub-op step*/>,
               Future (*)(TX, mako_args_t const&, ByteString& /*key1*/, ByteString& /*key2*/, ByteString& /*value*/)>
    operation_fn_table{
	    { { OP_GETREADVERSION, 0 },
	      [](TX tx, mako_args_t const&, ByteString&, ByteString&, ByteString&) {
	          return tx.get_read_version().erase_type();
	      } },
	    { { OP_GET, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString&) {
	          const auto num = next_key(args);
	          genkey(key, KEY_PREFIX, args, num);
	          return tx.get(key, false /*snapshot*/).erase_type();
	      } },
	    { { OP_GETRANGE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& begin, ByteString& end, ByteString&) {
	          const auto num_begin = next_key(args);
	          genkey(begin, KEY_PREFIX, args, num_begin);
	          auto num_end = num_begin + args.txnspec.ops[OP_GETRANGE][OP_RANGE] - 1;
	          if (num_end > args.rows - 1)
		          num_end = args.rows - 1;
	          genkey(end, KEY_PREFIX, args, num_end);
	          return tx
	              .get_range<key_select::inclusive, key_select::inclusive>(begin,
	                                                                       end,
	                                                                       0 /*limit*/,
	                                                                       0 /*target_bytes*/,
	                                                                       args.streaming_mode,
	                                                                       0 /*iteration*/,
	                                                                       false /*snapshot*/,
	                                                                       args.txnspec.ops[OP_GETRANGE][OP_REVERSE])
	              .erase_type();
	      } },
	    { { OP_SGET, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString&) {
	          const auto num = next_key(args);
	          genkey(key, KEY_PREFIX, args, num);
	          return tx.get(key, true /*snapshot*/).erase_type();
	      } },
	    { { OP_SGETRANGE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& begin, ByteString& end, ByteString&) {
	          const auto num_begin = next_key(args);
	          genkey(begin, KEY_PREFIX, args, num_begin);
	          auto num_end = num_begin + args.txnspec.ops[OP_SGETRANGE][OP_RANGE] - 1;
	          if (num_end > args.rows - 1)
		          num_end = args.rows - 1;
	          genkey(end, KEY_PREFIX, args, num_end);
	          return tx
	              .get_range<key_select::inclusive, key_select::inclusive>(begin,
	                                                                       end,
	                                                                       0 /*limit*/,
	                                                                       0 /*target_bytes*/,
	                                                                       args.streaming_mode,
	                                                                       0 /*iteration*/,
	                                                                       true /*snapshot*/,
	                                                                       args.txnspec.ops[OP_SGETRANGE][OP_REVERSE])
	              .erase_type();
	      } },
	    { { OP_UPDATE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString&) {
	          const auto num = next_key(args);
	          genkey(key, KEY_PREFIX, args, num);
	          return tx.get(key, false /*snapshot*/).erase_type();
	      } },
	    { { OP_UPDATE, 1 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString& value) {
	          randstr(value, args.value_length);
	          tx.set(key, value);
	          return Future();
	      } },
	    { { OP_INSERT, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString& value) {
	          genkeyprefix(key, KEY_PREFIX, args);
	          // concat([padding], key_prefix, random_string): reasonably unique
	          randstr<false /*clear-before-append*/>(key, args.key_length - static_cast<int>(key.size()));
	          randstr(value, args.value_length);
	          tx.set(key, value);
	          return Future();
	      } },
	    { { OP_INSERTRANGE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString& value) {
	          genkeyprefix(key, KEY_PREFIX, args);
	          const auto prefix_len = static_cast<int>(key.size());
	          const auto range = args.txnspec.ops[OP_INSERTRANGE][OP_RANGE];
	          assert(range > 0);
	          const auto range_digits = digits(range);
	          assert(args.key_length - prefix_len >= range_digits);
	          const auto rand_len = args.key_length - prefix_len - range_digits;
	          // concat([padding], prefix, random_string, range_digits)
	          randstr<false /*clear-before-append*/>(key, rand_len);
	          randstr(value, args.value_length);
	          for (auto i = 0; i < range; i++) {
		          fmt::format_to(std::back_inserter(key), "{0:0{1}d}", i, range_digits);
		          tx.set(key, value);
		          key.resize(key.size() - static_cast<size_t>(range_digits));
	          }
	          return Future();
	      } },
	    { { OP_OVERWRITE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString& value) {
	          genkey(key, KEY_PREFIX, args, next_key(args));
	          randstr(value, args.value_length);
	          tx.set(key, value);
	          return Future();
	      } },
	    { { OP_CLEAR, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString&) {
	          genkey(key, KEY_PREFIX, args, next_key(args));
	          tx.clear(key);
	          return Future();
	      } },
	    { { OP_SETCLEAR, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString& value) {
	          genkeyprefix(key, KEY_PREFIX, args);
	          const auto prefix_len = static_cast<int>(key.size());
	          randstr<false /*append-after-clear*/>(key, args.key_length - prefix_len);
	          randstr(value, args.value_length);
	          tx.set(key, value);
	          return tx.commit().erase_type();
	      } },
	    { { OP_SETCLEAR, 1 },
	      [](TX tx, mako_args_t const& args, ByteString& key, ByteString&, ByteString&) {
	          tx.reset(); // assuming commit from step 0 worked.
	          tx.clear(key); // key should forward unchanged from step 0
	          return Future();
	      } },
	    { { OP_CLEARRANGE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& begin, ByteString& end, ByteString&) {
	          const auto num_begin = next_key(args);
	          genkey(begin, KEY_PREFIX, args, num_begin);
	          const auto range = args.txnspec.ops[OP_CLEARRANGE][OP_RANGE];
	          assert(range > 0);
	          genkey(end, KEY_PREFIX, args, std::min(args.rows - 1, num_begin + range - 1));
	          tx.clear_range(begin, end);
	          return Future();
	      } },
	    { { OP_SETCLEARRANGE, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& key_begin, ByteString& key, ByteString& value) {
	          genkeyprefix(key, KEY_PREFIX, args);
	          const auto prefix_len = static_cast<int>(key.size());
	          const auto range = args.txnspec.ops[OP_SETCLEARRANGE][OP_RANGE];
	          assert(range > 0);
	          const auto range_digits = digits(range);
	          assert(args.key_length - prefix_len >= range_digits);
	          const auto rand_len = args.key_length - prefix_len - range_digits;
	          // concat([padding], prefix, random_string, range_digits)
	          randstr<false /*clear-before-append*/>(key, rand_len);
	          randstr(value, args.value_length);
	          for (auto i = 0; i <= range; i++) {
		          fmt::format_to(std::back_inserter(key), "{0:0{1}d}", i, range_digits);
		          if (i == range)
			          break; // preserve "exclusive last"
		          // preserve first key for step 1
		          if (i == 0)
			          key_begin = key;
		          tx.set(key, value);
		          // preserve last key for step 1
		          key.resize(key.size() - static_cast<size_t>(range_digits));
	          }
	          return tx.commit().erase_type();
	      } },
	    { { OP_SETCLEARRANGE, 1 },
	      [](TX tx, mako_args_t const& args, ByteString& begin, ByteString& end, ByteString&) {
	          tx.reset();
	          tx.clear_range(begin, end);
	          return Future();
	      } },
	    { { OP_READ_BG, 0 },
	      [](TX tx, mako_args_t const& args, ByteString& begin, ByteString& end, ByteString&) {
	          const auto num_begin = next_key(args);
	          genkey(begin, KEY_PREFIX, args, num_begin);
	          const auto range = args.txnspec.ops[OP_READ_BG][OP_RANGE];
	          assert(range > 0);
	          genkey(end, KEY_PREFIX, args, std::min(args.rows - 1, num_begin + range - 1));
	          auto err = Error{};

	          err = tx.set_option_nothrow(FDB_TR_OPTION_READ_YOUR_WRITES_DISABLE, BytesRef());
	          if (err) {
		          // Issuing read/writes before disabling RYW results in error.
		          // Possible malformed workload?
		          // As workloads execute in sequence, retrying would likely repeat this error.
		          fmt::print(stderr, "ERROR: TR_OPTION_READ_YOUR_WRITES_DISABLE: {}", err.what());
		          return Future();
	          }

	          auto mem = std::unique_ptr<uint8_t*[]>(new uint8_t*[MAX_BG_IDS]);
	          // Allocate a separate context per call to avoid multiple threads accessing
	          BGLocalFileContext fileContext;
	          fileContext.bgFilePath = args.bg_file_path;
	          fileContext.nextId = 0;
	          fileContext.data_by_id = mem.get();
	          memset(fileContext.data_by_id, 0, MAX_BG_IDS * sizeof(uint8_t*));

	          native::FDBReadBlobGranuleContext granuleContext;
	          granuleContext.userContext = &fileContext;
	          granuleContext.start_load_f = &granule_start_load;
	          granuleContext.get_load_f = &granule_get_load;
	          granuleContext.free_load_f = &granule_free_load;
	          granuleContext.debugNoMaterialize = !args.bg_materialize_files;

	          auto r =
	              tx.read_blob_granules(begin, end, 0 /*begin_version*/, -1 /*end_version, use txn's*/, granuleContext);

	          mem.reset();

	          auto out = Result::KeyValueArray{};
	          err = r.get_keyvalue_array_nothrow(out);
	          if (!err || err.is(2037 /*blob_granule_not_materialized*/))
		          return Future();
	          const auto fp = (err.is(1020 /*not_committed*/) || err.is(1021 /*commit_unknown_result*/) ||
	                           err.is(1213 /*tag_throttled*/))
	                              ? annoyme
	                              : stderr;
	          fmt::print(fp, "ERROR: get_keyvalue_array() after read_blob_granules(): {}\n", err.what());
	          return tx.on_error(err).erase_type();
	      } }
    };

using OpIterator = std::tuple<int /*op*/, int /*count*/, int /*step*/>;

constexpr const OpIterator OpEnd = OpIterator(MAX_OP, -1, -1);

OpIterator get_op_begin(mako_args_t const& args) noexcept {
	for (auto op = 0; op < MAX_OP; op++) {
		if (op == OP_COMMIT || op == OP_TRANSACTION || args.txnspec.ops[op][OP_COUNT] == 0)
			continue;
		return OpIterator(op, 0, 0);
	}
	return OpEnd;
}

OpIterator get_op_next(mako_args_t const& args, OpIterator current) noexcept {
	if (OpEnd == current)
		return OpEnd;
	auto [op, count, step] = current;
	assert(op < MAX_OP && op != OP_TRANSACTION && op != OP_COMMIT);
	if (op_desc[op].steps() > step + 1)
		return OpIterator(op, count, step + 1);
	count++;
	for (; op < MAX_OP; op++, count = 0) {
		if (op == OP_COMMIT || op == OP_TRANSACTION || args.txnspec.ops[op][OP_COUNT] <= count)
			continue;
		return OpIterator(op, count, 0);
	}
	return OpEnd;
}

/* run one transaction */
int run_one_transaction(TX tx, mako_args_t const& args, mako_stats_t& stats, sample_bin_array_t& sample_bins) {
	// reuse memory for keys to avoid realloc overhead
	auto key1 = ByteString{};
	key1.reserve(args.key_length);
	auto key2 = ByteString{};
	key2.reserve(args.key_length);
	auto val = ByteString{};
	val.reserve(args.value_length);

	auto time_tx_start = steady_clock::now();

	auto op_iter = get_op_begin(args);
	auto needs_commit = false;
	auto time_per_op_start = std::array<timepoint_t, MAX_OP>{};
	const auto do_sample = (stats.get_tx_count() % args.sampling) == 0;
	while (op_iter != OpEnd) {
		const auto [op, count, step] = op_iter;
		const auto step_kind = op_desc[op].step_kind(step);
		const auto op_key = std::make_pair(op, step);
		const auto time_step_start = steady_clock::now();
		if (step == 0 /* first step */) {
			time_per_op_start[op] = time_step_start;
		}
		auto f = operation_fn_table.at(op_key)(tx, args, key1, key2, val);
		auto future_rc = FutureRC::OK;
		if (f) {
			if (step_kind != StepKind::ON_ERROR) {
				future_rc = wait_and_handle_error(tx, f, op_desc[op].name());
			} else {
				auto followup_rc = wait_and_handle_for_on_error(tx, f, op_desc[op].name());
				if (followup_rc == FutureRC::OK) {
					future_rc = FutureRC::RETRY;
				}
			}
		}
		const auto time_step_end = steady_clock::now();
		const auto step_usec = to_integer_microseconds(time_step_end - time_step_start);
		if (future_rc != FutureRC::OK) {
			if (future_rc == FutureRC::CONFLICT) {
				stats.incr_conflict_count();
			} else if (future_rc == FutureRC::RETRY) {
				stats.incr_error_count(op);
			} else {
				// abort
				tx.reset();
				return -1;
			}
			// retry from first op
			op_iter = get_op_begin(args);
			needs_commit = false;
			continue;
		}
		// step successful
		if (step_kind == StepKind::COMMIT) {
			const auto tx_usec = to_integer_microseconds(time_step_end - time_tx_start);
			stats.add_latency(OP_COMMIT, step_usec);
			stats.add_latency(OP_TRANSACTION, tx_usec);
			if (do_sample) {
				sample_bins[OP_COMMIT].put(step_usec);
				sample_bins[OP_TRANSACTION].put(tx_usec);
			}
			time_tx_start = time_step_end; // new tx begins
		}

		// op completed successfully
		if (step + 1 == op_desc[op].steps() /* last step */) {
			if (op_desc[op].needs_commit())
				needs_commit = true;
			const auto op_usec = to_integer_microseconds(time_step_end - time_per_op_start[op]);
			stats.add_latency(op, op_usec);
			if (do_sample)
				sample_bins[op].put(op_usec);
		}
		// move to next op
		op_iter = get_op_next(args, op_iter);

		// reached the end?
		if (op_iter == OpEnd && (needs_commit || args.commit_get)) {
			const auto time_commit_start = steady_clock::now();
			auto f = tx.commit();
			const auto rc = wait_and_handle_error(tx, f, "COMMIT_AT_TX_END");
			const auto time_commit_end = steady_clock::now();
			const auto commit_usec = to_integer_microseconds(time_commit_end - time_commit_start);
			const auto tx_usec = to_integer_microseconds(time_commit_end - time_tx_start);
			if (rc == FutureRC::OK) {
				stats.add_latency(OP_COMMIT, commit_usec);
				stats.add_latency(OP_TRANSACTION, tx_usec);
				if (do_sample) {
					sample_bins[OP_COMMIT].put(commit_usec);
					sample_bins[OP_TRANSACTION].put(tx_usec);
				}
			} else {
				if (rc == FutureRC::CONFLICT)
					stats.incr_conflict_count();
				else
					stats.incr_error_count(OP_COMMIT);
				if (rc == FutureRC::ABORT) {
					tx.reset();
					return -1;
				}
				// restart from beginning
				op_iter = get_op_begin(args);
			}
		}
	}
	stats.incr_tx_count();
	/* make sure to reset transaction */
	tx.reset();
	return 0;
}

int run_workload(TX tx,
                 mako_args_t const& args,
                 int const thread_tps,
                 std::atomic<double> const& throttle_factor,
                 int const thread_iters,
                 std::atomic<int> const& signal,
                 mako_stats_t& stats,
                 sample_bin_array_t& sample_bins,
                 int const dotrace,
                 int const dotagging) {
	auto traceid = std::string{};
	auto tagstr = std::string{};

	if (thread_tps < 0)
		return 0;

	if (dotrace)
		traceid.reserve(32);

	if (dotagging)
		tagstr.reserve(16);

	auto current_tps = static_cast<int>(thread_tps * throttle_factor.load());

	auto time_prev = steady_clock::now();
	auto time_last_trace = time_prev;

	auto rc = 0;
	auto xacts = 0;
	auto total_xacts = int64_t{};
	/* main transaction loop */
	while (1) {
		while ((thread_tps > 0) && (xacts >= current_tps)) {
			/* throttle on */
			const auto time_now = steady_clock::now();
			if (to_double_seconds(time_now - time_prev) >= 1.0) {
				/* more than 1 second passed, no need to throttle */
				xacts = 0;
				time_prev = time_now;

				/* update throttle rate */
				current_tps = static_cast<int>(thread_tps * throttle_factor.load());
			} else {
				usleep(1000);
			}
		}
		/* enable transaction trace */
		if (dotrace) {
			const auto time_now = steady_clock::now();
			if (to_integer_seconds(time_now - time_last_trace) >= 1) {
				time_last_trace = time_now;
				traceid.clear();
				fmt::format_to(std::back_inserter(traceid), "makotrace{:0>19d}", total_xacts);
				fmt::print(debugme, "DEBUG: txn tracing {}\n", traceid);
				auto err = Error{};
				err = tx.set_option_nothrow(FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, to_bytes_ref(traceid));
				if (err) {
					fmt::print(stderr, "ERROR: TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER: {}\n", err.what());
				}
				err = tx.set_option_nothrow(FDB_TR_OPTION_LOG_TRANSACTION, BytesRef());
				if (err) {
					fmt::print(stderr, "ERROR: TR_OPTION_LOG_TRANSACTION: {}\n", err.what());
				}
			}
		}

		/* enable transaction tagging */
		if (dotagging > 0) {
			tagstr.clear();
			fmt::format_to(std::back_inserter(tagstr),
			               "{}{}{:0>3d}",
			               KEY_PREFIX,
			               args.txntagging_prefix,
			               urand(0, args.txntagging - 1));
			auto err = tx.set_option_nothrow(FDB_TR_OPTION_AUTO_THROTTLE_TAG, to_bytes_ref(tagstr));
			if (err) {
				fmt::print(stderr, "ERROR: TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER: {}\n", err.what());
			}
		}

		rc = run_one_transaction(tx, args, stats, sample_bins);
		if (rc) {
			fmt::print(annoyme, "ERROR: run_one_transaction failed ({})\n", rc);
		}

		if (thread_iters > 0) {
			if (thread_iters == xacts) {
				/* xact limit reached */
				break;
			}
		} else if (signal.load() == SIGNAL_RED) {
			/* signal turned red, target duration reached */
			break;
		}
		xacts++;
		total_xacts++;
	}
	return rc;
}

std::string get_stats_file_name(std::string_view dirname, int worker_id, int thread_id, int op) {

	return fmt::format("{}/{}_{}_{}", dirname, worker_id + 1, thread_id + 1, op_desc[op].name());
}

/* mako worker thread */
void worker_thread(thread_args_t& thread_args) {
	const auto& args = *thread_args.process->args;
	const auto parent_id = thread_args.process->parent_id;
	const auto worker_id = thread_args.process->worker_id;
	const auto thread_id = thread_args.thread_id;
	const auto dotrace = (worker_id == 0 && thread_id == 0 && args.txntrace) ? args.txntrace : 0;
	const auto dotagging = args.txntagging;
	const auto database_index = thread_args.database_index;
	const auto& signal = thread_args.process->shm->signal;
	const auto& throttle_factor = thread_args.process->shm->throttle_factor;
	auto& readycount = thread_args.process->shm->readycount;
	auto& stopcount = thread_args.process->shm->stopcount;
	static_assert(std::is_same_v<decltype(std::decay_t<decltype(*thread_args.process)>::shm), mako_shmhdr_t*>);
	auto& stats = *(reinterpret_cast<mako_stats_t*>(thread_args.process->shm + 1) /* skip header */ +
	                (worker_id * args.num_threads + thread_id));

	/* init per-thread latency statistics */
	new (&stats) mako_stats_t();

	fmt::print(debugme,
	           "DEBUG: worker_id:{} ({}) thread_id:{} ({}) database_index:{} (tid:{})\n",
	           worker_id,
	           args.num_processes,
	           thread_id,
	           args.num_threads,
	           database_index,
	           fmt::ptr(pthread_self()));

	const auto thread_tps =
	    args.tpsmax == 0 ? 0
	                     : compute_thread_tps(args.tpsmax, worker_id, thread_id, args.num_processes, args.num_threads);

	const auto thread_iters =
	    args.iteration == 0
	        ? 0
	        : compute_thread_iters(args.iteration, worker_id, thread_id, args.num_processes, args.num_threads);

	auto database = thread_args.process->databases[database_index];
	/* create my own transaction object */
	auto tx = database.create_tx();

	/* i'm ready */
	readycount.fetch_add(1);
	auto stopcount_guard = exit_guard([&stopcount]() { stopcount.fetch_add(1); });
	while (signal.load() == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	auto& sample_bins = thread_args.sample_bins;

	if (args.mode == MODE_CLEAN) {
		auto rc = cleanup(tx, args);
		if (rc < 0) {
			fmt::print(stderr, "ERROR: cleanup failed\n");
		}
	} else if (args.mode == MODE_BUILD) {
		auto rc = populate(tx, args, worker_id, thread_id, thread_tps, stats, sample_bins);
		if (rc < 0) {
			fmt::print(stderr, "ERROR: populate failed\n");
		}
	} else if (args.mode == MODE_RUN) {
		auto rc = run_workload(
		    tx, args, thread_tps, throttle_factor, thread_iters, signal, stats, sample_bins, dotrace, dotagging);
		if (rc < 0) {
			fmt::print(stderr, "ERROR: run_workload failed\n");
		}
	}

	if (args.mode == MODE_BUILD || args.mode == MODE_RUN) {
		const auto dirname = fmt::format("{}{}", TEMP_DATA_STORE, parent_id);
		const auto rc = mkdir(dirname.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
		if (rc < 0) {
			fmt::print(stderr, "ERROR: mkdir {}: {}\n", dirname, strerror(errno));
			return;
		}
		for (auto op = 0; op < MAX_OP; op++) {
			if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_COMMIT || op == OP_TRANSACTION) {
				const auto filename = get_stats_file_name(dirname, worker_id, thread_id, op);
				auto fp = fopen(filename.c_str(), "w");
				if (!fp) {
					fmt::print(stderr, "ERROR: fopen({}): {}\n", filename, strerror(errno));
					continue;
				}
				auto fclose_guard = exit_guard([fp]() { fclose(fp); });
				thread_args.sample_bins[op].for_each_block(
				    [fp](auto ptr, auto count) { fwrite(ptr, sizeof(*ptr) * count, 1, fp); });
			}
		}
	}
}

/* mako worker process */
int worker_process_main(mako_args_t& args, int worker_id, mako_shmhdr_t* shm, pid_t pid_main) {
	process_info_t process;

	process.worker_id = worker_id;
	process.parent_id = pid_main;
	process.args = &args;
	process.shm = shm;

	fmt::print(debugme, "DEBUG: worker {} started\n", worker_id);

	Error err;
	/* Everything starts from here */

	select_api_version(args.api_version);

	/* enable flatbuffers if specified */
	if (args.flatbuffers) {
#ifdef FDB_NET_OPTION_USE_FLATBUFFERS
		fprintf(debugme, "DEBUG: Using flatbuffers\n");
		err = network::set_option_nothrow(FDB_NET_OPTION_USE_FLATBUFFERS,
		                                  BytesRef(&args.flatbuffers, sizeof(args.flatbuffers)));
		if (err) {
			fmt::print(stderr, "ERROR: network_set_option(USE_FLATBUFFERS): {}\n", err.what());
		}
#else
		fmt::print(printme, "INFO: flatbuffers is not supported in FDB API version {}\n", FDB_API_VERSION);
#endif
	}

	/* Set client Log group */
	if (args.log_group[0] != '\0') {
		err = network::set_option_nothrow(FDB_NET_OPTION_TRACE_LOG_GROUP, BytesRef(to_byte_ptr(args.log_group)));
		if (err) {
			fmt::print(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_TRACE_LOG_GROUP): {}\n", err.what());
		}
	}

	/* enable tracing if specified */
	if (args.trace) {
		fmt::print(debugme,
		           "DEBUG: Enable Tracing in {} ({})\n",
		           (args.traceformat == 0) ? "XML" : "JSON",
		           (args.tracepath[0] == '\0') ? "current directory" : args.tracepath);
		err = network::set_option_nothrow(FDB_NET_OPTION_TRACE_ENABLE, BytesRef(to_byte_ptr(args.tracepath)));
		if (err) {
			fmt::print(stderr, "ERROR: network_set_option(TRACE_ENABLE): {}\n", err.what());
		}
		if (args.traceformat == 1) {
			err = network::set_option_nothrow(FDB_NET_OPTION_TRACE_FORMAT, BytesRef(to_byte_ptr("json")));
			if (err) {
				fmt::print(stderr, "ERROR: fdb_network_set_option(FDB_NET_OPTION_TRACE_FORMAT): {}\n", err.what());
			}
		}
	}

	/* enable knobs if specified */
	if (args.knobs[0] != '\0') {
		char delim[] = ", ";
		auto knob = strtok(args.knobs, delim);
		while (knob != NULL) {
			fmt::print(debugme, "DEBUG: Setting client knobs: {}\n", knob);
			err = network::set_option_nothrow(FDB_NET_OPTION_KNOB, BytesRef(to_byte_ptr(knob)));
			if (err) {
				fmt::print(stderr, "ERROR: fdb_network_set_option: {}\n", err.what());
			}
			knob = strtok(NULL, delim);
		}
	}

	if (args.client_threads_per_version > 0) {
		err = network::set_option_nothrow(FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, args.client_threads_per_version);
		if (err) {
			fmt::print(stderr,
			           "ERROR: fdb_network_set_option (FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION) ({}): {}\n",
			           args.client_threads_per_version,
			           err.what());
			// let's exit here since we do not want to confuse users
			// that mako is running with multi-threaded client enabled
			return -1;
		}
	}

	/* Network thread must be setup before doing anything */
	fprintf(debugme, "DEBUG: fdb_setup_network\n");
	network::setup();

	/* Each worker process will have its own network thread */
	fprintf(debugme, "DEBUG: creating network thread\n");
	auto network_thread = std::thread([]() {
		fmt::print(debugme, "DEBUG: network thread started\n");
		if (auto err = network::run()) {
			fmt::print(stderr, "ERROR: fdb_run_network: {}\n", err.what());
		}
	});

	/*** let's party! ***/

	/* set up database for worker threads */
	for (auto i = 0; i < args.num_databases; i++) {
		size_t cluster_index = args.num_fdb_clusters <= 1 ? 0 : i % args.num_fdb_clusters;
		process.databases.emplace_back(Database(args.cluster_files[cluster_index]));
		fmt::print(debugme, "DEBUG: creating database at cluster {}\n", args.cluster_files[cluster_index]);
		if (args.disable_ryw) {
			process.databases.back().set_option(FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE, BytesRef{});
		}
	}

	fmt::print(debugme, "DEBUG: creating {} worker threads\n", args.num_threads);
	auto worker_threads = std::vector<std::thread>(args.num_threads);

	/* spawn worker threads */
	auto thread_args = std::vector<thread_args_t>(args.num_threads);

	for (auto i = 0; i < args.num_threads; i++) {
		auto& this_args = thread_args[i];
		this_args.thread_id = i;
		this_args.database_index = i % args.num_databases;

		/* for ops to run, pre-allocate one latency sample block */
		for (int op = 0; op < MAX_OP; op++) {
			if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
				this_args.sample_bins[op].reserve_one();
			}
		}
		this_args.process = &process;
		worker_threads[i] = std::thread(worker_thread, std::ref(this_args));
	}

	/*** party is over ***/

	/* wait for everyone to finish */
	for (auto i = 0; i < args.num_threads; i++) {
		fmt::print(debugme, "DEBUG: worker_thread {} joining\n", i);
		worker_threads[i].join();
	}

	/* stop the network thread */
	fmt::print(debugme, "DEBUG: fdb_stop_network\n");
	err = network::stop();
	if (err) {
		fmt::print(stderr, "ERROR: stop_network(): {}\n", err.what());
	}

	/* wait for the network thread to join */
	fmt::print(debugme, "DEBUG: network_thread joining\n");
	network_thread.join();

	return 0;
}

/* initialize the parameters with default values */
int init_args(mako_args_t* args) {
	int i;
	if (!args)
		return -1;
	memset(args, 0, sizeof(mako_args_t)); /* zero-out everything */
	args->num_fdb_clusters = 0;
	args->num_databases = 1;
	args->api_version = max_api_version();
	args->json = 0;
	args->num_processes = 1;
	args->num_threads = 1;
	args->mode = MODE_INVALID;
	args->rows = 100000;
	args->row_digits = digits(args->rows);
	args->seconds = 30;
	args->iteration = 0;
	args->tpsmax = 0;
	args->tpsmin = -1;
	args->tpsinterval = 10;
	args->tpschange = TPS_SIN;
	args->sampling = 1000;
	args->key_length = 32;
	args->value_length = 16;
	args->zipf = 0;
	args->commit_get = 0;
	args->verbose = 1;
	args->flatbuffers = 0; /* internal */
	args->knobs[0] = '\0';
	args->log_group[0] = '\0';
	args->prefixpadding = 0;
	args->trace = 0;
	args->tracepath[0] = '\0';
	args->traceformat = 0; /* default to client's default (XML) */
	args->streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
	args->txntrace = 0;
	args->txntagging = 0;
	memset(args->txntagging_prefix, 0, TAGPREFIXLENGTH_MAX);
	for (i = 0; i < MAX_OP; i++) {
		args->txnspec.ops[i][OP_COUNT] = 0;
	}
	args->client_threads_per_version = 0;
	args->disable_ryw = 0;
	args->json_output_path[0] = '\0';
	args->bg_materialize_files = false;
	args->bg_file_path[0] = '\0';
	return 0;
}

/* parse transaction specification */
int parse_transaction(mako_args_t* args, char const* optarg) {
	char const* ptr = optarg;
	int op = 0;
	int rangeop = 0;
	int num;
	int error = 0;

	for (op = 0; op < MAX_OP; op++) {
		args->txnspec.ops[op][OP_COUNT] = 0;
		args->txnspec.ops[op][OP_RANGE] = 0;
	}

	op = 0;
	while (*ptr) {
// Clang gives false positive array bounds warning, which must be ignored:
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Warray-bounds"
		if (strncmp(ptr, "grv", 3) == 0) {
			op = OP_GETREADVERSION;
			ptr += 3;
		} else if (strncmp(ptr, "gr", 2) == 0) {
			op = OP_GETRANGE;
			rangeop = 1;
			ptr += 2;
		} else if (strncmp(ptr, "g", 1) == 0) {
			op = OP_GET;
			ptr++;
		} else if (strncmp(ptr, "sgr", 3) == 0) {
			op = OP_SGETRANGE;
			rangeop = 1;
			ptr += 3;
		} else if (strncmp(ptr, "sg", 2) == 0) {
			op = OP_SGET;
			ptr += 2;
		} else if (strncmp(ptr, "u", 1) == 0) {
			op = OP_UPDATE;
			ptr++;
		} else if (strncmp(ptr, "ir", 2) == 0) {
			op = OP_INSERTRANGE;
			rangeop = 1;
			ptr += 2;
		} else if (strncmp(ptr, "i", 1) == 0) {
			op = OP_INSERT;
			ptr++;
		} else if (strncmp(ptr, "cr", 2) == 0) {
			op = OP_CLEARRANGE;
			rangeop = 1;
			ptr += 2;
		} else if (strncmp(ptr, "c", 1) == 0) {
			op = OP_CLEAR;
			ptr++;
		} else if (strncmp(ptr, "scr", 3) == 0) {
			op = OP_SETCLEARRANGE;
			rangeop = 1;
			ptr += 3;
		} else if (strncmp(ptr, "sc", 2) == 0) {
			op = OP_SETCLEAR;
			ptr += 2;
		} else if (strncmp(ptr, "bg", 2) == 0) {
			op = OP_READ_BG;
			rangeop = 1;
			ptr += 2;
		} else {
			fprintf(debugme, "Error: Invalid transaction spec: %s\n", ptr);
			error = 1;
			break;
		}
#pragma clang diagnostic pop

		/* count */
		num = 0;
		if ((*ptr < '0') || (*ptr > '9')) {
			num = 1; /* if omitted, set it to 1 */
		} else {
			while ((*ptr >= '0') && (*ptr <= '9')) {
				num = num * 10 + *ptr - '0';
				ptr++;
			}
		}
		/* set count */
		args->txnspec.ops[op][OP_COUNT] = num;

		if (rangeop) {
			if (*ptr != ':') {
				error = 1;
				break;
			} else {
				ptr++; /* skip ':' */
				/* check negative '-' sign */
				if (*ptr == '-') {
					args->txnspec.ops[op][OP_REVERSE] = 1;
					ptr++;
				} else {
					args->txnspec.ops[op][OP_REVERSE] = 0;
				}
				num = 0;
				if ((*ptr < '0') || (*ptr > '9')) {
					error = 1;
					break;
				}
				while ((*ptr >= '0') && (*ptr <= '9')) {
					num = num * 10 + *ptr - '0';
					ptr++;
				}
				/* set range */
				args->txnspec.ops[op][OP_RANGE] = num;
			}
		}
		rangeop = 0;
	}

	if (error) {
		fprintf(stderr, "ERROR: invalid transaction specification %s\n", optarg);
		return -1;
	}

	if (args->verbose == VERBOSE_DEBUG) {
		for (op = 0; op < MAX_OP; op++) {
			fprintf(debugme, "DEBUG: OP: %d: %d: %d\n", op, args->txnspec.ops[op][0], args->txnspec.ops[op][1]);
		}
	}

	return 0;
}

void usage() {
	printf("Usage:\n");
	printf("%-24s %s\n", "-h, --help", "Print this message");
	printf("%-24s %s\n", "    --version", "Print FDB version");
	printf("%-24s %s\n", "-v, --verbose", "Specify verbosity");
	printf("%-24s %s\n", "-a, --api_version=API_VERSION", "Specify API_VERSION to use");
	printf("%-24s %s\n", "-c, --cluster=FILE", "Specify FDB cluster file");
	printf("%-24s %s\n", "-d, --num_databases=NUM_DATABASES", "Specify number of databases");
	printf("%-24s %s\n", "-p, --procs=PROCS", "Specify number of worker processes");
	printf("%-24s %s\n", "-t, --threads=THREADS", "Specify number of worker threads");
	printf("%-24s %s\n", "-r, --rows=ROWS", "Specify number of records");
	printf("%-24s %s\n", "-s, --seconds=SECONDS", "Specify the test duration in seconds\n");
	printf("%-24s %s\n", "", "This option cannot be specified with --iteration.");
	printf("%-24s %s\n", "-i, --iteration=ITERS", "Specify the number of iterations.\n");
	printf("%-24s %s\n", "", "This option cannot be specified with --seconds.");
	printf("%-24s %s\n", "    --keylen=LENGTH", "Specify the key lengths");
	printf("%-24s %s\n", "    --vallen=LENGTH", "Specify the value lengths");
	printf("%-24s %s\n", "-x, --transaction=SPEC", "Transaction specification");
	printf("%-24s %s\n", "    --tps|--tpsmax=TPS", "Specify the target max TPS");
	printf("%-24s %s\n", "    --tpsmin=TPS", "Specify the target min TPS");
	printf("%-24s %s\n", "    --tpsinterval=SEC", "Specify the TPS change interval (Default: 10 seconds)");
	printf("%-24s %s\n", "    --tpschange=<sin|square|pulse>", "Specify the TPS change type (Default: sin)");
	printf("%-24s %s\n", "    --sampling=RATE", "Specify the sampling rate for latency stats");
	printf("%-24s %s\n", "-m, --mode=MODE", "Specify the mode (build, run, clean)");
	printf("%-24s %s\n", "-z, --zipf", "Use zipfian distribution instead of uniform distribution");
	printf("%-24s %s\n", "    --commitget", "Commit GETs");
	printf("%-24s %s\n", "    --loggroup=LOGGROUP", "Set client log group");
	printf("%-24s %s\n", "    --prefix_padding", "Pad key by prefixing data (Default: postfix padding)");
	printf("%-24s %s\n", "    --trace", "Enable tracing");
	printf("%-24s %s\n", "    --tracepath=PATH", "Set trace file path");
	printf("%-24s %s\n", "    --trace_format <xml|json>", "Set trace format (Default: json)");
	printf("%-24s %s\n", "    --txntrace=sec", "Specify transaction tracing interval (Default: 0)");
	printf(
	    "%-24s %s\n", "    --txntagging", "Specify the number of different transaction tag (Default: 0, max = 1000)");
	printf("%-24s %s\n",
	       "    --txntagging_prefix",
	       "Specify the prefix of transaction tag - mako${txntagging_prefix} (Default: '')");
	printf("%-24s %s\n", "    --knobs=KNOBS", "Set client knobs");
	printf("%-24s %s\n", "    --flatbuffers", "Use flatbuffers");
	printf("%-24s %s\n", "    --streaming", "Streaming mode: all (default), iterator, small, medium, large, serial");
	printf("%-24s %s\n", "    --disable_ryw", "Disable snapshot read-your-writes");
	printf("%-24s %s\n", "    --json_report=PATH", "Output stats to the specified json file (Default: mako.json)");
	printf("%-24s %s\n",
	       "    --bg_file_path=PATH",
	       "Read blob granule files from the local filesystem at PATH and materialize the results.");
}

/* parse benchmark paramters */
int parse_args(int argc, char* argv[], mako_args_t* args) {
	int rc;
	int c;
	int idx;
	while (1) {
		const char* short_options = "a:c:d:p:t:r:s:i:x:v:m:hz";
		static struct option long_options[] = {
			/* name, has_arg, flag, val */
			{ "api_version", required_argument, NULL, 'a' },
			{ "cluster", required_argument, NULL, 'c' },
			{ "num_databases", optional_argument, NULL, 'd' },
			{ "procs", required_argument, NULL, 'p' },
			{ "threads", required_argument, NULL, 't' },
			{ "rows", required_argument, NULL, 'r' },
			{ "seconds", required_argument, NULL, 's' },
			{ "iteration", required_argument, NULL, 'i' },
			{ "keylen", required_argument, NULL, ARG_KEYLEN },
			{ "vallen", required_argument, NULL, ARG_VALLEN },
			{ "transaction", required_argument, NULL, 'x' },
			{ "tps", required_argument, NULL, ARG_TPS },
			{ "tpsmax", required_argument, NULL, ARG_TPSMAX },
			{ "tpsmin", required_argument, NULL, ARG_TPSMIN },
			{ "tpsinterval", required_argument, NULL, ARG_TPSINTERVAL },
			{ "tpschange", required_argument, NULL, ARG_TPSCHANGE },
			{ "sampling", required_argument, NULL, ARG_SAMPLING },
			{ "verbose", required_argument, NULL, 'v' },
			{ "mode", required_argument, NULL, 'm' },
			{ "knobs", required_argument, NULL, ARG_KNOBS },
			{ "loggroup", required_argument, NULL, ARG_LOGGROUP },
			{ "tracepath", required_argument, NULL, ARG_TRACEPATH },
			{ "trace_format", required_argument, NULL, ARG_TRACEFORMAT },
			{ "streaming", required_argument, NULL, ARG_STREAMING_MODE },
			{ "txntrace", required_argument, NULL, ARG_TXNTRACE },
			/* no args */
			{ "help", no_argument, NULL, 'h' },
			{ "zipf", no_argument, NULL, 'z' },
			{ "commitget", no_argument, NULL, ARG_COMMITGET },
			{ "flatbuffers", no_argument, NULL, ARG_FLATBUFFERS },
			{ "prefix_padding", no_argument, NULL, ARG_PREFIXPADDING },
			{ "trace", no_argument, NULL, ARG_TRACE },
			{ "txntagging", required_argument, NULL, ARG_TXNTAGGING },
			{ "txntagging_prefix", required_argument, NULL, ARG_TXNTAGGINGPREFIX },
			{ "version", no_argument, NULL, ARG_VERSION },
			{ "client_threads_per_version", required_argument, NULL, ARG_CLIENT_THREADS_PER_VERSION },
			{ "disable_ryw", no_argument, NULL, ARG_DISABLE_RYW },
			{ "json_report", optional_argument, NULL, ARG_JSON_REPORT },
			{ "bg_file_path", required_argument, NULL, ARG_BG_FILE_PATH },
			{ NULL, 0, NULL, 0 }
		};
		idx = 0;
		c = getopt_long(argc, argv, short_options, long_options, &idx);
		if (c < 0) {
			break;
		}
		switch (c) {
		case '?':
		case 'h':
			usage();
			return -1;
		case 'a':
			args->api_version = atoi(optarg);
			break;
		case 'c': {
			const char delim[] = ",";
			char* cluster_file = strtok(optarg, delim);
			while (cluster_file != NULL) {
				strcpy(args->cluster_files[args->num_fdb_clusters++], cluster_file);
				cluster_file = strtok(NULL, delim);
			}
			break;
		}
		case 'd':
			args->num_databases = atoi(optarg);
			break;
		case 'p':
			args->num_processes = atoi(optarg);
			break;
		case 't':
			args->num_threads = atoi(optarg);
			break;
		case 'r':
			args->rows = atoi(optarg);
			args->row_digits = digits(args->rows);
			break;
		case 's':
			args->seconds = atoi(optarg);
			break;
		case 'i':
			args->iteration = atoi(optarg);
			break;
		case 'x':
			rc = parse_transaction(args, optarg);
			if (rc < 0)
				return -1;
			break;
		case 'v':
			args->verbose = atoi(optarg);
			break;
		case 'z':
			args->zipf = 1;
			break;
		case 'm':
			if (strcmp(optarg, "clean") == 0) {
				args->mode = MODE_CLEAN;
			} else if (strcmp(optarg, "build") == 0) {
				args->mode = MODE_BUILD;
			} else if (strcmp(optarg, "run") == 0) {
				args->mode = MODE_RUN;
			}
			break;
		case ARG_KEYLEN:
			args->key_length = atoi(optarg);
			break;
		case ARG_VALLEN:
			args->value_length = atoi(optarg);
			break;
		case ARG_TPS:
		case ARG_TPSMAX:
			args->tpsmax = atoi(optarg);
			break;
		case ARG_TPSMIN:
			args->tpsmin = atoi(optarg);
			break;
		case ARG_TPSINTERVAL:
			args->tpsinterval = atoi(optarg);
			break;
		case ARG_TPSCHANGE:
			if (strcmp(optarg, "sin") == 0)
				args->tpschange = TPS_SIN;
			else if (strcmp(optarg, "square") == 0)
				args->tpschange = TPS_SQUARE;
			else if (strcmp(optarg, "pulse") == 0)
				args->tpschange = TPS_PULSE;
			else {
				fprintf(stderr, "--tpschange must be sin, square or pulse\n");
				return -1;
			}
			break;
		case ARG_SAMPLING:
			args->sampling = atoi(optarg);
			break;
		case ARG_VERSION:
			fprintf(stderr, "Version: %d\n", FDB_API_VERSION);
			exit(0);
			break;
		case ARG_COMMITGET:
			args->commit_get = 1;
			break;
		case ARG_FLATBUFFERS:
			args->flatbuffers = 1;
			break;
		case ARG_KNOBS:
			memcpy(args->knobs, optarg, strlen(optarg) + 1);
			break;
		case ARG_LOGGROUP:
			memcpy(args->log_group, optarg, strlen(optarg) + 1);
			break;
		case ARG_PREFIXPADDING:
			args->prefixpadding = 1;
			break;
		case ARG_TRACE:
			args->trace = 1;
			break;
		case ARG_TRACEPATH:
			args->trace = 1;
			memcpy(args->tracepath, optarg, strlen(optarg) + 1);
			break;
		case ARG_TRACEFORMAT:
			if (strncmp(optarg, "json", 5) == 0) {
				args->traceformat = 1;
			} else if (strncmp(optarg, "xml", 4) == 0) {
				args->traceformat = 0;
			} else {
				fprintf(stderr, "Error: Invalid trace_format %s\n", optarg);
				return -1;
			}
			break;
		case ARG_STREAMING_MODE:
			if (strncmp(optarg, "all", 3) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
			} else if (strncmp(optarg, "iterator", 8) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_ITERATOR;
			} else if (strncmp(optarg, "small", 5) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_SMALL;
			} else if (strncmp(optarg, "medium", 6) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_MEDIUM;
			} else if (strncmp(optarg, "large", 5) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_LARGE;
			} else if (strncmp(optarg, "serial", 6) == 0) {
				args->streaming_mode = FDB_STREAMING_MODE_SERIAL;
			} else {
				fprintf(stderr, "Error: Invalid streaming mode %s\n", optarg);
				return -1;
			}
			break;
		case ARG_TXNTRACE:
			args->txntrace = atoi(optarg);
			break;

		case ARG_TXNTAGGING:
			args->txntagging = atoi(optarg);
			if (args->txntagging > 1000) {
				args->txntagging = 1000;
			}
			break;
		case ARG_TXNTAGGINGPREFIX:
			if (strlen(optarg) > TAGPREFIXLENGTH_MAX) {
				fprintf(stderr, "Error: the length of txntagging_prefix is larger than %d\n", TAGPREFIXLENGTH_MAX);
				exit(0);
			}
			memcpy(args->txntagging_prefix, optarg, strlen(optarg));
			break;
		case ARG_CLIENT_THREADS_PER_VERSION:
			args->client_threads_per_version = atoi(optarg);
			break;
		case ARG_DISABLE_RYW:
			args->disable_ryw = 1;
			break;
		case ARG_JSON_REPORT:
			if (optarg == NULL && (argv[optind] == NULL || (argv[optind] != NULL && argv[optind][0] == '-'))) {
				// if --report_json is the last option and no file is specified
				// or --report_json is followed by another option
				char default_file[] = "mako.json";
				strncpy(args->json_output_path, default_file, strlen(default_file));
			} else {
				strncpy(args->json_output_path, optarg, strlen(optarg) + 1);
			}
			break;
		case ARG_BG_FILE_PATH:
			args->bg_materialize_files = true;
			strncpy(args->bg_file_path, optarg, strlen(optarg) + 1);
		}
	}

	if ((args->tpsmin == -1) || (args->tpsmin > args->tpsmax)) {
		args->tpsmin = args->tpsmax;
	}

	if (args->verbose >= VERBOSE_DEFAULT) {
		printme = stdout;
	} else {
		printme = fopen("/dev/null", "w");
	}
	if (args->verbose >= VERBOSE_ANNOYING) {
		annoyme = stdout;
	} else {
		annoyme = fopen("/dev/null", "w");
	}
	if (args->verbose >= VERBOSE_DEBUG) {
		debugme = stdout;
	} else {
		debugme = fopen("/dev/null", "w");
	}

	return 0;
}

char const* get_ops_name(int ops_code) {
	if (ops_code >= 0 && ops_code < MAX_OP)
		return op_desc[ops_code].name().data();
	return "";
}

int validate_args(mako_args_t* args) {
	if (args->mode == MODE_INVALID) {
		fprintf(stderr, "ERROR: --mode has to be set\n");
		return -1;
	}
	if (args->rows <= 0) {
		fprintf(stderr, "ERROR: --rows must be a positive integer\n");
		return -1;
	}
	if (args->key_length < 0) {
		fprintf(stderr, "ERROR: --keylen must be a positive integer\n");
		return -1;
	}
	if (args->value_length < 0) {
		fprintf(stderr, "ERROR: --vallen must be a positive integer\n");
		return -1;
	}
	if (args->num_fdb_clusters > NUM_CLUSTERS_MAX) {
		fprintf(stderr, "ERROR: Mako is not supported to do work to more than %d clusters\n", NUM_CLUSTERS_MAX);
		return -1;
	}
	if (args->num_databases > NUM_DATABASES_MAX) {
		fprintf(stderr, "ERROR: Mako is not supported to do work to more than %d databases\n", NUM_DATABASES_MAX);
		return -1;
	}
	if (args->num_databases < args->num_fdb_clusters) {
		fprintf(stderr,
		        "ERROR: --num_databases (%d) must be >= number of clusters(%d)\n",
		        args->num_databases,
		        args->num_fdb_clusters);
		return -1;
	}
	if (args->num_threads < args->num_databases) {
		fprintf(stderr,
		        "ERROR: --threads (%d) must be >= number of databases (%d)\n",
		        args->num_threads,
		        args->num_databases);
		return -1;
	}
	if (args->key_length < 4 /* "mako" */ + args->row_digits) {
		fprintf(stderr,
		        "ERROR: --keylen must be larger than %d to store \"mako\" prefix "
		        "and maximum row number\n",
		        4 + args->row_digits);
		return -1;
	}
	if (args->mode == MODE_RUN) {
		if ((args->seconds > 0) && (args->iteration > 0)) {
			fprintf(stderr, "ERROR: Cannot specify seconds and iteration together\n");
			return -1;
		}
		if ((args->seconds == 0) && (args->iteration == 0)) {
			fprintf(stderr, "ERROR: Must specify either seconds or iteration\n");
			return -1;
		}
		if (args->txntagging < 0) {
			fprintf(stderr, "ERROR: --txntagging must be a non-negative integer\n");
			return -1;
		}
	}
	return 0;
}

void print_stats(mako_args_t const& args, mako_stats_t const* stats, double const duration_sec, FILE* fp) {
	static mako_stats_t prev;

	auto current = mako_stats_t{};
	for (auto i = 0; i < args.num_processes; i++) {
		for (auto j = 0; j < args.num_threads; j++) {
			current.combine(stats[(i * args.num_threads) + j]);
		}
	}

	if (fp) {
		fwrite("{", 1, 1, fp);
	}
	put_title_r("OPS");
	auto print_err = false;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0) {
			const auto ops_total_diff = current.get_op_count(op) - prev.get_op_count(op);
			put_field(ops_total_diff);
			if (fp) {
				fmt::print(fp, "\"{}\": {},", get_ops_name(op), ops_total_diff);
			}
			print_err = print_err || (current.get_error_count(op) - prev.get_error_count(op)) > 0;
		}
	}
	/* TPS */
	const auto tps = (current.get_tx_count() - prev.get_tx_count()) / duration_sec;
	put_field_f(tps, 2);
	if (fp) {
		fprintf(fp, "\"tps\": %.2f,", tps);
	}

	/* Conflicts */
	const auto conflicts_diff = (current.get_conflict_count() - prev.get_conflict_count()) / duration_sec;
	put_field_f(conflicts_diff, 2);
	fmt::print("\n");
	if (fp) {
		fprintf(fp, "\"conflictsPerSec\": %.2f", conflicts_diff);
	}

	if (print_err) {
		put_title_r("Errors");
		for (auto op = 0; op < MAX_OP; op++) {
			if (args.txnspec.ops[op][OP_COUNT] > 0) {
				const auto errors_diff = current.get_error_count(op) - prev.get_error_count(op);
				put_field(errors_diff);
				if (fp) {
					fmt::print(fp, ",\"errors\": {}", errors_diff);
				}
			}
		}
		printf("\n");
	}
	if (fp) {
		fprintf(fp, "}");
	}
	// swap old stats for new
	prev = current;
}

void print_stats_header(mako_args_t const& args, bool show_commit, bool is_first_header_empty, bool show_op_stats) {
	/* header */
	if (is_first_header_empty)
		put_title("");
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0) {
			put_field(get_ops_name(op));
		}
	}

	if (show_commit)
		put_field("COMMIT");
	if (show_op_stats) {
		put_field("TRANSACTION");
	} else {
		put_field("TPS");
		put_field("Conflicts/s");
	}
	fmt::print("\n");

	put_title_bar();
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0) {
			put_field_bar();
		}
	}

	/* COMMIT */
	if (show_commit)
		put_field_bar();

	if (show_op_stats) {
		/* TRANSACTION */
		put_field_bar();
	} else {
		/* TPS */
		put_field_bar();

		/* Conflicts */
		put_field_bar();
	}
	fmt::print("\n");
}

void print_report(mako_args_t const& args,
                  mako_stats_t const* stats,
                  double const duration_sec,
                  pid_t pid_main,
                  FILE* fp) {

	auto final_stats = mako_stats_t{};
	for (auto i = 0; i < args.num_processes; i++) {
		for (auto j = 0; j < args.num_threads; j++) {
			const auto idx = i * args.num_threads + j;
			final_stats.combine(stats[idx]);
		}
	}

	/* overall stats */
	fmt::printf("\n====== Total Duration %6.3f sec ======\n\n", duration_sec);
	fmt::printf("Total Processes:  %8d\n", args.num_processes);
	fmt::printf("Total Threads:    %8d\n", args.num_threads);
	if (args.tpsmax == args.tpsmin)
		fmt::printf("Target TPS:       %8d\n", args.tpsmax);
	else {
		fmt::printf("Target TPS (MAX): %8d\n", args.tpsmax);
		fmt::printf("Target TPS (MIN): %8d\n", args.tpsmin);
		fmt::printf("TPS Interval:     %8d\n", args.tpsinterval);
		fmt::printf("TPS Change:       ");
		switch (args.tpschange) {
		case TPS_SIN:
			fmt::printf("%8s\n", "SIN");
			break;
		case TPS_SQUARE:
			fmt::printf("%8s\n", "SQUARE");
			break;
		case TPS_PULSE:
			fmt::printf("%8s\n", "PULSE");
			break;
		}
	}
	const auto tps_f = final_stats.get_tx_count() / duration_sec;
	const auto tps_i = static_cast<uint64_t>(tps_f);
	fmt::printf("Total Xacts:      %8lu\n", final_stats.get_tx_count());
	fmt::printf("Total Conflicts:  %8lu\n", final_stats.get_conflict_count());
	fmt::printf("Total Errors:     %8lu\n", final_stats.get_total_error_count());
	fmt::printf("Overall TPS:      %8lu\n\n", tps_i);

	if (fp) {
		fmt::fprintf(fp, "\"results\": {");
		fmt::fprintf(fp, "\"totalDuration\": %6.3f,", duration_sec);
		fmt::fprintf(fp, "\"totalProcesses\": %d,", args.num_processes);
		fmt::fprintf(fp, "\"totalThreads\": %d,", args.num_threads);
		fmt::fprintf(fp, "\"targetTPS\": %d,", args.tpsmax);
		fmt::fprintf(fp, "\"totalXacts\": %lu,", final_stats.get_tx_count());
		fmt::fprintf(fp, "\"totalConflicts\": %lu,", final_stats.get_conflict_count());
		fmt::fprintf(fp, "\"totalErrors\": %lu,", final_stats.get_total_error_count());
		fmt::fprintf(fp, "\"overallTPS\": %lu,", tps_i);
	}

	/* per-op stats */
	print_stats_header(args, true, true, false);

	/* OPS */
	put_title("Total OPS");
	if (fp) {
		fmt::fprintf(fp, "\"totalOps\": {");
	}
	auto first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if ((args.txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) || op == OP_COMMIT) {
			put_field(final_stats.get_op_count(op));
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), final_stats.get_op_count(op));
			}
		}
	}

	/* TPS */
	const auto tps = final_stats.get_tx_count() / duration_sec;
	put_field_f(tps, 2);

	/* Conflicts */
	const auto conflicts_rate = final_stats.get_conflict_count() / duration_sec;
	put_field_f(conflicts_rate, 2);
	fmt::print("\n");

	if (fp) {
		fmt::fprintf(fp, "}, \"tps\": %.2f, \"conflictsPerSec\": %.2f, \"errors\": {", tps, conflicts_rate);
	}

	/* Errors */
	put_title("Errors");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) {
			put_field(final_stats.get_error_count(op));
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), final_stats.get_error_count(op));
			}
		}
	}
	if (fp) {
		fmt::fprintf(fp, "}, \"numSamples\": {");
	}
	fmt::print("\n\n");

	fmt::print("Latency (us)");
	print_stats_header(args, true, false, true);

	/* Total Samples */
	put_title("Samples");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (final_stats.get_latency_us_total(op)) {
				put_field(final_stats.get_latency_sample_count(op));
			} else {
				put_field("N/A");
			}
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), final_stats.get_latency_sample_count(op));
			}
		}
	}
	fmt::print("\n");

	/* Min Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"minLatency\": {");
	}
	put_title("Min");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			const auto lat_min = final_stats.get_latency_us_min(op);
			if (lat_min == -1) {
				put_field("N/A");
			} else {
				put_field(lat_min);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), lat_min);
				}
			}
		}
	}
	fmt::print("\n");

	/* Avg Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"avgLatency\": {");
	}
	put_title("Avg");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			const auto lat_total = final_stats.get_latency_us_total(op);
			const auto lat_samples = final_stats.get_latency_sample_count(op);
			if (lat_total) {
				put_field(lat_total / lat_samples);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), lat_total / lat_samples);
				}
			} else {
				put_field("N/A");
			}
		}
	}
	fmt::printf("\n");

	/* Max Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"maxLatency\": {");
	}
	put_title("Max");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			const auto lat_max = final_stats.get_latency_us_max(op);
			if (lat_max == 0) {
				put_field("N/A");
			} else {
				put_field(lat_max);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), final_stats.get_latency_us_max(op));
				}
			}
		}
	}
	fmt::print("\n");

	auto data_points = std::array<std::vector<uint64_t>, MAX_OP>{};

	/* Median Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"medianLatency\": {");
	}
	put_title("Median");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			const auto lat_total = final_stats.get_latency_us_total(op);
			const auto lat_samples = final_stats.get_latency_sample_count(op);
			if (lat_total && lat_samples) {
				data_points[op].reserve(lat_samples);
				for (auto i = 0; i < args.num_processes; i++) {
					for (auto j = 0; j < args.num_threads; j++) {
						const auto dirname = fmt::format("{}{}", TEMP_DATA_STORE, pid_main);
						const auto filename = get_stats_file_name(dirname, i, j, op);
						auto fp = fopen(filename.c_str(), "r");
						if (!fp) {
							fmt::print(stderr, "ERROR: fopen({}): {}\n", strerror(errno));
							continue;
						}
						auto fclose_guard = exit_guard([fp]() { fclose(fp); });
						fseek(fp, 0, SEEK_END);
						const auto num_points = ftell(fp) / sizeof(uint64_t);
						fseek(fp, 0, 0);
						auto index = 0u;
						while (index < num_points) {
							auto value = uint64_t{};
							fread(&value, sizeof(uint64_t), 1, fp);
							data_points[op].push_back(value);
							++index;
						}
					}
				}
				std::sort(data_points[op].begin(), data_points[op].end());
				const auto num_points = data_points[op].size();
				auto median = uint64_t{};
				if (num_points & 1) {
					median = data_points[op][num_points / 2];
				} else {
					median = (data_points[op][num_points / 2] + data_points[op][num_points / 2 - 1]) >> 1;
				}
				put_field(median);
				if (fp) {
					if (first_op) {
						first_op = 0;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), median);
				}
			} else {
				put_field("N/A");
			}
		}
	}
	fmt::print("\n");

	/* 95%ile Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"p95Latency\": {");
	}
	put_title("95.0 pctile");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (data_points[op].empty() || !final_stats.get_latency_us_total(op)) {
				put_field("N/A");
				continue;
			}
			const auto num_points = data_points[op].size();
			const auto point_95pct = static_cast<size_t>(std::max(0., (num_points * 0.95) - 1));
			put_field(data_points[op][point_95pct]);
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), data_points[op][point_95pct]);
			}
		}
	}
	fmt::printf("\n");

	/* 99%ile Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"p99Latency\": {");
	}
	put_title("99.0 pctile");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (data_points[op].empty() || !final_stats.get_latency_us_total(op)) {
				put_field("N/A");
				continue;
			}
			const auto num_points = data_points[op].size();
			const auto point_99pct = static_cast<size_t>(std::max(0., (num_points * 0.99) - 1));
			put_field(data_points[op][point_99pct]);
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), data_points[op][point_99pct]);
			}
		}
	}
	fmt::print("\n");

	/* 99.9%ile Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"p99.9Latency\": {");
	}
	put_title("99.9 pctile");
	first_op = 1;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || op == OP_TRANSACTION || op == OP_COMMIT) {
			if (data_points[op].empty() || !final_stats.get_latency_us_total(op)) {
				put_field("N/A");
				continue;
			}
			const auto num_points = data_points[op].size();
			const auto point_99_9pct = static_cast<size_t>(std::max(0., (num_points * 0.999) - 1));
			put_field(data_points[op][point_99_9pct]);
			if (fp) {
				if (first_op) {
					first_op = 0;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", get_ops_name(op), data_points[op][point_99_9pct]);
			}
		}
	}
	fmt::print("\n");
	if (fp) {
		fmt::fprintf(fp, "}}");
	}

	const auto command_remove = fmt::format("rm -rf {}{}", TEMP_DATA_STORE, pid_main);
	system(command_remove.c_str());
}

int stats_process_main(mako_args_t const& args,
                       mako_stats_t* stats,
                       std::atomic<double>& throttle_factor,
                       std::atomic<int>& signal,
                       std::atomic<int>& stopcount,
                       pid_t pid_main) {
	bool first_stats = true;

	/* wait until the signal turn on */
	while (signal.load() == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	if (args.verbose >= VERBOSE_DEFAULT)
		print_stats_header(args, false, true, false);

	FILE* fp = NULL;
	if (args.json_output_path[0] != '\0') {
		fp = fopen(args.json_output_path, "w");
		fmt::fprintf(fp, "{\"makoArgs\": {");
		fmt::fprintf(fp, "\"api_version\": %d,", args.api_version);
		fmt::fprintf(fp, "\"json\": %d,", args.json);
		fmt::fprintf(fp, "\"num_processes\": %d,", args.num_processes);
		fmt::fprintf(fp, "\"num_threads\": %d,", args.num_threads);
		fmt::fprintf(fp, "\"mode\": %d,", args.mode);
		fmt::fprintf(fp, "\"rows\": %d,", args.rows);
		fmt::fprintf(fp, "\"seconds\": %d,", args.seconds);
		fmt::fprintf(fp, "\"iteration\": %d,", args.iteration);
		fmt::fprintf(fp, "\"tpsmax\": %d,", args.tpsmax);
		fmt::fprintf(fp, "\"tpsmin\": %d,", args.tpsmin);
		fmt::fprintf(fp, "\"tpsinterval\": %d,", args.tpsinterval);
		fmt::fprintf(fp, "\"tpschange\": %d,", args.tpschange);
		fmt::fprintf(fp, "\"sampling\": %d,", args.sampling);
		fmt::fprintf(fp, "\"key_length\": %d,", args.key_length);
		fmt::fprintf(fp, "\"value_length\": %d,", args.value_length);
		fmt::fprintf(fp, "\"commit_get\": %d,", args.commit_get);
		fmt::fprintf(fp, "\"verbose\": %d,", args.verbose);
		fmt::fprintf(fp, "\"cluster_files\": \"%s\",", args.cluster_files[0]);
		fmt::fprintf(fp, "\"log_group\": \"%s\",", args.log_group);
		fmt::fprintf(fp, "\"prefixpadding\": %d,", args.prefixpadding);
		fmt::fprintf(fp, "\"trace\": %d,", args.trace);
		fmt::fprintf(fp, "\"tracepath\": \"%s\",", args.tracepath);
		fmt::fprintf(fp, "\"traceformat\": %d,", args.traceformat);
		fmt::fprintf(fp, "\"knobs\": \"%s\",", args.knobs);
		fmt::fprintf(fp, "\"flatbuffers\": %d,", args.flatbuffers);
		fmt::fprintf(fp, "\"txntrace\": %d,", args.txntrace);
		fmt::fprintf(fp, "\"txntagging\": %d,", args.txntagging);
		fmt::fprintf(fp, "\"txntagging_prefix\": \"%s\",", args.txntagging_prefix);
		fmt::fprintf(fp, "\"streaming_mode\": %d,", args.streaming_mode);
		fmt::fprintf(fp, "\"disable_ryw\": %d,", args.disable_ryw);
		fmt::fprintf(fp, "\"json_output_path\": \"%s\"", args.json_output_path);
		fmt::fprintf(fp, "},\"samples\": [");
	}

	const auto time_start = steady_clock::now();
	auto time_prev = time_start;
	while (signal.load() != SIGNAL_RED) {
		usleep(100000); /* sleep for 100ms */
		auto time_now = steady_clock::now();

		/* print stats every (roughly) 1 sec */
		if (to_double_seconds(time_now - time_prev) >= 1.0) {

			/* adjust throttle rate if needed */
			if (args.tpsmax != args.tpsmin) {
				const auto tpsinterval = static_cast<double>(args.tpsinterval);
				const auto tpsmin = static_cast<double>(args.tpsmin);
				const auto tpsmax = static_cast<double>(args.tpsmax);
				const auto pos = fmod(to_double_seconds(time_now - time_start), tpsinterval);
				auto sin_factor = 0.;
				/* set the throttle factor between 0.0 and 1.0 */
				switch (args.tpschange) {
				case TPS_SIN:
					sin_factor = sin(pos / tpsinterval * M_PI * 2.0) / 2.0 + 0.5;
					throttle_factor = 1 - (sin_factor * (1.0 - (tpsmin / tpsmax)));
					break;
				case TPS_SQUARE:
					if (pos < (args.tpsinterval / 2)) {
						/* set to max */
						throttle_factor = 1.0;
					} else {
						/* set to min */
						throttle_factor = tpsmin / tpsmax;
					}
					break;
				case TPS_PULSE:
					if (pos < (1.0 / tpsinterval)) {
						/* set to max */
						throttle_factor = 1.0;
					} else {
						/* set to min */
						throttle_factor = tpsmin / tpsmax;
					}
					break;
				}
			}

			if (args.verbose >= VERBOSE_DEFAULT) {
				if (first_stats) {
					first_stats = false;
				} else {
					if (fp)
						fmt::fprintf(fp, ",");
				}
				print_stats(args, stats, to_double_seconds(time_now - time_prev), fp);
			}
			time_prev = time_now;
		}
	}

	if (fp) {
		fmt::fprintf(fp, "],");
	}

	/* print report */
	if (args.verbose >= VERBOSE_DEFAULT) {
		auto time_now = steady_clock::now();
		while (stopcount.load() < args.num_threads * args.num_processes) {
			usleep(10000); /* 10ms */
		}
		print_report(args, stats, to_double_seconds(time_now - time_start), pid_main, fp);
	}

	if (fp) {
		fmt::fprintf(fp, "}");
		fclose(fp);
	}

	return 0;
}

int main(int argc, char* argv[]) {
	setlinebuf(stdout);

	auto rc = int{};
	auto args = mako_args_t{};
	rc = init_args(&args);
	if (rc < 0) {
		fmt::print(stderr, "ERROR: init_args failed\n");
		return -1;
	}
	rc = parse_args(argc, argv, &args);
	if (rc < 0) {
		/* usage printed */
		return 0;
	}

	rc = validate_args(&args);
	if (rc < 0)
		return -1;

	if (args.mode == MODE_CLEAN) {
		/* cleanup will be done from a single thread */
		args.num_processes = 1;
		args.num_threads = 1;
	}

	if (args.mode == MODE_BUILD) {
		if (args.txnspec.ops[OP_INSERT][OP_COUNT] == 0) {
			parse_transaction(&args, "i100");
		}
	}

	const auto pid_main = getpid();
	/* create the shared memory for stats */
	const auto shmpath = fmt::format("mako{}", pid_main);
	auto shmfd = shm_open(shmpath.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (shmfd < 0) {
		fmt::print(stderr, "ERROR: shm_open failed\n");
		return -1;
	}
	auto shmfd_guard = exit_guard([shmfd, &shmpath]() {
		close(shmfd);
		shm_unlink(shmpath.c_str());
		unlink(shmpath.c_str());
	});

	/* allocate */
	const auto shmsize = sizeof(mako_shmhdr_t) + (sizeof(mako_stats_t) * args.num_processes * args.num_threads);
	auto shm = static_cast<mako_shmhdr_t*>(nullptr);
	if (ftruncate(shmfd, shmsize) < 0) {
		shm = static_cast<mako_shmhdr_t*>(MAP_FAILED);
		fmt::print(stderr, "ERROR: ftruncate (fd:{} size:{}) failed\n", shmfd, shmsize);
		return -1;
	}

	/* map it */
	shm = static_cast<mako_shmhdr_t*>(mmap(NULL, shmsize, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0));
	if (shm == MAP_FAILED) {
		fmt::print(stderr, "ERROR: mmap (fd:{} size:{}) failed\n", shmfd, shmsize);
		return -1;
	}
	auto munmap_guard = exit_guard([=]() { munmap(shm, shmsize); });

	static_assert(std::is_same_v<decltype(shm), mako_shmhdr_t*>);
	auto stats = reinterpret_cast<mako_stats_t*>(shm + 1);

	/* initialize the shared memory */
	memset(shm, 0, shmsize);

	/* get ready */
	shm->signal = SIGNAL_OFF;
	shm->readycount = 0;
	shm->stopcount = 0;
	shm->throttle_factor = 1.0;

	auto proc_type = proc_master;
	/* fork worker processes + 1 stats process */
	auto worker_pids = std::vector<pid_t>(args.num_processes + 1);

	auto worker_id = int{};

	/* forking (num_process + 1) children */
	/* last process is the stats handler */
	for (auto p = 0; p < args.num_processes + 1; p++) {
		auto pid = fork();
		if (pid != 0) {
			/* master */
			worker_pids[p] = pid;
			if (args.verbose == VERBOSE_DEBUG) {
				printf("DEBUG: worker %d (PID:%d) forked\n", p, worker_pids[p]);
			}
		} else {
			if (p < args.num_processes) {
				/* worker process */
				proc_type = proc_worker;
				worker_id = p;
			} else {
				/* stats */
				proc_type = proc_stats;
			}
			break;
		}
	}

	/* initialize the randomizer */
	srand(time(0) * getpid());

	/* initialize zipfian if necessary (per-process) */
	if (args.zipf) {
		zipfian_generator(args.rows);
	}

	if (proc_type == proc_worker) {
		/* worker process */
		worker_process_main(args, worker_id, shm, pid_main);
		/* worker can exit here */
		exit(0);
	} else if (proc_type == proc_stats) {
		/* stats */
		if (args.mode == MODE_CLEAN) {
			/* no stats needed for clean mode */
			exit(0);
		}
		stats_process_main(args, stats, shm->throttle_factor, shm->signal, shm->stopcount, pid_main);
		exit(0);
	}

	/* master */
	/* wait for everyone to be ready */
	while (shm->readycount < (args.num_processes * args.num_threads)) {
		usleep(1000);
	}
	shm->signal = SIGNAL_GREEN;

	if (args.mode == MODE_RUN) {
		/* run the benchmark */

		/* if seconds is specified, stop child processes after the specified
		 * duration */
		if (args.seconds > 0) {
			if (args.verbose == VERBOSE_DEBUG) {
				printf("DEBUG: master sleeping for %d seconds\n", args.seconds);
			}

			auto time_start = steady_clock::now();
			while (1) {
				usleep(100000); /* sleep for 100ms */
				auto time_now = steady_clock::now();
				/* doesn't have to be precise */
				if (to_double_seconds(time_now - time_start) > args.seconds) {
					if (args.verbose == VERBOSE_DEBUG) {
						printf("DEBUG: time's up (%d seconds)\n", args.seconds);
					}
					break;
				}
			}

			/* notify everyone the time's up */
			shm->signal = SIGNAL_RED;
		}
	}

	auto status = int{};
	/* wait for worker processes to exit */
	for (auto p = 0; p < args.num_processes; p++) {
		if (args.verbose == VERBOSE_DEBUG) {
			fmt::print("DEBUG: waiting worker {} (PID:{}) to exit\n", p, worker_pids[p]);
		}
		auto pid = waitpid(worker_pids[p], &status, 0 /* or what? */);
		if (pid < 0) {
			fmt::print(stderr, "ERROR: waitpid failed for worker process PID {}\n", worker_pids[p]);
		}
		if (args.verbose == VERBOSE_DEBUG) {
			fmt::print("DEBUG: worker {} (PID:{}) exited\n", p, worker_pids[p]);
		}
	}

	/* all worker threads finished, stop the stats */
	if (args.mode == MODE_BUILD || args.iteration > 0) {
		shm->signal = SIGNAL_RED;
	}

	/* wait for stats to stop */
	auto pid = waitpid(worker_pids[args.num_processes], &status, 0 /* or what? */);
	if (pid < 0) {
		fmt::print(stderr, "ERROR: waitpid failed for stats process PID {}\n", worker_pids[args.num_processes]);
	}

	return 0;
}
