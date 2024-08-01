/*
 * mako.cpp
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

#include <array>
#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <map>
#include <new>
#include <numeric>
#include <optional>
#if defined(__linux__)
#include <pthread.h>
#endif
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
#include <boost/asio.hpp>
#include <fmt/format.h>
#include <fmt/printf.h>
#include <fdb_api.hpp>
#include <unordered_map>
#include "fdbclient/zipf.h"

#include "admin_server.hpp"
#include "async.hpp"
#include "future.hpp"
#include "logger.hpp"
#include "mako.hpp"
#include "operations.hpp"
#include "process.hpp"
#include "utils.hpp"
#include "shm.hpp"
#include "stats.hpp"
#include "tenant.hpp"
#include "time.hpp"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

namespace mako {

/* args for threads */
struct alignas(64) ThreadArgs {
	int process_idx;
	int thread_idx;
	int active_tenants;
	int total_tenants;
	pid_t parent_id;
	Arguments const* args;
	shared_memory::Access shm;
	fdb::Database database; // database to work with
};

} // namespace mako

using namespace fdb;
using namespace mako;

thread_local Logger logr = Logger(MainProcess{}, VERBOSE_DEFAULT);

std::pair<Transaction, std::optional<std::string> /*token*/>
createNewTransaction(Database db, Arguments const& args, int id, std::optional<std::vector<Tenant>>& tenants) {
	// No tenants specified
	if (args.active_tenants <= 0) {
		return { db.createTransaction(), {} };
	}
	// Create Tenant Transaction
	int tenant_id = (id == -1) ? urand(0, args.active_tenants - 1) : id;
	Transaction tr;
	std::string tenant_name;
	// If provided tenants array, use it
	if (tenants) {
		tr = (*tenants)[tenant_id].createTransaction();
	} else {
		tenant_name = getTenantNameByIndex(tenant_id);
		Tenant t = db.openTenant(toBytesRef(tenant_name));
		tr = t.createTransaction();
	}
	if (args.enable_token_based_authorization) {
		assert(!args.authorization_tokens.empty());
		// lookup token based on tenant name and, if found, set authz token to transaction
		if (tenant_name.empty())
			tenant_name = getTenantNameByIndex(tenant_id);
		auto token_map_iter = args.authorization_tokens.find(tenant_name);
		if (token_map_iter != args.authorization_tokens.end()) {
			tr.setOption(FDB_TR_OPTION_AUTHORIZATION_TOKEN, token_map_iter->second);
			return { tr, { token_map_iter->second } };
		} else {
			logr.error("could not find token for tenant '{}'", tenant_name);
			_exit(1);
		}
	} else {
		return { tr, {} };
	}
}

int cleanupTenants(ipc::AdminServer& server, Arguments const& args, int db_id) {
	for (auto tenant_id = 0; tenant_id < args.total_tenants;) {
		const auto tenant_id_end = tenant_id + std::min(args.tenant_batch_size, args.total_tenants - tenant_id);
		auto res = server.send(ipc::BatchDeleteTenantRequest{ args.cluster_files[db_id], tenant_id, tenant_id_end });
		if (res.error_message) {
			logr.error("{}", *res.error_message);
			return -1;

		} else {
			logr.debug("deleted tenant [{}:{})", tenant_id, tenant_id_end);
			tenant_id = tenant_id_end;
		}
	}
	return 0;
}

/* cleanup database (no tenant-awareness) */
int cleanupNormalKeyspace(Database db, Arguments const& args) {
	assert(args.total_tenants == 0 && args.active_tenants == 0);
	const auto prefix_len = args.prefixpadding ? args.key_length - args.row_digits : intSize(KEY_PREFIX);
	auto genprefix = [&args](ByteString& s) {
		const auto padding_len = args.key_length - intSize(KEY_PREFIX) - args.row_digits;
		auto pos = 0;
		if (args.prefixpadding) {
			memset(s.data(), 'x', padding_len);
			pos += padding_len;
		}
		const auto key_prefix_len = intSize(KEY_PREFIX);
		memcpy(&s[pos], KEY_PREFIX.data(), key_prefix_len);
	};
	auto beginstr = ByteString(prefix_len + 1, '\0');
	genprefix(beginstr);
	auto endstr = ByteString(prefix_len + 1, '\xff');
	genprefix(endstr);

	auto watch = Stopwatch(StartAtCtor{});

	Transaction tx = db.createTransaction();
	while (true) {
		tx.clearRange(beginstr, endstr);
		auto future_commit = tx.commit();
		const auto rc = waitAndHandleError(tx, future_commit, "COMMIT_CLEANUP");
		if (rc == FutureRC::OK) {
			break;
		} else if (rc == FutureRC::RETRY) {
			// tx already reset
			continue;
		} else {
			return -1;
		}
	}

	logr.info("Clear range: {:6.3f} sec", toDoubleSeconds(watch.stop().diff()));
	return 0;
}

/* populate database */
int populate(Database db, const ThreadArgs& thread_args, int thread_tps, WorkflowStatistics& stats) {
	Arguments const& args = *thread_args.args;
	const auto process_idx = thread_args.process_idx;
	const auto thread_idx = thread_args.thread_idx;
	auto xacts = 0;
	auto keystr = ByteString{};
	auto valstr = ByteString{};
	keystr.resize(args.key_length);
	valstr.resize(args.value_length);
	const auto num_commit_every = args.txnspec.ops[OP_INSERT][OP_COUNT];
	const auto num_seconds_trace_every = args.txntrace;
	auto watch_total = Stopwatch(StartAtCtor{});
	auto watch_throttle = Stopwatch(watch_total.getStart());
	auto watch_tx = Stopwatch(watch_total.getStart());
	auto watch_trace = Stopwatch(watch_total.getStart());

	// tenants are assumed to have been generated by populateTenants() at main process, pre-fork
	std::optional<std::vector<Tenant>> tenants = args.prepareTenants(db);
	int populate_iters = args.active_tenants > 0 ? args.active_tenants : 1;
	// Each tenant should have the same range populated
	for (auto t_id = 0; t_id < populate_iters; ++t_id) {
		auto [tx, token] = createNewTransaction(db, args, t_id, tenants);
		const auto key_begin = insertBegin(args.rows, process_idx, thread_idx, args.num_processes, args.num_threads);
		const auto key_end = insertEnd(args.rows, process_idx, thread_idx, args.num_processes, args.num_threads);
		auto key_checkpoint = key_begin; // in case of commit failure, restart from this key
		double required_keys = (key_end - key_begin + 1) * args.load_factor;
		for (auto i = key_begin; i <= key_end; i++) {
			// Choose required_keys out of (key_end -i + 1) randomly, so the probability is required_keys / (key_end - i
			// + 1). Generate a random number in range [0, 1), if the generated number is smaller or equal to
			// required_keys / (key_end - i + 1), then choose this key.
			double r = rand() / (1.0 + RAND_MAX);
			if (r > required_keys / (key_end - i + 1)) {
				continue;
			}
			--required_keys;
			/* sequential keys */
			genKey(keystr.data(), KEY_PREFIX, args, i);
			/* random values */
			randomString(valstr.data(), args.value_length);

			while (thread_tps > 0 && xacts >= thread_tps /* throttle */) {
				if (toIntegerSeconds(watch_throttle.stop().diff()) >= 1) {
					xacts = 0;
					watch_throttle.startFromStop();
				} else {
					usleep(1000);
				}
			}
			if (num_seconds_trace_every) {
				if (toIntegerSeconds(watch_trace.stop().diff()) >= num_seconds_trace_every) {
					watch_trace.startFromStop();
					logr.debug("txn tracing {}", toCharsRef(keystr));
					auto err = Error{};
					err = tx.setOptionNothrow(FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, keystr);
					if (err) {
						logr.error("setOption(TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER): {}", err.what());
					}
					err = tx.setOptionNothrow(FDB_TR_OPTION_LOG_TRANSACTION, BytesRef());
					if (err) {
						logr.error("setOption(TR_OPTION_LOG_TRANSACTION): {}", err.what());
					}
				}
			}

			/* insert (SET) */
			tx.set(keystr, valstr);
			stats.incrOpCount(OP_INSERT);

			/* commit every 100 inserts (default) or if this is the last key */
			if ((i % num_commit_every == 0) || i == key_end) {
				const auto do_sample = (stats.getOpCount(OP_TRANSACTION) % args.sampling) == 0;
				auto watch_commit = Stopwatch(StartAtCtor{});
				auto future_commit = tx.commit();
				const auto rc = waitAndHandleError(tx, future_commit, "COMMIT_POPULATE_INSERT");
				watch_commit.stop();
				watch_tx.setStop(watch_commit.getStop());
				auto tx_restarter = ExitGuard([&watch_tx]() { watch_tx.startFromStop(); });
				if (rc == FutureRC::OK) {
					key_checkpoint = i + 1; // restart on failures from next key
					std::tie(tx, token) = createNewTransaction(db, args, t_id, tenants);
				} else if (rc == FutureRC::ABORT) {
					return -1;
				} else {
					i = key_checkpoint - 1; // restart from last committed
					continue;
				}
				/* xact latency stats */
				if (do_sample) {
					const auto commit_latency = watch_commit.diff();
					const auto tx_duration = watch_tx.diff();
					stats.addLatency(OP_COMMIT, commit_latency);
					stats.addLatency(OP_TRANSACTION, tx_duration);
				}
				stats.incrOpCount(OP_COMMIT);
				stats.incrOpCount(OP_TRANSACTION);

				xacts++; /* for throttling */
			}
		}
		logr.debug("Populated {} rows [{}, {}]: {:6.3f} sec",
		           key_end - key_begin + 1,
		           key_begin,
		           key_end,
		           toDoubleSeconds(watch_total.stop().diff()));
	}

	return 0;
}

void updateErrorStatsRunMode(WorkflowStatistics& stats, fdb::Error err, int op) {
	if (err) {
		if (err.is(1020 /*not_commited*/)) {
			stats.incrConflictCount();
		} else if (err.is(1031 /*timeout*/)) {
			stats.incrTimeoutCount(op);
		} else {
			stats.incrErrorCount(op);
		}
	}
}

/* run one iteration of configured transaction */
int runOneTransaction(Transaction& tx,
                      std::optional<std::string> const& token,
                      Arguments const& args,
                      WorkflowStatistics& stats,
                      ByteString& key1,
                      ByteString& key2,
                      ByteString& val) {
	const auto do_sample = (stats.getOpCount(OP_TRANSACTION) % args.sampling) == 0;
	auto watch_tx = Stopwatch(StartAtCtor{});
	auto watch_op = Stopwatch{};

	auto op_iter = getOpBegin(args);
	auto needs_commit = false;
transaction_begin:
	while (op_iter != OpEnd) {
		const auto& [op, count, step] = op_iter;
		const auto step_kind = opTable[op].stepKind(step);
		if (step == 0 /* first step */)
			prepareKeys(op, key1, key2, args);
		auto watch_step = Stopwatch(StartAtCtor{});
		if (step == 0)
			watch_op = Stopwatch(watch_step.getStart());
		auto f = opTable[op].stepFunction(step)(tx, args, key1, key2, val);
		auto future_rc = FutureRC::OK;
		if (f) {
			if (step_kind != StepKind::ON_ERROR) {
				future_rc = waitAndHandleError(tx, f, opTable[op].name(), args.isAnyTimeoutEnabled());
			} else {
				future_rc = waitAndHandleForOnError(tx, f, opTable[op].name(), args.isAnyTimeoutEnabled());
			}
			updateErrorStatsRunMode(stats, f.error(), op);
		}
		if (auto postStepFn = opTable[op].postStepFunction(step))
			postStepFn(f, tx, args, key1, key2, val);
		watch_step.stop();
		if (future_rc != FutureRC::OK) {
			if (future_rc == FutureRC::ABORT) {
				return -1;
			}
			// retry from first op
			op_iter = getOpBegin(args);
			needs_commit = false;
			continue;
		}
		// step successful
		if (step_kind == StepKind::COMMIT) {
			// reset transaction boundary
			if (do_sample) {
				const auto step_latency = watch_step.diff();
				stats.addLatency(OP_COMMIT, step_latency);
			}
			tx.reset();
			if (token)
				tx.setOption(FDB_TR_OPTION_AUTHORIZATION_TOKEN, *token);
			stats.incrOpCount(OP_COMMIT);
			needs_commit = false;
		}

		// op completed successfully
		if (step + 1 == opTable[op].steps() /* last step */) {
			if (opTable[op].needsCommit())
				needs_commit = true;
			watch_op.setStop(watch_step.getStop());
			if (do_sample) {
				const auto op_latency = watch_op.diff();
				stats.addLatency(op, op_latency);
			}
			stats.incrOpCount(op);
		}
		// move to next op
		op_iter = getOpNext(args, op_iter);
	}
	// reached the end?
	if (needs_commit || args.commit_get) {
		auto watch_commit = Stopwatch(StartAtCtor{});
		auto f = tx.commit();
		const auto rc = waitAndHandleError(tx, f, "COMMIT_AT_TX_END", args.isAnyTimeoutEnabled());
		updateErrorStatsRunMode(stats, f.error(), OP_COMMIT);
		watch_commit.stop();
		auto tx_resetter = ExitGuard([&tx, &token]() {
			tx.reset();
			if (token)
				tx.setOption(FDB_TR_OPTION_AUTHORIZATION_TOKEN, *token);
		});
		if (rc == FutureRC::OK) {
			if (do_sample) {
				const auto commit_latency = watch_commit.diff();
				stats.addLatency(OP_COMMIT, commit_latency);
			}
			stats.incrOpCount(OP_COMMIT);
		} else {
			if (rc == FutureRC::ABORT) {
				return -1;
			}
			// restart from beginning
			op_iter = getOpBegin(args);
			goto transaction_begin;
		}
	}
	// one transaction has completed successfully
	if (do_sample) {
		const auto tx_duration = watch_tx.stop().diff();
		stats.addLatency(OP_TRANSACTION, tx_duration);
	}
	stats.incrOpCount(OP_TRANSACTION);
	return 0;
}

int runWorkload(Database db,
                Arguments const& args,
                int const thread_tps,
                std::atomic<double> const& throttle_factor,
                int const thread_iters,
                std::atomic<int> const& signal,
                WorkflowStatistics& workflow_stats,
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

	// reuse memory for keys to avoid realloc overhead
	auto key1 = ByteString{};
	key1.resize(args.key_length);
	auto key2 = ByteString{};
	key2.resize(args.key_length);
	auto val = ByteString{};
	val.resize(args.value_length);

	std::optional<std::vector<fdb::Tenant>> tenants = args.prepareTenants(db);

	/* main transaction loop */
	while (1) {
		if ((thread_tps > 0 /* iff throttling on */) && (xacts >= current_tps)) {
			/* throttle on */
			auto time_now = steady_clock::now();
			while (toDoubleSeconds(time_now - time_prev) < 1.0) {
				usleep(1000);
				time_now = steady_clock::now();
			}

			/* more than 1 second passed*/
			xacts = 0;
			time_prev = time_now;

			/* update throttle rate */
			current_tps = static_cast<int>(thread_tps * throttle_factor.load());
		}

		if (current_tps > 0 || thread_tps == 0 /* throttling off */) {
			auto [tx, token] = createNewTransaction(db, args, -1, tenants);
			setTransactionTimeoutIfEnabled(args, tx);

			/* enable transaction trace */
			if (dotrace) {
				const auto time_now = steady_clock::now();
				if (toIntegerSeconds(time_now - time_last_trace) >= 1) {
					time_last_trace = time_now;
					traceid.clear();
					fmt::format_to(std::back_inserter(traceid), "makotrace{:0>19d}", total_xacts);
					logr.debug("txn tracing {}", traceid);
					auto err = Error{};
					err = tx.setOptionNothrow(FDB_TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER, toBytesRef(traceid));
					if (err) {
						logr.error("TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER: {}", err.what());
					}
					err = tx.setOptionNothrow(FDB_TR_OPTION_LOG_TRANSACTION, BytesRef());
					if (err) {
						logr.error("TR_OPTION_LOG_TRANSACTION: {}", err.what());
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
				auto err = tx.setOptionNothrow(FDB_TR_OPTION_AUTO_THROTTLE_TAG, toBytesRef(tagstr));
				if (err) {
					logr.error("TR_OPTION_DEBUG_TRANSACTION_IDENTIFIER: {}", err.what());
				}
			}

			rc = runOneTransaction(tx, token, args, workflow_stats, key1, key2, val);
			if (rc) {
				logr.warn("runOneTransaction failed ({})", rc);
			}

			xacts++;
			total_xacts++;
		}

		if (thread_iters != -1) {
			if (total_xacts >= thread_iters) {
				/* xact limit reached */
				break;
			}
		} else if (signal.load() == SIGNAL_RED) {
			/* signal turned red, target duration reached */
			break;
		}
	}
	return rc;
}

std::string getStatsFilename(std::string_view dirname, int process_idx, int thread_id, int op) {

	return fmt::format("{}/{}_{}_{}", dirname, process_idx + 1, thread_id + 1, opTable[op].name());
}

std::string getStatsFilename(std::string_view dirname, int process_idx, int thread_id) {
	return fmt::format("{}/{}_{}", dirname, process_idx + 1, thread_id + 1);
}

void dumpThreadSamples(Arguments const& args,
                       pid_t parent_id,
                       int process_idx,
                       int thread_id,
                       const WorkflowStatistics& stats,
                       bool overwrite = true) {
	const auto dirname = fmt::format("{}{}", TEMP_DATA_STORE, parent_id);
	const auto rc = mkdir(dirname.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
	if (rc < 0 && errno != EEXIST) {
		logr.error("mkdir {}: {}", dirname, strerror(errno));
		return;
	}
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			stats.writeToFile(getStatsFilename(dirname, process_idx, thread_id, op), op);
		}
	}
}

void runAsyncWorkload(Arguments const& args,
                      pid_t pid_main,
                      int process_idx,
                      shared_memory::Access shm,
                      boost::asio::io_context& io_context,
                      std::vector<Database>& databases) {
	auto dump_samples = [&args, pid_main, process_idx](auto&& states) {
		auto overwrite = true; /* overwrite or append */
		for (const auto& state : states) {
			dumpThreadSamples(args, pid_main, process_idx, 0 /*thread_id*/, state->stats, overwrite);
			overwrite = false;
		}
	};
	auto stopcount = std::atomic<int>{};
	if (args.mode == MODE_BUILD) {
		auto states = std::vector<PopulateStateHandle>(args.async_xacts);
		for (auto i = 0; i < args.async_xacts; i++) {
			const auto key_begin = insertBegin(args.rows, process_idx, i, args.num_processes, args.async_xacts);
			const auto key_end = insertEnd(args.rows, process_idx, i, args.num_processes, args.async_xacts);
			auto db = databases[i % args.num_databases];
			auto state =
			    std::make_shared<ResumableStateForPopulate>(Logger(WorkerProcess{}, args.verbose, process_idx, i),
			                                                db,
			                                                db.createTransaction(),
			                                                io_context,
			                                                args,
			                                                shm.workerStatsSlot(process_idx, i),
			                                                stopcount,
			                                                key_begin,
			                                                key_end);
			state->watch_tx.start();
			state->watch_total.start();
			states[i] = state;
		}
		while (shm.headerConst().signal.load() != SIGNAL_GREEN)
			usleep(1000);
		// launch [async_xacts] concurrent transactions
		for (auto state : states)
			state->postNextTick();
		while (stopcount.load() != args.async_xacts)
			usleep(1000);
		dump_samples(states);
	} else if (args.mode == MODE_RUN) {
		auto states = std::vector<RunWorkloadStateHandle>(args.async_xacts);
		for (auto i = 0; i < args.async_xacts; i++) {
			auto db = databases[i % args.num_databases];
			const auto max_iters =
			    args.iteration == 0
			        ? -1
			        : computeThreadIters(args.iteration, process_idx, i, args.num_processes, args.async_xacts);
			// argument validation should ensure max_iters > 0
			assert(args.iteration == 0 || max_iters > 0);

			auto state =
			    std::make_shared<ResumableStateForRunWorkload>(Logger(WorkerProcess{}, args.verbose, process_idx, i),
			                                                   db,
			                                                   db.createTransaction(),
			                                                   io_context,
			                                                   args,
			                                                   shm.workerStatsSlot(process_idx, i),
			                                                   stopcount,
			                                                   shm.headerConst().signal,
			                                                   max_iters,
			                                                   getOpBegin(args));
			states[i] = state;
			state->watch_tx.start();
		}
		while (shm.headerConst().signal.load() != SIGNAL_GREEN)
			usleep(1000);
		for (auto state : states)
			state->postNextTick();
		logr.debug("Launched {} concurrent transactions", states.size());
		while (stopcount.load() != args.async_xacts)
			usleep(1000);
		logr.debug("All transactions completed");
		dump_samples(states);
	}
}

/* mako worker thread */
void workerThread(const ThreadArgs& thread_args) {

	const auto& args = *thread_args.args;
	const auto parent_id = thread_args.parent_id;
	const auto process_idx = thread_args.process_idx;
	const auto thread_idx = thread_args.thread_idx;
	const auto dotrace = (process_idx == 0 && thread_idx == 0 && args.txntrace) ? args.txntrace : 0;
	auto database = thread_args.database;
	const auto dotagging = args.txntagging;
	const auto& signal = thread_args.shm.headerConst().signal;
	const auto& throttle_factor = thread_args.shm.headerConst().throttle_factor;
	auto& readycount = thread_args.shm.header().readycount;
	auto& stopcount = thread_args.shm.header().stopcount;
	auto& workflow_stats = thread_args.shm.workerStatsSlot(process_idx, thread_idx);
	auto& thread_stats = thread_args.shm.threadStatsSlot(process_idx, thread_idx);
	logr = Logger(WorkerProcess{}, args.verbose, process_idx, thread_idx);
	thread_stats.startThreadTimer();

	logr.debug("started, tid: {}", reinterpret_cast<uint64_t>(pthread_self()));

	const auto thread_tps =
	    args.tpsmax == 0 ? 0
	                     : computeThreadTps(args.tpsmax, process_idx, thread_idx, args.num_processes, args.num_threads);
	// argument validation should ensure thread_tps > 0
	assert(args.tpsmax == 0 || thread_tps > 0);

	const auto thread_iters =
	    args.iteration == 0
	        ? -1
	        : computeThreadIters(args.iteration, process_idx, thread_idx, args.num_processes, args.num_threads);
	// argument validation should ensure thread_iters > 0
	assert(args.iteration == 0 || thread_iters > 0);

	/* i'm ready */
	readycount.fetch_add(1);
	auto stopcount_guard = ExitGuard([&stopcount]() { stopcount.fetch_add(1); });
	while (signal.load() == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	if (args.mode == MODE_CLEAN) {
		auto rc = cleanupNormalKeyspace(database, args);
		if (rc < 0) {
			logr.error("cleanup failed");
		}
	} else if (args.mode == MODE_BUILD) {
		auto rc = populate(database, thread_args, thread_tps, workflow_stats);
		if (rc < 0) {
			logr.error("populate failed");
		}
	} else if (args.mode == MODE_RUN) {
		auto rc = runWorkload(
		    database, args, thread_tps, throttle_factor, thread_iters, signal, workflow_stats, dotrace, dotagging);
		if (rc < 0) {
			logr.error("runWorkload failed");
		}
	}

	if (args.mode == MODE_BUILD || args.mode == MODE_RUN) {
		dumpThreadSamples(args, parent_id, process_idx, thread_idx, workflow_stats);
	}

	thread_stats.endThreadTimer();
}

/* mako worker process */
int workerProcessMain(Arguments const& args, int process_idx, shared_memory::Access shm, pid_t pid_main) {
	logr.debug("started");

	auto err = Error{};
	/* Everything starts from here */
	if (args.setGlobalOptions() < 0) {
		return -1;
	}

	/* Network thread must be setup before doing anything */
	logr.debug("network::setup()");
	network::setup();

	shm.processStatsSlot(process_idx).startProcessTimer();

	/* Each worker process will have its own network thread */
	logr.debug("creating network thread");
	auto network_thread = std::thread([parent_logr = logr, process_idx, shm]() {
		shm.processStatsSlot(process_idx).startFDBNetworkTimer();

		logr = parent_logr;
		logr.debug("network thread started");
		if (auto err = network::run()) {
			logr.error("network::run(): {}", err.what());
		}

		shm.processStatsSlot(process_idx).endFDBNetworkTimer();
	});
#if defined(__linux__)
	pthread_setname_np(network_thread.native_handle(), "mako_network");
#endif

	// prevent any exception from unwinding stack without joining the network thread
	auto network_thread_guard = ExitGuard([&network_thread]() {
		/* stop the network thread */
		logr.debug("network::stop()");
		auto err = network::stop();
		if (err) {
			logr.error("network::stop(): {}", err.what());
		}

		/* wait for the network thread to join */
		logr.debug("waiting for network thread to join");
		network_thread.join();
	});

	/*** let's party! ***/

	auto databases = std::vector<fdb::Database>(args.num_databases);
	/* set up database for worker threads */
	for (auto i = 0; i < args.num_databases; i++) {
		int cluster_index = i % args.num_fdb_clusters;
		databases[i] = Database(args.cluster_files[cluster_index]);
		logr.debug("creating database at cluster {}", args.cluster_files[cluster_index]);
		if (args.disable_ryw) {
			databases[i].setOption(FDB_DB_OPTION_SNAPSHOT_RYW_DISABLE, BytesRef{});
		}
		if (args.transaction_timeout_db > 0 && args.mode == MODE_RUN) {
			databases[i].setOption(FDB_DB_OPTION_TRANSACTION_TIMEOUT, args.transaction_timeout_db);
		}
	}

	if (!args.async_xacts) {
		logr.debug("creating {} worker threads", args.num_threads);
		auto worker_threads = std::vector<std::thread>(args.num_threads);

		/* spawn worker threads */
		auto thread_args = std::vector<ThreadArgs>(args.num_threads);

		for (auto i = 0; i < args.num_threads; i++) {
			auto& this_args = thread_args[i];
			this_args.process_idx = process_idx;
			this_args.thread_idx = i;
			this_args.parent_id = pid_main;
			this_args.active_tenants = args.active_tenants;
			this_args.total_tenants = args.total_tenants;
			this_args.args = &args;
			this_args.shm = shm;
			this_args.database = databases[i % args.num_databases];
			worker_threads[i] = std::thread(workerThread, std::ref(this_args));
#if defined(__linux__)
			const auto thread_name = "mako_worker_" + std::to_string(i);
			pthread_setname_np(worker_threads[i].native_handle(), thread_name.c_str());
#endif
		}
		/* wait for everyone to finish */
		for (auto i = 0; i < args.num_threads; i++) {
			logr.debug("waiting for worker thread {} to join", i + 1);
			worker_threads[i].join();
		}
	} else {
		logr.debug("running async mode with {} concurrent transactions", args.async_xacts);
		auto ctx = boost::asio::io_context{};
		using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;
		auto wg = WorkGuard(ctx.get_executor());
		auto worker_threads = std::vector<std::thread>(args.num_threads);
		for (auto i = 0; i < args.num_threads; i++) {
			worker_threads[i] = std::thread([&ctx, &args, process_idx, i, shm]() {
				shm.threadStatsSlot(process_idx, i).startThreadTimer();

				logr = Logger(WorkerProcess{}, args.verbose, process_idx);
				logr.debug("Async-mode worker thread {} started", i + 1);
				ctx.run();
				logr.debug("Async-mode worker thread {} finished", i + 1);

				shm.threadStatsSlot(process_idx, i).endThreadTimer();
			});
#if defined(__linux__)
			const auto thread_name = "mako_worker_" + std::to_string(i);
			pthread_setname_np(worker_threads[i].native_handle(), thread_name.c_str());
#endif
		}

		shm.header().readycount.fetch_add(args.num_threads);
		runAsyncWorkload(args, pid_main, process_idx, shm, ctx, databases);
		wg.reset();
		for (auto& thread : worker_threads)
			thread.join();
		shm.header().stopcount.fetch_add(args.num_threads);
	}

	shm.processStatsSlot(process_idx).endProcessTimer();
	return 0;
}

/* initialize the parameters with default values */
Arguments::Arguments() {
	num_fdb_clusters = 0;
	num_databases = 1;
	api_version = maxApiVersion();
	json = 0;
	num_processes = 1;
	num_threads = 1;
	async_xacts = 0;
	mode = MODE_INVALID;
	rows = 100000;
	load_factor = 1.0;
	row_digits = digits(rows);
	seconds = 0;
	iteration = 0;
	tpsmax = 0;
	tpsmin = -1;
	tpsinterval = 10;
	tpschange = TPS_SIN;
	sampling = 1000;
	key_length = 32;
	value_length = 16;
	active_tenants = 0;
	total_tenants = 0;
	tenant_batch_size = 10000;
	zipf = 0;
	commit_get = 0;
	verbose = 1;
	flatbuffers = 0; /* internal */
	knobs[0] = '\0';
	log_group[0] = '\0';
	prefixpadding = 0;
	trace = 0;
	tracepath[0] = '\0';
	traceformat = 0; /* default to client's default (XML) */
	streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
	txntrace = 0;
	txntagging = 0;
	memset(cluster_files, 0, sizeof(cluster_files));
	memset(txntagging_prefix, 0, TAGPREFIXLENGTH_MAX);
	enable_token_based_authorization = false;
	for (auto i = 0; i < MAX_OP; i++) {
		txnspec.ops[i][OP_COUNT] = 0;
	}
	client_threads_per_version = 0;
	disable_client_bypass = false;
	disable_ryw = 0;
	json_output_path[0] = '\0';
	stats_export_path[0] = '\0';
	bg_materialize_files = false;
	bg_file_path[0] = '\0';
	distributed_tracer_client = 0;
	transaction_timeout_db = 0;
	transaction_timeout_tx = 0;
	num_report_files = 0;
}

int Arguments::setGlobalOptions() const {
	selectApiVersion(api_version);
	auto err = Error{};
	/* enable distributed tracing */
	switch (distributed_tracer_client) {
	case DistributedTracerClient::NETWORK_LOSSY:
		err = network::setOptionNothrow(FDB_NET_OPTION_DISTRIBUTED_CLIENT_TRACER, BytesRef(toBytePtr("network_lossy")));
		break;
	case DistributedTracerClient::LOG_FILE:
		err = network::setOptionNothrow(FDB_NET_OPTION_DISTRIBUTED_CLIENT_TRACER, BytesRef(toBytePtr("log_file")));
		break;
	}
	if (err) {
		logr.error("network::setOption(FDB_NET_OPTION_DISTRIBUTED_CLIENT_TRACER): {}", err.what());
	}

	if (tls_certificate_file.has_value() && (logr.isFor(ProcKind::ADMIN) || !isAuthorizationEnabled())) {
		logr.debug("TLS certificate file: {}", tls_certificate_file.value());
		network::setOption(FDB_NET_OPTION_TLS_CERT_PATH, tls_certificate_file.value());
	}

	if (tls_key_file.has_value() && (logr.isFor(ProcKind::ADMIN) || !isAuthorizationEnabled())) {
		logr.debug("TLS key file: {}", tls_key_file.value());
		network::setOption(FDB_NET_OPTION_TLS_KEY_PATH, tls_key_file.value());
	}

	if (tls_ca_file.has_value()) {
		logr.debug("TLS CA file: {}", tls_ca_file.value());
		network::setOption(FDB_NET_OPTION_TLS_CA_PATH, tls_ca_file.value());
	}

	/* enable flatbuffers if specified */
	if (flatbuffers) {
#ifdef FDB_NET_OPTION_USE_FLATBUFFERS
		logr.debug("Using flatbuffers");
		err = network::setOptionNothrow(FDB_NET_OPTION_USE_FLATBUFFERS, BytesRef(&flatbuffers, sizeof(flatbuffers)));
		if (err) {
			logr.error("network::setOption(USE_FLATBUFFERS): {}", err.what());
		}
#else
		logr.info("flatbuffers is not supported in FDB API version {}", FDB_API_VERSION);
#endif
	}

	/* Set client logr group */
	if (log_group[0] != '\0') {
		err = network::setOptionNothrow(FDB_NET_OPTION_TRACE_LOG_GROUP, BytesRef(toBytePtr(log_group)));
		if (err) {
			logr.error("network::setOption(FDB_NET_OPTION_TRACE_LOG_GROUP): {}", err.what());
		}
	}

	/* enable tracing if specified */
	if (trace) {
		logr.debug("Enable Tracing in {} ({})",
		           (traceformat == 0) ? "XML" : "JSON",
		           (tracepath[0] == '\0') ? "current directory" : tracepath);
		err = network::setOptionNothrow(FDB_NET_OPTION_TRACE_ENABLE, BytesRef(toBytePtr(tracepath)));
		if (err) {
			logr.error("network::setOption(TRACE_ENABLE): {}", err.what());
		}
		if (traceformat == 1) {
			err = network::setOptionNothrow(FDB_NET_OPTION_TRACE_FORMAT, BytesRef(toBytePtr("json")));
			if (err) {
				logr.error("network::setOption(FDB_NET_OPTION_TRACE_FORMAT): {}", err.what());
			}
		}
	}

	/* enable knobs if specified */
	if (knobs[0] != '\0') {
		auto k = std::string_view(knobs);
		const auto delim = std::string_view(", ");
		while (true) {
			k.remove_prefix(std::min(k.find_first_not_of(delim), k.size()));
			auto knob = k.substr(0, k.find_first_of(delim));
			if (knob.empty())
				break;
			logr.debug("Setting client knob: {}", knob);
			err = network::setOptionNothrow(FDB_NET_OPTION_KNOB, toBytesRef(knob));
			if (err) {
				logr.error("network::setOption({}): {}", knob, err.what());
			}
			k.remove_prefix(knob.size());
		}
	}

	if (client_threads_per_version > 0) {
		err = network::setOptionNothrow(FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION, client_threads_per_version);
		if (err) {
			logr.error("network::setOption (FDB_NET_OPTION_CLIENT_THREADS_PER_VERSION) ({}): {}",
			           client_threads_per_version,
			           err.what());
			// let's exit here since we do not want to confuse users
			// that mako is running with multi-threaded client enabled
			return -1;
		}
	}

	if (disable_client_bypass) {
		err = network::setOptionNothrow(FDB_NET_OPTION_DISABLE_CLIENT_BYPASS);
		if (err) {
			logr.error(
			    "network::setOption (FDB_NET_OPTION_DISABLE_CLIENT_BYPASS): {}", disable_client_bypass, err.what());
			return -1;
		}
	}
	return 0;
}

bool Arguments::isAnyTimeoutEnabled() const {
	return (transaction_timeout_tx > 0 || transaction_timeout_db > 0);
}

/* parse transaction specification */
int parseTransaction(Arguments& args, char const* optarg) {
	char const* ptr = optarg;
	int op = 0;
	int rangeop = 0;
	int num;
	int error = 0;

	for (op = 0; op < MAX_OP; op++) {
		args.txnspec.ops[op][OP_COUNT] = 0;
		args.txnspec.ops[op][OP_RANGE] = 0;
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
		} else if (strncmp(ptr, "o", 1) == 0) {
			op = OP_OVERWRITE;
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
			logr.error("Invalid transaction spec: {}", ptr);
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
		args.txnspec.ops[op][OP_COUNT] = num;

		if (rangeop) {
			if (*ptr != ':') {
				error = 1;
				break;
			} else {
				ptr++; /* skip ':' */
				/* check negative '-' sign */
				if (*ptr == '-') {
					args.txnspec.ops[op][OP_REVERSE] = 1;
					ptr++;
				} else {
					args.txnspec.ops[op][OP_REVERSE] = 0;
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
				args.txnspec.ops[op][OP_RANGE] = num;
			}
		}
		rangeop = 0;
	}

	if (error) {
		logr.error("invalid transaction specification {}", optarg);
		return -1;
	}

	if (args.verbose == VERBOSE_DEBUG) {
		for (op = 0; op < MAX_OP; op++) {
			logr.debug("OP: {}: {}: {}", op, args.txnspec.ops[op][0], args.txnspec.ops[op][1]);
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
	printf("%-24s %s\n", "    --async_xacts", "Specify number of concurrent transactions to be run in async mode");
	printf("%-24s %s\n", "-r, --rows=ROWS", "Specify number of records");
	printf("%-24s %s\n", "-l, --load_factor=LOAD_FACTOR", "Specify load factor");
	printf("%-24s %s\n", "-s, --seconds=SECONDS", "Specify the test duration in seconds\n");
	printf("%-24s %s\n", "", "This option cannot be specified with --iteration.");
	printf("%-24s %s\n", "-i, --iteration=ITERS", "Specify the number of iterations.\n");
	printf("%-24s %s\n", "", "This option cannot be specified with --seconds.");
	printf("%-24s %s\n", "    --keylen=LENGTH", "Specify the key lengths");
	printf("%-24s %s\n", "    --vallen=LENGTH", "Specify the value lengths");
	printf("%-24s %s\n", "    --active_tenants=ACTIVE_TENANTS", "Specify the number of tenants to use");
	printf("%-24s %s\n", "    --total_tenants=TOTAL_TENANTS", "Specify the number of tenants to create");
	printf("%-24s %s\n", "    --tenant_batch_size=SIZE", "Specify how many tenants to create/delete per transaction");
	printf("%-24s %s\n", "-x, --transaction=SPEC", "Transaction specification");
	printf("%-24s %s\n", "    --tps|--tpsmax=TPS", "Specify the target max TPS");
	printf("%-24s %s\n", "    --tpsmin=TPS", "Specify the target min TPS");
	printf("%-24s %s\n", "    --tpsinterval=SEC", "Specify the TPS change interval (Default: 10 seconds)");
	printf("%-24s %s\n", "    --tpschange=<sin|square|pulse>", "Specify the TPS change type (Default: sin)");
	printf("%-24s %s\n", "    --sampling=RATE", "Specify the sampling rate for latency stats");
	printf("%-24s %s\n", "-m, --mode=MODE", "Specify the mode (build, run, clean, report)");
	printf("%-24s %s\n", "-z, --zipf", "Use zipfian distribution instead of uniform distribution");
	printf("%-24s %s\n", "    --commitget", "Commit GETs");
	printf("%-24s %s\n", "    --loggroup=LOGGROUP", "Set client logr group");
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
	printf(
	    "%-24s %s\n", "    --disable_client_bypass", "Disable client-bypass forcing mako to use multi-version client");
	printf("%-24s %s\n", "    --json_report=PATH", "Output stats to the specified json file (Default: mako.json)");
	printf("%-24s %s\n",
	       "    --bg_file_path=PATH",
	       "Read blob granule files from the local filesystem at PATH and materialize the results.");
	printf("%-24s %s\n",
	       "    --stats_export_path=PATH",
	       "Write the serialized DDSketch data to file at PATH. Can be used in either run or build mode.");
	printf(
	    "%-24s %s\n", "    --distributed_tracer_client=CLIENT", "Specify client (disabled, network_lossy, log_file)");
	printf("%-24s %s\n", "    --tls_key_file=PATH", "Location of TLS key file");
	printf("%-24s %s\n", "    --tls_ca_file=PATH", "Location of TLS CA file");
	printf("%-24s %s\n", "    --tls_certificate_file=PATH", "Location of TLS certificate file");
	printf("%-24s %s\n",
	       "    --enable_token_based_authorization",
	       "Make worker thread connect to server as untrusted clients to access tenant data");
	printf("%-24s %s\n",
	       "    --authorization_keypair_id",
	       "ID of the public key in the public key set which will verify the authorization tokens generated by Mako");
	printf("%-24s %s\n",
	       "    --authorization_private_key_pem",
	       "PEM-encoded private key with which Mako will sign generated tokens");
	printf("%-24s %s\n",
	       "    --transaction_timeout_db=DURATION",
	       "Duration in milliseconds after which a transaction times out in run mode. Set as database option.");
	printf("%-24s %s\n",
	       "    --transaction_timeout_tx=DURATION",
	       "Duration in milliseconds after which a transaction times out in run mode. Set as transaction option");
}

/* parse benchmark parameters */
int parseArguments(int argc, char* argv[], Arguments& args) {
	int rc;
	int c;
	int idx;
	while (1) {
		const char* short_options = "a:c:d:p:t:r:s:i:x:v:m:hz";
		static struct option long_options[] = {
			/* name, has_arg, flag, val */
			/* options requiring an argument */
			{ "api_version", required_argument, NULL, 'a' },
			{ "cluster", required_argument, NULL, 'c' },
			{ "num_databases", required_argument, NULL, 'd' },
			{ "procs", required_argument, NULL, 'p' },
			{ "threads", required_argument, NULL, 't' },
			{ "async_xacts", required_argument, NULL, ARG_ASYNC },
			{ "rows", required_argument, NULL, 'r' },
			{ "load_factor", required_argument, NULL, 'l' },
			{ "seconds", required_argument, NULL, 's' },
			{ "iteration", required_argument, NULL, 'i' },
			{ "keylen", required_argument, NULL, ARG_KEYLEN },
			{ "vallen", required_argument, NULL, ARG_VALLEN },
			{ "active_tenants", required_argument, NULL, ARG_ACTIVE_TENANTS },
			{ "total_tenants", required_argument, NULL, ARG_TOTAL_TENANTS },
			{ "tenant_batch_size", required_argument, NULL, ARG_TENANT_BATCH_SIZE },
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
			{ "txntagging", required_argument, NULL, ARG_TXNTAGGING },
			{ "txntagging_prefix", required_argument, NULL, ARG_TXNTAGGINGPREFIX },
			{ "client_threads_per_version", required_argument, NULL, ARG_CLIENT_THREADS_PER_VERSION },
			{ "bg_file_path", required_argument, NULL, ARG_BG_FILE_PATH },
			{ "distributed_tracer_client", required_argument, NULL, ARG_DISTRIBUTED_TRACER_CLIENT },
			{ "tls_certificate_file", required_argument, NULL, ARG_TLS_CERTIFICATE_FILE },
			{ "tls_key_file", required_argument, NULL, ARG_TLS_KEY_FILE },
			{ "tls_ca_file", required_argument, NULL, ARG_TLS_CA_FILE },
			{ "authorization_keypair_id", required_argument, NULL, ARG_AUTHORIZATION_KEYPAIR_ID },
			{ "authorization_private_key_pem_file", required_argument, NULL, ARG_AUTHORIZATION_PRIVATE_KEY_PEM_FILE },
			{ "transaction_timeout_tx", required_argument, NULL, ARG_TRANSACTION_TIMEOUT_TX },
			{ "transaction_timeout_db", required_argument, NULL, ARG_TRANSACTION_TIMEOUT_DB },
			/* options which may or may not have an argument */
			{ "json_report", optional_argument, NULL, ARG_JSON_REPORT },
			{ "stats_export_path", optional_argument, NULL, ARG_EXPORT_PATH },
			/* options without an argument */
			{ "help", no_argument, NULL, 'h' },
			{ "zipf", no_argument, NULL, 'z' },
			{ "commitget", no_argument, NULL, ARG_COMMITGET },
			{ "flatbuffers", no_argument, NULL, ARG_FLATBUFFERS },
			{ "prefix_padding", no_argument, NULL, ARG_PREFIXPADDING },
			{ "trace", no_argument, NULL, ARG_TRACE },
			{ "version", no_argument, NULL, ARG_VERSION },
			{ "disable_client_bypass", no_argument, NULL, ARG_DISABLE_CLIENT_BYPASS },
			{ "disable_ryw", no_argument, NULL, ARG_DISABLE_RYW },
			{ "enable_token_based_authorization", no_argument, NULL, ARG_ENABLE_TOKEN_BASED_AUTHORIZATION },
			{ NULL, 0, NULL, 0 }
		};

/* For optional arguments, optarg is only set when the argument is passed as "--option=[ARGUMENT]" but not as
 "--option [ARGUMENT]". This function sets optarg in the latter case. See
 https://cfengine.com/blog/2021/optional-arguments-with-getopt-long/ for a more detailed explanation */
#define SET_OPT_ARG_IF_PRESENT()                                                                                       \
	{                                                                                                                  \
		if (optarg == NULL && optind < argc && argv[optind][0] != '-') {                                               \
			optarg = argv[optind++];                                                                                   \
		}                                                                                                              \
	}

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
			args.api_version = atoi(optarg);
			break;
		case 'c': {
			const char delim[] = ",";
			char* cluster_file = strtok(optarg, delim);
			while (cluster_file != NULL) {
				strcpy(args.cluster_files[args.num_fdb_clusters++], cluster_file);
				cluster_file = strtok(NULL, delim);
			}
			break;
		}
		case 'd':
			args.num_databases = atoi(optarg);
			break;
		case 'p':
			args.num_processes = atoi(optarg);
			break;
		case 't':
			args.num_threads = atoi(optarg);
			break;
		case 'r':
			args.rows = atoi(optarg);
			args.row_digits = digits(args.rows);
			break;
		case 'l':
			args.load_factor = atof(optarg);
			break;
		case 's':
			args.seconds = atoi(optarg);
			break;
		case 'i':
			args.iteration = atoi(optarg);
			break;
		case 'x':
			rc = parseTransaction(args, optarg);
			if (rc < 0)
				return -1;
			break;
		case 'v':
			args.verbose = atoi(optarg);
			break;
		case 'z':
			args.zipf = 1;
			break;
		case 'm':
			if (strcmp(optarg, "clean") == 0) {
				args.mode = MODE_CLEAN;
			} else if (strcmp(optarg, "build") == 0) {
				args.mode = MODE_BUILD;
			} else if (strcmp(optarg, "run") == 0) {
				args.mode = MODE_RUN;
			} else if (strcmp(optarg, "report") == 0) {
				args.mode = MODE_REPORT;
				int i = optind;
				for (; i < argc; i++) {
					if (argv[i][0] != '-') {
						const std::string report_file = argv[i];
						strncpy(args.report_files[args.num_report_files], report_file.c_str(), report_file.size());
						args.num_report_files++;
					} else {
						optind = i - 1;
						break;
					}
				}
			}
			break;
		case ARG_ASYNC:
			args.async_xacts = atoi(optarg);
			break;
		case ARG_KEYLEN:
			args.key_length = atoi(optarg);
			break;
		case ARG_VALLEN:
			args.value_length = atoi(optarg);
			break;
		case ARG_ACTIVE_TENANTS:
			args.active_tenants = atoi(optarg);
			break;
		case ARG_TOTAL_TENANTS:
			args.total_tenants = atoi(optarg);
			break;
		case ARG_TENANT_BATCH_SIZE:
			args.tenant_batch_size = atoi(optarg);
			break;
		case ARG_TPS:
		case ARG_TPSMAX:
			args.tpsmax = atoi(optarg);
			break;
		case ARG_TPSMIN:
			args.tpsmin = atoi(optarg);
			break;
		case ARG_TPSINTERVAL:
			args.tpsinterval = atoi(optarg);
			break;
		case ARG_TPSCHANGE:
			if (strcmp(optarg, "sin") == 0)
				args.tpschange = TPS_SIN;
			else if (strcmp(optarg, "square") == 0)
				args.tpschange = TPS_SQUARE;
			else if (strcmp(optarg, "pulse") == 0)
				args.tpschange = TPS_PULSE;
			else {
				logr.error("--tpschange must be sin, square or pulse");
				return -1;
			}
			break;
		case ARG_SAMPLING:
			args.sampling = atoi(optarg);
			break;
		case ARG_VERSION:
			logr.error("Version: {}", FDB_API_VERSION);
			_exit(0);
			break;
		case ARG_COMMITGET:
			args.commit_get = 1;
			break;
		case ARG_FLATBUFFERS:
			args.flatbuffers = 1;
			break;
		case ARG_KNOBS:
			memcpy(args.knobs, optarg, strlen(optarg) + 1);
			break;
		case ARG_LOGGROUP:
			memcpy(args.log_group, optarg, strlen(optarg) + 1);
			break;
		case ARG_PREFIXPADDING:
			args.prefixpadding = 1;
			break;
		case ARG_TRACE:
			args.trace = 1;
			break;
		case ARG_TRACEPATH:
			args.trace = 1;
			memcpy(args.tracepath, optarg, strlen(optarg) + 1);
			break;
		case ARG_TRACEFORMAT:
			if (strncmp(optarg, "json", 5) == 0) {
				args.traceformat = 1;
			} else if (strncmp(optarg, "xml", 4) == 0) {
				args.traceformat = 0;
			} else {
				logr.error("Invalid trace_format {}", optarg);
				return -1;
			}
			break;
		case ARG_STREAMING_MODE:
			if (strncmp(optarg, "all", 3) == 0) {
				args.streaming_mode = FDB_STREAMING_MODE_WANT_ALL;
			} else if (strncmp(optarg, "iterator", 8) == 0) {
				args.streaming_mode = FDB_STREAMING_MODE_ITERATOR;
			} else if (strncmp(optarg, "small", 5) == 0) {
				args.streaming_mode = FDB_STREAMING_MODE_SMALL;
			} else if (strncmp(optarg, "medium", 6) == 0) {
				args.streaming_mode = FDB_STREAMING_MODE_MEDIUM;
			} else if (strncmp(optarg, "large", 5) == 0) {
				args.streaming_mode = FDB_STREAMING_MODE_LARGE;
			} else if (strncmp(optarg, "serial", 6) == 0) {
				args.streaming_mode = FDB_STREAMING_MODE_SERIAL;
			} else {
				logr.error("Invalid streaming mode {}", optarg);
				return -1;
			}
			break;
		case ARG_TXNTRACE:
			args.txntrace = atoi(optarg);
			break;

		case ARG_TXNTAGGING:
			args.txntagging = atoi(optarg);
			if (args.txntagging > 1000) {
				args.txntagging = 1000;
			}
			break;
		case ARG_TXNTAGGINGPREFIX:
			if (strlen(optarg) > TAGPREFIXLENGTH_MAX) {
				logr.error("the length of txntagging_prefix is larger than {}", TAGPREFIXLENGTH_MAX);
				_exit(0);
			}
			memcpy(args.txntagging_prefix, optarg, strlen(optarg));
			break;
		case ARG_CLIENT_THREADS_PER_VERSION:
			args.client_threads_per_version = atoi(optarg);
			break;
		case ARG_DISABLE_CLIENT_BYPASS:
			args.disable_client_bypass = true;
			break;
		case ARG_DISABLE_RYW:
			args.disable_ryw = 1;
			break;
		case ARG_JSON_REPORT:
			SET_OPT_ARG_IF_PRESENT();
			if (!optarg) {
				char default_file[] = "mako.json";
				strncpy(args.json_output_path, default_file, sizeof(default_file));
			} else {
				strncpy(args.json_output_path, optarg, std::min(sizeof(args.json_output_path), strlen(optarg) + 1));
			}
			break;
		case ARG_BG_FILE_PATH:
			args.bg_materialize_files = true;
			strncpy(args.bg_file_path, optarg, std::min(sizeof(args.bg_file_path), strlen(optarg) + 1));
			break;
		case ARG_EXPORT_PATH:
			SET_OPT_ARG_IF_PRESENT();
			if (!optarg) {
				char default_file[] = "sketch_data.json";
				strncpy(args.stats_export_path, default_file, sizeof(default_file));
			} else {
				strncpy(args.stats_export_path, optarg, std::min(sizeof(args.stats_export_path), strlen(optarg) + 1));
			}
			break;
		case ARG_DISTRIBUTED_TRACER_CLIENT:
			if (strcmp(optarg, "disabled") == 0) {
				args.distributed_tracer_client = DistributedTracerClient::DISABLED;
			} else if (strcmp(optarg, "network_lossy") == 0) {
				args.distributed_tracer_client = DistributedTracerClient::NETWORK_LOSSY;
			} else if (strcmp(optarg, "log_file") == 0) {
				args.distributed_tracer_client = DistributedTracerClient::LOG_FILE;
			} else {
				args.distributed_tracer_client = -1;
			}
			break;
		case ARG_TLS_CERTIFICATE_FILE:
			args.tls_certificate_file = std::string(optarg);
			break;
		case ARG_TLS_KEY_FILE:
			args.tls_key_file = std::string(optarg);
			break;
		case ARG_TLS_CA_FILE:
			args.tls_ca_file = std::string(optarg);
			break;
		case ARG_AUTHORIZATION_KEYPAIR_ID:
			args.keypair_id = optarg;
			break;
		case ARG_AUTHORIZATION_PRIVATE_KEY_PEM_FILE: {
			std::string pem_filename(optarg);
			std::ifstream ifs(pem_filename);
			std::ostringstream oss;
			oss << ifs.rdbuf();
			args.private_key_pem = oss.str();
		} break;
		case ARG_TRANSACTION_TIMEOUT_TX:
			args.transaction_timeout_tx = atoi(optarg);
			break;
		case ARG_TRANSACTION_TIMEOUT_DB:
			args.transaction_timeout_db = atoi(optarg);
			break;
		case ARG_ENABLE_TOKEN_BASED_AUTHORIZATION:
			args.enable_token_based_authorization = true;
			break;
		}
	}

	if ((args.tpsmin == -1) || (args.tpsmin > args.tpsmax)) {
		args.tpsmin = args.tpsmax;
	}

	return 0;
}

int Arguments::validate() {
	if (mode == MODE_INVALID) {
		logr.error("--mode has to be set");
		return -1;
	}
	if (verbose < VERBOSE_NONE || verbose > VERBOSE_DEBUG) {
		logr.error("--verbose must be between 0 and 3");
		return -1;
	}
	if (rows <= 0) {
		logr.error("--rows must be a positive integer");
		return -1;
	}
	if (load_factor <= 0 || load_factor > 1) {
		logr.error("--load_factor must be in range (0, 1]");
		return -1;
	}
	if (key_length < 0) {
		logr.error("--keylen must be a positive integer");
		return -1;
	}
	if (value_length < 0) {
		logr.error("--vallen must be a positive integer");
		return -1;
	}
	if (num_fdb_clusters > NUM_CLUSTERS_MAX) {
		logr.error("Mako is not supported to do work to more than {} clusters", NUM_CLUSTERS_MAX);
		return -1;
	}
	if (num_databases > NUM_DATABASES_MAX) {
		logr.error("Mako is not supported to do work to more than {} databases", NUM_DATABASES_MAX);
		return -1;
	}
	if (num_databases < num_fdb_clusters) {
		logr.error("--num_databases ({}) must be >= number of clusters({})", num_databases, num_fdb_clusters);
		return -1;
	}
	// In sync mode, threads, and in async mode, async states / workflows are assigned to databases. Having more
	// databases than threads or async states / workflows leads to unused databases.
	if (async_xacts == 0 && num_threads < num_databases) {
		logr.error("--threads ({}) must be >= number of databases ({}) in sync mode", num_threads, num_databases);
		return -1;
	}
	if (async_xacts > 0 && async_xacts < num_databases) {
		logr.error("--async_xacts ({}) must be >= number of databases ({}) in async mode", async_xacts, num_databases);
		return -1;
	}
	// Having more threads than async workflows in the async mode does lead to unused threads.
	if (async_xacts > 0 && num_threads > async_xacts) {
		logr.error("--threads ({}) must be <= --async_xacts", num_threads);
		return -1;
	}
	if (key_length < 4 /* "mako" */ + row_digits) {
		logr.error("--keylen must be larger than {} to store \"mako\" prefix "
		           "and maximum row number",
		           4 + row_digits);
		return -1;
	}
	if (active_tenants > total_tenants) {
		logr.error("--active_tenants must be less than or equal to --total_tenants");
		return -1;
	}
	if (tenant_batch_size < 1) {
		logr.error("--tenant_batch_size must be at least 1");
		return -1;
	}
	if (mode == MODE_RUN) {
		if ((seconds > 0) && (iteration > 0)) {
			logr.error("Cannot specify seconds and iteration together");
			return -1;
		}
		if ((seconds == 0) && (iteration == 0)) {
			logr.error("Must specify either seconds or iteration");
			return -1;
		}
		if (txntagging < 0) {
			logr.error("--txntagging must be a non-negative integer");
			return -1;
		}
		if (iteration > 0) {
			if (async_xacts > 0 && async_xacts * num_processes > iteration) {
				logr.error("--async_xacts * --num_processes must be <= --iteration");
				return -1;
			} else if (async_xacts == 0 && num_threads * num_processes > iteration) {
				logr.error("--num_threads * --num_processes must be <= --iteration");
				return -1;
			}
		}
		if (transaction_timeout_db < 0 || transaction_timeout_tx < 0) {
			logr.error("--transaction_timeout_[tx|db] must be a non-negative integer");
			return -1;
		}
	}

	if (mode != MODE_RUN && (transaction_timeout_db != 0 || transaction_timeout_tx != 0)) {
		logr.error("--transaction_timeout_[tx|db] only supported in run mode");
		return -1;
	}

	if (mode == MODE_RUN || mode == MODE_BUILD) {
		if (tpsmax > 0) {
			if (async_xacts > 0) {
				logr.error("--tpsmax|--tps must be 0 or unspecified because throttling is not supported in async mode");
				return -1;
			} else if (async_xacts == 0 && num_threads * num_processes > tpsmax) {
				logr.error("--num_threads * --num_processes must be <= --tpsmax|--tps");
				return -1;
			}
		}
	}

	// ensure that all of the files provided to mako are valid and exist
	if (mode == MODE_REPORT) {
		if (!num_report_files) {
			logr.error("No files to merge");
		}
		for (int i = 0; i < num_report_files; i++) {
			struct stat buffer;
			if (stat(report_files[i], &buffer) != 0) {
				logr.error("Couldn't open file {}", report_files[i]);
				return -1;
			}
		}
	}
	if (distributed_tracer_client < 0) {
		logr.error("--distributed_tracer_client must specify either (disabled, network_lossy, log_file)");
		return -1;
	}

	if (enable_token_based_authorization) {
		if (async_xacts > 0) {
			logr.error("async mode does not support authorization yet");
			return -1;
		}
		if (num_fdb_clusters > 1) {
			logr.error("for simplicity, --enable_token_based_authorization must be used with exactly one fdb cluster");
			return -1;
		}
		if (active_tenants <= 0 || total_tenants <= 0) {
			logr.error("--enable_token_based_authorization must be used with at least one tenant");
			return -1;
		}
		if (!private_key_pem.has_value() || !keypair_id.has_value()) {
			logr.error("--enable_token_based_authorization must be used with --authorization_keypair_id and "
			           "--authorization_private_key_pem_file");
			return -1;
		}
		if (!tls_key_file.has_value() || !tls_certificate_file.has_value() || !tls_ca_file.has_value()) {
			logr.error(
			    "token-based authorization is enabled without explicit TLS parameter(s) (certificate, key, CA).");
			return -1;
		}
	}
	return 0;
}

bool Arguments::isAuthorizationEnabled() const noexcept {
	return mode != MODE_CLEAN && enable_token_based_authorization && active_tenants > 0 && tls_ca_file.has_value() &&
	       private_key_pem.has_value();
}

void Arguments::collectTenantIds() {
	auto db = Database(cluster_files[0]);
	tenant_ids.clear();
	tenant_ids.reserve(active_tenants);
}

void Arguments::generateAuthorizationTokens() {
	assert(active_tenants > 0);
	assert(keypair_id.has_value());
	assert(private_key_pem.has_value());
	authorization_tokens.clear();
	assert(num_fdb_clusters == 1);
	assert(!tenant_ids.empty());
	// assumes tenants have already been populated
	logr.info("generating authorization tokens to be used by worker threads");
	auto stopwatch = Stopwatch(StartAtCtor{});
	authorization_tokens =
	    generateAuthorizationTokenMap(active_tenants, keypair_id.value(), private_key_pem.value(), tenant_ids);
	assert(authorization_tokens.size() == active_tenants);
	logr.info("generated {} tokens in {:6.3f} seconds", active_tenants, toDoubleSeconds(stopwatch.stop().diff()));
}

std::optional<std::vector<fdb::Tenant>> Arguments::prepareTenants(fdb::Database db) const {
	if (active_tenants > 0) {
		std::vector<fdb::Tenant> tenants(active_tenants);
		for (auto i = 0; i < active_tenants; i++) {
			tenants[i] = db.openTenant(toBytesRef(getTenantNameByIndex(i)));
		}
		return tenants;
	} else {
		return {};
	}
}

void printStats(Arguments const& args, WorkflowStatistics const* stats, double const duration_sec, FILE* fp) {
	static WorkflowStatistics prev;

	const auto num_workers = args.async_xacts > 0 ? args.async_xacts : args.num_threads;
	auto current = WorkflowStatistics{};
	for (auto i = 0; i < args.num_processes * num_workers; i++) {
		current.combine(stats[i]);
	}

	if (fp) {
		fwrite("{", 1, 1, fp);
	}
	putTitleRight("OPS");
	auto print_err = false;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0) {
			const auto ops_total_diff = current.getOpCount(op) - prev.getOpCount(op);
			putField(ops_total_diff);
			if (fp) {
				fmt::print(fp, "\"{}\": {},", getOpName(op), ops_total_diff);
			}
			print_err = print_err || (current.getErrorCount(op) - prev.getErrorCount(op)) > 0;
		}
	}
	/* TPS */
	const auto tps = (current.getOpCount(OP_TRANSACTION) - prev.getOpCount(OP_TRANSACTION)) / duration_sec;
	putFieldFloat(tps, 2);
	if (fp) {
		fprintf(fp, "\"tps\": %.2f,", tps);
	}

	/* Conflicts */
	const auto conflicts_diff = (current.getConflictCount() - prev.getConflictCount()) / duration_sec;
	putFieldFloat(conflicts_diff, 2);
	fmt::print("\n");
	if (fp) {
		fprintf(fp, "\"conflictsPerSec\": %.2f", conflicts_diff);
	}

	if (print_err) {
		putTitleRight("Errors");
		for (auto op = 0; op < MAX_OP; op++) {
			if (args.txnspec.ops[op][OP_COUNT] > 0) {
				const auto errors_diff = current.getErrorCount(op) - prev.getErrorCount(op);
				putField(errors_diff);
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

void printStatsHeader(Arguments const& args, bool show_commit, bool is_first_header_empty, bool show_op_stats) {
	/* header */
	if (is_first_header_empty)
		putTitle("");
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0) {
			putField(getOpName(op));
		}
	}

	if (show_commit)
		putField("COMMIT");
	if (show_op_stats) {
		putField("TRANSACTION");
	} else {
		putField("TPS");
		putField("Conflicts/s");
	}
	fmt::print("\n");

	putTitleBar();
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0) {
			putFieldBar();
		}
	}

	/* COMMIT */
	if (show_commit)
		putFieldBar();

	if (show_op_stats) {
		/* TRANSACTION */
		putFieldBar();
	} else {
		/* TPS */
		putFieldBar();

		/* Conflicts */
		putFieldBar();
	}
	fmt::print("\n");
}

void printWorkerStats(WorkflowStatistics& final_stats, Arguments args, FILE* fp, bool is_report = false) {

	if (is_report) {
		for (auto op = 0; op < MAX_OP; op++) {
			if (final_stats.getLatencySampleCount(op) > 0 && op != OP_COMMIT && op != OP_TRANSACTION) {
				args.txnspec.ops[op][OP_COUNT] = 1;
			}
		}
	}

	fmt::print("Latency (us)");
	printStatsHeader(args, true, false, true);

	/* Total Samples */
	putTitle("Samples");
	bool first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			auto sample_size = final_stats.getLatencySampleCount(op);
			if (sample_size > 0) {
				putField(sample_size);
			} else {
				putField("N/A");
			}
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), sample_size);
			}
		}
	}
	fmt::print("\n");

	/* Min Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"minLatency\": {");
	}
	putTitle("Min");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			const auto lat_min = final_stats.getLatencyUsMin(op);
			if (lat_min == -1) {
				putField("N/A");
			} else {
				putField(lat_min);
				if (fp) {
					if (first_op) {
						first_op = false;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), lat_min);
				}
			}
		}
	}
	fmt::print("\n");

	/* Avg Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"avgLatency\": {");
	}
	putTitle("Avg");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			if (final_stats.getLatencySampleCount(op) > 0) {
				putField(final_stats.mean(op));
				if (fp) {
					if (first_op) {
						first_op = false;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), final_stats.mean(op));
				}
			} else {
				putField("N/A");
			}
		}
	}
	fmt::printf("\n");

	/* Max Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"maxLatency\": {");
	}
	putTitle("Max");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			const auto lat_max = final_stats.getLatencyUsMax(op);
			if (lat_max == 0) {
				putField("N/A");
			} else {
				putField(lat_max);
				if (fp) {
					if (first_op) {
						first_op = false;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), final_stats.getLatencyUsMax(op));
				}
			}
		}
	}
	fmt::print("\n");

	/* Median Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"medianLatency\": {");
	}
	putTitle("Median");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			const auto lat_total = final_stats.getLatencyUsTotal(op);
			const auto lat_samples = final_stats.getLatencySampleCount(op);
			if (lat_total && lat_samples) {
				auto median = final_stats.percentile(op, 0.5);
				putField(median);
				if (fp) {
					if (first_op) {
						first_op = false;
					} else {
						fmt::fprintf(fp, ",");
					}
					fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), median);
				}
			} else {
				putField("N/A");
			}
		}
	}
	fmt::print("\n");

	/* 95%ile Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"p95Latency\": {");
	}
	putTitle("95.0 pctile");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			if (!final_stats.getLatencySampleCount(op) || !final_stats.getLatencyUsTotal(op)) {
				putField("N/A");
				continue;
			}
			const auto point_95pct = final_stats.percentile(op, 0.95);
			putField(point_95pct);
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), point_95pct);
			}
		}
	}
	fmt::printf("\n");

	/* 99%ile Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"p99Latency\": {");
	}
	putTitle("99.0 pctile");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			if (!final_stats.getLatencySampleCount(op) || !final_stats.getLatencyUsTotal(op)) {
				putField("N/A");
				continue;
			}
			const auto point_99pct = final_stats.percentile(op, 0.99);
			putField(point_99pct);
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), point_99pct);
			}
		}
	}
	fmt::print("\n");

	/* 99.9%ile Latency */
	if (fp) {
		fmt::fprintf(fp, "}, \"p99.9Latency\": {");
	}
	putTitle("99.9 pctile");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if (args.txnspec.ops[op][OP_COUNT] > 0 || isAbstractOp(op)) {
			if (!final_stats.getLatencySampleCount(op) || !final_stats.getLatencyUsTotal(op)) {
				putField("N/A");
				continue;
			}
			const auto point_99_9pct = final_stats.percentile(op, 0.999);
			putField(point_99_9pct);
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), point_99_9pct);
			}
		}
	}
	fmt::print("\n");
	if (fp) {
		fmt::fprintf(fp, "}}");
	}
}

void loadSample(int pid_main, int op, std::vector<DDSketchMako>& data_points, int process_id, int thread_id) {
	const auto dirname = fmt::format("{}{}", TEMP_DATA_STORE, pid_main);
	const auto filename = getStatsFilename(dirname, process_id, thread_id, op);
	std::ifstream fp{ filename };
	std::ostringstream sstr;
	sstr << fp.rdbuf();
	DDSketchMako sketch;
	rapidjson::Document doc;
	doc.Parse(sstr.str().c_str());
	if (!doc.HasParseError()) {
		sketch.deserialize(doc);
		if (data_points[op].getPopulationSize() > 0) {
			data_points[op].mergeWith(sketch);
		} else {
			data_points[op] = sketch;
		}
	}
}

void printReport(Arguments const& args,
                 WorkflowStatistics const* worker_stats,
                 ThreadStatistics const* thread_stats,
                 ProcessStatistics const* process_stats,
                 double const duration_sec,
                 pid_t pid_main,
                 FILE* fp) {

	auto final_worker_stats = WorkflowStatistics{};
	const auto num_workers = args.async_xacts > 0 ? args.async_xacts : args.num_threads;

	for (auto i = 0; i < args.num_processes * num_workers; i++) {
		final_worker_stats.combine(worker_stats[i]);
	}

	double cpu_time_worker_threads =
	    std::accumulate(thread_stats,
	                    thread_stats + args.num_processes * args.num_threads,
	                    0.0,
	                    [](double x, const ThreadStatistics& s) { return x + s.getCPUTime(); });
	double total_duration_worker_threads =
	    std::accumulate(thread_stats,
	                    thread_stats + args.num_processes * args.num_threads,
	                    0.0,
	                    [](double x, const ThreadStatistics& s) { return x + s.getTotalDuration(); }) /
	    (args.num_processes * args.num_threads); // average

	double cpu_util_worker_threads = 100. * cpu_time_worker_threads / total_duration_worker_threads;

	double cpu_time_worker_processes = std::accumulate(
	    process_stats, process_stats + args.num_processes, 0.0, [](double x, const ProcessStatistics& s) {
		    return x + s.getProcessCPUTime();
	    });

	double total_duration_worker_processes =
	    std::accumulate(process_stats,
	                    process_stats + args.num_processes,
	                    0.0,
	                    [](double x, const ProcessStatistics& s) { return x + s.getProcessTotalDuration(); }) /
	    args.num_processes; // average

	double cpu_util_worker_processes = 100. * cpu_time_worker_processes / total_duration_worker_processes;

	double cpu_time_local_fdb_networks = std::accumulate(
	    process_stats, process_stats + args.num_processes, 0.0, [](double x, const ProcessStatistics& s) {
		    return x + s.getFDBNetworkCPUTime();
	    });

	double total_duration_local_fdb_networks =
	    std::accumulate(process_stats,
	                    process_stats + args.num_processes,
	                    0.0,
	                    [](double x, const ProcessStatistics& s) { return x + s.getProcessTotalDuration(); }) /
	    args.num_processes; // average

	double cpu_util_local_fdb_networks = 100. * cpu_time_local_fdb_networks / total_duration_local_fdb_networks;

	double cpu_util_external_fdb_networks =
	    100. * (cpu_time_worker_processes - cpu_time_worker_threads - cpu_time_local_fdb_networks) /
	    (total_duration_local_fdb_networks); // assume that external networks have same total duration as local networks

	/* overall stats */
	fmt::printf("\n====== Total Duration %6.3f sec ======\n\n", duration_sec);
	fmt::printf("Total Processes:   %8d\n", args.num_processes);
	fmt::printf("Total Threads:     %8d\n", args.num_threads);
	fmt::printf("Total Async Xacts: %8d\n", args.async_xacts);
	if (args.tpsmax == args.tpsmin)
		fmt::printf("Target TPS:        %8d\n", args.tpsmax);
	else {
		fmt::printf("Target TPS (MAX):  %8d\n", args.tpsmax);
		fmt::printf("Target TPS (MIN):  %8d\n", args.tpsmin);
		fmt::printf("TPS Interval:      %8d\n", args.tpsinterval);
		fmt::printf("TPS Change:        ");
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
	const auto tps_f = final_worker_stats.getOpCount(OP_TRANSACTION) / duration_sec;
	const auto tps_i = static_cast<uint64_t>(tps_f);

	fmt::printf("Total Xacts:       %8lu\n", final_worker_stats.getOpCount(OP_TRANSACTION));
	fmt::printf("Total Conflicts:   %8lu\n", final_worker_stats.getConflictCount());
	fmt::printf("Total Errors:      %8lu\n", final_worker_stats.getTotalErrorCount());
	fmt::printf("Total Timeouts:    %8lu\n", final_worker_stats.getTotalTimeoutCount());
	fmt::printf("Overall TPS:       %8lu\n\n", tps_i);
	fmt::printf("%%CPU Worker Processes:         %6.2f \n", cpu_util_worker_processes);
	fmt::printf("%%CPU Worker Threads:           %6.2f \n", cpu_util_worker_threads);
	fmt::printf("%%CPU Local Network Threads:    %6.2f \n", cpu_util_local_fdb_networks);
	fmt::printf("%%CPU External Network Threads: %6.2f \n\n", cpu_util_external_fdb_networks);

	if (fp) {
		fmt::fprintf(fp, "\"results\": {");
		fmt::fprintf(fp, "\"totalDuration\": %6.3f,", duration_sec);
		fmt::fprintf(fp, "\"totalProcesses\": %d,", args.num_processes);
		fmt::fprintf(fp, "\"totalThreads\": %d,", args.num_threads);
		fmt::fprintf(fp, "\"totalAsyncXacts\": %d,", args.async_xacts);
		fmt::fprintf(fp, "\"targetTPS\": %d,", args.tpsmax);
		fmt::fprintf(fp, "\"totalXacts\": %lu,", final_worker_stats.getOpCount(OP_TRANSACTION));
		fmt::fprintf(fp, "\"totalConflicts\": %lu,", final_worker_stats.getConflictCount());
		fmt::fprintf(fp, "\"totalErrors\": %lu,", final_worker_stats.getTotalErrorCount());
		fmt::fprintf(fp, "\"totalTimeouts\": %lu,", final_worker_stats.getTotalTimeoutCount());
		fmt::fprintf(fp, "\"overallTPS\": %lu,", tps_i);
		fmt::fprintf(fp, "\"workerProcesseCPU\": %.8f,", cpu_util_worker_processes);
		fmt::fprintf(fp, "\"workerThreadCPU\": %.8f,", cpu_util_worker_threads);
		fmt::fprintf(fp, "\"localNetworkCPU\": %.8f,", cpu_util_local_fdb_networks);
		fmt::fprintf(fp, "\"externalNetworkCPU\": %.8f,", cpu_util_external_fdb_networks);
	}

	/* per-op stats */
	printStatsHeader(args, true, true, false);

	/* OPS */
	putTitle("Total OPS");
	if (fp) {
		fmt::fprintf(fp, "\"totalOps\": {");
	}
	auto first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if ((args.txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) || op == OP_COMMIT) {
			putField(final_worker_stats.getOpCount(op));
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), final_worker_stats.getOpCount(op));
			}
		}
	}

	/* TPS */
	const auto tps = final_worker_stats.getOpCount(OP_TRANSACTION) / duration_sec;
	putFieldFloat(tps, 2);

	/* Conflicts */
	const auto conflicts_rate = final_worker_stats.getConflictCount() / duration_sec;
	putFieldFloat(conflicts_rate, 2);
	fmt::print("\n");

	if (fp) {
		fmt::fprintf(fp, "}, \"tps\": %.2f, \"conflictsPerSec\": %.2f, \"errors\": {", tps, conflicts_rate);
	}

	/* Errors */
	putTitle("Errors");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if ((args.txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) || op == OP_COMMIT) {
			putField(final_worker_stats.getErrorCount(op));
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), final_worker_stats.getErrorCount(op));
			}
		}
	}
	fmt::print("\n");

	/* Timeouts */
	if (fp) {
		fmt::fprintf(fp, "}, \"timeouts\": {");
	}
	putTitle("Timeouts");
	first_op = true;
	for (auto op = 0; op < MAX_OP; op++) {
		if ((args.txnspec.ops[op][OP_COUNT] > 0 && op != OP_TRANSACTION) || op == OP_COMMIT) {
			putField(final_worker_stats.getTimeoutCount(op));
			if (fp) {
				if (first_op) {
					first_op = false;
				} else {
					fmt::fprintf(fp, ",");
				}
				fmt::fprintf(fp, "\"%s\": %lu", getOpName(op), final_worker_stats.getTimeoutCount(op));
			}
		}
	}

	if (fp) {
		fmt::fprintf(fp, "}, \"numSamples\": {");
	}
	fmt::print("\n\n");

	// Get the sketches stored in file and merge them together
	std::vector<DDSketchMako> data_points(MAX_OP);
	for (auto op = 0; op < MAX_OP; op++) {
		for (auto i = 0; i < args.num_processes; i++) {

			if (args.async_xacts == 0) {
				for (auto j = 0; j < args.num_threads; j++) {
					loadSample(pid_main, op, data_points, i, j);
				}
			} else {
				// async mode uses only one file per process
				loadSample(pid_main, op, data_points, i, 0);
			}
		}
	}
	final_worker_stats.updateLatencies(data_points);

	printWorkerStats(final_worker_stats, args, fp);

	// export the ddsketch if the flag was set
	if (args.stats_export_path[0] != 0) {
		std::ofstream f(args.stats_export_path);
		f << final_worker_stats;
	}

	const auto command_remove = fmt::format("rm -rf {}{}", TEMP_DATA_STORE, pid_main);
	if (auto rc = system(command_remove.c_str())) {
		logr.error("Command {} returned {}", command_remove, rc);
		return;
	}
}

int statsProcessMain(Arguments const& args,
                     WorkflowStatistics const* worker_stats,
                     ThreadStatistics const* thread_stats,
                     ProcessStatistics const* process_stats,
                     std::atomic<double>& throttle_factor,
                     std::atomic<int> const& signal,
                     std::atomic<int> const& stopcount,
                     pid_t pid_main) {
	bool first_stats = true;

	/* wait until the signal turn on */
	while (signal.load() == SIGNAL_OFF) {
		usleep(10000); /* 10ms */
	}

	if (args.verbose >= VERBOSE_DEFAULT)
		printStatsHeader(args, false, true, false);

	FILE* fp = NULL;
	if (args.json_output_path[0] != '\0') {
		fp = fopen(args.json_output_path, "w");
		fmt::fprintf(fp, "{\"makoArgs\": {");
		fmt::fprintf(fp, "\"api_version\": %d,", args.api_version);
		fmt::fprintf(fp, "\"json\": %d,", args.json);
		fmt::fprintf(fp, "\"num_processes\": %d,", args.num_processes);
		fmt::fprintf(fp, "\"num_threads\": %d,", args.num_threads);
		fmt::fprintf(fp, "\"async_xacts\": %d,", args.async_xacts);
		fmt::fprintf(fp, "\"mode\": %d,", args.mode);
		fmt::fprintf(fp, "\"rows\": %d,", args.rows);
		fmt::fprintf(fp, "\"load_factor\": %lf,", args.load_factor);
		fmt::fprintf(fp, "\"seconds\": %d,", args.seconds);
		fmt::fprintf(fp, "\"iteration\": %d,", args.iteration);
		fmt::fprintf(fp, "\"tpsmax\": %d,", args.tpsmax);
		fmt::fprintf(fp, "\"tpsmin\": %d,", args.tpsmin);
		fmt::fprintf(fp, "\"tpsinterval\": %d,", args.tpsinterval);
		fmt::fprintf(fp, "\"tpschange\": %d,", args.tpschange);
		fmt::fprintf(fp, "\"sampling\": %d,", args.sampling);
		fmt::fprintf(fp, "\"key_length\": %d,", args.key_length);
		fmt::fprintf(fp, "\"value_length\": %d,", args.value_length);
		fmt::fprintf(fp, "\"active_tenants\": %d,", args.active_tenants);
		fmt::fprintf(fp, "\"total_tenants\": %d,", args.total_tenants);
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
		fmt::fprintf(fp, "\"streaming_mode\": %d,", static_cast<int>(args.streaming_mode));
		fmt::fprintf(fp, "\"disable_ryw\": %d,", args.disable_ryw);
		fmt::fprintf(fp, "\"transaction_timeout_db\": %d,", args.transaction_timeout_db);
		fmt::fprintf(fp, "\"transaction_timeout_tx\": %d,", args.transaction_timeout_tx);
		fmt::fprintf(fp, "\"json_output_path\": \"%s\"", args.json_output_path);
		fmt::fprintf(fp, "},\"samples\": [");
	}

	const auto time_start = steady_clock::now();
	auto time_prev = time_start;
	while (signal.load() != SIGNAL_RED) {
		usleep(100000); /* sleep for 100ms */
		auto time_now = steady_clock::now();

		/* print stats every (roughly) 1 sec */
		if (toDoubleSeconds(time_now - time_prev) >= 1.0) {

			/* adjust throttle rate if needed */
			if (args.tpsmax != args.tpsmin) {
				const auto tpsinterval = static_cast<double>(args.tpsinterval);
				const auto tpsmin = static_cast<double>(args.tpsmin);
				const auto tpsmax = static_cast<double>(args.tpsmax);
				const auto pos = fmod(toDoubleSeconds(time_now - time_start), tpsinterval);
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
				printStats(args, worker_stats, toDoubleSeconds(time_now - time_prev), fp);
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
		printReport(
		    args, worker_stats, thread_stats, process_stats, toDoubleSeconds(time_now - time_start), pid_main, fp);
	}

	if (fp) {
		fmt::fprintf(fp, "}");
		fclose(fp);
	}

	return 0;
}

WorkflowStatistics mergeSketchReport(Arguments& args) {

	WorkflowStatistics stats;
	for (int i = 0; i < args.num_report_files; i++) {
		std::ifstream f{ args.report_files[i] };
		WorkflowStatistics tmp;
		f >> tmp;
		stats.combine(tmp);
	}
	return stats;
}

int populateTenants(ipc::AdminServer& admin, const Arguments& args) {
	const auto num_dbs = std::min(args.num_fdb_clusters, args.num_databases);
	logr.info("populating {} tenants for {} database(s)", args.total_tenants, num_dbs);
	auto stopwatch = Stopwatch(StartAtCtor{});
	for (auto i = 0; i < num_dbs; i++) {
		for (auto tenant_id = 0; tenant_id < args.total_tenants;) {
			const auto tenant_id_end = tenant_id + std::min(args.tenant_batch_size, args.total_tenants - tenant_id);
			auto res = admin.send(ipc::BatchCreateTenantRequest{ args.cluster_files[i], tenant_id, tenant_id_end });
			if (res.error_message) {
				logr.error("cluster {}: {}", i + 1, *res.error_message);
				return -1;
			} else {
				logr.debug("created tenants [{}:{}) for cluster {}", tenant_id, tenant_id_end, i + 1);
				tenant_id = tenant_id_end;
			}
		}
	}
	logr.info("populated tenants in {:6.3f} seconds", toDoubleSeconds(stopwatch.stop().diff()));
	return 0;
}

int main(int argc, char* argv[]) {
	setlinebuf(stdout);

	auto rc = int{};
	auto args = Arguments{};
	rc = parseArguments(argc, argv, args);
	if (rc < 0) {
		/* usage printed */
		return 0;
	}
	if (args.active_tenants > 1) {
		args.rows = args.rows / args.active_tenants;
		args.row_digits = digits(args.rows);
	}

	// Allow specifying only the number of active tenants, in which case # active = # total
	if (args.active_tenants > 0 && args.total_tenants == 0) {
		args.total_tenants = args.active_tenants;
	}

	// set --seconds in case no ending condition has been set
	if (args.seconds == 0 && args.iteration == 0) {
		args.seconds = 30; // default value according to documentation
	}

	// if no cluster file is passed, fall back to default parameters
	// (envvar, 'fdb.cluster' or platform-dependent path)
	if (args.num_fdb_clusters == 0) {
		args.num_fdb_clusters = 1;
	}

	rc = args.validate();

	if (rc < 0)
		return -1;

	logr.setVerbosity(args.verbose);

	if (args.mode == MODE_CLEAN) {
		/* cleanup will be done from a single thread */
		args.num_processes = 1;
		args.num_threads = 1;
	}

	if (args.mode == MODE_BUILD) {
		if (args.txnspec.ops[OP_INSERT][OP_COUNT] == 0) {
			parseTransaction(args, "i100");
		}
	}

	if (args.mode == MODE_REPORT) {
		WorkflowStatistics stats = mergeSketchReport(args);
		printWorkerStats(stats, args, NULL, true);
		return 0;
	}

	if (args.total_tenants > 0 &&
	    (args.isAuthorizationEnabled() || args.mode == MODE_BUILD || args.mode == MODE_CLEAN)) {

		// below construction fork()s internally
		auto server = ipc::AdminServer(args);

		if (!server.isClient()) {
			// admin server has finished running. exit immediately
			return 0;
		} else {
			auto res = server.send(ipc::PingRequest{});
			if (res.error_message) {
				logr.error("admin server setup failed: {}", *res.error_message);
				return -1;
			} else {
				logr.info("admin server ready");
			}
		}
		// Use admin server as proxy to creating/deleting tenants or pre-fetching tenant IDs for token signing when
		// authorization is enabled This is necessary when tenant authorization is enabled, in which case the worker
		// threads connect to database as untrusted clients, as which they wouldn't be allowed to create/delete tenants
		// on their own. Although it is possible to allow worker threads to create/delete tenants in a
		// authorization-disabled mode, use the admin server anyway for simplicity.
		if (args.mode == MODE_CLEAN) {
			// short-circuit tenant cleanup
			const auto num_dbs = std::min(args.num_fdb_clusters, args.num_databases);
			for (auto db_id = 0; db_id < num_dbs; db_id++) {
				if (cleanupTenants(server, args, db_id) < 0) {
					return -1;
				}
			}
			return 0;
		} else if (args.mode == MODE_BUILD) {
			// handle population of tenants before-fork
			if (populateTenants(server, args) < 0) {
				return -1;
			}
		}
		if ((args.mode == MODE_BUILD || args.mode == MODE_RUN) && args.isAuthorizationEnabled()) {
			assert(args.num_fdb_clusters == 1);
			// need to fetch tenant IDs to pre-generate tokens
			// fetch all IDs in one go
			auto res = server.send(ipc::FetchTenantIdsRequest{ args.cluster_files[0], 0, args.active_tenants });
			if (res.error_message) {
				logr.error("tenant ID fetch failed: {}", *res.error_message);
				return -1;
			} else {
				logr.info("Successfully prefetched {} tenant IDs", res.ids.size());
				assert(res.ids.size() == args.active_tenants);
				args.tenant_ids = std::move(res.ids);
			}
			args.generateAuthorizationTokens();
		}
	}

	const auto pid_main = getpid();
	/* create the shared memory for stats */
	const auto shmpath = fmt::format("mako{}", pid_main);
	auto shmfd = shm_open(shmpath.c_str(), O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (shmfd < 0) {
		logr.error("shm_open failed: {}", strerror(errno));
		return -1;
	}
	auto shmfd_guard = ExitGuard([shmfd, &shmpath]() {
		close(shmfd);
		shm_unlink(shmpath.c_str());
		unlink(shmpath.c_str());
	});

	const auto async_mode = args.async_xacts > 0;
	const auto num_workers = async_mode ? args.async_xacts : args.num_threads;
	/* allocate */
	const auto shmsize = shared_memory::storageSize(args.num_processes, args.num_threads, num_workers);

	auto shm = std::add_pointer_t<void>{};
	if (ftruncate(shmfd, shmsize) < 0) {
		shm = MAP_FAILED;
		logr.error("ftruncate (fd:{} size:{}) failed", shmfd, shmsize);
		return -1;
	}

	/* map it */
	shm = mmap(NULL, shmsize, PROT_READ | PROT_WRITE, MAP_SHARED, shmfd, 0);
	if (shm == MAP_FAILED) {
		logr.error("mmap (fd:{} size:{}) failed", shmfd, shmsize);
		return -1;
	}
	auto munmap_guard = ExitGuard([=]() { munmap(shm, shmsize); });

	auto shm_access = shared_memory::Access(shm, args.num_processes, args.num_threads, num_workers);

	/* initialize the shared memory */
	shm_access.initMemory();

	/* get ready */
	auto& shm_hdr = shm_access.header();
	shm_hdr.signal = SIGNAL_OFF;
	shm_hdr.readycount = 0;
	shm_hdr.stopcount = 0;
	shm_hdr.throttle_factor = 1.0;

	auto proc_type = ProcKind::MAIN;
	/* fork worker processes + 1 stats process */
	auto worker_pids = std::vector<pid_t>(args.num_processes + 1);

	auto process_idx = int{};

	/* forking (num_process + 1) children */
	/* last process is the stats handler */
	for (auto p = 0; p < args.num_processes + 1; p++) {
		auto pid = fork();
		if (pid != 0) {
			/* master */
			worker_pids[p] = pid;
			if (args.verbose == VERBOSE_DEBUG) {
				logr.debug("worker {} (PID:{}) forked", p + 1, worker_pids[p]);
			}
		} else {
			if (p < args.num_processes) {
				/* worker process */
				logr = Logger(WorkerProcess{}, args.verbose, p);
				proc_type = ProcKind::WORKER;
				process_idx = p;
			} else {
				/* stats */
				logr = Logger(StatsProcess{}, args.verbose);
				proc_type = ProcKind::STATS;
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

	if (proc_type == ProcKind::WORKER) {
		/* worker process */

		workerProcessMain(args, process_idx, shm_access, pid_main);

		_exit(0);
	} else if (proc_type == ProcKind::STATS) {
		/* stats */
		if (args.mode == MODE_CLEAN) {
			/* no stats needed for clean mode */
			_exit(0);
		}
		statsProcessMain(args,
		                 shm_access.workerStatsConstArray(),
		                 shm_access.threadStatsConstArray(),
		                 shm_access.processStatsConstArray(),
		                 shm_hdr.throttle_factor,
		                 shm_hdr.signal,
		                 shm_hdr.stopcount,
		                 pid_main);
		_exit(0);
	}

	/* master */
	/* wait for everyone to be ready */
	while (shm_hdr.readycount.load() < (args.num_processes * args.num_threads)) {
		usleep(1000);
	}
	shm_hdr.signal.store(SIGNAL_GREEN);

	if (args.mode == MODE_RUN) {
		/* run the benchmark */

		/* if seconds is specified, stop child processes after the specified
		 * duration */
		if (args.seconds > 0) {
			logr.debug("master sleeping for {} seconds", args.seconds);

			auto time_start = steady_clock::now();
			while (1) {
				usleep(100000); /* sleep for 100ms */
				auto time_now = steady_clock::now();
				/* doesn't have to be precise */
				if (toDoubleSeconds(time_now - time_start) > args.seconds) {
					logr.debug("time's up ({} seconds)", args.seconds);
					break;
				}
			}

			/* notify everyone the time's up */
			shm_hdr.signal.store(SIGNAL_RED);
		}
	}

	auto status = int{};
	/* wait for worker processes to exit */
	for (auto p = 0; p < args.num_processes; p++) {
		logr.debug("waiting for worker process {} (PID:{}) to exit", p + 1, worker_pids[p]);
		auto pid = waitpid(worker_pids[p], &status, 0 /* or what? */);
		if (pid < 0) {
			logr.error("waitpid failed for worker process PID {}", worker_pids[p]);
		}
		logr.debug("worker {} (PID:{}) exited", p + 1, worker_pids[p]);
	}

	/* all worker threads finished, stop the stats */
	if (args.mode == MODE_BUILD || args.iteration > 0) {
		shm_hdr.signal.store(SIGNAL_RED);
	}

	/* wait for stats to stop */
	auto pid = waitpid(worker_pids[args.num_processes], &status, 0 /* or what? */);
	if (pid < 0) {
		logr.error("waitpid failed for stats process PID {}", worker_pids[args.num_processes]);
	}

	return 0;
}
