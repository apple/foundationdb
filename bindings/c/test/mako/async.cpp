/*
 * async.cpp
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

#include <boost/asio.hpp>
#include "async.hpp"
#include "future.hpp"
#include "logger.hpp"
#include "operations.hpp"
#include "stats.hpp"
#include "time.hpp"
#include "utils.hpp"

extern thread_local mako::Logger logr;

using namespace fdb;

namespace mako {

void ResumableStateForPopulate::postNextTick() {
	boost::asio::post(io_context, [this, state = shared_from_this()]() { runOneTick(); });
}

void ResumableStateForPopulate::runOneTick() {
	const auto num_commit_every = args.txnspec.ops[OP_INSERT][OP_COUNT];
	for (auto i = key_checkpoint; i <= key_end; i++) {
		genKey(keystr.data(), KEY_PREFIX, args, i);
		randomString(valstr.data(), args.value_length);
		tx.set(keystr, valstr);
		stats.incrOpCount(OP_INSERT);
		if (i == key_end || (i - key_begin + 1) % num_commit_every == 0) {
			watch_commit.start();
			tx.commit().then([this, state = shared_from_this(), i](Future f) {
				if (auto err = f.error()) {
					logr.printWithLogLevel(err.retryable() ? VERBOSE_WARN : VERBOSE_NONE,
					                       "ERROR",
					                       "commit for populate returned '{}'",
					                       err.what());
					tx.onError(err).then([this, state = shared_from_this()](Future f) {
						const auto f_rc = handleForOnError(tx, f, "ON_ERROR_FOR_POPULATE");
						if (f_rc == FutureRC::ABORT) {
							signalEnd();
							return;
						} else {
							postNextTick();
						}
					});
				} else {
					// successfully committed
					watch_commit.stop();
					watch_tx.setStop(watch_commit.getStop());
					if (stats.getOpCount(OP_TRANSACTION) % args.sampling == 0) {
						const auto commit_latency = watch_commit.diff();
						const auto tx_duration = watch_tx.diff();
						stats.addLatency(OP_COMMIT, commit_latency);
						stats.addLatency(OP_TRANSACTION, tx_duration);
					}
					stats.incrOpCount(OP_COMMIT);
					stats.incrOpCount(OP_TRANSACTION);
					tx.reset();
					if (args.transaction_timeout_txn > 0) {
						tx.setOption(FDB_TR_OPTION_TIMEOUT, args.transaction_timeout_txn);
					}
					watch_tx.startFromStop();
					key_checkpoint = i + 1;
					if (i != key_end) {
						postNextTick();
					} else {
						logr.debug("Populated {} rows [{}, {}]: {:6.3f} sec",
						           key_end - key_begin + 1,
						           key_begin,
						           key_end,
						           toDoubleSeconds(watch_total.stop().diff()));
						signalEnd();
						return;
					}
				}
			});
			break;
		}
	}
}

void ResumableStateForRunWorkload::postNextTick() {
	boost::asio::post(io_context, [this, state = shared_from_this()]() { runOneTick(); });
}

void ResumableStateForRunWorkload::runOneTick() {
	assert(iter != OpEnd);
	if (iter.step == 0 /* first step */)
		prepareKeys(iter.op, key1, key2, args);
	watch_step.start();
	if (iter.step == 0)
		watch_op = Stopwatch(watch_step.getStart());
	auto f = Future{};
	// to minimize context switch overhead, repeat immediately completed ops
	// in a loop, not an async continuation.
repeat_immediate_steps:
	f = opTable[iter.op].stepFunction(iter.step)(tx, args, key1, key2, val);
	if (!f) {
		// immediately completed client-side ops: e.g. set, setrange, clear, clearrange, ...
		updateStepStats();
		iter = getOpNext(args, iter);
		if (iter == OpEnd)
			onTransactionSuccess();
		else
			goto repeat_immediate_steps;
	} else {
		// step is blocking. register a continuation and return
		f.then([this, state = shared_from_this()](Future f) {
			if (auto postStepFn = opTable[iter.op].postStepFunction(iter.step))
				postStepFn(f, tx, args, key1, key2, val);
			if (iter.stepKind() != StepKind::ON_ERROR) {
				if (auto err = f.error()) {
					logr.printWithLogLevel(isExpectedError(err) ? VERBOSE_WARN : VERBOSE_NONE,
					                       "ERROR",
					                       "{}:{} returned '{}'",
					                       iter.opName(),
					                       iter.step,
					                       err.what());
					updateErrorStats(err, iter.op);
					tx.onError(err).then([this, state = shared_from_this()](Future f) {
						const auto rc =
						    handleForOnError(tx,
						                     f,
						                     fmt::format("{}:{}", iter.opName(), iter.step),
						                     args.transaction_timeout_txn > 0 || args.transaction_timeout_db > 0);
						onIterationEnd(rc);
					});
				} else {
					// async step succeeded
					updateStepStats();
					iter = getOpNext(args, iter);
					if (iter == OpEnd) {
						onTransactionSuccess();
					} else {
						postNextTick();
					}
				}
			} else {
				// blob granules op error
				updateErrorStats(f.error(), iter.op);
				FutureRC rc = handleForOnError(
				    tx, f, "BG_ON_ERROR", args.transaction_timeout_txn > 0 || args.transaction_timeout_db > 0);
				onIterationEnd(rc);
			}
		});
	}
}

void ResumableStateForRunWorkload::updateStepStats() {
	logr.debug("Step {}:{} succeeded", iter.opName(), iter.step);
	// step successful
	watch_step.stop();
	const auto do_sample = stats.getOpCount(OP_TRANSACTION) % args.sampling == 0;
	if (iter.stepKind() == StepKind::COMMIT) {
		// reset transaction boundary
		const auto step_latency = watch_step.diff();
		if (do_sample) {
			stats.addLatency(OP_COMMIT, step_latency);
		}
		tx.reset();
		if (args.transaction_timeout_txn > 0) {
			tx.setOption(FDB_TR_OPTION_TIMEOUT, args.transaction_timeout_txn);
		}
		stats.incrOpCount(OP_COMMIT);
		needs_commit = false;
	}
	// op completed successfully
	if (iter.step + 1 == opTable[iter.op].steps()) {
		if (opTable[iter.op].needsCommit())
			needs_commit = true;
		watch_op.setStop(watch_step.getStop());
		if (do_sample) {
			const auto op_latency = watch_op.diff();
			stats.addLatency(iter.op, op_latency);
		}
		stats.incrOpCount(iter.op);
	}
}

void ResumableStateForRunWorkload::onTransactionSuccess() {
	if (needs_commit || args.commit_get) {
		// task completed, need to commit before finish
		watch_commit.start();
		tx.commit().then([this, state = shared_from_this()](Future f) {
			if (auto err = f.error()) {
				// commit had errors
				logr.printWithLogLevel(isExpectedError(err) ? VERBOSE_WARN : VERBOSE_NONE,
				                       "ERROR",
				                       "Post-iteration commit returned error: {}",
				                       err.what());
				updateErrorStats(err, OP_COMMIT);
				tx.onError(err).then([this, state = shared_from_this()](Future f) {
					const auto rc = handleForOnError(
					    tx, f, "ON_ERROR", args.transaction_timeout_txn > 0 || args.transaction_timeout_db > 0);
					onIterationEnd(rc);
				});
			} else {
				// commit successful
				watch_commit.stop();
				watch_tx.setStop(watch_commit.getStop());
				if (stats.getOpCount(OP_TRANSACTION) % args.sampling == 0) {
					const auto commit_latency = watch_commit.diff();
					const auto tx_duration = watch_tx.diff();
					stats.addLatency(OP_COMMIT, commit_latency);
					stats.addLatency(OP_TRANSACTION, tx_duration);
				}
				stats.incrOpCount(OP_COMMIT);
				stats.incrOpCount(OP_TRANSACTION);
				tx.reset();
				if (args.transaction_timeout_txn > 0) {
					tx.setOption(FDB_TR_OPTION_TIMEOUT, args.transaction_timeout_txn);
				}
				watch_tx.startFromStop();
				onIterationEnd(FutureRC::OK);
			}
		});
	} else {
		// transaction completed but no need to commit
		watch_tx.stop();
		if (stats.getOpCount(OP_TRANSACTION) % args.sampling == 0) {
			const auto tx_duration = watch_tx.diff();
			stats.addLatency(OP_TRANSACTION, tx_duration);
		}
		stats.incrOpCount(OP_TRANSACTION);
		watch_tx.startFromStop();
		tx.reset();
		if (args.transaction_timeout_txn > 0) {
			tx.setOption(FDB_TR_OPTION_TIMEOUT, args.transaction_timeout_txn);
		}
		onIterationEnd(FutureRC::OK);
	}
}
void ResumableStateForRunWorkload::onIterationEnd(FutureRC rc) {
	// restart current iteration from beginning unless ended
	if (rc == FutureRC::OK || rc == FutureRC::ABORT) {
		total_xacts++;
	}
	if (ended()) {
		signalEnd();
	} else {
		iter = getOpBegin(args);
		needs_commit = false;
		postNextTick();
	}
}

void ResumableStateForRunWorkload::updateErrorStats(fdb::Error err, int op) {
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
bool ResumableStateForRunWorkload::isExpectedError(fdb::Error err) {
	return err.retryable() ||
	       ((args.transaction_timeout_txn > 0 || args.transaction_timeout_db > 0) && err.is(1031 /*timeout*/));
}

} // namespace mako
