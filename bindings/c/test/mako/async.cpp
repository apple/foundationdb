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
		genKey(keystr, KEY_PREFIX, args, i);
		randomString(valstr, args.value_length);
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
					if (stats.getOpCount(OP_TASK) % args.sampling == 0) {
						const auto commit_latency = watch_commit.diff();
						const auto tx_duration = watch_tx.diff();
						stats.addLatency(OP_COMMIT, commit_latency);
						stats.addLatency(OP_TRANSACTION, tx_duration);
						stats.addLatency(OP_TASK, tx_duration);
						sample_bins[OP_COMMIT].put(commit_latency);
						sample_bins[OP_TRANSACTION].put(tx_duration);
						sample_bins[OP_TASK].put(tx_duration);
					}
					stats.incrOpCount(OP_COMMIT);
					stats.incrOpCount(OP_TRANSACTION);
					stats.incrOpCount(OP_TASK);
					tx.reset();
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
	watch_step.start();
	if (iter.step == 0 /* first step */) {
		watch_per_op[iter.op] = Stopwatch(watch_step.getStart());
	}
	auto f = Future{};
	// to minimize context switch overhead, repeat immediately completed ops
	// in a loop, not async continuation.
repeat_immediate_steps:
	f = opTable[iter.op].stepFunction(iter.step)(tx, args, key1, key2, val);
	if (!f) {
		// immediately completed client-side ops: e.g. set, setrange, clear, clearrange, ...
		updateStepStats();
		iter = getOpNext(args, iter);
		if (iter == OpEnd)
			onTaskSuccess();
		else
			goto repeat_immediate_steps;
	} else {
		// step is blocking. register a continuation and return
		f.then([this, state = shared_from_this()](Future f) {
			if (iter.stepKind() != StepKind::ON_ERROR) {
				if (auto err = f.error()) {
					logr.printWithLogLevel(err.retryable() ? VERBOSE_WARN : VERBOSE_NONE,
										   "ERROR",
										   "{}:{} returned '{}'",
										   iter.opName(),
										   iter.step,
										   err.what());
					tx.onError(err).then([this, state = shared_from_this()](Future f) {
						const auto rc = handleForOnError(tx, f, fmt::format("{}:{}", iter.opName(), iter.step));
						if (rc == FutureRC::RETRY) {
							stats.incrErrorCount(iter.op);
						} else if (rc == FutureRC::CONFLICT) {
							stats.incrConflictCount();
						} else if (rc == FutureRC::ABORT) {
							tx.reset();
							signalEnd();
							return;
						}
						// restart this iteration from beginning
						iter = getOpBegin(args);
						needs_commit = false;
						postNextTick();
					});
				} else {
					// async step succeeded
					updateStepStats();
					iter = getOpNext(args, iter);
					if (iter == OpEnd) {
						onTaskSuccess();
					} else {
						postNextTick();
					}
				}
			} else {
				// blob granules op error
				auto rc = handleForOnError(tx, f, "BG_ON_ERROR");
				if (rc == FutureRC::RETRY) {
					stats.incrErrorCount(iter.op);
				} else if (rc == FutureRC::CONFLICT) {
					stats.incrConflictCount();
				} else if (rc == FutureRC::ABORT) {
					tx.reset();
					stopcount.fetch_add(1);
					return;
				}
				iter = getOpBegin(args);
				needs_commit = false;
				// restart this iteration from beginning
				postNextTick();
			}
		});
	}
}

void ResumableStateForRunWorkload::updateStepStats() {
	logr.debug("Step {}:{} succeeded", iter.opName(), iter.step);
	// step successful
	watch_step.stop();
	const auto do_sample = stats.getOpCount(OP_TASK) % args.sampling == 0;
	if (iter.stepKind() == StepKind::COMMIT) {
		// reset transaction boundary
		const auto step_latency = watch_step.diff();
		watch_tx.setStop(watch_step.getStop());
		if (do_sample) {
			const auto tx_duration = watch_tx.diff();
			stats.addLatency(OP_COMMIT, step_latency);
			stats.addLatency(OP_TRANSACTION, tx_duration);
			sample_bins[OP_COMMIT].put(step_latency);
			sample_bins[OP_TRANSACTION].put(tx_duration);
		}
		tx.reset();
		watch_tx.startFromStop();
		stats.incrOpCount(OP_COMMIT);
		stats.incrOpCount(OP_TRANSACTION);
		needs_commit = false;
	}
	// op completed successfully
	if (iter.step + 1 == opTable[iter.op].steps()) {
		if (opTable[iter.op].needsCommit())
			needs_commit = true;
		watch_per_op[iter.op].setStop(watch_step.getStop());
		if (do_sample) {
			const auto op_latency = watch_per_op[iter.op].diff();
			stats.addLatency(iter.op, op_latency);
			sample_bins[iter.op].put(op_latency);
		}
		stats.incrOpCount(iter.op);
	}
}

void ResumableStateForRunWorkload::onTaskSuccess() {
	if (needs_commit || args.commit_get) {
		// task completed, need to commit before finish
		watch_commit.start();
		tx.commit().then([this, state = shared_from_this()](Future f) {
			if (auto err = f.error()) {
				// commit had errors
				logr.printWithLogLevel(err.retryable() ? VERBOSE_WARN : VERBOSE_NONE,
									   "ERROR",
									   "Post-iteration commit returned error: {}",
									   err.what());
				tx.onError(err).then([this, state = shared_from_this()](Future f) {
					const auto rc = handleForOnError(tx, f, "ON_ERROR");
					if (rc == FutureRC::CONFLICT)
						stats.incrConflictCount();
					else
						stats.incrErrorCount(OP_COMMIT);
					if (rc == FutureRC::ABORT) {
						signalEnd();
						return;
					}
					if (ended()) {
						signalEnd();
					} else {
						iter = getOpBegin(args);
						needs_commit = false;
						postNextTick();
					}
				});
			} else {
				// commit successful
				watch_commit.stop();
				watch_tx.setStop(watch_commit.getStop());
				watch_task.setStop(watch_commit.getStop());
				if (stats.getOpCount(OP_TASK) % args.sampling == 0) {
					const auto commit_latency = watch_commit.diff();
					const auto tx_duration = watch_tx.diff();
					const auto task_duration = watch_task.diff();
					stats.addLatency(OP_COMMIT, commit_latency);
					stats.addLatency(OP_TRANSACTION, commit_latency);
					stats.addLatency(OP_TASK, task_duration);
					sample_bins[OP_COMMIT].put(commit_latency);
					sample_bins[OP_TRANSACTION].put(tx_duration);
					sample_bins[OP_TASK].put(task_duration);
				}
				stats.incrOpCount(OP_COMMIT);
				stats.incrOpCount(OP_TRANSACTION);
				stats.incrOpCount(OP_TASK);
				tx.reset();
				watch_tx.startFromStop();
				watch_task.startFromStop();
				if (ended()) {
					signalEnd();
				} else {
					// start next iteration
					iter = getOpBegin(args);
					postNextTick();
				}
			}
		});
	} else {
		// task completed but no need to commit
		watch_task.stop();
		if (stats.getOpCount(OP_TASK) % args.sampling == 0) {
			const auto task_duration = watch_task.diff();
			stats.addLatency(OP_TASK, task_duration);
			sample_bins[OP_TASK].put(task_duration);
		}
		stats.incrOpCount(OP_TASK);
		watch_task.startFromStop();
		tx.reset();
		if (ended()) {
			signalEnd();
		} else {
			iter = getOpBegin(args);
			// start next iteration
			postNextTick();
		}
	}
}

} // namespace mako
