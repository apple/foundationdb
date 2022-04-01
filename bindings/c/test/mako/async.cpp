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
	boost::asio::post(io_context, [this, state=shared_from_this()]() { runOneTick(); });
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
			auto f = tx.commit();
			f.then([this, state=shared_from_this(), i](Future f) {
				if (auto err = f.error()) {
					logr.printWithLogLevel(err.retryable() ? VERBOSE_WARN : VERBOSE_NONE,
					                       "ERROR",
					                       "commit for populate returned '{}'",
					                       err.what());
					auto err_f = tx.onError(err);
					err_f.then([this, state=shared_from_this()](Future f) {
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
	boost::asio::post(io_context, [this, state=shared_from_this()]() { runOneTick(); });
}

#define UNPACK_OP_INFO() \
	[[maybe_unused]] auto& [op, count, step] = op_iter; \
	[[maybe_unused]] const auto step_kind = opTable[op].stepKind(step)

void ResumableStateForRunWorkload::runOneTick() {
	UNPACK_OP_INFO();
	assert(op_iter != OpEnd);
	watch_step.start();
	if (step == 0 /* first step */) {
		watch_per_op[op] = Stopwatch(watch_step.getStart());
	}
	auto f = opTable[op].stepFunction(step)(tx, args, key1, key2, val);
	if (!f) {
		// immediately completed client-side ops: e.g. set, setrange, clear, clearrange, ...
		onStepSuccess();
	} else {
		f.then([this, state=shared_from_this()](Future f) {
			UNPACK_OP_INFO();
			if (step_kind != StepKind::ON_ERROR) {
				if (auto err = f.error()) {
					logr.printWithLogLevel(err.retryable() ? VERBOSE_WARN : VERBOSE_NONE,
					                       "ERROR",
					                       "step {} of op {} returned '{}'",
					                       step,
					                       opTable[op].name(),
					                       err.what());
					tx.onError(err).then([this, state=shared_from_this()](Future f) {
						UNPACK_OP_INFO();
						const auto rc = handleForOnError(tx, f, fmt::format("{}_STEP_{}", opTable[op].name(), step));
						if (rc == FutureRC::RETRY) {
							stats.incrErrorCount(op);
						} else if (rc == FutureRC::CONFLICT) {
							stats.incrConflictCount();
						} else if (rc == FutureRC::ABORT) {
							tx.reset();
							stopcount.fetch_add(1);
							return;
						}
						op_iter = getOpBegin(args);
						needs_commit = false;
						// restart this iteration from beginning
						postNextTick();
					});
					return;
				}
			} else {
				auto rc = handleForOnError(tx, f, "BG_ON_ERROR");
				if (rc == FutureRC::RETRY) {
					stats.incrErrorCount(op);
				} else if (rc == FutureRC::CONFLICT) {
					stats.incrConflictCount();
				} else if (rc == FutureRC::ABORT) {
					tx.reset();
					stopcount.fetch_add(1);
					return;
				}
				op_iter = getOpBegin(args);
				needs_commit = false;
				// restart this iteration from beginning
				postNextTick();
				return;
			}
			onStepSuccess();
		});
	}
}

void ResumableStateForRunWorkload::onStepSuccess() {
	UNPACK_OP_INFO();
	logr.debug("Step {}:{} succeeded", getOpName(op), step);
	// step successful
	watch_step.stop();
	const auto do_sample = stats.getOpCount(OP_TASK) % args.sampling == 0;
	if (step_kind == StepKind::COMMIT) {
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
	if (step + 1 == opTable[op].steps()) {
		if (opTable[op].needsCommit())
			needs_commit = true;
		watch_per_op[op].setStop(watch_step.getStop());
		if (do_sample) {
			const auto op_latency = watch_per_op[op].diff();
			stats.addLatency(op, op_latency);
			sample_bins[op].put(op_latency);
		}
		stats.incrOpCount(op);
	}
	op_iter = getOpNext(args, op_iter);
	if (op_iter == OpEnd) {
		if (needs_commit || args.commit_get) {
			// task completed, need to commit before finish
			watch_commit.start();
			auto f = tx.commit().eraseType();
			f.then([this, state=shared_from_this()](Future f) {
				UNPACK_OP_INFO();
				if (auto err = f.error()) {
					// commit had errors
					logr.printWithLogLevel(err.retryable() ? VERBOSE_WARN : VERBOSE_NONE,
					                       "ERROR",
					                       "Post-iteration commit returned error: {}",
					                       err.what());
					tx.onError(err).then([this, state=shared_from_this()](Future f) {
						const auto rc = handleForOnError(tx, f, "ON_ERROR");
						if (rc == FutureRC::CONFLICT)
							stats.incrConflictCount();
						else
							stats.incrErrorCount(OP_COMMIT);
						if (rc == FutureRC::ABORT) {
							signalEnd();
							return;
						}
						op_iter = getOpBegin(args);
						needs_commit = false;
						postNextTick();
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
						return;
					}
					// start next iteration
					op_iter = getOpBegin(args);
					postNextTick();
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
			watch_tx.startFromStop();
			watch_task.startFromStop();
			tx.reset();
			if (ended()) {
				signalEnd();
				return;
			}
			op_iter = getOpBegin(args);
			// run next iteration
			postNextTick();
		}
	} else {
		postNextTick();
	}
}

} // namespace mako
