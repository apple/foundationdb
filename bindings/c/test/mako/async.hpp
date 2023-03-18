/*
 * async.hpp
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

#ifndef MAKO_ASYNC_HPP
#define MAKO_ASYNC_HPP

#include <atomic>
#include <memory>
#include <boost/asio.hpp>
#include "logger.hpp"
#include "mako.hpp"
#include "future.hpp"
#include "shm.hpp"
#include "stats.hpp"
#include "time.hpp"

namespace mako {

// as we don't have coroutines yet, we need to store in heap the complete state of execution,
// such that we can resume exactly where we were from last database op.
struct ResumableStateForPopulate : std::enable_shared_from_this<ResumableStateForPopulate> {
	Logger logr;
	fdb::Database db;
	fdb::Transaction tx;
	boost::asio::io_context& io_context;
	Arguments const& args;
	WorkflowStatistics& stats;
	std::atomic<int>& stopcount;
	int key_begin;
	int key_end;
	int key_checkpoint;
	fdb::ByteString keystr;
	fdb::ByteString valstr;
	Stopwatch watch_tx;
	Stopwatch watch_commit;
	Stopwatch watch_total;

	ResumableStateForPopulate(Logger logr,
	                          fdb::Database db,
	                          fdb::Transaction tx,
	                          boost::asio::io_context& io_context,
	                          Arguments const& args,
	                          WorkflowStatistics& stats,
	                          std::atomic<int>& stopcount,
	                          int key_begin,
	                          int key_end)
	  : logr(logr), db(db), tx(tx), io_context(io_context), args(args), stats(stats), stopcount(stopcount),
	    key_begin(key_begin), key_end(key_end), key_checkpoint(key_begin) {
		keystr.resize(args.key_length);
		valstr.resize(args.value_length);
	}
	void runOneTick();
	void postNextTick();
	void signalEnd() { stopcount.fetch_add(1); }
};

using PopulateStateHandle = std::shared_ptr<ResumableStateForPopulate>;

struct ResumableStateForRunWorkload : std::enable_shared_from_this<ResumableStateForRunWorkload> {
	Logger logr;
	fdb::Database db;
	fdb::Transaction tx;
	boost::asio::io_context& io_context;
	Arguments const& args;
	WorkflowStatistics& stats;
	int64_t total_xacts;
	std::atomic<int>& stopcount;
	std::atomic<int> const& signal;
	int max_iters;
	OpIterator iter;
	fdb::ByteString key1;
	fdb::ByteString key2;
	fdb::ByteString val;
	Stopwatch watch_step;
	Stopwatch watch_op;
	Stopwatch watch_commit;
	Stopwatch watch_tx;
	bool needs_commit;

	ResumableStateForRunWorkload(Logger logr,
	                             fdb::Database db,
	                             fdb::Transaction tx,
	                             boost::asio::io_context& io_context,
	                             Arguments const& args,
	                             WorkflowStatistics& stats,
	                             std::atomic<int>& stopcount,
	                             std::atomic<int> const& signal,
	                             int max_iters,
	                             OpIterator iter)
	  : logr(logr), db(db), tx(tx), io_context(io_context), args(args), stats(stats), total_xacts(0),
	    stopcount(stopcount), signal(signal), max_iters(max_iters), iter(iter), needs_commit(false) {
		key1.resize(args.key_length);
		key2.resize(args.key_length);
		val.resize(args.value_length);
		setTransactionTimeoutIfEnabled(args, tx);
	}
	void signalEnd() noexcept { stopcount.fetch_add(1); }
	bool ended() noexcept { return (max_iters != -1 && total_xacts >= max_iters) || signal.load() == SIGNAL_RED; }
	void postNextTick();
	void runOneTick();
	void updateStepStats();
	void onTransactionSuccess();
	void onIterationEnd(FutureRC rc);
	void updateErrorStats(fdb::Error err, int op);
	bool isExpectedError(fdb::Error err);
};

using RunWorkloadStateHandle = std::shared_ptr<ResumableStateForRunWorkload>;

} // namespace mako

#endif /*MAKO_ASYNC_HPP*/
