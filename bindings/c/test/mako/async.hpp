#ifndef MAKO_ASYNC_HPP
#define MAKO_ASYNC_HPP

#include <atomic>
#include <memory>
#include <boost/asio.hpp>
#include "logger.hpp"
#include "mako.hpp"
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
	ThreadStatistics& stats;
	std::atomic<int>& stopcount;
	LatencySampleBinArray sample_bins;
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
	                          ThreadStatistics& stats,
	                          std::atomic<int>& stopcount,
	                          int key_begin,
	                          int key_end)
	  : logr(logr), db(db), tx(tx), io_context(io_context), args(args), stats(stats), stopcount(stopcount),
	    key_begin(key_begin), key_end(key_end), key_checkpoint(key_begin) {
		keystr.reserve(args.key_length);
		valstr.reserve(args.value_length);
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
	ThreadStatistics& stats;
	std::atomic<int>& stopcount;
	std::atomic<int> const& signal;
	int max_iters;
	OpIterator op_iter;
	LatencySampleBinArray sample_bins;
	fdb::ByteString key1;
	fdb::ByteString key2;
	fdb::ByteString val;
	std::array<Stopwatch, MAX_OP> watch_per_op;
	Stopwatch watch_step;
	Stopwatch watch_commit;
	Stopwatch watch_tx;
	Stopwatch watch_task;
	bool needs_commit;

	ResumableStateForRunWorkload(Logger logr,
	                             fdb::Database db,
	                             fdb::Transaction tx,
	                             boost::asio::io_context& io_context,
	                             Arguments const& args,
	                             ThreadStatistics& stats,
	                             std::atomic<int>& stopcount,
	                             std::atomic<int> const& signal,
	                             int max_iters,
	                             OpIterator op_iter)
	  : logr(logr), db(db), tx(tx), io_context(io_context), args(args), stats(stats), stopcount(stopcount),
	    signal(signal), max_iters(max_iters), op_iter(op_iter), needs_commit(false) {
		key1.reserve(args.key_length);
		key2.reserve(args.key_length);
		val.reserve(args.value_length);
	}
	void signalEnd() noexcept { stopcount.fetch_add(1); }
	bool ended() noexcept {
		return (max_iters != -1 && max_iters >= stats.getOpCount(OP_TASK)) || signal.load() == SIGNAL_RED;
	}
	void postNextTick();
	void runOneTick();
	void onStepSuccess();
};

using RunWorkloadStateHandle = std::shared_ptr<ResumableStateForRunWorkload>;

} // namespace mako

#endif /*MAKO_ASYNC_HPP*/
