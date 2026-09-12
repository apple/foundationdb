/*
 * async_test.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include "doctest.h"

#include "async.hpp"
#include "operations.hpp"
#include <algorithm>
#include <functional>

thread_local mako::Logger logr{ mako::MainProcess{}, mako::VERBOSE_NONE };

namespace mako {

// The driver tests do not link the benchmark's main or require a database.
Arguments::Arguments()
  : rows(100), row_digits(3), sampling(1), key_length(16), value_length(16), zipf(0), commit_get(0), txnspec{},
    prefixpadding(0), transaction_timeout_db(0), transaction_timeout_tx(0), max_grv_queue_delay_ms(-1) {}

bool Arguments::isAnyTimeoutEnabled() const {
	return transaction_timeout_tx > 0 || transaction_timeout_db > 0;
}

namespace {

struct StopTick {};
const auto staleStart = timepoint_t{} - std::chrono::seconds(1);

class AsyncWorkloadTest {
	static thread_local AsyncWorkloadTest* active;
	Arguments args;
	WorkflowStatistics stats;
	boost::asio::io_context io;
	std::atomic<int> stopcount{ 0 };
	std::atomic<int> signal{ SIGNAL_GREEN };
	RunWorkloadStateHandle state;
	std::function<void(ResumableStateForRunWorkload&)> observer;

public:
	AsyncWorkloadTest(WorkloadSpec spec, std::function<void(ResumableStateForRunWorkload&)> observer)
	  : observer(std::move(observer)) {
		assert(active == nullptr);
		args.txnspec = spec;
		state = std::make_shared<ResumableStateForRunWorkload>(
		    logr, fdb::Database{}, fdb::Transaction{}, io, args, stats, stopcount, signal, -1, getOpBegin(args));
		active = this;
	}
	~AsyncWorkloadTest() { active = nullptr; }
	AsyncWorkloadTest(const AsyncWorkloadTest&) = delete;
	AsyncWorkloadTest& operator=(const AsyncWorkloadTest&) = delete;

	ResumableStateForRunWorkload& workload() { return *state; }
	WorkflowStatistics const& statistics() const { return stats; }
	void runTick() { state->runOneTick(); }
	void pollTick() { io.poll_one(); }

	static fdb::Future step(fdb::Transaction&, Arguments const&, fdb::ByteString&, fdb::ByteString&, fdb::ByteString&) {
		active->observer(*active->state);
		return {};
	}
};

thread_local AsyncWorkloadTest* AsyncWorkloadTest::active = nullptr;

void checkPreparedKey(fdb::ByteString const& key) {
	CHECK(std::equal(KEY_PREFIX.begin(), KEY_PREFIX.end(), key.begin()));
}

void checkFreshTimers(ResumableStateForRunWorkload const& state) {
	CHECK(state.watch_step.getStart() != staleStart);
	CHECK(state.watch_op.getStart() == state.watch_step.getStart());
}

void poisonOperation(ResumableStateForRunWorkload& state) {
	state.key1.assign(state.key1.size(), '!');
	state.key2.assign(state.key2.size(), '!');
	// Distinct starts make resets observable without waiting for the clock to advance.
	state.watch_step = Stopwatch(staleStart);
	state.watch_op = Stopwatch(staleStart);
}

} // namespace

// Synthetic immediate steps exercise the real driver while the observer stops before commit/reset.
const std::array<Operation, MAX_OP> opTable = [] {
	std::array<Operation, MAX_OP> table{};
	const auto step = Step{ StepKind::IMM, &AsyncWorkloadTest::step };
	table[OP_OVERWRITE] = Operation{ "OVERWRITE", { step }, 1, true };
	table[OP_CLEAR] = Operation{ "CLEAR", { step }, 1, true };
	table[OP_SETCLEAR] = Operation{ "SETCLEAR", { step, step }, 2, true };
	table[OP_CLEARRANGE] = Operation{ "CLEARRANGE", { step }, 1, true };
	return table;
}();

TEST_CASE("mako async repeated immediate operations") {
	WorkloadSpec spec{};
	spec.ops[OP_OVERWRITE][OP_COUNT] = 10;
	spec.ops[OP_CLEAR][OP_COUNT] = 1;
	int overwrites = 0;
	AsyncWorkloadTest test(spec, [&](auto& state) {
		checkPreparedKey(state.key1);
		checkFreshTimers(state);
		if (state.iter.op == OP_CLEAR)
			throw StopTick{};
		CHECK(state.iter.count == overwrites++);
		poisonOperation(state);
	});
	CHECK_THROWS_AS(test.runTick(), StopTick);
	CHECK(overwrites == 10);
	CHECK(test.statistics().getOpCount(OP_OVERWRITE) == 10);
	CHECK(test.statistics().getLatencySampleCount(OP_OVERWRITE) == 10);
}

TEST_CASE("mako async immediate transition prepares range keys") {
	WorkloadSpec spec{};
	spec.ops[OP_OVERWRITE][OP_COUNT] = 1;
	spec.ops[OP_CLEARRANGE][OP_COUNT] = 1;
	spec.ops[OP_CLEARRANGE][OP_RANGE] = 7;
	AsyncWorkloadTest test(spec, [&](auto& state) {
		checkPreparedKey(state.key1);
		checkFreshTimers(state);
		if (state.iter.op == OP_CLEARRANGE) {
			checkPreparedKey(state.key2);
			throw StopTick{};
		}
		poisonOperation(state);
	});
	CHECK_THROWS_AS(test.runTick(), StopTick);
	CHECK(test.statistics().getOpCount(OP_OVERWRITE) == 1);
}

TEST_CASE("mako async preserves keys and operation timer between steps") {
	WorkloadSpec spec{};
	spec.ops[OP_SETCLEAR][OP_COUNT] = 2;
	spec.ops[OP_CLEARRANGE][OP_COUNT] = 1;
	AsyncWorkloadTest test(spec, [&](auto& state) {
		CHECK(state.watch_step.getStart() != staleStart);
		if (state.iter.step == 0) {
			checkPreparedKey(state.key1);
			checkFreshTimers(state);
		} else {
			CHECK(state.key1 == fdb::ByteString(state.key1.size(), '!'));
			CHECK(state.key2 == fdb::ByteString(state.key2.size(), '!'));
			CHECK(state.watch_op.getStart() == staleStart);
		}
		if (state.iter.op == OP_CLEARRANGE)
			throw StopTick{};
		poisonOperation(state);
	});
	SUBCASE("steps complete in the same tick") {}
	SUBCASE("resume at a later step") {
		test.workload().iter.step = 1;
		poisonOperation(test.workload());
	}
	CHECK_THROWS_AS(test.runTick(), StopTick);
	CHECK(test.statistics().getOpCount(OP_SETCLEAR) == 2);
}

TEST_CASE("mako async retry restarts operation preparation") {
	WorkloadSpec spec{};
	spec.ops[OP_OVERWRITE][OP_COUNT] = 2;
	spec.ops[OP_CLEAR][OP_COUNT] = 1;
	int overwrites = 0;
	AsyncWorkloadTest test(spec, [&](auto& state) {
		checkPreparedKey(state.key1);
		checkFreshTimers(state);
		if (state.iter.op == OP_CLEAR)
			throw StopTick{};
		CHECK(state.iter.count == overwrites++);
		poisonOperation(state);
	});
	test.workload().iter.count = 1;
	test.workload().needs_commit = true;
	poisonOperation(test.workload());
	test.workload().onIterationEnd(FutureRC::RETRY);
	CHECK(test.workload().total_xacts == 0);
	CHECK_FALSE(test.workload().needs_commit);
	CHECK_THROWS_AS(test.pollTick(), StopTick);
	CHECK(overwrites == 2);
}

} // namespace mako
