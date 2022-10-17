/*
 * TransactionCost.actor.cpp
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

#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

class TransactionCostWorkload : public TestWorkload {
	int iterations{ 1000 };
	Key prefix;
	bool debugTransactions{ false };

	class ITest {
	protected:
		uint64_t index;
		explicit ITest(uint64_t index) : index(index) {}

	public:
		void debugTransaction(Transaction& tr) { tr.debugTransaction(getDebugID(index)); }
		virtual Future<Void> setup(TransactionCostWorkload const& workload, Database const&) { return Void(); }
		virtual Future<Void> exec(TransactionCostWorkload const& workload, Transaction&) = 0;
		virtual int64_t expectedFinalCost() const = 0;
		virtual ~ITest() = default;
	};

	class ReadEmptyTest : public ITest {
	public:
		explicit ReadEmptyTest(uint64_t index) : ITest(index) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Transaction& tr) override {
			return success(tr.get(workload.getKey(20, index)));
		}

		int64_t expectedFinalCost() const override { return 1; }
	};

	class ReadLargeValueTest : public ITest {
		ACTOR static Future<Void> setup(TransactionCostWorkload const* workload,
		                                ReadLargeValueTest* self,
		                                Database cx) {
			state Transaction tr(cx);
			loop {
				try {
					tr.set(workload->getKey(20, self->index), getValue(CLIENT_KNOBS->READ_COST_BYTE_FACTOR));
					wait(tr.commit());
					return Void();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

	public:
		explicit ReadLargeValueTest(int64_t index) : ITest(index) {}

		Future<Void> setup(TransactionCostWorkload const& workload, Database const& cx) override {
			return setup(&workload, this, cx);
		}

		Future<Void> exec(TransactionCostWorkload const& workload, Transaction& tr) override {
			return success(tr.get(workload.getKey(20, index)));
		}

		int64_t expectedFinalCost() const override { return 2; }
	};

	class WriteTest : public ITest {
	public:
		explicit WriteTest(int64_t index) : ITest(index) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Transaction& tr) override {
			tr.set(workload.getKey(20, index), getValue(20));
			return Void();
		}

		int64_t expectedFinalCost() const override { return CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO; }
	};

	class ClearTest : public ITest {
	public:
		explicit ClearTest(int64_t index) : ITest(index) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Transaction& tr) override {
			tr.clear(singleKeyRange(workload.getKey(20, index)));
			return Void();
		}

		int64_t expectedFinalCost() const override {
			// Clears are not measured in Transaction::getTotalCost
			return 0;
		}
	};

	static std::unique_ptr<ITest> createRandomTest(int64_t index) {
		auto const rand = deterministicRandom()->randomInt(0, 4);
		if (rand == 0) {
			return std::make_unique<ReadEmptyTest>(index);
		} else if (rand == 1) {
			return std::make_unique<ReadLargeValueTest>(index);
		} else if (rand == 2) {
			return std::make_unique<WriteTest>(index);
		} else {
			return std::make_unique<ClearTest>(index);
		}
	}

	static constexpr auto transactionTypes = 4;

	Key getKey(uint32_t size, uint64_t index) const {
		return BinaryWriter::toValue(index, Unversioned()).withPrefix(prefix);
	}

	static Value getValue(uint32_t size) { return makeString(size); }

	static UID getDebugID(uint64_t index) { return UID(index << 32, index << 32); }

	ACTOR static Future<Void> runTest(TransactionCostWorkload* self, Database cx, ITest* test) {
		wait(test->setup(*self, cx));
		state Transaction tr(cx);
		if (self->debugTransactions) {
			test->debugTransaction(tr);
		}
		loop {
			try {
				wait(test->exec(*self, tr));
				wait(tr.commit());
				ASSERT_EQ(tr.getTotalCost(), test->expectedFinalCost());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> start(TransactionCostWorkload* self, Database cx) {
		state uint64_t i = 0;
		state Future<Void> f;
		// Must use shared_ptr because Flow doesn't support perfect forwarding into actors
		state std::shared_ptr<ITest> test;
		for (; i < self->iterations; ++i) {
			test = createRandomTest(i);
			wait(runTest(self, cx, test.get()));
		}
		return Void();
	}

public:
	TransactionCostWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		iterations = getOption(options, "iterations"_sr, 1000);
		prefix = getOption(options, "prefix"_sr, "transactionCost/"_sr);
		debugTransactions = getOption(options, "debug"_sr, false);
	}

	static constexpr auto NAME = "TransactionCost";

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return clientId ? Void() : start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TransactionCostWorkload> TransactionCostWorkloadFactory;
