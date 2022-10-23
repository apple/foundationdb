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

#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

class TransactionCostWorkload : public TestWorkload {
	int iterations{ 1000 };
	Key prefix;
	bool debugTransactions{ false };

	Key getKey(uint64_t testNumber, uint64_t index = 0) const {
		BinaryWriter bw(Unversioned());
		bw << bigEndian64(testNumber);
		bw << bigEndian64(index);
		return bw.toValue().withPrefix(prefix);
	}

	static Value getValue(uint32_t size) { return makeString(size); }

	static UID getDebugID(uint64_t testNumber) { return UID(testNumber << 32, testNumber << 32); }

	class ITest {
	protected:
		uint64_t testNumber;
		explicit ITest(uint64_t testNumber) : testNumber(testNumber) {}

	public:
		void debugTransaction(ReadYourWritesTransaction& tr) { tr.debugTransaction(getDebugID(testNumber)); }
		virtual Future<Void> setup(TransactionCostWorkload const& workload, Database const&) { return Void(); }
		virtual Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction>) = 0;
		virtual int64_t expectedFinalCost() const = 0;
		virtual ~ITest() = default;
	};

	class ReadEmptyTest : public ITest {
	public:
		explicit ReadEmptyTest(uint64_t testNumber) : ITest(testNumber) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			return success(tr->get(workload.getKey(testNumber)));
		}

		int64_t expectedFinalCost() const override { return CLIENT_KNOBS->READ_COST_BYTE_FACTOR; }
	};

	class ReadLargeValueTest : public ITest {
		ACTOR static Future<Void> setup(TransactionCostWorkload const* workload,
		                                ReadLargeValueTest* self,
		                                Database cx) {
			state Transaction tr(cx);
			loop {
				try {
					tr.set(workload->getKey(self->testNumber), getValue(CLIENT_KNOBS->READ_COST_BYTE_FACTOR));
					wait(tr.commit());
					return Void();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

	public:
		explicit ReadLargeValueTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> setup(TransactionCostWorkload const& workload, Database const& cx) override {
			return setup(&workload, this, cx);
		}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			return success(tr->get(workload.getKey(testNumber)));
		}

		int64_t expectedFinalCost() const override { return 2 * CLIENT_KNOBS->READ_COST_BYTE_FACTOR; }
	};

	class WriteTest : public ITest {
	public:
		explicit WriteTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			tr->set(workload.getKey(testNumber), getValue(20));
			return Void();
		}

		int64_t expectedFinalCost() const override {
			return CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR;
		}
	};

	class WriteLargeValueTest : public ITest {
	public:
		explicit WriteLargeValueTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			tr->set(workload.getKey(testNumber), getValue(CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR));
			return Void();
		}

		int64_t expectedFinalCost() const override {
			return 2 * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR;
		}
	};

	class WriteMultipleValuesTest : public ITest {
	public:
		explicit WriteMultipleValuesTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			for (int i = 0; i < 10; ++i) {
				tr->set(workload.getKey(testNumber, i), getValue(20));
			}
			return Void();
		}

		int64_t expectedFinalCost() const override {
			return 10 * CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO * CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR;
		}
	};

	class ClearTest : public ITest {
	public:
		explicit ClearTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			tr->clear(singleKeyRange(workload.getKey(testNumber)));
			return Void();
		}

		int64_t expectedFinalCost() const override { return CLIENT_KNOBS->WRITE_COST_BYTE_FACTOR; }
	};

	class ReadRangeTest : public ITest {
		ACTOR static Future<Void> setup(ReadRangeTest* self, TransactionCostWorkload const* workload, Database cx) {
			state Transaction tr(cx);
			loop {
				try {
					for (int i = 0; i < 10; ++i) {
						tr.set(workload->getKey(self->testNumber, i), workload->getValue(20));
					}
					wait(tr.commit());
					return Void();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

	public:
		explicit ReadRangeTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> setup(TransactionCostWorkload const& workload, Database const& cx) override {
			return setup(this, &workload, cx);
		}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			KeyRange const keys = KeyRangeRef(workload.getKey(testNumber, 0), workload.getKey(testNumber, 10));
			return success(tr->getRange(keys, 10));
		}

		int64_t expectedFinalCost() const override { return CLIENT_KNOBS->READ_COST_BYTE_FACTOR; }
	};

	class ReadMultipleValuesTest : public ITest {
		ACTOR static Future<Void> setup(ReadMultipleValuesTest* self,
		                                TransactionCostWorkload const* workload,
		                                Database cx) {
			state Transaction tr(cx);
			loop {
				try {
					for (int i = 0; i < 10; ++i) {
						tr.set(workload->getKey(self->testNumber, i), workload->getValue(20));
					}
					wait(tr.commit());
					return Void();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

	public:
		explicit ReadMultipleValuesTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> setup(TransactionCostWorkload const& workload, Database const& cx) override {
			return setup(this, &workload, cx);
		}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			std::vector<Future<Void>> futures;
			for (int i = 0; i < 10; ++i) {
				futures.push_back(success(tr->get(workload.getKey(testNumber, i))));
			}
			return waitForAll(futures);
		}

		int64_t expectedFinalCost() const override { return 10 * CLIENT_KNOBS->READ_COST_BYTE_FACTOR; }
	};

	class LargeReadRangeTest : public ITest {
		ACTOR static Future<Void> setup(LargeReadRangeTest* self,
		                                TransactionCostWorkload const* workload,
		                                Database cx) {
			state Transaction tr(cx);
			loop {
				try {
					for (int i = 0; i < 10; ++i) {
						tr.set(workload->getKey(self->testNumber, i),
						       workload->getValue(CLIENT_KNOBS->READ_COST_BYTE_FACTOR));
					}
					wait(tr.commit());
					return Void();
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

	public:
		explicit LargeReadRangeTest(int64_t testNumber) : ITest(testNumber) {}

		Future<Void> setup(TransactionCostWorkload const& workload, Database const& cx) override {
			return setup(this, &workload, cx);
		}

		Future<Void> exec(TransactionCostWorkload const& workload, Reference<ReadYourWritesTransaction> tr) override {
			KeyRange const keys = KeyRangeRef(workload.getKey(testNumber, 0), workload.getKey(testNumber, 10));
			return success(tr->getRange(keys, 10));
		}

		int64_t expectedFinalCost() const override { return 11 * CLIENT_KNOBS->READ_COST_BYTE_FACTOR; }
	};

	static std::unique_ptr<ITest> createRandomTest(int64_t testNumber) {
		auto const rand = deterministicRandom()->randomInt(0, 9);
		if (rand == 0) {
			return std::make_unique<ReadEmptyTest>(testNumber);
		} else if (rand == 1) {
			return std::make_unique<ReadLargeValueTest>(testNumber);
		} else if (rand == 2) {
			return std::make_unique<ReadMultipleValuesTest>(testNumber);
		} else if (rand == 3) {
			return std::make_unique<WriteTest>(testNumber);
		} else if (rand == 4) {
			return std::make_unique<WriteLargeValueTest>(testNumber);
		} else if (rand == 5) {
			return std::make_unique<WriteMultipleValuesTest>(testNumber);
		} else if (rand == 6) {
			return std::make_unique<ClearTest>(testNumber);
		} else if (rand == 7) {
			return std::make_unique<ReadRangeTest>(testNumber);
		} else {
			return std::make_unique<LargeReadRangeTest>(testNumber);
		}
	}

	ACTOR static Future<Void> runTest(TransactionCostWorkload* self, Database cx, ITest* test) {
		wait(test->setup(*self, cx));
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		if (self->debugTransactions) {
			test->debugTransaction(*tr);
		}
		loop {
			try {
				wait(test->exec(*self, tr));
				wait(tr->commit());
				ASSERT_EQ(tr->getTotalCost(), test->expectedFinalCost());
				return Void();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	ACTOR static Future<Void> start(TransactionCostWorkload* self, Database cx) {
		state uint64_t testNumber = 0;
		state Future<Void> f;
		// Must use shared_ptr because Flow doesn't support perfect forwarding into actors
		state std::shared_ptr<ITest> test;
		for (; testNumber < self->iterations; ++testNumber) {
			test = createRandomTest(testNumber);
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
