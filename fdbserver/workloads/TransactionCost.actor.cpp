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

	static constexpr auto transactionTypes = 3;

	ACTOR static Future<Void> read(Database cx, Optional<UID> debugID) {
		state Transaction tr(cx);
		if (debugID.present()) {
			tr.debugTransaction(debugID.get());
		}
		loop {
			try {
				ASSERT_EQ(tr.getTotalCost(), 0);
				wait(success(tr.get("foo"_sr)));
				ASSERT_EQ(tr.getTotalCost(), 1);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> write(Database cx, Optional<UID> debugID) {
		state Transaction tr(cx);
		if (debugID.present()) {
			tr.debugTransaction(debugID.get());
		}
		loop {
			try {
				ASSERT_EQ(tr.getTotalCost(), 0);
				tr.set("foo"_sr, "bar"_sr);
				ASSERT_EQ(tr.getTotalCost(), CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO);
				wait(tr.commit());
				ASSERT_EQ(tr.getTotalCost(), CLIENT_KNOBS->GLOBAL_TAG_THROTTLING_RW_FUNGIBILITY_RATIO);
				return Void();
			} catch (Error& e) {
				TraceEvent("TransactionCost_Error").error(e);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> clear(Database cx, Optional<UID> debugID) {
		state Transaction tr(cx);
		if (debugID.present()) {
			tr.debugTransaction(debugID.get());
		}
		loop {
			try {
				ASSERT_EQ(tr.getTotalCost(), 0);
				tr.clear(singleKeyRange("foo"_sr));
				// Clears are not measured in Transaction::getTotalCost
				ASSERT_EQ(tr.getTotalCost(), 0);
				wait(tr.commit());
				ASSERT_EQ(tr.getTotalCost(), 0);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> start(TransactionCostWorkload* self, Database cx) {
		state uint64_t i = 0;
		state Future<Void> f;
		for (; i < self->iterations; ++i) {
			int rand = deterministicRandom()->randomInt(0, transactionTypes);
			auto const debugID = self->debugTransactions ? UID(i << 32, i << 32) : Optional<UID>();
			if (rand == 0) {
				f = read(cx, debugID);
			} else if (rand == 1) {
				f = write(cx, debugID);
			} else if (rand == 2) {
				f = clear(cx, debugID);
			}
			wait(f);
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

	std::string description() const override { return NAME; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return start(this, cx); }

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<TransactionCostWorkload> Transaction("TransactionCost");
