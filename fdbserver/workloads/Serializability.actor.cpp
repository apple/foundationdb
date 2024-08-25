/*
 * Serializability.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/ActorCollection.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct SerializabilityWorkload : TestWorkload {
	static constexpr auto NAME = "Serializability";

	double testDuration;
	bool adjacentKeys;
	int nodes;
	int numOps;
	std::pair<int, int> valueSizeRange;
	int maxClearSize;
	std::string keyPrefix;

	bool success;

	struct GetRangeOperation {
		KeySelector begin;
		KeySelector end;
		int limit;
		Snapshot snapshot{ Snapshot::False };
		Reverse reverse{ Reverse::False };
	};

	struct GetKeyOperation {
		KeySelector key;
		Snapshot snapshot{ Snapshot::False };
	};

	struct GetOperation {
		Key key;
		Snapshot snapshot{ Snapshot::False };
	};

	struct TransactionOperation {
		Optional<Standalone<MutationRef>> mutationOp;
		Optional<GetRangeOperation> getRangeOp;
		Optional<GetKeyOperation> getKeyOp;
		Optional<GetOperation> getOp;
		Optional<Key> watchOp;
		Optional<KeyRange> writeConflictOp;
		Optional<KeyRange> readConflictOp;
	};

	SerializabilityWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), success(true) {
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		numOps = getOption(options, "numOps"_sr, 21);
		nodes = getOption(options, "nodes"_sr, 1000);

		adjacentKeys = false; // deterministicRandom()->random01() < 0.5;
		valueSizeRange = std::make_pair(0, 100);
		// keyPrefix = "\x02";

		maxClearSize = deterministicRandom()->randomInt(10, 2 * nodes);
		if (clientId == 0)
			TraceEvent("SerializabilityConfiguration")
			    .detail("Nodes", nodes)
			    .detail("AdjacentKeys", adjacentKeys)
			    .detail("ValueSizeMin", valueSizeRange.first)
			    .detail("ValueSizeMax", valueSizeRange.second)
			    .detail("MaxClearSize", maxClearSize);
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId == 0)
			return _start(cx, this);
		return Void();
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Value getRandomValue() const {
		return Value(
		    std::string(deterministicRandom()->randomInt(valueSizeRange.first, valueSizeRange.second + 1), 'x'));
	}

	Key getRandomKey() const { return getKeyForIndex(deterministicRandom()->randomInt(0, nodes)); }

	Key getKeyForIndex(int idx) const {
		if (adjacentKeys) {
			return Key(idx ? keyPrefix + std::string(idx, '\x00') : "");
		} else {
			return Key(keyPrefix + format("%010d", idx));
		}
	}

	KeySelector getRandomKeySelector() const {
		int scale = 1 << deterministicRandom()->randomInt(0, 14);
		return KeySelectorRef(
		    getRandomKey(), deterministicRandom()->random01() < 0.5, deterministicRandom()->randomInt(-scale, scale));
	}

	KeyRange getRandomRange(int sizeLimit) const {
		int startLocation = deterministicRandom()->randomInt(0, nodes);
		int scale = deterministicRandom()->randomInt(
		    0, deterministicRandom()->randomInt(2, 5) * deterministicRandom()->randomInt(2, 5));
		int endLocation = startLocation + deterministicRandom()->randomInt(
		                                      0, 1 + std::min(sizeLimit, std::min(nodes - startLocation, 1 << scale)));

		return KeyRangeRef(getKeyForIndex(startLocation), getKeyForIndex(endLocation));
	}

	std::vector<TransactionOperation> randomTransaction() {
		int maxOps = deterministicRandom()->randomInt(1, numOps);
		std::vector<TransactionOperation> result;
		bool hasMutation = false;
		for (int j = 0; j < maxOps; j++) {
			int operationType = deterministicRandom()->randomInt(0, 20);
			TransactionOperation op;
			if (operationType == 0) {
				GetKeyOperation getKey;
				getKey.key = getRandomKeySelector();
				getKey.snapshot.set(deterministicRandom()->coinflip());
				op.getKeyOp = getKey;
			} else if (operationType == 1) {
				GetRangeOperation getRange;
				getRange.begin = getRandomKeySelector();
				getRange.end = getRandomKeySelector();
				getRange.limit = deterministicRandom()->randomInt(0, 1 << deterministicRandom()->randomInt(1, 10));
				getRange.reverse.set(deterministicRandom()->coinflip());
				getRange.snapshot.set(deterministicRandom()->coinflip());
				op.getRangeOp = getRange;
			} else if (operationType == 2) {
				GetOperation getOp;
				getOp.key = getRandomKey();
				getOp.snapshot.set(deterministicRandom()->coinflip());
				op.getOp = getOp;
			} else if (operationType == 3) {
				KeyRange range = getRandomRange(maxClearSize);
				op.mutationOp = MutationRef(MutationRef::ClearRange, range.begin, range.end);
				if (!range.empty())
					hasMutation = true;
			} else if (operationType == 4) {
				KeyRange range = singleKeyRange(getRandomKey());
				op.mutationOp = MutationRef(MutationRef::ClearRange, range.begin, range.end);
				hasMutation = true;
			} else if (operationType == 5) {
				op.watchOp = getRandomKey();
			} else if (operationType == 6) {
				op.writeConflictOp = getRandomRange(maxClearSize);
			} else if (operationType == 7) {
				op.readConflictOp = getRandomRange(maxClearSize);
			} else if (operationType == 8) {
				Key key = getRandomKey();
				Value value = getRandomValue();
				MutationRef::Type opType;
				switch (deterministicRandom()->randomInt(0, 8)) {
				case 0:
					opType = MutationRef::AddValue;
					break;
				case 1:
					opType = MutationRef::And;
					break;
				case 2:
					opType = MutationRef::Or;
					break;
				case 3:
					opType = MutationRef::Xor;
					break;
				case 4:
					opType = MutationRef::Max;
					break;
				case 5:
					opType = MutationRef::Min;
					break;
				case 6:
					opType = MutationRef::ByteMin;
					break;
				case 7:
					opType = MutationRef::ByteMax;
					break;
				}
				op.mutationOp = MutationRef(opType, key, value);
				hasMutation = true;
			} else if (operationType >= 9) {
				Key key = getRandomKey();
				Value value = getRandomValue();
				op.mutationOp = MutationRef(MutationRef::SetValue, key, value);
				hasMutation = true;
			}
			result.push_back(op);
		}

		if (!hasMutation) {
			Key key = getRandomKey();
			Value value = getRandomValue();
			TransactionOperation op;
			op.mutationOp = MutationRef(MutationRef::SetValue, key, value);
			result.push_back(op);
		}

		return result;
	}

	template <class T>
	static void dontCheck(std::vector<Future<T>>& futures) {
		// Replace the last future in the vector with one that will be completed at the same time and
		// with the same error status, but has a constant result.  This is used to suppress the results
		// of reads that aren't deterministic in the test context.
		futures.back() = tag(::success(futures.back()), T());
	}

	ACTOR static Future<Void> runTransaction(ReadYourWritesTransaction* tr,
	                                         std::vector<TransactionOperation> ops,
	                                         std::vector<Future<Optional<Value>>>* getFutures,
	                                         std::vector<Future<Key>>* getKeyFutures,
	                                         std::vector<Future<RangeResult>>* getRangeFutures,
	                                         std::vector<Future<Void>>* watchFutures,
	                                         bool checkSnapshotReads) {
		state int opNum = 0;
		for (; opNum < ops.size(); opNum++) {
			if (ops[opNum].getKeyOp.present()) {
				auto& op = ops[opNum].getKeyOp.get();
				//TraceEvent("SRL_GetKey").detail("Key", op.key.toString()).detail("Snapshot", op.snapshot);
				getKeyFutures->push_back(tr->getKey(op.key, op.snapshot));
				if (op.snapshot && !checkSnapshotReads)
					dontCheck(*getKeyFutures);
			} else if (ops[opNum].getOp.present()) {
				auto& op = ops[opNum].getOp.get();
				//TraceEvent("SRL_Get").detail("Key", printable(op.key)).detail("Snapshot", op.snapshot);
				getFutures->push_back(tr->get(op.key, op.snapshot));
				if (op.snapshot && !checkSnapshotReads)
					dontCheck(*getFutures);
			} else if (ops[opNum].getRangeOp.present()) {
				auto& op = ops[opNum].getRangeOp.get();
				//TraceEvent("SRL_GetRange").detail("Begin", op.begin.toString()).detail("End", op.end.toString()).detail("Limit", op.limit).detail("Snapshot", op.snapshot).detail("Reverse", op.reverse);
				getRangeFutures->push_back(tr->getRange(op.begin, op.end, op.limit, op.snapshot, op.reverse));
				if (op.snapshot && !checkSnapshotReads)
					dontCheck(*getRangeFutures);
			} else if (ops[opNum].mutationOp.present()) {
				auto& op = ops[opNum].mutationOp.get();
				if (op.type == MutationRef::SetValue) {
					//TraceEvent("SRL_Set").detail("Mutation", op);
					tr->set(op.param1, op.param2);
				} else if (op.type == MutationRef::ClearRange) {
					//TraceEvent("SRL_Clear").detail("Mutation", op);
					tr->clear(KeyRangeRef(op.param1, op.param2));
				} else {
					//TraceEvent("SRL_AtomicOp").detail("Mutation", op);
					tr->atomicOp(op.param1, op.param2, op.type);
				}
			} else if (ops[opNum].readConflictOp.present()) {
				auto& op = ops[opNum].readConflictOp.get();
				//TraceEvent("SRL_ReadConflict").detail("Range", printable(op));
				tr->addReadConflictRange(op);
			} else if (ops[opNum].watchOp.present()) {
				auto& op = ops[opNum].watchOp.get();
				//TraceEvent("SRL_Watch").detail("Key", printable(op));
				watchFutures->push_back(tr->watch(op));
			} else if (ops[opNum].writeConflictOp.present()) {
				auto& op = ops[opNum].writeConflictOp.get();
				//TraceEvent("SRL_WriteConflict").detail("Range", printable(op));
				tr->addWriteConflictRange(op);
			}

			// sometimes wait for a random operation
			if (deterministicRandom()->random01() < 0.2) {
				state int waitType = deterministicRandom()->randomInt(0, 4);
				loop {
					if (waitType == 0 && getFutures->size()) {
						wait(::success(deterministicRandom()->randomChoice(*getFutures)));
						break;
					} else if (waitType == 1 && getKeyFutures->size()) {
						wait(::success(deterministicRandom()->randomChoice(*getKeyFutures)));
						break;
					} else if (waitType == 2 && getRangeFutures->size()) {
						wait(::success(deterministicRandom()->randomChoice(*getRangeFutures)));
						break;
					} else if (waitType == 3) {
						wait(delay(0.001 * deterministicRandom()->random01()));
						break;
					}
					waitType = (waitType + 1) % 4;
				}
			}
		}

		return Void();
	}

	ACTOR static Future<RangeResult> getDatabaseContents(Database cx, int nodes) {
		state ReadYourWritesTransaction tr(cx);

		RangeResult result = wait(tr.getRange(normalKeys, nodes + 1));
		ASSERT(result.size() <= nodes);
		return result;
	}

	ACTOR static Future<Void> resetDatabase(Database cx, Standalone<VectorRef<KeyValueRef>> data) {
		state ReadYourWritesTransaction tr(cx);

		tr.clear(normalKeys);
		for (auto kv : data)
			tr.set(kv.key, kv.value);
		wait(tr.commit());
		//TraceEvent("SRL_Reset");
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, SerializabilityWorkload* self) {
		state double startTime = now();

		loop {
			state std::vector<ReadYourWritesTransaction> tr;
			state std::vector<std::vector<Future<Optional<Value>>>> getFutures;
			state std::vector<std::vector<Future<Key>>> getKeyFutures;
			state std::vector<std::vector<Future<RangeResult>>> getRangeFutures;
			state std::vector<std::vector<Future<Void>>> watchFutures;

			for (int i = 0; i < 5; i++) {
				tr.push_back(ReadYourWritesTransaction(cx));
				getFutures.push_back(std::vector<Future<Optional<Value>>>());
				getKeyFutures.push_back(std::vector<Future<Key>>());
				getRangeFutures.push_back(std::vector<Future<RangeResult>>());
				watchFutures.push_back(std::vector<Future<Void>>());
			}

			try {
				if (now() - startTime > self->testDuration)
					return Void();

				// Generate initial data
				state Standalone<VectorRef<KeyValueRef>> initialData;
				int initialAmount = deterministicRandom()->randomInt(0, 100);
				for (int i = 0; i < initialAmount; i++) {
					Key key = self->getRandomKey();
					Value value = self->getRandomValue();
					initialData.push_back_deep(initialData.arena(), KeyValueRef(key, value));
					//TraceEvent("SRL_Init").detail("Key", printable(key)).detail("Value", printable(value));
				}

				// Generate three random transactions
				state std::vector<TransactionOperation> a = self->randomTransaction();
				state std::vector<TransactionOperation> b = self->randomTransaction();
				state std::vector<TransactionOperation> c = self->randomTransaction();

				// reset database to known state
				wait(resetDatabase(cx, initialData));

				wait(runTransaction(
				    &tr[0], a, &getFutures[0], &getKeyFutures[0], &getRangeFutures[0], &watchFutures[0], true));
				wait(tr[0].commit());

				//TraceEvent("SRL_FinishedA");

				wait(runTransaction(
				    &tr[1], b, &getFutures[0], &getKeyFutures[0], &getRangeFutures[0], &watchFutures[0], true));
				wait(tr[1].commit());

				//TraceEvent("SRL_FinishedB");

				wait(runTransaction(
				    &tr[2], c, &getFutures[2], &getKeyFutures[2], &getRangeFutures[2], &watchFutures[2], false));
				wait(tr[2].commit());

				// get contents of database
				state RangeResult result1 = wait(getDatabaseContents(cx, self->nodes));

				// reset database to known state
				wait(resetDatabase(cx, initialData));

				wait(runTransaction(
				    &tr[3], a, &getFutures[3], &getKeyFutures[3], &getRangeFutures[3], &watchFutures[3], true));
				wait(runTransaction(
				    &tr[3], b, &getFutures[3], &getKeyFutures[3], &getRangeFutures[3], &watchFutures[3], true));
				wait(runTransaction(
				    &tr[4], c, &getFutures[4], &getKeyFutures[4], &getRangeFutures[4], &watchFutures[4], false));
				wait(tr[3].commit());
				wait(tr[4].commit());

				// get contents of database
				RangeResult result2 = wait(getDatabaseContents(cx, self->nodes));

				if (result1.size() != result2.size()) {
					TraceEvent(SevError, "SRL_ResultMismatch")
					    .detail("Size1", result1.size())
					    .detail("Size2", result2.size());

					for (auto kv : result1)
						TraceEvent("SRL_Result1").detail("Kv", printable(kv));
					for (auto kv : result2)
						TraceEvent("SRL_Result2").detail("Kv", printable(kv));

					ASSERT(false);
				}

				for (int i = 0; i < result1.size(); i++) {
					if (result1[i] != result2[i]) {
						TraceEvent(SevError, "SRL_ResultMismatch")
						    .detail("I", i)
						    .detail("Result1", printable(result1[i]))
						    .detail("Result2", printable(result2[i]))
						    .detail("Result1Value", printable(result1[i].value))
						    .detail("Result2Value", printable(result2[i].value));

						for (auto kv : result1)
							TraceEvent("SRL_Result1").detail("Kv", printable(kv));
						for (auto kv : result2)
							TraceEvent("SRL_Result2").detail("Kv", printable(kv));

						ASSERT(false);
					}
				}

				for (int i = 0; i < getFutures[0].size(); i++) {
					ASSERT(getFutures[0][i].get() == getFutures[3][i].get());
				}
				for (int i = 0; i < getFutures[1].size(); i++) {
					ASSERT(getFutures[1][i].get() == getFutures[3][getFutures[0].size() + i].get());
				}
				for (int i = 0; i < getFutures[2].size(); i++) {
					ASSERT(getFutures[2][i].get() == getFutures[4][i].get());
				}

				for (int i = 0; i < getKeyFutures[0].size(); i++) {
					ASSERT(getKeyFutures[0][i].get() == getKeyFutures[3][i].get());
				}
				for (int i = 0; i < getKeyFutures[1].size(); i++) {
					ASSERT(getKeyFutures[1][i].get() == getKeyFutures[3][getKeyFutures[0].size() + i].get());
				}
				for (int i = 0; i < getKeyFutures[2].size(); i++) {
					ASSERT(getKeyFutures[2][i].get() == getKeyFutures[4][i].get());
				}

				for (int i = 0; i < getRangeFutures[0].size(); i++) {
					if (getRangeFutures[0][i].get().size() != getRangeFutures[3][i].get().size()) {
						TraceEvent(SevError, "SRL_ResultMismatch")
						    .detail("Size1", getRangeFutures[0][i].get().size())
						    .detail("Size2", getRangeFutures[3][i].get().size());

						for (auto kv : getRangeFutures[0][i].get())
							TraceEvent("SRL_Result1").detail("Kv", printable(kv));
						for (auto kv : getRangeFutures[3][i].get())
							TraceEvent("SRL_Result2").detail("Kv", printable(kv));

						ASSERT(false);
					}

					for (int j = 0; j < getRangeFutures[0][i].get().size(); j++) {
						if (getRangeFutures[0][i].get()[j] != getRangeFutures[3][i].get()[j]) {
							TraceEvent(SevError, "SRL_ResultMismatch")
							    .detail("J", j)
							    .detail("Result1", printable(getRangeFutures[0][i].get()[j]))
							    .detail("Result2", printable(getRangeFutures[3][i].get()[j]))
							    .detail("Result1Value", printable(getRangeFutures[0][i].get()[j].value))
							    .detail("Result2Value", printable(getRangeFutures[3][i].get()[j].value));

							for (auto kv : getRangeFutures[0][i].get())
								TraceEvent("SRL_Result1").detail("Kv", printable(kv));
							for (auto kv : getRangeFutures[3][i].get())
								TraceEvent("SRL_Result2").detail("Kv", printable(kv));

							ASSERT(false);
						}
					}

					ASSERT(getRangeFutures[0][i].get() == getRangeFutures[3][i].get());
				}

				for (int i = 0; i < getRangeFutures[1].size(); i++) {
					ASSERT(getRangeFutures[1][i].get() == getRangeFutures[3][getRangeFutures[0].size() + i].get());
				}

				for (int i = 0; i < getRangeFutures[2].size(); i++) {
					if (getRangeFutures[2][i].get().size() != getRangeFutures[4][i].get().size()) {
						TraceEvent(SevError, "SRL_ResultMismatch")
						    .detail("Size1", getRangeFutures[2][i].get().size())
						    .detail("Size2", getRangeFutures[4][i].get().size());

						for (auto kv : getRangeFutures[2][i].get())
							TraceEvent("SRL_Result1").detail("Kv", printable(kv));
						for (auto kv : getRangeFutures[4][i].get())
							TraceEvent("SRL_Result2").detail("Kv", printable(kv));

						ASSERT(false);
					}

					for (int j = 0; j < getRangeFutures[2][i].get().size(); j++) {
						if (getRangeFutures[2][i].get()[j] != getRangeFutures[4][i].get()[j]) {
							TraceEvent(SevError, "SRL_ResultMismatch")
							    .detail("J", j)
							    .detail("Result1", printable(getRangeFutures[2][i].get()[j]))
							    .detail("Result2", printable(getRangeFutures[4][i].get()[j]))
							    .detail("Result1Value", printable(getRangeFutures[2][i].get()[j].value))
							    .detail("Result2Value", printable(getRangeFutures[4][i].get()[j].value));

							for (auto kv : getRangeFutures[2][i].get())
								TraceEvent("SRL_Result1").detail("Kv", printable(kv));
							for (auto kv : getRangeFutures[4][i].get())
								TraceEvent("SRL_Result2").detail("Kv", printable(kv));

							ASSERT(false);
						}
					}

					ASSERT(getRangeFutures[2][i].get() == getRangeFutures[4][i].get());
				}
			} catch (Error& e) {
				state ReadYourWritesTransaction trErr(cx);
				wait(trErr.onError(e));
			}
		}
	}
};

WorkloadFactory<SerializabilityWorkload> SerializabilityWorkloadFactory;
