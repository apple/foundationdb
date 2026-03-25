/*
 * RandomSelector.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/workloads.actor.h"

struct RandomSelectorWorkload : TestWorkload {
	static constexpr auto NAME = "RandomSelector";

	int minOperationsPerTransaction, maxOperationsPerTransaction, maxKeySpace, maxOffset, minInitialAmount,
	    maxInitialAmount;
	double testDuration;
	bool fail;

	std::vector<Future<Void>> clients;
	PerfIntCounter transactions, retries;

	RandomSelectorWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), transactions("Transactions"), retries("Retries") {

		minOperationsPerTransaction = getOption(options, "minOperationsPerTransaction"_sr, 10);
		maxOperationsPerTransaction = getOption(options, "minOperationsPerTransaction"_sr, 50);
		maxKeySpace = getOption(options, "maxKeySpace"_sr, 20);
		maxOffset = getOption(options, "maxOffset"_sr, 5);
		minInitialAmount = getOption(options, "minInitialAmount"_sr, 5);
		maxInitialAmount = getOption(options, "maxInitialAmount"_sr, 10);
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		fail = false;
	}

	Future<Void> setup(Database const& cx) override { return randomSelectorSetup(cx->clone(), this); }

	Future<Void> start(Database const& cx) override {
		clients.push_back(timeout(randomSelectorClient(cx->clone(), this), testDuration, Void()));
		return delay(testDuration);
	}

	Future<bool> check(Database const& cx) override {
		clients.clear();
		return !fail;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		m.push_back(transactions.getMetric());
		m.push_back(retries.getMetric());
	}

	Future<Void> randomSelectorSetup(Database cx, RandomSelectorWorkload* self) {
		Value myValue = StringRef(format("%d", deterministicRandom()->randomInt(0, 10000000)));
		Transaction tr(cx);
		std::string clientID;

		clientID = format("%08d", self->clientId);
		while (true) {
			Error err;
			try {
				for (int i = 0; i < self->maxOffset; i++) {
					tr.set(StringRef(clientID + "a/" + format("%010d", i)), myValue);
					tr.set(StringRef(clientID + "c/" + format("%010d", i)), myValue);
					tr.set(StringRef(clientID + "e/" + format("%010d", i)), myValue);
				}
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
			tr.reset();
		}
	}

	Future<Void> randomSelectorClient(Database cx, RandomSelectorWorkload* self) {
		int i{ 0 };
		int j{ 0 };
		std::string clientID;
		std::string myKeyA;
		std::string myKeyB;
		std::string myValue;
		std::string myRandomIDKey;
		bool onEqualA{ false };
		bool onEqualB{ false };
		int offsetA{ 0 };
		int offsetB{ 0 };
		int randomLimit{ 0 };
		int randomByteLimit{ 0 };
		Reverse reverse = Reverse::False;
		Error error;

		clientID = format("%08d", self->clientId);

		while (true) {
			Transaction tr(cx);

			while (true) {
				Error err;
				try {
					tr.clear(KeyRangeRef(StringRef(clientID + "b/"), StringRef(clientID + "c/")));
					tr.clear(KeyRangeRef(StringRef(clientID + "d/"), StringRef(clientID + "e/")));
					for (i = 0;
					     i < deterministicRandom()->randomInt(self->minInitialAmount, self->maxInitialAmount + 1);
					     i++) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						tr.set(StringRef(clientID + "b/" + myKeyA), myValue);
						tr.set(StringRef(clientID + "d/" + myKeyA), myValue);
						//TraceEvent("RYOWInit").detail("Key",myKeyA).detail("Value",myValue);
					}
					co_await tr.commit();
					tr.reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}

			ReadYourWritesTransaction trRYOW(cx);

			Error err;
			try {
				for (i = 0; i < deterministicRandom()->randomInt(self->minOperationsPerTransaction,
				                                                 self->maxOperationsPerTransaction + 1);
				     i++) {
					j = deterministicRandom()->randomInt(0, 16);
					if (j < 3) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));

						//TraceEvent("RYOWset").detail("Key",myKeyA).detail("Value",myValue);
						trRYOW.set(StringRef(clientID + "b/" + myKeyA), myValue);

						while (true) {
							Error err;
							try {
								tr.set(StringRef(clientID + "d/" + myKeyA), myValue);
								co_await tr.commit();
								tr.reset();
								break;
							} catch (Error& e) {
								err = e;
							}
							co_await tr.onError(err);
						}
					} else if (j < 4) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						//TraceEvent("RYOWclear").detail("Key",myKeyA);
						trRYOW.clear(StringRef(clientID + "b/" + myKeyA));

						while (true) {
							Error err;
							try {
								tr.clear(StringRef(clientID + "d/" + myKeyA));
								co_await tr.commit();
								tr.reset();
								break;
							} catch (Error& e) {
								err = e;
							}
							co_await tr.onError(err);
						}

					} else if (j < 5) {
						int a = deterministicRandom()->randomInt(1, self->maxKeySpace + 1);
						int b = deterministicRandom()->randomInt(1, self->maxKeySpace + 1);
						myKeyA = format("%010d", std::min(a, b) - 1);
						myKeyB = format("%010d", std::max(a, b));

						//TraceEvent("RYOWclearRange").detail("KeyA",myKeyA).detail("KeyB",myKeyB);
						trRYOW.clear(
						    KeyRangeRef(StringRef(clientID + "b/" + myKeyA), StringRef(clientID + "b/" + myKeyB)));

						while (true) {
							Error err;
							try {
								tr.clear(KeyRangeRef(StringRef(clientID + "d/" + myKeyA),
								                     StringRef(clientID + "d/" + myKeyB)));
								co_await tr.commit();
								tr.reset();
								break;
							} catch (Error& e) {
								err = e;
							}
							co_await tr.onError(err);
						}
					} else if (j < 6) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));

						Optional<Value> getTest1;

						Optional<Value> getTest = co_await trRYOW.get(StringRef(clientID + "b/" + myKeyA));
						getTest1 = getTest;

						while (true) {
							{
								Error err;
								try {
									Optional<Value> getTest2 = co_await tr.get(StringRef(clientID + "d/" + myKeyA));

									if ((getTest1.present() &&
									     (!getTest2.present() || getTest1.get() != getTest2.get())) ||
									    (!getTest1.present() && getTest2.present())) {
										TraceEvent(SevError, "RanSelTestFailure")
										    .detail("Reason", "The get results did not match")
										    .detail("KeyA", myKeyA)
										    .detail("RYOW",
										            getTest1.present() ? printable(getTest1.get()) : "Not Present")
										    .detail("Regular",
										            getTest2.present() ? printable(getTest2.get()) : "Not Present");
										self->fail = true;
									}

									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								co_await tr.onError(err);
								tr.reset();
							}
						}
					} else if (j < 7) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWadd").detail("Key",myKeyA).detail("Value", "\\x01");
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::AddValue);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::AddValue);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 8) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWappendIfFits").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::AppendIfFits);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(
									    StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::AppendIfFits);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 9) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWand").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::And);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::And);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 10) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWor").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::Or);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::Or);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 11) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWxor").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::Xor);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::Xor);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 12) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWmax").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::Max);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::Max);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 13) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWmin").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::Min);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::Min);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 14) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWbytemin").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::ByteMin);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::ByteMin);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else if (j < 15) {
						myKeyA = format("%010d", deterministicRandom()->randomInt(0, self->maxKeySpace + 1));
						myRandomIDKey = format("%010d", deterministicRandom()->randomInt(0, 1000000000));
						myValue = format("%d", deterministicRandom()->randomInt(0, 10000000));
						//TraceEvent("RYOWbytemax").detail("Key",myKeyA).detail("Value", myValue);
						trRYOW.atomicOp(StringRef(clientID + "b/" + myKeyA), myValue, MutationRef::ByteMax);

						while (true) {
							{
								Error err;
								try {
									tr.set(StringRef(clientID + "z/" + myRandomIDKey), StringRef());
									tr.atomicOp(StringRef(clientID + "d/" + myKeyA), myValue, MutationRef::ByteMax);
									co_await tr.commit();
									tr.reset();
									break;
								} catch (Error& e) {
									err = e;
								}
								error = err;
								co_await tr.onError(err);
								if (error.code() == error_code_commit_unknown_result) {
									Optional<Value> thing = co_await tr.get(StringRef(clientID + "z/" + myRandomIDKey));
									if (thing.present())
										break;
								}
							}
						}
					} else {
						int a = deterministicRandom()->randomInt(1, self->maxKeySpace + 1);
						int b = deterministicRandom()->randomInt(1, self->maxKeySpace + 1);
						myKeyA = format("%010d", std::min(a, b) - 1);
						myKeyB = format("%010d", std::max(a, b));
						onEqualA = deterministicRandom()->randomInt(0, 2) != 0;
						onEqualB = deterministicRandom()->randomInt(0, 2) != 0;
						offsetA = deterministicRandom()->randomInt(-1 * self->maxOffset / 2, self->maxOffset / 2);
						offsetB = deterministicRandom()->randomInt(-1 * self->maxOffset / 2, self->maxOffset / 2);
						randomLimit = deterministicRandom()->randomInt(0, 2 * self->maxOffset + self->maxKeySpace);
						randomByteLimit =
						    deterministicRandom()->randomInt(0, (self->maxOffset + self->maxKeySpace) * 512);
						reverse.set(deterministicRandom()->coinflip());

						//TraceEvent("RYOWgetRange").detail("KeyA", myKeyA).detail("KeyB", myKeyB).detail("OnEqualA",onEqualA).detail("OnEqualB",onEqualB).detail("OffsetA",offsetA).detail("OffsetB",offsetB).detail("RandomLimit",randomLimit).detail("RandomByteLimit", randomByteLimit).detail("Reverse", reverse);

						RangeResult getRangeTest1;
						RangeResult getRangeTest = co_await trRYOW.getRange(
						    KeySelectorRef(StringRef(clientID + "b/" + myKeyA), onEqualA, offsetA),
						    KeySelectorRef(StringRef(clientID + "b/" + myKeyB), onEqualB, offsetB),
						    randomLimit,
						    Snapshot::False,
						    reverse);
						getRangeTest1 = getRangeTest;

						while (true) {
							Error err;
							try {
								RangeResult getRangeTest2 = co_await tr.getRange(
								    KeySelectorRef(StringRef(clientID + "d/" + myKeyA), onEqualA, offsetA),
								    KeySelectorRef(StringRef(clientID + "d/" + myKeyB), onEqualB, offsetB),
								    randomLimit,
								    Snapshot::False,
								    reverse);

								bool fail = false;
								if (getRangeTest1.size() != getRangeTest2.size()) {
									TraceEvent(SevError, "RanSelTestFailure")
									    .detail("Reason", "The getRange results did not match sizes")
									    .detail("Size1", getRangeTest1.size())
									    .detail("Size2", getRangeTest2.size())
									    .detail("Limit", randomLimit)
									    .detail("ByteLimit", randomByteLimit)
									    .detail("Bytes1", getRangeTest1.expectedSize())
									    .detail("Bytes2", getRangeTest2.expectedSize())
									    .detail("Reverse", reverse);
									fail = true;
									self->fail = true;
								}
								for (int k = 0; k < std::min(getRangeTest1.size(), getRangeTest2.size()); k++) {
									if (getRangeTest1[k].value != getRangeTest2[k].value) {
										std::string keyA = printable(getRangeTest1[k].key);
										std::string valueA = printable(getRangeTest1[k].value);
										std::string keyB = printable(getRangeTest2[k].key);
										std::string valueB = printable(getRangeTest2[k].value);
										TraceEvent(SevError, "RanSelTestFailure")
										    .detail("Reason", "The getRange results did not match contents")
										    .detail("KeyA", keyA)
										    .detail("ValueA", valueA)
										    .detail("KeyB", keyB)
										    .detail("ValueB", valueB)
										    .detail("Reverse", reverse);
										fail = true;
										self->fail = true;
									}
								}
								if (fail) {
									std::string outStr1 = "";
									for (int k = 0; k < getRangeTest1.size(); k++) {
										outStr1 = outStr1 + printable(getRangeTest1[k].key) + " " +
										          format("%d", getRangeTest1[k].value.size()) + " ";
									}

									std::string outStr2 = "";
									for (int k = 0; k < getRangeTest2.size(); k++) {
										outStr2 = outStr2 + printable(getRangeTest2[k].key) + " " +
										          format("%d", getRangeTest2[k].value.size()) + " ";
									}

									TraceEvent("RanSelTestLog").detail("RYOW", outStr1).detail("Normal", outStr2);
								}

								tr.reset();
								break;
							} catch (Error& e) {
								err = e;
							}
							co_await tr.onError(err);
						}
					}
				}

				co_await trRYOW.commit();

				++self->transactions;

				Transaction finalTransaction(cx);

				while (true) {
					Error err;
					try {
						RangeResult finalTest1 = co_await finalTransaction.getRange(
						    KeyRangeRef(StringRef(clientID + "b/"), StringRef(clientID + "c/")), self->maxKeySpace);
						RangeResult finalTest2 = co_await finalTransaction.getRange(
						    KeyRangeRef(StringRef(clientID + "d/"), StringRef(clientID + "e/")), self->maxKeySpace);

						if (finalTest1.size() != finalTest2.size()) {
							TraceEvent(SevError, "RanSelTestFailure")
							    .detail("Reason", "The final results did not match sizes");
							self->fail = true;
						}
						for (int k = 0; k < finalTest1.size(); k++)
							if (finalTest1[k].value != finalTest2[k].value) {
								TraceEvent(SevError, "RanSelTestFailure")
								    .detail("Reason", "The final results did not match contents")
								    .detail("KeyA", printable(finalTest1[k].key))
								    .detail("ValueA", printable(finalTest1[k].value))
								    .detail("KeyB", printable(finalTest2[k].key))
								    .detail("ValueB", printable(finalTest2[k].value))
								    .detail("Reverse", reverse);
								self->fail = true;
							}
						break;
					} catch (Error& e) {
						err = e;
					}
					co_await finalTransaction.onError(err);
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await trRYOW.onError(err);
			}
			++self->retries;
		}
	}
};

WorkloadFactory<RandomSelectorWorkload> RandomSelectorWorkloadFactory;
