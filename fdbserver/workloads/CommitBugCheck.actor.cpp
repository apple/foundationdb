/*
 * CommitBugCheck.actor.cpp
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

#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Regression tests for 2 commit related bugs
struct CommitBugWorkload : TestWorkload {
	static constexpr auto NAME = "CommitBug";
	bool success;

	CommitBugWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) { success = true; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return timeout(bug1(cx, this) && bug2(cx, this), 60, Void()); }

	ACTOR Future<Void> bug1(Database cx, CommitBugWorkload* self) {
		state Key key = StringRef(format("B1Key%d", self->clientId));
		state Value val1 = "Value1"_sr;
		state Value val2 = "Value2"_sr;

		loop {
			state Transaction tr(cx);
			loop {
				try {
					tr.set(key, val1);
					wait(tr.commit());
					tr.reset();
					break;
				} catch (Error& e) {
					TraceEvent("CommitBugSetVal1Error").error(e);
					CODE_PROBE(e.code() == error_code_commit_unknown_result, "Commit unknown result");
					wait(tr.onError(e));
				}
			}

			loop {
				try {
					tr.set(key, val2);
					wait(tr.commit());
					tr.reset();
					break;
				} catch (Error& e) {
					TraceEvent("CommitBugSetVal2Error").error(e);
					wait(tr.onError(e));
				}
			}

			loop {
				try {
					Optional<Value> v = wait(tr.get(key));
					if (!v.present() || v.get() != val2) {
						TraceEvent(SevError, "CommitBugFailed")
						    .detail("Value", v.present() ? printable(v.get()) : "Not present");
						self->success = false;
						return Void();
					}

					break;
				} catch (Error& e) {
					TraceEvent("CommitBugGetValError").error(e);
					wait(tr.onError(e));
				}
			}

			loop {
				try {
					tr.clear(key);
					wait(tr.commit());
					tr.reset();
					break;
				} catch (Error& e) {
					TraceEvent("CommitBugClearValError").error(e);
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR Future<Void> bug2(Database cx, CommitBugWorkload* self) {
		state Key key = StringRef(format("B2Key%d", self->clientId));

		state int i;
		for (i = 0; i < 1000; ++i) {
			state Transaction tr(cx);

			loop {
				try {
					Optional<Value> val = wait(tr.get(key));
					state int num = 0;
					if (val.present()) {
						num = atoi(val.get().toString().c_str());
						if (num != i) {
							TraceEvent(SevError, "CommitBug2Failed").detail("Value", num).detail("Expected", i);
							self->success = false;
							return Void();
						}
					}

					TraceEvent("CommitBug2SetKey").detail("Num", i + 1);
					tr.set(key, StringRef(format("%d", i + 1)));
					wait(tr.commit());
					TraceEvent("CommitBug2SetCompleted").detail("Num", i + 1);
					break;
				} catch (Error& error) {
					state Error e = error;
					if (e.code() != error_code_not_committed && e.code() != error_code_transaction_too_old) {
						tr.reset();
						loop {
							try {
								TraceEvent("CommitBug2SetKey").detail("Num", i + 1);
								tr.set(key, StringRef(format("%d", i + 1)));
								TraceEvent("CommitBug2SetCompleted").detail("Num", i + 1);
								wait(tr.commit());
								break;
							} catch (Error& err) {
								wait(tr.onError(err));
							}
						}

						break;
					} else {
						CODE_PROBE(true, "Commit conflict");

						TraceEvent("CommitBug2Error").error(e).detail("AttemptedNum", i + 1);
						wait(tr.onError(e));
					}
				}
			}
		}

		return Void();
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<CommitBugWorkload> CommitBugWorkloadFactory;
