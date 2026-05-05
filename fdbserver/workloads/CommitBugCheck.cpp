/*
 * CommitBugCheck.cpp
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

#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"

// Regression tests for 2 commit related bugs
struct CommitBugWorkload : TestWorkload {
	static constexpr auto NAME = "CommitBug";
	bool success;

	explicit CommitBugWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) { success = true; }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override { return timeout(bug1(cx, this) && bug2(cx, this), 60, Void()); }

	Future<Void> bug1(Database cx, CommitBugWorkload* self) {
		Key key = StringRef(format("B1Key%d", self->clientId));
		Value val1 = "Value1"_sr;
		Value val2 = "Value2"_sr;

		while (true) {
			Transaction tr(cx);
			while (true) {
				Error err;
				try {
					tr.set(key, val1);
					co_await tr.commit();
					tr.reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("CommitBugSetVal1Error").error(err);
				CODE_PROBE(err.code() == error_code_commit_unknown_result, "Commit unknown result");
				co_await tr.onError(err);
			}

			while (true) {
				Error err;
				try {
					tr.set(key, val2);
					co_await tr.commit();
					tr.reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("CommitBugSetVal2Error").error(err);
				co_await tr.onError(err);
			}

			while (true) {
				Error err;
				try {
					Optional<Value> v = co_await tr.get(key);
					if (!v.present() || v.get() != val2) {
						TraceEvent(SevError, "CommitBugFailed")
						    .detail("Value", v.present() ? printable(v.get()) : "Not present");
						self->success = false;
						co_return;
					}

					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("CommitBugGetValError").error(err);
				co_await tr.onError(err);
			}

			while (true) {
				Error err;
				try {
					tr.clear(key);
					co_await tr.commit();
					tr.reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				TraceEvent("CommitBugClearValError").error(err);
				co_await tr.onError(err);
			}
		}
	}

	Future<Void> bug2(Database cx, CommitBugWorkload* self) {
		Key key = StringRef(format("B2Key%d", self->clientId));

		for (int i = 0; i < 1000; ++i) {
			Transaction tr(cx);

			while (true) {
				Error caughtErr;
				try {
					Optional<Value> val = co_await tr.get(key);
					int num = 0;
					if (val.present()) {
						num = atoi(val.get().toString().c_str());
						if (num != i) {
							TraceEvent(SevError, "CommitBug2Failed").detail("Value", num).detail("Expected", i);
							self->success = false;
							co_return;
						}
					}

					TraceEvent("CommitBug2SetKey").detail("Num", i + 1);
					tr.set(key, StringRef(format("%d", i + 1)));
					co_await tr.commit();
					TraceEvent("CommitBug2SetCompleted").detail("Num", i + 1);
					break;
				} catch (Error& error) {
					caughtErr = error;
				}
				Error e = caughtErr;
				if (e.code() != error_code_not_committed && e.code() != error_code_transaction_too_old) {
					tr.reset();
					while (true) {
						{
							Error caughtErr;
							try {
								TraceEvent("CommitBug2SetKey").detail("Num", i + 1);
								tr.set(key, StringRef(format("%d", i + 1)));
								TraceEvent("CommitBug2SetCompleted").detail("Num", i + 1);
								co_await tr.commit();
								break;
							} catch (Error& err) {
								caughtErr = err;
							}
							co_await tr.onError(caughtErr);
						}
					}

					break;
				} else {
					CODE_PROBE(true, "Commit conflict");

					TraceEvent("CommitBug2Error").error(e).detail("AttemptedNum", i + 1);
					co_await tr.onError(e);
				}
			}
		}
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<CommitBugWorkload> CommitBugWorkloadFactory;
