/*
 * RestoreMultiRanges.actor.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.actor.h"
#include "fdbclient/BackupContainer.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct RestoreMultiRangesWorkload : TestWorkload {

	FileBackupAgent backupAgent;
	Reference<IBackupContainer> backupContainer;

	RestoreMultiRangesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	static constexpr const char* NAME = "RestoreMultiRanges";

	ACTOR static Future<Void> clearDatabase(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.clear(normalKeys);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> prepareDatabase(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.reset();
				tr.set("a"_sr, "a"_sr);
				tr.set("aaaa"_sr, "aaaa"_sr);
				tr.set("b"_sr, "b"_sr);
				tr.set("bb"_sr, "bb"_sr);
				tr.set("bbb"_sr, "bbb"_sr);
				wait(tr.commit());
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	static void logTestData(const VectorRef<KeyValueRef>& data) {
		TraceEvent("TestFailureDetail").log();
		int index = 0;
		for (auto& entry : data) {
			TraceEvent("CurrentDataEntry")
			    .detail("Index", index)
			    .detail("Key", entry.key.toString())
			    .detail("Value", entry.value.toString());
			index++;
		}
	}

	ACTOR static Future<bool> verifyDatabase(Database cx) {
		state UID randomID = nondeterministicRandom()->randomUniqueID();
		TraceEvent("RestoreMultiRanges_Verify").detail("UID", randomID);
		state Transaction tr(cx);
		state KeyRangeRef range("a"_sr, "z"_sr);
		loop {
			try {
				tr.reset();
				tr.debugTransaction(randomID);
				RangeResult kvs = wait(tr.getRange(range, 10));
				if (kvs.size() != 4) {
					logTestData(kvs);
					TraceEvent(SevError, "TestFailureInfo")
					    .detail("DataSize", kvs.size())
					    .detail("Expect", 4)
					    .detail("Workload", NAME);
					return false;
				}
				KeyRef keys[4] = { "a"_sr, "aaaa"_sr, "bb"_sr, "bbb"_sr };
				for (size_t i = 0; i < 4; ++i) {
					if (kvs[i].key != keys[i]) {
						TraceEvent(SevError, "TestFailureInfo")
						    .detail("ExpectKey", keys[i])
						    .detail("Got", kvs[i].key)
						    .detail("Index", i);
						return false;
					}
				}
				TraceEvent("RestoreMultiRanges_VerifyPassed");
				return true;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> _start(RestoreMultiRangesWorkload* self, Database cx) {
		TraceEvent("RestoreMultiRanges_StartBackup");
		wait(clearDatabase(cx));
		wait(prepareDatabase(cx));

		state std::string backupContainer = "file://simfdb/backups/";
		state std::string tagName = "default";
		state Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef("a"_sr, "z"_sr));
		TraceEvent("RestoreMultiRanges_SubmitBackup");
		try {
			wait(self->backupAgent.submitBackup(cx,
			                                    StringRef(backupContainer),
			                                    {},
			                                    deterministicRandom()->randomInt(0, 60),
			                                    deterministicRandom()->randomInt(0, 100),
			                                    tagName,
			                                    backupRanges,
			                                    true,
			                                    StopWhenDone::True));
		} catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("RestoreMultiRanges_WaitBackup");
		state Reference<IBackupContainer> container;
		wait(success(self->backupAgent.waitBackup(cx, tagName, StopWhenDone::True, &container)));

		TraceEvent("RestoreMultiRanges_ClearDatabase");
		wait(clearDatabase(cx));

		TraceEvent("RestoreMultiRanges_Restore");
		state Standalone<VectorRef<KeyRangeRef>> ranges;
		ranges.push_back_deep(ranges.arena(), KeyRangeRef("a"_sr, "aaaaa"_sr));
		ranges.push_back_deep(ranges.arena(), KeyRangeRef("bb"_sr, "bbbbb"_sr)); // Skip "b"
		wait(success(self->backupAgent.restore(cx,
		                                       cx,
		                                       Key(tagName),
		                                       Key(container->getURL()),
		                                       {},
		                                       ranges,
		                                       WaitForComplete::True,
		                                       ::invalidVersion,
		                                       Verbose::True)));
		TraceEvent("RestoreMultiRanges_Success");
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(this, cx); }
	Future<bool> check(Database const& cx) override { return verifyDatabase(cx); }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreMultiRangesWorkload> RestoreMultiRangesWorkloadFactory;
