/*
 * RestoreMultiRanges.cpp
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

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"

struct RestoreMultiRangesWorkload : TestWorkload {

	FileBackupAgent backupAgent;
	Reference<IBackupContainer> backupContainer;
	Optional<std::string> encryptionKeyFileName;

	RestoreMultiRangesWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}
	}

	static constexpr const char* NAME = "RestoreMultiRanges";

	static Future<Void> clearDatabase(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.clear(normalKeys);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> prepareDatabase(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.reset();
				tr.set("a"_sr, "a"_sr);
				tr.set("aaaa"_sr, "aaaa"_sr);
				tr.set("b"_sr, "b"_sr);
				tr.set("bb"_sr, "bb"_sr);
				tr.set("bbb"_sr, "bbb"_sr);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
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

	static Future<bool> verifyDatabase(Database cx) {
		UID randomID = nondeterministicRandom()->randomUniqueID();
		TraceEvent("RestoreMultiRanges_Verify").detail("UID", randomID);
		Transaction tr(cx);
		KeyRangeRef range("a"_sr, "z"_sr);
		while (true) {
			Error err;
			try {
				tr.reset();
				tr.debugTransaction(randomID);
				RangeResult kvs = co_await tr.getRange(range, 10);
				if (kvs.size() != 4) {
					logTestData(kvs);
					TraceEvent(SevError, "TestFailureInfo")
					    .detail("DataSize", kvs.size())
					    .detail("Expect", 4)
					    .detail("Workload", NAME);
					co_return false;
				}
				KeyRef keys[4] = { "a"_sr, "aaaa"_sr, "bb"_sr, "bbb"_sr };
				for (size_t i = 0; i < 4; ++i) {
					if (kvs[i].key != keys[i]) {
						TraceEvent(SevError, "TestFailureInfo")
						    .detail("ExpectKey", keys[i])
						    .detail("Got", kvs[i].key)
						    .detail("Index", i);
						co_return false;
					}
				}
				TraceEvent("RestoreMultiRanges_VerifyPassed");
				co_return true;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> _start(Database cx) {
		TraceEvent("RestoreMultiRanges_StartBackup");
		co_await clearDatabase(cx);
		co_await prepareDatabase(cx);

		if (encryptionKeyFileName.present()) {
			co_await BackupContainerFileSystem::createTestEncryptionKeyFile(encryptionKeyFileName.get());
		}

		std::string backupContainer = "file://simfdb/backups/";
		std::string tagName = "default";
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		backupRanges.push_back_deep(backupRanges.arena(), KeyRangeRef("a"_sr, "z"_sr));
		TraceEvent("RestoreMultiRanges_SubmitBackup");
		try {
			co_await backupAgent.submitBackup(cx,
			                                  StringRef(backupContainer),
			                                  {},
			                                  deterministicRandom()->randomInt(0, 60),
			                                  deterministicRandom()->randomInt(0, 100),
			                                  tagName,
			                                  backupRanges,
			                                  StopWhenDone::True,
			                                  UsePartitionedLog::False,
			                                  IncrementalBackupOnly::False,
			                                  encryptionKeyFileName,
			                                  encryptionKeyFileName.present() ? DEFAULT_ENCRYPTION_BLOCK_SIZE : 0,
			                                  0);
		} catch (Error& e) {
			if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
				throw;
		}

		TraceEvent("RestoreMultiRanges_WaitBackup");
		Reference<IBackupContainer> container;
		co_await backupAgent.waitBackup(cx, tagName, StopWhenDone::True, &container);

		TraceEvent("RestoreMultiRanges_ClearDatabase");
		co_await clearDatabase(cx);

		TraceEvent("RestoreMultiRanges_Restore");
		Standalone<VectorRef<KeyRangeRef>> ranges;
		ranges.push_back_deep(ranges.arena(), KeyRangeRef("a"_sr, "aaaaa"_sr));
		ranges.push_back_deep(ranges.arena(), KeyRangeRef("bb"_sr, "bbbbb"_sr)); // Skip "b"
		co_await backupAgent.restore(cx,
		                             cx,
		                             Key(tagName),
		                             Key(container->getURL()),
		                             {},
		                             ranges,
		                             WaitForComplete::True,
		                             ::invalidVersion,
		                             Verbose::True,
		                             Key(),
		                             Key(),
		                             LockDB::True,
		                             UnlockDB::True,
		                             OnlyApplyMutationLogs::False,
		                             InconsistentSnapshotOnly::False,
		                             ::invalidVersion,
		                             encryptionKeyFileName);
		TraceEvent("RestoreMultiRanges_Success");
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return verifyDatabase(cx); }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<RestoreMultiRangesWorkload> RestoreMultiRangesWorkloadFactory;
