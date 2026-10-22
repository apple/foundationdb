/*
 * SubmitBackup.cpp
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

#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "BackupRangeSpec.h"

struct SubmitBackupWorkload : TestWorkload {
	static constexpr auto NAME = "SubmitBackup";

	FileBackupAgent backupAgent;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	int initSnapshotInterval;
	int snapshotInterval;
	StopWhenDone stopWhenDone{ false };
	IncrementalBackupOnly incremental{ false };
	Optional<std::string> encryptionKeyFileName;
	// Whole keyspace (addDefaultBackupRanges) when unset.
	Optional<Standalone<VectorRef<KeyRangeRef>>> explicitBackupRanges;

	explicit SubmitBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		initSnapshotInterval = getOption(options, "initSnapshotInterval"_sr, 0);
		snapshotInterval = getOption(options, "snapshotInterval"_sr, 1e8);
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, true));
		incremental.set(getOption(options, "incremental"_sr, false));
		explicitBackupRanges = BackupRangeSpec::readOptionalRanges(options, "backupRanges"_sr, "backupRangePrefix"_sr);

		if (getOption(options, "encrypted"_sr, deterministicRandom()->random01() < 0.5)) {
			encryptionKeyFileName = "simfdb/" + getTestEncryptionFileName();
		}
	}

	Future<Void> _start(Database cx) {
		co_await delay(delayFor);
		Standalone<VectorRef<KeyRangeRef>> backupRanges;
		if (explicitBackupRanges.present()) {
			backupRanges = explicitBackupRanges.get();
		} else {
			addDefaultBackupRanges(backupRanges);
		}

		if (encryptionKeyFileName.present()) {
			co_await BackupContainerFileSystem::createTestEncryptionKeyFile(encryptionKeyFileName.get());
		}

		try {
			co_await backupAgent.submitBackup(cx,
			                                  backupDir,
			                                  {},
			                                  initSnapshotInterval,
			                                  snapshotInterval,
			                                  tag.toString(),
			                                  backupRanges,
			                                  stopWhenDone,
			                                  MutationLogType::DEFAULT,
			                                  incremental,
			                                  encryptionKeyFileName,
			                                  encryptionKeyFileName.present() ? DEFAULT_ENCRYPTION_BLOCK_SIZE : 0,
			                                  0);
		} catch (Error& e) {
			TraceEvent("BackupSubmitError").error(e);
			if (e.code() != error_code_backup_duplicate) {
				throw;
			}
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<SubmitBackupWorkload> SubmitBackupWorkloadFactory;

TEST_CASE("/fdbserver/workloads/BackupRangeSpec/parse") {
	auto ranges = BackupRangeSpec::parseRanges("test"_sr, "\\x02perf\\x00:\\x02perf\\x01;d/:e;:\\x01a\\x3bb");
	ASSERT_EQ(ranges.size(), 3);
	ASSERT(ranges[0] == KeyRangeRef("\x02perf\x00"_sr, "\x02perf\x01"_sr));
	ASSERT(ranges[1] == KeyRangeRef("d/"_sr, "e"_sr));
	ASSERT(ranges[2] == KeyRangeRef(""_sr,
	                                "\x01"
	                                "a;b"_sr));

	ASSERT(BackupRangeSpec::parsePrefixRange("test"_sr, "d\\\\") == prefixRange("d\\"_sr));

	for (const char* bad : { "a",
	                         "a:b:c",
	                         "b:a",
	                         "a:a",
	                         "a:b;",
	                         "a:b;a1:c",
	                         "a:\\xff\\xff\\x00",
	                         "a\\x:b",
	                         "a\\q:b",
	                         "a:\\xff\\x00",
	                         "\\xff:\\xff\\x00",
	                         "a:b;\\xff\\x02/x:\\xff\\x02/y" }) {
		bool threw = false;
		try {
			BackupRangeSpec::parseRanges("test"_sr, bad);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_test_specification_invalid);
			threw = true;
		}
		ASSERT(threw);
	}

	for (const char* badPrefix : { "\\xff", "\\xff\\x02/x" }) {
		bool threw = false;
		try {
			BackupRangeSpec::parsePrefixRange("test"_sr, badPrefix);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_test_specification_invalid);
			threw = true;
		}
		ASSERT(threw);
	}

	Standalone<VectorRef<KeyValueRef>> options;
	options.push_back_deep(options.arena(), KeyValueRef("p"_sr, "d/"_sr));
	auto fromPrefix = BackupRangeSpec::readOptionalRanges(options, "r"_sr, "p"_sr);
	ASSERT(fromPrefix.present() && fromPrefix.get().size() == 1 && fromPrefix.get()[0] == prefixRange("d/"_sr));
	ASSERT(!BackupRangeSpec::readOptionalRanges(options, "r"_sr, "p"_sr).present());

	return Void();
}
