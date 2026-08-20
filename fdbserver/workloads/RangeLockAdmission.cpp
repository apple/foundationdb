/*
 * RangeLockAdmission.cpp
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
#include "fdbclient/BackupFileFormat.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/RangeLock.h"
#include "fdbclient/RangeLockConfiguration.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

class RangeLockAdmission : public TestWorkload {
public:
	static constexpr auto NAME = "RangeLockAdmission";

	explicit RangeLockAdmission(WorkloadContext const& wcx)
	  : TestWorkload(wcx), expectAcquisitionEnabled(getOption(options, "expectAcquisitionEnabled"_sr, false)),
	    expectShardEncoding(getOption(options, "expectShardEncoding"_sr, true)) {
		ASSERT(expectAcquisitionEnabled != expectShardEncoding);
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("RandomRangeLock"); }
	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}
		ASSERT((co_await getRangeLockConfiguration(cx)).isReady());
		const RangeLockAdmissionStatus status = co_await getRangeLockAdmissionStatus(cx);
		ASSERT(status.allProxiesHaveValidState);
		ASSERT(status.allProxiesEnableAcquisition == expectAcquisitionEnabled);
		ASSERT(status.allProxiesEncodeShardLocations == expectShardEncoding);
		ASSERT(!status.dataDistributorEncodesShardLocations.present());
		const RangeLockAdmissionStatus bulkLoadStatus = co_await getRangeLockAdmissionStatus(cx, true);
		ASSERT(bulkLoadStatus.dataDistributorEncodesShardLocations == Optional<bool>(expectShardEncoding));
		ASSERT(co_await getBulkLoadMode(cx) == 0);
		ASSERT(!(co_await getRunningBulkLoadJobHandle(cx)).present());

		co_await registerRangeLockOwner(cx, ownerName, "Range-lock admission regression");
		co_await testGenericLock(cx);
		co_await testRejectedRawMetadata(cx);

		if (expectShardEncoding) {
			// Disabling new acquisitions must still permit dispatch of an existing fenced job.
			ASSERT(co_await setBulkLoadMode(cx, 1) == 0);
			ASSERT(co_await getBulkLoadMode(cx) == 1);
			co_await testRejectedBulkLoad(cx);
			ASSERT(co_await getBulkLoadMode(cx) == 1);
			ASSERT(co_await setBulkLoadMode(cx, 0) == 1);
		} else {
			const auto modeBefore = co_await readSystemRange(cx, singleKeyRange(bulkLoadModeKey));
			co_await expectError(setBulkLoadMode(cx, 1), error_code_bulkload_invalid_configuration);
			ASSERT(co_await readSystemRange(cx, singleKeyRange(bulkLoadModeKey)) == modeBefore);
			ASSERT(co_await getBulkLoadMode(cx) == 0);
			co_await testRejectedBulkLoad(cx);
			ASSERT(co_await setBulkLoadMode(cx, 0) == 0);
		}

		ASSERT(co_await getBulkLoadMode(cx) == 0);
		co_await testRejectedRestore(cx);
		co_await removeRangeLockOwner(cx, ownerName);
		TraceEvent("RangeLockAdmissionPassed")
		    .detail("AcquisitionEnabled", expectAcquisitionEnabled)
		    .detail("ShardEncoding", expectShardEncoding);
	}

	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& metrics) override {}

private:
	using SystemRows = std::vector<std::pair<Key, Value>>;
	static constexpr Version restoreSnapshotVersion = 100;
	const bool expectAcquisitionEnabled;
	const bool expectShardEncoding;
	const std::string ownerName = "RangeLockAdmission";
	const Key protectedKey = "rangeLockAdmission/generic"_sr;
	const Key bulkLoadKey = "rangeLockAdmission/bulkLoad"_sr;
	const Key restoreKey = "rangeLockAdmission/restore"_sr;

	template <class T>
	static Future<Void> expectError(Future<T> operation, int expectedCode) {
		ErrorOr<T> result = co_await errorOr(operation);
		if (result.isError() && result.getError().code() == error_code_actor_cancelled) {
			throw result.getError();
		}
		if (!result.isError() || result.getError().code() != expectedCode) {
			TraceEvent(SevError, "RangeLockAdmissionUnexpectedResult")
			    .detail("ExpectedError", expectedCode)
			    .detail("ActualError", result.isError() ? result.getError().code() : 0);
			ASSERT(false);
		}
	}

	static Future<SystemRows> readSystemRanges(Database cx, std::vector<KeyRange> ranges) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				SystemRows result;
				for (const auto& range : ranges) {
					Key begin = range.begin;
					while (begin < range.end) {
						RangeResult page = co_await tr.getRange(KeyRangeRef(begin, range.end), 1000);
						for (const auto& kv : page) {
							result.emplace_back(Key(kv.key), Value(kv.value));
						}
						if (!page.more) {
							break;
						}
						ASSERT(!page.empty());
						begin = keyAfter(page.back().key);
					}
				}
				co_return result;
			} catch (Error& e) {
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	static Future<SystemRows> readSystemRange(Database cx, KeyRange range) { return readSystemRanges(cx, { range }); }

	static Future<SystemRows> readRestoreSubmissionMetadata(Database cx) {
		// Restore config and the shared backup-agent task/future buckets must be unchanged on admission failure.
		return readSystemRanges(
		    cx,
		    { KeyRange(fileRestorePrefixRange), KeyRange(fileBackupPrefixRange), singleKeyRange(databaseLockedKey) });
	}

	static Future<Void> write(Database cx, Key key) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.set(key, "value"_sr);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_transaction_rejected_range_locked) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> testGenericLock(Database cx) {
		const KeyRange range = singleKeyRange(protectedKey);
		if (expectAcquisitionEnabled) {
			// Shard-location encoding is a BulkLoad prerequisite, not a generic range-lock prerequisite.
			co_await takeExclusiveReadLockOnRange(cx, range, ownerName);
			const auto locks = co_await findExclusiveReadLockOnRange(cx, range, ownerName);
			ASSERT_EQ(locks.size(), 1);
			co_await expectError(write(cx, protectedKey), error_code_transaction_rejected_range_locked);
			co_await releaseExclusiveReadLockOnRange(cx, range, ownerName);
		} else {
			const auto before = co_await readSystemRange(cx, rangeLockKeys);
			co_await expectError(takeExclusiveReadLockOnRange(cx, range, ownerName), error_code_range_lock_not_ready);
			const Optional<RangeLockOwner> owner = co_await getRangeLockOwner(cx, ownerName);
			ASSERT(owner.present());
			const RangeLockState fencedLock(
			    RangeLockType::ExclusiveReadLock, ownerName, range, deterministicRandom()->randomUniqueID().toString());
			co_await expectError(takeExclusiveReadLockOnRange(cx, fencedLock, owner.get().getGeneration()),
			                     error_code_range_lock_not_ready);
			ASSERT(co_await readSystemRange(cx, rangeLockKeys) == before);
			ASSERT((co_await findExclusiveReadLockOnRange(cx, range, ownerName)).empty());
		}
		co_await write(cx, protectedKey);
	}

	static Future<Void> mutateRawBoundary(Database cx, Key key, Value value, MutationRef::Type type) {
		Transaction tr(cx);
		while (true) {
			Error error;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				co_await tr.get(key);
				if (type == MutationRef::SetValue) {
					tr.set(key, value);
				} else {
					tr.atomicOp(key, value, type);
				}
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				if (e.code() == error_code_range_lock_not_ready) {
					throw;
				}
				error = e;
			}
			co_await tr.onError(error);
		}
	}

	Future<Void> testRejectedRawMetadata(Database cx) {
		const auto before = co_await readSystemRange(cx, rangeLockKeys);
		const Optional<RangeLockOwner> owner = co_await getRangeLockOwner(cx, ownerName);
		ASSERT(owner.present());
		const Key boundary = protectedKey.withPrefix(rangeLockPrefix);
		// A different valid object used to reach ObjectReader's file-ID ASSERT.
		co_await expectError(mutateRawBoundary(cx, boundary, rangeLockOwnerValue(owner.get()), MutationRef::SetValue),
		                     error_code_range_lock_not_ready);
		co_await expectError(mutateRawBoundary(cx, boundary, "\x01"_sr, MutationRef::AddValue),
		                     error_code_range_lock_not_ready);
		ASSERT(co_await readSystemRange(cx, rangeLockKeys) == before);
		ASSERT((co_await getRangeLockAdmissionStatus(cx)).allProxiesHaveValidState);
		co_await write(cx, protectedKey);
	}

	Future<Void> testRejectedBulkLoad(Database cx) {
		const BulkLoadJobState job(deterministicRandom()->randomUniqueID(),
		                           "range-lock-admission-unused-source",
		                           singleKeyRange(bulkLoadKey),
		                           BulkLoadTransportMethod::CP);
		const auto jobsBefore = co_await readSystemRange(cx, bulkLoadJobKeys);
		const auto tasksBefore = co_await readSystemRange(cx, bulkLoadTaskKeys);
		const auto fencesBefore = co_await readSystemRange(cx, bulkLoadJobRangeLockKeys);
		const auto locksBefore = co_await readSystemRange(cx, rangeLockKeys);
		co_await expectError(submitBulkLoadJobWithLock(cx, job), error_code_bulkload_invalid_configuration);
		co_await expectError(submitBulkLoadJob(cx, job), error_code_bulkload_invalid_configuration);
		ASSERT(!(co_await getRunningBulkLoadJobHandle(cx)).present());
		ASSERT(co_await readSystemRange(cx, bulkLoadJobKeys) == jobsBefore);
		ASSERT(co_await readSystemRange(cx, bulkLoadTaskKeys) == tasksBefore);
		ASSERT(co_await readSystemRange(cx, bulkLoadJobRangeLockKeys) == fencesBefore);
		ASSERT(co_await readSystemRange(cx, rangeLockKeys) == locksBefore);
		co_await write(cx, bulkLoadKey);
	}

	static Future<Reference<IBackupContainer>> createRestoreFixture(KeyRange range) {
		const std::string url =
		    "file://simfdb/range-lock-admission/" + deterministicRandom()->randomUniqueID().toString();
		Reference<IBackupContainer> container = IBackupContainer::openContainer(url, {}, {}, 0);
		co_await container->create();

		// A complete empty snapshot at the target version needs neither logs nor a running backup agent.
		constexpr int blockSize = 1024;
		Reference<IBackupFile> file =
		    co_await container->writeRangeFile(restoreSnapshotVersion, 0, restoreSnapshotVersion, blockSize);
		const uint32_t fileVersion = BACKUP_AGENT_SNAPSHOT_FILE_VERSION;
		co_await file->append(&fileVersion, sizeof(fileVersion));
		co_await file->appendStringRefWithLen(range.begin);
		co_await file->appendStringRefWithLen(range.end);
		ASSERT(file->size() < blockSize);
		const Value padding = fileBackup::makePadding(blockSize - static_cast<int>(file->size()));
		co_await file->append(padding.begin(), padding.size());
		co_await file->finish();
		co_await container->writeKeyspaceSnapshotFile({ file->getFileName() },
		                                              { std::make_pair(Key(range.begin), Key(range.end)) },
		                                              file->size(),
		                                              IncludeKeyRangeMap::True);
		const BackupDescription description = co_await container->describeBackup(true, 0);
		ASSERT(description.maxRestorableVersion == Optional<Version>(restoreSnapshotVersion));
		Standalone<VectorRef<KeyRangeRef>> ranges;
		ranges.push_back_deep(ranges.arena(), range);
		const Optional<RestorableFileSet> restoreSet =
		    co_await container->getRestoreSet(restoreSnapshotVersion, ranges);
		ASSERT(restoreSet.present());
		ASSERT_EQ(restoreSet.get().ranges.size(), 1);
		ASSERT(restoreSet.get().logs.empty());
		ASSERT_EQ(restoreSet.get().targetVersion, restoreSnapshotVersion);
		co_return container;
	}

	static Future<Version> submitFixtureRestore(FileBackupAgent* backupAgent,
	                                            Database cx,
	                                            Reference<IBackupContainer> container,
	                                            Key tag,
	                                            KeyRange range,
	                                            UID restoreUid,
	                                            bool useRangeFileRestore) {
		Standalone<VectorRef<KeyRangeRef>> ranges;
		ranges.push_back_deep(ranges.arena(), range);
		return backupAgent->restore(cx,
		                            {},
		                            tag,
		                            Key(container->getURL()),
		                            container->getProxy(),
		                            ranges,
		                            WaitForComplete::False,
		                            restoreSnapshotVersion,
		                            Verbose::False,
		                            Key(),
		                            Key(),
		                            LockDB::True,
		                            UnlockDB::True,
		                            OnlyApplyMutationLogs::False,
		                            InconsistentSnapshotOnly::False,
		                            invalidVersion,
		                            {},
		                            restoreUid,
		                            useRangeFileRestore);
	}

	Future<Void> testRejectedRestore(Database cx) {
		FileBackupAgent backupAgent;
		const KeyRange range = singleKeyRange(restoreKey);
		const Reference<IBackupContainer> container = co_await createRestoreFixture(range);
		const UID restoreUid = deterministicRandom()->randomUniqueID();
		const Key tag = Key("RangeLockAdmissionRestore/" + restoreUid.toString());
		const Key tagKey = makeRestoreTag(tag.toString()).key;
		ASSERT((co_await readSystemRange(cx, singleKeyRange(tagKey))).empty());
		ASSERT((co_await readSystemRange(cx, singleKeyRange(databaseLockedKey))).empty());
		const auto before = co_await readRestoreSubmissionMetadata(cx);
		co_await expectError(submitFixtureRestore(&backupAgent, cx, container, tag, range, restoreUid, false),
		                     error_code_bulkload_invalid_configuration);
		ASSERT(co_await readRestoreSubmissionMetadata(cx) == before);
		co_await write(cx, protectedKey);

		// These test configurations have no backup agents: the traditional control stays queued until explicitly
		// aborted. An already-committed UID must remain idempotent even when new BulkLoad restores are disabled.
		ASSERT(co_await submitFixtureRestore(&backupAgent, cx, container, tag, range, restoreUid, true) ==
		       restoreSnapshotVersion);
		const auto queued = co_await readRestoreSubmissionMetadata(cx);
		ASSERT(queued != before);
		ASSERT(!(co_await readSystemRange(cx, singleKeyRange(databaseLockedKey))).empty());
		ASSERT(co_await submitFixtureRestore(&backupAgent, cx, container, tag, range, restoreUid, false) ==
		       restoreSnapshotVersion);
		ASSERT(co_await readRestoreSubmissionMetadata(cx) == queued);
		ASSERT(co_await backupAgent.abortRestore(cx, tag) == FileBackupAgent::ERestoreState::ABORTED);
		ASSERT((co_await readSystemRange(cx, singleKeyRange(databaseLockedKey))).empty());

		// A rejected replacement must not clear the previous restore's terminal config or queue another task.
		const auto aborted = co_await readRestoreSubmissionMetadata(cx);
		co_await expectError(
		    submitFixtureRestore(
		        &backupAgent, cx, container, tag, range, deterministicRandom()->randomUniqueID(), false),
		    error_code_bulkload_invalid_configuration);
		ASSERT(co_await readRestoreSubmissionMetadata(cx) == aborted);
		ASSERT(co_await backupAgent.waitRestore(cx, tag, Verbose::False) == FileBackupAgent::ERestoreState::ABORTED);
		co_await write(cx, protectedKey);
		co_await container->deleteContainer();
	}
};

WorkloadFactory<RangeLockAdmission> RangeLockAdmissionFactory;
