/*
 * BackupSteps.h
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

#pragma once

#ifndef FDBSERVER_BACKUP_STEPS_H
#define FDBSERVER_BACKUP_STEPS_H

#include <algorithm>
#include <string>
#include <vector>

#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyBackedTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/RunRYWTransaction.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/tester/workloads.h"

// Steps shared by the workloads that act on an existing backup, taking explicit parameters.
namespace BackupSteps {

// Discontinues the backup `tag`, tolerating one that no longer needs it.
inline Future<Void> discontinueBackup(Database cx, FileBackupAgent* agent, Key tag) {
	try {
		TraceEvent("DiscontinuingBackup").log();
		co_await agent->discontinueBackup(cx, tag);
	} catch (Error& e) {
		TraceEvent("ErrorDiscontinuingBackup").error(e);
		if (e.code() != error_code_backup_unneeded) {
			throw;
		}
	}
}

// Which backup system a step drives. Only the file backup agent (v1) is implemented; parseBackupVersion rejects any
// other name with unsupported_operation.
enum class BackupVersion { V1 };

inline BackupVersion parseBackupVersion(StringRef name) {
	if (name == "v1"_sr) {
		return BackupVersion::V1;
	}
	TraceEvent(SevWarnAlways, "BackupVersionNotSupported").detail("Version", name);
	throw unsupported_operation();
}

// What a restore needs, with named fields instead of the 17 positional arguments of FileBackupAgent::restore.
struct RestoreSpec {
	Key tag;
	Reference<IBackupContainer> container;
	Standalone<VectorRef<KeyRangeRef>> ranges;
	Key addPrefix;
	Key removePrefix;
	// Restoring in place locks the database for the restore and unlocks it afterwards.
	LockDB lockDB{ false };
	Version targetVersion = ::invalidVersion;
	Optional<std::string> encryptionKeyFileName;
};

// Restores `spec` and returns the version restored to.
inline Future<Version> restoreBackup(Database cx, FileBackupAgent* agent, BackupVersion version, RestoreSpec spec) {
	ASSERT(version == BackupVersion::V1);
	co_return co_await agent->restore(cx,
	                                  cx,
	                                  spec.tag,
	                                  Key(spec.container->getURL()),
	                                  spec.container->getProxy(),
	                                  spec.ranges,
	                                  WaitForComplete::True,
	                                  spec.targetVersion,
	                                  Verbose::True,
	                                  spec.addPrefix,
	                                  spec.removePrefix,
	                                  spec.lockDB,
	                                  UnlockDB(spec.lockDB),
	                                  OnlyApplyMutationLogs::False,
	                                  InconsistentSnapshotOnly::False,
	                                  ::invalidVersion,
	                                  spec.encryptionKeyFileName);
}

// What starting a backup needs.
struct SubmitSpec {
	Key backupDir;
	Key tag;
	Standalone<VectorRef<KeyRangeRef>> ranges;
	int initSnapshotInterval = 0;
	int snapshotInterval = 1e8;
	StopWhenDone stopWhenDone{ true };
	IncrementalBackupOnly incremental{ false };
	Optional<std::string> encryptionKeyFileName;
};

inline Future<Void> submitBackup(Database cx, FileBackupAgent* agent, BackupVersion version, SubmitSpec spec) {
	ASSERT(version == BackupVersion::V1);
	co_await agent->submitBackup(cx,
	                             spec.backupDir,
	                             {},
	                             spec.initSnapshotInterval,
	                             spec.snapshotInterval,
	                             spec.tag.toString(),
	                             spec.ranges,
	                             spec.stopWhenDone,
	                             MutationLogType::DEFAULT,
	                             spec.incremental,
	                             spec.encryptionKeyFileName,
	                             spec.encryptionKeyFileName.present() ? DEFAULT_ENCRYPTION_BLOCK_SIZE : 0,
	                             0);
}

// Discontinues the backup `tag` and waits until it is COMPLETED, so that no backup work is left on the cluster.
inline Future<Void> stopBackup(Database cx, FileBackupAgent* agent, Key tag) {
	co_await discontinueBackup(cx, agent, tag);
	co_await agent->waitBackup(cx, tag.toString(), StopWhenDone::True);
}

inline Future<Optional<Key>> firstKeyInTx(Reference<ReadYourWritesTransaction> tr, KeyRange range) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	RangeResult first = co_await tr->getRange(range, 1);
	co_return first.empty() ? Optional<Key>() : Optional<Key>(first[0].key);
}

// The first key in `range`, if any. It reads system keys and is lock-aware, so it works for the validation prefix and
// while the database is locked.
inline Future<Optional<Key>> firstKeyIn(Database cx, KeyRange range) {
	return runRYWTransaction(cx, [=](Reference<ReadYourWritesTransaction> tr) { return firstKeyInTx(tr, range); });
}

// Ends backup `tag` once it is restorable. With buggify it first waits for the backup to be restorable and reports
// (SevError) one that has no container or is not restorable; then it aborts the backup when `abortInstead`, otherwise
// discontinues it. A backup that is already unneeded or a duplicate is not an error. `stopDifferentialDelay` is only
// traced. Events are named <eventPrefix>DoBackup... so that each workload keeps its own names.
// buggify() is decided per source line, so the workloads that call this share one activation of that branch: they all
// wait for restorability, or none does.
inline Future<Void> endBackupAfterRestorable(Database cx,
                                             FileBackupAgent* backupAgent,
                                             Key tag,
                                             UID randomID,
                                             std::string eventPrefix,
                                             bool abortInstead,
                                             double stopDifferentialDelay) {
	try {
		if (buggify()) {
			KeyBackedTag backupTag = makeBackupTag(tag.toString());
			TraceEvent((eventPrefix + "DoBackupWaitForRestorable").c_str(), randomID).detail("Tag", backupTag.tagName);

			// Wait until the backup is in a restorable state and get the status, URL, and UID atomically
			Reference<IBackupContainer> lastBackupContainer;
			UID lastBackupUID;
			EBackupState resultWait = co_await backupAgent->waitBackup(
			    cx, backupTag.tagName, StopWhenDone::False, &lastBackupContainer, &lastBackupUID);

			TraceEvent((eventPrefix + "DoBackupWaitForRestorable").c_str(), randomID)
			    .detail("Tag", backupTag.tagName)
			    .detail("Result", BackupAgentBase::getStateText(resultWait));

			bool restorable = false;
			if (lastBackupContainer) {
				Future<BackupDescription> fdesc = lastBackupContainer->describeBackup();
				co_await ready(fdesc);

				if (!fdesc.isError()) {
					BackupDescription desc = fdesc.get();
					co_await desc.resolveVersionTimes(cx);
					printf("BackupDescription:\n%s\n", desc.toString().c_str());
					restorable = desc.maxRestorableVersion.present();
				}
			}

			TraceEvent((eventPrefix + "LastBackupContainer").c_str(), randomID)
			    .detail("BackupTag", printable(tag))
			    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
			    .detail("LastBackupUID", lastBackupUID)
			    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait))
			    .detail("Restorable", restorable);

			// Do not check the backup, if aborted
			if (resultWait == EBackupState::STATE_ABORTED) {
			}
			// Ensure that a backup container was found
			else if (!lastBackupContainer) {
				TraceEvent(SevError, (eventPrefix + "MissingBackupContainer").c_str(), randomID)
				    .detail("LastBackupUID", lastBackupUID)
				    .detail("BackupTag", printable(tag))
				    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait));
				printf("BackupCorrectnessMissingBackupContainer   tag: %s  status: %s\n",
				       printable(tag).c_str(),
				       BackupAgentBase::getStateText(resultWait));
			}
			// Check that backup is restorable
			else if (!restorable) {
				TraceEvent(SevError, (eventPrefix + "NotRestorable").c_str(), randomID)
				    .detail("LastBackupUID", lastBackupUID)
				    .detail("BackupTag", printable(tag))
				    .detail("BackupFolder", lastBackupContainer->getURL())
				    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait));
				printf("BackupCorrectnessNotRestorable:  tag: %s\n", printable(tag).c_str());
			}

			// Abort the backup, if not the first backup because the second backup may have aborted the backup
			// by now
			if (abortInstead) {
				TraceEvent((eventPrefix + "DoBackupAbortBackup2").c_str(), randomID)
				    .detail("Tag", printable(tag))
				    .detail("WaitStatus", BackupAgentBase::getStateText(resultWait))
				    .detail("LastBackupContainer", lastBackupContainer ? lastBackupContainer->getURL() : "")
				    .detail("Restorable", restorable);
				co_await backupAgent->abortBackup(cx, tag.toString());
			} else {
				TraceEvent((eventPrefix + "DoBackupDiscontinueBackup").c_str(), randomID)
				    .detail("Tag", printable(tag))
				    .detail("DifferentialAfter", stopDifferentialDelay);
				co_await backupAgent->discontinueBackup(cx, tag);
			}
		}

		else {
			TraceEvent((eventPrefix + "DoBackupDiscontinueBackup").c_str(), randomID)
			    .detail("Tag", printable(tag))
			    .detail("DifferentialAfter", stopDifferentialDelay);
			co_await backupAgent->discontinueBackup(cx, tag);
		}
	} catch (Error& e) {
		TraceEvent((eventPrefix + "DoBackupDiscontinueBackupException").c_str(), randomID)
		    .error(e)
		    .detail("Tag", printable(tag));
		if (e.code() != error_code_backup_unneeded && e.code() != error_code_backup_duplicate)
			throw;
	}
}

// The restorable snapshot with the greatest endVersion, which is the one a restore to the latest restorable version
// uses.
inline const KeyspaceSnapshotFile* latestRestorableSnapshot(BackupDescription const& desc) {
	const KeyspaceSnapshotFile* snapshot = nullptr;
	for (const auto& s : desc.snapshots) {
		if (s.restorable.orDefault(false) && (!snapshot || s.endVersion > snapshot->endVersion)) {
			snapshot = &s;
		}
	}
	return snapshot;
}

inline std::string describeRange(KeyRangeRef const& r) {
	return printable(r.begin) + ":" + printable(r.end);
}

inline std::string describeRanges(std::vector<KeyRange> const& ranges) {
	std::string result;
	for (auto const& r : ranges) {
		result += (result.empty() ? "" : ";") + describeRange(r);
	}
	return result;
}

// Returns `requested` if set, else `backupRanges`. Every requested entry must lie within one backup range:
// submitBackup coalesces them, so a range spanning two is not covered. Otherwise traces `notCoveredEvent` and throws
// restore_error.
inline Standalone<VectorRef<KeyRangeRef>> selectBackupRanges(
    std::vector<KeyRange> const& backupRanges,
    Optional<Standalone<VectorRef<KeyRangeRef>>> const& requested,
    const char* notCoveredEvent) {
	Standalone<VectorRef<KeyRangeRef>> ranges;
	if (requested.present()) {
		for (auto const& r : requested.get()) {
			bool covered =
			    std::any_of(backupRanges.begin(), backupRanges.end(), [&](KeyRange const& b) { return b.contains(r); });
			if (!covered) {
				TraceEvent(SevWarnAlways, notCoveredEvent)
				    .detail("RestoreRange", describeRange(r))
				    .detail("BackupRanges", describeRanges(backupRanges));
				throw restore_error();
			}
			ranges.push_back_deep(ranges.arena(), r);
		}
	} else {
		for (auto const& r : backupRanges) {
			ranges.push_back_deep(ranges.arena(), r);
		}
	}
	return ranges;
}

// The ranges the backup `backupUid` covers.
inline Future<std::vector<KeyRange>> readBackupRanges(Database cx, UID backupUid) {
	return BackupConfig(backupUid).backupRanges().getOrThrow(cx.getReference());
}

} // namespace BackupSteps

#endif
