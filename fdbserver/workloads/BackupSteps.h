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

// A committed snapshot restart.
struct SnapshotRestart {
	UID backupUid;
	Version newBeginVersion = invalidVersion;
	Version commitVersion = invalidVersion;
};

// A point in a snapshot that a restart can aim at.
enum class SnapshotPhase {
	// A dispatch task is in mid-run, so its range tasks are likely in flight too.
	Dispatching,
	// Every backup range has a range file, so the last dispatch or the manifest task is next.
	RangeFilesDone,
};

// True if the range file map of `config` covers every backup range.
inline Future<bool> rangeFilesCoverBackup(Reference<ReadYourWritesTransaction> tr, BackupConfig config) {
	std::vector<KeyRange> backupRanges = co_await config.backupRanges().getD(tr);
	BackupConfig::RangeFileMapT::RangeResultType files =
	    co_await config.snapshotRangeFileMap().getRange(tr, {}, {}, CLIENT_KNOBS->TOO_MANY);
	if (backupRanges.empty() || files.more) {
		co_return false;
	}
	std::vector<KeyRange> covered;
	covered.reserve(files.results.size());
	for (const auto& [end, slice] : files.results) {
		covered.push_back(KeyRangeRef(slice.begin, end));
	}
	std::sort(covered.begin(), covered.end(), [](KeyRange const& a, KeyRange const& b) { return a.begin < b.begin; });
	for (const auto& range : backupRanges) {
		Key reached = range.begin;
		for (const auto& c : covered) {
			if (c.begin <= reached && c.end > reached) {
				reached = c.end;
			}
		}
		if (reached < range.end) {
			co_return false;
		}
	}
	co_return true;
}

// Waits up to `timeout` seconds for the snapshot of backup `tag` to reach `phase`.
inline Future<Void> waitForSnapshotPhase(Database cx, Key tag, SnapshotPhase phase, double timeout) {
	double start = now();
	auto tr = makeReference<ReadYourWritesTransaction>(cx);
	while (now() - start < timeout) {
		Error err;
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<UidAndAbortedFlagT> uidFlag = co_await makeBackupTag(tag.toString()).get(tr);
			if (uidFlag.present()) {
				BackupConfig config(uidFlag.get().first);
				bool reached = false;
				if (phase == SnapshotPhase::Dispatching) {
					Optional<Key> batchFuture = co_await config.snapshotBatchFuture().get(tr);
					reached = batchFuture.present();
				} else {
					reached = co_await rangeFilesCoverBackup(tr, config);
				}
				if (reached) {
					co_return;
				}
			}
			co_await delay(deterministicRandom()->random01() * 0.5);
			continue;
		} catch (Error& e) {
			err = e;
		}
		co_await tr->onError(err);
	}
}

// Restarts the snapshot of backup `tag` up to `count` times, each after a random delay of up to `maxDelay` seconds,
// sometimes back to back or aimed at a SnapshotPhase, and returns the restarts that committed. Waits for the backup
// to start, and stops once it refuses: it has finished, was aborted, or is being discontinued.
inline Future<std::vector<SnapshotRestart>> restartSnapshots(Database cx,
                                                             FileBackupAgent* agent,
                                                             Key tag,
                                                             int count,
                                                             double maxDelay,
                                                             std::string eventPrefix) {
	std::vector<SnapshotRestart> restarts;
	for (int i = 0; i < count;) {
		co_await delay(deterministicRandom()->coinflip() ? deterministicRandom()->random01() * maxDelay
		                                                 : deterministicRandom()->random01());
		int aim = deterministicRandom()->randomInt(0, 3);
		if (aim == 1) {
			co_await waitForSnapshotPhase(cx, tag, SnapshotPhase::Dispatching, maxDelay);
		} else if (aim == 2) {
			co_await waitForSnapshotPhase(cx, tag, SnapshotPhase::RangeFilesDone, maxDelay);
		}
		Optional<int64_t> intervalSeconds;
		if (deterministicRandom()->coinflip()) {
			intervalSeconds = deterministicRandom()->coinflip() ? 0 : deterministicRandom()->randomInt(1, 30);
		}

		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		EBackupState state = EBackupState::STATE_NEVERRAN;
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<UidAndAbortedFlagT> uidFlag = co_await makeBackupTag(tag.toString()).get(tr);
				if (uidFlag.present()) {
					state = co_await BackupConfig(uidFlag.get().first)
					            .stateEnum()
					            .getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN);
				}
				Version newBeginVersion = co_await agent->restartSnapshot(tr, tag, intervalSeconds);
				co_await tr->commit();
				restarts.push_back({ uidFlag.get().first, newBeginVersion, tr->getCommittedVersion() });
				TraceEvent((eventPrefix + "RestartedSnapshot").c_str())
				    .detail("Tag", tag)
				    .detail("BackupUID", restarts.back().backupUid)
				    .detail("NewBeginVersion", newBeginVersion)
				    .detail("CommitVersion", restarts.back().commitVersion)
				    .detail("IntervalSeconds", intervalSeconds);
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_backup_unneeded || err.code() == error_code_backup_error) {
				TraceEvent((eventPrefix + "RestartSnapshotRefused").c_str())
				    .error(err)
				    .detail("Tag", tag)
				    .detail("State", BackupAgentBase::getStateText(state));
				// Until the backup starts, it has no snapshot to restart.
				if (state == EBackupState::STATE_SUBMITTED) {
					break;
				}
				co_return restarts;
			}
			co_await tr->onError(err);
		}
		if (state != EBackupState::STATE_SUBMITTED) {
			++i;
		}
	}
	co_return restarts;
}

// Waits until backup `restart.backupUid` completes a snapshot that began after `restart`, or stops running. A
// replaced snapshot chain that stalls would otherwise go unnoticed, since a restorable backup discontinues at once.
inline Future<Void> waitForSnapshotAfterRestart(Database cx,
                                                SnapshotRestart restart,
                                                double timeout,
                                                std::string eventPrefix) {
	BackupConfig config(restart.backupUid);
	double start = now();
	while (true) {
		EBackupState state = EBackupState::STATE_NEVERRAN;
		Optional<Version> latestSnapshotEndVersion;
		co_await runRYWTransaction(cx, [&](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			return store(state, config.stateEnum().getD(tr, Snapshot::False, EBackupState::STATE_NEVERRAN)) &&
			       store(latestSnapshotEndVersion, config.latestSnapshotEndVersion().get(tr));
		});
		bool running = state == EBackupState::STATE_RUNNING || state == EBackupState::STATE_RUNNING_DIFFERENTIAL;
		if (!running || latestSnapshotEndVersion.orDefault(invalidVersion) > restart.commitVersion) {
			TraceEvent((eventPrefix + "SnapshotAfterRestartDone").c_str())
			    .detail("BackupUID", restart.backupUid)
			    .detail("State", BackupAgentBase::getStateText(state))
			    .detail("LatestSnapshotEndVersion", latestSnapshotEndVersion)
			    .detail("RestartCommitVersion", restart.commitVersion)
			    .detail("Seconds", now() - start);
			co_return;
		}
		if (now() - start > timeout) {
			TraceEvent(SevError, (eventPrefix + "SnapshotAfterRestartStalled").c_str())
			    .detail("BackupUID", restart.backupUid)
			    .detail("LatestSnapshotEndVersion", latestSnapshotEndVersion)
			    .detail("RestartCommitVersion", restart.commitVersion)
			    .detail("Seconds", now() - start);
			co_return;
		}
		co_await delay(1.0);
	}
}

// Every snapshot of backup `backupUid` completed after a restart must begin at or after the restart's new snapshot:
// a range file left from the dropped snapshot would pull the begin version back. Old snapshots end before the
// restart commits, since their manifest task reads the range file map before it. Returns false, and traces a
// SevError, for each snapshot that breaks this.
inline bool checkSnapshotsAfterRestarts(BackupDescription const& desc,
                                        UID backupUid,
                                        std::vector<SnapshotRestart> const& restarts,
                                        std::string const& eventPrefix) {
	bool ok = true;
	for (const auto& restart : restarts) {
		if (restart.backupUid != backupUid) {
			continue;
		}
		for (const auto& snapshot : desc.snapshots) {
			if (snapshot.isBulkDump() || snapshot.endVersion <= restart.commitVersion) {
				continue;
			}
			CODE_PROBE(true, "Backup snapshot checked against an earlier snapshot restart");
			if (snapshot.beginVersion < restart.newBeginVersion) {
				TraceEvent(SevError, (eventPrefix + "SnapshotBeginsBeforeRestart").c_str())
				    .detail("BackupUID", backupUid)
				    .detail("Snapshot", snapshot.fileName)
				    .detail("SnapshotBeginVersion", snapshot.beginVersion)
				    .detail("SnapshotEndVersion", snapshot.endVersion)
				    .detail("RestartNewBeginVersion", restart.newBeginVersion)
				    .detail("RestartCommitVersion", restart.commitVersion);
				ok = false;
			}
		}
	}
	return ok;
}

} // namespace BackupSteps

#endif
