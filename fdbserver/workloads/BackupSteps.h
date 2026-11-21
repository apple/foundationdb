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
#include "fdbserver/mocks3/MockS3Server.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
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

// Which backup system a step drives. The product takes independent choices: how snapshots are made (SnapshotMode), how
// they are restored (RestoreMode) and the type of the mutation logs. `version` picks the logs and the defaults of the
// other two:
//   v1: range file snapshots and restore, mutation logs written by the backup agents.
//   v2: partitioned mutation logs written by backup workers; snapshots and restore as in v1.
// BulkDump snapshots and BulkLoad restore are chosen by name (parseSnapshotMode, parseRestoreMode) and work with
// either. v3 (range-partitioned mutation logs) is rejected: the product writes them but has no restore for them yet.
enum class BackupVersion { V1, V2 };

inline BackupVersion parseBackupVersion(StringRef name) {
	if (name == "v1"_sr) {
		return BackupVersion::V1;
	}
	if (name == "v2"_sr) {
		return BackupVersion::V2;
	}
	TraceEvent(SevWarnAlways, "BackupVersionNotSupported")
	    .detail("Version", name)
	    .detail("Reason",
	            name == "v3"_sr ? "range-partitioned mutation logs cannot be restored yet" : "unknown version");
	throw unsupported_operation();
}

inline MutationLogType mutationLogTypeOf(BackupVersion version) {
	switch (version) {
	case BackupVersion::V1:
		return MutationLogType::DEFAULT;
	case BackupVersion::V2:
		return MutationLogType::PARTITIONED_LOG;
	}
	UNREACHABLE();
}

inline SnapshotMode parseSnapshotMode(StringRef name) {
	if (name == "rangefile"_sr) {
		return SnapshotMode::RANGEFILE;
	}
	if (name == "bulkdump"_sr) {
		return SnapshotMode::BULKDUMP;
	}
	if (name == "both"_sr) {
		return SnapshotMode::BOTH;
	}
	TraceEvent(SevWarnAlways, "BackupSnapshotModeNotSupported").detail("SnapshotMode", name);
	throw test_specification_invalid();
}

inline RestoreMode parseRestoreMode(StringRef name) {
	if (name == "rangefile"_sr) {
		return RestoreMode::RANGEFILE;
	}
	if (name == "bulkload"_sr) {
		return RestoreMode::BULKLOAD;
	}
	TraceEvent(SevWarnAlways, "BackupRestoreModeNotSupported").detail("RestoreMode", name);
	throw test_specification_invalid();
}

// In simulation, serves a blobstore:// URL that points at this machine with the MockS3 server, unless a server is
// already registered for the address. Callers racing on it wait for the same registration. Other URLs and real clusters
// are left alone.
inline Future<Void> startMockS3ForLocalUrl(std::string url) {
	static Future<Void> registration;
	if (!g_network->isSimulated() || !url.starts_with("blobstore://") ||
	    (url.find("127.0.0.1") == std::string::npos && url.find("localhost") == std::string::npos)) {
		co_return;
	}
	if (!registration.isValid()) {
		if (g_simulator->httpHandlers.contains("127.0.0.1:8080")) {
			co_return;
		}
		registration = registerMockS3Server("127.0.0.1", "8080");
	}
	co_await registration;
}

// Clears `ranges`, user and system keys alike.
inline Future<Void> clearRanges(Database cx, Standalone<VectorRef<KeyRangeRef>> ranges) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			for (auto const& r : ranges) {
				tr.clear(r);
			}
			co_await tr.commit();
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

// The versions a backup can be restored to, from its first restorable version to its last.
struct RestorableWindow {
	Version begin;
	Version end;
};

inline Optional<RestorableWindow> restorableWindow(BackupDescription const& desc) {
	if (!desc.minRestorableVersion.present() || !desc.maxRestorableVersion.present()) {
		return Optional<RestorableWindow>();
	}
	return RestorableWindow{ desc.minRestorableVersion.get(), desc.maxRestorableVersion.get() };
}

// The versions inside both windows; empty (begin > end) while they do not overlap.
inline RestorableWindow commonWindow(RestorableWindow const& a, RestorableWindow const& b) {
	return RestorableWindow{ std::max(a.begin, b.begin), std::min(a.end, b.end) };
}

// The version to restore to: `target`, or the given fraction of `window` (0 is its start, 1 its end) when `fraction` is
// set, or invalidVersion for the latest restorable one. A version outside the window is traced as
// <eventPrefix>TargetVersionOutsideWindow and fails with restore_error, before anything is cleared.
inline Version resolveTargetVersion(RestorableWindow const& window,
                                    Version target,
                                    Optional<double> fraction,
                                    std::string eventPrefix) {
	if (fraction.present()) {
		target = window.begin + static_cast<Version>(fraction.get() * double(window.end - window.begin));
	}
	if (target != ::invalidVersion && (target < window.begin || target > window.end)) {
		TraceEvent(SevWarnAlways, (eventPrefix + "TargetVersionOutsideWindow").c_str())
		    .detail("TargetVersion", target)
		    .detail("WindowBegin", window.begin)
		    .detail("WindowEnd", window.end);
		throw restore_error();
	}
	return target;
}

inline Reference<IBackupContainer> openContainerAt(std::string const& url,
                                                   Optional<std::string> const& encryptionKeyFileName) {
	return IBackupContainer::openContainer(
	    url, {}, encryptionKeyFileName, encryptionKeyFileName.present() ? DEFAULT_ENCRYPTION_BLOCK_SIZE : 0);
}

// The URL of the container of an existing backup, if there is one. `url` is the URL of a backup container, or of a
// directory of containers (a backup makes one named `backup-<time>` inside the directory it is given), from which the
// one whose name sorts last is chosen; a directory without one has no backup yet. A directory is not probed by opening
// it as a container: that creates the container's directories inside it. Only a file directory can be listed; a blob
// store URL with a path is taken as the container itself.
inline Future<Optional<std::string>> findContainerUrl(std::string url) {
	std::vector<std::string> inDirectory;
	Error listError;
	try {
		inDirectory = co_await IBackupContainer::listContainers(url, {});
	} catch (Error& e) {
		listError = e;
	}
	if (listError.code() == error_code_backup_invalid_url) {
		co_return url;
	}
	if (listError.code() != invalid_error_code) {
		throw listError;
	}
	std::vector<std::string> backups;
	for (auto const& candidate : inDirectory) {
		std::string path = candidate.substr(0, candidate.find('?'));
		while (!path.empty() && path.back() == '/') {
			path.pop_back();
		}
		if (path.substr(path.find_last_of('/') + 1).starts_with("backup-")) {
			backups.push_back(candidate);
		}
	}
	if (backups.empty()) {
		co_return Optional<std::string>();
	}
	co_return *std::max_element(backups.begin(), backups.end());
}

// The container of an existing backup, found as findContainerUrl does; `url` itself when it has none, which is the
// container if `url` is one.
inline Future<Reference<IBackupContainer>> openExistingContainer(std::string url,
                                                                 Optional<std::string> encryptionKeyFileName) {
	Optional<std::string> found = co_await findContainerUrl(url);
	co_return openContainerAt(found.orDefault(url), encryptionKeyFileName);
}

// The encryption key file of the simulation tests, if it exists: only then are the backups encrypted.
inline Optional<std::string> testEncryptionKeyFile() {
	std::string keyFileName = "simfdb/" + getTestEncryptionFileName();
	return fileExists(keyFileName) ? Optional<std::string>(keyFileName) : Optional<std::string>();
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
	// Range file restore when absent.
	Optional<RestoreMode> restoreMode;
};

// A key range of about `bytes` of snapshot data, picked at random from the range files of the restore set of
// `container` at `targetVersion` (the latest restorable version when invalid).
struct PickedRange {
	KeyRange range;
	int files = 0;
	int64_t bytes = 0;
	int64_t poolFiles = 0;
	int64_t poolBytes = 0;
};

// Picks consecutive range files, in key order, until their sizes add up to `bytes`, and returns the key range they
// cover. The files come from the restore set at `targetVersion`, which partitions the backed-up keys, so the range lies
// inside the backup. When `within` is not empty only files that lie entirely inside one of its ranges are candidates,
// and the picked files all lie inside the same one, so the range never spans keys outside `within`. `seed` makes the
// pick repeatable (0 for a different pick each time). Fails with restore_error when the restore set has no key ranges
// (a backup older than 6.3) or no candidate run of files holds `bytes`.
inline Future<PickedRange> pickRandomRange(Reference<IBackupContainer> container,
                                           Version targetVersion,
                                           int64_t bytes,
                                           int64_t seed,
                                           Standalone<VectorRef<KeyRangeRef>> within) {
	if (targetVersion == ::invalidVersion) {
		BackupDescription desc = co_await container->describeBackup();
		Optional<RestorableWindow> window = restorableWindow(desc);
		if (!window.present()) {
			TraceEvent(SevWarnAlways, "PickRandomRangeNothingRestorable");
			throw restore_error();
		}
		targetVersion = window.get().end;
	}
	Optional<RestorableFileSet> restoreSet = co_await container->getRestoreSet(targetVersion);
	if (!restoreSet.present()) {
		TraceEvent(SevWarnAlways, "PickRandomRangeNoRestoreSet").detail("TargetVersion", targetVersion);
		throw restore_error();
	}
	struct Candidate {
		KeyRange range;
		int64_t size;
		int pool;
	};
	std::vector<Candidate> candidates;
	for (auto const& file : restoreSet.get().ranges) {
		auto found = restoreSet.get().keyRanges.find(file.fileName);
		if (found == restoreSet.get().keyRanges.end()) {
			TraceEvent(SevWarnAlways, "PickRandomRangeNoKeyRanges")
			    .detail("TargetVersion", targetVersion)
			    .detail("File", file.fileName);
			throw restore_error();
		}
		int pool = 0;
		if (!within.empty()) {
			pool = -1;
			for (int i = 0; i < within.size(); ++i) {
				if (within[i].begin <= found->second.begin && found->second.end <= within[i].end) {
					pool = i;
					break;
				}
			}
			if (pool < 0) {
				continue;
			}
		}
		candidates.push_back({ found->second, file.fileSize, pool });
	}
	std::sort(candidates.begin(), candidates.end(), [](Candidate const& a, Candidate const& b) {
		return a.pool != b.pool ? a.pool < b.pool : a.range.begin < b.range.begin;
	});
	PickedRange picked;
	picked.poolFiles = candidates.size();
	for (auto const& c : candidates) {
		picked.poolBytes += c.size;
	}
	// A start is valid when the files from it, up to the end of its pool range, hold `bytes`.
	std::vector<size_t> starts;
	std::vector<int64_t> remaining(candidates.size(), 0);
	for (size_t i = candidates.size(); i-- > 0;) {
		bool sameAsNext = i + 1 < candidates.size() && candidates[i + 1].pool == candidates[i].pool;
		remaining[i] = candidates[i].size + (sameAsNext ? remaining[i + 1] : 0);
		if (remaining[i] >= bytes) {
			starts.push_back(i);
		}
	}
	if (starts.empty()) {
		TraceEvent(SevWarnAlways, "PickRandomRangeTooLarge")
		    .detail("Bytes", bytes)
		    .detail("CandidateFiles", picked.poolFiles)
		    .detail("CandidateBytes", picked.poolBytes);
		throw restore_error();
	}
	DeterministicRandom seeded(seed != 0 ? static_cast<uint32_t>(seed) : deterministicRandom()->randomUInt32());
	size_t first = starts[seeded.randomInt(0, static_cast<int>(starts.size()))];
	size_t last = first;
	int64_t total = candidates[first].size;
	while (total < bytes) {
		++last;
		total += candidates[last].size;
	}
	picked.range = KeyRangeRef(candidates[first].range.begin, candidates[last].range.end);
	picked.files = static_cast<int>(last - first + 1);
	picked.bytes = total;
	TraceEvent("PickedRestoreRange")
	    .detail("TargetVersion", targetVersion)
	    .detail("Bytes", picked.bytes)
	    .detail("Files", picked.files)
	    .detail("PoolFiles", picked.poolFiles)
	    .detail("PoolBytes", picked.poolBytes)
	    .detail("Seed", seed)
	    .detail("Begin", printable(picked.range.begin).substr(0, 120))
	    .detail("End", printable(picked.range.end).substr(0, 120));
	co_return picked;
}

// A restore of `ranges` (as the backup has them) from `container` under `addPrefix`, to `targetVersion` (the latest
// restorable one when invalid).
inline RestoreSpec prefixedRestoreSpec(Reference<IBackupContainer> container,
                                       Key tag,
                                       Standalone<VectorRef<KeyRangeRef>> ranges,
                                       Key addPrefix,
                                       Optional<std::string> encryptionKeyFileName,
                                       Version targetVersion = ::invalidVersion) {
	RestoreSpec spec;
	spec.tag = tag;
	spec.container = container;
	spec.ranges = ranges;
	spec.addPrefix = addPrefix;
	spec.targetVersion = targetVersion;
	spec.encryptionKeyFileName = encryptionKeyFileName;
	return spec;
}

// Restores `spec` and returns the version restored to.
inline Future<Version> restoreBackup(Database cx, FileBackupAgent* agent, BackupVersion version, RestoreSpec spec) {
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
	                                  spec.encryptionKeyFileName,
	                                  Optional<UID>(),
	                                  spec.restoreMode.orDefault(RestoreMode::RANGEFILE) == RestoreMode::RANGEFILE);
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
	// Range file snapshots when absent.
	Optional<SnapshotMode> snapshotMode;
};

inline Future<Void> submitBackup(Database cx, FileBackupAgent* agent, BackupVersion version, SubmitSpec spec) {
	co_await agent->submitBackup(cx,
	                             spec.backupDir,
	                             {},
	                             spec.initSnapshotInterval,
	                             spec.snapshotInterval,
	                             spec.tag.toString(),
	                             spec.ranges,
	                             spec.stopWhenDone,
	                             mutationLogTypeOf(version),
	                             spec.incremental,
	                             spec.encryptionKeyFileName,
	                             spec.encryptionKeyFileName.present() ? DEFAULT_ENCRYPTION_BLOCK_SIZE : 0,
	                             static_cast<int>(spec.snapshotMode.orDefault(SnapshotMode::RANGEFILE)));
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

// What a stopped backup still has registered with the cluster. Mutation-log registration makes the commit proxies
// copy every mutation of the backed-up ranges into the log keyspace, which would double the writes of a restore run
// beside it.
struct BackupStopReport {
	// The backup's own record of how far its mutation log is copied; completing the backup removes it.
	bool ownRecordLeft = false;
	// Other backups (or DR) that share this backup's log destination: removing one backup leaves the registration
	// and the log data of the others in place.
	int otherBackups = 0;
	// Entries that still map a backed-up range to the shared mutation log.
	int logRangesLeft = 0;
	// Whether any mutation log data of the shared destination remains.
	bool logDataLeft = false;

	// True when this backup no longer causes mutation logging. The registration and data are judged only when no
	// other backup shares them.
	bool stopped() const { return !ownRecordLeft && (otherBackups > 0 || (logRangesLeft == 0 && !logDataLeft)); }
};

inline Future<BackupStopReport> checkBackupStoppedTx(Reference<ReadYourWritesTransaction> tr, UID backupUid) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	BackupStopReport report;
	Optional<Key> destUid = co_await BackupConfig(backupUid).destUidValue().get(tr);
	if (!destUid.present()) {
		co_return report;
	}
	Key ownRecord = BinaryWriter::toValue(backupUid, Unversioned())
	                    .withPrefix(destUid.get())
	                    .withPrefix(backupLatestVersionsPrefix);
	Optional<Value> own = co_await tr->get(ownRecord);
	report.ownRecordLeft = own.present();
	RangeResult records =
	    co_await tr->getRange(prefixRange(destUid.get().withPrefix(backupLatestVersionsPrefix)), 1000);
	report.otherBackups = records.size() - (report.ownRecordLeft ? 1 : 0);
	RangeResult registered = co_await tr->getRange(prefixRange(destUid.get().withPrefix(logRangesRange.begin)), 1000);
	report.logRangesLeft = registered.size();
	RangeResult data = co_await tr->getRange(prefixRange(destUid.get().withPrefix(backupLogKeys.begin)), 1);
	report.logDataLeft = !data.empty();
	co_return report;
}

// The number of mutation-log registrations, of any backup or DR, that cover part of `ranges`. A write to a range that
// is registered is copied into the log keyspace by the commit proxies.
inline Future<int> countLogRangesOverlappingTx(Reference<ReadYourWritesTransaction> tr,
                                               Standalone<VectorRef<KeyRangeRef>> ranges) {
	tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr->setOption(FDBTransactionOptions::LOCK_AWARE);
	RangeResult registered = co_await tr->getRange(logRangesRange, CLIENT_KNOBS->TOO_MANY);
	int overlapping = 0;
	for (auto const& kv : registered) {
		KeyRef begin = logRangesDecodeKey(kv.key);
		Key end = logRangesDecodeValue(kv.value);
		for (auto const& r : ranges) {
			if (begin < r.end && r.begin < end) {
				++overlapping;
				break;
			}
		}
	}
	co_return overlapping;
}

inline Future<int> countLogRangesOverlapping(Database cx, Standalone<VectorRef<KeyRangeRef>> ranges) {
	return runRYWTransaction(
	    cx, [=](Reference<ReadYourWritesTransaction> tr) { return countLogRangesOverlappingTx(tr, ranges); });
}

// Reads what backup `backupUid` still has registered. Completing a backup (including discontinuing a restorable
// one) removes its own record in the transaction that sets it COMPLETED, and the registration and log data with it
// unless another backup shares them.
inline Future<BackupStopReport> checkBackupStopped(Database cx, UID backupUid) {
	return runRYWTransaction(
	    cx, [=](Reference<ReadYourWritesTransaction> tr) { return checkBackupStoppedTx(tr, backupUid); });
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

// What to do when a mutation-log registration of any backup or DR covers part of the ranges about to be restored into:
// its log would copy the restore's writes.
enum class OnLoggedRanges { Ignore, Warn, Fail };

// Clears `ranges` and checks that they are empty and, unless `onLogged` is Ignore, that no backup logs them. The ranges
// must be quiescent: a write between the clear and the check fails with restore_error (event
// <eventPrefix>RangeNotCleared); logged ranges are traced as <eventPrefix>LoggingActive and fail with backup_error when
// `onLogged` is Fail.
inline Future<Void> prepareEmptyRanges(Database cx,
                                       Standalone<VectorRef<KeyRangeRef>> ranges,
                                       std::string eventPrefix,
                                       OnLoggedRanges onLogged) {
	if (onLogged != OnLoggedRanges::Ignore) {
		int logging = co_await countLogRangesOverlapping(cx, ranges);
		if (logging > 0) {
			TraceEvent(onLogged == OnLoggedRanges::Fail ? SevWarnAlways : SevWarn,
			           (eventPrefix + "LoggingActive").c_str())
			    .detail("Registrations", logging);
			if (onLogged == OnLoggedRanges::Fail) {
				throw backup_error();
			}
		}
	}
	co_await clearRanges(cx, ranges);
	for (auto const& r : ranges) {
		Optional<Key> first = co_await firstKeyIn(cx, r);
		if (first.present()) {
			TraceEvent(SevWarnAlways, (eventPrefix + "RangeNotCleared").c_str())
			    .detail("Range", describeRange(r))
			    .detail("FirstKey", first.get());
			throw restore_error();
		}
	}
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
