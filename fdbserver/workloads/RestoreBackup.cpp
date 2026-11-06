/*
 * RestoreBackup.cpp
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

#include <algorithm>
#include <cmath>
#include <limits>

#include "fdbclient/FDBTypes.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SystemData.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/BackupContainerFileSystem.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/tester/workloads.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "BackupRangeSpec.h"
#include "BackupSteps.h"
#include "ValidateRestoreAudit.h"

// TODO: explain the purpose of this workload and how it different from the
// 20+ (literally) other backup/restore workloads.

struct RestoreBackupWorkload : TestWorkload {

	FileBackupAgent backupAgent;
	Reference<IBackupContainer> backupContainer;

	Standalone<StringRef> backupDir;
	Standalone<StringRef> tag;
	double delayFor;
	StopWhenDone stopWhenDone{ false };
	Optional<std::string> encryptionKeyFileName;
	// A subset of the backup's ranges to restore; the whole backup when unset.
	Optional<Standalone<VectorRef<KeyRangeRef>>> restoreRanges;
	Key addPrefix;
	Key removePrefix;
	UID backupUID;
	// The extra describeBackup() call is skipped in simulation so existing seeds are unaffected.
	bool reportBackupMetrics;
	std::vector<PerfMetric> backupMetrics;
	BackupSteps::BackupVersion version;
	// Fail unless the backup is completed and has nothing registered with the cluster before the restore starts.
	bool requireBackupStopped;
	// Compare the RangeDigest of the restored ranges with that of the live data before it was cleared. Needs an
	// in-place restore, and ranges that nothing writes to between the two digests.
	bool verifyDigest;
	// Deadline of the wait for data distribution to go idle (checked between probes) and bound on one attempt of a
	// digest audit, from it being scheduled to it finishing. An audit that fails is run again, and the ranges are
	// digested one after the other, so the whole digest can take longer.
	double digestTimeoutSeconds;
	// Restore the backup that is in the container at backupDir without waiting for or stopping a running backup
	// (stopWhenDone is rejected). The ranges to restore must then be given (restoreRanges or restoreRangePrefix): the
	// backup's own record of its ranges is not read. An in-place restore fails (requireBackupStopped, default true;
	// false only traces) while any backup or DR logs part of the restored ranges: its log would copy the restore's
	// writes.
	bool useExistingBackup;
	// The version to restore to: the latest restorable one when invalid, else a version inside the restorable window
	// of the backup, given directly or as the fraction of that window (0 is its start, 1 its end).
	Version targetVersion;
	Optional<double> targetVersionFraction;
	Optional<double> restoreDuration;
	Optional<double> snapshotMB;

	explicit RestoreBackupWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDir = getOption(options, "backupDir"_sr, "file://simfdb/backups/"_sr);
		tag = getOption(options, "tag"_sr, "default"_sr);
		delayFor = getOption(options, "delayFor"_sr, 10.0);
		version = BackupSteps::parseBackupVersion(getOption(options, "version"_sr, "v1"_sr));
		stopWhenDone.set(getOption(options, "stopWhenDone"_sr, false));
		requireBackupStopped = getOption(options, "requireBackupStopped"_sr, true);
		restoreRanges = BackupRangeSpec::readOptionalRanges(options, "restoreRanges"_sr, "restoreRangePrefix"_sr);
		addPrefix = BackupRangeSpec::readKeyOption(options, "addPrefix"_sr);
		removePrefix = BackupRangeSpec::readKeyOption(options, "removePrefix"_sr);
		reportBackupMetrics = getOption(options, "reportBackupMetrics"_sr, !g_network->isSimulated());
		verifyDigest = getOption(options, "verifyDigest"_sr, false);
		digestTimeoutSeconds = getOption(options, "digestTimeoutSeconds"_sr, 1800.0);
		useExistingBackup = getOption(options, "useExistingBackup"_sr, false);
		targetVersion = getOption(options, "targetVersion"_sr, static_cast<int64_t>(::invalidVersion));
		double fraction = getOption(options, "targetVersionFraction"_sr, std::numeric_limits<double>::quiet_NaN());
		if (!std::isnan(fraction)) {
			targetVersionFraction = fraction;
		}
		if (useExistingBackup && (!restoreRanges.present() || stopWhenDone)) {
			TraceEvent(SevWarnAlways, "RestoreBackupExistingBackupInvalidOptions")
			    .detail("Tag", tag)
			    .detail("HasRanges", restoreRanges.present())
			    .detail("StopWhenDone", static_cast<bool>(stopWhenDone));
			throw test_specification_invalid();
		}
		if (targetVersionFraction.present() &&
		    (fraction < 0.0 || fraction > 1.0 || targetVersion != ::invalidVersion)) {
			TraceEvent(SevWarnAlways, "RestoreBackupInvalidTargetVersion")
			    .detail("TargetVersion", targetVersion)
			    .detail("TargetVersionFraction", fraction);
			throw test_specification_invalid();
		}
		// The digest after a restore to an earlier version differs from the one before it by construction.
		if (verifyDigest && (targetVersion != ::invalidVersion || targetVersionFraction.present())) {
			TraceEvent(SevWarnAlways, "RestoreBackupDigestWithTargetVersion").detail("Tag", tag);
			throw test_specification_invalid();
		}

		std::string keyFileName = "simfdb/" + getTestEncryptionFileName();
		// Only set encryptionKeyFileName if the encryption key file exists during backup.
		if (fileExists(keyFileName)) {
			encryptionKeyFileName = keyFileName;
		}
	}

	static constexpr auto NAME = "RestoreBackup";

	Future<Void> waitOnBackup(Database cx) {
		Version waitForVersion{ 0 };
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				Version v = co_await tr.getReadVersion();
				waitForVersion = v;
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
		EBackupState backupState =
		    co_await backupAgent.waitBackup(cx, tag.toString(), stopWhenDone, &backupContainer, &backupUID);
		if (backupState == EBackupState::STATE_COMPLETED) {
			co_return;
		} else if (backupState == EBackupState::STATE_RUNNING_DIFFERENTIAL) {
			ASSERT(!stopWhenDone);
			while (true) {
				BackupDescription desc = co_await backupContainer->describeBackup(true);
				TraceEvent("BackupVersionGate")
				    .detail("MaxLogEndVersion", desc.maxLogEnd.present() ? desc.maxLogEnd.get() : invalidVersion)
				    .detail("ContiguousLogEndVersion",
				            desc.contiguousLogEnd.present() ? desc.contiguousLogEnd.get() : invalidVersion)
				    .detail("TargetVersion", waitForVersion);
				if (desc.contiguousLogEnd.present() && desc.contiguousLogEnd.get() >= waitForVersion) {
					double stopStart = now();
					co_await BackupSteps::stopBackup(cx, &backupAgent, tag);
					backupMetrics.emplace_back("Backup stop wait (s)", now() - stopStart, Averaged::False);
					co_return;
				}
				co_await delay(5.0);
			}
		} else {
			TraceEvent(SevError, "BadBackupState").detail("BackupState", BackupAgentBase::getStateText(backupState));
			ASSERT(false);
			co_return;
		}
	}

	static double versionsToSeconds(Version v) { return double(v) / SERVER_KNOBS->VERSIONS_PER_SECOND; }

	// Records metrics about the backup being restored, which describe the whole backup even when restoreRanges
	// selects a subset. The snapshot is the restorable one with the greatest
	// endVersion, which is the snapshot a restore to the latest restorable version uses.
	Future<Void> describeBackupMetrics() {
		BackupDescription desc = co_await backupContainer->describeBackup();
		const KeyspaceSnapshotFile* snapshot = BackupSteps::latestRestorableSnapshot(desc);
		if (snapshot) {
			snapshotMB = snapshot->totalSize / 1e6;
			backupMetrics.emplace_back("Backup snapshot duration (s)",
			                           versionsToSeconds(snapshot->endVersion - snapshot->beginVersion),
			                           Averaged::False);
			backupMetrics.emplace_back("Backup snapshot size (MB)", snapshotMB.get(), Averaged::False);
		}
		if (desc.minLogBegin.present() && desc.maxLogEnd.present()) {
			backupMetrics.emplace_back("Backup log coverage (s)",
			                           versionsToSeconds(desc.maxLogEnd.get() - desc.minLogBegin.get()),
			                           Averaged::False);
		}
		if (desc.minLogBegin.present() && desc.contiguousLogEnd.present()) {
			backupMetrics.emplace_back("Backup contiguous log coverage (s)",
			                           versionsToSeconds(desc.contiguousLogEnd.get() - desc.minLogBegin.get()),
			                           Averaged::False);
		}
		if (desc.minRestorableVersion.present() && desc.maxRestorableVersion.present()) {
			backupMetrics.emplace_back(
			    "Backup restorable window (s)",
			    versionsToSeconds(desc.maxRestorableVersion.get() - desc.minRestorableVersion.get()),
			    Averaged::False);
		}
	}

	// A restore must not share the cluster with backup work: a registered mutation log would copy the restore's own
	// writes into the log keyspace.
	Future<Void> confirmBackupStopped(Database cx) {
		BackupSteps::BackupStopReport report = co_await BackupSteps::checkBackupStopped(cx, backupUID);
		if (!report.stopped()) {
			TraceEvent(requireBackupStopped ? SevWarnAlways : SevWarn, "RestoreBackupBackupNotStopped")
			    .detail("Tag", tag)
			    .detail("OwnRecordLeft", report.ownRecordLeft)
			    .detail("OtherBackups", report.otherBackups)
			    .detail("LogRangesLeft", report.logRangesLeft)
			    .detail("LogDataLeft", report.logDataLeft);
			if (requireBackupStopped) {
				throw backup_error();
			}
		}
	}

	// Digests `ranges` and records how long it took.
	Future<std::vector<RangeDigestResult>> digestAndRecord(Database cx,
	                                                       Standalone<VectorRef<KeyRangeRef>> ranges,
	                                                       std::string label,
	                                                       bool isBaseline = false) {
		// The digest is only exact over settled data distribution; the restore leaves shards moving.
		if (!co_await waitForQuiescentDataDistribution(cx, dbInfo, digestTimeoutSeconds, "RestoreBackupDigest")) {
			throw timed_out();
		}
		AuditWaitOptions waitOptions;
		waitOptions.traceContext = "RestoreBackupDigest";
		waitOptions.maxWaitSeconds = digestTimeoutSeconds;
		double digestStart = now();
		std::vector<RangeDigestResult> results = co_await digestRanges(cx, ranges, waitOptions);
		double elapsed = now() - digestStart;
		int64_t kvCount = 0;
		int64_t byteCount = 0;
		for (auto const& d : results) {
			kvCount += d.kvCount;
			byteCount += d.byteCount;
		}
		TraceEvent("RestoreBackupDigest")
		    .detail("Label", label.c_str())
		    .detail("RangeCount", ranges.size())
		    .detail("KVCount", kvCount)
		    .detail("Bytes", byteCount)
		    .detail("DurationSeconds", elapsed);
		backupMetrics.emplace_back("Digest " + label + " (s)", elapsed, Averaged::False);
		if (isBaseline) {
			backupMetrics.emplace_back("Digest key-value count", double(kvCount), Averaged::False);
			backupMetrics.emplace_back("Digest data (MB)", byteCount / 1e6, Averaged::False);
		}
		co_return results;
	}

	Future<Void> openExistingBackup() {
		backupContainer = co_await BackupSteps::openExistingContainer(backupDir.toString(), encryptionKeyFileName);
		TraceEvent("RestoreBackupExistingContainer").detail("Tag", tag).detail("Url", backupContainer->getURL());
	}

	// Returns the version to restore to, checked against the restorable window of the backup before anything is
	// cleared: invalid for the latest restorable version, which must exist.
	Future<Version> resolveTargetVersion() {
		BackupDescription desc = co_await backupContainer->describeBackup();
		Optional<BackupSteps::RestorableWindow> window = BackupSteps::restorableWindow(desc);
		if (!window.present()) {
			TraceEvent(SevWarnAlways, "RestoreBackupNothingRestorable").detail("Tag", tag);
			throw restore_error();
		}
		co_return BackupSteps::resolveTargetVersion(
		    window.get(), targetVersion, targetVersionFraction, "RestoreBackup");
	}

	// Returns the ranges to restore: restoreRanges if set, else the ranges the backup covers. Every restoreRanges
	// entry must lie within one backup range: submitBackup coalesces them, so a range spanning two is not covered.
	// An existing backup is not read for its ranges: they are restoreRanges.
	Future<Standalone<VectorRef<KeyRangeRef>>> getRangesToRestore(Database cx) {
		Standalone<VectorRef<KeyRangeRef>> ranges;
		if (useExistingBackup) {
			ranges = restoreRanges.get();
		} else {
			std::vector<KeyRange> backupRanges = co_await BackupSteps::readBackupRanges(cx, backupUID);
			ranges = BackupSteps::selectBackupRanges(backupRanges, restoreRanges, "RestoreRangeNotInBackup");
		}
		for (auto const& r : ranges) {
			if (!r.begin.startsWith(removePrefix) || !r.end.startsWith(removePrefix)) {
				TraceEvent(SevWarnAlways, "RestoreRangeOutsideRemovePrefix")
				    .detail("RestoreRange", BackupSteps::describeRange(r))
				    .detail("RemovePrefix", removePrefix);
				throw restore_error();
			}
		}
		co_return ranges;
	}

	Future<Void> _start(Database cx) {
		co_await delay(delayFor);
		if (useExistingBackup) {
			co_await openExistingBackup();
		} else {
			co_await waitOnBackup(cx);
			co_await confirmBackupStopped(cx);
		}
		if (reportBackupMetrics) {
			try {
				co_await describeBackupMetrics();
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				TraceEvent(SevWarn, "RestoreBackupDescribeFailed").error(e);
			}
		}
		Standalone<VectorRef<KeyRangeRef>> ranges = co_await getRangesToRestore(cx);
		// A running backup is restored to its latest restorable version unless a version is asked for.
		Version restoreTarget = ::invalidVersion;
		if (useExistingBackup || targetVersion != ::invalidVersion || targetVersionFraction.present()) {
			restoreTarget = co_await resolveTargetVersion();
		}
		// An in-place restore overwrites live keys: it clears exactly the ranges it restores and takes the database
		// lock. With a prefix change the destination must already be empty, so nothing is cleared and the restore
		// must not block other clients or restores.
		bool inPlace = addPrefix.empty() && removePrefix.empty();
		std::vector<RangeDigestResult> digestsBefore;
		if (verifyDigest) {
			if (!inPlace) {
				// The keys of a restore with a prefix change differ from the live ones.
				TraceEvent(SevWarnAlways, "RestoreBackupDigestNeedsInPlace")
				    .detail("AddPrefix", addPrefix)
				    .detail("RemovePrefix", removePrefix);
				throw restore_error();
			}
			digestsBefore = co_await digestAndRecord(cx, ranges, "before", /*isBaseline=*/true);
			int64_t digestedKeys = 0;
			for (auto const& d : digestsBefore) {
				digestedKeys += d.kvCount;
			}
			if (digestedKeys == 0) {
				// Equal digests of empty ranges prove nothing.
				TraceEvent(SevWarnAlways, "RestoreBackupDigestNothingToVerify").detail("RangeCount", ranges.size());
				throw restore_error();
			}
		}
		if (inPlace) {
			// An existing backup may be logging the ranges: its log would copy the restore's writes.
			co_await BackupSteps::prepareEmptyRanges(cx,
			                                         ranges,
			                                         "RestoreBackup",
			                                         !useExistingBackup     ? BackupSteps::OnLoggedRanges::Ignore
			                                         : requireBackupStopped ? BackupSteps::OnLoggedRanges::Fail
			                                                                : BackupSteps::OnLoggedRanges::Warn);
		}

		LockDB lockDB(inPlace);
		TraceEvent("RestoreBackupStart")
		    .detail("Tag", tag)
		    .detail("InPlace", inPlace)
		    .detail("RangeCount", ranges.size())
		    .detail("AddPrefix", addPrefix)
		    .detail("RemovePrefix", removePrefix)
		    .detail("UseExistingBackup", useExistingBackup)
		    .detail("TargetVersion", restoreTarget)
		    .detail("SnapshotMB", snapshotMB.orDefault(-1));
		double restoreStart = now();
		BackupSteps::RestoreSpec spec;
		spec.tag = tag;
		spec.container = backupContainer;
		spec.ranges = ranges;
		spec.addPrefix = addPrefix;
		spec.removePrefix = removePrefix;
		spec.lockDB = lockDB;
		spec.encryptionKeyFileName = encryptionKeyFileName;
		spec.targetVersion = restoreTarget;
		Version restoredVersion = co_await BackupSteps::restoreBackup(cx, &backupAgent, version, spec);
		restoreDuration = now() - restoreStart;
		TraceEvent("RestoreBackupDone")
		    .detail("Tag", tag)
		    .detail("DurationSeconds", restoreDuration.get())
		    .detail("RestoredVersion", restoredVersion);
		if (verifyDigest) {
			std::vector<RangeDigestResult> digestsAfter = co_await digestAndRecord(cx, ranges, "after");
			// The digests are taken over settled data distribution, so a difference is a failure.
			verifyDigestsEqual(ranges, digestsBefore, digestsAfter);
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.insert(m.end(), backupMetrics.begin(), backupMetrics.end());
		if (!restoreDuration.present()) {
			return;
		}
		m.emplace_back("Restore duration (s)", restoreDuration.get(), Averaged::False);
		// Mutation log bytes are not exposed by BackupDescription, so this is snapshot-only.
		if (snapshotMB.present() && restoreDuration.get() > 0) {
			m.emplace_back(
			    "Restore snapshot throughput (MB/s)", snapshotMB.get() / restoreDuration.get(), Averaged::False);
		}
	}
};

WorkloadFactory<RestoreBackupWorkload> RestoreBackupWorkloadFactory;
