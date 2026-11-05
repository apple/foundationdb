/*
 * CompareBackups.cpp
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

#include "fdbclient/BackupAgent.h"
#include "fdbclient/BackupContainer.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbserver/tester/TestEncryptionUtils.h"
#include "fdbserver/tester/workloads.h"
#include "BackupRangeSpec.h"
#include "BackupSteps.h"
#include "RangeCompare.h"

// Checks that two backups of the same data restore to the same key-values over the given ranges: both are restored to
// the same version, each under its own prefix next to the live data, and the two copies are compared key by key. Unlike
// ValidateBackup it needs no quiescent source: the version restored to is inside the restorable window of both backups,
// so the copies are equal however the live data changes meanwhile. The backups may still be running or finished, or be
// containers made by an earlier run. Each backup is given as the URL of a blob store container or as a file directory
// holding its container (`backup-<time>`; the last by name is used), which is waited for until it exists.
struct CompareBackupsWorkload : TestWorkload {
	static constexpr auto NAME = "CompareBackups";

	FileBackupAgent agentA;
	FileBackupAgent agentB;
	Standalone<StringRef> backupDirA;
	Standalone<StringRef> backupDirB;
	// Where the two backups are restored to: the ranges of the backups prefixed by these, which must be empty and not
	// be logged by any backup.
	Key prefixA;
	Key prefixB;
	// The ranges to restore and compare, as the backups have them.
	Standalone<VectorRef<KeyRangeRef>> ranges;
	BackupSteps::BackupVersion version;
	double delayFor;
	// How long to wait for both backups to be restorable.
	double waitRestorableSeconds;
	// The version to restore both to; the latest version restorable in both when invalid.
	Version targetVersion;
	// A self-test: writes one extra key into the copy of the second backup and fails unless the comparison finds it.
	bool expectDifferences;
	bool clearWhenDone;
	int parallelism;
	int64_t shardBytes;
	int pageRows;
	Optional<std::string> encryptionKeyFileName;
	std::vector<PerfMetric> metrics;

	explicit CompareBackupsWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		backupDirA = getOption(options, "backupDirA"_sr, ""_sr);
		backupDirB = getOption(options, "backupDirB"_sr, ""_sr);
		prefixA = BackupRangeSpec::readKeyOption(options, "prefixA"_sr);
		prefixB = BackupRangeSpec::readKeyOption(options, "prefixB"_sr);
		if (prefixA.empty()) {
			prefixA = Key("compare/a/"_sr);
		}
		if (prefixB.empty()) {
			prefixB = Key("compare/b/"_sr);
		}
		auto requested = BackupRangeSpec::readOptionalRanges(options, "ranges"_sr, "rangePrefix"_sr);
		version = BackupSteps::parseBackupVersion(getOption(options, "version"_sr, "v1"_sr));
		delayFor = getOption(options, "delayFor"_sr, 0.0);
		waitRestorableSeconds = getOption(options, "waitRestorableSeconds"_sr, 600.0);
		targetVersion = getOption(options, "targetVersion"_sr, static_cast<int64_t>(::invalidVersion));
		expectDifferences = getOption(options, "expectDifferences"_sr, false);
		clearWhenDone = getOption(options, "clearWhenDone"_sr, true);
		parallelism = getOption(options, "parallelism"_sr, 8);
		shardBytes = getOption(options, "shardBytes"_sr, static_cast<int64_t>(50000000));
		pageRows = getOption(options, "pageRows"_sr, RangeCompare::defaultPageRows);
		if (backupDirA.empty() || backupDirB.empty() || !requested.present() || prefixA == prefixB) {
			TraceEvent(SevWarnAlways, "CompareBackupsInvalidOptions")
			    .detail("HasDirectories", !backupDirA.empty() && !backupDirB.empty())
			    .detail("HasRanges", requested.present())
			    .detail("PrefixesDiffer", prefixA != prefixB);
			throw test_specification_invalid();
		}
		ranges = requested.get();

		std::string keyFileName = "simfdb/" + getTestEncryptionFileName();
		if (fileExists(keyFileName)) {
			encryptionKeyFileName = keyFileName;
		}
	}

	Standalone<VectorRef<KeyRangeRef>> prefixedRanges(Key const& prefix) const {
		Standalone<VectorRef<KeyRangeRef>> result;
		for (auto const& r : ranges) {
			result.push_back_deep(result.arena(), KeyRangeRef(r.begin.withPrefix(prefix), r.end.withPrefix(prefix)));
		}
		return result;
	}

	// Waits until both backups have a restorable version in common, and returns the version to restore both to and
	// their containers. The containers are looked up again on every attempt: a backup that has not started yet has
	// none.
	Future<Version> chooseVersion(Reference<IBackupContainer>* a, Reference<IBackupContainer>* b) {
		double start = now();
		while (true) {
			Optional<std::string> urlA = co_await BackupSteps::findContainerUrl(backupDirA.toString());
			Optional<std::string> urlB = co_await BackupSteps::findContainerUrl(backupDirB.toString());
			Optional<BackupSteps::RestorableWindow> windowA;
			Optional<BackupSteps::RestorableWindow> windowB;
			if (urlA.present() && urlB.present()) {
				*a = BackupSteps::openContainerAt(urlA.get(), encryptionKeyFileName);
				*b = BackupSteps::openContainerAt(urlB.get(), encryptionKeyFileName);
				windowA = BackupSteps::restorableWindow(co_await (*a)->describeBackup());
				windowB = BackupSteps::restorableWindow(co_await (*b)->describeBackup());
			}
			if (windowA.present() && windowB.present()) {
				// The windows of two backups made at the same time start at different snapshot versions and have no
				// version in common until the logs of the earlier one reach the later one's snapshot.
				BackupSteps::RestorableWindow common = BackupSteps::commonWindow(windowA.get(), windowB.get());
				bool reachable =
				    targetVersion == ::invalidVersion ? common.begin <= common.end : targetVersion <= common.end;
				if (reachable) {
					Version target =
					    BackupSteps::resolveTargetVersion(common, targetVersion, Optional<double>(), "CompareBackups");
					TraceEvent("CompareBackupsVersion")
					    .detail("BeginA", windowA.get().begin)
					    .detail("EndA", windowA.get().end)
					    .detail("BeginB", windowB.get().begin)
					    .detail("EndB", windowB.get().end)
					    .detail("Target", target);
					co_return target == ::invalidVersion ? common.end : target;
				}
			}
			if (now() - start > waitRestorableSeconds) {
				TraceEvent(SevWarnAlways, "CompareBackupsNotRestorable")
				    .detail("FoundA", urlA.present())
				    .detail("FoundB", urlB.present())
				    .detail("BeginA", windowA.present() ? windowA.get().begin : -1)
				    .detail("EndA", windowA.present() ? windowA.get().end : -1)
				    .detail("BeginB", windowB.present() ? windowB.get().begin : -1)
				    .detail("EndB", windowB.present() ? windowB.get().end : -1)
				    .detail("TargetVersion", targetVersion);
				throw restore_error();
			}
			co_await delay(5.0);
		}
	}

	BackupSteps::RestoreSpec restoreSpec(Reference<IBackupContainer> container,
	                                     Key const& tag,
	                                     Key const& prefix,
	                                     Version target) const {
		BackupSteps::RestoreSpec spec;
		spec.tag = tag;
		spec.container = container;
		spec.ranges = ranges;
		spec.addPrefix = prefix;
		spec.targetVersion = target;
		spec.encryptionKeyFileName = encryptionKeyFileName;
		return spec;
	}

	Standalone<VectorRef<KeyRangeRef>> destinations() const {
		Standalone<VectorRef<KeyRangeRef>> result = prefixedRanges(prefixA);
		for (auto const& r : prefixedRanges(prefixB)) {
			result.push_back_deep(result.arena(), r);
		}
		return result;
	}

	// Makes the copies differ in all three ways the comparison reports: a key only in the second copy, a key only in
	// the first, and a key whose value differs.
	Future<Void> injectDifferences(Database cx) {
		Key beginA = ranges[0].begin.withPrefix(prefixA);
		Key beginB = ranges[0].begin.withPrefix(prefixB);
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				RangeResult first = co_await tr.getRange(KeyRangeRef(beginB, ranges[0].end.withPrefix(prefixB)), 1);
				if (!first.empty()) {
					tr.set(first[0].key, first[0].value.withSuffix("x"_sr));
				}
				tr.set(beginB.withSuffix("\x00compareOnlyInSecond"_sr), "x"_sr);
				tr.set(beginA.withSuffix("\x00compareOnlyInFirst"_sr), "x"_sr);
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> abortRestores(Database cx) {
		for (auto [agent, tag] : { std::pair<FileBackupAgent*, Key>(&agentA, "compareA"_sr),
		                           std::pair<FileBackupAgent*, Key>(&agentB, "compareB"_sr) }) {
			try {
				co_await agent->abortRestore(cx, tag);
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw;
				}
				TraceEvent(SevWarn, "CompareBackupsAbortRestoreFailed").error(e).detail("Tag", tag);
			}
		}
	}

	// Restores both backups to the chosen version, compares the copies and checks the outcome.
	Future<Void> run(Database cx) {
		Reference<IBackupContainer> containerA;
		Reference<IBackupContainer> containerB;
		Version target = co_await chooseVersion(&containerA, &containerB);
		// A backup that logs the destinations would copy the restores' writes into its log.
		co_await BackupSteps::prepareEmptyRanges(
		    cx, destinations(), "CompareBackups", BackupSteps::OnLoggedRanges::Fail);

		double restoreStart = now();
		std::vector<Future<Version>> restores;
		restores.push_back(
		    BackupSteps::restoreBackup(cx, &agentA, version, restoreSpec(containerA, "compareA"_sr, prefixA, target)));
		restores.push_back(
		    BackupSteps::restoreBackup(cx, &agentB, version, restoreSpec(containerB, "compareB"_sr, prefixB, target)));
		co_await waitForAll(restores);
		double restoreSeconds = now() - restoreStart;
		if (expectDifferences) {
			co_await injectDifferences(cx);
		}
		double compareStart = now();
		RangeCompareReport total;
		for (auto const& r : ranges) {
			total.add(co_await compareRanges(cx, r, prefixA, prefixB, parallelism, shardBytes, 10, pageRows), 10);
		}
		double compareSeconds = now() - compareStart;
		TraceEvent("CompareBackupsSummary")
		    .detail("Version", target)
		    .detail("KeysA", total.leftKeys)
		    .detail("KeysB", total.rightKeys)
		    .detail("BytesA", total.leftBytes)
		    .detail("BytesB", total.rightBytes)
		    .detail("Differences", total.differences)
		    .detail("Shards", total.shards)
		    .detail("RestoreSeconds", restoreSeconds)
		    .detail("CompareSeconds", compareSeconds);
		for (int i = 0; i < total.firstDifferences.size(); ++i) {
			TraceEvent("CompareBackupsDifference").detail("Index", i).detail("What", total.firstDifferences[i]);
		}
		metrics.emplace_back("Compared key-values", double(total.leftKeys), Averaged::False);
		metrics.emplace_back("Compared data (MB)", total.leftBytes / 1e6, Averaged::False);
		metrics.emplace_back("Differences", double(total.differences), Averaged::False);
		metrics.emplace_back("Restore both backups duration (s)", restoreSeconds, Averaged::False);
		metrics.emplace_back("Compare duration (s)", compareSeconds, Averaged::False);

		// Two empty copies are equal and prove nothing.
		if (total.leftKeys == 0) {
			TraceEvent(SevWarnAlways, "CompareBackupsNothingCompared");
			throw restore_error();
		}
		if (expectDifferences) {
			if (total.differences != 3) {
				TraceEvent(SevWarnAlways, "CompareBackupsMissedInjectedDifference")
				    .detail("Differences", total.differences);
				throw restore_error();
			}
		} else if (total.differences > 0) {
			TraceEvent(SevWarnAlways, "CompareBackupsDifferent").detail("Differences", total.differences);
			throw restore_error();
		}
	}

	Future<Void> _start(Database cx) {
		co_await delay(delayFor);
		Error failure;
		try {
			co_await run(cx);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			failure = e;
		}
		if (failure.code() != invalid_error_code) {
			// Stop a restore that is still running, so that it does not keep writing into the prefixes.
			co_await abortRestores(cx);
		}
		if (clearWhenDone) {
			co_await BackupSteps::clearRanges(cx, destinations());
		}
		if (failure.code() != invalid_error_code) {
			throw failure;
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override { return clientId ? Void() : _start(cx); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override { m.insert(m.end(), metrics.begin(), metrics.end()); }
};

WorkloadFactory<CompareBackupsWorkload> CompareBackupsWorkloadFactory;
