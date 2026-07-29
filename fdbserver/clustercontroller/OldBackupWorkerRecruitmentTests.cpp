/*
 * OldBackupWorkerRecruitmentTests.cpp
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

#include "ClusterController.h"

#include "fdbserver/core/RecoveryState.h"
#include "flow/UnitTest.h"

namespace {

BackupInterface makeBackupWorker(NetworkAddress const& address, UID id, std::string const& processId) {
	LocalityData locality;
	locality.set(LocalityData::keyProcessId, Standalone<StringRef>(processId));
	BackupInterface worker(locality);
	worker.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ address }, id));
	return worker;
}

LogSystemConfig makeOldBackupConfiguration(LogEpoch currentEpoch,
                                           LogEpoch oldEpoch,
                                           Version oldEndVersion,
                                           int oldClassicTags,
                                           int oldRangePartitionedTags,
                                           BackupInterface const& currentWorker,
                                           std::vector<BackupInterface> const& oldWorkers) {
	LogSystemConfig config(currentEpoch);
	config.logRouterTags = 1;
	config.rangePartitionedBackupWorkerTags = oldRangePartitionedTags > 0 ? 1 : 0;
	config.oldestBackupEpoch = oldEpoch;

	TLogSet currentSet;
	currentSet.backupWorkers.emplace_back(currentWorker);
	config.tLogs.push_back(currentSet);

	TLogSet oldSet;
	for (auto const& worker : oldWorkers) {
		oldSet.backupWorkers.emplace_back(worker);
	}
	OldTLogConf old;
	old.epoch = oldEpoch;
	old.epochBegin = 100;
	old.epochEnd = oldEndVersion;
	old.logRouterTags = oldClassicTags;
	old.rangePartitionedBackupWorkerTags = oldRangePartitionedTags;
	old.tLogs.push_back(oldSet);
	config.oldTLogs.push_back(old);
	return config;
}

} // namespace

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/monitorBeforeFullyRecovered") {
	ASSERT(!canMonitorOldBackupWorkers(RecoveryState::UNINITIALIZED));
	ASSERT(!canMonitorOldBackupWorkers(RecoveryState::READING_CSTATE));
	ASSERT(!canMonitorOldBackupWorkers(RecoveryState::LOCKING_CSTATE));
	ASSERT(!canMonitorOldBackupWorkers(RecoveryState::RECRUITING));
	ASSERT(!canMonitorOldBackupWorkers(RecoveryState::RECOVERY_TRANSACTION));
	ASSERT(!canMonitorOldBackupWorkers(RecoveryState::WRITING_CSTATE));
	ASSERT(canMonitorOldBackupWorkers(RecoveryState::ACCEPTING_COMMITS));
	ASSERT(canMonitorOldBackupWorkers(RecoveryState::ALL_LOGS_RECRUITED));
	ASSERT(canMonitorOldBackupWorkers(RecoveryState::STORAGE_RECOVERED));
	ASSERT(canMonitorOldBackupWorkers(RecoveryState::FULLY_RECOVERED));
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/preservesFailureDeadlineAcrossSameRecoveryDbInfoChanges") {
	constexpr LogEpoch monitoredRecovery = 17;
	for (int update = 0; update < 100; ++update) {
		ASSERT(!shouldRestartOldBackupWorkerMonitor(
		    RecoveryState::ACCEPTING_COMMITS, monitoredRecovery, monitoredRecovery, true));
	}
	ASSERT(!shouldRestartOldBackupWorkerMonitor(
	    RecoveryState::ALL_LOGS_RECRUITED, monitoredRecovery, monitoredRecovery, true));
	ASSERT(!shouldRestartOldBackupWorkerMonitor(
	    RecoveryState::STORAGE_RECOVERED, monitoredRecovery, monitoredRecovery, true));
	ASSERT(!shouldRestartOldBackupWorkerMonitor(
	    RecoveryState::FULLY_RECOVERED, monitoredRecovery, monitoredRecovery, true));

	ASSERT(shouldRestartOldBackupWorkerMonitor(
	    RecoveryState::ACCEPTING_COMMITS, monitoredRecovery, monitoredRecovery + 1, true));
	ASSERT(
	    shouldRestartOldBackupWorkerMonitor(RecoveryState::WRITING_CSTATE, monitoredRecovery, monitoredRecovery, true));
	ASSERT(shouldRestartOldBackupWorkerMonitor(
	    RecoveryState::ACCEPTING_COMMITS, monitoredRecovery, monitoredRecovery, false));
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/collectsOnlyOldGenerationInterfaces") {
	constexpr LogEpoch currentEpoch = 20;
	constexpr LogEpoch oldEpoch = 19;
	NetworkAddress sharedAddress(IPAddress(0x01010101), 1);
	BackupInterface currentWorker = makeBackupWorker(sharedAddress, UID(1, 1), "shared-process");
	BackupInterface colocatedOldWorker = makeBackupWorker(sharedAddress, UID(1, 2), "shared-process");
	BackupInterface oldOnlyWorker =
	    makeBackupWorker(NetworkAddress(IPAddress(0x02020202), 1), UID(2, 1), "old-only-process");
	LogSystemConfig config = makeOldBackupConfiguration(
	    currentEpoch, oldEpoch, 500, 2, 0, currentWorker, { colocatedOldWorker, oldOnlyWorker });

	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	ASSERT_EQ(oldWorkers.size(), 2);
	ASSERT(oldWorkers[0].interf == colocatedOldWorker);
	ASSERT(oldWorkers[1].interf == oldOnlyWorker);
	ASSERT(oldWorkers[0].interf.id() != currentWorker.id());
	ASSERT(oldWorkers[0].interf.address() == currentWorker.address());
	ASSERT_EQ(oldWorkers[0].backupEpoch, oldEpoch);
	ASSERT_EQ(oldWorkers[0].epochEnd, 500);
	ASSERT_EQ(oldWorkers[0].totalTags, 2);
	ASSERT(!oldWorkers[0].rangePartitioned);
	ASSERT_EQ(oldWorkers[1].backupEpoch, oldEpoch);
	ASSERT_EQ(oldWorkers[1].totalTags, 2);
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/collectsRangePartitionedGeneration") {
	constexpr LogEpoch currentEpoch = 30;
	constexpr LogEpoch oldEpoch = 29;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x03030303), 1), UID(3, 1), "current");
	BackupInterface oldWorker = makeBackupWorker(NetworkAddress(IPAddress(0x04040404), 1), UID(4, 1), "old-range");
	LogSystemConfig config =
	    makeOldBackupConfiguration(currentEpoch, oldEpoch, 800, 0, 3, currentWorker, { oldWorker });

	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	ASSERT_EQ(oldWorkers.size(), 1);
	ASSERT(oldWorkers[0].interf == oldWorker);
	ASSERT(oldWorkers[0].rangePartitioned);
	ASSERT_EQ(oldWorkers[0].totalTags, 3);
	ASSERT_EQ(oldWorkers[0].backupEpoch, oldEpoch);
	ASSERT_EQ(oldWorkers[0].epochEnd, 800);
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/ignoresMissingWorkersAndZeroTagGenerations") {
	constexpr LogEpoch currentEpoch = 80;
	constexpr LogEpoch oldEpoch = 79;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x10101010), 1), UID(16, 1), "current");
	BackupInterface oldWorker = makeBackupWorker(NetworkAddress(IPAddress(0x11111111), 1), UID(17, 1), "stale");

	LogSystemConfig zeroTags =
	    makeOldBackupConfiguration(currentEpoch, oldEpoch, 900, 0, 0, currentWorker, { oldWorker });
	ASSERT(collectOldBackupWorkers(zeroTags).empty());

	LogSystemConfig missing = makeOldBackupConfiguration(currentEpoch, oldEpoch, 900, 1, 0, currentWorker, {});
	missing.oldTLogs[0].tLogs[0].backupWorkers.emplace_back(UID(99, 1));
	ASSERT(collectOldBackupWorkers(missing).empty());

	LogSystemConfig noOldGenerations(currentEpoch);
	TLogSet currentSet;
	currentSet.backupWorkers.emplace_back(currentWorker);
	noOldGenerations.tLogs.push_back(currentSet);
	ASSERT(collectOldBackupWorkers(noOldGenerations).empty());
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/resolvesExactPersistedClassicTag") {
	constexpr LogEpoch oldEpoch = 39;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x05050505), 1), UID(5, 1), "current");
	BackupInterface firstOldWorker = makeBackupWorker(NetworkAddress(IPAddress(0x06060606), 1), UID(6, 1), "first");
	BackupInterface failedWorker = makeBackupWorker(NetworkAddress(IPAddress(0x07070707), 1), UID(7, 1), "failed");
	LogSystemConfig config =
	    makeOldBackupConfiguration(oldEpoch + 1, oldEpoch, 600, 2, 0, currentWorker, { firstOldWorker, failedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	Tag firstTag(tagLocalityLogRouter, 0);
	Tag failedTag(tagLocalityLogRouter, 1);
	std::map<UID, WorkerBackupStatus> progress;
	progress.emplace(firstOldWorker.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 2));
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 349, failedTag, 2));
	std::map<Tag, Version> unfinished{ { firstTag, 200 }, { failedTag, 350 } };

	Optional<Tag> resolved = resolveOldBackupWorkerTag(oldWorkers[1], oldWorkers, progress, unfinished);
	ASSERT(resolved.present());
	ASSERT(resolved.get() == failedTag);
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/resolvesExactPersistedRangePartitionedTag") {
	constexpr LogEpoch oldEpoch = 49;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x08080808), 1), UID(8, 1), "current");
	BackupInterface failedWorker = makeBackupWorker(NetworkAddress(IPAddress(0x09090909), 1), UID(9, 1), "failed");
	LogSystemConfig config =
	    makeOldBackupConfiguration(oldEpoch + 1, oldEpoch, 900, 0, 2, currentWorker, { failedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	Tag failedTag(tagLocalityRangePartitionedBackup, 1);
	std::map<UID, WorkerBackupStatus> progress;
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 450, failedTag, 2));
	std::map<Tag, Version> unfinished{ { Tag(tagLocalityRangePartitionedBackup, 0), 100 }, { failedTag, 451 } };

	Optional<Tag> resolved = resolveOldBackupWorkerTag(oldWorkers[0], oldWorkers, progress, unfinished);
	ASSERT(resolved.present());
	ASSERT(resolved.get() == failedTag);
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/infersDistinctMissingProgressTags") {
	constexpr LogEpoch oldEpoch = 59;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x0a0a0a0a), 1), UID(10, 1), "current");
	BackupInterface knownWorker = makeBackupWorker(NetworkAddress(IPAddress(0x0b0b0b0b), 1), UID(11, 1), "known");
	BackupInterface firstUnsavedWorker =
	    makeBackupWorker(NetworkAddress(IPAddress(0x0c0c0c0c), 1), UID(12, 1), "first-unsaved");
	BackupInterface secondUnsavedWorker =
	    makeBackupWorker(NetworkAddress(IPAddress(0x0d0d0d0d), 1), UID(13, 1), "second-unsaved");
	LogSystemConfig config = makeOldBackupConfiguration(
	    oldEpoch + 1, oldEpoch, 700, 3, 0, currentWorker, { knownWorker, firstUnsavedWorker, secondUnsavedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	Tag knownTag(tagLocalityLogRouter, 0);
	Tag firstUnsavedTag(tagLocalityLogRouter, 1);
	Tag secondUnsavedTag(tagLocalityLogRouter, 2);
	std::map<UID, WorkerBackupStatus> progress;
	progress.emplace(knownWorker.id(), WorkerBackupStatus(oldEpoch, 150, knownTag, 3));
	std::map<Tag, Version> unfinished{ { knownTag, 151 }, { firstUnsavedTag, 100 }, { secondUnsavedTag, 100 } };

	Optional<Tag> firstResolved = resolveOldBackupWorkerTag(oldWorkers[1], oldWorkers, progress, unfinished);
	Optional<Tag> secondResolved = resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished);
	ASSERT(firstResolved.present());
	ASSERT(secondResolved.present());
	ASSERT(firstResolved.get() == firstUnsavedTag);
	ASSERT(secondResolved.get() == secondUnsavedTag);
	ASSERT(firstResolved.get() != secondResolved.get());
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/ambiguousMissingProgressFailsClosed") {
	constexpr LogEpoch oldEpoch = 89;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x12121212), 1), UID(18, 1), "current");
	BackupInterface firstUnsavedWorker =
	    makeBackupWorker(NetworkAddress(IPAddress(0x13131313), 1), UID(19, 1), "first-unsaved");
	BackupInterface failedWorker =
	    makeBackupWorker(NetworkAddress(IPAddress(0x14141414), 1), UID(20, 1), "failed-unsaved");
	LogSystemConfig config = makeOldBackupConfiguration(
	    oldEpoch + 1, oldEpoch, 800, 2, 0, currentWorker, { firstUnsavedWorker, failedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	std::map<Tag, Version> unfinished{ { Tag(tagLocalityLogRouter, 1), 300 } };
	std::map<UID, WorkerBackupStatus> progress;

	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[1], oldWorkers, progress, unfinished).present());
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/rejectsInvalidSiblingProgressAndDuplicateTags") {
	constexpr LogEpoch oldEpoch = 99;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x15151515), 1), UID(21, 1), "current");
	BackupInterface firstSibling = makeBackupWorker(NetworkAddress(IPAddress(0x16161616), 1), UID(22, 1), "first");
	BackupInterface secondSibling = makeBackupWorker(NetworkAddress(IPAddress(0x17171717), 1), UID(23, 1), "second");
	BackupInterface failedWorker = makeBackupWorker(NetworkAddress(IPAddress(0x18181818), 1), UID(24, 1), "failed");
	LogSystemConfig config = makeOldBackupConfiguration(
	    oldEpoch + 1, oldEpoch, 900, 3, 0, currentWorker, { firstSibling, secondSibling, failedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	Tag firstTag(tagLocalityLogRouter, 0);
	Tag secondTag(tagLocalityLogRouter, 1);
	Tag failedTag(tagLocalityLogRouter, 2);
	std::map<Tag, Version> unfinished{ { firstTag, 200 }, { secondTag, 300 }, { failedTag, 400 } };
	std::map<UID, WorkerBackupStatus> progress;

	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch - 1, 199, firstTag, 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(),
	                 WorkerBackupStatus(oldEpoch, 199, Tag(tagLocalityRangePartitionedBackup, 0), 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 4));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, firstTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch - 1, 199, firstTag, 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 399, failedTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(),
	                 WorkerBackupStatus(oldEpoch, 199, Tag(tagLocalityRangePartitionedBackup, 0), 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 399, failedTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 4));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 399, failedTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 399, secondTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, firstTag, 3));
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 399, failedTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[2], oldWorkers, progress, unfinished).present());
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/validatesPersistedProgressBeforeAggregation") {
	constexpr LogEpoch oldEpoch = 109;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x19191919), 1), UID(25, 1), "current");
	BackupInterface firstSibling = makeBackupWorker(NetworkAddress(IPAddress(0x1a1a1a1a), 1), UID(26, 1), "first");
	BackupInterface secondSibling = makeBackupWorker(NetworkAddress(IPAddress(0x1b1b1b1b), 1), UID(27, 1), "second");
	BackupInterface failedWorker = makeBackupWorker(NetworkAddress(IPAddress(0x1c1c1c1c), 1), UID(28, 1), "failed");
	LogSystemConfig config = makeOldBackupConfiguration(
	    oldEpoch + 1, oldEpoch, 1'000, 3, 0, currentWorker, { firstSibling, secondSibling, failedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	Tag firstTag(tagLocalityLogRouter, 0);
	Tag secondTag(tagLocalityLogRouter, 1);
	Tag failedTag(tagLocalityLogRouter, 2);
	std::map<UID, WorkerBackupStatus> progress;
	progress.emplace(firstSibling.id(), WorkerBackupStatus(oldEpoch, 199, firstTag, 3));
	progress.emplace(secondSibling.id(), WorkerBackupStatus(oldEpoch, 299, secondTag, 3));
	ASSERT(validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));

	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 399, failedTag, 3));
	ASSERT(validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));

	progress[firstSibling.id()] = WorkerBackupStatus(oldEpoch - 1, 199, firstTag, 3);
	ASSERT(!validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));

	progress[firstSibling.id()] = WorkerBackupStatus(oldEpoch, 199, Tag(tagLocalityRangePartitionedBackup, 0), 3);
	ASSERT(!validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));

	progress[firstSibling.id()] = WorkerBackupStatus(oldEpoch, 199, firstTag, 4);
	ASSERT(!validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));

	progress[firstSibling.id()] = WorkerBackupStatus(oldEpoch, 199, secondTag, 3);
	ASSERT(!validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));

	progress[firstSibling.id()] = WorkerBackupStatus(oldEpoch, 199, failedTag, 3);
	ASSERT(!validateOldBackupWorkerProgress(oldWorkers[2], oldWorkers, progress));
	return Void();
}

TEST_CASE("/fdbserver/clustercontroller/oldBackupWorker/rejectsStaleFinishedAndWrongFlavorTags") {
	constexpr LogEpoch oldEpoch = 69;
	BackupInterface currentWorker = makeBackupWorker(NetworkAddress(IPAddress(0x0e0e0e0e), 1), UID(14, 1), "current");
	BackupInterface failedWorker = makeBackupWorker(NetworkAddress(IPAddress(0x0f0f0f0f), 1), UID(15, 1), "failed");
	LogSystemConfig config =
	    makeOldBackupConfiguration(oldEpoch + 1, oldEpoch, 750, 2, 0, currentWorker, { failedWorker });
	std::vector<OldBackupWorkerInfo> oldWorkers = collectOldBackupWorkers(config);
	Tag classicTag(tagLocalityLogRouter, 1);
	Tag rangeTag(tagLocalityRangePartitionedBackup, 1);
	std::map<Tag, Version> unfinished{ { classicTag, 300 } };
	std::map<UID, WorkerBackupStatus> progress;

	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch - 1, 299, classicTag, 2));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[0], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 299, rangeTag, 2));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[0], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 299, classicTag, 3));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[0], oldWorkers, progress, unfinished).present());

	progress.clear();
	progress.emplace(failedWorker.id(), WorkerBackupStatus(oldEpoch, 749, classicTag, 2));
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[0], oldWorkers, progress, {}).present());

	progress.clear();
	ASSERT(!resolveOldBackupWorkerTag(oldWorkers[0], oldWorkers, progress, {}).present());
	return Void();
}
