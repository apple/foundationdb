/*
 * LogSystemRecoveryTests.cpp
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

#include "fdbserver/logsystem/LogSystem.h"
#include "fdbserver/logsystem/LogSystemConsumer.h"
#include "flow/UnitTest.h"

namespace {

Reference<LogSet> makeSingleLogSet(const std::vector<TLogInterface>& tlogs, bool isLocal = true) {
	auto logSet = makeReference<LogSet>();
	logSet->isLocal = isLocal;
	for (const auto& tlog : tlogs) {
		logSet->logServers.push_back(
		    makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(OptionalInterface<TLogInterface>(tlog)));
	}
	return logSet;
}

BackupInterface makeBackupWorkerInterface(NetworkAddress const& address, UID id, std::string const& processId) {
	LocalityData locality;
	locality.set(LocalityData::keyProcessId, Standalone<StringRef>(processId));
	BackupInterface worker(locality);
	worker.waitFailure = RequestStream<ReplyPromise<Void>>(Endpoint({ address }, id));
	return worker;
}

void addBackupWorker(Reference<LogSet> const& logSet, BackupInterface const& worker) {
	logSet->backupWorkers.push_back(
	    makeReference<AsyncVar<OptionalInterface<BackupInterface>>>(OptionalInterface<BackupInterface>(worker)));
}

std::tuple<int, std::vector<TLogLockResult>, bool> makeLogGroupResults(
    int replicationFactor,
    const std::vector<std::vector<UnknownCommittedVersions>>& perTLogUCV,
    const std::vector<TLogInterface>& tlogs,
    bool nonAvailableTLogsCompletePolicy = true,
    const std::vector<Version>& knownCommitted = {}) {
	std::vector<TLogLockResult> lockResults;
	lockResults.reserve(tlogs.size());
	for (int i = 0; i < tlogs.size(); ++i) {
		TLogLockResult result;
		result.logId = tlogs[i].id();
		result.knownCommittedVersion = (i < knownCommitted.size()) ? knownCommitted[i] : 0;
		for (const auto& ucv : perTLogUCV[i]) {
			result.unknownCommittedVersions.push_back(ucv);
		}
		lockResults.push_back(result);
	}
	return std::make_tuple(replicationFactor, std::move(lockResults), nonAvailableTLogsCompletePolicy);
}

} // namespace

void forceLinkLogSystemRecoveryTests() {}

TEST_CASE("/LogSystem/GetPseudoPopTag/LogRouterWithoutMappedLocality") {
	LocalityData locality;
	auto logSystem = makeReference<LogSystem>(UID(), locality, LogEpoch(1));
	ASSERT(!logSystem->hasPseudoLocality(tagLocalityLogRouterMapped));

	Tag tag = logSystem->getPseudoPopTag(Tag(tagLocalityLogRouter, 0), ProcessClass::LogRouterClass);
	ASSERT(tag == Tag(tagLocalityLogRouterMapped, 0));
	return Void();
}

TEST_CASE("/LogSystem/PopLogRouter/CurrentGenerationAcceptsPredecessor") {
	constexpr Version generationStart = 100;
	constexpr int8_t remoteTLogLocality = 1;
	LocalityData locality;
	TLogInterface router(locality);
	auto currentSet = makeReference<LogSet>();
	currentSet->locality = remoteTLogLocality;
	currentSet->startVersion = generationStart;
	currentSet->logRouters.push_back(
	    makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(OptionalInterface<TLogInterface>(router)));

	auto logSystem = makeReference<LogSystem>(UID(), locality, LogEpoch(1));
	logSystem->tLogs.push_back(currentSet);
	LogSystemConsumer consumer(logSystem);
	Tag tag(tagLocalityRemoteLog, 0);
	auto routerTag = std::make_pair(router.id(), tag);

	consumer.popLogRouter(generationStart - 2, tag, 0, remoteTLogLocality);
	ASSERT(!logSystem->outstandingPops.contains(routerTag));

	consumer.popLogRouter(generationStart - 1, tag, 0, remoteTLogLocality);
	ASSERT(logSystem->outstandingPops.contains(routerTag));
	ASSERT(logSystem->outstandingPops.at(routerTag).first == generationStart - 1);
	return Void();
}

TEST_CASE("/LogSystem/PeekLogRouter/EmptyOldRangeIsExhausted") {
	LocalityData locality;
	TLogInterface router(locality);
	auto oldSet = makeReference<LogSet>();
	oldSet->logRouters.push_back(
	    makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(OptionalInterface<TLogInterface>(router)));

	auto logSystem = makeReference<LogSystem>(UID(), locality, LogEpoch(1));
	OldLogData old;
	old.epochEnd = 100;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	LogSystemConsumer consumer(logSystem);
	auto cursor = consumer.peekLogRouter(router.id(), old.epochEnd, Tag(tagLocalityLogRouter, 0), false);
	ASSERT(cursor->isExhausted());
	ASSERT(cursor->version().version == old.epochEnd);
	return Void();
}

TEST_CASE("/LogSystem/ReplaceBackupWorker/OldOnlyWorkerWithMoreRetainedTags") {
	constexpr LogEpoch currentEpoch = 10;
	constexpr LogEpoch oldEpoch = 9;
	auto logSystem = makeReference<LogSystem>(UID(), LocalityData(), currentEpoch);
	logSystem->logRouterTags = 1;
	logSystem->oldestBackupEpoch = oldEpoch;

	BackupInterface currentWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x01010101), 1), UID(1, 1), "current");
	BackupInterface firstOldWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x02020202), 1), UID(2, 1), "old-first");
	BackupInterface oldOnlyWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x03030303), 1), UID(3, 1), "old-only");
	BackupInterface replacement =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x04040404), 1), UID(4, 1), "replacement");

	auto currentSet = makeReference<LogSet>();
	addBackupWorker(currentSet, currentWorker);
	logSystem->tLogs.push_back(currentSet);

	auto oldSet = makeReference<LogSet>();
	addBackupWorker(oldSet, firstOldWorker);
	addBackupWorker(oldSet, oldOnlyWorker);
	OldLogData old;
	old.epoch = oldEpoch;
	old.logRouterTags = 2;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	auto observedOldWorker = oldSet->backupWorkers[1];
	Future<Void> interfaceChanged = observedOldWorker->onChange();
	Future<Void> backupChanged = logSystem->backupWorkerChanged.onTrigger();
	ASSERT(logSystem->replaceBackupWorker(oldEpoch, oldOnlyWorker.id(), replacement));
	ASSERT(interfaceChanged.isReady());
	ASSERT(backupChanged.isReady());
	ASSERT_EQ(logSystem->epoch, currentEpoch);
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), oldEpoch);
	ASSERT_EQ(logSystem->logRouterTags, 1);
	ASSERT_EQ(logSystem->oldLogData[0].logRouterTags, 2);
	ASSERT_EQ(currentSet->backupWorkers.size(), 1);
	ASSERT(currentSet->backupWorkers[0]->get().interf() == currentWorker);
	ASSERT_EQ(oldSet->backupWorkers.size(), 2);
	ASSERT(oldSet->backupWorkers[0]->get().interf() == firstOldWorker);
	ASSERT(oldSet->backupWorkers[1] == observedOldWorker);
	ASSERT(oldSet->backupWorkers[1]->get().interf() == replacement);
	return Void();
}

TEST_CASE("/LogSystem/ReplaceBackupWorker/ColocatedCurrentRoleDoesNotChange") {
	constexpr LogEpoch currentEpoch = 20;
	constexpr LogEpoch oldEpoch = 19;
	NetworkAddress sharedAddress(IPAddress(0x05050505), 1);
	BackupInterface currentWorker = makeBackupWorkerInterface(sharedAddress, UID(5, 1), "shared-worker");
	BackupInterface colocatedOldWorker = makeBackupWorkerInterface(sharedAddress, UID(5, 2), "shared-worker");
	BackupInterface replacement =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x06060606), 1), UID(6, 1), "replacement");

	auto logSystem = makeReference<LogSystem>(UID(), LocalityData(), currentEpoch);
	logSystem->oldestBackupEpoch = oldEpoch;
	const bool recoveryWasComplete = logSystem->recoveryCompleteWrittenToCoreState.get();
	auto currentSet = makeReference<LogSet>();
	addBackupWorker(currentSet, currentWorker);
	logSystem->tLogs.push_back(currentSet);
	auto oldSet = makeReference<LogSet>();
	addBackupWorker(oldSet, colocatedOldWorker);
	OldLogData old;
	old.epoch = oldEpoch;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	auto observedOldWorker = oldSet->backupWorkers[0];
	Future<Void> interfaceChanged = observedOldWorker->onChange();
	ASSERT(currentWorker.address() == colocatedOldWorker.address());
	ASSERT(currentWorker.locality.processId() == colocatedOldWorker.locality.processId());
	ASSERT(currentWorker.id() != colocatedOldWorker.id());
	ASSERT(logSystem->replaceBackupWorker(oldEpoch, colocatedOldWorker.id(), replacement));
	ASSERT(interfaceChanged.isReady());
	ASSERT_EQ(logSystem->epoch, currentEpoch);
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), oldEpoch);
	ASSERT_EQ(logSystem->recoveryCompleteWrittenToCoreState.get(), recoveryWasComplete);
	ASSERT_EQ(currentSet->backupWorkers.size(), 1);
	ASSERT(currentSet->backupWorkers[0]->get().interf() == currentWorker);
	ASSERT_EQ(oldSet->backupWorkers.size(), 1);
	ASSERT(oldSet->backupWorkers[0] == observedOldWorker);
	ASSERT(oldSet->backupWorkers[0]->get().interf() == replacement);
	return Void();
}

TEST_CASE("/LogSystem/ReplaceBackupWorker/StaleAndCurrentGenerationRequestsFailClosed") {
	constexpr LogEpoch currentEpoch = 30;
	constexpr LogEpoch oldEpoch = 29;
	BackupInterface currentWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x07070707), 1), UID(7, 1), "current");
	BackupInterface oldWorker = makeBackupWorkerInterface(NetworkAddress(IPAddress(0x08080808), 1), UID(8, 1), "old");
	BackupInterface replacement =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x09090909), 1), UID(9, 1), "replacement");

	auto logSystem = makeReference<LogSystem>(UID(), LocalityData(), currentEpoch);
	logSystem->oldestBackupEpoch = oldEpoch;
	auto currentSet = makeReference<LogSet>();
	addBackupWorker(currentSet, currentWorker);
	logSystem->tLogs.push_back(currentSet);
	auto oldSet = makeReference<LogSet>();
	addBackupWorker(oldSet, oldWorker);
	OldLogData old;
	old.epoch = oldEpoch;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	auto observedOldWorker = oldSet->backupWorkers[0];
	Future<Void> interfaceChanged = observedOldWorker->onChange();
	Future<Void> backupChanged = logSystem->backupWorkerChanged.onTrigger();
	ASSERT(!logSystem->replaceBackupWorker(currentEpoch, currentWorker.id(), replacement));
	ASSERT(!logSystem->replaceBackupWorker(oldEpoch - 1, oldWorker.id(), replacement));
	ASSERT(!logSystem->replaceBackupWorker(oldEpoch, UID(99, 1), replacement));
	ASSERT(!logSystem->replaceBackupWorker(oldEpoch, currentWorker.id(), replacement));
	ASSERT(!interfaceChanged.isReady());
	ASSERT(!backupChanged.isReady());
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), oldEpoch);
	ASSERT(currentSet->backupWorkers[0]->get().interf() == currentWorker);
	ASSERT(oldSet->backupWorkers[0]->get().interf() == oldWorker);

	ASSERT(logSystem->replaceBackupWorker(oldEpoch, oldWorker.id(), replacement));
	ASSERT(interfaceChanged.isReady());
	ASSERT(backupChanged.isReady());
	ASSERT(oldSet->backupWorkers[0] == observedOldWorker);
	ASSERT(oldSet->backupWorkers[0]->get().interf() == replacement);
	return Void();
}

TEST_CASE("/LogSystem/ReplaceBackupWorker/StaleCompletionCannotRemoveReplacement") {
	constexpr LogEpoch currentEpoch = 40;
	constexpr LogEpoch oldEpoch = 39;
	BackupInterface currentWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x0a0a0a0a), 1), UID(10, 1), "current");
	BackupInterface oldWorker = makeBackupWorkerInterface(NetworkAddress(IPAddress(0x0b0b0b0b), 1), UID(11, 1), "old");
	BackupInterface replacement =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x0c0c0c0c), 1), UID(12, 1), "replacement");

	auto logSystem = makeReference<LogSystem>(UID(), LocalityData(), currentEpoch);
	logSystem->oldestBackupEpoch = oldEpoch;
	auto currentSet = makeReference<LogSet>();
	addBackupWorker(currentSet, currentWorker);
	logSystem->tLogs.push_back(currentSet);
	auto oldSet = makeReference<LogSet>();
	addBackupWorker(oldSet, oldWorker);
	OldLogData old;
	old.epoch = oldEpoch;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	ASSERT(logSystem->replaceBackupWorker(oldEpoch, oldWorker.id(), replacement));
	ASSERT(!logSystem->removeBackupWorker(BackupWorkerDoneRequest(oldWorker.id(), oldEpoch)));
	ASSERT_EQ(oldSet->backupWorkers.size(), 1);
	ASSERT(oldSet->backupWorkers[0]->get().interf() == replacement);
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), oldEpoch);

	ASSERT(logSystem->removeBackupWorker(BackupWorkerDoneRequest(replacement.id(), oldEpoch)));
	ASSERT(oldSet->backupWorkers.empty());
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), currentEpoch);
	ASSERT(currentSet->backupWorkers[0]->get().interf() == currentWorker);
	return Void();
}

TEST_CASE("/LogSystem/ReplaceBackupWorker/CompletedReplacementIsNotInstalled") {
	constexpr LogEpoch currentEpoch = 45;
	constexpr LogEpoch oldEpoch = 44;
	BackupInterface currentWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x11111111), 1), UID(17, 1), "current");
	BackupInterface oldWorker = makeBackupWorkerInterface(NetworkAddress(IPAddress(0x12121212), 1), UID(18, 1), "old");
	BackupInterface completedReplacement =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x13131313), 1), UID(19, 1), "completed-replacement");

	auto logSystem = makeReference<LogSystem>(UID(), LocalityData(), currentEpoch);
	logSystem->oldestBackupEpoch = oldEpoch;
	auto currentSet = makeReference<LogSet>();
	addBackupWorker(currentSet, currentWorker);
	logSystem->tLogs.push_back(currentSet);
	auto oldSet = makeReference<LogSet>();
	addBackupWorker(oldSet, oldWorker);
	OldLogData old;
	old.epoch = oldEpoch;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	ASSERT(!logSystem->removeBackupWorker(BackupWorkerDoneRequest(completedReplacement.id(), oldEpoch)));
	ASSERT(logSystem->removedBackupWorkers.contains(completedReplacement.id()));
	Future<Void> backupChanged = logSystem->backupWorkerChanged.onTrigger();
	ASSERT(!logSystem->replaceBackupWorker(oldEpoch, oldWorker.id(), completedReplacement));
	ASSERT(backupChanged.isReady());
	ASSERT(!logSystem->removedBackupWorkers.contains(completedReplacement.id()));
	ASSERT(oldSet->backupWorkers.empty());
	ASSERT_EQ(logSystem->epoch, currentEpoch);
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), currentEpoch);
	ASSERT_EQ(currentSet->backupWorkers.size(), 1);
	ASSERT(currentSet->backupWorkers[0]->get().interf() == currentWorker);
	return Void();
}

TEST_CASE("/LogSystem/ReplaceBackupWorker/RangePartitionedRetainedGeneration") {
	constexpr LogEpoch currentEpoch = 50;
	constexpr LogEpoch oldEpoch = 49;
	BackupInterface currentWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x0d0d0d0d), 1), UID(13, 1), "current-range");
	BackupInterface firstOldWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x0e0e0e0e), 1), UID(14, 1), "old-range-first");
	BackupInterface failedOldWorker =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x0f0f0f0f), 1), UID(15, 1), "old-range-failed");
	BackupInterface replacement =
	    makeBackupWorkerInterface(NetworkAddress(IPAddress(0x10101010), 1), UID(16, 1), "range-replacement");

	auto logSystem = makeReference<LogSystem>(UID(), LocalityData(), currentEpoch);
	logSystem->oldestBackupEpoch = oldEpoch;
	logSystem->rangePartitionedBackupWorkerTags = 1;
	auto currentSet = makeReference<LogSet>();
	addBackupWorker(currentSet, currentWorker);
	logSystem->tLogs.push_back(currentSet);
	auto oldSet = makeReference<LogSet>();
	addBackupWorker(oldSet, firstOldWorker);
	addBackupWorker(oldSet, failedOldWorker);
	OldLogData old;
	old.epoch = oldEpoch;
	old.rangePartitionedBackupWorkerTags = 2;
	old.tLogs.push_back(oldSet);
	logSystem->oldLogData.push_back(old);

	ASSERT(logSystem->replaceBackupWorker(oldEpoch, failedOldWorker.id(), replacement));
	ASSERT_EQ(logSystem->epoch, currentEpoch);
	ASSERT_EQ(logSystem->getOldestBackupEpoch(), oldEpoch);
	ASSERT_EQ(logSystem->rangePartitionedBackupWorkerTags, 1);
	ASSERT_EQ(logSystem->oldLogData[0].rangePartitionedBackupWorkerTags, 2);
	ASSERT(currentSet->backupWorkers[0]->get().interf() == currentWorker);
	ASSERT(oldSet->backupWorkers[0]->get().interf() == firstOldWorker);
	ASSERT(oldSet->backupWorkers[1]->get().interf() == replacement);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/Simple") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Construct two local tLogs backed by a single LogSet.
	// Both tLogs have known committed version 100 and report a higher durable
	// version 110 that was sent to both log servers.
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv }, { ucv } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "SimpleTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	if (recoverVersion != 110) {
		TraceEvent(SevError, "SimpleTestRecoverVersionFailed").detail("Expected", 110).detail("Got", recoverVersion);
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/FallbackToMaxKCV") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// When no unknown committed versions are reported, should fall back to maxKCV
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	auto logGroupResults = makeLogGroupResults(2, { {}, {} }, { tlogA, tlogB }, true, { 80, 90 });

	Version minDV = 70;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 90) {
		TraceEvent(SevError, "FallbackTestMaxKCVFailed").detail("Expected", 90).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 90);

	if (recoverVersion != 90) {
		TraceEvent(SevError, "FallbackTestRecoverVersionFailed").detail("Expected", 90).detail("Got", recoverVersion);
	}
	ASSERT(recoverVersion == 90);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/HaltOnMissingDelivery") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// When an available tLog didn't receive a version, recovery should halt at the previous version
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucvLate(120, 110, std::vector<uint16_t>{ 0, 1 });
	// Only tlogA reports the 120 version (tlogB missed it).
	auto logGroupResults = makeLogGroupResults(2, { { ucv, ucvLate }, { ucv } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "MissingDeliveryTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Because not all available tLogs received 120, the recovery version should stay at 110.
	if (recoverVersion != 110) {
		TraceEvent(SevError, "MissingDeliveryTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "tlogB did not receive version 120");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/PolicyNotSatisfied") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// When a version was sent to both tLogs but only received by one (insufficient for RF=2)
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucv2(120, 110, std::vector<uint16_t>{ 0, 1 });
	// Version 120 was sent to BOTH tLogs but only tlogA received it.
	// With replication factor 2, we need both to receive it.
	auto logGroupResults = makeLogGroupResults(2, { { ucv, ucv2 }, { ucv } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "PolicyNotSatisfiedTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	if (recoverVersion != 110) {
		TraceEvent(SevError, "PolicyNotSatisfiedTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "Version 120 sent to both tLogs but only received by 1 (RF=2)");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/MinDVRespected") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that recovery version respects maxKCV when minDV < maxKCV
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(95, 90, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv }, { ucv } }, { tlogA, tlogB }, true, { 90, 90 });

	Version minDV = 80;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 90) {
		TraceEvent(SevError, "MinDVRespectedTestMaxKCVFailed").detail("Expected", 90).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 90);

	if (recoverVersion != 95) {
		TraceEvent(SevError, "MinDVRespectedTestRecoverVersionFailed")
		    .detail("Expected", 95)
		    .detail("Got", recoverVersion)
		    .detail("MinDV", minDV)
		    .detail("MaxKCV", maxKCV);
	}
	ASSERT(recoverVersion == 95);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/BrokenChain") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that recovery halts when prevVersion chain is broken
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	// Version 110 and 120 sent to both, but 120's prevVersion != 110 (broken chain)
	UnknownCommittedVersions ucv110(110, 100, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucv120(120, 115, std::vector<uint16_t>{ 0, 1 }); // prevVersion=115, not 110!
	auto logGroupResults =
	    makeLogGroupResults(2, { { ucv110, ucv120 }, { ucv110, ucv120 } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "BrokenChainTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Should stop at 110 because prevVersion chain breaks at 120
	if (recoverVersion != 110) {
		TraceEvent(SevError, "BrokenChainTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "Version 120 has prevVersion=115, expected 110");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/MultipleLogSets") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests recovery with multiple LogSets (primary + satellite)
	LocalityData locality;
	TLogInterface primary1(locality), primary2(locality);
	TLogInterface satellite1(locality), satellite2(locality);

	// Two LogSets: primary (local) + satellite (non-local)
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ primary1, primary2 }, true),
		                                       makeSingleLogSet({ satellite1, satellite2 }, false) };

	// Only the 2 primary tLogs report version 110 (satellite LogSet is non-local and not in logGroupResults)
	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv }, { ucv } }, { primary1, primary2 }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "MultipleLogSetsTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	if (recoverVersion != 110) {
		TraceEvent(SevError, "MultipleLogSetsTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("NumLogSets", logServers.size());
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/PartialAvailabilityPolicyFail") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that available tLogs must satisfy replication policy
	LocalityData locality;
	TLogInterface tlogA(locality), tlogB(locality), tlogC(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB, tlogC }) };

	// Version 120 sent to all 3, but only 2 of 3 received it (not enough for RF=3)
	UnknownCommittedVersions ucv110(110, 100, std::vector<uint16_t>{ 0, 1, 2 });
	UnknownCommittedVersions ucv120(120, 110, std::vector<uint16_t>{ 0, 1, 2 });
	// Only tlogA and tlogB report receiving 120
	auto logGroupResults = makeLogGroupResults(
	    3, { { ucv110, ucv120 }, { ucv110, ucv120 }, { ucv110 } }, { tlogA, tlogB, tlogC }, false, { 100, 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "PartialAvailabilityTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Should stay at 110 because 120 doesn't satisfy RF=3
	if (recoverVersion != 110) {
		TraceEvent(SevError, "PartialAvailabilityTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "Only 2 of 3 tLogs received version 120 (RF=3)");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/VersionsBelowMaxKCV") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that versions <= maxKCV are filtered out
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	// Report version 80 (below maxKCV=100), should be ignored
	UnknownCommittedVersions ucv80(80, 70, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv80 }, { ucv80 } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "VersionsBelowMaxKCVTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Should fall back to maxKCV since all UCVs are <= maxKCV
	if (recoverVersion != 100) {
		TraceEvent(SevError, "VersionsBelowMaxKCVTestRecoverVersionFailed")
		    .detail("Expected", 100)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "All UCVs below maxKCV should be filtered");
	}
	ASSERT(recoverVersion == 100);
	return Void();
}

TEST_CASE("/LogSystem/GetRecoverVersionUnicast/RandomVersionsPartialDelivery") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that recovery handles random versions in range (maxKCV, highest_version) that:
	// 1. Are not reported at all (missing from UCVs)
	// 2. Are only received by a subset of logs (partial delivery)
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	TLogInterface tlogC(locality);
	TLogInterface tlogD(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB, tlogC, tlogD }) };

	// Setup: maxKCV=100, potential versions 110, 112, 115, 118, 120, 125
	// Version 110: received by tlogA and tlogB (full delivery)
	// Version 112: sent to tlogC and tlogD, but NOT REPORTED at all (missing from UCVs)
	// Version 115: only received by tlogA (partial delivery - indicated by tLogLocIds={0})
	// Version 118: sent to tlogC and tlogD, but NOT REPORTED at all (missing from UCVs)
	// Version 120: received by tlogA and tlogB (full delivery)
	// Version 125: only received by tlogA (partial delivery - indicated by tLogLocIds={0})

	UnknownCommittedVersions ucv110(110, 100, std::vector<uint16_t>{ 0, 1 });
	// Version 112 is missing - not in any UCV list
	UnknownCommittedVersions ucv115(115, 112, std::vector<uint16_t>{ 0 }); // Only tlogA (loc 0)
	// Version 118 is missing - not in any UCV list
	UnknownCommittedVersions ucv120(120, 118, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucv125(125, 120, std::vector<uint16_t>{ 0 }); // Only tlogA (loc 0)

	// tlogA reports versions 110, 115, 120, 125
	// tlogB only reports versions 110, 120 (missing 115, 125)
	// tlogC doesn't report any versions
	// tlogD doesn't report any versions
	auto logGroupResults = makeLogGroupResults(2,
	                                           { { ucv110, ucv115, ucv120, ucv125 }, { ucv110, ucv120 }, {}, {} },
	                                           { tlogA, tlogB, tlogC, tlogD },
	                                           true,
	                                           { 100, 100, 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "RandomVersionsPartialDeliveryTestMaxKCVFailed")
		    .detail("Expected", 100)
		    .detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Recovery should stop at 110 because:
	// - 110 was received by tlogA and tlogB (satisfies RF=2)
	// - 112 was not received by tlogC and tlogD
	// - 115 was only received by tlogA (tLogLocIds={0}, doesn't satisfy RF=2)
	// Even though 120 was received by both tlogA and tlogB, the prevVersion chain requires 112, 115, and 118
	if (recoverVersion != 110) {
		TraceEvent(SevError, "RandomVersionsPartialDeliveryTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason",
		            "Missing versions 112 and 118, break recovery before 120. "
		            "Version 115 received by only tlogA (subset), also breaks recovery before 120. ");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}
