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
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "flow/CoroUtils.h"
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

Reference<LogSet> makeRemotePrefixLogSet(const std::vector<TLogInterface>& tlogs,
                                         bool isLocal,
                                         int8_t locality,
                                         Version startVersion) {
	auto logSet = makeSingleLogSet(tlogs, isLocal);
	logSet->locality = locality;
	logSet->startVersion = startVersion;
	logSet->tLogVersion = TLogVersion::V6;
	logSet->tLogReplicationFactor = 1;
	logSet->tLogPolicy = makeReference<PolicyOne>();
	for (const auto& tlog : tlogs) {
		logSet->tLogLocalities.push_back(tlog.filteredLocality);
	}
	return logSet;
}

TLogInterface makeRemotePrefixRouterClient(TLogInterface router) {
	// Streaming health checks and reply backpressure require transport-backed peek streams.
	router.peekMessages = RequestStream<TLogPeekRequest>(router.peekMessages.getEndpoint());
	router.peekStreamMessages = RequestStream<TLogPeekStreamRequest>(router.peekStreamMessages.getEndpoint());
	return router;
}

Reference<LogSystem> makeLaggingRemoteLogSystem(const std::vector<TLogInterface>& remoteLogs,
                                                const TLogInterface& oldRouter,
                                                const TLogInterface& currentRouter) {
	constexpr LogEpoch epoch = 2;
	LocalityData locality;
	auto logSystem = makeReference<LogSystem>(UID(), locality, epoch);
	logSystem->logSystemType = LogSystemType::tagPartitioned;
	logSystem->expectedLogSets = 2;
	logSystem->oldestBackupEpoch = epoch;
	logSystem->repopulateRegionAntiQuorum = 1;
	logSystem->recoveryComplete = Void();
	logSystem->remoteRecovery = Void();
	logSystem->remoteRecoveryComplete = Never();
	logSystem->hasRemoteServers = true;
	logSystem->logRouterTags = 1;
	logSystem->tLogs.push_back(makeRemotePrefixLogSet({ TLogInterface(locality) }, true, 0, 100));
	auto remote = makeRemotePrefixLogSet(remoteLogs, false, 1, 60);
	remote->logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
	    OptionalInterface<TLogInterface>(makeRemotePrefixRouterClient(currentRouter))));
	logSystem->tLogs.push_back(remote);

	OldLogData old;
	old.epoch = epoch - 1;
	old.epochBegin = 50;
	old.epochEnd = 100;
	old.recoverAt = 109;
	old.logRouterTags = 1;
	old.tLogs.push_back(makeRemotePrefixLogSet({ TLogInterface(locality) }, true, 0, 50));
	auto oldRemote = makeRemotePrefixLogSet({ TLogInterface(locality) }, false, 1, 60);
	oldRemote->logRouters.push_back(makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(
	    OptionalInterface<TLogInterface>(makeRemotePrefixRouterClient(oldRouter))));
	old.tLogs.push_back(oldRemote);
	logSystem->oldLogData.push_back(old);
	// Make the ordinary generation-purge criteria eligible so the prefix barrier is the only retention gate.
	logSystem->recoveredVersion->set(old.recoverAt + 1);
	logSystem->remoteRecoveredVersion->set(old.recoverAt + 1);
	return logSystem;
}

DBCoreState makePendingRemotePrefixCoreState(const Reference<LogSystem>& logSystem) {
	DBCoreState pendingState;
	logSystem->toCoreState(pendingState);
	pendingState.recoveryCount = logSystem->epoch;
	ASSERT(!pendingState.oldTLogData.empty());
	ASSERT_EQ(pendingState.oldTLogData.size(), logSystem->oldLogData.size());
	ASSERT_EQ(pendingState.tLogs.size(), logSystem->expectedLogSets);
	const auto oldGenerations = pendingState.oldTLogData;
	logSystem->purgeOldRecoveredGenerationsCoreState(pendingState);
	ASSERT(pendingState.oldTLogData == oldGenerations);
	logSystem->coreStateWritten(pendingState);
	ASSERT(logSystem->remoteLogsWrittenToCoreState);
	ASSERT(!logSystem->recoveryCompleteWrittenToCoreState.get());
	return pendingState;
}

DBCoreState makeRecoveredRemotePrefixCoreState(const Reference<LogSystem>& logSystem) {
	DBCoreState finalState;
	logSystem->toCoreState(finalState);
	finalState.recoveryCount = logSystem->epoch;
	ASSERT(finalState.oldTLogData.empty());
	ASSERT_EQ(finalState.tLogs.size(), logSystem->expectedLogSets);
	logSystem->coreStateWritten(finalState);
	ASSERT(logSystem->recoveryCompleteWrittenToCoreState.get());
	return finalState;
}

void assertRemotePrefixCoreStateError(const Reference<LogSystem>& logSystem, int errorCode) {
	Optional<Error> error;
	try {
		DBCoreState state;
		logSystem->toCoreState(state);
	} catch (Error& e) {
		error = e;
	}
	ASSERT(error.present());
	ASSERT_EQ(error.get().code(), errorCode);
	ASSERT(!logSystem->recoveryCompleteWrittenToCoreState.get());
}

TLogQueuingMetricsReply makeRemotePrefixMetricsReply(Version version) {
	TLogQueuingMetricsReply reply{};
	reply.localTime = now();
	reply.instanceID = 1;
	reply.v = version;
	return reply;
}

Future<Void> serveRemotePrefixMetrics(TLogInterface tlog,
                                      Reference<AsyncVar<Version>> version,
                                      PromiseStream<Version> reports) {
	while (true) {
		TLogQueuingMetricsRequest req = co_await tlog.getQueuingMetrics.getFuture();
		const Version reported = version->get();
		req.reply.send(makeRemotePrefixMetricsReply(reported));
		reports.send(reported);
	}
}

TLogPeekReply makeRemotePrefixPeekReply(Version begin, Optional<Version> requestedEnd, Version end) {
	TLogPeekReply reply;
	reply.begin = begin;
	reply.end = std::min(end, requestedEnd.orDefault(end));
	ASSERT_LT(begin, reply.end);
	reply.popped = begin;
	reply.maxKnownVersion = reply.end - 1;
	reply.minKnownCommittedVersion = reply.end - 1;
	return reply;
}

Future<Void> serveRemotePrefixRouter(TLogInterface router,
                                     Version begin,
                                     Version end,
                                     PromiseStream<Version> requests) {
	while (true) {
		co_await Choose()
		    .When(router.peekMessages.getFuture(),
		          [&](const TLogPeekRequest& req) {
			          ASSERT_GE(req.begin, begin);
			          req.reply.send(makeRemotePrefixPeekReply(req.begin, req.end, end));
			          requests.send(req.begin);
		          })
		    .When(router.peekStreamMessages.getFuture(),
		          [&](const TLogPeekStreamRequest& req) {
			          ASSERT_GE(req.begin, begin);
			          req.reply.setByteLimit(req.limitBytes);
			          Future<Void> ready = req.reply.onReady();
			          ASSERT(ready.isReady() && !ready.isError());
			          req.reply.send(TLogPeekStreamReply(makeRemotePrefixPeekReply(req.begin, req.end, end)));
			          req.reply.sendError(end_of_stream());
			          requests.send(req.begin);
		          })
		    .run();
	}
}

Future<Void> advanceRemotePrefixCursorTo(Reference<IPeekCursor> cursor, Version end) {
	while (cursor->version().version < end) {
		co_await cursor->getMore();
	}
	co_return;
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

TEST_CASE("/LogSystem/RemoteLogPrefix/TrackerInstallation") {
	constexpr double timeoutSeconds = 30.0;
	LocalityData locality;
	TLogInterface remote(locality);
	auto logSystem = makeLaggingRemoteLogSystem({ remote }, TLogInterface(locality), TLogInterface(locality));
	Reference<LogSet> remoteSet = logSystem->tLogs.back();
	logSystem->tLogs.pop_back();
	Promise<Void> remoteRecruitment;
	logSystem->remoteRecovery = remoteRecruitment.getFuture();

	DBCoreState partialState;
	logSystem->toCoreState(partialState);
	partialState.recoveryCount = logSystem->epoch;
	ASSERT_EQ(partialState.tLogs.size(), 1);
	ASSERT_EQ(partialState.oldTLogData.size(), 1);
	logSystem->coreStateWritten(partialState);
	Future<Void> beforeInstallation = logSystem->onCoreStateChanged();
	ASSERT(!beforeInstallation.isReady());

	// The prefix tracker must be installed before remote recruitment reports completion.
	logSystem->tLogs.push_back(remoteSet);
	Future<Void> prefixDurable = logSystem->onRemoteLogPrefixDurable();
	TLogQueuingMetricsRequest initialRequest =
	    co_await timeoutError(waitAndForward(remote.getQueuingMetrics.getFuture()), timeoutSeconds);
	initialRequest.reply.send(makeRemotePrefixMetricsReply(99));
	ASSERT(!prefixDurable.isReady());
	remoteRecruitment.send(Void());
	co_await timeoutError(beforeInstallation, timeoutSeconds);

	makePendingRemotePrefixCoreState(logSystem);
	Future<Void> afterInstallation = logSystem->onCoreStateChanged();
	ASSERT(!afterInstallation.isReady());
	TLogQueuingMetricsRequest caughtUpRequest =
	    co_await timeoutError(waitAndForward(remote.getQueuingMetrics.getFuture()), timeoutSeconds);
	ASSERT(!afterInstallation.isReady());
	caughtUpRequest.reply.send(makeRemotePrefixMetricsReply(100));
	co_await timeoutError(afterInstallation, timeoutSeconds);
	co_await timeoutError(prefixDurable, timeoutSeconds);
	ASSERT(!logSystem->remoteRecoveryComplete.isReady());
	makeRecoveredRemotePrefixCoreState(logSystem);
	co_return;
}

TEST_CASE("/LogSystem/RemoteLogPrefix/RemainsReadable") {
	constexpr double timeoutSeconds = 30.0;
	LocalityData locality;
	TLogInterface remoteA(locality);
	TLogInterface remoteB(locality);
	TLogInterface oldRouter(locality);
	TLogInterface currentRouter(locality);
	auto versionA = makeReference<AsyncVar<Version>>(100);
	auto versionB = makeReference<AsyncVar<Version>>(99);
	PromiseStream<Version> reportsA;
	PromiseStream<Version> reportsB;
	PromiseStream<Version> oldRequests;
	PromiseStream<Version> currentRequests;
	Future<Void> mockActors = waitForAll(std::vector<Future<Void>>{
	    serveRemotePrefixMetrics(remoteA, versionA, reportsA),
	    serveRemotePrefixMetrics(remoteB, versionB, reportsB),
	    serveRemotePrefixRouter(oldRouter, 60, 100, oldRequests),
	    serveRemotePrefixRouter(currentRouter, 100, 110, currentRequests),
	});
	auto logSystem = makeLaggingRemoteLogSystem({ remoteA, remoteB }, oldRouter, currentRouter);
	const auto oldRoles = logSystem->getLogSystemConfig().oldTLogs;
	ASSERT(logSystem->storageRecovered());
	const DBCoreState beforeTracking = makePendingRemotePrefixCoreState(logSystem);
	Future<Void> prefixDurable = logSystem->onRemoteLogPrefixDurable();
	Future<Void> coreStateChanged = logSystem->onCoreStateChanged();
	Future<Void> configChanged = logSystem->onLogSystemConfigChange();
	const Version reportedA = co_await timeoutError(waitAndForward(reportsA.getFuture()), timeoutSeconds);
	const Version reportedB = co_await timeoutError(waitAndForward(reportsB.getFuture()), timeoutSeconds);
	ASSERT_EQ(reportedA, 100);
	ASSERT_EQ(reportedB, 99);
	ASSERT(!prefixDurable.isReady());
	ASSERT(!coreStateChanged.isReady());
	ASSERT(!configChanged.isReady());
	const DBCoreState pendingState = makePendingRemotePrefixCoreState(logSystem);
	ASSERT(pendingState.oldTLogData == beforeTracking.oldTLogData);
	ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);

	// The real remote cursor must still reach the handoff through the old router.
	auto oldConsumer =
	    makeLogSystemFromLogSystemConfig(UID(), locality, logSystem->getLogSystemConfig())->makeConsumer();
	auto oldCursor = oldConsumer->peek(UID(), 60, Optional<Version>(99), Tag(tagLocalityRemoteLog, 0), false);
	co_await timeoutError(advanceRemotePrefixCursorTo(oldCursor, 100) || mockActors, timeoutSeconds);
	const Version oldBegin = co_await timeoutError(waitAndForward(oldRequests.getFuture()), timeoutSeconds);
	ASSERT_EQ(oldBegin, 60);
	ASSERT_EQ(oldCursor->version().version, 100);
	ASSERT(!prefixDurable.isReady());

	versionB->set(100);
	co_await timeoutError(prefixDurable || mockActors, timeoutSeconds);
	co_await timeoutError(coreStateChanged || mockActors, timeoutSeconds);
	ASSERT(!configChanged.isReady());
	ASSERT(!logSystem->remoteRecoveryComplete.isReady());
	ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);
	DBCoreState purgeCandidate = pendingState;
	logSystem->purgeOldRecoveredGenerationsCoreState(purgeCandidate);
	ASSERT(purgeCandidate.oldTLogData.empty());
	makeRecoveredRemotePrefixCoreState(logSystem);
	ASSERT(!configChanged.isReady());
	ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);

	auto currentConsumer =
	    makeLogSystemFromLogSystemConfig(UID(), locality, logSystem->getLogSystemConfig())->makeConsumer();
	auto currentCursor = currentConsumer->peek(UID(), 100, Optional<Version>(109), Tag(tagLocalityRemoteLog, 0), false);
	co_await timeoutError(advanceRemotePrefixCursorTo(currentCursor, 110) || mockActors, timeoutSeconds);
	const Version currentBegin = co_await timeoutError(waitAndForward(currentRequests.getFuture()), timeoutSeconds);
	ASSERT_EQ(currentBegin, 100);
	ASSERT_EQ(currentCursor->version().version, 110);
	co_return;
}

TEST_CASE("/LogSystem/RemoteLogPrefix/InterruptedWait") {
	constexpr double timeoutSeconds = 30.0;
	LocalityData locality;
	{
		TLogInterface remote(locality);
		auto logSystem = makeLaggingRemoteLogSystem({ remote }, TLogInterface(locality), TLogInterface(locality));
		const auto oldRoles = logSystem->getLogSystemConfig().oldTLogs;
		makePendingRemotePrefixCoreState(logSystem);
		Future<Void> prefixDurable = logSystem->onRemoteLogPrefixDurable();
		Future<Void> coreStateChanged = logSystem->onCoreStateChanged();
		TLogQueuingMetricsRequest request =
		    co_await timeoutError(waitAndForward(remote.getQueuingMetrics.getFuture()), timeoutSeconds);
		prefixDurable.cancel();
		ErrorOr<Void> result = co_await timeoutError(errorOr(prefixDurable), timeoutSeconds);
		ASSERT(result.isError() && result.getError().code() == error_code_actor_cancelled);
		ErrorOr<Void> changedResult = co_await timeoutError(errorOr(coreStateChanged), timeoutSeconds);
		ASSERT(changedResult.isError() && changedResult.getError().code() == error_code_actor_cancelled);
		request.reply.send(makeRemotePrefixMetricsReply(100));
		ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);
		assertRemotePrefixCoreStateError(logSystem, error_code_actor_cancelled);
	}
	{
		TLogInterface remote(locality);
		auto logSystem = makeLaggingRemoteLogSystem({ remote }, TLogInterface(locality), TLogInterface(locality));
		const auto oldRoles = logSystem->getLogSystemConfig().oldTLogs;
		makePendingRemotePrefixCoreState(logSystem);
		Future<Void> prefixDurable = logSystem->onRemoteLogPrefixDurable();
		Future<Void> coreStateChanged = logSystem->onCoreStateChanged();
		TLogQueuingMetricsRequest request =
		    co_await timeoutError(waitAndForward(remote.getQueuingMetrics.getFuture()), timeoutSeconds);
		request.reply.sendError(operation_failed());
		ErrorOr<Void> result = co_await timeoutError(errorOr(prefixDurable), timeoutSeconds);
		ASSERT(result.isError() && result.getError().code() == error_code_tlog_failed);
		ErrorOr<Void> changedResult = co_await timeoutError(errorOr(coreStateChanged), timeoutSeconds);
		ASSERT(changedResult.isError() && changedResult.getError().code() == error_code_tlog_failed);
		ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);
		assertRemotePrefixCoreStateError(logSystem, error_code_tlog_failed);
	}

	TLogInterface original(locality);
	TLogInterface replacement(original.id(), original.getSharedTLogID(), locality);
	auto logSystem = makeLaggingRemoteLogSystem({ original }, TLogInterface(locality), TLogInterface(locality));
	const auto oldRoles = logSystem->getLogSystemConfig().oldTLogs;
	makePendingRemotePrefixCoreState(logSystem);
	Future<Void> prefixDurable = logSystem->onRemoteLogPrefixDurable();
	TLogQueuingMetricsRequest staleRequest =
	    co_await timeoutError(waitAndForward(original.getQueuingMetrics.getFuture()), timeoutSeconds);
	logSystem->tLogs[1]->logServers[0]->setUnconditional(OptionalInterface<TLogInterface>(replacement));
	TLogQueuingMetricsRequest replacementRequest =
	    co_await timeoutError(waitAndForward(replacement.getQueuingMetrics.getFuture()), timeoutSeconds);
	staleRequest.reply.send(makeRemotePrefixMetricsReply(100));
	replacementRequest.reply.send(makeRemotePrefixMetricsReply(99));
	ASSERT(!prefixDurable.isReady());
	makePendingRemotePrefixCoreState(logSystem);
	ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);

	TLogQueuingMetricsRequest caughtUpRequest =
	    co_await timeoutError(waitAndForward(replacement.getQueuingMetrics.getFuture()), timeoutSeconds);
	caughtUpRequest.reply.send(makeRemotePrefixMetricsReply(100));
	co_await timeoutError(prefixDurable, timeoutSeconds);
	ASSERT(!logSystem->remoteRecoveryComplete.isReady());
	ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);
	makeRecoveredRemotePrefixCoreState(logSystem);
	ASSERT(logSystem->getLogSystemConfig().oldTLogs == oldRoles);
	co_return;
}

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
