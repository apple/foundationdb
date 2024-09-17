/*
 * TestTLogServer.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/TestTLogServer.actor.h"

#include <filesystem>
#include <vector>

#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/Knobs.h"
#include "flow/IRandom.h"
#include "flow/DebugTrace.h"

#include "flow/actorcompiler.h" // must be last include

Reference<TLogTestContext> initTLogTestContext(TestTLogOptions tLogOptions,
                                               Optional<Reference<TLogTestContext>> oldTLogTestContext) {
	Reference<TLogTestContext> context(new TLogTestContext(tLogOptions));
	context->logID = deterministicRandom()->randomUniqueID();
	context->workerID = deterministicRandom()->randomUniqueID();
	context->diskQueueBasename = tLogOptions.diskQueueBasename;
	context->numCommits = tLogOptions.numCommits;
	context->numTags = tLogOptions.numTags;
	context->numLogServers = tLogOptions.numLogServers;
	context->dcID = "test"_sr;
	context->tagLocality = context->primaryLocality;
	context->dbInfo = ServerDBInfo();
	if (oldTLogTestContext.present()) {
		OldTLogConf oldTLogConf;
		oldTLogConf.tLogs = oldTLogTestContext.get()->dbInfo.logSystemConfig.tLogs;
		for (uint32_t i = 0; i < context->numLogServers; i++) {
			oldTLogConf.tLogs[i].locality = oldTLogTestContext.get()->primaryLocality;
			oldTLogConf.tLogs[i].isLocal = true;
		}
		oldTLogConf.epochBegin = oldTLogTestContext.get()->initVersion;
		oldTLogConf.epochEnd = oldTLogTestContext.get()->numCommits;
		oldTLogConf.logRouterTags = 0;
		oldTLogConf.recoverAt = oldTLogTestContext.get()->initVersion;
		oldTLogConf.epoch = oldTLogTestContext.get()->epoch;
		context->tagLocality = oldTLogTestContext.get()->primaryLocality;
		context->epoch = oldTLogTestContext.get()->epoch + 1;
		context->dbInfo.logSystemConfig.oldTLogs.push_back(oldTLogConf);
		context->commitHistory = oldTLogTestContext.get()->commitHistory;
	}
	context->dbInfo.logSystemConfig.logSystemType = LogSystemType::tagPartitioned;
	context->dbInfo.logSystemConfig.recruitmentID = deterministicRandom()->randomUniqueID();
	context->initVersion = tLogOptions.initVersion;
	context->recover = tLogOptions.recover;
	context->dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(context->dbInfo);

	return context;
}

// Create and start a tLog. If optional parmeters are set, the tLog is a new generation of "tLogID"
// as described by initReq. Otherwise, it is a newborn generation 0 tLog.
ACTOR Future<Void> getTLogCreateActor(Reference<TLogTestContext> pTLogTestContext,
                                      TestTLogOptions tLogOptions,
                                      uint16_t processID,
                                      InitializeTLogRequest* initReq = nullptr,
                                      UID tLogID = UID()) {

	// build per-tLog state.
	state Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
	pTLogContext->tagProcessID = processID;

	pTLogContext->tLogID = tLogID != UID(0, 0) ? tLogID : deterministicRandom()->randomUniqueID();

	TraceEvent("TestTLogServerEnterGetTLogCreateActor", pTLogContext->tLogID).detail("Epoch", pTLogTestContext->epoch);

	std::filesystem::create_directory(tLogOptions.dataFolder);

	// create persistent storage
	std::string diskQueueBasename = pTLogTestContext->diskQueueBasename + "." + pTLogContext->tLogID.toString() + "." +
	                                std::to_string(pTLogTestContext->epoch) + ".";
	state std::string diskQueueFilename = tLogOptions.dataFolder + "/" + diskQueueBasename;
	pTLogContext->persistentQueue =
	    openDiskQueue(diskQueueFilename, tLogOptions.diskQueueExtension, pTLogContext->tLogID, DiskQueueVersion::V2);

	state std::string kvStoreFilename = tLogOptions.dataFolder + "/" + tLogOptions.kvStoreFilename + "." +
	                                    pTLogContext->tLogID.toString() + "." +
	                                    std::to_string(pTLogTestContext->epoch) + ".";
	pTLogContext->persistentData = keyValueStoreMemory(kvStoreFilename,
	                                                   pTLogContext->tLogID,
	                                                   tLogOptions.kvMemoryLimit,
	                                                   tLogOptions.kvStoreExtension,
	                                                   KeyValueStoreType::MEMORY_RADIXTREE);

	// prepare tLog construction.
	Standalone<StringRef> machineID = "machine"_sr;
	LocalityData localities(
	    Optional<Standalone<StringRef>>(), pTLogTestContext->zoneID, machineID, pTLogTestContext->dcID);
	localities.set(StringRef("datacenter"_sr), pTLogTestContext->dcID);

	Reference<AsyncVar<bool>> isDegraded = FlowTransport::transport().getDegraded();
	Reference<AsyncVar<UID>> activeSharedTLog(new AsyncVar<UID>(pTLogContext->tLogID));
	Reference<AsyncVar<bool>> enablePrimaryTxnSystemHealthCheck(new AsyncVar<bool>(false));
	state PromiseStream<InitializeTLogRequest> promiseStream = PromiseStream<InitializeTLogRequest>();
	Promise<Void> oldLog;
	Promise<Void> recovery;

	// construct tLog.
	state Future<Void> tl = ::tLog(pTLogContext->persistentData,
	                               pTLogContext->persistentQueue,
	                               pTLogTestContext->dbInfoRef,
	                               localities,
	                               promiseStream,
	                               pTLogContext->tLogID,
	                               pTLogTestContext->workerID,
	                               false, /* restoreFromDisk */
	                               oldLog,
	                               recovery,
	                               pTLogTestContext->diskQueueBasename,
	                               isDegraded,
	                               activeSharedTLog,
	                               enablePrimaryTxnSystemHealthCheck);

	state InitializeTLogRequest initTLogReq = InitializeTLogRequest();
	if (initReq != nullptr) {
		initTLogReq = *initReq;
	} else {
		std::vector<Tag> tags;
		for (uint32_t tagID = 0; tagID < pTLogTestContext->numTags; tagID++) {
			Tag tag(pTLogTestContext->tagLocality, tagID);
			tags.push_back(tag);
		}
		initTLogReq.epoch = 1;
		initTLogReq.allTags = tags;
		initTLogReq.isPrimary = true;
		initTLogReq.locality = pTLogTestContext->primaryLocality;
		initTLogReq.recoveryTransactionVersion = pTLogTestContext->initVersion;
	}

	TLogInterface interface = wait(promiseStream.getReply(initTLogReq));
	pTLogContext->TestTLogInterface = interface;
	pTLogContext->init = promiseStream;

	// inform other actors tLog is ready.
	pTLogContext->TLogCreated.send(true);

	TraceEvent("TestTLogServerInitializedTLog", pTLogContext->tLogID);

	// wait for either test completion or tLog failure.
	choose {
		when(wait(tl)) {}
		when(bool testCompleted = wait(pTLogContext->TestTLogServerCompleted.getFuture())) {
			ASSERT_EQ(testCompleted, true);
		}
	}

	wait(delay(1.0));

	// delete old disk queue files
	deleteFile(diskQueueFilename + "0." + tLogOptions.diskQueueExtension);
	deleteFile(diskQueueFilename + "1." + tLogOptions.diskQueueExtension);
	deleteFile(kvStoreFilename + "0." + tLogOptions.kvStoreExtension);
	deleteFile(kvStoreFilename + "1." + tLogOptions.kvStoreExtension);

	return Void();
}

ACTOR Future<Void> TLogTestContext::sendPushMessages(TLogTestContext* pTLogTestContext) {

	TraceEvent("TestTLogServerEnterPush", pTLogTestContext->workerID);

	state uint16_t logID = 0;
	for (; logID < pTLogTestContext->numLogServers; logID++) {
		state Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[logID];
		bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
		ASSERT_EQ(tLogReady, true);
	}

	state uint32_t i = 0;
	state Version prev = pTLogTestContext->initVersion - 1;
	state Version next = pTLogTestContext->initVersion;

	for (; i < pTLogTestContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);

		// build commit request
		LogPushData toCommit(pTLogTestContext->ls, pTLogTestContext->numLogServers);
		// UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(SpanContext());

		std::unordered_map<uint16_t, Version> tpcvMap;

		uint32_t replicaCount = pTLogTestContext->tLogOptions.replicaCount;
		uint32_t startLogServer = 0;
		uint32_t teamSize = pTLogTestContext->numLogServers / replicaCount;
		for (uint32_t tagID = 0; tagID < pTLogTestContext->numTags; tagID++, startLogServer++) {
			for (uint32_t logServer = startLogServer; logServer < pTLogTestContext->numLogServers;
			     logServer += teamSize) {
				// test tLogs advancing versions at different rates
				// if (!tagID && i > 5 && i > pTLogTestContext->numCommits / 2) {
				// 	continue;
				// }
				Tag tag(pTLogTestContext->tagLocality, tagID);
				std::vector<Tag> tags = { tag };
				toCommit.addTags(tags);
				toCommit.writeTypedMessage(m, false, true);
				tpcvMap[logServer] = prev;
				pTLogTestContext->commitHistory[std::tuple(tagID, logServer)].push_back(next);
				TraceEvent("AddTags").detail("C", i).detail("T", tag).detail("L", logServer).detail("N", next);
			}
		}
		if (toCommit.getMutationCount()) {
			const auto versionSet = ILogSystem::PushVersionSet{ prev, next, prev, prev };
			Future<Version> loggingComplete =
			    pTLogTestContext->ls->push(versionSet, toCommit, SpanContext(), UID(), tpcvMap);
			Version ver = wait(loggingComplete);
			ASSERT_LE(ver, next);
		}
		prev = next;
		pTLogTestContext->tLogOptions.versions.push_back(next);
		next += SERVER_KNOBS->VERSIONS_PER_SECOND + deterministicRandom()->randomInt(0, 20);
	}

	TraceEvent("TestTLogServerExitPush", pTLogTestContext->workerID)
	    .detail("NumVersions", pTLogTestContext->tLogOptions.versions.size());

	return Void();
}

// send peek/pop through a given TLog interface and tag
ACTOR Future<Void> TLogTestContext::peekCommitMessages(TLogTestContext* pTLogTestContext,
                                                       uint16_t tagID,
                                                       uint32_t logID) {
	state Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[logID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	// peek from the same tag
	state Tag tag(pTLogTestContext->tagLocality, tagID);

	TraceEvent("TestTLogServerEnterPeek", pTLogTestContext->workerID).detail("LogID", logID).detail("Tag", tag);

	state uint32_t i = 0;
	state std::vector<Version> versions = pTLogTestContext->commitHistory[std::tuple(tagID, logID)];
	for (Version v : versions) {
		state Version begin = v;
		// wait for next message commit
		::TLogPeekRequest request(begin, tag, false, false);
		::TLogPeekReply reply = wait(pTLogContext->TestTLogInterface.peekMessages.getReply(request));
		TraceEvent("TestTLogServerTryValidateDataOnPeek", pTLogTestContext->workerID)
		    .detail("R", begin)
		    .detail("B", reply.begin.present() ? reply.begin.get() : -1)
		    .detail("P", reply.popped)
		    .detail("To", pTLogContext->TestTLogInterface.toString());

		if (reply.popped.present() && reply.popped.get() > begin) {
			break;
		}
		// validate versions
		ASSERT_GE(reply.maxKnownVersion, i);

		// deserialize package, first the version header
		ArenaReader rd = ArenaReader(reply.arena, reply.messages, AssumeVersion(g_network->protocolVersion()));
		ASSERT_EQ(*(int32_t*)rd.peekBytes(4), VERSION_HEADER);
		int32_t dummy; // skip past VERSION_HEADER
		Version ver;
		rd >> dummy >> ver;

		// deserialize transaction header
		int32_t messageLength;
		uint16_t tagCount;
		uint32_t sub = 1;
		if (FLOW_KNOBS->WRITE_TRACING_ENABLED) {
			rd >> messageLength >> sub >> tagCount;
			rd.readBytes(tagCount * sizeof(Tag));

			// deserialize span id
			if (sub == 1) {
				SpanContextMessage contextMessage;
				rd >> contextMessage;
			}
		}

		// deserialize mutation header
		if (sub == 1) {
			rd >> messageLength >> sub >> tagCount;
			rd.readBytes(tagCount * sizeof(Tag));
		}
		// deserialize mutation
		MutationRef m;
		rd >> m;

		// validate data
		Standalone<StringRef> expectedKey = StringRef(format("key %d", i));
		Standalone<StringRef> expectedVal = StringRef(format("value %d", i));
		ASSERT_WE_THINK(m.param1 == expectedKey);
		ASSERT_WE_THINK(m.param2 == expectedVal);

		TraceEvent("TestTLogServerValidatedDataOnPeek", pTLogTestContext->workerID)
		    .detail("CommitCount", i)
		    .detail("LogID", logID)
		    .detail("TagID", tag);

		// go directly to pop as there is no SS.
		::TLogPopRequest requestPop(begin, begin, tag);
		wait(pTLogContext->TestTLogInterface.popMessages.getReply(requestPop));
		i++;
	}

	TraceEvent("TestTLogServerExitPeek", pTLogTestContext->workerID).detail("LogID", logID).detail("TagID", tag);

	return Void();
}

ACTOR Future<Void> buildTLogSet(Reference<TLogTestContext> pTLogTestContext) {
	state TLogSet tLogSet;
	state uint16_t processID = 0;

	tLogSet.tLogLocalities.push_back(LocalityData());
	tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
	tLogSet.locality = pTLogTestContext->primaryLocality;
	tLogSet.isLocal = true;
	tLogSet.tLogVersion = TLogVersion::V6;
	tLogSet.tLogReplicationFactor = 1;
	for (; processID < pTLogTestContext->numLogServers; processID++) {
		state Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
		ASSERT_EQ(isCreated, true);

		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->TestTLogInterface));
	}
	pTLogTestContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);
	for (processID = 0; processID < pTLogTestContext->numLogServers; processID++) {
		Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		// start transactions
		pTLogContext->TLogStarted.send(true);
	}
	return Void();
}

// This test creates a tLog and pushes data to it. the If the recovery
// test switch is on, a new "generation" of tLogs is then created. These enter recover mode
// and pull data from the old generation. The data is peeked from either the old or new generation
// depending on the recovery switch, validated, and popped.

ACTOR Future<Void> startTestsTLogRecoveryActors(TestTLogOptions params) {
	state std::vector<Future<Void>> tLogActors;
	state Reference<TLogTestContext> pTLogTestContextEpochOne =
	    initTLogTestContext(params, Optional<Reference<TLogTestContext>>());

	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);

	state uint16_t tLogIdx = 0;

	TraceEvent("TestTLogServerEnterRecoveryTest");

	// Create the first "old" generation of tLogs
	for (tLogIdx = 0; tLogIdx < pTLogTestContextEpochOne->numLogServers; tLogIdx++) {
		Reference<TLogContext> pTLogContext(new TLogContext(tLogIdx));
		pTLogTestContextEpochOne->pTLogContextList.push_back(pTLogContext);
		tLogActors.emplace_back(
		    getTLogCreateActor(pTLogTestContextEpochOne, pTLogTestContextEpochOne->tLogOptions, tLogIdx));
	}

	// wait for tLogs to be created, and signal pushes can start
	wait(buildTLogSet(pTLogTestContextEpochOne));

	PromiseStream<Future<Void>> promises;

	pTLogTestContextEpochOne->ls = ILogSystem::fromServerDBInfo(
	    pTLogTestContextEpochOne->logID, pTLogTestContextEpochOne->dbInfo, false, promises);

	wait(pTLogTestContextEpochOne->sendPushMessages());

	state uint32_t tagID = 0;
	state std::map<std::tuple<uint32_t, uint32_t>, std::vector<Version>> commitHistory;

	if (!pTLogTestContextEpochOne->recover) {
		commitHistory = pTLogTestContextEpochOne->commitHistory;
		for (const auto& [loc, ver] : commitHistory) {
			wait(pTLogTestContextEpochOne->peekCommitMessages(std::get<0>(loc), std::get<1>(loc)));
		}
	} else {
		// Done with old generation. Lock the old generations of tLogs.
		state Reference<TLogTestContext> pTLogTestContextEpochTwo =
		    initTLogTestContext(TestTLogOptions(params), pTLogTestContextEpochOne);
		// for validation: the expected versions in EpochTwo that were written in EpochOne
		pTLogTestContextEpochTwo->tLogOptions.versions = pTLogTestContextEpochOne->tLogOptions.versions;
		commitHistory = pTLogTestContextEpochTwo->commitHistory;
		pTLogTestContextEpochTwo->pTLogContextList.resize(commitHistory.size());
		for (const auto& [loc, ver] : commitHistory) {
			tLogIdx = std::get<1>(loc);
			tagID = std::get<0>(loc);
			TLogLockResult data = wait(pTLogTestContextEpochOne->pTLogContextList[tLogIdx]
			                               ->TestTLogInterface.lock.template getReply<TLogLockResult>());
			TraceEvent("TestTLogServerLockResult")
			    .detail("KCV", data.knownCommittedVersion)
			    .detail("T", tagID)
			    .detail("L", tLogIdx);

			Reference<TLogContext> pNewTLogContext(new TLogContext(tLogIdx));
			pTLogTestContextEpochTwo->pTLogContextList[tLogIdx] = pNewTLogContext;
			state InitializeTLogRequest req;
			req.recruitmentID = pTLogTestContextEpochTwo->dbInfo.logSystemConfig.recruitmentID;
			req.recoverAt = pTLogTestContextEpochOne->tLogOptions.versions.back();
			// for testing + 10;
			// wait(pTLogTestContextEpochOne->pTLogContextList[tLogIdx]
			//          ->TestTLogInterface.setClusterRecoveryVersion.getReply(
			//              setClusterRecoveryVersionRequest(req.recoverAt + 1)));
			req.startVersion = pTLogTestContextEpochOne->initVersion + 1;
			req.recoveryTransactionVersion = pTLogTestContextEpochOne->initVersion;
			req.knownCommittedVersion = 0;
			req.epoch = pTLogTestContextEpochTwo->epoch;
			req.logVersion = TLogVersion::V6;
			req.locality = pTLogTestContextEpochTwo->primaryLocality;
			req.isPrimary = true;
			req.logRouterTags = 0;
			req.recoverTags = { Tag(pTLogTestContextEpochTwo->primaryLocality, tagID) };
			req.recoverFrom = pTLogTestContextEpochOne->dbInfo.logSystemConfig;
			req.recoverFrom.logRouterTags = 0;

			const TestTLogOptions& tLogOptions = pTLogTestContextEpochTwo->tLogOptions;

			tLogActors.emplace_back(getTLogCreateActor(pTLogTestContextEpochTwo,
			                                           tLogOptions,
			                                           tLogIdx,
			                                           &req,
			                                           pTLogTestContextEpochOne->pTLogContextList[tLogIdx]->tLogID));

			state Reference<TLogContext> pTLogContext = pTLogTestContextEpochTwo->pTLogContextList[tLogIdx];
			bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
			ASSERT_EQ(isCreated, true);
			pTLogContext->TLogStarted.send(true);
		}

		for (const auto& [loc, ver] : commitHistory) {
			wait(pTLogTestContextEpochTwo->peekCommitMessages(std::get<0>(loc), std::get<1>(loc)));
		}

		// signal that tLogs can be destroyed
		for (tLogIdx = 0; tLogIdx < pTLogTestContextEpochOne->numLogServers; tLogIdx++) {
			pTLogTestContextEpochTwo->pTLogContextList[tLogIdx]->TestTLogServerCompleted.send(true);
		}
	}

	for (tLogIdx = 0; tLogIdx < pTLogTestContextEpochOne->numLogServers; tLogIdx++) {
		pTLogTestContextEpochOne->pTLogContextList[tLogIdx]->TestTLogServerCompleted.send(true);
	}

	// wait for tLogs to destruct
	wait(waitForAll(tLogActors));

	TraceEvent("TestTLogServerExitRecoveryTest");

	return Void();
}

// TEST_CASE("/fdbserver/test/TestTLogCommits") {
// 	params.set("recover", static_cast<int64_t>(0));
// 	TestTLogOptions testTLogOptions(params);
// 	wait(startTestsTLogRecoveryActors(testTLogOptions));
// 	return Void();
// }

// // Two tlogs, one tag. Write 10 commits up to version V. Start recovery with RV set to V+10.
// TEST_CASE("/fdbserver/test/TestTLogRecovery1") {
// 	TestTLogOptions testTLogOptions(params);
// 	wait(startTestsTLogRecoveryActors(testTLogOptions));
// 	return Void();
// }

// // Four tlogs, two tags.
// TEST_CASE("/fdbserver/test/TestTLogRecovery2") {
// 	params.set("numLogServers", static_cast<int64_t>(4));
// 	params.set("numTags", static_cast<int64_t>(2));

// 	TestTLogOptions testTLogOptions(params);
// 	wait(startTestsTLogRecoveryActors(testTLogOptions));
// 	return Void();
// }
