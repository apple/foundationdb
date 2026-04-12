/*
 * TestTLogServer.cpp
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

#include "TestTLogServer.h"

#include <filesystem>
#include <vector>

#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/kvstore/IDiskQueue.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/TLogInterface.h"
#include "fdbserver/tlog/TLogServer.actor.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "fdbserver/logsystem/LogSystem.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "flow/IRandom.h"
#include "flow/DebugTrace.h"

#include "flow/CoroUtils.h"

namespace {

OldTLogConf buildOldTLogConf(const TLogTestContext& oldCtx, int numLogServers, int8_t primaryLocality) {
	OldTLogConf oldTLogConf;
	oldTLogConf.tLogs = oldCtx.dbInfo.logSystemConfig.tLogs;
	for (int i = 0; i < numLogServers && i < oldTLogConf.tLogs.size(); ++i) {
		oldTLogConf.tLogs[i].locality = primaryLocality;
		oldTLogConf.tLogs[i].isLocal = true;
	}
	oldTLogConf.epochBegin = oldCtx.initVersion;
	oldTLogConf.epochEnd = oldCtx.numCommits;
	oldTLogConf.logRouterTags = 0;
	oldTLogConf.recoverAt = oldCtx.initVersion;
	oldTLogConf.epoch = oldCtx.epoch;
	return oldTLogConf;
}

std::string makeDiskQueueBasename(const TLogTestContext& ctx, UID tLogId) {
	return ctx.diskQueueBasename + "." + tLogId.toString() + "." + std::to_string(ctx.epoch) + ".";
}

std::string makeKVStoreBasename(const TestTLogOptions& options, const TLogTestContext& ctx, UID tLogId) {
	return options.kvStoreFilename + "." + tLogId.toString() + "." + std::to_string(ctx.epoch) + ".";
}

InitializeTLogRequest buildInitializeRequest(const TLogTestContext& ctx) {
	InitializeTLogRequest req;
	std::vector<Tag> tags;
	tags.reserve(ctx.numTags);
	for (uint32_t tagID = 0; tagID < ctx.numTags; tagID++) {
		tags.emplace_back(ctx.tagLocality, tagID);
	}
	req.epoch = 1;
	req.allTags = tags;
	req.isPrimary = true;
	req.locality = ctx.primaryLocality;
	req.recoveryTransactionVersion = ctx.initVersion;
	return req;
}

struct TempStorageFiles {
	std::string queueBase;
	std::string queueExt;
	std::string kvBase;
	std::string kvExt;
	bool active{ false };

	TempStorageFiles() = default;
	TempStorageFiles(std::string queueBase, std::string queueExt, std::string kvBase, std::string kvExt)
	  : queueBase(std::move(queueBase)), queueExt(std::move(queueExt)), kvBase(std::move(kvBase)),
	    kvExt(std::move(kvExt)), active(true) {}

	~TempStorageFiles() { cleanup(); }

	void cleanup() {
		if (!active)
			return;
		if (!queueBase.empty()) {
			deleteFile(queueBase + "0." + queueExt);
			deleteFile(queueBase + "1." + queueExt);
		}
		if (!kvBase.empty()) {
			deleteFile(kvBase + "0." + kvExt);
			deleteFile(kvBase + "1." + kvExt);
		}
		active = false;
	}
};

struct StorageResources {
	std::string diskQueueFilename;
	std::string kvStoreFilename;
	TempStorageFiles tempFiles;

	StorageResources() = default;
	StorageResources(std::string dq, std::string kv, TempStorageFiles files)
	  : diskQueueFilename(std::move(dq)), kvStoreFilename(std::move(kv)), tempFiles(std::move(files)) {}
};

StorageResources setupPersistentStorage(Reference<TLogContext> tLogContext,
                                        const TLogTestContext& testContext,
                                        const TestTLogOptions& options) {
	std::string diskQueueFilename = options.dataFolder + "/" + makeDiskQueueBasename(testContext, tLogContext->tLogID);
	tLogContext->persistentQueue =
	    openDiskQueue(diskQueueFilename, options.diskQueueExtension, tLogContext->tLogID, DiskQueueVersion::V2);

	std::string kvStoreFilename =
	    options.dataFolder + "/" + makeKVStoreBasename(options, testContext, tLogContext->tLogID);
	tLogContext->persistentData = keyValueStoreMemory(kvStoreFilename,
	                                                  tLogContext->tLogID,
	                                                  options.kvMemoryLimit,
	                                                  options.kvStoreExtension,
	                                                  KeyValueStoreType::MEMORY_RADIXTREE);

	TempStorageFiles tempFiles(
	    diskQueueFilename, options.diskQueueExtension, kvStoreFilename, options.kvStoreExtension);
	return StorageResources(diskQueueFilename, kvStoreFilename, std::move(tempFiles));
}

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
		const TLogTestContext& oldCtx = *oldTLogTestContext.get();
		context->dbInfo.logSystemConfig.oldTLogs.push_back(
		    buildOldTLogConf(oldCtx, context->numLogServers, oldCtx.primaryLocality));
		context->tagLocality = oldCtx.primaryLocality;
		context->epoch = oldCtx.epoch + 1;
		context->commitHistory = oldCtx.commitHistory;
	}
	context->dbInfo.logSystemConfig.logSystemType = LogSystemType::tagPartitioned;
	context->dbInfo.logSystemConfig.recruitmentID = deterministicRandom()->randomUniqueID();
	context->initVersion = tLogOptions.initVersion;
	context->recover = tLogOptions.recover;
	context->dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(context->dbInfo);

	return context;
}

} // namespace

// Create and start a tLog. If optional parmeters are set, the tLog is a new generation of "tLogID"
// as described by initReq. Otherwise, it is a newborn generation 0 tLog.
Future<Void> getTLogCreateActor(Reference<TLogTestContext> pTLogTestContext,
                                TestTLogOptions tLogOptions,
                                uint16_t processID,
                                InitializeTLogRequest* initReq = nullptr,
                                UID tLogID = UID()) {

	// build per-tLog state.
	Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
	pTLogContext->tagProcessID = processID;

	pTLogContext->tLogID = tLogID != UID(0, 0) ? tLogID : deterministicRandom()->randomUniqueID();

	TraceEvent("TestTLogServerEnterGetTLogCreateActor", pTLogContext->tLogID).detail("Epoch", pTLogTestContext->epoch);

	std::filesystem::create_directory(tLogOptions.dataFolder);

	// create persistent storage
	StorageResources storage = setupPersistentStorage(pTLogContext, *pTLogTestContext, tLogOptions);

	// prepare tLog construction.
	Standalone<StringRef> machineID = "machine"_sr;
	LocalityData localities(
	    Optional<Standalone<StringRef>>(), pTLogTestContext->zoneID, machineID, pTLogTestContext->dcID);
	localities.set(StringRef("datacenter"_sr), pTLogTestContext->dcID);

	Reference<AsyncVar<bool>> isDegraded = FlowTransport::transport().getDegraded();
	Reference<AsyncVar<bool>> lowDiskTLogExclusion(new AsyncVar<bool>(false));
	Reference<AsyncVar<UID>> activeSharedTLog(new AsyncVar<UID>(pTLogContext->tLogID));
	Reference<AsyncVar<bool>> enablePrimaryTxnSystemHealthCheck(new AsyncVar<bool>(false));
	PromiseStream<InitializeTLogRequest> promiseStream;
	Promise<Void> oldLog;
	Promise<Void> recovery;

	// construct tLog.
	Future<Void> tl = ::tLog(pTLogContext->persistentData,
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
	                         lowDiskTLogExclusion,
	                         activeSharedTLog,
	                         enablePrimaryTxnSystemHealthCheck);

	InitializeTLogRequest initTLogReq = initReq != nullptr ? *initReq : buildInitializeRequest(*pTLogTestContext);

	TLogInterface interface = co_await promiseStream.getReply(initTLogReq);
	pTLogContext->TestTLogInterface = interface;
	pTLogContext->init = promiseStream;

	// inform other actors tLog is ready.
	pTLogContext->TLogCreated.send(true);

	TraceEvent("TestTLogServerInitializedTLog", pTLogContext->tLogID);

	// wait for either test completion or tLog failure.
	auto choice = co_await race(tl, pTLogContext->TestTLogServerCompleted.getFuture());
	if (choice.index() == 1) {
		bool testCompleted = std::get<1>(std::move(choice));
		ASSERT_EQ(testCompleted, true);
	}

	co_await delay(1.0);

	storage.tempFiles.cleanup();
}

Future<Void> TLogTestContext::sendPushMessages(TLogTestContext* pTLogTestContext) {

	TraceEvent("TestTLogServerEnterPush", pTLogTestContext->workerID);

	for (uint16_t logID = 0; logID < pTLogTestContext->numLogServers; ++logID) {
		Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[logID];
		bool tLogReady = co_await pTLogContext->TLogStarted.getFuture();
		ASSERT_EQ(tLogReady, true);
	}

	Version prev = pTLogTestContext->initVersion - 1;
	Version next = pTLogTestContext->initVersion;

	for (uint32_t i = 0; i < pTLogTestContext->numCommits; ++i) {
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
			const auto versionSet = LogPushVersionSet{ prev, next, prev, prev };
			Future<Version> loggingComplete =
			    pTLogTestContext->ls->push(versionSet, toCommit, SpanContext(), UID(), tpcvMap);
			Version ver = co_await loggingComplete;
			ASSERT_LE(ver, next);
		}
		prev = next;
		pTLogTestContext->tLogOptions.versions.push_back(next);
		next += SERVER_KNOBS->VERSIONS_PER_SECOND + deterministicRandom()->randomInt(0, 20);
	}

	TraceEvent("TestTLogServerExitPush", pTLogTestContext->workerID)
	    .detail("NumVersions", pTLogTestContext->tLogOptions.versions.size());
}

// send peek/pop through a given TLog interface and tag
Future<Void> TLogTestContext::peekCommitMessages(TLogTestContext* pTLogTestContext, uint16_t tagID, uint32_t logID) {
	Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[logID];
	bool tLogReady = co_await pTLogContext->TLogStarted.getFuture();
	ASSERT_EQ(tLogReady, true);

	// peek from the same tag
	Tag tag(pTLogTestContext->tagLocality, tagID);

	TraceEvent("TestTLogServerEnterPeek", pTLogTestContext->workerID).detail("LogID", logID).detail("Tag", tag);

	uint32_t i = 0;
	std::vector<Version> versions = pTLogTestContext->commitHistory[std::tuple(tagID, logID)];
	for (Version v : versions) {
		Version begin = v;
		// wait for next message commit
		::TLogPeekRequest request(begin, tag, false, false);
		::TLogPeekReply reply = co_await pTLogContext->TestTLogInterface.peekMessages.getReply(request);
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
		co_await pTLogContext->TestTLogInterface.popMessages.getReply(requestPop);
		i++;
	}

	TraceEvent("TestTLogServerExitPeek", pTLogTestContext->workerID).detail("LogID", logID).detail("TagID", tag);
}

Future<Void> buildTLogSet(Reference<TLogTestContext> pTLogTestContext) {
	TLogSet tLogSet;

	tLogSet.tLogLocalities.push_back(LocalityData());
	tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
	tLogSet.locality = pTLogTestContext->primaryLocality;
	tLogSet.isLocal = true;
	tLogSet.tLogVersion = TLogVersion::V6;
	tLogSet.tLogReplicationFactor = 1;
	for (uint16_t processID = 0; processID < pTLogTestContext->numLogServers; ++processID) {
		Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		bool isCreated = co_await pTLogContext->TLogCreated.getFuture();
		ASSERT_EQ(isCreated, true);

		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->TestTLogInterface));
	}
	pTLogTestContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);
	for (uint16_t processID = 0; processID < pTLogTestContext->numLogServers; ++processID) {
		Reference<TLogContext> pTLogContext = pTLogTestContext->pTLogContextList[processID];
		// start transactions
		pTLogContext->TLogStarted.send(true);
	}
}

// This test creates a tLog and pushes data to it. the If the recovery
// test switch is on, a new "generation" of tLogs is then created. These enter recover mode
// and pull data from the old generation. The data is peeked from either the old or new generation
// depending on the recovery switch, validated, and popped.

Future<Void> startTestsTLogRecoveryActors(TestTLogOptions params) {
	std::vector<Future<Void>> tLogActors;
	Reference<TLogTestContext> pTLogTestContextEpochOne =
	    initTLogTestContext(params, Optional<Reference<TLogTestContext>>());

	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);

	uint16_t tLogIdx = 0;

	TraceEvent("TestTLogServerEnterRecoveryTest");

	// Create the first "old" generation of tLogs
	for (tLogIdx = 0; tLogIdx < pTLogTestContextEpochOne->numLogServers; tLogIdx++) {
		Reference<TLogContext> pTLogContext(new TLogContext(tLogIdx));
		pTLogTestContextEpochOne->pTLogContextList.push_back(pTLogContext);
		tLogActors.emplace_back(
		    getTLogCreateActor(pTLogTestContextEpochOne, pTLogTestContextEpochOne->tLogOptions, tLogIdx));
	}

	// wait for tLogs to be created, and signal pushes can start
	co_await buildTLogSet(pTLogTestContextEpochOne);

	PromiseStream<Future<Void>> promises;

	pTLogTestContextEpochOne->ls = makeLogSystemFromServerDBInfo(
	    pTLogTestContextEpochOne->logID, pTLogTestContextEpochOne->dbInfo, false, promises);

	co_await pTLogTestContextEpochOne->sendPushMessages();

	uint32_t tagID = 0;
	std::map<std::tuple<uint32_t, uint32_t>, std::vector<Version>> commitHistory;

	if (!pTLogTestContextEpochOne->recover) {
		commitHistory = pTLogTestContextEpochOne->commitHistory;
		for (const auto& [loc, ver] : commitHistory) {
			co_await pTLogTestContextEpochOne->peekCommitMessages(std::get<0>(loc), std::get<1>(loc));
		}
	} else {
		// Done with old generation. Lock the old generations of tLogs.
		Reference<TLogTestContext> pTLogTestContextEpochTwo =
		    initTLogTestContext(TestTLogOptions(params), pTLogTestContextEpochOne);
		// for validation: the expected versions in EpochTwo that were written in EpochOne
		pTLogTestContextEpochTwo->tLogOptions.versions = pTLogTestContextEpochOne->tLogOptions.versions;
		commitHistory = pTLogTestContextEpochTwo->commitHistory;
		pTLogTestContextEpochTwo->pTLogContextList.resize(commitHistory.size());
		for (const auto& [loc, ver] : commitHistory) {
			tLogIdx = std::get<1>(loc);
			tagID = std::get<0>(loc);
			TLogLockResult data = co_await pTLogTestContextEpochOne->pTLogContextList[tLogIdx]
			                          ->TestTLogInterface.lock.template getReply<TLogLockResult>();
			TraceEvent("TestTLogServerLockResult")
			    .detail("KCV", data.knownCommittedVersion)
			    .detail("T", tagID)
			    .detail("L", tLogIdx);

			Reference<TLogContext> pNewTLogContext(new TLogContext(tLogIdx));
			pTLogTestContextEpochTwo->pTLogContextList[tLogIdx] = pNewTLogContext;
			InitializeTLogRequest req;
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

			Reference<TLogContext> pTLogContext = pTLogTestContextEpochTwo->pTLogContextList[tLogIdx];
			bool isCreated = co_await pTLogContext->TLogCreated.getFuture();
			ASSERT_EQ(isCreated, true);
			pTLogContext->TLogStarted.send(true);
		}

		for (const auto& [loc, ver] : commitHistory) {
			co_await pTLogTestContextEpochTwo->peekCommitMessages(std::get<0>(loc), std::get<1>(loc));
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
	co_await waitForAll(tLogActors);

	TraceEvent("TestTLogServerExitRecoveryTest");
}

// TEST_CASE("/fdbserver/test/TestTLogCommits") {
// 	params.set("recover", static_cast<int64_t>(0));
// 	TestTLogOptions testTLogOptions(params);
// 	co_await startTestsTLogRecoveryActors(testTLogOptions);
// }

// // Two tlogs, one tag. Write 10 commits up to version V. Start recovery with RV set to V+10.
// TEST_CASE("/fdbserver/test/TestTLogRecovery1") {
// 	TestTLogOptions testTLogOptions(params);
// 	co_await startTestsTLogRecoveryActors(testTLogOptions);
// }

// // Four tlogs, two tags.
// TEST_CASE("/fdbserver/test/TestTLogRecovery2") {
// 	params.set("numLogServers", static_cast<int64_t>(4));
// 	params.set("numTags", static_cast<int64_t>(2));

// 	TestTLogOptions testTLogOptions(params);
// 	co_await startTestsTLogRecoveryActors(testTLogOptions);
// }
