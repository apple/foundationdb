/*
 * MockTLog.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/MockTLog.actor.h"

#include <filesystem>
#include <vector>

#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/LogSystem.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // has to be last include

// These tests start TLogs and drive transactions through them. There is one test that calls
// the TLog commit interface directly and another using the LogSystem's push interface.
// The later supports a single log group made up of "numLogServers" servers and
// "numTagsPerServer" tags per log server.
// A third test exercises tLog recovery.
//
// These test's purpose is to help with microbenchmarks and experimentation.

// build test state.
std::shared_ptr<TLogDriverContext> initTLogDriverContext(TestTLogDriverOptions tLogOptions, int locality = 0) {
	std::shared_ptr<TLogDriverContext> context(new TLogDriverContext(tLogOptions));
	context->logID = deterministicRandom()->randomUniqueID();
	context->workerID = deterministicRandom()->randomUniqueID();
	context->diskQueueBasename = tLogOptions.diskQueueBasename;
	context->numCommits = tLogOptions.numCommits;
	context->numTagsPerServer = tLogOptions.numTagsPerServer;
	context->numLogServers = tLogOptions.numLogServers;
	context->dcID = StringRef("test");
	context->tagLocality = locality; // one data center.
	context->dbInfo = ServerDBInfo();
	context->dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(context->dbInfo);
	context->dbInfo.logSystemConfig.logSystemType = LogSystemType::tagPartitioned;
	context->dbInfo.logSystemConfig.recruitmentID = deterministicRandom()->randomUniqueID();

	return context;
}

// run a single tLog. If optional parmeters are set, the tLog is a new generation of "tLogID"
// as described in initReq. Otherwise, it is a generation 0 tLog.
ACTOR Future<Void> getTLogCreateActor(std::shared_ptr<TLogDriverContext> pTLogDriverContext,
                                      TestTLogDriverOptions tLogOptions,
                                      uint16_t processID,
                                      InitializeTLogRequest* initReq = nullptr,
                                      UID tLogID = UID()) {

	// build per-tLog state.
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
	pTLogContext->tagProcessID = processID;

	pTLogContext->tLogID = tLogID != UID(0, 0) ? tLogID : deterministicRandom()->randomUniqueID();

	TraceEvent("MockTLogTestEnterGetTLogCreateActor", pTLogContext->tLogID).detail("Epoch", pTLogDriverContext->epoch);

	std::filesystem::create_directory(tLogOptions.dataFolder);

	// create persistent storage
	std::string diskQueueBasename = pTLogDriverContext->diskQueueBasename + "." + pTLogContext->tLogID.toString() +
	                                "." + std::to_string(pTLogDriverContext->epoch) + ".";
	state std::string diskQueueFilename = tLogOptions.dataFolder + "/" + diskQueueBasename;
	pTLogContext->persistentQueue =
	    openDiskQueue(diskQueueFilename, tLogOptions.diskQueueExtension, pTLogContext->tLogID, DiskQueueVersion::V1);

	state std::string kvStoreFilename = tLogOptions.dataFolder + "/" + tLogOptions.kvStoreFilename + "." +
	                                    pTLogContext->tLogID.toString() + "." +
	                                    std::to_string(pTLogDriverContext->epoch) + ".";
	pTLogContext->persistentData = keyValueStoreMemory(kvStoreFilename,
	                                                   pTLogContext->tLogID,
	                                                   tLogOptions.kvMemoryLimit,
	                                                   tLogOptions.kvStoreExtension,
	                                                   KeyValueStoreType::MEMORY_RADIXTREE);

	// prepare tLog construction.
	Standalone<StringRef> machineID = StringRef("machine");
	LocalityData localities(
	    Optional<Standalone<StringRef>>(), pTLogDriverContext->zoneID, machineID, pTLogDriverContext->dcID);
	localities.set(StringRef("datacenter"), pTLogDriverContext->dcID);
	pTLogDriverContext->dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(pTLogDriverContext->dbInfo);

	Reference<AsyncVar<bool>> isDegraded = FlowTransport::transport().getDegraded();
	Reference<AsyncVar<UID>> activeSharedTLog(new AsyncVar<UID>(pTLogContext->tLogID));
	Reference<AsyncVar<bool>> enablePrimaryTxnSystemHealthCheck(new AsyncVar<bool>(false));
	state PromiseStream<InitializeTLogRequest> promiseStream = PromiseStream<InitializeTLogRequest>();
	Promise<Void> oldLog;
	Promise<Void> recovery;

	// construct tLog.
	state Future<Void> tl = ::tLog(pTLogContext->persistentData,
	                               pTLogContext->persistentQueue,
	                               pTLogDriverContext->dbInfoRef,
	                               localities,
	                               promiseStream,
	                               pTLogContext->tLogID,
	                               pTLogDriverContext->workerID,
	                               false, /* restoreFromDisk */
	                               oldLog,
	                               recovery,
	                               pTLogDriverContext->diskQueueBasename,
	                               isDegraded,
	                               activeSharedTLog,
	                               enablePrimaryTxnSystemHealthCheck);

	// start tlog.
	state InitializeTLogRequest initTLogReq = InitializeTLogRequest();
	if (initReq != nullptr) {
		initTLogReq = *initReq;
	} else {
		std::vector<Tag> tags;
		for (uint32_t tagID = 0; tagID < pTLogDriverContext->numTagsPerServer; tagID++) {
			Tag tag(pTLogDriverContext->tagLocality, tagID);
			tags.push_back(tag);
		}
		initTLogReq.epoch = 1;
		initTLogReq.allTags = tags;
		initTLogReq.isPrimary = true;
		initTLogReq.locality = 0;
		initTLogReq.recoveryTransactionVersion = 1;
	}
	// initTLogReq.clusterId = deterministicRandom()->randomUniqueID();
	TLogInterface interface = wait(promiseStream.getReply(initTLogReq));
	pTLogContext->MockTLogInterface = interface;
	pTLogContext->init = promiseStream;

	// inform other actors tLog is ready.
	pTLogContext->TLogCreated.send(true);

	TraceEvent("MockTLogTestInitializedTLog", pTLogContext->tLogID);

	// wait for either test completion or abnormal failure.
	choose {
		when(wait(tl)) {}
		when(bool testCompleted = wait(pTLogContext->MockTLogTestCompleted.getFuture())) {
			ASSERT_EQ(testCompleted, true);
		}
	}

	wait(delay(1.0));

	// delete old disk queue files
	deleteFile(diskQueueFilename + "0." + tLogOptions.diskQueueExtension);
	deleteFile(diskQueueFilename + "1." + tLogOptions.diskQueueExtension);
	deleteFile(kvStoreFilename + "0." + tLogOptions.kvStoreExtension);
	deleteFile(kvStoreFilename + "1." + tLogOptions.kvStoreExtension);

	TraceEvent("ExitGetTLogCreateActor", pTLogContext->tLogID);

	return Void();
}

// sent commits through TLog interface.
ACTOR Future<Void> TLogDriverContext::sendCommitMessages_impl(TLogDriverContext* pTLogDriverContext,
                                                              uint16_t processID) {
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	TraceEvent("MockTLogEnterSendCommits");

	state Version prev = 0;
	state Version next = 1;
	state int i = 0;
	for (; i < pTLogDriverContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);

		// build commit request
		LogPushData toCommit(pTLogDriverContext->ls, pTLogDriverContext->numLogServers);
		// UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(SpanContext());
		std::vector<Tag> tags;

		// Currently every commit will use all tags, which is not representative of real-world scenarios.
		// TODO randomize tags to mimic real workloads
		for (uint32_t tagID = 0; tagID < pTLogDriverContext->numTagsPerServer; tagID++) {
			Tag tag(pTLogDriverContext->tagLocality, tagID);
			tags.push_back(tag);
		}
		toCommit.addTags(tags);
		toCommit.writeTypedMessage(m);
		TraceEvent("MockTLogDan2");

		int location = 0;
		Standalone<StringRef> msg = toCommit.getMessages(location);

		// send commit and wait for reply.
		::TLogCommitRequest request(SpanContext(),
		                            msg.arena(),
		                            prev,
		                            next,
		                            prev,
		                            prev,
		                            msg,
		                            pTLogDriverContext->numLogServers,
		                            deterministicRandom()->randomUniqueID());
		::TLogCommitReply reply = wait(pTLogContext->MockTLogInterface.commit.getReply(request));
		ASSERT_LE(reply.version, next);
		prev++;
		next++;
	}

	TraceEvent("MockTLogExitSendCommits");

	return Void();
}

// Send pushes through the LogSystem interface. There is one of these actors for each log server.
ACTOR Future<Void> TLogDriverContext::sendPushMessages_impl(TLogDriverContext* pTLogDriverContext) {

	TraceEvent("MockTLogTestEnterPush", pTLogDriverContext->workerID);

	state uint16_t logID = 0;
	for (logID = 0; logID < pTLogDriverContext->numLogServers; logID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[logID];
		bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
		ASSERT_EQ(tLogReady, true);
	}

	state Version prev = 0;
	state Version next = 1;
	state int i = 0;
	for (; i < pTLogDriverContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);
		TraceEvent("MockTLogPush").detail("I", next);

		// build commit request
		LogPushData toCommit(pTLogDriverContext->ls, pTLogDriverContext->numLogServers /* tLogCount */);
		// UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(SpanContext());

		// for each tag
		for (uint32_t tagID = 0; tagID < pTLogDriverContext->numTagsPerServer; tagID++) {
			Tag tag(pTLogDriverContext->tagLocality, tagID);
			std::vector<Tag> tags = { tag };
			toCommit.addTags(tags);
			toCommit.writeTypedMessage(m);
		}
		Future<Version> loggingComplete = pTLogDriverContext->ls->push(prev, next, prev, prev, toCommit, SpanContext());
		Version ver = wait(loggingComplete);
		ASSERT_LE(ver, next);
		prev++;
		next++;
	}

	TraceEvent("MockTLogTestExitPush", pTLogDriverContext->workerID).detail("LogID", logID);

	return Void();
}

// send peek/pop through a given TLog interface (logGroupID) for a given tag (shardTag).
ACTOR Future<Void> TLogDriverContext::peekCommitMessages_impl(TLogDriverContext* pTLogDriverContext,
                                                              uint16_t logID,
                                                              uint32_t tagID) {
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[logID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	// peek from the same tag
	state Tag tag(pTLogDriverContext->tagLocality, tagID);

	TraceEvent("MockTLogTestEnterPeek", pTLogDriverContext->workerID).detail("LogID", logID).detail("Tag", tag);

	state Version begin = 1;
	state int i;
	for (i = 0; i < pTLogDriverContext->numCommits; i++) {
		// wait for next message commit
		::TLogPeekRequest request(begin, tag, false, false);
		::TLogPeekReply reply = wait(pTLogContext->MockTLogInterface.peekMessages.getReply(request));
		TraceEvent("MockTLogTestTryValidateDataOnPeek", pTLogDriverContext->workerID)
		    .detail("B", reply.begin.present() ? reply.begin.get() : -1);

		if (true) {
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
		}
		TraceEvent("MockTLogTestValidatedDataOnPeek", pTLogDriverContext->workerID)
		    .detail("Commit count", i)
		    .detail("LogID", logID)
		    .detail("TagID", tag);

		// go directly to pop as there is no SS.
		::TLogPopRequest requestPop(begin, begin, tag);
		wait(pTLogContext->MockTLogInterface.popMessages.getReply(requestPop));

		begin++;
	}

	TraceEvent("MockTLogTestExitPeek", pTLogDriverContext->workerID).detail("LogID", logID).detail("TagID", tag);

	return Void();
}

// wait for all tLogs to be created. Then build a single tLog server, then
// signal transactions can start.
ACTOR Future<Void> getTLogGroupActor(std::shared_ptr<TLogDriverContext> pTLogDriverContext) {
	// create tLog
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[0];
	bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
	ASSERT_EQ(isCreated, true);
	TraceEvent("MockTLogCreatedTLog");

	// setup log system and tlog group
	pTLogDriverContext->tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->MockTLogInterface));
	pTLogDriverContext->tLogSet.tLogLocalities.push_back(LocalityData());
	pTLogDriverContext->tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
	pTLogDriverContext->tLogSet.locality = 0;
	pTLogDriverContext->tLogSet.isLocal = true;
	pTLogDriverContext->tLogSet.tLogVersion = TLogVersion::V6;

	pTLogDriverContext->dbInfo.logSystemConfig.tLogs.push_back(pTLogDriverContext->tLogSet);
	// pTLogDriverContext->dbInfo.logSystemConfig.oldTLogs.push_back(pTLogDriverContext->oldTLogSet);
	PromiseStream<Future<Void>> promises;
	pTLogDriverContext->ls =
	    ILogSystem::fromServerDBInfo(pTLogDriverContext->logID, pTLogDriverContext->dbInfo, false, promises);

	// start transactions
	pTLogDriverContext->pTLogContextList[0]->TLogStarted.send(true);
	Future<Void> commit = pTLogDriverContext->sendCommitMessages();
	Future<Void> peek = pTLogDriverContext->peekCommitMessages();
	wait(commit && peek);

	// tell tLog actor to initiate shutdown.
	pTLogDriverContext->pTLogContextList[0]->MockTLogTestCompleted.send(true);

	TraceEvent("MockTLogExitTLogGroupActor");

	return Void();
}

// wait for all tLogs to be created. Then start actor to do push, then
// start actors to do peeks, then signal transactions can start.
ACTOR Future<Void> runTestsTLogGroupActors(std::shared_ptr<TLogDriverContext> pTLogDriverContext) {
	// create tLog
	state uint16_t processID = 0;
	// state std::vector<Future<Void>> logRouterActors;
	state TLogSet tLogSet;

	for (; processID < pTLogDriverContext->numLogServers; processID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
		bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
		ASSERT_EQ(isCreated, true);

		// setup log system and tlog group
		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->MockTLogInterface));
		tLogSet.tLogLocalities.push_back(LocalityData());
		tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
		tLogSet.locality = 0;
		tLogSet.isLocal = true;
		tLogSet.tLogVersion = TLogVersion::V6;
	}
	pTLogDriverContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);
	for (processID = 0; processID < pTLogDriverContext->numLogServers; processID++) {
		std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
		// start transactions
		pTLogContext->TLogStarted.send(true);
	}

	PromiseStream<Future<Void>> promises;
	pTLogDriverContext->ls =
	    ILogSystem::fromServerDBInfo(pTLogDriverContext->logID, pTLogDriverContext->dbInfo, false, promises);

	std::vector<Future<Void>> actors;

	// start push actor
	actors.emplace_back(pTLogDriverContext->sendPushMessages());

	// start peek actors
	for (processID = 0; processID < pTLogDriverContext->numLogServers; processID++) {
		for (uint32_t tagID = 0; tagID < pTLogDriverContext->numTagsPerServer; tagID++) {
			actors.emplace_back(pTLogDriverContext->peekCommitMessages(processID, tagID));
		}
	}

	wait(waitForAll(actors));

	// tell tLog actors to initiate shutdown.
	for (processID = 0; processID < pTLogDriverContext->numLogServers; processID++) {
		pTLogDriverContext->pTLogContextList[processID]->MockTLogTestCompleted.send(true);
	}

	return Void();
}

// wait for all tLogs to be created. Then start actor to do push, then
// start actors to do peeks, then signal transactions can start.

ACTOR Future<Void> buildTLogSet(std::shared_ptr<TLogDriverContext> pTLogDriverContext) {
	state TLogSet tLogSet;
	state uint16_t processID = 0;

	for (; processID < pTLogDriverContext->numLogServers; processID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
		bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
		ASSERT_EQ(isCreated, true);

		// setup log system and tlog group
		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->MockTLogInterface));
		tLogSet.tLogLocalities.push_back(LocalityData());
		tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
		tLogSet.locality = 0;
		tLogSet.isLocal = true;
		tLogSet.tLogVersion = TLogVersion::V6;
		tLogSet.tLogReplicationFactor = 1;
	}
	pTLogDriverContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);
	for (processID = 0; processID < pTLogDriverContext->numLogServers; processID++) {
		std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
		// start transactions
		pTLogContext->TLogStarted.send(true);
	}
	return Void();
}

// create actors and return them in a list.
std::vector<Future<Void>> startTLogTestActors(const UnitTestParameters& params) {
	TraceEvent("MockTLogTestEnterStartTestActors");

	std::vector<Future<Void>> actors;
	std::shared_ptr<TLogDriverContext> pTLogDriverContext = initTLogDriverContext(TestTLogDriverOptions(params));
	const TestTLogDriverOptions& tLogOptions = pTLogDriverContext->tLogOptions;
	std::shared_ptr<TLogContext> pTLogContext(new TLogContext());
	pTLogDriverContext->pTLogContextList.push_back(pTLogContext);

	// Create a single TLog
	actors.emplace_back(getTLogCreateActor(pTLogDriverContext, tLogOptions, 0 /* processID */));

	// Create TLog group to drive tansactions
	actors.emplace_back(getTLogGroupActor(pTLogDriverContext));
	TraceEvent("MockTLogTestExitStartTestActors");

	return actors;
}

// create actors and return them in a list.
std::vector<Future<Void>> startTLogGroupActors(const UnitTestParameters& params) {
	std::vector<Future<Void>> actors;
	std::shared_ptr<TLogDriverContext> pTLogDriverContext = initTLogDriverContext(TestTLogDriverOptions(params));
	const TestTLogDriverOptions& tLogOptions = pTLogDriverContext->tLogOptions;

	// Create one TLog for each log server. Only a single group of log servers is supported.
	for (int processID = 0; processID < pTLogDriverContext->numLogServers; processID++) {
		std::shared_ptr<TLogContext> pTLogContext(new TLogContext(processID));
		pTLogDriverContext->pTLogContextList.push_back(pTLogContext);
	}
	for (int processID = 0; processID < pTLogDriverContext->numLogServers; processID++) {
		actors.emplace_back(getTLogCreateActor(pTLogDriverContext, tLogOptions, processID));
	}

	// start fake proxy, which will create peek and commit actors.
	actors.emplace_back(runTestsTLogGroupActors(pTLogDriverContext));
	return actors;
}

// This test creates tLogs and pushes data to them. The tLogs are then locked. A new "generation"
// of tLogs is then created. These enter recover mode and pull data from the old generation.
// The data is peeked from the new generation and validated.

ACTOR Future<Void> startTestsTLogRecoveryActors(UnitTestParameters params) {
	state std::vector<Future<Void>> tLogActors;
	state std::shared_ptr<TLogDriverContext> pTLogDriverContextEpochOne =
	    initTLogDriverContext(TestTLogDriverOptions(params));
	const TestTLogDriverOptions& tLogOptions = pTLogDriverContextEpochOne->tLogOptions;
	state uint16_t instanceIndex = 0;

	TraceEvent("MockTLogTestEnterTLogRecoveryActors");

	// Create the first "old" generation of tLogs
	std::shared_ptr<TLogContext> pTLogContext(new TLogContext(instanceIndex));
	pTLogDriverContextEpochOne->pTLogContextList.push_back(pTLogContext);
	tLogActors.emplace_back(getTLogCreateActor(pTLogDriverContextEpochOne, tLogOptions, instanceIndex));

	// wait for tLogs to be created, and signal pushes can start
	wait(buildTLogSet(pTLogDriverContextEpochOne));

	PromiseStream<Future<Void>> promises;
	pTLogDriverContextEpochOne->ls = ILogSystem::fromServerDBInfo(
	    pTLogDriverContextEpochOne->logID, pTLogDriverContextEpochOne->dbInfo, false, promises);

	// Push commits, but do not peek from them. They will be peeked by the next
	// generation of tLogs.
	wait(pTLogDriverContextEpochOne->sendPushMessages());

	// Done with old generation. Lock the old generation of tLogs.
	TLogLockResult data = wait(
	    pTLogDriverContextEpochOne->pTLogContextList[instanceIndex]->MockTLogInterface.lock.getReply<TLogLockResult>());
	TraceEvent("MockTLogLockResult").detail("KCV", data.knownCommittedVersion);

	// Setup configuration for a new generation of tLogs
	state std::shared_ptr<TLogDriverContext> pTLogDriverContextEpochTwo =
	    initTLogDriverContext(TestTLogDriverOptions(params), 0);

	// next epoch
	pTLogDriverContextEpochTwo->epoch = pTLogDriverContextEpochOne->epoch + 1;

	std::shared_ptr<TLogContext> pNewTLogContext(new TLogContext(instanceIndex));
	pTLogDriverContextEpochTwo->pTLogContextList.push_back(pNewTLogContext);

	// The InitializeTLogRequest includes what versions should be recovered from
	// the previous generation. Those values are manually set here.
	InitializeTLogRequest req;
	req.recruitmentID = pTLogDriverContextEpochTwo->dbInfo.logSystemConfig.recruitmentID;
	req.recoverAt = 3;
	req.startVersion = 2;
	req.remoteTag = Tag(tagLocalityRemoteLog, 0);
	req.recoveryTransactionVersion = 1;
	req.knownCommittedVersion = 1;
	req.epoch = pTLogDriverContextEpochTwo->epoch;
	req.logVersion = TLogVersion::V6;
	req.locality = 0;
	req.isPrimary = true;
	req.logRouterTags = 0; // the number of LR, not spun up for recovery
	req.recoverTags = { Tag(0, 0) };
	req.recoverFrom = pTLogDriverContextEpochOne->dbInfo.logSystemConfig;
	req.recoverFrom.logRouterTags = 0; // move

	// Describe the old log generation. There is one such structure for each generation.
	OldTLogConf oldTLogConf;
	oldTLogConf.tLogs = pTLogDriverContextEpochOne->dbInfo.logSystemConfig.tLogs;
	oldTLogConf.tLogs[0].locality = 0;
	oldTLogConf.tLogs[0].isLocal = true;
	oldTLogConf.epochBegin = 1;
	oldTLogConf.epochEnd = 3;
	oldTLogConf.logRouterTags = 0;
	oldTLogConf.recoverAt = 1; // recoverAt version for old epoch, not new one
	oldTLogConf.epoch = 1; // old epoch, not new one
	pTLogDriverContextEpochTwo->dbInfo.logSystemConfig.oldTLogs.push_back(oldTLogConf);
	pTLogDriverContextEpochTwo->tagLocality = 0;

	const TestTLogDriverOptions& tLogOptions = pTLogDriverContextEpochTwo->tLogOptions;

	// Create the new generation's tLogs
	tLogActors.emplace_back(getTLogCreateActor(pTLogDriverContextEpochTwo,
	                                           tLogOptions,
	                                           instanceIndex,
	                                           &req,
	                                           pTLogDriverContextEpochOne->pTLogContextList[instanceIndex]->tLogID));

	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContextEpochTwo->pTLogContextList[0];
	bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
	ASSERT_EQ(isCreated, true);
	pTLogContext->TLogStarted.send(true);

	TraceEvent("MockTLogStartTestsTLogRecovery");

	// read data from old generation of tLogs
	wait(pTLogDriverContextEpochTwo->peekCommitMessages(0, 0));

	// signal that the old tLogs can be destroyed
	pTLogDriverContextEpochTwo->pTLogContextList[instanceIndex]->MockTLogTestCompleted.send(true);
	pTLogDriverContextEpochOne->pTLogContextList[instanceIndex]->MockTLogTestCompleted.send(true);

	// wait for the tLogs to destruct
	wait(waitForAll(tLogActors));

	TraceEvent("MockTLogTestExitTLogRecoveryActors");

	return Void();
}

// test a single tLog
TEST_CASE("/fdbserver/test/mocktlogdriver") {
	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
	wait(waitForAll(startTLogTestActors(params)));
	TraceEvent("MockTLogExitCreateInstance");
	return Void();
}

// test a group of tLogs
TEST_CASE("/fdbserver/test/mocktloggroupdriver") {
	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
	wait(waitForAll(startTLogGroupActors(params)));
	TraceEvent("MockTLogExitCreateInstance");
	return Void();
}

// test a group of tLogs and recovery
TEST_CASE("/fdbserver/test/mocktlogrecoverydriver") {
	FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
	wait(startTestsTLogRecoveryActors(params));
	TraceEvent("MockTLogExitCreateInstance");
	return Void();
}