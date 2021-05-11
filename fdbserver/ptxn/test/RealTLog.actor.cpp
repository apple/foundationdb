/*
 * TLog.actor.cpp
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

#include "fdbserver/ptxn/test/RealTLog.actor.h"

#include <vector>

#include "fdbrpc/Locality.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/TLogInterface.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/IDiskQueue.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/LogSystem.h"
#include "flow/actorcompiler.h" // has to be last include

// This test starts a tLogs and runs commits, peeks, and pops. There is one test that calls
// the tLog interface directly and another using the LogSystem's push interface.
//
// Multiple log groups are supported. In the future more than one shard could be supported
// per log group, but right now tags are hardcoded to 0.
//
// The test's purpose is to help with microbenchmarks and experimentation.
// The storage server is assumed to run at infinite speed.

namespace ptxn::test {

// build test state.
std::shared_ptr<TLogDriverContext> initTLogDriverContext(const TestDriverOptions& options,
                                                         const TestTLogDriverOptions tLogOptions) {
	std::shared_ptr<TLogDriverContext> context(new TLogDriverContext());
	context->logID = deterministicRandom()->randomUniqueID();
	context->workerID = deterministicRandom()->randomUniqueID();
	context->diskQueueBasename = tLogOptions.diskQueueBasename;
	context->numCommits = options.numCommits;
	context->numShardsPerGroup = tLogOptions.numShardsPerGroup;
	context->numLogGroups = tLogOptions.numLogGroups;
	context->dcID = LiteralStringRef("test");
	context->tagLocality = 0; // one data center.
	context->dbInfo = ServerDBInfo();
	context->dbInfo.logSystemConfig.logSystemType = LogSystemType::tagPartitioned;

	return context;
}

// run a single tLog.
ACTOR Future<Void> getTLogCreateActor(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                      std::shared_ptr<TLogDriverContext> pTLogDriverContext,
                                      TestTLogDriverOptions tLogOptions,
                                      uint16_t processID) {

	// build per-tLog state.
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
	pTLogContext->tagProcessID = processID;
	pTLogContext->tLogID = deterministicRandom()->randomUniqueID();

	// delete old disk queue files, then create persistent storage
	std::string diskQueueBasename = pTLogDriverContext->diskQueueBasename + "." + std::to_string(processID) + ".";
	deleteFile(diskQueueBasename + "0." + tLogOptions.diskQueueExtension);
	deleteFile(diskQueueBasename + "1." + tLogOptions.diskQueueExtension);
	pTLogContext->persistentQueue =
	    openDiskQueue(diskQueueBasename, tLogOptions.diskQueueExtension, pTLogContext->tLogID, DiskQueueVersion::V1);
	std::string kvStoreFilename = tLogOptions.kvStoreFilename + "." + std::to_string(processID) + ".";
	pTLogContext->persistentData = keyValueStoreMemory(
	    kvStoreFilename, pTLogContext->tLogID, tLogOptions.kvMemoryLimit, "fdr", KeyValueStoreType::MEMORY_RADIXTREE);

	// prepare tLog construction.
	Standalone<StringRef> machineID = LiteralStringRef("machine");
	LocalityData localities(
	    Optional<Standalone<StringRef>>(), pTLogDriverContext->zoneID, machineID, pTLogDriverContext->dcID);
	localities.set(LiteralStringRef("datacenter"), pTLogDriverContext->dcID);
	Reference<AsyncVar<ServerDBInfo>> dbInfoRef = makeReference<AsyncVar<ServerDBInfo>>(pTLogDriverContext->dbInfo);
	Reference<AsyncVar<bool>> isDegraded = FlowTransport::transport().getDegraded();
	Reference<AsyncVar<UID>> activeSharedTLog(new AsyncVar<UID>(pTLogContext->tLogID));
	state PromiseStream<InitializeTLogRequest> promiseStream = PromiseStream<InitializeTLogRequest>();
	Promise<Void> oldLog;
	Promise<Void> recovery;

	// construct tLog.
	state Future<Void> tl = tLog(pTLogContext->persistentData,
	                             pTLogContext->persistentQueue,
	                             dbInfoRef,
	                             localities,
	                             promiseStream,
	                             pTLogContext->tLogID,
	                             pTLogDriverContext->workerID,
	                             false, /* restoreFromDisk */
	                             oldLog,
	                             recovery,
	                             diskQueueBasename,
	                             isDegraded,
	                             activeSharedTLog);

	// start tlog.
	state InitializeTLogRequest initTlogReq = InitializeTLogRequest();
	initTlogReq.isPrimary = true;
	TLogInterface interface = wait(promiseStream.getReply(initTlogReq));
	pTLogContext->realTLogInterface = interface;

	// inform other actors tLog is ready.
	pTLogContext->TLogCreated.send(true);

	// wait for either test completion or abnormal failure.
	choose {
		when(wait(tl)) {}
		when(bool testCompleted = wait(pTLogContext->realTLogTestCompleted.getFuture())) {
			ASSERT_EQ(testCompleted, true);
		}
	}

	return Void();
}

// sent commits through TLog interface.
ACTOR Future<Void> TLogDriverContext::sendCommitMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                                              TLogDriverContext* pTLogDriverContext,
                                                              uint16_t processID) {
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	state Version prev = 0;
	state Version next = 1;
	state int i = 0;
	for (; i < pTLogDriverContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);
		Tag tag(pTLogDriverContext->tagLocality, 1 /* tag */);

		// build commit request
		LogPushData toCommit(pTLogDriverContext->ls);
		UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(spanID);
		vector<Tag> tags = { tag };
		toCommit.addTags(tags);
		toCommit.writeTypedMessage(m);
		int location = 0;
		Standalone<StringRef> msg = toCommit.getMessages(location);

		// send commit and wait for reply.
		::TLogCommitRequest request(
		    SpanID(), msg.arena(), prev, next, prev, prev, msg, deterministicRandom()->randomUniqueID());
		::TLogCommitReply reply = wait(pTLogContext->realTLogInterface.commit.getReply(request));
		ASSERT_LE(reply.version, next);
		prev++;
		next++;
	}

	return Void();
}

// sent commits through TLog interface.
ACTOR Future<Void> TLogDriverContext::sendPushMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                                            TLogDriverContext* pTLogDriverContext,
                                                            uint16_t processID) {
	for (processID = 0; i < pTLogDriverContext->numShardsPerGroup; i++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
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

		// build commit request
		LogPushData toCommit(pTLogDriverContext->ls);
		UID spanID = deterministicRandom()->randomUniqueID();
		toCommit.addTransactionInfo(spanID);

		// just one shard
		Tag tag(pTLogDriverContext->tagLocality, 1);
		vector<Tag> tags = { tag };
		toCommit.addTags(tags);

		toCommit.writeTypedMessage(m);

		Future<Version> loggingComplete =
		    pTLogDriverContext->ls->push(prev, next, prev, prev, toCommit, deterministicRandom()->randomUniqueID());
		Version ver = wait(loggingComplete);
		ASSERT_LE(ver, next);
		prev++;
		next++;
	}

	return Void();
}

// send peek/pop through TLog interface for a given tag (shard).
ACTOR Future<Void> TLogDriverContext::peekCommitMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                                              TLogDriverContext* pTLogDriverContext,
                                                              uint16_t processID,
															  uint32_t shardTag) {
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
	bool tLogReady = wait(pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	// peek from the same shard (tag)
	state Tag tag(pTLogDriverContext->tagLocality, 1);

	state Version begin = 1;
	state int i;
	for (i = 0; i < pTLogDriverContext->numCommits; i++) {
		// wait for next message commit
		::TLogPeekRequest request(begin, tag, false, false);
		::TLogPeekReply reply = wait(pTLogContext->realTLogInterface.peekMessages.getReply(request));

		// validate versions
		ASSERT_GE(reply.maxKnownVersion, i);

		// deserialize package, first the version header
		ArenaReader rd = ArenaReader(reply.arena, reply.messages, AssumeVersion(g_network->protocolVersion()));
		ASSERT_EQ(*(int32_t*)rd.peekBytes(4), VERSION_HEADER);
		TagsAndMessage messageAndTags = TagsAndMessage();
		int32_t dummy; // skip past VERSION_HEADER
		Version ver;
		rd >> dummy >> ver;

		// deserialize transaction header
		int32_t messageLength;
		uint16_t tagCount;
		uint32_t sub;
		rd >> messageLength >> sub >> tagCount;
		rd.readBytes(tagCount * sizeof(Tag));

		// deserialize span id
		SpanContextMessage contextMessage;
		rd >> contextMessage;

		// deserialize mutation header
		rd >> messageLength >> sub >> tagCount;
		rd.readBytes(tagCount * sizeof(Tag));

		// deserialize mutation
		MutationRef m;
		rd >> m;

		// validate data
		Standalone<StringRef> expectedKey = StringRef(format("key %d", i));
		Standalone<StringRef> expectedVal = StringRef(format("value %d", i));
		ASSERT_WE_THINK(m.param1 == expectedKey);
		ASSERT_WE_THINK(m.param2 == expectedVal);

		// go directly to pop as there is no SS.
		::TLogPopRequest requestPop(begin, begin, tag);
		wait(pTLogContext->realTLogInterface.popMessages.getReply(requestPop));

		begin++;
	}

	return Void();
}

// wait for all tLogs to be created. Then build tLog group, then
// signal transactions can start.
ACTOR Future<Void> getTLogGroupActor(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                     std::shared_ptr<TLogDriverContext> pTLogDriverContext) {
	// create tLog
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[0];
	bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
	ASSERT_EQ(isCreated, true);

	// setup log system and tlog group
	pTLogDriverContext->tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->realTLogInterface));
	pTLogDriverContext->tLogSet.tLogLocalities.push_back(LocalityData());
	pTLogDriverContext->tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
	pTLogDriverContext->tLogSet.locality = 0;
	pTLogDriverContext->tLogSet.isLocal = true;
	pTLogDriverContext->tLogSet.tLogVersion = TLogVersion::V6;

	pTLogDriverContext->dbInfo.logSystemConfig.tLogs.push_back(pTLogDriverContext->tLogSet);
	PromiseStream<Future<Void>> promises;
	pTLogDriverContext->ls =
	    ILogSystem::fromServerDBInfo(pTLogDriverContext->logID, pTLogDriverContext->dbInfo, false, promises);

	// start transactions
	pTLogDriverContext->pTLogContextList[0]->TLogStarted.send(true);
	Future<Void> commit = pTLogDriverContext->sendCommitMessages(pTestDriverContext, 0);
	Future<Void> peek = pTLogDriverContext->peekCommitMessages(pTestDriverContext, 0, 1);
	wait(commit && peek);

	// tell tLog actor to initiate shutdown.
	pTLogDriverContext->pTLogContextList[0]->realTLogTestCompleted.send(true);

	return Void();
}

// wait for all tLogs to be created. Then build tLog group, then
// signal transactions can start.
ACTOR Future<Void> getProxyActor(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                 std::shared_ptr<TLogDriverContext> pTLogDriverContext) {
	// create tLog
	state uint16_t processID = 0;
	for (; processID < pTLogDriverContext->numShardsPerGroup; processID++) {
		state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContextList[processID];
		bool isCreated = wait(pTLogContext->TLogCreated.getFuture());
		ASSERT_EQ(isCreated, true);

		// setup log system and tlog group
		TLogSet tLogSet;
		tLogSet.tLogs.push_back(OptionalInterface<TLogInterface>(pTLogContext->realTLogInterface));
		tLogSet.tLogLocalities.push_back(LocalityData());
		tLogSet.tLogPolicy = Reference<IReplicationPolicy>(new PolicyOne());
		tLogSet.locality = 0;
		tLogSet.isLocal = true;
		tLogSet.tLogVersion = TLogVersion::V6;
		pTLogDriverContext->dbInfo.logSystemConfig.tLogs.push_back(tLogSet);

		// start transactions
		pTLogContext->TLogStarted.send(true);
	}

	PromiseStream<Future<Void>> promises;
	pTLogDriverContext->ls =
	    ILogSystem::fromServerDBInfo(pTLogDriverContext->logID, pTLogDriverContext->dbInfo, false, promises);

	// start transactions
	state std::vector<Future<Void>> actors;
	actors.emplace_back(pTLogDriverContext->sendPushMessages(pTestDriverContext, processID));
	for (processID = 0; processID < pTLogDriverContext->numShardsPerGroup; processID++) {
		actors.emplace_back(pTLogDriverContext->peekCommitMessages(pTestDriverContext, processID, 1 /* tag */));
	}

	wait(waitForAll(actors));

	// tell tLog actors to initiate shutdown.
	for (processID = 0; processID < pTLogDriverContext->numShardsPerGroup; processID++) {
		pTLogDriverContext->pTLogContextList[processID]->realTLogTestCompleted.send(true);
	}

	return Void();
}

// create actors and return them in a list.
void startActors(std::vector<Future<Void>>& actors,
                 std::shared_ptr<TestDriverContext> pTestDriverContext,
                 std::shared_ptr<TLogDriverContext> pTLogDriverContext,
                 const TestTLogDriverOptions tLogOptions) {
	int processID = 0;
	std::shared_ptr<TLogContext> pTLogContext(new TLogContext(processID));
	pTLogDriverContext->pTLogContextList.push_back(pTLogContext);
	actors.emplace_back(
	    getTLogCreateActor(pTestDriverContext, pTLogDriverContext, tLogOptions, 0 /* processID used in tag */));
	actors.emplace_back(getTLogGroupActor(pTestDriverContext, pTLogDriverContext));
}

// create actors and return them in a list.
void startNewActors(std::vector<Future<Void>>& actors,
                    std::shared_ptr<TestDriverContext> pTestDriverContext,
                    std::shared_ptr<TLogDriverContext> pTLogDriverContext,
                    const TestTLogDriverOptions tLogOptions) {
	for (int processID = 0; processID < pTLogDriverContext->numShardsPerGroup; processID++) {
		std::shared_ptr<TLogContext> pTLogContext(new TLogContext(processID));
		pTLogDriverContext->pTLogContextList.push_back(pTLogContext);
	}
	for (int processID = 0; processID < pTLogDriverContext->numShardsPerGroup; processID++) {
		actors.emplace_back(getTLogCreateActor(pTestDriverContext, pTLogDriverContext, tLogOptions, processID));
	}
	actors.emplace_back(getProxyActor(pTestDriverContext, pTLogDriverContext));
}

} // namespace ptxn::test

// single tLog test
TEST_CASE("/fdbserver/ptxn/test/realtlogdriver") {
	using namespace ptxn::test;

	std::vector<Future<Void>> actors;
	TestDriverOptions driverOptions(params);
	TestTLogDriverOptions tLogOptions(params);
	std::shared_ptr<TestDriverContext> context = initTestDriverContext(driverOptions);
	std::shared_ptr<TLogDriverContext> realTLogContext = initTLogDriverContext(driverOptions, tLogOptions);
	startActors(actors, context, realTLogContext, tLogOptions);
	wait(waitForAll(actors));
	return Void();
}

// multiple tLog test
TEST_CASE("/fdbserver/ptxn/test/realtlogsdriver") {
	using namespace ptxn::test;

	std::vector<Future<Void>> actors;
	TestDriverOptions driverOptions(params);
	TestTLogDriverOptions tLogOptions(params);
	std::shared_ptr<TestDriverContext> context = initTestDriverContext(driverOptions);
	std::shared_ptr<TLogDriverContext> realTLogContext = initTLogDriverContext(driverOptions, tLogOptions);
	startNewActors(actors, context, realTLogContext, tLogOptions);
	wait(waitForAll(actors));
	return Void();
}