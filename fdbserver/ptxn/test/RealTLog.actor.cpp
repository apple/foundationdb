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

// This test starts a single tLog and runs commits, peeks, and pops using the tLog interface.
// The test's purpose is to help with microbenchmarks and experimentation.
// The storage server is assumed to run at infinite speed.

// In the future this could support multiple tlogs (single or multiple groups),
// along with higher level components such as TagPartitionedLogSystem.

namespace ptxn::test {

// build test state.
std::shared_ptr<TLogDriverContext> initTLogDriverContext(const TestDriverOptions& options,
                                                         const TestTLogDriverOptions tLogOptions) {
	std::shared_ptr<TLogDriverContext> context(new TLogDriverContext());
	context->logID = deterministicRandom()->randomUniqueID();
	context->workerID = deterministicRandom()->randomUniqueID();
	context->diskQueueBasename = tLogOptions.diskQueueBasename;
	context->numCommits = options.numCommits;
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
	state std::shared_ptr<TLogContext> pTLogContext(new TLogContext());
	pTLogContext->tagProcessID = processID;
	pTLogContext->tLogID = deterministicRandom()->randomUniqueID();
	pTLogContext->persistentQueue = openDiskQueue(pTLogDriverContext->diskQueueBasename,
	                                              tLogOptions.diskQueueExtension,
	                                              pTLogContext->tLogID,
	                                              DiskQueueVersion::V1);
	pTLogContext->persistentData = keyValueStoreMemory(tLogOptions.kvStoreFilename,
	                                                   pTLogContext->tLogID,
	                                                   tLogOptions.kvMemoryLimit,
	                                                   "fdr",
	                                                   KeyValueStoreType::MEMORY_RADIXTREE);
	pTLogDriverContext->pTLogContext = pTLogContext;

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
	                             pTLogDriverContext->diskQueueBasename,
	                             isDegraded,
	                             activeSharedTLog);

	// start tlog.
	state InitializeTLogRequest initTLogReq = InitializeTLogRequest();
	Tag tag(pTLogDriverContext->tagLocality, 0 /* tag */);
	std::vector<Tag> tags = { tag };
	initTLogReq.allTags = tags;
	initTLogReq.locality = 0;
	initTLogReq.isPrimary = true;

	TLogInterface interface = wait(promiseStream.getReply(initTLogReq));
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
                                                              TLogDriverContext* pTLogDriverContext) {
	bool tLogReady = wait(pTLogDriverContext->pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	state Version prev = 0;
	state Version next = 1;
	state int i = 0;

	for (; i < pTLogDriverContext->numCommits; i++) {
		Standalone<StringRef> key = StringRef(format("key %d", i));
		Standalone<StringRef> val = StringRef(format("value %d", i));
		MutationRef m(MutationRef::Type::SetValue, key, val);
		Tag tag(pTLogDriverContext->tagLocality, pTLogDriverContext->pTLogContext->tagProcessID);

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

		::TLogCommitReply reply = wait(pTLogDriverContext->pTLogContext->realTLogInterface.commit.getReply(request));
		ASSERT_LE(reply.version, next);
		prev++;
		next++;
	}

	return Void();
}

// send peek/pop through TLog interface.
ACTOR Future<Void> TLogDriverContext::peekCommitMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                                              TLogDriverContext* pTLogDriverContext) {
	bool tLogReady = wait(pTLogDriverContext->pTLogContext->TLogStarted.getFuture());
	ASSERT_EQ(tLogReady, true);

	state Tag tag(pTLogDriverContext->tagLocality, pTLogDriverContext->pTLogContext->tagProcessID);
	state Version begin = 1;
	state int i;

	for (i = 0; i < pTLogDriverContext->numCommits; i++) {
		// wait for next message commit

		::TLogPeekRequest request(begin, tag, false, false);
		::TLogPeekReply reply =
		    wait(pTLogDriverContext->pTLogContext->realTLogInterface.peekMessages.getReply(request));

		// validate versions
		ASSERT_GE(reply.maxKnownVersion, i);

		// deserialize package, first the version header
		ArenaReader rd = ArenaReader(reply.arena, reply.messages, AssumeVersion(g_network->protocolVersion()));
		ASSERT_EQ(*(int32_t*)rd.peekBytes(4), VERSION_HEADER);
		TagsAndMessage messageAndTags = TagsAndMessage();
		int32_t dummy; // skip past VERSION_HEADER
		Version ver;
		rd >> dummy >> ver;

		int32_t messageLength;
		uint16_t tagCount;
		uint32_t sub;
		// deserialize transaction header
		// deserialize span id
		if (FLOW_KNOBS->WRITE_TRACING_ENABLED) {
			rd >> messageLength >> sub >> tagCount;
			rd.readBytes(tagCount * sizeof(Tag));

			SpanContextMessage contextMessage;
			rd >> contextMessage;
		}
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
		wait(pTLogDriverContext->pTLogContext->realTLogInterface.popMessages.getReply(requestPop));

		begin++;
	}

	return Void();
}

// wait for all tLogs to be created (currently only one). Then build tLog group, then
// signal transactions can start.
ACTOR Future<Void> getTLogGroupActor(std::shared_ptr<TestDriverContext> pTestDriverContext,
                                     std::shared_ptr<TLogDriverContext> pTLogDriverContext) {
	// create tLog
	state std::shared_ptr<TLogContext> pTLogContext = pTLogDriverContext->pTLogContext;
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
	pTLogDriverContext->ls = ILogSystem::fromServerDBInfo(pTLogDriverContext->logID, pTLogDriverContext->dbInfo, false);

	// start transactions
	pTLogDriverContext->pTLogContext->TLogStarted.send(true);
	Future<Void> commit = pTLogDriverContext->sendCommitMessages(pTestDriverContext);
	Future<Void> peek = pTLogDriverContext->peekCommitMessages(pTestDriverContext);
	wait(commit && peek);

	// tell tLog actor to initiate shutdown.
	pTLogDriverContext->pTLogContext->realTLogTestCompleted.send(true);

	return Void();
}

// create actors and return them in a list.
void startActors(std::vector<Future<Void>>& actors,
                 std::shared_ptr<TestDriverContext> pTestDriverContext,
                 std::shared_ptr<TLogDriverContext> pTLogDriverContext,
                 const TestTLogDriverOptions tLogOptions) {
	actors.emplace_back(
	    getTLogCreateActor(pTestDriverContext, pTLogDriverContext, tLogOptions, 0 /* processID used in tag */));
	actors.emplace_back(getTLogGroupActor(pTestDriverContext, pTLogDriverContext));
}

} // namespace ptxn::test

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
