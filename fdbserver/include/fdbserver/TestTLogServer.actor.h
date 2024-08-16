/*
 * TestTLogServer.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_TEST_TLOG_ACTOR_G_H)
#define FDBSERVER_TEST_TLOG_ACTOR_G_H
#include "fdbserver/TestTLogServer.actor.g.h"
#elif !defined(FDBSERVER_TEST_TLOG_ACTOR_H)
#define FDBSERVER_TEST_TLOG_ACTOR_H

#include <memory>
#include <unordered_map>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/TLogInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

#pragma once

struct TestTLogOptions {
	std::string diskQueueBasename;
	std::string diskQueueExtension;
	std::string kvStoreFilename;
	std::string dataFolder;
	std::string kvStoreExtension;
	std::vector<Version> versions;
	int64_t kvMemoryLimit;
	uint32_t numTags;
	uint32_t numLogServers;
	uint32_t numCommits;
	uint32_t initVersion;
	uint32_t recover;
	uint32_t replicaCount;

	explicit TestTLogOptions(const UnitTestParameters& params) {
		diskQueueBasename = params.get("diskQueueBasename").orDefault("folder");
		diskQueueExtension = params.get("diskQueueFileExtension").orDefault("ext");
		kvStoreExtension = params.get("diskQueueFileExtension").orDefault("fdr");
		kvStoreFilename = params.get("kvStoreFilename").orDefault("kvstore");
		dataFolder = params.get("dataFolder").orDefault("simfdb");
		kvMemoryLimit = params.getDouble("kvMemoryLimit").orDefault(0x500e6);
		numTags = params.getInt("numTags").orDefault(1);
		numLogServers = params.getInt("numLogServers").orDefault(2);
		numCommits = params.getInt("numCommits").orDefault(10);
		initVersion = params.getInt("initVersion").orDefault(1);
		recover = params.getInt("recover").orDefault(1);
		replicaCount = params.getInt("replicaCount").orDefault(2);
	}
};

// single tLog state
struct TLogContext : NonCopyable, public ReferenceCounted<TLogContext> {
	UID tLogID;
	::TLogInterface TestTLogInterface;
	::TLogInterface MockLogRouterInterface;
	PromiseStream<InitializeTLogRequest> init;
	uint16_t tagProcessID;
	IKeyValueStore* persistentData;
	IDiskQueue* persistentQueue;

	// test states
	Promise<bool> TLogCreated;
	Promise<bool> TLogStarted;
	Promise<bool> TestTLogServerCompleted;

	TLogContext(uint32_t inProcessID = 0) : tagProcessID(inProcessID){};
};

// test state
struct TLogTestContext : NonCopyable, public ReferenceCounted<TLogTestContext> {

	ACTOR static Future<Void> sendPushMessages(TLogTestContext* pTLogTestContext);

	Future<Void> sendPushMessages() { return sendPushMessages(this); }

	ACTOR static Future<Void> sendCommitMessages(TLogTestContext* pTLogTestContext, uint16_t processID);

	Future<Void> sendCommitMessages(uint16_t processID = 0) { return sendCommitMessages(this, processID); }

	Future<Void> peekCommitMessages(uint32_t tagID = 0, uint16_t logID = 0) {
		return peekCommitMessages(this, tagID, logID);
	}

	ACTOR static Future<Void> peekCommitMessages(TLogTestContext* pTLogTestContext, uint16_t logGroupID, uint32_t tag);

	TLogTestContext(TestTLogOptions& tLogOptions) : tLogOptions(tLogOptions), epoch(1) {}

	// paramaters
	std::string diskQueueBasename;
	uint32_t numCommits;
	uint32_t numTags;
	uint32_t numLogServers;
	uint32_t initVersion;
	uint32_t recover;

	// tLog state
	std::vector<Reference<TLogContext>> pTLogContextList;
	TestTLogOptions tLogOptions;
	std::map<std::tuple<uint32_t, uint32_t>, std::vector<Version>> commitHistory;

	// system state
	UID logID;
	UID workerID;
	Reference<ILogSystem> ls;
	ServerDBInfo dbInfo;
	Reference<AsyncVar<ServerDBInfo>> dbInfoRef;
	Standalone<StringRef> dcID;
	Optional<Standalone<StringRef>> zoneID;
	int8_t tagLocality;
	uint32_t epoch;
	const uint32_t primaryLocality = 0;
};

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_TEST_TLOG_ACTOR_G_H
