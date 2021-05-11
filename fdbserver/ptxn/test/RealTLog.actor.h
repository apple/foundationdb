/*
 * FakeTLog.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_PTXN_TEST_REALTLOG_ACTOR_G_H)
#define FDBSERVER_PTXN_TEST_REALTLOG_ACTOR_G_H
#include "fdbserver/ptxn/test/RealTLog.actor.g.h"
#elif !defined(FDBSERVER_PTXN_TEST_REALTLOG_ACTOR_H)
#define FDBSERVER_PTXN_TEST_REALTLOG_ACTOR_H

#include <memory>
#include <unordered_map>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/LogSystem.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/StorageServerInterface.h"
#include "flow/flow.h"

#include "flow/actorcompiler.h" // has to be last include

#pragma once

namespace ptxn::test {

struct TestTLogDriverOptions {
	std::string diskQueueBasename;
	std::string diskQueueExtension;
	std::string kvStoreFilename;
	int64_t kvMemoryLimit;
	int numShardsPerGroup;
	int numLogGroups;

	explicit TestTLogDriverOptions(const UnitTestParameters& params) {
		diskQueueBasename = params.get("diskQueueBasename").orDefault("folder");
		diskQueueExtension = params.get("diskQueueFileExtension").orDefault("ext");
		kvStoreFilename = params.get("kvStoreFilename").orDefault("kvstore");
		kvMemoryLimit = params.getDouble("kvMemoryLimit").orDefault(0x500e6);
		numShardsPerGroup = params.getInt("numShardsPerGroup").orDefault(2);
		numLogGroups = params.getInt("numLogGroups").orDefault(1);
	}
};

// state maintained for a single tlog.
struct TLogContext {
	UID tLogID;
	::TLogInterface realTLogInterface;
	uint16_t tagProcessID;
	IKeyValueStore* persistentData;
	IDiskQueue* persistentQueue;

	// test states
	Promise<bool> TLogCreated;
	Promise<bool> TLogStarted;
	Promise<bool> realTLogTestCompleted;

	TLogContext(int inProcessID) : tagProcessID(inProcessID){};
};

// state maintained for all tlogs.
struct TLogDriverContext {

	Future<Void> sendCommitMessages(std::shared_ptr<TestDriverContext> pTestDriverContext, uint16_t processID) {
		return sendCommitMessages_impl(pTestDriverContext, this, processID);
	}

	Future<Void> sendPushMessages(std::shared_ptr<TestDriverContext> pTestDriverContext, uint16_t processID) {
		return sendPushMessages_impl(pTestDriverContext, this, processID);
	}

	ACTOR static Future<Void> sendCommitMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
	                                                  TLogDriverContext* pTLogDriverContext,
	                                                  uint16_t processID);

	ACTOR static Future<Void> sendPushMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
	                                                TLogDriverContext* pTLogDriverContext,
	                                                uint16_t processID);

	Future<Void> peekCommitMessages(std::shared_ptr<TestDriverContext> pTestDriverContext, uint16_t processID, uint32_t tag) {
		return peekCommitMessages_impl(pTestDriverContext, this, processID, tag);
	}

	ACTOR static Future<Void> peekCommitMessages_impl(std::shared_ptr<TestDriverContext> pTestDriverContext,
	                                                  TLogDriverContext* pTLogDriverContext,
	                                                  uint16_t processID,
													uint32_t tag);


	UID logID;
	UID workerID;

	// paramaters
	std::string diskQueueBasename;
	int numCommits;
	int numShardsPerGroup;
	int numLogGroups;

	// test driver state
	std::vector<std::shared_ptr<TLogContext>> pTLogContextList;

	// fdb state
	Reference<ILogSystem> ls;
	ServerDBInfo dbInfo;
	TLogSet tLogSet;
	Standalone<StringRef> dcID;
	Optional<Standalone<StringRef>> zoneID;
	int8_t tagLocality;
};

} // namespace ptxn::test

#include "flow/unactorcompiler.h"
#endif // FDBSERVER_PTXN_TEST_REALTLOG_ACTOR_H
