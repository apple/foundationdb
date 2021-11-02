/*
 * Driver.h
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

#ifndef FDBSERVER_PTXN_TEST_DRIVER_H
#define FDBSERVER_PTXN_TEST_DRIVER_H

#include <functional>
#include <memory>
#include <unordered_map>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/StorageServerInterface.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/ptxn/test/CommitUtils.h"
#include "flow/UnitTest.h"

namespace ptxn::test {

// Driver options for starting mock environment.
struct TestDriverOptions {
	static const int DEFAULT_NUM_COMMITS = 3;
	static const int DEFAULT_NUM_TEAMS = 10;
	static const int DEFAULT_NUM_PROXIES = 1;
	static const int DEFAULT_NUM_TLOGS = 3;
	static const int DEFAULT_NUM_TLOG_GROUPS = 4;
	static const int DEFAULT_NUM_STORAGE_SERVERS = 3;
	static const int DEFAULT_NUM_RESOLVERS = 2;
	static const int DEFAULT_SKIP_COMMIT_VALIDATION = false;
	static const MessageTransferModel DEFAULT_MESSAGE_TRANSFER_MODEL = MessageTransferModel::TLogActivelyPush;

	int numCommits;
	int numStorageTeams;
	int numProxies;
	int numTLogs;
	int numTLogGroups;
	int numStorageServers;
	int numResolvers;
	bool skipCommitValidation;
	MessageTransferModel transferModel;

	explicit TestDriverOptions(const UnitTestParameters&);
};

struct TestDriverContext {
	// Num commits to be created
	int numCommits;

	// commit version gap
	int commitVersionGap;

	// Teams
	int numStorageTeamIDs;
	std::vector<StorageTeamID> storageTeamIDs;

	MessageTransferModel messageTransferModel;

	// whether to skip persistence validation
	bool skipCommitValidation;

	// Sequencer
	std::shared_ptr<MasterInterface> sequencerInterface;

	// Proxies
	bool useFakeProxy;
	int numProxies;

	// Resolvers
	int numResolvers;
	std::vector<std::shared_ptr<ResolverInterface>> resolverInterfaces;

	// TLog
	bool useFakeTLog;
	int numTLogs;
	int numTLogGroups;
	std::vector<TLogGroup> tLogGroups;
	std::vector<std::vector<ptxn::TLogGroup>> groupsPerTLog;
	std::unordered_map<TLogGroupID, std::shared_ptr<TLogInterfaceBase>> tLogGroupLeaders;
	std::unordered_map<TLogGroupID, Version> tLogGroupVersion;
	std::vector<std::shared_ptr<TLogInterfaceBase>> tLogInterfaces;
	std::unordered_map<StorageTeamID, TLogGroupID> storageTeamIDTLogGroupIDMapper;
	std::shared_ptr<TLogInterfaceBase> getTLogLeaderByStorageTeamID(const StorageTeamID& storageTeamID);
	// For a given storageTeamID, returns the pair(prevVersion, currentVersion).
	std::pair<Version, Version> getCommitVersionPair(const TLogGroupID& tLogGroupID, const Version& currentVersion);

	// Storage Server
	bool useFakeStorageServer;
	int numStorageServers;
	std::vector<std::shared_ptr<StorageServerInterfaceBase>> storageServerInterfaces;
	std::vector<StorageServerInterface> storageServers;
	std::unordered_map<StorageTeamID, std::shared_ptr<StorageServerInterfaceBase>>
	    storageTeamIDStorageServerInterfaceMapper;
	std::shared_ptr<StorageServerInterfaceBase> getStorageServerInterface(const StorageTeamID&);

	// Generated commits
	CommitRecord commitRecord;
};

// Returns an initialized TestDriverContext with default values specified in "options".
std::shared_ptr<TestDriverContext> initTestDriverContext(const TestDriverOptions& options);

// Starts all fake resolvers specified in the pTestDriverContext.
void startFakeResolver(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

// Starts all fake proxies specified in the pTestDriverContext.
void startFakeProxy(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

// Starts all fake TLogs specified in the pTestDriverContext.
void startFakeTLog(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

// Starts all fake Storage Servers specified in the pTestDriverContext.
void startFakeStorageServer(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

// Starts all fake sequencers specified in the pTestDriverContext.
void startFakeSequencer(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_DRIVER_H
