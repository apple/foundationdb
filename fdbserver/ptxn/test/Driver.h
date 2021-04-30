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
#include <fdbserver/WorkerInterface.actor.h>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/StorageServerInterface.h"
#include "fdbserver/ptxn/TLogInterface.h"

namespace ptxn {

struct CommitValidationRecord {
	bool tLogValidated = false;
	bool storageServerValidated = false;

	bool validated() const;
};

struct CommitRecord {
	Version version;
	StorageTeamID teamID;
	std::vector<MutationRef> mutations;

	CommitValidationRecord validation;

	CommitRecord(const Version& version, const StorageTeamID& teamID, std::vector<MutationRef>&& mutationRef);
};

struct TestDriverContext {
	// Teams
	int numTeamIDs;
	std::vector<StorageTeamID> teamIDs;

	MessageTransferModel messageTransferModel;

	// Proxies
	bool useFakeProxy;
	int numProxies;

	// TLog
	bool useFakeTLog;
	int numTLogs;
	int numTLogGroups;
	std::vector<TLogGroup> tLogGroups;
	std::unordered_map<TLogGroupID, std::shared_ptr<TLogInterfaceBase>> tLogGroupLeaders;
	std::vector<std::shared_ptr<TLogInterfaceBase>> tLogInterfaces;
	std::unordered_map<StorageTeamID, std::shared_ptr<TLogInterfaceBase>> teamIDTLogInterfaceMapper;
	std::shared_ptr<TLogInterfaceBase> getTLogInterface(const StorageTeamID&);

	// Storage Server
	bool useFakeStorageServer;
	int numStorageServers;
	std::vector<std::shared_ptr<StorageServerInterfaceBase>> storageServerInterfaces;
	std::unordered_map<StorageTeamID, std::shared_ptr<StorageServerInterfaceBase>> teamIDStorageServerInterfaceMapper;
	std::shared_ptr<StorageServerInterfaceBase> getStorageServerInterface(const StorageTeamID&);

	// Stores the generated commits
	Arena mutationsArena;
	std::vector<CommitRecord> commitRecord;
};

// Print out *ALL* commits that has triggered.
void printCommitRecord(const std::vector<CommitRecord>& records);

// Print out those commits are not being completed, i.e., persisted in TLogs and Storage Servers
void printNotValidatedRecords(const std::vector<CommitRecord>& records);

// Check if all records are validated
bool isAllRecordsValidated(const std::vector<CommitRecord>& records);

// Check if a set of mutations is coming from a previous know commit
void verifyMutationsInRecord(std::vector<CommitRecord>& record,
                             const Version&,
                             const StorageTeamID&,
                             const std::vector<MutationRef>& mutations,
                             std::function<void(CommitValidationRecord&)> validateUpdater);

std::shared_ptr<TestDriverContext> initTestDriverContext();

void startFakeProxy(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

void startFakeTLog(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

void startFakeStorageServer(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext);

} // namespace ptxn

#endif // FDBSERVER_PTXN_TEST_DRIVER_H
