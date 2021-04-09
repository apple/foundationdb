/*
 * Driver.actor.cpp
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

#include "fdbserver/ptxn/test/Driver.h"

#include <iostream>
#include <memory>
#include <unordered_map>
#include <utility>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/test/FakeProxy.actor.h"
#include "fdbserver/ptxn/test/FakeStorageServer.actor.h"
#include "fdbserver/ptxn/test/FakeTLog.actor.h"
#include "flow/UnitTest.h"

namespace ptxn {

// FIXME Make this configuration flexible.
// This can be done by modifying the UnitTest and tester part.
static const int NUM_COMMITS = 3;
static const int NUM_TEAMS = 10;
static const int NUM_PROXIES = 1;
static const int NUM_TLOGS = 3;
static const int NUM_STORAGE_SERVERS = 3;
static const MessageTransferModel MESSAGE_TRANSFER_MODEL = MessageTransferModel::TLogActivelyPush;

CommitRecord::CommitRecord(const Version& version_, const TeamID& teamID_, std::vector<MutationRef>&& mutations_)
  : version(version_), teamID(teamID_), mutations(std::move(mutations_)) {}

bool CommitValidationRecord::validated() const { return tLogValidated && storageServerValidated; }

std::shared_ptr<TestDriverContext> initTestDriverContext() {
	std::shared_ptr<TestDriverContext> context(new TestDriverContext());

	context->numTeamIDs = NUM_TEAMS;
	context->messageTransferModel = MESSAGE_TRANSFER_MODEL;

	// FIXME use C++20 range
	for (int i = 0; i < context->numTeamIDs; ++i) {
		context->teamIDs.push_back(TeamID{ deterministicRandom()->randomUniqueID() });
	}

	// Prepare Proxies
	context->numProxies = NUM_PROXIES;

	// Prepare TLogInterfaces
	context->numTLogs = NUM_TLOGS;
	for (int i = 0; i < context->numTLogs; ++i) {
		context->tLogInterfaces.push_back(getNewTLogInterface(context->messageTransferModel));
		context->tLogInterfaces.back()->initEndpoints();
	}

	// Prepare StorageServerInterfaces
	context->numStorageServers = NUM_STORAGE_SERVERS;
	for (int i = 0; i < context->numTLogs; ++i) {
		context->storageServerInterfaces.push_back(getNewStorageServerInterface(context->messageTransferModel));
		context->storageServerInterfaces.back()->initEndpoints();
	}

	// Assign teams to interfaces
	auto assignTeamToInterface = [&](auto& mapper, auto interface) {
		int numInterfaces = interface.size();
		int index = 0;
		for (int i = 0; i < context->numTeamIDs; ++i) {
			const TeamID& teamID = context->teamIDs[i];
			mapper[teamID] = interface[index];

			++index;
			index %= numInterfaces;
		}
	};
	assignTeamToInterface(context->teamIDTLogInterfaceMapper, context->tLogInterfaces);
	assignTeamToInterface(context->teamIDStorageServerInterfaceMapper, context->storageServerInterfaces);

	return context;
}

std::shared_ptr<TLogInterfaceBase> TestDriverContext::getTLogInterface(const TeamID& teamID) {
	return teamIDTLogInterfaceMapper.at(teamID);
}

std::shared_ptr<StorageServerInterfaceBase> TestDriverContext::getStorageServerInterface(const TeamID& teamID) {
	return teamIDStorageServerInterfaceMapper.at(teamID);
}

void startFakeProxy(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	for (int i = 0; i < pTestDriverContext->numProxies; ++i) {
		std::shared_ptr<FakeProxyContext> pFakeProxyContext(new FakeProxyContext{ NUM_COMMITS, pTestDriverContext });
		actors.emplace_back(fakeProxy(pFakeProxyContext));
	}
}

void startFakeTLog(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	for (int i = 0; i < pTestDriverContext->numTLogs; ++i) {
		std::shared_ptr<FakeTLogContext> pFakeTLogContext(
		    new FakeTLogContext{ pTestDriverContext, pTestDriverContext->tLogInterfaces[i] });
		actors.emplace_back(getFakeTLogActor(pTestDriverContext->messageTransferModel, pFakeTLogContext));
	}
}

void startFakeStorageServer(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	for (int i = 0; i < pTestDriverContext->numStorageServers; ++i) {
		std::shared_ptr<FakeStorageServerContext> pFakeStorageServerContext(
		    new FakeStorageServerContext{ pTestDriverContext, pTestDriverContext->storageServerInterfaces[i] });
		actors.emplace_back(
		    getFakeStorageServerActor(pTestDriverContext->messageTransferModel, pFakeStorageServerContext));
	}
}

void printCommitRecord(const std::vector<CommitRecord>& records) {
	std::cout << "Commits from Proxy: \n\n";
	Version currentVersion = 0;
	for (const auto& record : records) {
		if (record.version != currentVersion) {
			std::cout << "\n\tVersion: " << record.version << "\n\n";
			currentVersion = record.version;
		}
		std::cout << "\t\tTeam ID: " << record.teamID.toString() << std::endl;
		for (const auto& mutation : record.mutations) {
			std::cout << "\t\t\t" << mutation.toString() << std::endl;
		}
	}
}

void printNotValidatedRecords(const std::vector<CommitRecord>& records) {
	std::cout << "Unvalidated commits: \n\n";
	for (const auto& record: records) {
		if (record.validation.validated()) continue;
		std::cout << "\tVersion: " << record.version << "\tTeam ID: " << record.teamID.toString() << std::endl;
		for (const auto& mutation: record.mutations) {
			std::cout << "\t\t\t" << mutation.toString() << std::endl;
		}

		if (!record.validation.tLogValidated) {
			std::cout << "\tTLog has not validated the reception of this commit." << std::endl;
		}
		if (!record.validation.storageServerValidated) {
			std::cout << "\tStorageServer has not validated the reception of this commit." << std::endl;
		}
	}
}

bool isAllRecordsValidated(const std::vector<CommitRecord>& records) {
	for (auto& record: records) {
		if (!record.validation.validated()) {
			return false;
		}
	}
	return true;
}

void verifyMutationsInRecord(std::vector<CommitRecord>& records,
                             const Version& version,
                             const TeamID& teamID,
                             const std::vector<MutationRef>& mutations,
                             std::function<void(CommitValidationRecord&)> validateUpdater) {
	for (auto& record : records) {
		if (record.version == version && record.teamID == teamID && record.mutations.size() == mutations.size()) {
			bool isSame = true;
			for (size_t i = 0; i < mutations.size(); ++i) {
				if (!(record.mutations[i].type == mutations[i].type &&
				      record.mutations[i].param1 == mutations[i].param1 &&
				      record.mutations[i].param2 == mutations[i].param2)) {
					isSame = false;
					break;
				}
			}
			if (isSame) {
				validateUpdater(record.validation);
				return;
			}
		}
	}

	printCommitRecord(records);
	std::cout << "\n\nLooking for: Version " << version << "\tTeam ID: " << teamID.toString() << "\n";
	for (const auto& mutation : mutations) {
		std::cout << "\t\t\t" << mutation.toString() << std::endl;
	}

	throw internal_error_msg("Mutations does not match previous record");
}

} // namespace ptxn

TEST_CASE("/fdbserver/ptxn/test/driver") {
	using namespace ptxn;

	std::vector<Future<Void>> actors;

	std::shared_ptr<TestDriverContext> context = initTestDriverContext();

	startFakeProxy(actors, context);
	startFakeTLog(actors, context);
	startFakeStorageServer(actors, context);

	wait(quorum(actors, 1));

	return Void();
}
