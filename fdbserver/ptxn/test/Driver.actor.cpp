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

#include <algorithm>
#include <iomanip>
#include <iostream>
#include <memory>
#include <functional>
#include <unordered_map>
#include <utility>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/test/FakeProxy.actor.h"
#include "fdbserver/ptxn/test/FakeResolver.actor.h"
#include "fdbserver/ptxn/test/FakeStorageServer.actor.h"
#include "fdbserver/ptxn/test/FakeTLog.actor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ptxn/TLogPeekCursor.h"
#include "fdbserver/ResolverInterface.h"
#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"
#include "flow/String.h"
#include "flow/Trace.h"

#include "flow/actorcompiler.h" // This must be the last #include

namespace ptxn::test {

CommitRecord::CommitRecord(const Version& version_, const TeamID& teamID_, std::vector<MutationRef>&& mutations_)
  : version(version_), teamID(teamID_), mutations(std::move(mutations_)) {}

bool CommitValidationRecord::validated() const {
	return tLogValidated && storageServerValidated;
}

TestDriverOptions::TestDriverOptions(const UnitTestParameters& params)
  : numCommits(params.getInt("numCommits").orDefault(DEFAULT_NUM_COMMITS)),
    numTeams(params.getInt("numTeams").orDefault(DEFAULT_NUM_TEAMS)),
    numProxies(params.getInt("numProxies").orDefault(DEFAULT_NUM_PROXIES)),
    numTLogs(params.getInt("numTLogs").orDefault(DEFAULT_NUM_TLOGS)),
    numStorageServers(params.getInt("numStorageServers").orDefault(DEFAULT_NUM_STORAGE_SERVERS)),
    numResolvers(params.getInt("numResolvers").orDefault(DEFAULT_NUM_RESOLVERS)),
    transferModel(static_cast<MessageTransferModel>(
        params.getInt("messageTransferModel").orDefault(static_cast<int>(DEFAULT_MESSAGE_TRANSFER_MODEL)))) {}

std::shared_ptr<TestDriverContext> initTestDriverContext(const TestDriverOptions& options) {
	print::print(options);

	std::shared_ptr<TestDriverContext> context(new TestDriverContext());

	context->numCommits = options.numCommits;
	context->numTeamIDs = options.numTeams;
	context->messageTransferModel = options.transferModel;

	// FIXME use C++20 range
	for (int i = 0; i < context->numTeamIDs; ++i) {
		context->teamIDs.push_back(getNewTeamID());
	}

	// Prepare Proxies
	context->numProxies = options.numProxies;

	// Prepare Resolvers
	context->numResolvers = options.numResolvers;

	// Prepare TLogInterfaces
	context->numTLogs = options.numTLogs;
	context->numTLogGroups = options.numTLogGroups;
	// For now, each tlog group spans all the TLogs, i.e., number of group numbers == num of TLogs
	for (int i = 0; i < context->numTLogs; ++i) {
		context->tLogInterfaces.push_back(getNewTLogInterface(context->messageTransferModel,
		                                                      deterministicRandom()->randomUniqueID(),
		                                                      deterministicRandom()->randomUniqueID(),
		                                                      LocalityData()));
		context->tLogInterfaces.back()->initEndpoints();
	}

	for (int i = 0; i < context->numTLogGroups; ++i) {
		context->tLogGroups.push_back(TLogGroup(deterministicRandom()->randomUniqueID()));
		context->tLogGroupLeaders[context->tLogGroups.back().logGroupId] =
		    context->tLogInterfaces[deterministicRandom()->randomInt(0, context->numTLogs)];
	}

	// Prepare StorageServerInterfaces
	context->numStorageServers = options.numStorageServers;
	for (int i = 0; i < context->numTLogs; ++i) {
		context->storageServerInterfaces.push_back(getNewStorageServerInterface(context->messageTransferModel));
		context->storageServerInterfaces.back()->initEndpoints();
	}

	// Assign teams to interfaces
	auto assignTeamToInterface = [&](auto& mapper, auto interface) {
		int numInterfaces = interface.size();
		int index = 0;
		for (int i = 0; i < context->numTeamIDs; ++i) {
			const StorageTeamID& teamID = context->teamIDs[i];
			mapper[teamID] = interface[index];

			++index;
			index %= numInterfaces;
		}
	};
	assignTeamToInterface(context->teamIDStorageServerInterfaceMapper, context->storageServerInterfaces);

	for (int i = 0, index = 0; i < context->numTeamIDs; ++i) {
		const StorageTeamID& teamID = context->teamIDs[i];
		TLogGroup& tLogGroup = context->tLogGroups[index];
		context->teamIDTLogInterfaceMapper[teamID] = context->tLogGroupLeaders[tLogGroup.logGroupId];
		// Ignore tags for now.
		tLogGroup.storageTeams[teamID] = {};
		++index;
		index %= context->tLogGroups.size();
	}
	return context;
}

std::shared_ptr<TLogInterfaceBase> TestDriverContext::getTLogInterface(const StorageTeamID& teamID) {
	return teamIDTLogInterfaceMapper.at(teamID);
}

std::shared_ptr<StorageServerInterfaceBase> TestDriverContext::getStorageServerInterface(const StorageTeamID& teamID) {
	return teamIDStorageServerInterfaceMapper.at(teamID);
}

void startFakeProxy(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	for (int i = 0; i < pTestDriverContext->numProxies; ++i) {
		std::shared_ptr<FakeProxyContext> pFakeProxyContext(
		    new FakeProxyContext{ pTestDriverContext->numCommits, pTestDriverContext });
		actors.emplace_back(fakeProxy(pFakeProxyContext));
	}
}

// Starts all fake resolvers. For now, use "resolverCore" to start the actor.
// TODO: change to "resolver" after we have fake ServerDBInfo object.
void startFakeResolver(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	for (int i = 0; i < pTestDriverContext->numResolvers; ++i) {
		std::shared_ptr<ResolverInterface> recruited(new ResolverInterface);
		// recruited.locality = locality;
		recruited->initEndpoints();

		InitializeResolverRequest req;
		req.recoveryCount = 1;
		req.commitProxyCount = pTestDriverContext->numProxies;
		req.resolverCount = pTestDriverContext->numResolvers;

		actors.emplace_back(::resolverCore(*recruited, req));
		pTestDriverContext->resolverInterfaces.push_back(recruited);
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

bool isAllRecordsValidated(const std::vector<CommitRecord>& records) {
	for (auto& record : records) {
		if (!record.validation.validated()) {
			return false;
		}
	}
	return true;
}

void verifyMutationsInRecord(std::vector<CommitRecord>& records,
                             const Version& version,
                             const StorageTeamID& teamID,
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

	print::printCommitRecord(records);
	std::cout << "\n\nLooking for: Version " << version << "\tTeam ID: " << teamID.toString() << "\n";
	for (const auto& mutation : mutations) {
		std::cout << "\t\t\t" << mutation.toString() << std::endl;
	}

	throw internal_error_msg("Mutations does not match previous record");
}

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/driver") {
	using namespace ptxn::test;

	TestDriverOptions options(params);
	print::print(options);

	std::shared_ptr<TestDriverContext> context = initTestDriverContext(options);
	std::vector<Future<Void>> actors;

	startFakeProxy(actors, context);
	startFakeTLog(actors, context);
	startFakeStorageServer(actors, context);

	wait(quorum(actors, 1));

	return Void();
}