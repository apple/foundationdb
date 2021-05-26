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
#include "fdbserver/ResolverInterface.h"
#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"
#include "flow/String.h"
#include "flow/Trace.h"

#include "flow/actorcompiler.h" // This must be the last #include

namespace ptxn::test {

CommitRecord::CommitRecord(const Version& version_,
                           const StorageTeamID& storageTeamID_,
                           std::vector<MutationRef>&& mutations_)
  : version(version_), storageTeamID(storageTeamID_), mutations(std::move(mutations_)) {}

bool CommitValidationRecord::validated() const {
	return tLogValidated && storageServerValidated;
}

TestDriverOptions::TestDriverOptions(const UnitTestParameters& params)
  : numCommits(params.getInt("numCommits").orDefault(DEFAULT_NUM_COMMITS)),
    numStorageTeams(params.getInt("numStorageTeams").orDefault(DEFAULT_NUM_TEAMS)),
    numProxies(params.getInt("numProxies").orDefault(DEFAULT_NUM_PROXIES)),
    numTLogs(params.getInt("numTLogs").orDefault(DEFAULT_NUM_TLOGS)),
    numTLogGroups(params.getInt("numTLogGroups").orDefault(DEFAULT_NUM_TLOG_GROUPS)),
    numStorageServers(params.getInt("numStorageServers").orDefault(DEFAULT_NUM_STORAGE_SERVERS)),
    numResolvers(params.getInt("numResolvers").orDefault(DEFAULT_NUM_RESOLVERS)),
    skipCommitValidation(params.getBool("skipCommitValidation").orDefault(DEFAULT_SKIP_COMMIT_VALIDATION)),
    transferModel(static_cast<MessageTransferModel>(
        params.getInt("messageTransferModel").orDefault(static_cast<int>(DEFAULT_MESSAGE_TRANSFER_MODEL)))) {}

std::shared_ptr<TestDriverContext> initTestDriverContext(const TestDriverOptions& options) {
	print::print(options);

	std::shared_ptr<TestDriverContext> context(new TestDriverContext());

	context->numCommits = options.numCommits;
	context->numStorageTeamIDs = options.numStorageTeams;
	context->messageTransferModel = options.transferModel;

	// FIXME use C++20 range
	for (int i = 0; i < context->numStorageTeamIDs; ++i) {
		context->storageTeamIDs.push_back(getNewStorageTeamID());
	}

	context->commitVersionGap = 10000;
	context->skipCommitValidation = options.skipCommitValidation;
	// Prepare Proxies
	context->numProxies = options.numProxies;

	// Prepare Resolvers
	context->numResolvers = options.numResolvers;

	// Prepare TLogInterfaces
	// For now, each tlog group spans all the TLogs, i.e., number of group numbers == num of TLogs
	context->numTLogs = options.numTLogs;
	for (int i = 0; i < context->numTLogs; ++i) {
		context->tLogInterfaces.push_back(getNewTLogInterface(context->messageTransferModel,
		                                                      deterministicRandom()->randomUniqueID(),
		                                                      deterministicRandom()->randomUniqueID(),
		                                                      LocalityData()));
		context->tLogInterfaces.back()->initEndpoints();
	}

	context->numTLogGroups = options.numTLogGroups;
	for (int i = 0; i < context->numTLogGroups; ++i) {
		context->tLogGroups.push_back(TLogGroup(randomUID()));
		context->tLogGroupLeaders[context->tLogGroups.back().logGroupId] =
		    context->tLogInterfaces[deterministicRandom()->randomInt(0, context->numTLogs)];
	}

	// Prepare StorageServerInterfaces
	context->numStorageServers = options.numStorageServers;
	for (int i = 0; i < context->numTLogs; ++i) {
		context->storageServerInterfaces.push_back(getNewStorageServerInterface(context->messageTransferModel));
		context->storageServerInterfaces.back()->initEndpoints();
	}

	// Assign storage teams to storage interfaces
	auto assignTeamToInterface = [&](auto& mapper, auto interface) {
		int numInterfaces = interface.size();
		int index = 0;
		for (int i = 0; i < context->numStorageTeamIDs; ++i) {
			const StorageTeamID& storageTeamID = context->storageTeamIDs[i];
			mapper[storageTeamID] = interface[index];

			++index;
			index %= numInterfaces;
		}
	};
	assignTeamToInterface(context->storageTeamIDStorageServerInterfaceMapper, context->storageServerInterfaces);

	// Assign storage teams to tlog groups
	for (int i = 0, index = 0; i < context->numStorageTeamIDs; ++i) {
		const StorageTeamID& storageTeamID = context->storageTeamIDs[i];
		TLogGroup& tLogGroup = context->tLogGroups[index];
		context->storageTeamIDTLogGroupIDMapper[storageTeamID] = tLogGroup.logGroupId;
		// TODO: support tags when implementing pop
		tLogGroup.storageTeams[storageTeamID] = {};

		++index;
		index %= context->tLogGroups.size();
	}
	return context;
}

std::shared_ptr<TLogInterfaceBase> TestDriverContext::getTLogInterface(const StorageTeamID& storageTeamID) {
	return tLogGroupLeaders.at(storageTeamIDTLogGroupIDMapper.at(storageTeamID));
}

std::shared_ptr<StorageServerInterfaceBase> TestDriverContext::getStorageServerInterface(const StorageTeamID& storageTeamID) {
	return storageTeamIDStorageServerInterfaceMapper.at(storageTeamID);
}

std::pair<Version, Version> TestDriverContext::getCommitVersionPair(const StorageTeamID& storageTeamId) {
	ASSERT(storageTeamIDTLogGroupIDMapper.count(storageTeamId));
	Version prevVersion = tLogGroupVersion[storageTeamIDTLogGroupIDMapper.at(storageTeamId)];
	Version commitVersion = prevVersion + commitVersionGap;
	tLogGroupVersion[storageTeamIDTLogGroupIDMapper.at(storageTeamId)] = commitVersion;
	return { prevVersion, commitVersion };
}

void startFakeProxy(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	for (int i = 0; i < pTestDriverContext->numProxies; ++i) {
		std::shared_ptr<FakeProxyContext> pFakeProxyContext(
		    new FakeProxyContext{ i, pTestDriverContext->numCommits, pTestDriverContext });
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
                             const StorageTeamID& storageTeamID,
                             const std::vector<MutationRef>& mutations,
                             std::function<void(CommitValidationRecord&)> validateUpdater) {
	for (auto& record : records) {
		if (record.version == version && record.storageTeamID == storageTeamID && record.mutations.size() == mutations.size()) {
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
	std::cout << "\n\nLooking for: Version " << version << "\tTeam ID: " << storageTeamID.toString() << "\n";
	for (const auto& mutation : mutations) {
		std::cout << "\t\t\t" << mutation.toString() << std::endl;
	}

	throw internal_error_msg("Mutations does not match previous record");
}

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/driver") {
	using namespace ptxn::test;

	TestDriverOptions options(params);

	std::shared_ptr<TestDriverContext> context = initTestDriverContext(options);
	std::cout << 1 << std::endl;
	std::vector<Future<Void>> actors;
	std::cout << 2 << std::endl;

	startFakeProxy(actors, context);
	startFakeTLog(actors, context);
	startFakeStorageServer(actors, context);

	wait(quorum(actors, 1));

	return Void();
}