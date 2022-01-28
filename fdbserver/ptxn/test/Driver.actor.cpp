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
#include <functional>
#include <iomanip>
#include <iostream>
#include <memory>
#include <set>
#include <unordered_map>
#include <utility>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/Config.h"
#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/test/FakeProxy.actor.h"
#include "fdbserver/ptxn/test/FakeResolver.actor.h"
#include "fdbserver/ptxn/test/FakeSequencer.actor.h"
#include "fdbserver/ptxn/test/FakeStorageServer.actor.h"
#include "fdbserver/ptxn/test/FakeTLog.actor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/ResolverInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/Error.h"
#include "flow/genericactors.actor.h"
#include "flow/IRandom.h"
#include "flow/String.h"
#include "flow/Trace.h"

#include "flow/actorcompiler.h" // This must be the last #include

namespace ptxn::test {

const int TestDriverOptions::DEFAULT_NUM_COMMITS = 3;
const int TestDriverOptions::DEFAULT_NUM_TEAMS = 3;
const int TestDriverOptions::DEFAULT_NUM_PROXIES = 1;
const int TestDriverOptions::DEFAULT_NUM_TLOGS = 3;
const int TestDriverOptions::DEFAULT_NUM_TLOG_GROUPS = 4;
const int TestDriverOptions::DEFAULT_NUM_STORAGE_SERVERS = 3;
const int TestDriverOptions::DEFAULT_NUM_RESOLVERS = 2;
const int TestDriverOptions::DEFAULT_SKIP_COMMIT_VALIDATION = false;
const MessageTransferModel TestDriverOptions::DEFAULT_MESSAGE_TRANSFER_MODEL = MessageTransferModel::TLogActivelyPush;

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

void TestDriverContext::updateServerDBInfo(Reference<AsyncVar<ServerDBInfo>> dbInfo,
                                           const std::vector<ptxn::TLogInterface_PassivelyPull>& interfaces) {
	ServerDBInfo info;
	LogSystemConfig& lsConfig = info.logSystemConfig;
	lsConfig.logSystemType = LogSystemType::teamPartitioned;
	info.recoveryState = RecoveryState::FULLY_RECOVERED;

	// For now, assume we only have primary TLog set
	lsConfig.tLogs.clear();
	lsConfig.tLogs.emplace_back(TLogSet());
	TLogSet& logset = lsConfig.tLogs[0];
	for (const auto& group : tLogGroups) {
		logset.tLogGroupIDs.push_back(group.logGroupId);
	}
	for (const auto& tlogIf : interfaces) {
		logset.tLogsPtxn.emplace_back(tlogIf);
	}
	// TODO: fill in ptxnTLogGroups fields

	dbInfo->setUnconditional(info);
}

TLogGroup& TestDriverContext::getTLogGroup(TLogGroupID gid) {
	for (auto& group : tLogGroups) {
		if (group.logGroupId == gid) {
			return group;
		}
	}
	UNREACHABLE();
}

std::shared_ptr<TestDriverContext> initTestDriverContext(const TestDriverOptions& options) {
	print::PrintTiming printTiming(__FUNCTION__);
	print::print(options);

	std::shared_ptr<TestDriverContext> context = std::make_shared<TestDriverContext>();

	context->numCommits = options.numCommits;
	context->numStorageTeamIDs = options.numStorageTeams;
	context->messageTransferModel = options.transferModel;

	// FIXME use C++20 range
	for (int i = 0; i < context->numStorageTeamIDs; ++i) {
		context->storageTeamIDs.push_back(getNewStorageTeamID());
		printTiming << "Storage Team ID: " << context->storageTeamIDs.back().toString() << std::endl;
	}

	context->commitVersionGap = 10000;
	context->skipCommitValidation = options.skipCommitValidation;

	// Prepare sequencer
	context->sequencerInterface = std::make_shared<MasterInterface>();
	context->sequencerInterface->initEndpoints();

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
		const auto& interface = randomlyPick(context->tLogInterfaces);
		context->tLogGroups.push_back(TLogGroup(randomUID()));
		context->tLogGroupLeaders[context->tLogGroups.back().logGroupId] = interface;
	}

	// Prepare StorageServerInterfaces
	context->numStorageServers = options.numStorageServers;
	for (int i = 0; i < context->numStorageServers; ++i) {
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
	std::vector<TLogGroupID> groups;
	for (const auto& g : context->tLogGroups) {
		groups.push_back(g.logGroupId);
	}
	for (const StorageTeamID& storageTeamID : context->storageTeamIDs) {
		TLogGroupID groupId = tLogGroupByStorageTeamID(groups, storageTeamID);
		TLogGroup& tLogGroup = context->getTLogGroup(groupId);
		context->storageTeamIDTLogGroupIDMapper[storageTeamID] = groupId;
		// TODO: support tags when implementing pop
		tLogGroup.storageTeams[storageTeamID] = {};
	}
	return context;
}

std::shared_ptr<TLogInterfaceBase> TestDriverContext::getTLogLeaderByStorageTeamID(const StorageTeamID& storageTeamID) {
	if (auto iter = storageTeamIDTLogGroupIDMapper.find(storageTeamID); iter != storageTeamIDTLogGroupIDMapper.end()) {
		if (auto iter2 = tLogGroupLeaders.find(iter->second); iter2 != tLogGroupLeaders.end()) {
			return iter2->second;
		}
		throw internal_error_msg("TLogGroupID has no leader assigned");
	}
	throw internal_error_msg("Storage Team ID not found in storageTeamIDTLogGroupIDMapper");
}

std::shared_ptr<StorageServerInterfaceBase> TestDriverContext::getStorageServerInterface(
    const StorageTeamID& storageTeamID) {
	return storageTeamIDStorageServerInterfaceMapper.at(storageTeamID);
}

std::pair<Version, Version> TestDriverContext::getCommitVersionPair(const TLogGroupID& tLogGroupID,
                                                                    const Version& currentVersion) {
	Version prevVersion = tLogGroupVersion[tLogGroupID];
	Version commitVersion = currentVersion;
	tLogGroupVersion[tLogGroupID] = commitVersion;
	return { prevVersion, commitVersion };
}

void startFakeSequencer(std::vector<Future<Void>>& actors, std::shared_ptr<TestDriverContext> pTestDriverContext) {
	std::shared_ptr<FakeSequencerContext> pFakeSequencerContext = std::make_shared<FakeSequencerContext>();
	pFakeSequencerContext->pTestDriverContext = pTestDriverContext;
	pFakeSequencerContext->pSequencerInterface = pTestDriverContext->sequencerInterface;
	actors.emplace_back(fakeSequencer(pFakeSequencerContext));
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

		// TODO: make a usable "db" for resolverCore.
		Reference<AsyncVar<ServerDBInfo>> db;
		actors.emplace_back(::resolverCore(*recruited, req, db));
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

namespace details {

void TLogGroupFixture::setUp(const int numTLogGroups, const int numStorageTeamIDs) {

	ASSERT(numTLogGroups <= numStorageTeamIDs);

	test::print::PrintTiming printTiming("TLogGroupFixture::setUp");

	for (int i = 0; i < numTLogGroups; ++i) {
		const auto tLogGroupID = randomUID();
		tLogGroupIDs.push_back(tLogGroupID);
		tLogGroupStorageTeamMapping[tLogGroupID];
	}
	std::sort(std::begin(tLogGroupIDs), std::end(tLogGroupIDs));
	storageTeamIDs = generateRandomStorageTeamIDs(numStorageTeamIDs);

	// Assign storageTeamIDs to TLog groups
	for (const auto& storageTeamID : storageTeamIDs) {
		const auto tLogGroupID = tLogGroupByStorageTeamID(tLogGroupIDs, storageTeamID);
		tLogGroupStorageTeamMapping[tLogGroupID].insert(storageTeamID);
	}

	// Reverse mapping
	for (const auto& [tLogGroupID, storageTeamIDs] : tLogGroupStorageTeamMapping) {
		for (const auto& storageTeamID : storageTeamIDs) {
			storageTeamTLogGroupMapping[storageTeamID] = tLogGroupID;
		}
	}

	// Create TLogGroup objects
	for (const auto& [tLogGroupID, storageTeamIDs] : tLogGroupStorageTeamMapping) {
		tLogGroups.emplace_back(tLogGroupID);
		for (const auto& storageTeamID : storageTeamIDs) {
			tLogGroups.back().storageTeams[storageTeamID];
		}
	}
}

void TLogGroupWithPrivateMutationsFixture::setUp(const int numTLogGroups, const int numStorageTeamIDs) {
	test::print::PrintTiming printTiming("TLogGroupWithPrivateMutationsFixture::setUp");

	TLogGroupFixture::setUp(numTLogGroups, numStorageTeamIDs);

	privateMutationsStorageTeamID = getNewStorageTeamID();
	printTiming << "Team mutation storage team ID: " << privateMutationsStorageTeamID << std::endl;
	storageTeamIDs.push_back(privateMutationsStorageTeamID);

	const auto privateMutationsTLogGroupID = tLogGroupByStorageTeamID(tLogGroupIDs, privateMutationsStorageTeamID);
	tLogGroupStorageTeamMapping[privateMutationsTLogGroupID].insert(privateMutationsStorageTeamID);
	storageTeamTLogGroupMapping[privateMutationsStorageTeamID] = privateMutationsTLogGroupID;
}

void MessageFixture::generateMutationsToStorageTeamIDs(const std::vector<StorageTeamID>& storageTeamIDs,
                                                       const int initialVersion,
                                                       const int numVersions,
                                                       const int numMutationsInVersion) {}

void MessageFixture::setUp(const TLogGroupFixture& tLogGroupFixture,
                           const int initialVersion,
                           const int numVersions,
                           const int numMutationsInVersion) {
	// FIXME Think about generations
	test::print::PrintTiming printTiming("MessageFixture::setUp");

	Version storageTeamVersion = 0;
	Version commitVersion = initialVersion;
	for (int _ = 0; _ < numVersions; ++_) {
		Arena mutationArena;
		VectorRef<MutationRef> mutationRefs;
		generateMutationRefs(numMutationsInVersion, mutationArena, mutationRefs);
		distributeMutationRefs(
		    mutationRefs, commitVersion, ++storageTeamVersion, tLogGroupFixture.storageTeamIDs, commitRecord);
		increaseVersion(commitVersion);
	}

	commitRecord.updateVersionInformation();
}

void MessageWithPrivateMutationsFixture::setUp(const TLogGroupWithPrivateMutationsFixture& tLogGroupFixture,
                                               const int initialVersion,
                                               const int numVersions,
                                               const int numMutationsInVersion,
                                               const StorageTeamID& privateMutationStorageTeamID,
                                               const std::vector<UID>& storageServerIDs) {
	// FIXME Think about generations

	test::print::PrintTiming printTiming("MessageWithPrivateMutationsFixture::setUp");
	static const int INV_TEAM_MUTATION_INJECTION_RATE = 2;

	// FIXME At this stage we only set up storage server ID for one server
	ASSERT(storageServerIDs.size() == 1);

	// All storage team ids but the private mutation team
	auto storageTeamIDs = tLogGroupFixture.storageTeamIDs;
	storageTeamIDs.erase(std::remove(std::begin(storageTeamIDs),
	                                 std::end(storageTeamIDs),
	                                 tLogGroupFixture.privateMutationsStorageTeamID),
	                     std::end(storageTeamIDs));
	printTiming << "Excluded storage team ID: " << tLogGroupFixture.privateMutationsStorageTeamID << std::endl;

	Version storageTeamVersion = 0;
	Version commitVersion = initialVersion;
	for (int _ = 0; _ < numVersions; ++_) {
		// Create fake mutations for storage teams, excluding the private mutation team
		Arena mutationArena;
		VectorRef<MutationRef> mutationRefs;
		generateMutationRefs(numMutationsInVersion, mutationArena, mutationRefs);
		distributeMutationRefs(mutationRefs, commitVersion, ++storageTeamVersion, storageTeamIDs, commitRecord);
		increaseVersion(commitVersion);

		// Inject a team mutation change if possible
		if (deterministicRandom()->randomInt(0, INV_TEAM_MUTATION_INJECTION_RATE) != 0) {
			continue;
		}

		// The team mutation commit is not generated, thus not included in numVersions
		const int numSamples = deterministicRandom()->randomInt(1, storageTeamIDs.size() + 1);
		const auto storageTeamIDs_ = randomlyPick<std::vector<StorageTeamID>>(storageTeamIDs, numSamples);
		const auto storageTeamIDs = std::set<StorageTeamID>(std::begin(storageTeamIDs_), std::end(storageTeamIDs_));
		StorageServerStorageTeams storageTeams(privateMutationStorageTeamID, storageTeamIDs);
		// FIXME consider multiple storage server IDs
		Key key = storageServerToTeamIdKey(storageServerIDs[0]);
		Value value = storageTeams.toValue();
		// The team mutation storage team version will always be 1
		commitRecord.messages[commitVersion][privateMutationStorageTeamID].push_back(
		    commitRecord.messageArena,
		    { ++storageTeamVersion, MutationRef(commitRecord.messageArena, MutationRef::SetValue, key, value) });
		commitRecord.commitVersionStorageTeamVersionMapper[commitVersion] = storageTeamVersion;
		increaseVersion(commitVersion);
	}

	commitRecord.updateVersionInformation();
}

#pragma region ptxnTLogFixture

const int ptxnTLogFixture::NUM_TLOG_PER_GROUP = 3;

ptxnTLogFixture::~ptxnTLogFixture() {
	for (auto& actor : actors) {
		actor.cancel();
	}
}

void ptxnTLogFixture::setUp(const int numTLogs) {
	ptxn::test::print::PrintTiming printTiming("ptxnTLogFixture::setUp");

	// Create TLog servers
	for (int i = 0; i < numTLogs; ++i) {
		std::shared_ptr<FakeTLogContext> pTLogContext = std::make_shared<FakeTLogContext>();

		tLogContexts.push_back(pTLogContext);
		pTLogContext->pTestDriverContext = pTestDriverContext;

		// Assign interface
		auto pTLogInterface = createTLogInterface();
		pTLogInterface->initEndpoints();
		tLogInterfaces.push_back(pTLogInterface);
		pTLogContext->pTLogInterface = pTLogInterface;

		printTiming << "Created TLogInterface " << i << " with ID " << pTLogInterface->id() << std::endl;
	}

	// Assign tLog group and storage team IDs
	int tLogContextIndex = 0;
	ASSERT(tLogGroupFixture.tLogGroupIDs.size() == tLogGroupFixture.getNumTLogGroups());
	ASSERT(numTLogs == tLogContexts.size());
	for (auto tLogGroupIndex = 0; tLogGroupIndex < tLogGroupFixture.getNumTLogGroups(); ++tLogGroupIndex) {
		const auto& tLogGroupID = tLogGroupFixture.tLogGroupIDs[tLogGroupIndex];
		const auto& pTLogContext = tLogContexts[tLogContextIndex];

		ASSERT(tLogGroupFixture.tLogGroupStorageTeamMapping.count(tLogGroupID));
		for (const auto& storageTeamID : tLogGroupFixture.tLogGroupStorageTeamMapping.at(tLogGroupID)) {
			pTLogContext->storageTeamIDs.push_back(storageTeamID);
		}
		tLogGroupLeaders[tLogGroupID] = pTLogContext->pTLogInterface;
		printTiming << "Assigned TLogInterface " << pTLogContext->pTLogInterface->id() << " to TLogGroup "
		            << tLogGroupID << std::endl;

		tLogContextIndex = (tLogContextIndex + 1) % numTLogs;
	}

	// Start TLogs
	for (const auto& pTLogContext : tLogContexts) {
		actors.push_back(createTLogActor(pTLogContext));
	}
}

ptxn::details::TLogInterfaceSharedPtrWrapper ptxnTLogFixture::getTLogLeaderByTLogGroupID(
    const TLogGroupID& tLogGroupID) const {

	if (tLogGroupLeaders.find(tLogGroupID) != std::end(tLogGroupLeaders)) {
		return tLogGroupLeaders.at(tLogGroupID);
	}

	ASSERT(false);

	// This is to mute the compiler warning of not returning a value
	return {};
}

ptxn::details::TLogInterfaceSharedPtrWrapper ptxnTLogFixture::getTLogLeaderByStorageTeamID(
    const StorageTeamID& storageTeamID) const {

	const auto iter = tLogGroupFixture.storageTeamTLogGroupMapping.find(storageTeamID);
	if (iter != std::end(tLogGroupFixture.storageTeamTLogGroupMapping)) {
		return getTLogLeaderByTLogGroupID(iter->second);
	}

	ASSERT(false);

	// This is to mute the compiler warning of not returning a value
	return {};
}

#pragma endregion ptxnTLogFixture

std::shared_ptr<FakeTLogContext> ptxnFakeTLogFixture::getTLogContextByIndex(const int index) {
	ASSERT(index >= 0 && index < static_cast<int>(tLogContexts.size()));

	return tLogContexts[index];
}

#pragma region ptxnStorageServerFixture

const KeyValueStoreType ptxnStorageServerFixture::keyValueStoreType(KeyValueStoreType::MEMORY);

std::string ptxnStorageServerFixture::StorageServerResources::getFolder() const {
	return concatToString("./", "storage-", storageTeamID, ".", keyValueStoreType);
}

ptxnStorageServerFixture::StorageServerResources::StorageServerResources(const StorageTeamID& storageTeamID_)
  : storageTeamID(storageTeamID_),
    interface(std::make_shared<::StorageServerInterface>(deterministicRandom()->randomUniqueID())),
    clusterID(deterministicRandom()->randomUniqueID()),
    /* IKeyValueStore has protected destructor and is not intended to be deleted, since we are doing test we tolerate
       this memory leakage */
    kvStore(openKVStore(keyValueStoreType, getFolder(), interface->id(), MEMORY_LIMIT), [](IKeyValueStore* _) {}),
    seedTag(Tag(tagLocalitySpecial, deterministicRandom()->randomInt(1, 65536))), seedVersion(0) {}

void ptxnStorageServerFixture::setUp(const int numStorageServers) {
	// FIXME At this stage we only support one single storage server, as there is only one private mutation team
	ASSERT(numStorageServers == 1);

	const auto& asyncServerDBInfoRef = serverDBInfoFixture.getAsyncServerDBInfoRef();
	const auto& storageTeamID =
	    dynamic_cast<const TLogGroupWithPrivateMutationsFixture&>(tLogGroupFixture).privateMutationsStorageTeamID;
	storageServerResources.emplace_back(storageTeamID);
	initializeStorageReplies.emplace_back();

	std::vector<ptxn::StorageTeamID> storageTeams{ storageTeamID };
	const auto& resource = storageServerResources.back();
	const auto& initializeReply = initializeStorageReplies.back();
	actors.push_back(storageServer(resource.kvStore.get(),
	                               *resource.interface,
	                               resource.seedTag,
	                               /* clusterId */ deterministicRandom()->randomUniqueID(),
	                               resource.seedVersion,
	                               initializeReply,
	                               asyncServerDBInfoRef,
	                               /* folder */ "./",
	                               storageTeams));
}

ptxnStorageServerFixture::~ptxnStorageServerFixture() {
	for (auto& actor : actors) {
		actor.cancel();
	}
}

#pragma endregion ptxnStorageServerFixture

#pragma region ServerDBInfoFixture

void ServerDBInfoFixture::setUp() {
	serverDBInfo.id = deterministicRandom()->randomUniqueID();
	serverDBInfo.recoveryState = RecoveryState::ACCEPTING_COMMITS;
	serverDBInfo.isTestEnvironment = true;

	auto tLogFixture = TestEnvironment::getTLogs();

	auto& logSystemConfig = serverDBInfo.logSystemConfig;
	logSystemConfig.logSystemType = LogSystemType::teamPartitioned;
	logSystemConfig.tLogs.emplace_back();

	auto& tLogSet = logSystemConfig.tLogs.back();
	for (auto tLogInterface : tLogFixture->tLogInterfaces) {
		tLogSet.tLogsPtxn.emplace_back(*std::dynamic_pointer_cast<TLogInterface_PassivelyPull>(tLogInterface));
	}
	for (const auto& [tLogGroupID, tLogLeaderInterface] : tLogFixture->tLogGroupLeaders) {
		tLogSet.tLogGroupIDs.push_back(tLogGroupID);
		tLogSet.ptxnTLogGroups.emplace_back();
		tLogSet.ptxnTLogGroups.back().emplace_back(
		    *std::dynamic_pointer_cast<TLogInterface_PassivelyPull>(tLogLeaderInterface));
	}
}

Reference<const AsyncVar<ServerDBInfo>> ServerDBInfoFixture::getAsyncServerDBInfoRef() const {
	return makeReference<AsyncVar<ServerDBInfo>>(serverDBInfo);
}

#pragma endregion ServerDBInfoFixture

#pragma region ptxnTLogPassivelyPullFixture

std::shared_ptr<TLogInterfaceBase> ptxnFakeTLogPassivelyPullFixture::createTLogInterface() {
	return getNewTLogInterface(MessageTransferModel::StorageServerActivelyPull);
}

Future<Void> ptxnFakeTLogPassivelyPullFixture::createTLogActor(std::shared_ptr<FakeTLogContext> pContext) {
	return getFakeTLogActor(MessageTransferModel::StorageServerActivelyPull, pContext);
}

#pragma endregion ptxnTLogPassivelyPullFixture

} // namespace details

#pragma region TestEnvironment

std::unique_ptr<details::TestEnvironmentImpl> TestEnvironment::pImpl = nullptr;

TestEnvironment& TestEnvironment::initDriverContext() {
	ASSERT(!pImpl->testDriverContextImpl);

	pImpl->testDriverContextImpl = std::make_shared<TestDriverContext>();

	return *this;
}

TestEnvironment& TestEnvironment::initTLogGroup(const int numTLogGroupIDs, const int numStorageTeamIDs) {
	ASSERT(pImpl->testDriverContextImpl);
	ASSERT(!pImpl->tLogGroup);

	pImpl->tLogGroup = std::make_unique<details::TLogGroupFixture>(*pImpl->testDriverContextImpl);
	pImpl->tLogGroup->setUp(numTLogGroupIDs, numStorageTeamIDs);

	return *this;
}

TestEnvironment& TestEnvironment::initTLogGroupWithPrivateMutationsFixture(const int numTLogGroupIDs,
                                                                           const int numStorageTeamIDs) {
	ASSERT(pImpl->testDriverContextImpl);
	ASSERT(!pImpl->tLogGroup);

	pImpl->tLogGroup = std::make_unique<details::TLogGroupWithPrivateMutationsFixture>(*pImpl->testDriverContextImpl);
	dynamic_cast<details::TLogGroupWithPrivateMutationsFixture&>(*pImpl->tLogGroup)
	    .setUp(numTLogGroupIDs, numStorageTeamIDs);

	return *this;
}

TestEnvironment& TestEnvironment::initPtxnTLog(const MessageTransferModel model, const int numTLogs, bool useFake) {
	ASSERT(pImpl->testDriverContextImpl);
	ASSERT(pImpl->tLogGroup);
	ASSERT(!pImpl->tLogs);

	if (useFake) {
		switch (model) {
		case MessageTransferModel::StorageServerActivelyPull:
			pImpl->tLogs = std::make_shared<details::ptxnFakeTLogPassivelyPullFixture>(pImpl->testDriverContextImpl,
			                                                                           *pImpl->tLogGroup);
			break;
		default:
			ASSERT(false);
		}
	} else {
		ASSERT(false);
	}

	pImpl->tLogs->setUp(numTLogs);

	return *this;
}

TestEnvironment& TestEnvironment::initMessages(const int initialVersion,
                                               const int numVersions,
                                               const int numMutationsInVersion) {
	ASSERT(pImpl->testDriverContextImpl);
	ASSERT(pImpl->tLogGroup);
	ASSERT(!pImpl->messages);

	pImpl->messages = std::make_unique<details::MessageFixture>(pImpl->testDriverContextImpl->commitRecord);
	pImpl->messages->setUp(*pImpl->tLogGroup, initialVersion, numVersions, numMutationsInVersion);

	return *this;
}

// storageServerIDs are used if exists, otherwise use the
TestEnvironment& TestEnvironment::initMessagesWithPrivateMutations(
    const int initialVersion,
    const int numVersions,
    const int numMutationsInVersion,
    Optional<std::vector<UID>> optionalStorageServerIDs) {
	ASSERT(pImpl->testDriverContextImpl);
	ASSERT(pImpl->tLogGroup);
	ASSERT(!pImpl->messages);

	std::vector<UID> storageServerIDs;

	if (optionalStorageServerIDs.present()) {
		storageServerIDs = optionalStorageServerIDs.get();
	} else {
		ASSERT(pImpl->storageServers);
		for (const auto& storageServerResource : pImpl->storageServers->storageServerResources) {
			ASSERT(storageServerResource.interface);
			storageServerIDs.push_back(storageServerResource.interface->id());
		}
	}

	pImpl->messages =
	    std::make_unique<details::MessageWithPrivateMutationsFixture>(pImpl->testDriverContextImpl->commitRecord);
	const auto& tLogGroup = dynamic_cast<details::TLogGroupWithPrivateMutationsFixture&>(*pImpl->tLogGroup);
	dynamic_cast<details::MessageWithPrivateMutationsFixture&>(*pImpl->messages)
	    .setUp(tLogGroup,
	           initialVersion,
	           numVersions,
	           numMutationsInVersion,
	           tLogGroup.privateMutationsStorageTeamID,
	           storageServerIDs);

	return *this;
}

TestEnvironment& TestEnvironment::initPtxnStorageServer(const int numStorageServers) {
	ASSERT(pImpl->tLogGroup);
	ASSERT(pImpl->serverDBInfo);
	ASSERT(!pImpl->storageServers);

	pImpl->storageServers =
	    std::make_shared<details::ptxnStorageServerFixture>(*pImpl->tLogGroup, *pImpl->serverDBInfo);

	pImpl->storageServers->setUp(numStorageServers);

	return *this;
}

TestEnvironment& TestEnvironment::initServerDBInfo() {
	ASSERT(pImpl);
	// TODO Rethink is -- does server db info depends on TLogs in test?
	ASSERT(pImpl->tLogs);
	ASSERT(!pImpl->serverDBInfo);

	pImpl->serverDBInfo = std::make_unique<details::ServerDBInfoFixture>();
	pImpl->serverDBInfo->setUp();

	return *this;
}

CommitRecord& TestEnvironment::getCommitRecords() {
	ASSERT(pImpl);
	ASSERT(pImpl->testDriverContextImpl);

	return pImpl->testDriverContextImpl->commitRecord;
}

const details::TLogGroupFixture& TestEnvironment::getTLogGroup() {
	ASSERT(pImpl);
	ASSERT(pImpl->tLogGroup);

	return *pImpl->tLogGroup;
}

std::shared_ptr<details::ptxnTLogFixture> TestEnvironment::getTLogs() {
	ASSERT(pImpl);
	ASSERT(pImpl->tLogs);

	return pImpl->tLogs;
}

details::ServerDBInfoFixture& TestEnvironment::getServerDBInfo() {
	ASSERT(pImpl);
	ASSERT(pImpl->serverDBInfo);

	return *pImpl->serverDBInfo;
}

std::shared_ptr<details::ptxnStorageServerFixture> TestEnvironment::getStorageServers() {
	ASSERT(pImpl);
	ASSERT(pImpl->storageServers);

	return pImpl->storageServers;
}

#pragma endregion TestEnvironment

} // namespace ptxn::test

TEST_CASE("/fdbserver/ptxn/test/driver") {
	using namespace ptxn::test;

	TestDriverOptions options(params);

	std::shared_ptr<TestDriverContext> context = initTestDriverContext(options);
	std::vector<Future<Void>> actors;

	startFakeSequencer(actors, context);
	startFakeProxy(actors, context);
	startFakeTLog(actors, context);
	startFakeStorageServer(actors, context);

	wait(quorum(actors, 1));

	return Void();
}
