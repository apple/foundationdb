/*
 * TestTLogServer.actor.cpp
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

#include <algorithm>
#include <memory>
#include <random>
#include <vector>

#include "fdbserver/IDiskQueue.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/ptxn/TLogInterface.h"
#include "fdbserver/ptxn/MessageSerializer.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/FakeLogSystem.h"
#include "fdbserver/ptxn/test/FakePeekCursor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "flow/Arena.h"

#include "flow/actorcompiler.h" // has to be the last file included

namespace {

// duplicate from worker.actor.cpp, but not seeing a good way to import that code
std::string filenameFromId(KeyValueStoreType storeType, std::string folder, std::string prefix, UID id) {

	if (storeType == KeyValueStoreType::SSD_BTREE_V1)
		return joinPath(folder, prefix + id.toString() + ".fdb");
	else if (storeType == KeyValueStoreType::SSD_BTREE_V2)
		return joinPath(folder, prefix + id.toString() + ".sqlite");
	else if (storeType == KeyValueStoreType::MEMORY || storeType == KeyValueStoreType::MEMORY_RADIXTREE)
		return joinPath(folder, prefix + id.toString() + "-");
	else if (storeType == KeyValueStoreType::SSD_REDWOOD_V1)
		return joinPath(folder, prefix + id.toString() + ".redwood");
	else if (storeType == KeyValueStoreType::SSD_ROCKSDB_V1)
		return joinPath(folder, prefix + id.toString() + ".rocksdb");

	TraceEvent(SevError, "UnknownStoreType").detail("StoreType", storeType.toString());
	UNREACHABLE();
}

ACTOR Future<Void> startTLogServers(std::vector<Future<Void>>* actors,
                                    std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                                    std::string folder) {
	state std::vector<ptxn::InitializePtxnTLogRequest> tLogInitializations;
	state std::unordered_map<ptxn::TLogGroupID, int> groupToLeaderId;
	pContext->groupsPerTLog.resize(pContext->numTLogs);
	for (int i = 0, index = 0; i < pContext->numTLogGroups; ++i) {
		ptxn::TLogGroup& tLogGroup = pContext->tLogGroups[i];
		pContext->groupsPerTLog[index].push_back(tLogGroup);
		groupToLeaderId[tLogGroup.logGroupId] = index;
		++index;
		index %= pContext->numTLogs;
	}
	state int i = 0;
	for (; i < pContext->numTLogs; i++) {
		PromiseStream<ptxn::InitializePtxnTLogRequest> initializeTLog;
		Promise<Void> recovered;
		tLogInitializations.emplace_back();
		tLogInitializations.back().isPrimary = true;
		tLogInitializations.back().storeType = KeyValueStoreType::MEMORY;
		tLogInitializations.back().tlogGroups = pContext->groupsPerTLog[i];
		UID tlogId = ptxn::test::randomUID();
		UID workerId = ptxn::test::randomUID();
		StringRef fileVersionedLogDataPrefix = "log2-"_sr;
		StringRef fileLogDataPrefix = "log-"_sr;
		ptxn::InitializePtxnTLogRequest req = tLogInitializations.back();
		const StringRef prefix = req.logVersion > TLogVersion::V2 ? fileVersionedLogDataPrefix : fileLogDataPrefix;
		std::unordered_map<ptxn::TLogGroupID, std::pair<IKeyValueStore*, IDiskQueue*>> persistentDataAndQueues;
		for (ptxn::TLogGroup& tlogGroup : pContext->groupsPerTLog[i]) {
			std::string filename =
			    filenameFromId(req.storeType, folder, prefix.toString() + "test", tlogGroup.logGroupId);
			IKeyValueStore* data = keyValueStoreMemory(joinPath(folder, "loggroup"), tlogGroup.logGroupId, 500e6);
			IDiskQueue* queue = openDiskQueue(joinPath(folder, "logqueue-" + tlogGroup.logGroupId.toString() + "-"),
			                                  "fdq",
			                                  tlogGroup.logGroupId,
			                                  DiskQueueVersion::V1);
			persistentDataAndQueues[tlogGroup.logGroupId] = std::make_pair(data, queue);
		}

		actors->push_back(ptxn::tLog(persistentDataAndQueues,
		                             makeReference<AsyncVar<ServerDBInfo>>(),
		                             LocalityData(),
		                             initializeTLog,
		                             tlogId,
		                             workerId,
		                             false,
		                             Promise<Void>(),
		                             Promise<Void>(),
		                             folder,
		                             makeReference<AsyncVar<bool>>(false),
		                             makeReference<AsyncVar<UID>>(tlogId)));
		initializeTLog.send(tLogInitializations.back());
		std::cout << "Recruit tlog " << i << " : " << tlogId.shortString() << ", workerID: " << workerId.shortString()
		          << "\n";
	}

	// replace fake TLogInterface with recruited interface
	std::vector<Future<ptxn::TLogInterface_PassivelyPull>> interfaceFutures(pContext->numTLogs);
	for (i = 0; i < pContext->numTLogs; i++) {
		interfaceFutures[i] = tLogInitializations[i].reply.getFuture();
	}
	std::vector<ptxn::TLogInterface_PassivelyPull> interfaces = wait(getAll(interfaceFutures));
	for (i = 0; i < pContext->numTLogs; i++) {
		// This is awkward, but we can't do: *(pContext->tLogInterfaces[i]) = interfaces[i]
		// because this only copies the base class data. The pointer can no longer
		// be casted back to "TLogInterface_PassivelyPull".
		std::shared_ptr<ptxn::TLogInterface_PassivelyPull> tli(new ptxn::TLogInterface_PassivelyPull());
		*tli = interfaces[i];
		pContext->tLogInterfaces[i] = std::static_pointer_cast<ptxn::TLogInterfaceBase>(tli);
	}
	// Update the TLogGroupID to interface mapping
	for (auto& [tLogGroupID, tLogGroupLeader] : pContext->tLogGroupLeaders) {
		tLogGroupLeader = pContext->tLogInterfaces[groupToLeaderId[tLogGroupID]];
	}
	return Void();
}

void generateMutations(const Version& version,
                       const int numMutations,
                       const std::vector<ptxn::StorageTeamID>& storageTeamIDs,
                       ptxn::test::CommitRecord& commitRecord) {
	Arena arena;
	VectorRef<MutationRef> mutationRefs;
	ptxn::test::generateMutationRefs(numMutations, arena, mutationRefs);
	ptxn::test::distributeMutationRefs(mutationRefs, version, storageTeamIDs, commitRecord);
	commitRecord.messageArena.dependsOn(arena);
}

Standalone<StringRef> serializeMutations(const Version& version,
                                         const ptxn::StorageTeamID storageTeamID,
                                         const ptxn::test::CommitRecord& commitRecord) {
	ptxn::ProxySubsequencedMessageSerializer serializer(version);
	for (const auto& [_, message] : commitRecord.messages.at(version).at(storageTeamID)) {
		serializer.write(std::get<MutationRef>(message), storageTeamID);
	};
	auto serialized = serializer.getSerialized(storageTeamID);
	return serialized;
}

const int COMMIT_PEEK_CHECK_MUTATIONS = 20;

// Randomly commit to a tlog, then peek data, and verify if the data is consistent.
ACTOR Future<Void> commitPeekAndCheck(std::shared_ptr<ptxn::test::TestDriverContext> pContext) {
	state ptxn::test::print::PrintTiming printTiming("tlog/commitPeekAndCheck");

	const ptxn::TLogGroup& group = pContext->tLogGroups[0];
	ASSERT(!group.storageTeams.empty());
	state ptxn::StorageTeamID storageTeamID = group.storageTeams.begin()->first;
	printTiming << "Storage Team ID: " << storageTeamID.toString() << std::endl;

	state std::shared_ptr<ptxn::TLogInterfaceBase> tli = pContext->getTLogLeaderByStorageTeamID(storageTeamID);
	state Version prevVersion = 0; // starts from 0 for first epoch
	state Version beginVersion = 150;
	state Version endVersion(beginVersion + deterministicRandom()->randomInt(5, 20));
	state Optional<UID> debugID(ptxn::test::randomUID());

	generateMutations(beginVersion, COMMIT_PEEK_CHECK_MUTATIONS, { storageTeamID }, pContext->commitRecord);
	printTiming << "Generated " << pContext->commitRecord.getNumTotalMessages() << " messages" << std::endl;
	auto serialized = serializeMutations(beginVersion, storageTeamID, pContext->commitRecord);
	std::unordered_map<ptxn::StorageTeamID, StringRef> messages = { { storageTeamID, serialized } };

	// Commit
	ptxn::TLogCommitRequest commitRequest(ptxn::test::randomUID(),
	                                      pContext->storageTeamIDTLogGroupIDMapper[storageTeamID],
	                                      serialized.arena(),
	                                      messages,
	                                      prevVersion,
	                                      beginVersion,
	                                      0,
	                                      0,
	                                      {},
	                                      {},
	                                      std::map<ptxn::StorageTeamID, vector<Tag>>(),
	                                      debugID);
	ptxn::test::print::print(commitRequest);

	ptxn::TLogCommitReply commitReply = wait(tli->commit.getReply(commitRequest));
	ptxn::test::print::print(commitReply);

	// Peek
	ptxn::TLogPeekRequest peekRequest(debugID,
	                                  beginVersion,
	                                  endVersion,
	                                  false,
	                                  false,
	                                  storageTeamID,
	                                  pContext->storageTeamIDTLogGroupIDMapper[storageTeamID]);
	ptxn::test::print::print(peekRequest);

	ptxn::TLogPeekReply peekReply = wait(tli->peek.getReply(peekRequest));
	ptxn::test::print::print(peekReply);

	// Verify
	ptxn::SubsequencedMessageDeserializer deserializer(peekReply.data);
	ASSERT(storageTeamID == deserializer.getStorageTeamID());
	ASSERT_EQ(beginVersion, deserializer.getFirstVersion());
	ASSERT_EQ(beginVersion, deserializer.getLastVersion());
	int i = 0;
	for (auto iter = deserializer.begin(); iter != deserializer.end(); ++iter, ++i) {
		const ptxn::VersionSubsequenceMessage& m = *iter;
		ASSERT_EQ(beginVersion, m.version);
		ASSERT_EQ(i + 1, m.subsequence); // subsequence starts from 1
		ASSERT(pContext->commitRecord.messages[beginVersion][storageTeamID][i].second ==
		       std::get<MutationRef>(m.message));
	}
	printTiming << "Received " << i << " mutations" << std::endl;
	ASSERT_EQ(i, pContext->commitRecord.messages[beginVersion][storageTeamID].size());

	return Void();
}

ACTOR Future<Void> startStorageServers(std::vector<Future<Void>>* actors,
                                       std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                                       std::string folder) {
	ptxn::test::print::PrintTiming printTiming("testTLogServer/startStorageServers");
	// For demo purpose, each storage server only has one storage team
	ASSERT_EQ(pContext->numStorageServers, pContext->numStorageTeamIDs);
	state std::vector<InitializeStorageRequest> storageInitializations;
	state uint8_t locality = 0; // data center locality

	ServerDBInfo dbInfoBuilder;
	dbInfoBuilder.recoveryState = RecoveryState::ACCEPTING_COMMITS;
	dbInfoBuilder.logSystemConfig.logSystemType = LogSystemType::tagPartitioned;
	dbInfoBuilder.logSystemConfig.tLogs.emplace_back();
	dbInfoBuilder.isTestEnvironment = true;
	auto& tLogSet = dbInfoBuilder.logSystemConfig.tLogs.back();
	tLogSet.locality = locality;

	printTiming << "Assign TLog group leaders" << std::endl;
	for (auto& [groupID, interf] : pContext->tLogGroupLeaders) {
		auto tlogInterf = std::dynamic_pointer_cast<ptxn::TLogInterface_PassivelyPull>(interf);
		ASSERT(tlogInterf != nullptr);
		OptionalInterface<ptxn::TLogInterface_PassivelyPull> optionalInterface =
		    OptionalInterface<ptxn::TLogInterface_PassivelyPull>(*tlogInterf);
		tLogSet.tLogGroupIDs.push_back(groupID);
		tLogSet.ptxnTLogGroups.emplace_back();
		tLogSet.ptxnTLogGroups.back().push_back(optionalInterface);
	}
	state Reference<AsyncVar<ServerDBInfo>> dbInfo = makeReference<AsyncVar<ServerDBInfo>>(dbInfoBuilder);
	state Version tssSeedVersion = 0;
	state int i = 0;
	printTiming << "Recruiting new storage servers" << std::endl;
	for (; i < pContext->numStorageServers; i++) {
		pContext->storageServers.emplace_back();
		auto& recruited = pContext->storageServers.back();
		PromiseStream<InitializeStorageRequest> initializeStorage;
		Promise<Void> recovered;
		storageInitializations.emplace_back();

		actors->push_back(storageServer(openKVStore(KeyValueStoreType::StoreType::SSD_BTREE_V2,
		                                            joinPath(folder, "storage-" + recruited.id().toString() + ".ssd-2"),
		                                            recruited.id(),
		                                            0),
		                                recruited,
		                                Tag(locality, i),
		                                tssSeedVersion,
		                                storageInitializations.back().reply,
		                                dbInfo,
		                                folder,
		                                pContext->storageTeamIDs[0]));
		initializeStorage.send(storageInitializations.back());
		printTiming << "Recruited storage server " << i
		            << " : Storage Server Debug ID = " << recruited.id().shortString() << "\n";
	}

	// replace fake Storage Servers with recruited interface
	printTiming << "Updating interfaces" << std::endl;
	std::vector<Future<InitializeStorageReply>> interfaceFutures(pContext->numStorageServers);
	for (i = 0; i < pContext->numStorageServers; i++) {
		interfaceFutures[i] = storageInitializations[i].reply.getFuture();
	}
	std::vector<InitializeStorageReply> interfaces = wait(getAll(interfaceFutures));
	for (i = 0; i < pContext->numStorageServers; i++) {
		pContext->storageServers[i] = interfaces[i].interf;
	}
	return Void();
}

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/run_tlog_server") {
	ptxn::test::TestDriverOptions options(params);
	// Commit validation in real TLog is not supported for now
	options.skipCommitValidation = true;
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);

	state std::string folder = "simfdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);
	// start a real TLog server
	wait(startTLogServers(&actors, pContext, folder));
	// TODO: start fake proxy to talk to real TLog servers.
	startFakeSequencer(actors, pContext);
	startFakeProxy(actors, pContext);
	wait(quorum(actors, 1));
	platform::eraseDirectoryRecursive(folder);
	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/peek_tlog_server") {
	state ptxn::test::TestDriverOptions options(params);
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);

	for (const auto& group : pContext->tLogGroups) {
		ptxn::test::print::print(group);
	}

	state std::string folder = "simfdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);
	// start a real TLog server
	wait(startTLogServers(&actors, pContext, folder));
	wait(commitPeekAndCheck(pContext));

	platform::eraseDirectoryRecursive(folder);
	return Void();
}

namespace {

Version& increaseVersion(Version& version) {
	version += deterministicRandom()->randomInt(5, 10);
	return version;
}

Standalone<StringRef> getLogEntryContent(ptxn::TLogCommitRequest req, UID tlogId) {
	ptxn::TLogQueueEntryRef qe;
	qe.version = req.version;
	// when knownCommittedVersion starts to be changed(now it is 0 constant), here it needs to be changed too
	qe.knownCommittedVersion = 0;
	qe.id = tlogId;
	qe.storageTeams.reserve(req.messages.size());
	qe.messages.reserve(req.messages.size());
	for (auto& message : req.messages) {
		qe.storageTeams.push_back(message.first);
		qe.messages.push_back(message.second);
	}
	BinaryWriter wr(Unversioned()); // outer framing is not versioned
	wr << uint32_t(0);
	IncludeVersion(ProtocolVersion::withTLogQueueEntryRef()).write(wr); // payload is versioned
	wr << qe;
	wr << uint8_t(1);
	*(uint32_t*)wr.getData() = wr.getLength() - sizeof(uint32_t) - sizeof(uint8_t);
	return wr.toValue();
}

ACTOR Future<std::vector<Standalone<StringRef>>> commitInject(std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                                                              ptxn::StorageTeamID storageTeamID,
                                                              int numCommits) {
	state ptxn::test::print::PrintTiming printTiming("tlog/commitInject");

	state const ptxn::TLogGroupID tLogGroupID = pContext->storageTeamIDTLogGroupIDMapper.at(storageTeamID);
	state std::shared_ptr<ptxn::TLogInterfaceBase> pInterface = pContext->getTLogLeaderByStorageTeamID(storageTeamID);
	ASSERT(pInterface);

	state Version currVersion = 0;
	state Version prevVersion = currVersion;
	increaseVersion(currVersion);

	state std::vector<ptxn::TLogCommitRequest> requests;
	state std::vector<Standalone<StringRef>> writtenMessages;
	for (auto i = 0; i < numCommits; ++i) {
		generateMutations(currVersion, 16, { storageTeamID }, pContext->commitRecord);
		auto serialized = serializeMutations(currVersion, storageTeamID, pContext->commitRecord);
		std::unordered_map<ptxn::StorageTeamID, StringRef> messages = { { storageTeamID, serialized } };
		requests.emplace_back(ptxn::test::randomUID(),
		                      pContext->storageTeamIDTLogGroupIDMapper[storageTeamID],
		                      serialized.arena(),
		                      messages,
		                      prevVersion,
		                      currVersion,
		                      0,
		                      0,
		                      std::set<ptxn::StorageTeamID>{},
		                      std::set<ptxn::StorageTeamID>{},
		                      std::map<ptxn::StorageTeamID, vector<Tag>>(),
		                      Optional<UID>());
		writtenMessages.emplace_back(getLogEntryContent(requests.back(), pInterface->id()));
		prevVersion = currVersion;
		increaseVersion(currVersion);
	}
	printTiming << "Generated " << numCommits << " commit requests" << std::endl;
	{
		std::mt19937 g(deterministicRandom()->randomUInt32());
		std::shuffle(std::begin(requests), std::end(requests), g);
	}

	state std::vector<Future<ptxn::TLogCommitReply>> replies;
	state int index = 0;
	for (index = 0; index < numCommits; ++index) {
		printTiming << "Sending version " << requests[index].version << std::endl;
		replies.push_back(pInterface->commit.getReply(requests[index]));
		wait(delay(0.5));
	}
	wait(waitForAll(replies));
	printTiming << "Received all replies" << std::endl;

	return writtenMessages;
}

ACTOR Future<Void> verifyPeek(std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                              ptxn::StorageTeamID storageTeamID,
                              int numCommits) {
	state ptxn::test::print::PrintTiming printTiming("tlog/verifyPeek");

	state const ptxn::TLogGroupID tLogGroupID = pContext->storageTeamIDTLogGroupIDMapper.at(storageTeamID);
	state std::shared_ptr<ptxn::TLogInterfaceBase> pInterface = pContext->getTLogLeaderByStorageTeamID(storageTeamID);
	ASSERT(pInterface);

	state Version version = 0;

	state int receivedVersions = 0;
	loop {
		ptxn::TLogPeekRequest request(Optional<UID>(), version, 0, false, false, storageTeamID, tLogGroupID);
		request.endVersion.reset();
		ptxn::TLogPeekReply reply = wait(pInterface->peek.getReply(request));

		ptxn::SubsequencedMessageDeserializer deserializer(reply.data);
		Version v = deserializer.getFirstVersion();

		if (v == invalidVersion) {
			// The TLog has not received committed data, wait and check again
			wait(delay(0.001));
		} else {
			printTiming << concatToString("Received version range [",
			                              deserializer.getFirstVersion(),
			                              ", ",
			                              deserializer.getLastVersion(),
			                              "]")
			            << std::endl;
			std::vector<MutationRef> mutationRefs;
			auto iter = deserializer.begin();
			Arena deserializeArena = iter.arena();
			for (; iter != deserializer.end(); ++iter) {
				const auto& vsm = *iter;
				if (v != vsm.version) {
					printTiming << "Checking version " << v << std::endl;
					ASSERT(pContext->commitRecord.messages.find(v) != pContext->commitRecord.messages.end());
					const auto& recordedMessages = pContext->commitRecord.messages.at(v).at(storageTeamID);
					ASSERT(mutationRefs.size() == recordedMessages.size());
					for (size_t i = 0; i < mutationRefs.size(); ++i) {
						ASSERT(mutationRefs[i] == std::get<MutationRef>(recordedMessages[i].second));
					}

					mutationRefs.clear();
					v = vsm.version;
					++receivedVersions;
				}
				mutationRefs.emplace_back(std::get<MutationRef>(vsm.message));
			}

			{
				printTiming << "Checking version " << v << std::endl;
				const auto& recordedMessages = pContext->commitRecord.messages.at(v).at(storageTeamID);
				ASSERT(mutationRefs.size() == recordedMessages.size());
				for (size_t i = 0; i < mutationRefs.size(); ++i) {
					ASSERT(mutationRefs[i] == std::get<MutationRef>(recordedMessages[i].second));
				}

				++receivedVersions;
			}

			version = deserializer.getLastVersion() + 1;
		}

		if (receivedVersions == numCommits) {
			printTiming << "Over" << std::endl;
			break;
		}
	}

	return Void();
}

// Not officially used since reading from storage server in simulation is not supported yet
#if 0
ACTOR Future<Void> verifyReadStorageServer(std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                                           ptxn::StorageTeamID storageTeamID) {
	state ptxn::test::print::PrintTiming printTiming("storageServer/verifyRead");

	state StorageServerInterface pInterface = pContext->storageServers[0];

	// Get a snapshot of committed Key/Value pairs at the newest version
	state Version latestVersion;
	state std::unordered_map<StringRef, StringRef> keyValues;
	for (const auto& [version, _1] : pContext->commitRecord.messages) {
		latestVersion = version;
		for (const auto& [storageTeamID_, _2] : _1) {
			if (storageTeamID != storageTeamID_) {
				continue;
			}

			for (const auto& [subsequence, message] : _2) {
				ASSERT(message.getType() == ptxn::Message::Type::MUTATION_REF);
				keyValues[std::get<MutationRef>(message).param1] = std::get<MutationRef>(message).param2;
			}
		}
	}

	loop {
		state int tryTimes = 0;
		state bool receivedAllNewValues = true;
		state std::unordered_map<StringRef, StringRef>::iterator iter = std::begin(keyValues);

		while(iter != std::end(keyValues)) {
			state GetValueRequest getValueRequest;
			getValueRequest.key = iter->first;
			getValueRequest.version = latestVersion;

			state GetValueReply getValueReply = wait(pInterface.getValue.getReply(getValueRequest));
			if (!getValueReply.value.present() || getValueReply.value.get() != iter->second) {
				receivedAllNewValues = false;
			}
		}

		if (receivedAllNewValues) {
			break;
		}

		if (++tryTimes == 3) {
			throw internal_error_msg(
			    "After several tries the storage server is still not receiving most recent key value pairs");
		}
		wait(delay(0.1));
	}

	return Void();
}
#endif

} // anonymous namespace

TEST_CASE("/fdbserver/ptxn/test/commit_peek") {
	state ptxn::test::TestDriverOptions options(params);
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);
	state const int NUM_COMMITS = 10;

	for (const auto& group : pContext->tLogGroups) {
		ptxn::test::print::print(group);
	}

	const ptxn::TLogGroup& group = pContext->tLogGroups[0];
	state ptxn::StorageTeamID storageTeamID = group.storageTeams.begin()->first;

	state std::string folder = "simfdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);

	wait(startTLogServers(&actors, pContext, folder));
	std::vector<Standalone<StringRef>> messages = wait(commitInject(pContext, storageTeamID, NUM_COMMITS));
	wait(verifyPeek(pContext, storageTeamID, NUM_COMMITS));
	platform::eraseDirectoryRecursive(folder);
	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/run_storage_server") {
	state ptxn::test::TestDriverOptions options(params);
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);

	for (const auto& group : pContext->tLogGroups) {
		ptxn::test::print::print(group);
	}

	state std::string folder = "simfdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);
	// start real TLog servers
	wait(startTLogServers(&actors, pContext, folder));

	// Inject data, and verify the read via peek, not cursor

	std::vector<Standalone<StringRef>> messages = wait(commitInject(pContext, pContext->storageTeamIDs[0], 10));
	wait(verifyPeek(pContext, pContext->storageTeamIDs[0], 10));
	// start real storage servers
	wait(startStorageServers(&actors, pContext, folder));

	wait(delay(2.0));

	platform::eraseDirectoryRecursive(folder);
	return Void();
}

TEST_CASE("/fdbserver/ptxn/test/lock_tlog") {
	// idea: 1. lock tlog server first
	//       2. write to a random storage team affiliated to the locked tlog
	//       3. expect tlog_stopped error.

	state ptxn::test::TestDriverOptions options(params);
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);

	state std::unordered_set<ptxn::TLogGroupID> expectedLockedGroup;
	state std::unordered_set<ptxn::TLogGroupID> groupLocked;
	state std::string folder = "simfdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);
	// start real TLog servers
	wait(startTLogServers(&actors, pContext, folder));

	for (const auto& group : pContext->groupsPerTLog[0]) {
		// insert all groups affiliated to tlog[0] into a expectedSet
		expectedLockedGroup.insert(group.logGroupId);
		ptxn::test::print::print(group);
	}
	ptxn::TLogLockResult result = wait(pContext->tLogInterfaces[0]->lock.getReply<ptxn::TLogLockResult>());
	for (auto& it : result.groupResults) {
		groupLocked.insert(it.id);
	}
	bool allGroupLocked = expectedLockedGroup == groupLocked;
	ASSERT(allGroupLocked);
	ASSERT(!groupLocked.empty()); // at least 1 group belongs to tlog[0]

	int index = 0;
	for (; index < pContext->numStorageTeamIDs; index++) {
		// find the first storage team affiliated to tlog[0]
		if (pContext->getTLogLeaderByStorageTeamID(pContext->storageTeamIDs[index]) == pContext->tLogInterfaces[0]) {
			break;
		}
	}
	ASSERT(index < pContext->numStorageTeamIDs);
	state bool tlogStopped = false;
	try {
		std::vector<Standalone<StringRef>> messages = wait(commitInject(pContext, pContext->storageTeamIDs[index], 1));
	} catch (Error& e) {
		if (e.code() == error_code_tlog_stopped) {
			tlogStopped = true;
		}
	}
	ASSERT(tlogStopped);

	platform::eraseDirectoryRecursive(folder);
	return Void();
}

ACTOR Future<std::pair<std::vector<Standalone<StringRef>>, std::vector<Version> >> commitInjectReturnVersions(std::shared_ptr<ptxn::test::TestDriverContext> pContext,
                                                              ptxn::StorageTeamID storageTeamID,
                                                              int numCommits) {
	state ptxn::test::print::PrintTiming printTiming("tlog/commitInject");

	state const ptxn::TLogGroupID tLogGroupID = pContext->storageTeamIDTLogGroupIDMapper.at(storageTeamID);
	state std::shared_ptr<ptxn::TLogInterfaceBase> pInterface = pContext->getTLogLeaderByStorageTeamID(storageTeamID);
	ASSERT(pInterface);

	state Version currVersion = 0;
	state Version prevVersion = currVersion;
	increaseVersion(currVersion);

	state std::vector<ptxn::TLogCommitRequest> requests;
	state std::vector<Standalone<StringRef>> writtenMessages;
	state std::vector<Version> versions;
	for (auto i = 0; i < numCommits; ++i) {
		generateMutations(currVersion, 16, { storageTeamID }, pContext->commitRecord);
		auto serialized = serializeMutations(currVersion, storageTeamID, pContext->commitRecord);
		std::unordered_map<ptxn::StorageTeamID, StringRef> messages = { { storageTeamID, serialized } };
		requests.emplace_back(ptxn::test::randomUID(),
		                      pContext->storageTeamIDTLogGroupIDMapper[storageTeamID],
		                      serialized.arena(),
		                      messages,
		                      prevVersion,
		                      currVersion,
		                      0,
		                      0,
		                      Optional<UID>());
		writtenMessages.emplace_back(getLogEntryContent(requests.back(), pInterface->id()));
		versions.push_back(currVersion);
		prevVersion = currVersion;
		increaseVersion(currVersion);
	}
	printTiming << "Generated " << numCommits << " commit requests" << std::endl;
	{
		std::mt19937 g(deterministicRandom()->randomUInt32());
		std::shuffle(std::begin(requests), std::end(requests), g);
	}

	state std::vector<Future<ptxn::TLogCommitReply>> replies;
	state int index = 0;
	for (index = 0; index < numCommits; ++index) {
		printTiming << "Sending version " << requests[index].version << std::endl;
		replies.push_back(pInterface->commit.getReply(requests[index]));
		wait(delay(0.5));
	}
	wait(waitForAll(replies));
	printTiming << "Received all replies" << std::endl;

	return std::make_pair(writtenMessages, versions);
}

TEST_CASE("/fdbserver/ptxn/test/read_persisted_disk_on_tlog") {
	state ptxn::test::TestDriverOptions options(params);
	state std::vector<Future<Void>> actors;
	state std::shared_ptr<ptxn::test::TestDriverContext> pContext = ptxn::test::initTestDriverContext(options);
	(const_cast<ServerKnobs*> SERVER_KNOBS)->TLOG_SPILL_THRESHOLD = 0;

	for (const auto& group : pContext->tLogGroups) {
		ptxn::test::print::print(group);
	}
	const ptxn::TLogGroup& group = pContext->tLogGroups[0];
	state ptxn::StorageTeamID storageTeamID = group.storageTeams.begin()->first;

	state std::string folder = "simfdb/" + deterministicRandom()->randomAlphaNumeric(10);
	platform::createDirectory(folder);

	state std::vector<ptxn::InitializePtxnTLogRequest> tLogInitializations;
	state std::unordered_map<ptxn::TLogGroupID, IDiskQueue*> qs;
	pContext->groupsPerTLog.resize(pContext->numTLogs);
	state std::unordered_map<ptxn::TLogGroupID, IKeyValueStore*> ds;
	state std::unordered_map<ptxn::TLogGroupID, int> groupToLeaderId;
	for (int i = 0, index = 0; i < pContext->numTLogGroups; ++i) {
		ptxn::TLogGroup& tLogGroup = pContext->tLogGroups[i];
		pContext->groupsPerTLog[index].push_back(tLogGroup);
		groupToLeaderId[tLogGroup.logGroupId] = index;
		++index;
		index %= pContext->numTLogs;
	}

	state int i = 0;
	for (; i < pContext->numTLogs; i++) {
		PromiseStream<ptxn::InitializePtxnTLogRequest> initializeTLog;
		Promise<Void> recovered;
		tLogInitializations.emplace_back();
		tLogInitializations.back().isPrimary = true;
		tLogInitializations.back().storeType = KeyValueStoreType::MEMORY;
		tLogInitializations.back().tlogGroups = pContext->groupsPerTLog[i];
		UID tlogId = ptxn::test::randomUID();
		UID workerId = ptxn::test::randomUID();
		StringRef fileVersionedLogDataPrefix = "log2-"_sr;
		StringRef fileLogDataPrefix = "log-"_sr;
		ptxn::InitializePtxnTLogRequest req = tLogInitializations.back();
		const StringRef prefix = req.logVersion > TLogVersion::V2 ? fileVersionedLogDataPrefix : fileLogDataPrefix;

		std::unordered_map<ptxn::TLogGroupID, std::pair<IKeyValueStore*, IDiskQueue*>> persistentDataAndQueues;
		for (ptxn::TLogGroup& tlogGroup : pContext->groupsPerTLog[i]) {
			std::string filename =
			    filenameFromId(req.storeType, folder, prefix.toString() + "test", tlogGroup.logGroupId);
			IKeyValueStore* data = openKVStore(req.storeType, filename, tlogGroup.logGroupId, 500e6);
			state IDiskQueue* queue = new InMemoryDiskQueue(tlogGroup.logGroupId);
			qs[tlogGroup.logGroupId] = queue;
			ds[tlogGroup.logGroupId] = data;
			persistentDataAndQueues[tlogGroup.logGroupId] = std::make_pair(data, queue);
		}

		actors.push_back(ptxn::tLog(persistentDataAndQueues,
		                            makeReference<AsyncVar<ServerDBInfo>>(),
		                            LocalityData(),
		                            initializeTLog,
		                            tlogId,
		                            workerId,
		                            false,
		                            Promise<Void>(),
		                            Promise<Void>(),
		                            folder,
		                            makeReference<AsyncVar<bool>>(false),
		                            makeReference<AsyncVar<UID>>(tlogId)));
		initializeTLog.send(tLogInitializations.back());
		std::cout << "Recruit tlog " << i << " : " << tlogId.shortString() << ", workerID: " << workerId.shortString()
		          << "\n";
	}

	// replace fake TLogInterface with recruited interface
	std::vector<Future<ptxn::TLogInterface_PassivelyPull>> interfaceFutures(pContext->numTLogs);
	for (i = 0; i < pContext->numTLogs; i++) {
		interfaceFutures[i] = tLogInitializations[i].reply.getFuture();
	}
	std::vector<ptxn::TLogInterface_PassivelyPull> interfaces = wait(getAll(interfaceFutures));
	for (i = 0; i < pContext->numTLogs; i++) {
		*(pContext->tLogInterfaces[i]) = interfaces[i];
	}

	for (auto& [tLogGroupID, tLogGroupLeader] : pContext->tLogGroupLeaders) {
		tLogGroupLeader = pContext->tLogInterfaces[groupToLeaderId[tLogGroupID]];
	}

	state IKeyValueStore* d = ds[pContext->storageTeamIDTLogGroupIDMapper[storageTeamID]];

	state std::pair<std::vector<Standalone<StringRef>>, std::vector<Version> > res =
	    wait(commitInjectReturnVersions(pContext, storageTeamID, pContext->numCommits));
	state std::vector<Standalone<StringRef>> expectedMessages = res.first;
	wait(verifyPeek(pContext, storageTeamID, pContext->numCommits));

	// only wrote to a single storageTeamId, thus only 1 tlogGroup, while each tlogGroup has their own disk queue.
	state IDiskQueue* q = qs[pContext->storageTeamIDTLogGroupIDMapper[storageTeamID]];
	state bool exist = false;
	// commit to IKeyValueStore might happen in any version of our commits(might happen more than time)
	for (i = 0; i < res.second.size(); i++) {
		state Key k = ptxn::persistStorageTeamMessageRefsKey(
		    pContext->getTLogLeaderByStorageTeamID(storageTeamID)->id(), storageTeamID, res.second[i]);
		state Optional<Value> v = wait(d->readValue(k));
		exist = exist || v.present();
	}

	// we can only assert v is present, because its value is encoded by TLog and it is hard to decode it
	ASSERT(exist);
	// in this test, Location must has the same `lo` and `hi`
	// because I did not implement merging multiple location into a single StringRef and return for InMemoryDiskQueue
	ASSERT(q->getNextReadLocation().hi + pContext->numCommits == q->getNextCommitLocation().hi);
	state int commitCnt = 0;

	loop {
		state IDiskQueue::location nextLoc = q->getNextReadLocation();
		state Standalone<StringRef> actual = wait(q->read(nextLoc, nextLoc, CheckHashes::False));
		// Assert contents read are the ones that we previously wrote
		ASSERT(actual.toString() == expectedMessages[commitCnt].toString());
		q->pop(nextLoc);
		if (q->getNextReadLocation().hi >= q->getNextCommitLocation().hi) {
			break;
		}
		commitCnt++;
	}

	ASSERT(q->getNextReadLocation() == q->getNextCommitLocation());

	platform::eraseDirectoryRecursive(folder);
	return Void();
}
