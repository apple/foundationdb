/*
 * RealStorageServer.actor.cpp
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

#include "fdbclient/StorageServerInterface.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/ptxn/test/Driver.h"
#include "fdbserver/ptxn/test/FakeLogSystem.h"
#include "fdbserver/ptxn/test/FakePeekCursor.h"
#include "fdbserver/ptxn/test/Utils.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/WorkerInterface.actor.h"

#include "flow/IRandom.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ptxn::test {

struct ServerTestDriver {
	ISimulator::ProcessInfo* previousProcess;
	TaskPriority previousTask;

	ACTOR static Future<Void> switchToServerProcess(ServerTestDriver* self) {
		self->previousProcess = g_simulator.getCurrentProcess();
		self->previousTask = g_pSimulator->getCurrentTask();
		wait(g_simulator.onProcess(
		    g_simulator.newProcess(
		        "TestSystem",
		        IPAddress(0x01010101),
		        1,
		        false,
		        1,
		        LocalityData(Optional<Standalone<StringRef>>(),
		                     Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
		                     Standalone<StringRef>(deterministicRandom()->randomUniqueID().toString()),
		                     Optional<Standalone<StringRef>>()),
		        ProcessClass(ProcessClass::TesterClass, ProcessClass::CommandLineSource),
		        "",
		        "",
		        currentProtocolVersion),
		    TaskPriority::DefaultYield));
		Sim2FileSystem::newFileSystem();
		FlowTransport::createInstance(false, 1, WLTOKEN_RESERVED_COUNT);
		return Void();
	}

	ACTOR static Future<Void> switchBack(ServerTestDriver* self) {
		wait(g_simulator.onProcess(self->previousProcess, self->previousTask));
		return Void();
	}
};

struct StorageServerTestDriver : ServerTestDriver {
	struct Options {
		const int nMutationsPerMore;
		const Optional<int> maxMutations;
		const int advanceVersionsPerMutation;
		const KeyValueStoreType storeType;

		explicit Options(const UnitTestParameters& params)
		  : nMutationsPerMore(params.getInt("nMutationsPerMore").get()),
		    maxMutations(params.getInt("maxMutations").castTo<int>()),
		    advanceVersionsPerMutation(params.getInt("advanceVersionsPerMutation").get()),
		    storeType(KeyValueStoreType::fromString(params.get("keyValueStoreType").orDefault("ssd-2"))) {}
	} options;

	Reference<ptxn::test::FakePeekCursor> cursor;

	// Default tag.
	Tag tag = Tag(1, 1);
	// Set by initEndpoints()
	StorageServerInterface ssi;
	ActorCollection actors = ActorCollection(false);

	StorageServerTestDriver(const UnitTestParameters& params) : ServerTestDriver(), options(params) { initEndpoints(); }

	void initEndpoints() {
		UID uid = nondeterministicRandom()->randomUniqueID();
		ssi = StorageServerInterface(uid);
		ssi.locality = LocalityData();
		ssi.initEndpoints();
	}
};

ACTOR Future<Void> runStorageServer(StorageServerTestDriver* self) {
	state print::PrintTiming printTiming(__FUNCTION__);

	wait(delay(1));

	StorageServerInterface& ssi = self->ssi;

	const auto storeType = self->options.storeType;

	const std::string folder = ".";
	const std::string fileName = joinPath(folder, "storage-" + ssi.id().toString() + "." + storeType.toString());
	printTiming << "new Storage Server file name: " << fileName << std::endl;
	deleteFile(fileName);
	IKeyValueStore* data = openKVStore(storeType, fileName, ssi.id(), 0);

	ServerDBInfo dbInfoBuilder;
	dbInfoBuilder.logSystemConfig.logSystemType = LogSystemType::fake_FakePeekCursor;
	dbInfoBuilder.recoveryState = RecoveryState::ACCEPTING_COMMITS;
	dbInfoBuilder.isTestEnvironment = true;

	Reference<AsyncVar<ServerDBInfo>> dbInfo = makeReference<AsyncVar<ServerDBInfo>>(dbInfoBuilder);
	ReplyPromise<InitializeStorageReply> storageReady;
	const Version tssSeedVersion = 0;

	// Initialize the cursor for the storage server interface
	FakeLogSystem_CustomPeekCursor::getCursorByID(ssi.uniqueID) = self->cursor;

	printTiming << "Starting Storage Server." << std::endl;
	UID clusterId = deterministicRandom()->randomUniqueID();
	state Future<Void> ss =
	    storageServer(data, ssi, self->tag, clusterId, tssSeedVersion, storageReady, dbInfo, folder);
	printTiming << "Storage Server started." << std::endl;

	self->actors.add(ss);
	wait(ss);

	return Void();
}

ACTOR Future<Void> verifyGetValue(StorageServerTestDriver* self, Key key, Version version, Value expectedValue) {
	state print::PrintTiming printTiming(__FUNCTION__);

	wait(ServerTestDriver::switchToServerProcess(self));

	GetValueRequest getValueRequest = GetValueRequest(UID(), // spanContext
	                                                  key,
	                                                  version,
	                                                  Optional<TagSet>(), // tags
	                                                  Optional<UID>()); // debugID
	printTiming << "Sending getValue request for key " << key.toString() << std::endl;

	self->ssi.getValue.send(getValueRequest);

	GetValueReply getValueReply = wait(getValueRequest.reply.getFuture());
	const Value& value = getValueReply.value.get();
	printTiming << "Get value: " << value.toString() << ", expected " << expectedValue.toString() << std::endl;
	ASSERT(value == expectedValue);

	wait(ServerTestDriver::switchBack(self));
	return Void();
}

ACTOR Future<Void> verifyGetValueFromId(StorageServerTestDriver* self, int id, Version version) {
	auto idStr = std::to_string(id);
	wait(verifyGetValue(self, Standalone<StringRef>("Key-" + idStr), version, Standalone<StringRef>("Value-" + idStr)));
	return Void();
}

} // namespace ptxn::test

TEST_CASE("fdbserver/ptxn/test/storageserver") {
	state ptxn::test::print::PrintTiming printTiming("fdbserver/ptxn/test/storageserver");
	state ptxn::test::StorageServerTestDriver driver(params);

	Arena arena;
	Standalone<VectorRef<Tag>> tags;
	tags.push_back(tags.arena(), driver.tag);
	auto supplier =
	    ptxn::test::FakePeekCursor::VersionedMessageSupplier(0, tags, driver.options.advanceVersionsPerMutation);
	driver.cursor = makeReference<ptxn::test::FakePeekCursor>(
	    driver.options.nMutationsPerMore, driver.options.maxMutations, supplier, arena);

	int verifyId = params.getInt("verifyId").get();
	state Version commitVersion = ptxn::test::FakePeekCursor::VersionedMessageSupplier::commitVersion(
	    verifyId, driver.options.advanceVersionsPerMutation);
	printTiming << "Commit version = " << commitVersion << std::endl;
	state Future<Void> verify = ptxn::test::verifyGetValueFromId(&driver, verifyId, commitVersion);

	loop choose {
		when(wait(ptxn::test::runStorageServer(&driver))) {
			printTiming << "Storage serves exited unexpectedly" << std::endl;
			ASSERT(false);
		}
		when(wait(verify)) { break; }
		when(wait(driver.actors.getResult())) {}
	}

	return Void();
}
