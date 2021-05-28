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

#include "fdbrpc/simulator.h"
#include "fdbserver/MockPeekCursor.h"
#include "fdbserver/MockLogSystem.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/IKeyValueStore.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/RecoveryState.h"
#include "fdbserver/ptxn/test/Driver.h"

#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ptxn {

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
		FlowTransport::createInstance(false, 1);
		return Void();
	}

	ACTOR static Future<Void> switchBack(ServerTestDriver* self) {
		wait(g_simulator.onProcess(self->previousProcess, self->previousTask));
		return Void();
	}
};

struct StorageServerTestDriver : ServerTestDriver {
	Reference<MockPeekCursor> mockPeekCursor;

	// Default tag.
	Tag tag = Tag(1, 1);
	// Set by initEndpoints()
	StorageServerInterface ssi;
	ActorCollection actors = ActorCollection(false);

	StorageServerTestDriver() : ServerTestDriver() { initEndpoints(); }

	void initEndpoints() {
		UID uid = nondeterministicRandom()->randomUniqueID();
		ssi = StorageServerInterface(uid);
		ssi.locality = LocalityData();
		ssi.initEndpoints();
	}

	ACTOR static Future<Void> runStorageServer(StorageServerTestDriver* self) {
		wait(delay(1));

		StorageServerInterface& ssi = self->ssi;

		// TODO: Make this parameterizable. We'll eventually want to test this against more than the one storage server
		// implementation.
		KeyValueStoreType storeType = KeyValueStoreType::SSD_BTREE_V2;
		state std::string folder = ".";
		std::string fileName = joinPath(folder, "storage-" + ssi.id().toString() + "." + storeType.toString());
		std::cout << "new Storage Server file name: " << fileName << std::endl;
		deleteFile(fileName);
		state IKeyValueStore* data = openKVStore(storeType, fileName, ssi.id(), 0);

		state ReplyPromise<InitializeStorageReply> storageReady;

		ServerDBInfo dbInfoBuilder;
		dbInfoBuilder.recoveryState = RecoveryState::ACCEPTING_COMMITS;
		state Reference<AsyncVar<ServerDBInfo>> dbInfo = makeReference<AsyncVar<ServerDBInfo>>(dbInfoBuilder);

		std::shared_ptr<MockLogSystem> mockLogSystem = std::make_shared<MockLogSystem>();
		mockLogSystem->cursor = self->mockPeekCursor.castTo<ILogSystem::IPeekCursor>();

		std::cout << "Starting Storage Server." << std::endl;
		state Future<Void> ss = storageServer(data, ssi, self->tag, storageReady, dbInfo, folder, mockLogSystem);
		std::cout << "Storage Server started." << std::endl;

		self->actors.add(ss);
		wait(ss);

		return Void();
	}

	ACTOR static Future<Void> verifyGetValue(StorageServerTestDriver* self,
	                                         KeyRef key,
	                                         Version version,
	                                         ValueRef expectedValue) {
		wait(switchToServerProcess(self));

		GetValueRequest getValueRequest = GetValueRequest(UID(), // spanContext
		                                                  key,
		                                                  version,
		                                                  Optional<TagSet>(), // tags
		                                                  Optional<UID>()); // debugID
		self->ssi.getValue.send(getValueRequest);

		GetValueReply getValueReply = wait(getValueRequest.reply.getFuture());
		const Value& value = getValueReply.value.get();
		std::cout << "Get value: " << value.toString() << std::endl;
		ASSERT(value == expectedValue);

		wait(switchBack(self));
		return Void();
	}
};
} // namespace ptxn

TEST_CASE("fdbserver/ptxn/test/storageserver") {

	state ptxn::StorageServerTestDriver driver;

	// Set the mock cursor with some input.
	Arena arena;
	MutationRef mutation(arena, MutationRef::SetValue, "Hello"_sr, "World"_sr);
	std::string message = BinaryWriter::toValue(mutation, AssumeVersion(currentProtocolVersion)).toString();
	std::vector<Tag> tags = { driver.tag };
	state std::vector<MockPeekCursor::OneVersionMessages> allVersionMessages = { MockPeekCursor::OneVersionMessages(
		8, // version
		{ MockPeekCursor::MessageAndTags(message, tags) }) };
	driver.mockPeekCursor = makeReference<MockPeekCursor>(allVersionMessages, arena);

	loop choose{ when(wait(ptxn::StorageServerTestDriver::runStorageServer(&driver))){
		std::cout << "Storage serves exited unexpectedly" << std::endl;
	ASSERT(false);
        }
        when(wait(ptxn::StorageServerTestDriver::verifyGetValue(&driver, "Hello"_sr, 8, "World"_sr))) {
	        break;
        }
        when(wait(driver.actors.getResult())) {}
    };

    return Void();
}
