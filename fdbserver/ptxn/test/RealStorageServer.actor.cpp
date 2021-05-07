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

ACTOR Future<Void> driverRunStorageServer(StorageServerInterface* ssi,
                                          std::vector<Future<Void>>* actors,
                                          Tag tag,
                                          Reference<MockPeekCursor> mockPeekCursor) {

	KeyValueStoreType storeType = KeyValueStoreType::SSD_BTREE_V2;
	state std::string folder = ".";
	std::string fileName = joinPath(folder, "storage-" + ssi->id().toString() + "." + storeType.toString());
	std::cout << "new Storage Server file name: " << fileName << std::endl;
	deleteFile(fileName);
	state IKeyValueStore* data = openKVStore(storeType, fileName, ssi->id(), 0);

	state ReplyPromise<InitializeStorageReply> storageReady;

	// Initialize dbInfo with mocked cursor and mocked loy system.

	// TODO: Use Reference API?
	// TODO: delete mockLogSystem eventually.
	state MockLogSystem* mockLogSystem = new MockLogSystem();
	mockLogSystem->cursor = mockPeekCursor.castTo<ILogSystem::IPeekCursor>();

	LogSystemConfig logSystemConfig;
	logSystemConfig.logSystemType = LogSystemType::mock;
	logSystemConfig.mockLogSystem = mockLogSystem;

	ServerDBInfo dbInfoBuilder;
	dbInfoBuilder.recoveryState = RecoveryState::ACCEPTING_COMMITS;
	dbInfoBuilder.logSystemConfig = logSystemConfig;
	state Reference<AsyncVar<ServerDBInfo>> dbInfo = makeReference<AsyncVar<ServerDBInfo>>(dbInfoBuilder);
	// Initialize dbInfo done.

	std::cout << "Starting Storage Server" << std::endl;
	state Future<Void> ss = storageServer(data, *ssi, tag, storageReady, dbInfo, folder);
	actors->emplace_back(ss);

	// Delay 0.1 seconds to wait storage server processing the mutations.
	// TODO: Wait until certain desired version is reached rather than predefined delay time.
	wait(delay(0.1));
	std::cout << "After waiting for 0.1 seconds" << std::endl;

	return Void();
}

ACTOR Future<Void> driverVerifyGetValue(StorageServerInterface* ssi,
                                        KeyRef key,
                                        Version version,
                                        ValueRef expectedValue) {
	GetValueRequest getValueRequest(UID(), // spanContext
	                                key,
	                                version,
	                                Optional<TagSet>(), // tags
	                                Optional<UID>()); // debugID
	getValueRequest._requestTime = timer();

	ssi->getValue.send(getValueRequest);

	GetValueReply getValueReply = wait(getValueRequest.reply.getFuture());

	const Value& value = getValueReply.value.get();
	std::cout << "Get value: " << value.toString() << std::endl;
	ASSERT(value == expectedValue);

	return Void();
}

struct StorageServerTestDriver {
	Reference<MockPeekCursor> mockPeekCursor;

	// Default tag.
	Tag tag = Tag(1, 1);
	// Set by initEndpoints()
	StorageServerInterface ssi;
	std::vector<Future<Void>> actors;

	StorageServerTestDriver() { initEndpoints(); }

	void initEndpoints() {
		UID uid = nondeterministicRandom()->randomUniqueID();
		ssi = StorageServerInterface(uid);
		ssi.locality = LocalityData();
		ssi.initEndpoints();
	}

	Future<Void> runStorageServer() { return driverRunStorageServer(&ssi, &actors, tag, mockPeekCursor); }

	Future<Void> verifyGetValue(KeyRef key, Version version, ValueRef expectedValue) {
		return driverVerifyGetValue(&ssi, key, version, expectedValue);
	}
};
} // namespace ptxn

TEST_CASE("fdbserver/ptxn/test/storageserver") {
	using namespace ptxn;

	state ptxn::StorageServerTestDriver driver;

	// Set the mock cursor with some input
	Arena arena;
	MutationRef mutation(arena, MutationRef::SetValue, "Hello"_sr, "World"_sr);
	std::string message = BinaryWriter::toValue(mutation, AssumeVersion(currentProtocolVersion)).toString();
	std::vector<Tag> tags = { driver.tag };
	state std::vector<MockPeekCursor::OneVersionMessages> allVersionMessages = { MockPeekCursor::OneVersionMessages(
		8, // version
		{ MockPeekCursor::MessageAndTags(message, tags) }) };
	driver.mockPeekCursor = makeReference<MockPeekCursor>(allVersionMessages, arena);

	wait(driver.runStorageServer());

	driver.verifyGetValue("Hello"_sr, 8, "World"_sr);

	return Void();
}
