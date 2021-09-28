/*
 * ClientLibManagement.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/MultiVersionClientControl.actor.h"
#include "fdbserver/workloads/AsyncFile.actor.h"

#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ClientLibManagementWorkload : public TestWorkload {

	static constexpr const char TEST_FILE_NAME[] = "dummyclientlib";
	static constexpr size_t FILE_CHUNK_SIZE = 128 * 1024;
	static constexpr size_t TEST_FILE_SIZE = FILE_CHUNK_SIZE * 80; // 10MB

	RandomByteGenerator rbg;

	ClientLibManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	std::string description() const override { return "ClientLibManagement"; }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0)
			return _setup(this);

		return Void();
	}

	Reference<AsyncFileBuffer> allocateBuffer(size_t size) { return makeReference<AsyncFileBuffer>(size, false); }

	ACTOR Future<Void> _setup(ClientLibManagementWorkload* self) {
		int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE;
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(TEST_FILE_NAME, flags, 0666));
		ASSERT(file.isValid());

		state Reference<AsyncFileBuffer> data = self->allocateBuffer(FILE_CHUNK_SIZE);
		state int64_t i;
		state Future<Void> lastWrite = Void();
		for (i = 0; i < TEST_FILE_SIZE; i += FILE_CHUNK_SIZE) {
			self->rbg.writeRandomBytesToBuffer(data->buffer, FILE_CHUNK_SIZE);
			wait(file->write(data->buffer, FILE_CHUNK_SIZE, i));
		}
		wait(file->sync());
		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	static std::string randomHexadecimalStr(int length) {
		std::string s;
		s.reserve(length);
		for (int i = 0; i < length; i++) {
			uint32_t hexDigit = static_cast<char>(deterministicRandom()->randomUInt32() % 16);
			char ch = (hexDigit >= 10 ? hexDigit - 10 + 'a' : hexDigit + '0');
			s += ch;
		}
		return s;
	}

	ACTOR static Future<Void> _start(ClientLibManagementWorkload* self, Database cx) {
		json_spirit::mObject metadataJson;
		metadataJson["platform"] = "x86_64-linux";
		metadataJson["version"] = "7.1.0";
		metadataJson["githash"] = randomHexadecimalStr(40);
		metadataJson["type"] = "debug";
		metadataJson["checksum"] = randomHexadecimalStr(32);
		metadataJson["status"] = "available";

		state Standalone<StringRef> metadataStr =
		    StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));

		wait(uploadClientLibrary(cx, metadataStr, LiteralStringRef(TEST_FILE_NAME)));

		return Void();
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ClientLibManagementWorkload> ClientLibOperationsWorkloadFactory("ClientLibManagement");
