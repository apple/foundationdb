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

/**
 * Workload for testing ClientLib management operations, declared in
 * MultiVersionClientControl.actor.h
 */
struct ClientLibManagementWorkload : public TestWorkload {

	static constexpr const char TEST_FILE_NAME[] = "dummyclientlib";
	static constexpr size_t FILE_CHUNK_SIZE = 128 * 1024;
	static constexpr size_t TEST_FILE_SIZE = FILE_CHUNK_SIZE * 80; // 10MB

	RandomByteGenerator rbg;
	std::string uploadedClientLibId;
	bool success;

	/*----------------------------------------------------------------
	 *  Interface
	 */

	ClientLibManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx), success(true) {}

	std::string description() const override { return "ClientLibManagement"; }

	Future<Void> setup(Database const& cx) override {
		if (clientId == 0)
			return _setup(this);

		return Void();
	}

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	/*----------------------------------------------------------------
	 *  Setup
	 */

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

	/*----------------------------------------------------------------
	 *  Tests
	 */

	ACTOR static Future<Void> _start(ClientLibManagementWorkload* self, Database cx) {
		wait(testUploadClientLibInvalidInput(self, cx));
		wait(testClientLibUploadFileDoesNotExist(self, cx));
		wait(testUploadClientLib(self, cx));
		wait(testDownloadClientLib(self, cx));
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLibInvalidInput(ClientLibManagementWorkload* self, Database cx) {
		state std::vector<std::string> invalidMetadataStrs = {
			"{foo", // invalid json
			"[]", // json array
		};

		// add garbage attribute
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		metadataJson["unknownattr"] = "someval";
		invalidMetadataStrs.push_back(json_spirit::write_string(json_spirit::mValue(metadataJson)));

		const char* mandatoryAttrs[] = { "platform", "version", "checksum", "type" };

		for (const char* attr : mandatoryAttrs) {
			validClientLibMetadataSample(metadataJson);
			metadataJson.erase(attr);
			invalidMetadataStrs.push_back(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		}

		for (auto& testMetadataStr : invalidMetadataStrs) {
			state StringRef metadataStr = StringRef(testMetadataStr);
			try {
				// Try to pass some invalid metadata input
				wait(uploadClientLibrary(cx, metadataStr, LiteralStringRef(TEST_FILE_NAME)));
				self->unexpectedSuccess(
				    "InvalidMetadata", error_code_client_lib_invalid_metadata, metadataStr.toString().c_str());
			} catch (Error& e) {
				self->testErrorCode("InvalidMetadata",
				                    error_code_client_lib_invalid_metadata,
				                    e.code(),
				                    metadataStr.toString().c_str());
			}
		}

		return Void();
	}

	ACTOR static Future<Void> testClientLibUploadFileDoesNotExist(ClientLibManagementWorkload* self, Database cx) {
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		state Standalone<StringRef> metadataStr =
		    StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		try {
			wait(uploadClientLibrary(cx, metadataStr, LiteralStringRef("some_not_existing_file_name")));
			self->unexpectedSuccess("FileDoesNotExist", error_code_file_not_found);
		} catch (Error& e) {
			self->testErrorCode("FileDoesNotExist", error_code_file_not_found, e.code());
		}
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLib(ClientLibManagementWorkload* self, Database cx) {
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);

		state Standalone<StringRef> metadataStr =
		    StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));

		getClientLibIdFromMetadataJson(metadataStr, self->uploadedClientLibId);

		// Test two concurrent uploads of the same library, one of the must fail and another succeed
		state std::vector<Future<ErrorOr<Void>>> concurrentUploads;
		for (int i1 = 0; i1 < 2; i1++) {
			Future<Void> uploadActor = uploadClientLibrary(cx, metadataStr, LiteralStringRef(TEST_FILE_NAME));
			concurrentUploads.push_back(errorOr(uploadActor));
		}

		wait(waitForAll(concurrentUploads));

		int successCnt = 0;
		for (auto uploadRes : concurrentUploads) {
			if (uploadRes.get().isError()) {
				self->testErrorCode(
				    "ConcurrentUpload", error_code_client_lib_already_exists, uploadRes.get().getError().code());
			} else {
				successCnt++;
			}
		}

		if (successCnt == 0) {
			TraceEvent(SevError, "ClientLibUploadFailed").log();
			self->success = false;
			return Void();
		} else if (successCnt > 1) {
			TraceEvent(SevError, "ClientLibConflictingUpload").log();
		}
		return Void();
	}

	ACTOR static Future<Void> testClientLibDownloadNotExisting(ClientLibManagementWorkload* self, Database cx) {
		// Generate a random valid clientLibId
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		Standalone<StringRef> metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		state std::string clientLibId;
		getClientLibIdFromMetadataJson(metadataStr, clientLibId);

		state std::string destFileName = format("clientLibDownload%d", self->clientId);

		try {
			wait(downloadClientLibrary(cx, metadataStr, destFileName));
			self->unexpectedSuccess("ClientLibDoesNotExist", error_code_client_lib_not_found);
		} catch (Error& e) {
			self->testErrorCode("ClientLibDoesNotExist", error_code_client_lib_not_found, e.code());
		}
		return Void();
	}

	ACTOR static Future<Void> testDownloadClientLib(ClientLibManagementWorkload* self, Database cx) {
		state std::string destFileName = format("clientLibDownload%d", self->clientId);
		wait(downloadClientLibrary(cx, self->uploadedClientLibId, StringRef(destFileName)));

		FILE* f = fopen(destFileName.c_str(), "r");
		if (f == nullptr) {
			TraceEvent(SevError, "ClientLibDownloadFileDoesNotExist").detail("FileName", destFileName);
			self->success = false;
		} else {
			fseek(f, 0L, SEEK_END);
			size_t fileSize = ftell(f);
			if (fileSize != TEST_FILE_SIZE) {
				TraceEvent(SevError, "ClientLibDownloadFileSizeMismatch")
				    .detail("ExpectedSize", TEST_FILE_SIZE)
				    .detail("ActualSize", fileSize);
				self->success = false;
			}
			fclose(f);
		}

		return Void();
	}

	/* ----------------------------------------------------------------
	 * Utility methods
	 */

	Reference<AsyncFileBuffer> allocateBuffer(size_t size) { return makeReference<AsyncFileBuffer>(size, false); }

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

	static void validClientLibMetadataSample(json_spirit::mObject& metadataJson) {
		metadataJson.clear();
		metadataJson["platform"] = "x86_64-linux";
		metadataJson["version"] = "7.1.0";
		metadataJson["githash"] = randomHexadecimalStr(40);
		metadataJson["type"] = "debug";
		metadataJson["checksum"] = randomHexadecimalStr(32);
		metadataJson["status"] = "available";
	}

	void unexpectedSuccess(const char* testName, int expectedErrorCode, const char* optionalDetails = nullptr) {
		TraceEvent trEv(SevError, "ClientLibManagementUnexpectedSuccess");
		trEv.detail("Test", testName).detail("ExpectedError", expectedErrorCode);
		if (optionalDetails != nullptr) {
			trEv.detail("Details", optionalDetails);
		}
		success = false;
	}

	void testErrorCode(const char* testName,
	                   int expectedErrorCode,
	                   int actualErrorCode,
	                   const char* optionalDetails = nullptr) {
		if (actualErrorCode != expectedErrorCode) {
			TraceEvent trEv(SevError, "ClientLibManagementUnexpectedError");
			trEv.detail("Test", testName)
			    .detail("ExpectedError", expectedErrorCode)
			    .detail("ActualError", actualErrorCode);
			if (optionalDetails != nullptr) {
				trEv.detail("Details", optionalDetails);
			}
			success = false;
		}
	}
};

WorkloadFactory<ClientLibManagementWorkload> ClientLibOperationsWorkloadFactory("ClientLibManagement");
