/*
 * ClientLibManagementWorkload.actor.cpp
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

#include "fdbrpc/IAsyncFile.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbclient/ClientLibManagement.actor.h"
#include "fdbserver/workloads/AsyncFile.actor.h"
#include "fdbclient/md5/md5.h"
#include "flow/Error.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace ClientLibManagement;

/**
 * Workload for testing ClientLib management operations, declared in
 * MultiVersionClientControl.actor.h
 */
struct ClientLibManagementWorkload : public TestWorkload {
	static constexpr size_t FILE_CHUNK_SIZE = 128 * 1024; // Used for test setup only

	size_t testFileSize = 0;
	RandomByteGenerator rbg;
	std::string uploadedClientLibId;
	json_spirit::mObject uploadedMetadataJson;
	Standalone<StringRef> generatedChecksum;
	std::string generatedFileName;
	bool success = true;

	/*----------------------------------------------------------------
	 *  Interface
	 */

	ClientLibManagementWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		int minTestFileSize = getOption(options, LiteralStringRef("minTestFileSize"), 0);
		int maxTestFileSize = getOption(options, LiteralStringRef("maxTestFileSize"), 1024 * 1024);
		testFileSize = deterministicRandom()->randomInt(minTestFileSize, maxTestFileSize + 1);
	}

	std::string description() const override { return "ClientLibManagement"; }

	Future<Void> setup(Database const& cx) override { return _setup(this); }

	Future<Void> start(Database const& cx) override { return _start(this, cx); }

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	/*----------------------------------------------------------------
	 *  Setup
	 */

	ACTOR Future<Void> _setup(ClientLibManagementWorkload* self) {
		state Reference<AsyncFileBuffer> data = self->allocateBuffer(FILE_CHUNK_SIZE);
		state size_t fileOffset;
		state MD5_CTX sum;
		state size_t bytesToWrite;

		self->generatedFileName = format("clientLibUpload%d", self->clientId);
		int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE |
		                IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
		state Reference<IAsyncFile> file =
		    wait(IAsyncFileSystem::filesystem()->open(self->generatedFileName, flags, 0666));

		::MD5_Init(&sum);

		for (fileOffset = 0; fileOffset < self->testFileSize; fileOffset += FILE_CHUNK_SIZE) {
			self->rbg.writeRandomBytesToBuffer(data->buffer, FILE_CHUNK_SIZE);
			bytesToWrite = std::min(FILE_CHUNK_SIZE, self->testFileSize - fileOffset);
			wait(file->write(data->buffer, bytesToWrite, fileOffset));

			::MD5_Update(&sum, data->buffer, bytesToWrite);
		}
		wait(file->sync());

		self->generatedChecksum = MD5SumToHexString(sum);

		return Void();
	}

	/*----------------------------------------------------------------
	 *  Tests
	 */

	ACTOR static Future<Void> _start(ClientLibManagementWorkload* self, Database cx) {
		wait(testUploadClientLibInvalidInput(self, cx));
		wait(testClientLibUploadFileDoesNotExist(self, cx));
		wait(testUploadClientLib(self, cx));
		if (!self->success) {
			// The further tests depend on successful upload
			return Void();
		}
		wait(testClientLibListAfterUpload(self, cx));
		wait(testDownloadClientLib(self, cx));
		wait(testDeleteClientLib(self, cx));
		wait(testUploadedClientLibInList(self, cx, ClientLibFilter(), false, "No filter, after delete"));
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLibInvalidInput(ClientLibManagementWorkload* self, Database cx) {
		state std::vector<std::string> invalidMetadataStrs = {
			"{foo", // invalid json
			"[]", // json array
		};
		state StringRef metadataStr;

		// add garbage attribute
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		metadataJson["unknownattr"] = "someval";
		invalidMetadataStrs.push_back(json_spirit::write_string(json_spirit::mValue(metadataJson)));

		const std::string mandatoryAttrs[] = { CLIENTLIB_ATTR_PLATFORM,    CLIENTLIB_ATTR_VERSION,
			                                   CLIENTLIB_ATTR_CHECKSUM,    CLIENTLIB_ATTR_TYPE,
			                                   CLIENTLIB_ATTR_GIT_HASH,    CLIENTLIB_ATTR_PROTOCOL,
			                                   CLIENTLIB_ATTR_API_VERSION, CLIENTLIB_ATTR_CHECKSUM_ALG };

		for (const std::string& attr : mandatoryAttrs) {
			validClientLibMetadataSample(metadataJson);
			metadataJson.erase(attr);
			invalidMetadataStrs.push_back(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		}

		for (auto& testMetadataStr : invalidMetadataStrs) {
			metadataStr = StringRef(testMetadataStr);
			try {
				// Try to pass some invalid metadata input
				wait(uploadClientLibrary(cx, metadataStr, self->generatedFileName));
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
		state Standalone<StringRef> metadataStr;
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		try {
			wait(uploadClientLibrary(cx, metadataStr, LiteralStringRef("some_not_existing_file_name")));
			self->unexpectedSuccess("FileDoesNotExist", error_code_file_not_found);
		} catch (Error& e) {
			self->testErrorCode("FileDoesNotExist", error_code_file_not_found, e.code());
		}
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLibWrongChecksum(ClientLibManagementWorkload* self, Database cx) {
		state Standalone<StringRef> metadataStr;
		validClientLibMetadataSample(self->uploadedMetadataJson);
		metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(self->uploadedMetadataJson)));
		getClientLibIdFromMetadataJson(metadataStr, self->uploadedClientLibId);
		try {
			wait(uploadClientLibrary(cx, metadataStr, self->generatedFileName));
			self->unexpectedSuccess("ChecksumMismatch", error_code_client_lib_invalid_binary);
		} catch (Error& e) {
			self->testErrorCode("ChecksumMismatch", error_code_client_lib_invalid_binary, e.code());
		}

		wait(testUploadedClientLibInList(self, cx, ClientLibFilter(), false, "After upload with wrong checksum"));
		return Void();
	}

	ACTOR static Future<Void> testUploadClientLib(ClientLibManagementWorkload* self, Database cx) {
		state Standalone<StringRef> metadataStr;
		state std::vector<Future<ErrorOr<Void>>> concurrentUploads;
		validClientLibMetadataSample(self->uploadedMetadataJson);
		self->uploadedMetadataJson[CLIENTLIB_ATTR_CHECKSUM] = self->generatedChecksum.toString();
		// avoid clientLibId clashes, when multiple clients try to upload the same file
		self->uploadedMetadataJson[CLIENTLIB_ATTR_TYPE] = format("devbuild%d", self->clientId);
		metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(self->uploadedMetadataJson)));
		getClientLibIdFromMetadataJson(metadataStr, self->uploadedClientLibId);

		// Test two concurrent uploads of the same library, one of the must fail and another succeed
		for (int i1 = 0; i1 < 2; i1++) {
			Future<Void> uploadActor = uploadClientLibrary(cx, metadataStr, self->generatedFileName);
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
		state std::string clientLibId;
		state std::string destFileName;
		json_spirit::mObject metadataJson;
		validClientLibMetadataSample(metadataJson);
		Standalone<StringRef> metadataStr = StringRef(json_spirit::write_string(json_spirit::mValue(metadataJson)));
		getClientLibIdFromMetadataJson(metadataStr, clientLibId);

		destFileName = format("clientLibDownload%d", self->clientId);

		try {
			wait(downloadClientLibrary(cx, clientLibId, destFileName));
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
			if (fileSize != self->testFileSize) {
				TraceEvent(SevError, "ClientLibDownloadFileSizeMismatch")
				    .detail("ExpectedSize", self->testFileSize)
				    .detail("ActualSize", fileSize);
				self->success = false;
			}
			fclose(f);
		}

		return Void();
	}

	ACTOR static Future<Void> testDeleteClientLib(ClientLibManagementWorkload* self, Database cx) {
		wait(deleteClientLibrary(cx, self->uploadedClientLibId));
		return Void();
	}

	ACTOR static Future<Void> testClientLibListAfterUpload(ClientLibManagementWorkload* self, Database cx) {
		state int uploadedApiVersion = self->uploadedMetadataJson[CLIENTLIB_ATTR_API_VERSION].get_int();
		state ClientLibPlatform uploadedPlatform =
		    getPlatformByName(self->uploadedMetadataJson[CLIENTLIB_ATTR_PLATFORM].get_str());
		state std::string uploadedVersion = self->uploadedMetadataJson[CLIENTLIB_ATTR_VERSION].get_str();
		state ClientLibFilter filter;

		filter = ClientLibFilter();
		wait(testUploadedClientLibInList(self, cx, filter, true, "No filter"));
		filter = ClientLibFilter().filterAvailable();
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter available"));
		filter = ClientLibFilter().filterAvailable().filterCompatibleAPI(uploadedApiVersion);
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter available, the same API"));
		filter = ClientLibFilter().filterAvailable().filterCompatibleAPI(uploadedApiVersion + 1);
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter available, newer API"));
		filter = ClientLibFilter().filterCompatibleAPI(uploadedApiVersion).filterPlatform(uploadedPlatform);
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter the same API, the same platform"));
		ASSERT(uploadedPlatform != CLIENTLIB_X86_64_WINDOWS);
		filter = ClientLibFilter().filterAvailable().filterPlatform(CLIENTLIB_X86_64_WINDOWS);
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter available, different platform"));
		filter = ClientLibFilter().filterAvailable().filterNewerPackageVersion(uploadedVersion);
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter available, the same version"));
		filter =
		    ClientLibFilter().filterAvailable().filterNewerPackageVersion("1.15.10").filterPlatform(uploadedPlatform);
		wait(testUploadedClientLibInList(
		    self, cx, filter, true, "Filter available, an older version, the same platform"));
		filter = ClientLibFilter()
		             .filterAvailable()
		             .filterNewerPackageVersion(uploadedVersion)
		             .filterPlatform(uploadedPlatform);
		wait(testUploadedClientLibInList(
		    self, cx, filter, false, "Filter available, the same version, the same platform"));
		filter = ClientLibFilter().filterNewerPackageVersion("100.1.1");
		wait(testUploadedClientLibInList(self, cx, filter, false, "Filter a newer version"));
		filter = ClientLibFilter().filterNewerPackageVersion("1.15.10");
		wait(testUploadedClientLibInList(self, cx, filter, true, "Filter an older version"));
		return Void();
	}

	ACTOR static Future<Void> testUploadedClientLibInList(ClientLibManagementWorkload* self,
	                                                      Database cx,
	                                                      ClientLibFilter filter,
	                                                      bool expectInList,
	                                                      const char* testDescr) {
		Standalone<VectorRef<StringRef>> allLibs = wait(listClientLibraries(cx, filter));
		bool found = false;
		for (StringRef metadataJson : allLibs) {
			std::string clientLibId;
			getClientLibIdFromMetadataJson(metadataJson, clientLibId);
			if (clientLibId == self->uploadedClientLibId) {
				found = true;
			}
		}
		if (found != expectInList) {
			TraceEvent(SevError, "ClientLibInListTestFailed")
			    .detail("Test", testDescr)
			    .detail("ClientLibId", self->uploadedClientLibId)
			    .detail("Expected", expectInList)
			    .detail("Actual", found);
			self->success = false;
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
		metadataJson[CLIENTLIB_ATTR_PLATFORM] = getPlatformName(CLIENTLIB_X86_64_LINUX);
		metadataJson[CLIENTLIB_ATTR_VERSION] = "7.1.0";
		metadataJson[CLIENTLIB_ATTR_GIT_HASH] = randomHexadecimalStr(40);
		metadataJson[CLIENTLIB_ATTR_TYPE] = "debug";
		metadataJson[CLIENTLIB_ATTR_CHECKSUM] = randomHexadecimalStr(32);
		metadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(CLIENTLIB_AVAILABLE);
		metadataJson[CLIENTLIB_ATTR_API_VERSION] = 710;
		metadataJson[CLIENTLIB_ATTR_PROTOCOL] = "fdb00b07001001";
		metadataJson[CLIENTLIB_ATTR_CHECKSUM_ALG] = "md5";
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
