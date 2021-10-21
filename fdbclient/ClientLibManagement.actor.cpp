/*
 * ClientLibManagement.actor.cpp
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

#include "fdbclient/ClientLibManagement.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ClientKnobs.h"
#include "fdbclient/versions.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/Platform.h"

#include <algorithm>
#include <string>
#include <stdio.h>

#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace ClientLibManagement {

struct ClientLibBinaryInfo {
	size_t totalBytes = 0;
	size_t chunkCnt = 0;
	size_t chunkSize = 0;
	Standalone<StringRef> sumBytes;
};

#define ASSERT_INDEX_IN_RANGE(idx, arr) ASSERT(idx >= 0 && idx < sizeof(arr) / sizeof(arr[0]))

const std::string& getStatusName(ClientLibStatus status) {
	static const std::string statusNames[] = { "disabled", "available", "uploading" };
	int idx = static_cast<int>(status);
	ASSERT_INDEX_IN_RANGE(idx, statusNames);
	return statusNames[idx];
}

ClientLibStatus getStatusByName(std::string_view statusName) {
	static std::map<std::string_view, ClientLibStatus> statusByName;
	// initialize the map on demand
	if (statusByName.empty()) {
		for (int i = 0; i < static_cast<int>(ClientLibStatus::COUNT); i++) {
			ClientLibStatus status = static_cast<ClientLibStatus>(i);
			statusByName[getStatusName(status)] = status;
		}
	}
	auto statusIter = statusByName.find(statusName);
	if (statusIter == statusByName.cend()) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Unknown status value %s", std::string(statusName).c_str()));
		throw client_lib_invalid_metadata();
	}
	return statusIter->second;
}

const std::string& getPlatformName(ClientLibPlatform platform) {
	static const std::string platformNames[] = { "unknown", "x84_64-linux", "x86_64-windows", "x86_64-macos" };
	int idx = static_cast<int>(platform);
	ASSERT_INDEX_IN_RANGE(idx, platformNames);
	return platformNames[idx];
}

ClientLibPlatform getPlatformByName(std::string_view platformName) {
	static std::map<std::string_view, ClientLibPlatform> platformByName;
	// initialize the map on demand
	if (platformByName.empty()) {
		for (int i = 0; i < static_cast<int>(ClientLibStatus::COUNT); i++) {
			ClientLibPlatform platform = static_cast<ClientLibPlatform>(i);
			platformByName[getPlatformName(platform)] = platform;
		}
	}
	auto platfIter = platformByName.find(platformName);
	if (platfIter == platformByName.cend()) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Unknown platform value %s", std::string(platformName).c_str()));
		throw client_lib_invalid_metadata();
	}
	return platfIter->second;
}

const std::string& getChecksumAlgName(ClientLibChecksumAlg checksumAlg) {
	static const std::string checksumAlgNames[] = { "md5" };
	int idx = static_cast<int>(checksumAlg);
	ASSERT_INDEX_IN_RANGE(idx, checksumAlgNames);
	return checksumAlgNames[idx];
}

ClientLibChecksumAlg getChecksumAlgByName(std::string_view checksumAlgName) {
	static std::map<std::string_view, ClientLibChecksumAlg> checksumAlgByName;
	// initialize the map on demand
	if (checksumAlgByName.empty()) {
		for (int i = 0; i < (int)ClientLibChecksumAlg::COUNT; i++) {
			ClientLibChecksumAlg checksumAlg = static_cast<ClientLibChecksumAlg>(i);
			checksumAlgByName[getChecksumAlgName(checksumAlg)] = checksumAlg;
		}
	}
	auto iter = checksumAlgByName.find(checksumAlgName);
	if (iter == checksumAlgByName.cend()) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Unknown checksum algorithm %s", std::string(checksumAlgName).c_str()));
		throw client_lib_invalid_metadata();
	}
	return iter->second;
}

namespace {

bool isValidTargetStatus(ClientLibStatus status) {
	return status == ClientLibStatus::AVAILABLE || status == ClientLibStatus::DISABLED;
}

json_spirit::mObject parseMetadataJson(StringRef metadataString) {
	json_spirit::mValue parsedMetadata;
	if (!json_spirit::read_string(metadataString.toString(), parsedMetadata) ||
	    parsedMetadata.type() != json_spirit::obj_type) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "InvalidJSON")
		    .detail("Configuration", metadataString);
		throw client_lib_invalid_metadata();
	}

	return parsedMetadata.get_obj();
}

const std::string& getMetadataStrAttr(const json_spirit::mObject& metadataJson, const std::string& attrName) {
	auto attrIter = metadataJson.find(attrName);
	if (attrIter == metadataJson.cend() || attrIter->second.type() != json_spirit::str_type) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Missing attribute %s", attrName.c_str()));
		throw client_lib_invalid_metadata();
	}
	return attrIter->second.get_str();
}

int getMetadataIntAttr(const json_spirit::mObject& metadataJson, const std::string& attrName) {
	auto attrIter = metadataJson.find(attrName);
	if (attrIter == metadataJson.cend() || attrIter->second.type() != json_spirit::int_type) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Missing attribute %s", attrName.c_str()));
		throw client_lib_invalid_metadata();
	}
	return attrIter->second.get_int();
}

bool validVersionPartNum(int num) {
	return (num >= 0 && num < 1000);
}

int getNumericVersionEncoding(const std::string& versionStr) {
	int major, minor, patch;
	int charsScanned;
	int numScanned = sscanf(versionStr.c_str(), "%d.%d.%d%n", &major, &minor, &patch, &charsScanned);
	if (numScanned != 3 || !validVersionPartNum(major) || !validVersionPartNum(minor) || !validVersionPartNum(patch) ||
	    charsScanned != versionStr.size()) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Invalid version string %s", versionStr.c_str()));
		throw client_lib_invalid_metadata();
	}
	return ((major * 1000) + minor) * 1000 + patch;
}

Standalone<StringRef> getIdFromMetadataJson(const json_spirit::mObject& metadataJson) {
	std::ostringstream libIdBuilder;
	libIdBuilder << getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_PLATFORM) << "/";
	libIdBuilder << format("%09d", getNumericVersionEncoding(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_VERSION)))
	             << "/";
	libIdBuilder << getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_TYPE) << "/";
	libIdBuilder << getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM);
	return Standalone<StringRef>(libIdBuilder.str());
}

Key metadataKeyFromId(StringRef clientLibId) {
	return clientLibId.withPrefix(clientLibMetadataPrefix);
}

Key chunkKeyPrefixFromId(StringRef clientLibId) {
	return clientLibId.withPrefix(clientLibBinaryPrefix).withSuffix(LiteralStringRef("/"));
}

KeyRef chunkKeyFromNo(StringRef clientLibBinPrefix, size_t chunkNo, Arena& arena) {
	return clientLibBinPrefix.withSuffix(format("%06zu", chunkNo), arena);
}

ClientLibPlatform getCurrentClientPlatform() {
#ifdef __x86_64__
#if defined(_WIN32)
	return ClientLibPlatform::X86_64_WINDOWS;
#elif defined(__linux__)
	return ClientLibPlatform::X86_64_LINUX;
#elif defined(__FreeBSD__) || defined(__APPLE__)
	return ClientLibPlatform::X86_64_MACOS;
#else
	return ClientLibPlatform::UNKNOWN;
#endif
#else // not __x86_64__
	return ClientLibPlatform::UNKNOWN;
#endif
}

Standalone<StringRef> byteArrayToHexString(StringRef input) {
	static const char* digits = "0123456789abcdef";
	Standalone<StringRef> output = makeString(input.size() * 2);
	char* pout = reinterpret_cast<char*>(mutateString(output));
	for (const uint8_t* pin = input.begin(); pin != input.end(); ++pin) {
		*pout++ = digits[(*pin >> 4) & 0xF];
		*pout++ = digits[(*pin) & 0xF];
	}
	return output;
}

} // namespace

Standalone<StringRef> md5SumToHexString(MD5_CTX& sum) {
	Standalone<StringRef> sumBytes = makeString(16);
	::MD5_Final(mutateString(sumBytes), &sum);
	return byteArrayToHexString(sumBytes);
}

ClientLibFilter& ClientLibFilter::filterNewerPackageVersion(const std::string& versionStr) {
	matchNewerPackageVersion = true;
	this->numericPkgVersion = getNumericVersionEncoding(versionStr);
	return *this;
}

Standalone<StringRef> getClientLibIdFromMetadataJson(StringRef metadataString) {
	json_spirit::mObject parsedMetadata = parseMetadataJson(metadataString);
	return getIdFromMetadataJson(parsedMetadata);
}

namespace {

ACTOR Future<Void> uploadClientLibBinary(Database db,
                                         StringRef libFilePath,
                                         KeyRef chunkKeyPrefix,
                                         ClientLibBinaryInfo* binInfo) {

	state int chunkSize = getAlignedUpperBound(CLIENT_KNOBS->MVC_CLIENTLIB_CHUNK_SIZE, 1024);
	state int transactionSize = std::max(CLIENT_KNOBS->MVC_CLIENTLIB_CHUNKS_PER_TRANSACTION, 1) * chunkSize;
	state size_t fileOffset = 0;
	state size_t chunkNo = 0;
	state MD5_CTX sum;
	state Arena arena;
	state StringRef buf;
	state Transaction tr;
	state size_t firstChunkNo;

	// Disabling AIO, because it supports only page-aligned writes
	state Reference<IAsyncFile> fClientLib = wait(IAsyncFileSystem::filesystem()->open(
	    libFilePath.toString(), IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO, 0));

	::MD5_Init(&sum);

	loop {
		arena = Arena();
		buf = makeAlignedString(_PAGE_SIZE, transactionSize, arena);
		state int bytesRead = wait(fClientLib->read(mutateString(buf), transactionSize, fileOffset));
		fileOffset += bytesRead;
		if (bytesRead <= 0) {
			break;
		}

		::MD5_Update(&sum, buf.begin(), bytesRead);

		tr = Transaction(db);
		firstChunkNo = chunkNo;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				int bufferOffset = 0;
				chunkNo = firstChunkNo;
				while (bufferOffset < bytesRead) {
					size_t chunkLen = std::min(chunkSize, bytesRead - bufferOffset);
					KeyRef chunkKey = chunkKeyFromNo(chunkKeyPrefix, chunkNo, arena);
					chunkNo++;
					tr.set(chunkKey, ValueRef(mutateString(buf) + bufferOffset, chunkLen));
					bufferOffset += chunkLen;
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		if (bytesRead < transactionSize) {
			break;
		}
	}
	binInfo->totalBytes = fileOffset;
	binInfo->chunkCnt = chunkNo;
	binInfo->chunkSize = chunkSize;
	binInfo->sumBytes = md5SumToHexString(sum);
	return Void();
}

} // namespace

ACTOR Future<Void> uploadClientLibrary(Database db,
                                       Standalone<StringRef> metadataString,
                                       Standalone<StringRef> libFilePath) {
	state json_spirit::mObject metadataJson;
	state Standalone<StringRef> clientLibId;
	state Key clientLibMetaKey;
	state Key clientLibBinPrefix;
	state std::string jsStr;
	state Transaction tr;
	state ClientLibBinaryInfo binInfo;
	state ClientLibStatus targetStatus;

	metadataJson = parseMetadataJson(metadataString);

	json_spirit::mValue schema;
	if (!json_spirit::read_string(JSONSchemas::clientLibMetadataSchema.toString(), schema)) {
		ASSERT(false);
	}

	std::string errorStr;
	if (!schemaMatch(schema.get_obj(), metadataJson, errorStr, SevWarnAlways)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "SchemaMismatch")
		    .detail("Configuration", metadataString)
		    .detail("Error", errorStr);
		throw client_lib_invalid_metadata();
	}

	clientLibId = getIdFromMetadataJson(metadataJson);
	clientLibMetaKey = metadataKeyFromId(clientLibId);
	clientLibBinPrefix = chunkKeyPrefixFromId(clientLibId);

	targetStatus = getStatusByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_STATUS));
	if (!isValidTargetStatus(targetStatus)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "InvalidTargetStatus")
		    .detail("Configuration", metadataString);
		throw client_lib_invalid_metadata();
	}

	// check if checksumalg and platform attributes have valid values
	getChecksumAlgByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM_ALG));
	getPlatformByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_PLATFORM));

	// Check if further mandatory attributes are set
	getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_GIT_HASH);
	getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_PROTOCOL);
	getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_API_VERSION);

	metadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(ClientLibStatus::UPLOADING);
	jsStr = json_spirit::write_string(json_spirit::mValue(metadataJson));

	/*
	 * Check if the client library with the same identifier already exists.
	 * If not, write its metadata with "uploading" state to prevent concurrent uploads
	 */
	tr = Transaction(db);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> existingMeta = wait(tr.get(clientLibMetaKey));
			if (existingMeta.present()) {
				TraceEvent(SevWarnAlways, "ClientLibraryAlreadyExists")
				    .detail("Key", clientLibMetaKey)
				    .detail("ExistingMetadata", existingMeta.get().toString());
				throw client_lib_already_exists();
			}

			TraceEvent("ClientLibraryBeginUpload").detail("Key", clientLibMetaKey);

			tr.set(clientLibMetaKey, ValueRef(jsStr));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	/*
	 * Upload the binary of the client library in chunks
	 */
	wait(uploadClientLibBinary(db, libFilePath, clientLibBinPrefix, &binInfo));

	std::string checkSum = getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM);
	if (binInfo.sumBytes != StringRef(checkSum)) {
		TraceEvent(SevWarnAlways, "ClientLibraryChecksumMismatch")
		    .detail("Expected", checkSum)
		    .detail("Actual", binInfo.sumBytes)
		    .detail("Configuration", metadataString);
		// Rollback the upload operation
		try {
			wait(deleteClientLibrary(db, clientLibId));
		} catch (Error& e) {
			TraceEvent(SevError, "ClientLibraryUploadRollbackFailed").error(e);
		}
		throw client_lib_invalid_binary();
	}

	/*
	 * Update the metadata entry, with additional information about the binary
	 * and change its state from "uploading" to the given one
	 */
	metadataJson[CLIENTLIB_ATTR_SIZE] = static_cast<int64_t>(binInfo.totalBytes);
	metadataJson[CLIENTLIB_ATTR_CHUNK_COUNT] = static_cast<int64_t>(binInfo.chunkCnt);
	metadataJson[CLIENTLIB_ATTR_CHUNK_SIZE] = static_cast<int64_t>(binInfo.chunkSize);
	metadataJson[CLIENTLIB_ATTR_FILENAME] = basename(libFilePath.toString());
	metadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(targetStatus);
	jsStr = json_spirit::write_string(json_spirit::mValue(metadataJson));

	tr.reset();
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.set(clientLibMetaKey, ValueRef(jsStr));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	TraceEvent("ClientLibraryUploadDone").detail("Key", clientLibMetaKey);
	return Void();
}

ACTOR Future<Void> downloadClientLibrary(Database db,
                                         Standalone<StringRef> clientLibId,
                                         Standalone<StringRef> libFilePath) {
	state Key clientLibMetaKey = metadataKeyFromId(clientLibId);
	state Key chunkKeyPrefix = chunkKeyPrefixFromId(clientLibId);
	state int chunksPerTransaction = std::max(CLIENT_KNOBS->MVC_CLIENTLIB_CHUNKS_PER_TRANSACTION, 1);
	state int transactionSize;
	state json_spirit::mObject metadataJson;
	state std::string checkSum;
	state size_t chunkCount;
	state size_t binarySize;
	state size_t expectedChunkSize;
	state Transaction tr;
	state size_t fileOffset;
	state MD5_CTX sum;
	state Arena arena;
	state StringRef buf;
	state size_t bufferOffset;
	state size_t fromChunkNo;
	state size_t toChunkNo;
	state std::vector<Future<Optional<Value>>> chunkFutures;

	TraceEvent("ClientLibraryBeginDownload").detail("Key", clientLibMetaKey);

	/*
	 * First read the metadata to get information about the status and
	 * the chunk count of the client library
	 */
	loop {
		tr = Transaction(db);
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			Optional<Value> metadataOpt = wait(tr.get(clientLibMetaKey));
			if (!metadataOpt.present()) {
				TraceEvent(SevWarnAlways, "ClientLibraryNotFound").detail("Key", clientLibMetaKey);
				throw client_lib_not_found();
			}
			metadataJson = parseMetadataJson(metadataOpt.get());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	// Allow downloading only libraries in the available state
	if (getStatusByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_STATUS)) != ClientLibStatus::AVAILABLE) {
		throw client_lib_not_available();
	}

	// Disabling AIO, because it supports only page-aligned writes
	int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE |
	                IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO;
	state Reference<IAsyncFile> fClientLib =
	    wait(IAsyncFileSystem::filesystem()->open(libFilePath.toString(), flags, 0666));

	checkSum = getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM);
	chunkCount = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_CHUNK_COUNT);
	binarySize = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_SIZE);
	expectedChunkSize = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_CHUNK_SIZE);
	transactionSize = chunksPerTransaction * expectedChunkSize;
	fileOffset = 0;
	fromChunkNo = 0;

	::MD5_Init(&sum);

	arena = Arena();
	buf = makeAlignedString(_PAGE_SIZE, transactionSize, arena);

	loop {
		if (fromChunkNo == chunkCount) {
			break;
		}

		tr = Transaction(db);
		toChunkNo = std::min(chunkCount, fromChunkNo + chunksPerTransaction);

		// read a batch of file chunks concurrently
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);

				chunkFutures.clear();
				for (size_t chunkNo = fromChunkNo; chunkNo < toChunkNo; chunkNo++) {
					KeyRef chunkKey = chunkKeyFromNo(chunkKeyPrefix, chunkNo, arena);
					chunkFutures.push_back(tr.get(chunkKey));
				}

				wait(waitForAll(chunkFutures));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// check the read chunks and copy them to a buffer
		bufferOffset = 0;
		size_t chunkNo = fromChunkNo;
		for (auto chunkOptFuture : chunkFutures) {
			if (!chunkOptFuture.get().present()) {
				TraceEvent(SevWarnAlways, "ClientLibraryChunkNotFound")
				    .detail("Key", chunkKeyFromNo(chunkKeyPrefix, chunkNo, arena));
				throw client_lib_invalid_binary();
			}
			StringRef chunkVal = chunkOptFuture.get().get();

			// All chunks exept for the last one must be of the expected size to guarantee
			// alignment when writing to file
			if ((chunkNo != (chunkCount - 1) && chunkVal.size() != expectedChunkSize) ||
			    chunkVal.size() > expectedChunkSize) {
				TraceEvent(SevWarnAlways, "ClientLibraryInvalidChunkSize")
				    .detail("Key", chunkKeyFromNo(chunkKeyPrefix, chunkNo, arena))
				    .detail("MaxSize", expectedChunkSize)
				    .detail("ActualSize", chunkVal.size());
				throw client_lib_invalid_binary();
			}

			memcpy(mutateString(buf) + bufferOffset, chunkVal.begin(), chunkVal.size());
			bufferOffset += chunkVal.size();
			chunkNo++;
		}

		// write the chunks to the file, update checksum
		if (bufferOffset > 0) {
			wait(fClientLib->write(buf.begin(), bufferOffset, fileOffset));
			fileOffset += bufferOffset;
			::MD5_Update(&sum, buf.begin(), bufferOffset);
		}

		// move to the next batch
		fromChunkNo = toChunkNo;
	}

	// check if the downloaded file size is as expected
	if (fileOffset != binarySize) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidSize")
		    .detail("ExpectedSize", binarySize)
		    .detail("ActualSize", fileOffset);
		throw client_lib_invalid_binary();
	}

	// check if the checksum of downloaded file is as expected
	Standalone<StringRef> sumBytesStr = md5SumToHexString(sum);
	if (sumBytesStr != StringRef(checkSum)) {
		TraceEvent(SevWarnAlways, "ClientLibraryChecksumMismatch")
		    .detail("Expected", checkSum)
		    .detail("Actual", sumBytesStr)
		    .detail("Key", clientLibMetaKey);
		throw client_lib_invalid_binary();
	}

	wait(fClientLib->sync());

	TraceEvent("ClientLibraryDownloadDone").detail("Key", clientLibMetaKey);
	return Void();
}

ACTOR Future<Void> deleteClientLibrary(Database db, Standalone<StringRef> clientLibId) {
	state Key clientLibMetaKey = metadataKeyFromId(clientLibId.toString());
	state Key chunkKeyPrefix = chunkKeyPrefixFromId(clientLibId.toString());

	TraceEvent("ClientLibraryBeginDelete").detail("Key", clientLibMetaKey);

	loop {
		state Transaction tr(db);
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			Optional<Value> metadataOpt = wait(tr.get(clientLibMetaKey));
			if (!metadataOpt.present()) {
				TraceEvent(SevWarnAlways, "ClientLibraryNotFound").detail("Key", clientLibMetaKey);
				throw client_lib_not_found();
			}
			tr.clear(prefixRange(chunkKeyPrefix));
			tr.clear(clientLibMetaKey);
			wait(tr.commit());
			break;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}

	TraceEvent("ClientLibraryDeleteDone").detail("Key", clientLibMetaKey);
	return Void();
}

namespace {

void applyClientLibFilter(const ClientLibFilter& filter,
                          const RangeResultRef& scanResults,
                          Standalone<VectorRef<StringRef>>& filteredResults) {
	for (const auto& [k, v] : scanResults) {
		try {
			json_spirit::mObject metadataJson = parseMetadataJson(v);
			if (filter.matchAvailableOnly && getStatusByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_STATUS)) !=
			                                     ClientLibStatus::AVAILABLE) {
				continue;
			}
			if (filter.matchCompatibleAPI &&
			    getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_API_VERSION) < filter.apiVersion) {
				continue;
			}
			if (filter.matchNewerPackageVersion && !filter.matchPlatform &&
			    getNumericVersionEncoding(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_VERSION)) <=
			        filter.numericPkgVersion) {
				continue;
			}
			filteredResults.push_back_deep(filteredResults.arena(), v);
		} catch (Error& e) {
			// Entries with invalid metadata on the cluster
			// Can happen only if the official management interface is bypassed
			ASSERT(e.code() == error_code_client_lib_invalid_metadata);
			TraceEvent(SevError, "ClientLibraryIgnoringInvalidMetadata").detail("Metadata", v);
		}
	}
}

} // namespace

ACTOR Future<Standalone<VectorRef<StringRef>>> listClientLibraries(Database db, ClientLibFilter filter) {
	state Standalone<VectorRef<StringRef>> result;
	state Transaction tr(db);
	state PromiseStream<Standalone<RangeResultRef>> scanResults;
	state Key fromKey;
	state Key toKey;
	state KeyRangeRef scanRange;
	state Future<Void> stream;

	loop {
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			if (filter.matchPlatform) {
				Key prefixWithPlatform =
				    clientLibMetadataPrefix.withSuffix(std::string(getPlatformName(filter.platformVal)));
				fromKey = prefixWithPlatform.withSuffix(LiteralStringRef("/"));
				if (filter.matchNewerPackageVersion) {
					fromKey = fromKey.withSuffix(format("%09d", filter.numericPkgVersion + 1));
				}
				toKey = prefixWithPlatform.withSuffix(LiteralStringRef("0"));
				scanRange = KeyRangeRef(fromKey, toKey);
			} else {
				scanRange = clientLibMetadataKeys;
			}
			scanResults = PromiseStream<Standalone<RangeResultRef>>();
			stream = tr.getRangeStream(scanResults, scanRange, GetRangeLimits());
			loop {
				Standalone<RangeResultRef> scanResultRange = waitNext(scanResults.getFuture());
				applyClientLibFilter(filter, scanResultRange, result);
			}
		} catch (Error& e) {
			if (e.code() == error_code_end_of_stream) {
				break;
			}
			wait(tr.onError(e));
		}
	}
	return result;
}

} // namespace ClientLibManagement