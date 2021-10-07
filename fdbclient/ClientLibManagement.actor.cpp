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
#include "fdbclient/md5/md5.h"
#include "fdbclient/libb64/encode.h"
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
	std::string sumBytes;
};

const std::string& getStatusName(ClientLibStatus status) {
	static const std::string statusNames[] = { "disabled", "available", "uploading", "deleting" };
	return statusNames[status];
}

ClientLibStatus getStatusByName(std::string_view statusName) {
	static std::map<std::string_view, ClientLibStatus> statusByName;
	// initialize the map on demand
	if (statusByName.empty()) {
		for (int i = 0; i < CLIENTLIB_STATUS_COUNT; i++) {
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
	return platformNames[platform];
}

ClientLibPlatform getPlatformByName(std::string_view platformName) {
	static std::map<std::string_view, ClientLibPlatform> platformByName;
	// initialize the map on demand
	if (platformByName.empty()) {
		for (int i = 0; i < CLIENTLIB_PLATFORM_COUNT; i++) {
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

namespace {

bool isValidTargetStatus(ClientLibStatus status) {
	return status == CLIENTLIB_AVAILABLE || status == CLIENTLIB_DISABLED;
}

void parseMetadataJson(StringRef metadataString, json_spirit::mObject& metadataJson) {
	json_spirit::mValue parsedMetadata;
	if (!json_spirit::read_string(metadataString.toString(), parsedMetadata) ||
	    parsedMetadata.type() != json_spirit::obj_type) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "InvalidJSON")
		    .detail("Configuration", metadataString);
		throw client_lib_invalid_metadata();
	}

	metadataJson = parsedMetadata.get_obj();
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
	int numScanned = sscanf(versionStr.c_str(), "%d.%d.%d", &major, &minor, &patch);
	if (numScanned != 3 || !validVersionPartNum(major) || !validVersionPartNum(minor) || !validVersionPartNum(patch)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Error", format("Invalid version string %s", versionStr.c_str()));
		throw client_lib_invalid_metadata();
	}
	return ((major * 1000) + minor) * 1000 + patch;
}

void getIdFromMetadataJson(const json_spirit::mObject& metadataJson, std::string& clientLibId) {
	std::ostringstream libIdBuilder;
	libIdBuilder << getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_PLATFORM) << "/";
	libIdBuilder << format("%09d", getNumericVersionEncoding(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_VERSION)))
	             << "/";
	libIdBuilder << getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_TYPE) << "/";
	libIdBuilder << getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM);
	clientLibId = libIdBuilder.str();
}

Key metadataKeyFromId(const std::string& clientLibId) {
	return StringRef(clientLibId).withPrefix(clientLibMetadataPrefix);
}

Key chunkKeyPrefixFromId(const std::string& clientLibId) {
	return StringRef(clientLibId).withPrefix(clientLibBinaryPrefix).withSuffix(LiteralStringRef("/"));
}

KeyRef chunkKeyFromNo(StringRef clientLibBinPrefix, size_t chunkNo, Arena& arena) {
	return clientLibBinPrefix.withSuffix(format("%06zu", chunkNo), arena);
}

ClientLibPlatform getCurrentClientPlatform() {
#ifdef __x86_64__
#if defined(_WIN32)
	return CLIENTLIB_X86_64_WINDOWS;
#elif defined(__linux__)
	return CLIENTLIB_X86_64_LINUX;
#elif defined(__FreeBSD__) || defined(__APPLE__)
	return CLIENTLIB_X86_64_MACOS;
#else
	return CLIENTLIB_UNKNOWN_PLATFORM;
#endif
#else // not __x86_64__
	return CLIENTLIB_UNKNOWN_PLATFORM;
#endif
}
} // namespace

ClientLibFilter& ClientLibFilter::filterNewerPackageVersion(const std::string& versionStr) {
	matchNewerPackageVersion = true;
	this->numericPkgVersion = getNumericVersionEncoding(versionStr);
	return *this;
}

void getClientLibIdFromMetadataJson(StringRef metadataString, std::string& clientLibId) {
	json_spirit::mObject parsedMetadata;
	parseMetadataJson(metadataString, parsedMetadata);
	getIdFromMetadataJson(parsedMetadata, clientLibId);
}

namespace {

ACTOR Future<Void> uploadClientLibBinary(Database db,
                                         StringRef libFilePath,
                                         KeyRef chunkKeyPrefix,
                                         ClientLibBinaryInfo* binInfo) {

	state size_t fileOffset = 0;
	state size_t chunkNo = 0;
	state int transactionSize = getAlignedUpperBound(CLIENT_KNOBS->MVC_CLIENTLIB_TRANSACTION_SIZE, _PAGE_SIZE);
	state int chunkSize = getAlignedUpperBound(CLIENT_KNOBS->MVC_CLIENTLIB_CHUNK_SIZE, 1024);
	if (transactionSize % chunkSize != 0) {
		TraceEvent(SevError, "ClientLibraryInvalidConfig")
		    .detail("Reason", format("Invalid chunk size configuration, falling back to %d", _PAGE_SIZE));
		chunkSize = _PAGE_SIZE;
	}

	state Reference<IAsyncFile> fClientLib = wait(IAsyncFileSystem::filesystem()->open(
	    libFilePath.toString(), IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0));

	state MD5_CTX sum;
	::MD5_Init(&sum);

	loop {
		state Arena arena;
		state StringRef buf = makeAlignedString(_PAGE_SIZE, transactionSize, arena);
		state int bytesRead = wait(fClientLib->read(mutateString(buf), transactionSize, fileOffset));
		fileOffset += bytesRead;
		if (bytesRead <= 0) {
			break;
		}

		::MD5_Update(&sum, buf.begin(), bytesRead);

		state Transaction tr(db);
		state size_t firstChunkNo = chunkNo;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
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

	std::string sumBytes;
	sumBytes.resize(16);
	::MD5_Final((unsigned char*)sumBytes.data(), &sum);
	binInfo->sumBytes = base64::encoder::from_string(sumBytes);
	return Void();
}

ACTOR Future<Void> deleteClientLibBinary(Database db, Key chunkKeyPrefix, size_t chunkCount) {
	state size_t chunkNo = 0;
	state size_t keysPerTransaction = CLIENT_KNOBS->MVC_CLIENTLIB_DELETE_KEYS_PER_TRANSACTION;
	loop {
		state Arena arena;
		state Transaction tr1(db);
		loop {
			try {
				tr1.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state size_t endChunk = std::min(chunkNo + keysPerTransaction, chunkCount);
				tr1.clear(KeyRangeRef(chunkKeyFromNo(chunkKeyPrefix, chunkNo, arena),
				                      chunkKeyFromNo(chunkKeyPrefix, endChunk, arena)));
				wait(tr1.commit());
				chunkNo = endChunk;
				break;
			} catch (Error& e) {
				wait(tr1.onError(e));
			}
		}
		if (chunkNo == chunkCount) {
			break;
		}
	}
	return Void();
}

ACTOR Future<Void> deleteClientLibMetadataEntry(Database db, Key clientLibMetaKey) {
	loop {
		state Transaction tr2(db);
		try {
			tr2.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr2.clear(clientLibMetaKey);
			wait(tr2.commit());
			break;
		} catch (Error& e) {
			wait(tr2.onError(e));
		}
	}
	return Void();
}

} // namespace

ACTOR Future<Void> uploadClientLibrary(Database db, StringRef metadataString, StringRef libFilePath) {
	state json_spirit::mObject metadataJson;
	parseMetadataJson(metadataString, metadataJson);

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

	state std::string clientLibId;
	getIdFromMetadataJson(metadataJson, clientLibId);
	state Key clientLibMetaKey = metadataKeyFromId(clientLibId);
	state Key clientLibBinPrefix = chunkKeyPrefixFromId(clientLibId);

	state ClientLibStatus targetStatus = getStatusByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_STATUS));
	if (!isValidTargetStatus(targetStatus)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "InvalidTargetStatus")
		    .detail("Configuration", metadataString);
		throw client_lib_invalid_metadata();
	}

	// Check if further mandatory attributes are set
	getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_GIT_HASH);
	getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_PROTOCOL);
	getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_API_VERSION);

	metadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(CLIENTLIB_UPLOADING);
	state std::string jsStr = json_spirit::write_string(json_spirit::mValue(metadataJson));

	/*
	 * Check if the client library with the same identifier already exists.
	 * If not, write its metadata with "uploading" state to prevent concurrent uploads
	 */
	state Transaction tr(db);
	loop {
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
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
			if (e.code() == error_code_client_lib_already_exists) {
				throw;
			}
			wait(tr.onError(e));
		}
	}

	/*
	 * Upload the binary of the client library in chunks
	 */
	state ClientLibBinaryInfo binInfo;
	wait(uploadClientLibBinary(db, libFilePath, clientLibBinPrefix, &binInfo));

	std::string checkSum = getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM);
	if (binInfo.sumBytes != checkSum) {
		TraceEvent(SevWarnAlways, "ClientLibraryChecksumMismatch")
		    .detail("Expected", checkSum)
		    .detail("Actual", binInfo.sumBytes)
		    .detail("Configuration", metadataString);
		// Rollback the upload operation
		try {
			wait(deleteClientLibBinary(db, clientLibBinPrefix, binInfo.chunkCnt));
			wait(deleteClientLibMetadataEntry(db, clientLibMetaKey));
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

ACTOR Future<Void> downloadClientLibrary(Database db, StringRef clientLibId, StringRef libFilePath) {
	state Key clientLibMetaKey = metadataKeyFromId(clientLibId.toString());
	state Key chunkKeyPrefix = chunkKeyPrefixFromId(clientLibId.toString());
	state json_spirit::mObject metadataJson;

	TraceEvent("ClientLibraryBeginDownload").detail("Key", clientLibMetaKey);

	/*
	 * First read the metadata to get information about the status and
	 * the chunk count of the client library
	 */
	loop {
		state Transaction tr(db);
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> metadataOpt = wait(tr.get(clientLibMetaKey));
			if (!metadataOpt.present()) {
				TraceEvent(SevWarnAlways, "ClientLibraryNotFound").detail("Key", clientLibMetaKey);
				throw client_lib_not_found();
			}
			parseMetadataJson(metadataOpt.get(), metadataJson);
			break;
		} catch (Error& e) {
			if (e.code() == error_code_client_lib_not_found) {
				throw;
			}
			wait(tr.onError(e));
		}
	}

	// Allow downloading only libraries in the available state
	if (getStatusByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_STATUS)) != CLIENTLIB_AVAILABLE) {
		throw client_lib_not_available();
	}

	int64_t flags = IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_READWRITE | IAsyncFile::OPEN_CREATE |
	                IAsyncFile::OPEN_UNCACHED;
	state Reference<IAsyncFile> fClientLib =
	    wait(IAsyncFileSystem::filesystem()->open(libFilePath.toString(), flags, 0666));

	state std::string checkSum = getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_CHECKSUM);
	state int transactionSize = getAlignedUpperBound(CLIENT_KNOBS->MVC_CLIENTLIB_TRANSACTION_SIZE, _PAGE_SIZE);
	state size_t chunkCount = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_CHUNK_COUNT);
	state size_t binarySize = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_SIZE);
	state size_t expectedChunkSize = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_CHUNK_SIZE);
	ASSERT(transactionSize % expectedChunkSize == 0);
	state size_t fileOffset = 0;
	state size_t chunkNo = 0;

	state MD5_CTX sum;
	::MD5_Init(&sum);

	loop {
		state Arena arena;
		state Transaction tr1(db);
		state StringRef buf = makeAlignedString(_PAGE_SIZE, transactionSize, arena);
		state size_t bufferOffset = 0;
		state size_t firstChunkNo = chunkNo;

		loop {
			try {
				bufferOffset = 0;
				chunkNo = firstChunkNo;
				tr1.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				loop {
					state KeyRef chunkKey = chunkKeyFromNo(chunkKeyPrefix, chunkNo, arena);
					state Optional<Value> chunkValOpt = wait(tr1.get(chunkKey));
					if (!chunkValOpt.present()) {
						TraceEvent(SevWarnAlways, "ClientLibraryChunkNotFound").detail("Key", chunkKey);
						throw client_lib_invalid_binary();
					}
					StringRef chunkVal = chunkValOpt.get();
					// All chunks exept for the last one must be of the expected size to guarantee
					// alignment when writing to file
					if ((chunkNo != chunkCount && chunkVal.size() != expectedChunkSize) ||
					    chunkVal.size() > expectedChunkSize) {
						TraceEvent(SevWarnAlways, "ClientLibraryInvalidChunkSize")
						    .detail("Key", chunkKey)
						    .detail("MaxSize", expectedChunkSize)
						    .detail("ActualSize", chunkVal.size());
						throw client_lib_invalid_binary();
					}
					memcpy(mutateString(buf) + bufferOffset, chunkVal.begin(), chunkVal.size());
					chunkNo++;
					bufferOffset += chunkVal.size();

					// finish transaction if last chunk read or transaction size limit reached
					if (bufferOffset + expectedChunkSize > transactionSize || chunkNo == chunkCount) {
						break;
					}
				}
				break;
			} catch (Error& e) {
				if (e.code() == error_code_client_lib_invalid_binary) {
					throw;
				}
				wait(tr1.onError(e));
			}
		}

		if (bufferOffset > 0) {
			wait(fClientLib->write(buf.begin(), bufferOffset, fileOffset));
			fileOffset += bufferOffset;
			::MD5_Update(&sum, buf.begin(), bufferOffset);
		}

		if (chunkNo == chunkCount) {
			break;
		}
	}

	if (fileOffset != binarySize) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidSize")
		    .detail("ExpectedSize", binarySize)
		    .detail("ActualSize", fileOffset);
		throw client_lib_invalid_binary();
	}

	std::string sumBytes;
	sumBytes.resize(16);
	::MD5_Final((unsigned char*)sumBytes.data(), &sum);
	std::string sumBase64 = base64::encoder::from_string(sumBytes);
	if (sumBase64 != checkSum) {
		TraceEvent(SevWarnAlways, "ClientLibraryChecksumMismatch")
		    .detail("Expected", checkSum)
		    .detail("Actual", sumBase64)
		    .detail("Key", clientLibMetaKey);
		throw client_lib_invalid_binary();
	}

	wait(fClientLib->sync());

	TraceEvent("ClientLibraryDownloadDone").detail("Key", clientLibMetaKey);
	return Void();
}

ACTOR Future<Void> deleteClientLibrary(Database db, StringRef clientLibId) {
	state Key clientLibMetaKey = metadataKeyFromId(clientLibId.toString());
	state Key chunkKeyPrefix = chunkKeyPrefixFromId(clientLibId.toString());
	state json_spirit::mObject metadataJson;

	TraceEvent("ClientLibraryBeginDelete").detail("Key", clientLibMetaKey);

	/*
	 * Get the client lib metadata to check if it exists. If so set its state to "deleting"
	 */
	loop {
		state Transaction tr(db);
		try {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			Optional<Value> metadataOpt = wait(tr.get(clientLibMetaKey));
			if (!metadataOpt.present()) {
				TraceEvent(SevWarnAlways, "ClientLibraryNotFound").detail("Key", clientLibMetaKey);
				throw client_lib_not_found();
			}
			parseMetadataJson(metadataOpt.get(), metadataJson);
			metadataJson[CLIENTLIB_ATTR_STATUS] = getStatusName(CLIENTLIB_DELETING);
			state std::string jsStr = json_spirit::write_string(json_spirit::mValue(metadataJson));
			tr.set(clientLibMetaKey, ValueRef(jsStr));
			wait(tr.commit());
			break;
		} catch (Error& e) {
			if (e.code() == error_code_client_lib_not_found) {
				throw;
			}
			wait(tr.onError(e));
		}
	}

	state size_t chunkCount = getMetadataIntAttr(metadataJson, CLIENTLIB_ATTR_CHUNK_COUNT);
	wait(deleteClientLibBinary(db, chunkKeyPrefix, chunkCount));
	wait(deleteClientLibMetadataEntry(db, clientLibMetaKey));

	TraceEvent("ClientLibraryDeleteDone").detail("Key", clientLibMetaKey);
	return Void();
}

namespace {

void applyClientLibFilter(const ClientLibFilter& filter,
                          const RangeResultRef& scanResults,
                          Standalone<VectorRef<StringRef>>& filteredResults) {
	for (const auto& [k, v] : scanResults) {
		try {
			json_spirit::mObject metadataJson;
			parseMetadataJson(v, metadataJson);
			if (filter.matchAvailableOnly &&
			    getStatusByName(getMetadataStrAttr(metadataJson, CLIENTLIB_ATTR_STATUS)) != CLIENTLIB_AVAILABLE) {
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
	loop {
		state Transaction tr(db);
		state PromiseStream<Standalone<RangeResultRef>> scanResults;
		try {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			state KeyRangeRef scanRange;
			if (filter.matchPlatform) {
				Key prefixWithPlatform =
				    clientLibMetadataPrefix.withSuffix(std::string(getPlatformName(filter.platformVal)));
				state Key fromKey = prefixWithPlatform.withSuffix(LiteralStringRef("/"));
				if (filter.matchNewerPackageVersion) {
					fromKey = fromKey.withSuffix(format("%09d", filter.numericPkgVersion + 1));
				}
				state Key toKey = prefixWithPlatform.withSuffix(LiteralStringRef("0"));
				scanRange = KeyRangeRef(StringRef(fromKey), toKey);
			} else {
				scanRange = clientLibMetadataKeys;
			}
			state Future<Void> stream = tr.getRangeStream(scanResults, scanRange, GetRangeLimits());
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

Future<Standalone<VectorRef<StringRef>>> listCompatibleClientLibraries(Database db, int apiVersion) {
	return listClientLibraries(
	    db,
	    ClientLibFilter().filterAvailable().filterCompatibleAPI(apiVersion).filterPlatform(getCurrentClientPlatform()));
}

} // namespace ClientLibManagement