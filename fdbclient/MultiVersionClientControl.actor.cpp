/*
 * MultiVersionClientControl.actor.cpp
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

#include "fdbclient/MultiVersionClientControl.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/Platform.h"

#include <algorithm>
#include <string>
#include <stdio.h>

#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

constexpr size_t CLIENTLIB_CHUNK_SIZE = 8192;
constexpr size_t CLIENTLIB_TRANSACTION_SIZE = CLIENTLIB_CHUNK_SIZE * 32;

struct ClientLibBinaryInfo {
	size_t totalBytes;
	size_t chunkCnt;
};

ACTOR Future<Void> uploadClientLibBinary(Database db,
                                         StringRef libFilePath,
                                         KeyRef chunkKeyPrefix,
                                         ClientLibBinaryInfo* binInfo) {

	state size_t fileOffset = 0;
	state size_t chunkNo = 0;

	state Reference<IAsyncFile> fClientLib = wait(IAsyncFileSystem::filesystem()->open(
	    libFilePath.toString(), IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO, 0));

	loop {
		state Arena arena;
		state StringRef buf = makeString(CLIENTLIB_TRANSACTION_SIZE, arena);
		state int bytesRead = wait(fClientLib->read(mutateString(buf), CLIENTLIB_TRANSACTION_SIZE, fileOffset));
		fileOffset += bytesRead;
		if (bytesRead <= 0) {
			break;
		}

		state Transaction tr(db);
		state size_t firstChunkNo = chunkNo;
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				size_t bufferOffset = 0;
				chunkNo = firstChunkNo;
				while (bufferOffset < bytesRead) {
					size_t chunkLen = std::min(CLIENTLIB_CHUNK_SIZE, bytesRead - bufferOffset);
					KeyRef chunkKey = chunkKeyPrefix.withSuffix(format("%06zu", chunkNo), arena);
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

		if (bytesRead < CLIENTLIB_TRANSACTION_SIZE) {
			break;
		}
	}
	binInfo->totalBytes = fileOffset;
	binInfo->chunkCnt = chunkNo;
	return Void();
}

static bool getClientLibIdFromMetadataJson(const json_spirit::mObject& metadataJson,
                                           std::string& clientLibId,
                                           std::string* errorStr = nullptr) {
	const char* clientLibIdAttrs[] = { "platform", "version", "type", "checksum" };
	std::ostringstream libIdBuilder;
	for (auto attrName : clientLibIdAttrs) {
		auto attrIter = metadataJson.find(attrName);
		if (attrIter == metadataJson.cend() || attrIter->second.type() != json_spirit::str_type) {
			if (errorStr != nullptr) {
				*errorStr = format("Missing identification attribute %s", attrName);
			}
			return false;
		}
		if (attrName != clientLibIdAttrs[0]) {
			libIdBuilder << "/";
		}
		libIdBuilder << attrIter->second.get_str();
	}
	clientLibId = libIdBuilder.str();
	return true;
}

ACTOR Future<Void> uploadClientLibrary(Database db, StringRef metadataString, StringRef libFilePath) {

	json_spirit::mValue parsedMetadata;
	if (!json_spirit::read_string(metadataString.toString(), parsedMetadata)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "InvalidJSON")
		    .detail("Configuration", metadataString);
		throw client_lib_invalid_metadata();
	}

	state json_spirit::mObject metadataJson = parsedMetadata.get_obj();

	json_spirit::mValue schema;
	if (!json_spirit::read_string(JSONSchemas::clientLibMetadataSchema.toString(), schema)) {
		ASSERT(false);
	}

	std::string errorStr;
	if (!schemaMatch(schema.get_obj(), metadataJson, errorStr)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "SchemaMismatch")
		    .detail("Configuration", metadataString)
		    .detail("Error", errorStr);
		throw client_lib_invalid_metadata();
	}

	std::string clientLibId;
	if (!getClientLibIdFromMetadataJson(metadataJson, clientLibId, &errorStr)) {
		TraceEvent(SevWarnAlways, "ClientLibraryInvalidMetadata")
		    .detail("Reason", "InvalidIdentification")
		    .detail("Configuration", metadataString)
		    .detail("Error", errorStr);
		throw client_lib_invalid_metadata();
	}

	state Key clientLibMetaKey = StringRef(clientLibId).withPrefix(clientLibMetadataPrefix);
	state Key clientLibBinPrefix =
	    StringRef(clientLibId).withPrefix(clientLibBinaryPrefix).withSuffix(LiteralStringRef("/"));

	state std::string targetStatus = metadataJson["status"].get_str();
	metadataJson["status"] = "uploading";
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
	state ClientLibBinaryInfo binInfo = {};
	wait(uploadClientLibBinary(db, libFilePath, clientLibBinPrefix, &binInfo));

	/*
	 * Update the metadata entry, with additional information about the binary
	 * and change its state from "uploading" to the given one
	 */
	metadataJson["size"] = static_cast<int64_t>(binInfo.totalBytes);
	metadataJson["chunkcount"] = static_cast<int64_t>(binInfo.totalBytes);
	metadataJson["filename"] = basename(libFilePath.toString());
	metadataJson["status"] = targetStatus;
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