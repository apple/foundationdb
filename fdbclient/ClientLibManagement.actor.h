/*
 * ClientLibManagement.actor.h
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

#pragma once
#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_G_H)
#define FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_G_H
#include "fdbclient/ClientLibManagement.actor.g.h"
#elif !defined(FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_H)
#define FDBCLIENT_MULTI_VERSION_CLIENT_CONTROL_ACTOR_H

#include <string>
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/md5/md5.h"

#include "flow/actorcompiler.h" // has to be last include

namespace ClientLibManagement {

enum class ClientLibStatus {
	DISABLED = 0,
	UPLOADING, // 1
	DOWNLOAD, // 2
	ACTIVE, // 3
	COUNT // must be the last one
};

enum class ClientLibPlatform {
	UNKNOWN = 0,
	X86_64_LINUX,
	X86_64_WINDOWS,
	X86_64_MACOS,
	COUNT // must be the last one
};

// Currently we support only one,
// but we may want to change it in the future
enum class ClientLibChecksumAlg {
	MD5 = 0,
	COUNT // must be the last one
};

inline const std::string CLIENTLIB_ATTR_PLATFORM{ "platform" };
inline const std::string CLIENTLIB_ATTR_STATUS{ "status" };
inline const std::string CLIENTLIB_ATTR_CHECKSUM{ "checksum" };
inline const std::string CLIENTLIB_ATTR_VERSION{ "version" };
inline const std::string CLIENTLIB_ATTR_TYPE{ "type" };
inline const std::string CLIENTLIB_ATTR_API_VERSION{ "apiversion" };
inline const std::string CLIENTLIB_ATTR_PROTOCOL{ "protocol" };
inline const std::string CLIENTLIB_ATTR_GIT_HASH{ "githash" };
inline const std::string CLIENTLIB_ATTR_FILENAME{ "filename" };
inline const std::string CLIENTLIB_ATTR_SIZE{ "size" };
inline const std::string CLIENTLIB_ATTR_CHUNK_COUNT{ "chunkcount" };
inline const std::string CLIENTLIB_ATTR_CHUNK_SIZE{ "chunksize" };
inline const std::string CLIENTLIB_ATTR_CHECKSUM_ALG{ "checksumalg" };

struct ClientLibFilter {
	bool matchAvailableOnly = false;
	bool matchPlatform = false;
	bool matchCompatibleAPI = false;
	bool matchNewerPackageVersion = false;
	ClientLibPlatform platformVal = ClientLibPlatform::UNKNOWN;
	int apiVersion = 0;
	int numericPkgVersion = 0;

	ClientLibFilter& filterAvailable() {
		matchAvailableOnly = true;
		return *this;
	}

	ClientLibFilter& filterPlatform(ClientLibPlatform platformVal) {
		matchPlatform = true;
		this->platformVal = platformVal;
		return *this;
	}

	ClientLibFilter& filterCompatibleAPI(int apiVersion) {
		matchCompatibleAPI = true;
		this->apiVersion = apiVersion;
		return *this;
	}

	// expects a version string like "6.3.10"
	ClientLibFilter& filterNewerPackageVersion(const std::string& versionStr);
};

const std::string& getStatusName(ClientLibStatus status);
ClientLibStatus getStatusByName(std::string_view statusName);

const std::string& getPlatformName(ClientLibPlatform platform);
ClientLibPlatform getPlatformByName(std::string_view platformName);

const std::string& getChecksumAlgName(ClientLibChecksumAlg checksumAlg);
ClientLibChecksumAlg getChecksumAlgByName(std::string_view checksumAlgName);

// encodes MD5 result to a hexadecimal string to be provided in the checksum attribute
Standalone<StringRef> md5SumToHexString(MD5_CTX& sum);

// Upload a client library binary from a file and associated metadata JSON
// to the system keyspace of the database
ACTOR Future<Void> uploadClientLibrary(Database db,
                                       Standalone<StringRef> metadataString,
                                       Standalone<StringRef> libFilePath);

// Determine clientLibId from the relevant attributes of the metadata JSON
Standalone<StringRef> getClientLibIdFromMetadataJson(StringRef metadataString);

// Download a client library binary from the system keyspace of the database
// and save it at the given file path
ACTOR Future<Void> downloadClientLibrary(Database db,
                                         Standalone<StringRef> clientLibId,
                                         Standalone<StringRef> libFilePath);

// Delete the client library binary from to the system keyspace of the database
ACTOR Future<Void> deleteClientLibrary(Database db, Standalone<StringRef> clientLibId);

// List client libraries available on the cluster, with the specified filter
// Returns metadata JSON of each library
ACTOR Future<Standalone<VectorRef<StringRef>>> listClientLibraries(Database db, ClientLibFilter filter);

// Get the current status of an uploaded client library
ACTOR Future<ClientLibStatus> getClientLibraryStatus(Database db, Standalone<StringRef> clientLibId);

// Change client library metadata status
ACTOR Future<Void> changeClientLibraryStatus(Database db, Standalone<StringRef> clientLibId, ClientLibStatus newStatus);

} // namespace ClientLibManagement

#include "flow/unactorcompiler.h"
#endif