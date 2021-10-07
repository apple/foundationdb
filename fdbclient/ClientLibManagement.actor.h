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

#include "flow/actorcompiler.h" // has to be last include

namespace ClientLibManagement {

enum ClientLibStatus {
	CLIENTLIB_DISABLED = 0,
	CLIENTLIB_AVAILABLE, // 1
	CLIENTLIB_UPLOADING, // 2
	CLIENTLIB_DELETING, // 3
	CLIENTLIB_STATUS_COUNT // must be the last one
};

enum ClientLibPlatform {
	CLIENTLIB_UNKNOWN_PLATFORM = 0,
	CLIENTLIB_X86_64_LINUX,
	CLIENTLIB_X86_64_WINDOWS,
	CLIENTLIB_X86_64_MACOS,
	CLIENTLIB_PLATFORM_COUNT // must be the last one
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

struct ClientLibFilter {
	bool matchAvailableOnly;
	bool matchPlatform;
	bool matchCompatibleAPI;
	bool matchNewerPackageVersion;
	ClientLibPlatform platformVal;
	int apiVersion;
	int numericPkgVersion;

	ClientLibFilter()
	  : matchAvailableOnly(false), matchPlatform(false), matchCompatibleAPI(false), matchNewerPackageVersion(false),
	    platformVal(CLIENTLIB_UNKNOWN_PLATFORM), apiVersion(0) {}

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

// Upload a client library binary from a file and associated metadata JSON
// to the system keyspace of the database
ACTOR Future<Void> uploadClientLibrary(Database db, StringRef metadataString, StringRef libFilePath);

// Determine clientLibId from the relevant attributes of the metadata JSON
void getClientLibIdFromMetadataJson(StringRef metadataString, std::string& clientLibId);

// Download a client library binary from the system keyspace of the database
// and save it at the given file path
ACTOR Future<Void> downloadClientLibrary(Database db, StringRef clientLibId, StringRef libFilePath);

// Delete the client library binary from to the system keyspace of the database
ACTOR Future<Void> deleteClientLibrary(Database db, StringRef clientLibId);

// List client libraries available on the cluster, with the specified filter
// Returns metadata JSON of each library
ACTOR Future<Standalone<VectorRef<StringRef>>> listClientLibraries(Database db, ClientLibFilter filter);

// List available client libraries that are compatible with the current client
Future<Standalone<VectorRef<StringRef>>> listAvailableCompatibleClientLibraries(Database db, int apiVersion);

} // namespace ClientLibManagement

#include "flow/unactorcompiler.h"
#endif