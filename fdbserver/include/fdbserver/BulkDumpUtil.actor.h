/*
 * BulkDumpUtil.actor.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <cstdint>
#include <string>
#if defined(NO_INTELLISENSE) && !defined(FDBSERVER_BULKDUMPUTIL_ACTOR_G_H)
#define FDBSERVER_BULKDUMPUTIL_ACTOR_G_H
#include "fdbserver/BulkDumpUtil.actor.g.h"
#elif !defined(FDBSERVER_BULKDUMPUTIL_ACTOR_H)
#define FDBSERVER_BULKDUMPUTIL_ACTOR_H
#pragma once

#include "fdbclient/BulkDumping.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/actorcompiler.h" // has to be last include

struct SSBulkDumpTask {
	SSBulkDumpTask(const StorageServerInterface& targetServer,
	               const std::vector<UID>& checksumServers,
	               const BulkDumpState& bulkDumpState)
	  : targetServer(targetServer), checksumServers(checksumServers), bulkDumpState(bulkDumpState){};

	std::string toString() const {
		return "[BulkDumpState]: " + bulkDumpState.toString() + ", [TargetServer]: " + targetServer.toString() +
		       ", [ChecksumServers]: " + describe(checksumServers);
	}

	StorageServerInterface targetServer;
	std::vector<UID> checksumServers;
	BulkDumpState bulkDumpState;
};

struct BulkDumpManifest {
	std::string dataFilePath;
	std::string manifestFilePath;
	std::string bytesSampleFilePath;
	Key beginKey;
	Key endKey;
	Version version;
	std::string checksum;
	int64_t bytes;

	BulkDumpManifest(const std::string& dataFilePath,
	                 const std::string& manifestFilePath,
	                 const std::string& bytesSampleFilePath,
	                 const Key& beginKey,
	                 const Key& endKey,
	                 const Version& version,
	                 const std::string& checksum,
	                 int64_t bytes)
	  : dataFilePath(dataFilePath), manifestFilePath(manifestFilePath), bytesSampleFilePath(bytesSampleFilePath),
	    beginKey(beginKey), endKey(endKey), version(version), checksum(checksum), bytes(bytes) {}

	std::string toString() const {
		return "[DataFilePath]: " + dataFilePath + ", [ManifestFilePath]: " + manifestFilePath +
		       ", [BytesSampleFilePath]: " + bytesSampleFilePath + ", [BeginKey]: " + beginKey.toHexString() +
		       ", [EndKey]: " + endKey.toHexString() + ", [Version]: " + std::to_string(version) +
		       ", [Checksum]: " + checksum + ", [Bytes]: " + std::to_string(bytes);
	}
};

SSBulkDumpTask getSSBulkDumpTask(const std::map<std::string, std::vector<StorageServerInterface>>& locations,
                                 const BulkDumpState& bulkDumpState);

std::string generateRandomBulkDumpDataFileName(Version version);

BulkDumpManifest dumpDataFileToLocalDirectory(const std::map<Key, Value>& sortedKVS,
                                              const std::string& rootFolder,
                                              const std::string& relativeFolder,
                                              Version dumpVersion,
                                              const KeyRange& dumpRange,
                                              int64_t dumpBytes);

ACTOR Future<Void> bulkDumpTransportCP_impl(std::string fromFolder,
                                            std::string toFolder,
                                            size_t fileBytesMax,
                                            UID logId);

ACTOR Future<Void> uploadFiles(BulkDumpTransportMethod transportMethod,
                               std::string fromPath,
                               std::string toPath,
                               std::string relativeFolder,
                               UID logId);

ACTOR Future<Void> persistCompleteBulkDumpRange(Database cx, BulkDumpState bulkDumpState);

#include "flow/unactorcompiler.h"
#endif
