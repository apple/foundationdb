/*
 * BlobConnectionProvider.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include <string>

#include "flow/IRandom.h"
#include "fdbclient/BlobConnectionProvider.h"

struct SingleBlobConnectionProvider : BlobConnectionProvider {
public:
	std::pair<Reference<BackupContainerFileSystem>, std::string> createForWrite(std::string newFileName) {
		return std::pair(conn, newFileName);
	}

	Reference<BackupContainerFileSystem> getForRead(std::string filePath) { return conn; }

	SingleBlobConnectionProvider(std::string url, bool useBackupPath) {
		conn = BackupContainerFileSystem::openContainerFS(url, {}, {}, useBackupPath);
	}

	bool needsRefresh() const { return false; }

	bool isExpired() const { return false; }

	void update(Standalone<BlobMetadataDetailsRef> newBlobMetadata) { ASSERT(false); }

private:
	Reference<BackupContainerFileSystem> conn;
};

// Could always include number of partitions as validation in sanity check or something?
// Ex: partition_numPartitions/filename instead of partition/filename
struct StorageLocationBlobConnectionProvider : BlobConnectionProvider {
	std::pair<Reference<BackupContainerFileSystem>, std::string> createForWrite(std::string newFileName) {
		// choose a partition randomly, to distribute load
		int writePartition = deterministicRandom()->randomInt(0, connections.size());
		// include location id in the filename
		BlobMetadataLocationId locationId = metadata.locations[writePartition].locationId;
		return std::pair(connections[writePartition], std::to_string(locationId) + "/" + newFileName);
	}

	Reference<BackupContainerFileSystem> getForRead(std::string filePath) {
		CODE_PROBE(isExpired(), "storage location blob connection using expired blob metadata for read!");
		size_t slash = filePath.find("/");
		ASSERT(slash != std::string::npos);

		BlobMetadataLocationId locationId = stoll(filePath.substr(0, slash));
		ASSERT(locationId >= 0);

		auto conn = locationToConnectionIndex.find(locationId);
		ASSERT(conn != locationToConnectionIndex.end());

		int connectionIdx = conn->second;
		ASSERT(connectionIdx >= 0);
		ASSERT(connectionIdx < connections.size());
		return connections[connectionIdx];
	}

	void updateMetadata(const Standalone<BlobMetadataDetailsRef>& newMetadata, bool checkPrevious) {
		ASSERT(newMetadata.locations.size() >= 1);
		if (checkPrevious) {
			CODE_PROBE(true, "Updating blob metadata details with new credentials");
			// FIXME: validate only the credentials changed and the locations are the same. They don't necessarily need
			// to be provided in the same order though.
			ASSERT(newMetadata.locations.size() == metadata.locations.size());
			ASSERT(newMetadata.locations.size() == locationToConnectionIndex.size());
			for (int i = 0; i < newMetadata.locations.size(); i++) {
				ASSERT(locationToConnectionIndex.count(newMetadata.locations[i].locationId));
			}
			if (newMetadata.expireAt <= metadata.expireAt) {
				return;
			}
		}
		metadata = newMetadata;
		connections.clear();
		locationToConnectionIndex.clear();
		for (int i = 0; i < metadata.locations.size(); i++) {
			// these should be whole blob urls
			auto& it = metadata.locations[i];
			ASSERT(it.path.toString().find("://") != std::string::npos);
			connections.push_back(BackupContainerFileSystem::openContainerFS(it.path.toString(), {}, {}, false));
			locationToConnectionIndex[it.locationId] = i;
		}

		ASSERT(connections.size() == metadata.locations.size());
		ASSERT(connections.size() == locationToConnectionIndex.size());
	}

	StorageLocationBlobConnectionProvider(const Standalone<BlobMetadataDetailsRef> metadata) {
		updateMetadata(metadata, false);
	}

	bool needsRefresh() const { return now() >= metadata.refreshAt; }

	bool isExpired() const { return now() >= metadata.expireAt; }

	void update(Standalone<BlobMetadataDetailsRef> newBlobMetadata) { updateMetadata(newBlobMetadata, true); }

private:
	Standalone<BlobMetadataDetailsRef> metadata;
	std::vector<Reference<BackupContainerFileSystem>> connections;
	std::unordered_map<BlobMetadataLocationId, int> locationToConnectionIndex;
};

Reference<BlobConnectionProvider> BlobConnectionProvider::newBlobConnectionProvider(std::string blobUrl) {
	// still use backup mode path for backwards compatibility with 71.2
	return makeReference<SingleBlobConnectionProvider>(blobUrl, true);
}

Reference<BlobConnectionProvider> BlobConnectionProvider::newBlobConnectionProvider(
    Standalone<BlobMetadataDetailsRef> blobMetadata) {
	return makeReference<StorageLocationBlobConnectionProvider>(blobMetadata);
}