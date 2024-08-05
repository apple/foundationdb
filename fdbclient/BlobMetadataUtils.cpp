/*
 * BlobMetadataUtils.cpp
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

#include "fdbclient/BlobMetadataUtils.h"

#include "fmt/format.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "fdbclient/Knobs.h"
#include "flow/IConnection.h"
#include "fdbclient/S3BlobStore.h"

std::string buildPartitionPath(const std::string& url, const std::string& partition) {
	ASSERT(!partition.empty());
	ASSERT(partition.front() != '/');
	ASSERT(partition.back() == '/');
	StringRef u(url);
	if (u.startsWith("file://"_sr)) {
		ASSERT(u.endsWith("/"_sr));
		return url + partition;
	} else if (u.startsWith("blobstore://"_sr)) {
		std::string resource;
		std::string lastOpenError;
		S3BlobStoreEndpoint::ParametersT backupParams;

		std::string urlCopy = url;

		Reference<S3BlobStoreEndpoint> bstore =
		    S3BlobStoreEndpoint::fromString(url, {}, &resource, &lastOpenError, &backupParams);

		ASSERT(!resource.empty());
		ASSERT(resource.back() != '/');
		size_t resourceStart = url.find(resource);
		ASSERT(resourceStart != std::string::npos);

		return urlCopy.insert(resourceStart + resource.size(), "/" + partition);
	} else {
		// FIXME: support azure
		throw backup_invalid_url();
	}
}

Standalone<BlobMetadataDetailsRef> createRandomTestBlobMetadata(const std::string& baseUrl,
                                                                BlobMetadataDomainId domainId) {
	Standalone<BlobMetadataDetailsRef> metadata;
	metadata.domainId = domainId;
	// 0 == no partition, 1 == suffix partitioned, 2 == storage location partitioned
	int type;
	if (CLIENT_KNOBS->DETERMINISTIC_BLOB_METADATA) {
		type = domainId % 3;
	} else {
		type = deterministicRandom()->randomInt(0, 3);
	}
	int partitionCount;
	if (type == 0) {
		partitionCount = 0;
	} else if (CLIENT_KNOBS->DETERMINISTIC_BLOB_METADATA) {
		partitionCount = 2 + domainId % 5;
	} else {
		partitionCount = deterministicRandom()->randomInt(2, 12);
	}
	// guarantee unique location for each domain for now
	BlobMetadataLocationId locIdBase = domainId * 100;
	TraceEvent ev(SevDebug, "SimBlobMetadata");
	ev.detail("DomainId", domainId).detail("TypeNum", type).detail("PartitionCount", partitionCount);
	if (type == 0) {
		// single storage location
		std::string partition = std::to_string(domainId) + "/";
		metadata.locations.emplace_back_deep(metadata.arena(), locIdBase, buildPartitionPath(baseUrl, partition));
		ev.detail("Location", metadata.locations.back().path);
	}
	if (type == 1) {
		// simulate hash prefixing in s3
		for (int i = 0; i < partitionCount; i++) {
			std::string partitionName;
			if (CLIENT_KNOBS->DETERMINISTIC_BLOB_METADATA) {
				partitionName = std::to_string(i);
			} else {
				partitionName = deterministicRandom()->randomUniqueID().shortString();
			}
			std::string partition = partitionName + "-" + std::to_string(domainId) + "/";
			metadata.locations.emplace_back_deep(
			    metadata.arena(), locIdBase + i, buildPartitionPath(baseUrl, partition));
			ev.detail("P" + std::to_string(i), metadata.locations.back().path);
		}
	}
	if (type == 2) {
		// simulate separate storage location per partition
		for (int i = 0; i < partitionCount; i++) {
			std::string partition = std::to_string(domainId) + "_" + std::to_string(i) + "/";
			metadata.locations.emplace_back_deep(
			    metadata.arena(), locIdBase + i, buildPartitionPath(baseUrl, partition));
			ev.detail("P" + std::to_string(i), metadata.locations.back().path);
		}
	}

	// set random refresh + expire time
	bool doExpire = CLIENT_KNOBS->DETERMINISTIC_BLOB_METADATA ? domainId % 2 : deterministicRandom()->coinflip();
	if (doExpire) {
		if (CLIENT_KNOBS->DETERMINISTIC_BLOB_METADATA) {
			metadata.refreshAt = now() + CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
			metadata.expireAt = metadata.refreshAt + 0.2 * CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
		} else {
			metadata.refreshAt =
			    now() + deterministicRandom()->random01() * CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
			metadata.expireAt =
			    metadata.refreshAt + deterministicRandom()->random01() * CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
		}
	} else {
		metadata.refreshAt = std::numeric_limits<double>::max();
		metadata.expireAt = metadata.refreshAt;
	}

	return metadata;
}