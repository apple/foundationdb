/*
 * BlobMetadataUtils.h
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

#ifndef BLOB_METADATA_UTILS_H
#define BLOB_METADATA_UTILS_H

#include "flow/Arena.h"
#include "flow/FileIdentifier.h"

using BlobMetadataDomainId = int64_t;
using BlobMetadataLocationId = int64_t;

/*
 * There are 2 cases for blob metadata. These are all represented fundamentally in the same input schema.
 *  1. A non-partitioned blob store. Files will be written with the specified location.
 *  2. A partitioned blob store. Files will be written with one of the partition's locationId prefixes.
 * Partitioning is desired in blob stores such as s3 that can run into metadata hotspotting issues.
 */

// FIXME: do internal deduping of locations based on location id
struct BlobMetadataLocationRef {
	BlobMetadataLocationId locationId;
	StringRef path;

	BlobMetadataLocationRef() {}
	BlobMetadataLocationRef(Arena& arena, const BlobMetadataLocationRef& from)
	  : locationId(from.locationId), path(arena, from.path) {}

	explicit BlobMetadataLocationRef(Arena& ar, BlobMetadataLocationId locationId, StringRef path)
	  : locationId(locationId), path(ar, path) {}
	explicit BlobMetadataLocationRef(BlobMetadataLocationId locationId, StringRef path)
	  : locationId(locationId), path(path) {}

	int expectedSize() const { return sizeof(BlobMetadataLocationRef) + path.size(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, locationId, path);
	}
};

struct BlobMetadataDetailsRef {
	constexpr static FileIdentifier file_identifier = 6685526;
	BlobMetadataDomainId domainId;
	VectorRef<BlobMetadataLocationRef> locations;

	// cache options
	double refreshAt;
	double expireAt;

	BlobMetadataDetailsRef() {}
	BlobMetadataDetailsRef(Arena& arena, const BlobMetadataDetailsRef& from)
	  : domainId(from.domainId), locations(arena, from.locations), refreshAt(from.refreshAt), expireAt(from.expireAt) {}

	explicit BlobMetadataDetailsRef(Arena& ar,
	                                BlobMetadataDomainId domainId,
	                                VectorRef<BlobMetadataLocationRef> locations,
	                                double refreshAt,
	                                double expireAt)
	  : domainId(domainId), locations(ar, locations), refreshAt(refreshAt), expireAt(expireAt) {
		ASSERT(!locations.empty());
	}

	explicit BlobMetadataDetailsRef(BlobMetadataDomainId domainId,
	                                VectorRef<BlobMetadataLocationRef> locations,
	                                double refreshAt,
	                                double expireAt)
	  : domainId(domainId), locations(locations), refreshAt(refreshAt), expireAt(expireAt) {
		ASSERT(!locations.empty());
	}

	int expectedSize() const { return sizeof(BlobMetadataDetailsRef) + locations.expectedSize(); }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, locations, refreshAt, expireAt);
	}
};

Standalone<BlobMetadataDetailsRef> createRandomTestBlobMetadata(const std::string& baseUrl,
                                                                BlobMetadataDomainId domainId);

#endif