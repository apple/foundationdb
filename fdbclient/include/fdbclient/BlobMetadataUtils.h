/*
 * BlobMetadataUtils.h
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

#ifndef BLOB_METADATA_UTILS_H
#define BLOB_METADATA_UTILS_H

#include "flow/Arena.h"
#include "flow/FileIdentifier.h"

using BlobMetadataDomainId = int64_t;

/*
 * There are 3 cases for blob metadata.
 *  1. A non-partitioned blob store. baseUrl is set, and partitions is empty. Files will be written with this prefix.
 *  2. A sub-path partitioned blob store. baseUrl is set, and partitions contains 2 or more sub-paths. Files will be
 * written with a prefix of the base url and then one of the sub-paths.
 *  3. A separate-storage-location partitioned blob store. baseUrl is NOT set, and partitions contains 2 or more full
 * fdb blob urls. Files will be written with one of the partition prefixes.
 * Partitioning is desired in blob stores such as s3 that can run into metadata hotspotting issues.
 */
struct BlobMetadataDetailsRef {
	constexpr static FileIdentifier file_identifier = 6685526;
	BlobMetadataDomainId domainId;
	Optional<StringRef> base;
	VectorRef<StringRef> partitions;

	// cache options
	double refreshAt;
	double expireAt;

	BlobMetadataDetailsRef() {}
	BlobMetadataDetailsRef(Arena& arena, const BlobMetadataDetailsRef& from)
	  : domainId(from.domainId), partitions(arena, from.partitions), refreshAt(from.refreshAt),
	    expireAt(from.expireAt) {
		if (from.base.present()) {
			base = StringRef(arena, from.base.get());
		}
	}

	explicit BlobMetadataDetailsRef(Arena& ar,
	                                BlobMetadataDomainId domainId,
	                                Optional<StringRef> base,
	                                VectorRef<StringRef> partitions,
	                                double refreshAt,
	                                double expireAt)
	  : domainId(domainId), partitions(ar, partitions), refreshAt(refreshAt), expireAt(expireAt) {
		if (base.present()) {
			base = StringRef(ar, base.get());
		}
	}

	explicit BlobMetadataDetailsRef(BlobMetadataDomainId domainId,
	                                Optional<StringRef> base,
	                                VectorRef<StringRef> partitions,
	                                double refreshAt,
	                                double expireAt)
	  : domainId(domainId), base(base), partitions(partitions), refreshAt(refreshAt), expireAt(expireAt) {}

	int expectedSize() const {
		return sizeof(BlobMetadataDetailsRef) + (base.present() ? base.get().size() : 0) + partitions.expectedSize();
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, domainId, base, partitions, refreshAt, expireAt);
	}
};

Standalone<BlobMetadataDetailsRef> createRandomTestBlobMetadata(const std::string& baseUrl,
                                                                BlobMetadataDomainId domainId);

#endif