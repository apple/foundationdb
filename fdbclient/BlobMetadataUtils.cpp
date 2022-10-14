/*
 * BlobMetadataUtils.cpp
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

#include "fdbclient/BlobMetadataUtils.h"

#include "fmt/format.h"
#include "flow/IRandom.h"
#include "flow/flow.h"
#include "fdbclient/Knobs.h"

// TODO: switch this to use bg_url instead of hardcoding file://fdbblob, so it works as FDBPerfKmsConnector
// FIXME: make this (more) deterministic outside of simulation for FDBPerfKmsConnector
Standalone<BlobMetadataDetailsRef> createRandomTestBlobMetadata(BlobMetadataDomainId domainId,
                                                                BlobMetadataDomainName domainName) {
	Standalone<BlobMetadataDetailsRef> metadata;
	metadata.domainId = domainId;
	metadata.arena().dependsOn(domainName.arena());
	metadata.domainName = domainName;
	// 0 == no partition, 1 == suffix partitioned, 2 == storage location partitioned
	int type = deterministicRandom()->randomInt(0, 3);
	int partitionCount = (type == 0) ? 0 : deterministicRandom()->randomInt(2, 12);
	fmt::print("SimBlobMetadata ({})\n", domainId);
	TraceEvent ev(SevDebug, "SimBlobMetadata");
	ev.detail("DomainId", domainId).detail("TypeNum", type).detail("PartitionCount", partitionCount);
	if (type == 0) {
		// single storage location
		metadata.base = StringRef(metadata.arena(), "file://fdbblob/" + std::to_string(domainId) + "/");
		fmt::print("  {}\n", metadata.base.get().printable());
		ev.detail("Base", metadata.base);
	}
	if (type == 1) {
		// simulate hash prefixing in s3
		metadata.base = StringRef(metadata.arena(), "file://fdbblob/"_sr);
		ev.detail("Base", metadata.base);
		fmt::print("    {} ({})\n", metadata.base.get().printable(), partitionCount);
		for (int i = 0; i < partitionCount; i++) {
			metadata.partitions.push_back_deep(metadata.arena(),
			                                   deterministicRandom()->randomUniqueID().shortString() + "-" +
			                                       std::to_string(domainId) + "/");
			fmt::print("      {}\n", metadata.partitions.back().printable());
			ev.detail("P" + std::to_string(i), metadata.partitions.back());
		}
	}
	if (type == 2) {
		// simulate separate storage location per partition
		for (int i = 0; i < partitionCount; i++) {
			metadata.partitions.push_back_deep(
			    metadata.arena(), "file://fdbblob" + std::to_string(domainId) + "_" + std::to_string(i) + "/");
			fmt::print("      {}\n", metadata.partitions.back().printable());
			ev.detail("P" + std::to_string(i), metadata.partitions.back());
		}
	}

	// set random refresh + expire time
	if (deterministicRandom()->coinflip()) {
		metadata.refreshAt = now() + deterministicRandom()->random01() * CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
		metadata.expireAt =
		    metadata.refreshAt + deterministicRandom()->random01() * CLIENT_KNOBS->BLOB_METADATA_REFRESH_INTERVAL;
	} else {
		metadata.refreshAt = std::numeric_limits<double>::max();
		metadata.expireAt = metadata.refreshAt;
	}

	return metadata;
}