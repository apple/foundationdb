/*
 * BulkLoading.cpp
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

#include "fdbclient/BulkLoading.h"

std::string generateBulkLoadJobManifestFileName() {
	return "job-manifest.txt";
}

std::string generateRandomBulkLoadDataFileName() {
	return deterministicRandom()->randomUniqueID().toString() + "-data.sst";
}

std::string generateRandomBulkLoadBytesSampleFileName() {
	return deterministicRandom()->randomUniqueID().toString() + "-bytesample.sst";
}

std::string generateEmptyManifestFileName() {
	return "manifest-empty.sst";
}

std::string generateBulkLoadJobManifestFileContent(const std::map<Key, BulkLoadManifest>& manifests) {
	std::string root = "";
	std::string content;
	for (const auto& [beginKey, manifest] : manifests) {
		if (root.empty()) {
			root = manifest.getRootPath();
		} else {
			ASSERT(manifest.getRootPath() == root);
		}
		content = content + manifest.generateEntryInJobManifest() + "\n";
	}
	std::string head = "Manifest count: " + std::to_string(manifests.size()) + ", Root: " + root + "\n";
	return head + content;
}

// For submitting a task manually (for testing)
BulkLoadTaskState newBulkLoadTaskLocalSST(const UID& jobId,
                                          const KeyRange& range,
                                          const BulkLoadFileSet& fileSet,
                                          const BulkLoadByteSampleSetting& byteSampleSetting,
                                          Version snapshotVersion,
                                          const std::string& checksum,
                                          int64_t bytes) {
	BulkLoadManifest manifest(fileSet,
	                          range.begin,
	                          range.end,
	                          snapshotVersion,
	                          checksum,
	                          bytes,
	                          byteSampleSetting,
	                          BulkLoadType::SST,
	                          BulkLoadTransportMethod::CP);
	return BulkLoadTaskState(jobId, manifest);
}
