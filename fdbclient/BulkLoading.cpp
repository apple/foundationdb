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

const int bulkLoadJobManifestFileFormatVersion = 1;

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

// Generate the bulkload job manifest file. Here is an example:
// Row 0: [FormatVersion]: 1, [ManifestCount]: 3, [RootPath]: "/tmp";
// Row 1: "", "01", 100, 9000, "range1", "manifest1.txt"
// Row 2: "01", "02 ff", 200, 0, "range2", "manifest2.txt"
// Row 3: "02 ff", "ff", 300, 8100, "range3", "manifest3.txt"
// In this example, the job manifest file is in the format of version 1.
// The file contains three ranges: "" ~ "\x01", "\x01" ~ "\x02\xff", and "\x02\xff" ~ "\xff".
// For the first range, the data version is at 100, the data size is 9KB, the manifest file path is
// "/tmp/range1/manifest1.txt". For the second range, the data version is at 200, the data size is 0 indicating this is
// an empty range. The manifest file path is "/tmp/range2/manifest2.txt". For the third range, the data version is at
// 300, the data size is 8.1KB, the manifest file path is "/tmp/range1/manifest3.txt".
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
	std::string head = "[FormatVersion]: " + std::to_string(bulkLoadJobManifestFileFormatVersion) +
	                   ", [ManifestCount]: " + std::to_string(manifests.size()) + ", [RootPath]: " + root + "\n";
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
