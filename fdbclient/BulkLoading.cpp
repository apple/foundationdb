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

std::string generateBulkLoadJobManifestFileContent(const std::map<Key, BulkLoadManifest>& manifests) {
	std::string root = "";
	std::string content;
	for (const auto& [beginKey, manifest] : manifests) {
		if (root.empty()) {
			root = manifest.fileSet.rootPath;
		} else {
			ASSERT(manifest.fileSet.rootPath == root);
		}
		content = content + manifest.generateEntryInJobManifest() + "\n";
	}
	std::string head = "Manifest count: " + std::to_string(manifests.size()) + ", Root: " + root + "\n";
	return head + content;
}

BulkLoadTaskState newBulkLoadTaskLocalSST(KeyRange range,
                                          std::string folder,
                                          std::string dataFile,
                                          std::string bytesSampleFile) {
	std::unordered_set<std::string> dataFiles;
	dataFiles.insert(dataFile);
	return BulkLoadTaskState(range, BulkLoadType::SST, BulkLoadTransportMethod::CP, folder, dataFiles, bytesSampleFile);
}
