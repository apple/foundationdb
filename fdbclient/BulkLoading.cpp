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

std::string stringRemovePrefix(std::string str, const std::string& prefix) {
	if (str.compare(0, prefix.length(), prefix) == 0) {
		str.erase(0, prefix.length());
	} else {
		return "";
	}
	return str;
}

// A revert function of StringRef.toFullHexStringPlain()
Key getKeyFromHexString(const std::string& rawString) {
	if (rawString.empty()) {
		return Key();
	}
	std::vector<uint8_t> byteList;
	ASSERT((rawString.size() + 1) % 3 == 0);
	for (size_t i = 0; i < rawString.size(); i += 3) {
		std::string byteString = rawString.substr(i, 2);
		uint8_t byte = static_cast<uint8_t>(std::stoul(byteString, nullptr, 16));
		byteList.push_back(byte);
		ASSERT(i + 2 >= rawString.size() || rawString[i + 2] == ' ');
	}
	return Standalone(StringRef(byteList.data(), byteList.size()));
}

std::string getBulkLoadJobManifestFileName() {
	return "job-manifest.txt";
}

std::string generateBulkLoadBytesSampleFileNameFromDataFileName(const std::string& dataFileName) {
	return dataFileName + "-sample.sst";
}

std::string generateEmptyManifestFileName() {
	return "manifest-empty.sst";
}

// Generate the bulkload job manifest file. Here is an example.
// Assuming the job manifest file is in the folder: "/tmp".
// Row 0: [FormatVersion]: 1, [ManifestCount]: 3;
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
	std::string content;
	for (const auto& [beginKey, manifest] : manifests) {
		content = content + BulkLoadJobManifestFileManifestEntry(manifest).toString() + "\n";
	}
	std::string head =
	    BulkLoadJobManifestFileHeader(bulkLoadJobManifestFileFormatVersion, manifests.size()).toString() + "\n";
	return head + content;
}

// For submitting a task manually (for testing)
BulkLoadTaskState createNewBulkLoadTask(const UID& jobId,
                                        const KeyRange& range,
                                        const BulkLoadFileSet& fileSet,
                                        const BulkLoadByteSampleSetting& byteSampleSetting,
                                        const Version& snapshotVersion,
                                        const std::string& checksum,
                                        const int64_t& bytes,
                                        const int64_t& keyCount,
                                        const BulkLoadType& type,
                                        const BulkLoadTransportMethod& transportMethod) {
	BulkLoadManifest manifest(fileSet,
	                          range.begin,
	                          range.end,
	                          snapshotVersion,
	                          checksum,
	                          bytes,
	                          keyCount,
	                          byteSampleSetting,
	                          type,
	                          transportMethod);
	return BulkLoadTaskState(jobId, manifest);
}

BulkLoadJobState createNewBulkLoadJob(const UID& dumpJobIdToLoad,
                                      const KeyRange& range,
                                      const std::string& remoteRoot,
                                      const BulkLoadTransportMethod& transportMethod) {
	return BulkLoadJobState(dumpJobIdToLoad, remoteRoot, range, transportMethod);
}
