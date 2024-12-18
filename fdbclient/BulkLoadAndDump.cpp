/*
 * BulkLoadAndDump.cpp
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

#include "fdbclient/BulkLoadAndDump.h"
#include "flow/Platform.h"

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

std::string generateBulkLoadJobManifestFileName(const UID& jobId) {
	return jobId.toString() + "-job-manifest.txt";
}

std::pair<BulkLoadFileSet, BulkLoadFileSet> generateBulkLoadFileSetting(Version version,
                                                                        const std::string& relativeFolder,
                                                                        const std::string& rootLocal,
                                                                        const std::string& rootRemote) {
	// Generate file names based on data version
	const std::string manifestFileName = std::to_string(version) + "-manifest.txt";
	const std::string dataFileName = std::to_string(version) + "-data.sst";
	const std::string byteSampleFileName = std::to_string(version) + "-sample.sst";
	BulkLoadFileSet fileSetLocal(rootLocal, relativeFolder, manifestFileName, dataFileName, byteSampleFileName);
	BulkLoadFileSet fileSetRemote(rootRemote, relativeFolder, manifestFileName, dataFileName, byteSampleFileName);
	return std::make_pair(fileSetLocal, fileSetRemote);
}

std::string generateBulkLoadJobRoot(const std::string& root, const UID& jobId) {
	return joinPath(root, jobId.toString());
}

std::string generateBulkLoadJobManifestFileContent(const std::map<Key, BulkLoadManifest>& manifests) {
	std::string root = "";
	std::string manifestList;
	for (const auto& [beginKey, manifest] : manifests) {
		if (root.empty()) {
			root = manifest.fileSet.rootPath;
		} else {
			ASSERT(manifest.fileSet.rootPath == root);
		}
		manifestList =
		    manifestList +
		    BulkDumpJobManifestEntry(manifest.getBeginKey(),
		                             manifest.getEndKey(),
		                             joinPath(manifest.fileSet.relativePath, manifest.fileSet.manifestFileName),
		                             manifest.version,
		                             manifest.bytes)
		        .toString() +
		    "\n";
	}
	std::string head = BulkDumpJobManifestHeader(manifests.size(), root).toString() + "\n";
	return head + manifestList;
}

BulkLoadTaskState newBulkLoadTaskLocalSST(UID jobID,
                                          KeyRange range,
                                          std::string folder,
                                          std::string dataFile,
                                          std::string bytesSampleFile) {
	std::unordered_set<std::string> dataFiles;
	dataFiles.insert(dataFile);
	return BulkLoadTaskState(
	    range, BulkLoadFileType::SST, BulkLoadTransportMethod::CP, folder, dataFiles, bytesSampleFile, jobID);
}

BulkLoadJobState newBulkLoadJobLocalSST(const UID& jobId, const KeyRange& range, const std::string& remoteRoot) {
	return BulkLoadJobState(jobId, remoteRoot, range, BulkLoadTransportMethod::CP);
}

BulkDumpState newBulkDumpJobLocalSST(const KeyRange& range, const std::string& remoteRoot) {
	return BulkDumpState(range, BulkLoadFileType::SST, BulkLoadTransportMethod::CP, remoteRoot);
}
