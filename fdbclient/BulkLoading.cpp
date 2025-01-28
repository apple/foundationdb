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
#include "flow/Error.h"

#include <boost/url/url.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/error_types.hpp>
#include <boost/url/string_view.hpp>

std::string stringRemovePrefix(std::string str, const std::string& prefix) {
	if (str.compare(0, prefix.length(), prefix) == 0) {
		str.erase(0, prefix.length());
	} else {
		throw bulkload_manifest_decode_error();
	}
	return str;
}

Key getKeyFromHexString(const std::string& hexRawString) {
	if (hexRawString.empty()) {
		return Key();
	}
	// Here is an example of the input hexRawString:
	// "01 02 03". This raw string should be convered to the Key: "\x01\x02\x03".
	// Note that the space is not added for the last byte in the original string.
	ASSERT((hexRawString.size() + 1) % 3 == 0);
	std::string res;
	res.resize((hexRawString.size() + 1) / 3);
	for (size_t i = 0; i < hexRawString.size(); i += 3) {
		std::string byteString = hexRawString.substr(i, 2);
		uint8_t byte = static_cast<uint8_t>(std::stoul(byteString, nullptr, 16));
		res[i / 3] = byte;
		ASSERT(i + 2 >= hexRawString.size() || hexRawString[i + 2] == ' ');
	}
	return Standalone(StringRef(res));
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
	std::string res = BulkLoadJobManifestFileHeader(bulkLoadManifestFormatVersion, manifests.size()).toString() + "\n";
	for (const auto& [beginKey, manifest] : manifests) {
		res = res + BulkLoadJobFileManifestEntry(manifest).toString() + "\n";
	}
	return res;
}

// For now, we only support blobstore:// urls. Later, lets add support for file:// urls, etc.
// 'blobstore://' is the first match, credentials including '@' are optional and second regex match.
// The third match is the host + path, etc. of the url.
static const std::regex BLOBSTORE_URL_PATTERN(R"((blobstore://)([A-Za-z0-9_-]+:[A-Za-z0-9_-]+:[A-Za-z0-9_-]+@)?(.+)$)");

std::string getPath(const std::string& path) {
	std::smatch matches;
	if (!std::regex_match(path, matches, BLOBSTORE_URL_PATTERN)) {
		return path;
	}
	// We want boost::url to parse out the path but it cannot digest credentials. Strip them out
	// before passing to boost::url.
	try {
		boost::urls::url url = boost::urls::parse_uri(matches[1].str() + matches[3].str()).value();
		return url.path();
	} catch (const boost::urls::system_error& e) {
		throw std::invalid_argument("Invalid url " + path + " " + e.what());
	}
}

// TODO(BulkLoad): use this everywhere
std::string appendToPath(const std::string& path, const std::string& append) {
	std::smatch matches;
	if (!std::regex_match(path, matches, BLOBSTORE_URL_PATTERN)) {
		return joinPath(path, append);
	}
	// We want boost::url to parse out the path but it cannot digest credentials. Strip them out
	// before passing to boost::url.
	try {
		boost::urls::url url = boost::urls::parse_uri(matches[1].str() + matches[3].str()).value();
		auto newUrl = std::string(url.set_path(joinPath(url.path(), append)).buffer());
		return matches[1].str() + matches[2].str() + newUrl.substr(matches[1].str().length());
	} catch (const boost::urls::system_error& e) {
		throw std::invalid_argument("Invalid url " + path + " " + e.what());
	}
}

std::string getBulkLoadJobRoot(const std::string& root, const UID& jobId) {
	return appendToPath(root, jobId.toString());
}

// For submitting a task manually (for testing)
BulkLoadTaskState createBulkLoadTask(const UID& jobId,
                                     const KeyRange& range,
                                     const BulkLoadFileSet& fileSet,
                                     const BulkLoadByteSampleSetting& byteSampleSetting,
                                     const Version& snapshotVersion,
                                     const int64_t& bytes,
                                     const int64_t& keyCount,
                                     const BulkLoadType& type,
                                     const BulkLoadTransportMethod& transportMethod) {
	BulkLoadManifest manifest(
	    fileSet, range.begin, range.end, snapshotVersion, bytes, keyCount, byteSampleSetting, type, transportMethod);
	return BulkLoadTaskState(jobId, manifest);
}

BulkLoadJobState createBulkLoadJob(const UID& dumpJobIdToLoad,
                                   const KeyRange& range,
                                   const std::string& jobRoot,
                                   const BulkLoadTransportMethod& transportMethod) {
	return BulkLoadJobState(dumpJobIdToLoad, jobRoot, range, transportMethod);
}