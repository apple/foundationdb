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
#include "fdbclient/SystemData.h"

#include <boost/url/url.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/error_types.hpp>
#include <boost/url/string_view.hpp>

bool getConductBulkLoadFromDataMoveId(const UID& dataMoveId) {
	bool nowAssigned = false;
	bool emptyRange = false;
	DataMoveType dataMoveType = DataMoveType::LOGICAL;
	DataMovementReason dataMoveReason = DataMovementReason::INVALID;
	decodeDataMoveId(dataMoveId, nowAssigned, emptyRange, dataMoveType, dataMoveReason);
	bool conductBulkLoad =
	    dataMoveType == DataMoveType::LOGICAL_BULKLOAD || dataMoveType == DataMoveType::PHYSICAL_BULKLOAD;
	if (conductBulkLoad) {
		ASSERT(!emptyRange && dataMoveIdIsValidForBulkLoad(dataMoveId));
		ASSERT(nowAssigned);
	}
	if (!nowAssigned) {
		ASSERT(!conductBulkLoad);
	}
	return conductBulkLoad;
}

bool dataMoveIdIsValidForBulkLoad(const UID& dataMoveId) {
	return dataMoveId.isValid() && dataMoveId != anonymousShardId;
}

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

std::string convertBulkLoadJobPhaseToString(const BulkLoadJobPhase& phase) {
	if (phase == BulkLoadJobPhase::Invalid) {
		return "Invalid";
	} else if (phase == BulkLoadJobPhase::Submitted) {
		return "Submitted";
	} else if (phase == BulkLoadJobPhase::Complete) {
		return "Complete";
	} else if (phase == BulkLoadJobPhase::Error) {
		return "Error";
	} else if (phase == BulkLoadJobPhase::Cancelled) {
		return "Cancelled";
	} else {
		TraceEvent(SevError, "UnexpectedBulkLoadJobPhase").detail("Val", phase);
		return "";
	}
}

// TODO(BulkLoad): Support file:// urls, etc.
// For now, we only support blobstore:// urls.
// 'blobstore://' is the first match, credentials including '@' are optional and second regex match.
// The third match is the host + path, etc. of the url.
static const std::regex BLOBSTORE_URL_PATTERN(R"((blobstore://)([A-Z0-9]+:[A-Za-z0-9+/=]+:[A-Za-z0-9+/=]+@)?(.+)$)");

std::string getPath(const std::string& path) {
	std::smatch matches;
	if (!std::regex_match(path, matches, BLOBSTORE_URL_PATTERN)) {
		return path;
	}
	// We want boost::url to parse out the path but it cannot digest credentials. Strip them out
	// before passing to boost::url.
	try {
		return boost::urls::parse_uri(matches[1].str() + matches[3].str()).value().path();
	} catch (std::system_error& e) {
		TraceEvent(SevError, "BulkLoadGetPathError")
		    .detail("Path", path)
		    .detail("Error", e.what())
		    .detail("Matches", matches.str());
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
	} catch (std::system_error& e) {
		TraceEvent(SevError, "BulkLoadAppendToPathError")
		    .detail("Path", path)
		    .detail("Error", e.what())
		    .detail("Matches", matches.str());
		throw std::invalid_argument("Invalid url " + path + " " + e.what());
	}
}

std::string getBulkLoadJobRoot(const std::string& root, const UID& jobId) {
	return appendToPath(root, jobId.toString());
}

std::string convertBulkLoadTransportMethodToString(BulkLoadTransportMethod method) {
	if (method == BulkLoadTransportMethod::Invalid) {
		return "Invalid";
	} else if (method == BulkLoadTransportMethod::CP) {
		return "LocalFileCopy";
	} else if (method == BulkLoadTransportMethod::BLOBSTORE) {
		return "BlobStore";
	} else {
		TraceEvent(SevError, "UnexpectedBulkLoadTransportMethod").detail("Val", method);
		return "";
	}
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
	BulkLoadManifestSet manifests(1);
	manifests.addManifest(manifest);
	return BulkLoadTaskState(jobId, manifests, range);
}

BulkLoadJobState createBulkLoadJob(const UID& dumpJobIdToLoad,
                                   const KeyRange& range,
                                   const std::string& jobRoot,
                                   const BulkLoadTransportMethod& transportMethod) {
	return BulkLoadJobState(dumpJobIdToLoad, jobRoot, range, transportMethod);
}
