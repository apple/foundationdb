/*
 * BulkLoading.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "BackupContainerBlobStore.h"
#include "fdbclient/BulkLoading.h"
#include "fdbclient/SystemData.h"
#include "flow/UnitTest.h"

#include <boost/url/url.hpp>
#include <boost/url/parse.hpp>

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
		std::string result = boost::urls::parse_uri(matches[1].str() + matches[3].str()).value().path();
		// Remove leading slash if present - S3 object keys don't have leading slashes,
		// and BackupContainer's dataPath() expects paths without them
		if (!result.empty() && result[0] == '/') {
			result = result.substr(1);
		}
		return result;
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

// Constructs a URL with the path modified to include "data/" prefix.
// This is used for BulkDump/BulkLoad to write under the backup container's data directory.
// Input:  blobstore://creds@host/backup_container?bucket=... , "bulkdump_data"
// Output: blobstore://creds@host/data/backup_container/bulkdump_data?bucket=...
// If the URL carries a "prefix" parameter the data tree lives under that key prefix,
// consistent with BackupContainerBlobStore's layout:
// Input:  blobstore://creds@host/backup_container?bucket=...&prefix=p , "bulkdump_data"
// Output: blobstore://creds@host/p/data/backup_container/bulkdump_data?bucket=...&prefix=p
std::string getBackupDataPath(const std::string& url, const std::string& suffix) {
	std::smatch matches;
	if (!std::regex_match(url, matches, BLOBSTORE_URL_PATTERN)) {
		// For local paths, prepend "data/" and append suffix
		return joinPath(joinPath("data", url), suffix);
	}
	// Parse the URL through the same code path the backup container uses so the two agree
	// on the resource and on parameter semantics (last value of a repeated parameter wins,
	// '&amp;' unescaping, raw values).
	std::string resource;
	std::string parseError;
	IBlobStoreEndpoint::ParametersT backupParams;
	IBlobStoreEndpoint::fromString(url, {}, &resource, &parseError, &backupParams);
	std::string keyPrefix;
	auto it = backupParams.find("prefix");
	if (it != backupParams.end()) {
		keyPrefix = BackupContainerBlobStore::normalizePrefix(it->second);
	}

	// Assemble the object path with the same string concatenation semantics as
	// BackupContainerBlobStore::dataPath(): the container name is used byte-for-byte and a
	// trailing slash suppresses the extra separator.  joinPath() must not be used here as it
	// would collapse empty path segments in names like "/tenant" or "tenant//".
	std::string newPath;
	if (!keyPrefix.empty()) {
		newPath = keyPrefix + "/";
	}
	newPath += "data/";
	if (!resource.empty() && resource.back() == '/') {
		newPath += resource + suffix;
	} else {
		newPath += resource + "/" + suffix;
	}

	// Reassemble the URL around the new path, keeping the host and query byte-for-byte.
	std::string rest = matches[3].str(); // <host>[:port][/<resource>][?<query>]
	std::string hostPort = rest.substr(0, rest.find_first_of("/?"));
	std::string query;
	size_t queryStart = rest.find('?');
	if (queryStart != std::string::npos) {
		query = rest.substr(queryStart);
	}
	return matches[1].str() + matches[2].str() + hostPort + "/" + newPath + query;
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

TEST_CASE("/bulkload/getBackupDataPath/prefix") {
	// Without a prefix parameter the data tree stays at the bucket root.
	ASSERT(getBackupDataPath("blobstore://host:80/some/container?bucket=b&region=r", "bulkdump_data") ==
	       "blobstore://host:80/data/some/container/bulkdump_data?bucket=b&region=r");
	// With a prefix parameter the data tree lives under the prefix, matching
	// BackupContainerBlobStore's layout.
	ASSERT(getBackupDataPath("blobstore://host:80/some/container?bucket=b&region=r&prefix=p1/p2", "bulkdump_data") ==
	       "blobstore://host:80/p1/p2/data/some/container/bulkdump_data?bucket=b&region=r&prefix=p1/p2");
	// The prefix value is normalized the same way the backup container normalizes it.
	ASSERT(getBackupDataPath("blobstore://host:80/c?bucket=b&region=r&prefix=/p/", "d") ==
	       "blobstore://host:80/p/data/c/d?bucket=b&region=r&prefix=/p/");
	// Parameter semantics match the backup container's URL parsing: the last value of a
	// repeated parameter wins, and HTML-encoded '&amp;' separators are unescaped.
	ASSERT(getBackupDataPath("blobstore://host:80/c?bucket=b&region=r&prefix=p1&prefix=p2", "d") ==
	       "blobstore://host:80/p2/data/c/d?bucket=b&region=r&prefix=p1&prefix=p2");
	ASSERT(getBackupDataPath("blobstore://host:80/c?bucket=b&region=r&amp;prefix=p", "d") ==
	       "blobstore://host:80/p/data/c/d?bucket=b&region=r&amp;prefix=p");
	// Container names are used byte-for-byte, matching BackupContainerBlobStore::dataPath():
	// leading slashes and empty path segments survive (joinPath() would collapse them) and a
	// trailing slash suppresses the extra separator.
	ASSERT(getBackupDataPath("blobstore://host:80//tenant?bucket=b&region=r&prefix=p", "d") ==
	       "blobstore://host:80/p/data//tenant/d?bucket=b&region=r&prefix=p");
	ASSERT(getBackupDataPath("blobstore://host:80/tenant//?bucket=b&region=r", "d") ==
	       "blobstore://host:80/data/tenant//d?bucket=b&region=r");
	ASSERT(getBackupDataPath("blobstore://host:80/tenant//?bucket=b&region=r&prefix=p", "d") ==
	       "blobstore://host:80/p/data/tenant//d?bucket=b&region=r&prefix=p");
	// The credentials part of the URL is preserved byte-for-byte.
	ASSERT(getBackupDataPath("blobstore://AKID:secRet+/=:toKen+/=@host:80/c?bucket=b&region=r&prefix=p", "d") ==
	       "blobstore://AKID:secRet+/=:toKen+/=@host:80/p/data/c/d?bucket=b&region=r&prefix=p");
	return Void();
}
