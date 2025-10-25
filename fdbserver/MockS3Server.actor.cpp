/*
 * MockS3Server.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2025 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/MockS3Server.h"

#include "fdbrpc/HTTP.h"
#include "fdbrpc/simulator.h"
#include "flow/Trace.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"

#include <string>
#include <map>
#include <sstream>
#include <regex>
#include <utility>
#include <iostream>
#include <algorithm>

#include "flow/IAsyncFile.h" // For IAsyncFileSystem
#include "flow/Platform.h" // For platform::createDirectory

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/prettywriter.h"

#include "flow/actorcompiler.h" // This must be the last #include.

/*
 * ACTOR STATE VARIABLE INITIALIZATION
 *
 * The FoundationDB actor compiler requires that ACTORs with early returns (before any wait())
 * must declare at least one state variable BEFORE the early return to properly initialize
 * the actor's Promise object.
 *
 * PATTERN:
 *   ACTOR Future<Void> someActor(...) {
 *       state SomeType variable; // Declare state BEFORE any early returns
 *       if (earlyExitCondition)
 *           return Void();       // Now safe
 *       variable = computeValue();
 *       wait(someAsyncOp(variable));
 *   }
 *
 * This file follows this pattern - all state variables that will be used after wait() calls
 * are declared at the beginning of the ACTOR, before any early returns or exception-throwing code.
 */

// Global storage for MockS3 (shared across all simulated processes)
struct MockS3GlobalStorage {
	struct ObjectData {
		std::string content;
		HTTP::Headers headers;
		std::map<std::string, std::string> tags;
		std::string etag;
		double lastModified;

		ObjectData() : lastModified(now()) {}
		ObjectData(const std::string& data) : content(data), lastModified(now()) { etag = generateETag(data); }

		static std::string generateETag(const std::string& content) {
			return "\"" + HTTP::computeMD5Sum(content) + "\"";
		}
	};

	struct MultipartUpload {
		std::string uploadId;
		std::string bucket;
		std::string object;
		std::map<int, std::pair<std::string, std::string>> parts; // partNum -> {etag, content}
		HTTP::Headers metadata;
		double initiated;

		MultipartUpload() = default;
		MultipartUpload(const std::string& b, const std::string& o) : bucket(b), object(o), initiated(now()) {
			uploadId = deterministicRandom()->randomUniqueID().toString();
		}
	};

	std::map<std::string, std::map<std::string, ObjectData>> buckets;
	std::map<std::string, MultipartUpload> multipartUploads;

	// Persistence configuration
	std::string persistenceDir;
	bool persistenceEnabled = false;
	bool persistenceLoaded = false;

	// Note: In FDB simulation, function-local statics are SHARED across all simulated processes
	// because they all run on the same OS thread. This is exactly what we want for MockS3 storage.
	MockS3GlobalStorage() { TraceEvent("MockS3GlobalStorageCreated").detail("Address", format("%p", this)); }

	// Clear all stored data - called at the start of each simulation test to prevent
	// data accumulation across multiple tests
	void clearStorage() {
		buckets.clear();
		multipartUploads.clear();
		TraceEvent("MockS3GlobalStorageCleared").detail("Address", format("%p", this));
	}

	// Enable persistence to specified directory
	// Note: When using simulation filesystem, directories are created automatically by file open()
	// and each simulated machine has its own isolated directory structure
	void enablePersistence(const std::string& dir) {
		persistenceDir = dir;
		persistenceEnabled = true;
		persistenceLoaded = false;

		TraceEvent("MockS3PersistenceEnabled")
		    .detail("Directory", dir)
		    .detail("Address", format("%p", this))
		    .detail("UsingSimulationFS", g_network->isSimulated());
	}

	// Get paths for persistence files
	// Note: Object names with slashes map directly to filesystem directory structure
	std::string getObjectDataPath(const std::string& bucket, const std::string& object) const {
		return persistenceDir + "/objects/" + bucket + "/" + object + ".data";
	}

	std::string getObjectMetaPath(const std::string& bucket, const std::string& object) const {
		return persistenceDir + "/objects/" + bucket + "/" + object + ".meta.json";
	}

	std::string getMultipartStatePath(const std::string& uploadId) const {
		return persistenceDir + "/multipart/" + uploadId + ".state.json";
	}

	std::string getMultipartPartPath(const std::string& uploadId, int partNum) const {
		return persistenceDir + "/multipart/" + uploadId + ".part." + std::to_string(partNum);
	}

	std::string getMultipartPartMetaPath(const std::string& uploadId, int partNum) const {
		return persistenceDir + "/multipart/" + uploadId + ".part." + std::to_string(partNum) + ".meta.json";
	}
};

// Accessor function - uses function-local static for lazy initialization
// In simulation, this static is shared across all simulated processes (same OS thread)
static MockS3GlobalStorage& getGlobalStorage() {
	static MockS3GlobalStorage storage;
	return storage;
}

// Helper: Create all parent directories for a file path
// Uses platform::createDirectory which handles recursive creation and EEXIST errors
static void createParentDirectories(const std::string& filePath) {
	size_t lastSlash = filePath.find_last_of('/');
	if (lastSlash != std::string::npos && lastSlash > 0) {
		std::string parentDir = filePath.substr(0, lastSlash);
		platform::createDirectory(parentDir); // Handles recursive creation and EEXIST
	}
}

// ACTOR: Atomic file write using simulation filesystem without chaos injection
// Chaos-free because AsyncFileChaos only affects files with "storage-" in the name
// (see AsyncFileChaos.h:40). OPEN_NO_AIO controls AsyncFileNonDurable behavior.
ACTOR static Future<Void> atomicWriteFile(std::string path, std::string content) {
	state bool initialized = true; // Ensure Promise initialization before try block

	try {
		// Create all parent directories
		createParentDirectories(path);

		// Use simulation filesystem with atomic write
		// No chaos injection: simfdb/mocks3/* files don't match "storage-*" pattern
		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    path,
		    IAsyncFile::OPEN_ATOMIC_WRITE_AND_CREATE | IAsyncFile::OPEN_CREATE | IAsyncFile::OPEN_READWRITE |
		        IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO,
		    0644));

		wait(file->write(content.data(), content.size(), 0));
		wait(file->sync()); // Atomic rename happens here
		file = Reference<IAsyncFile>();

		TraceEvent("MockS3PersistenceWriteSuccess").detail("Path", path).detail("Size", content.size());
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3PersistenceWriteException").error(e).detail("Path", path);
	}
	return Void();
}

// ACTOR: Read file content using simulation filesystem without chaos
// Chaos-free because AsyncFileChaos only affects files with "storage-" in the name
ACTOR static Future<std::string> readFileContent(std::string path) {
	state bool exists = fileExists(path); // State variable before any early returns

	try {
		if (!exists) {
			return std::string();
		}

		state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(
		    path, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED | IAsyncFile::OPEN_NO_AIO, 0644));
		state int64_t fileSize = wait(file->size());

		state std::string content;
		content.resize(fileSize);
		int bytesRead = wait(file->read((uint8_t*)content.data(), fileSize, 0));
		file = Reference<IAsyncFile>();

		if (bytesRead != fileSize) {
			TraceEvent(SevWarn, "MockS3PersistenceReadShort")
			    .detail("Path", path)
			    .detail("Expected", fileSize)
			    .detail("Actual", bytesRead);
		}

		return content;
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3PersistenceReadException").error(e).detail("Path", path);
		return std::string();
	}
}

// ACTOR: Delete file using simulation filesystem
// Wraps deleteFile with trace events and error handling for MockS3 persistence cleanup
ACTOR static Future<Void> deletePersistedFile(std::string path) {
	try {
		wait(IAsyncFileSystem::filesystem()->deleteFile(path, true)); // Durable delete
		TraceEvent("MockS3PersistenceDelete").detail("Path", path);
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3PersistenceDeleteException").error(e).detail("Path", path);
	}
	return Void();
}

// JSON Serialization using rapidjson
static std::string serializeObjectMeta(const MockS3GlobalStorage::ObjectData& obj) {
	using namespace rapidjson;
	Document doc;
	doc.SetObject();
	auto& allocator = doc.GetAllocator();

	doc.AddMember("etag", Value(obj.etag.c_str(), allocator), allocator);
	doc.AddMember("lastModified", obj.lastModified, allocator);

	Value tagsObj(kObjectType);
	for (const auto& tag : obj.tags) {
		tagsObj.AddMember(Value(tag.first.c_str(), allocator), Value(tag.second.c_str(), allocator), allocator);
	}
	doc.AddMember("tags", tagsObj, allocator);

	StringBuffer buffer;
	PrettyWriter<StringBuffer> writer(buffer);
	doc.Accept(writer);
	return buffer.GetString();
}

// JSON Deserialization using rapidjson
static void deserializeObjectMeta(const std::string& jsonStr, MockS3GlobalStorage::ObjectData& obj) {
	using namespace rapidjson;
	Document doc;
	doc.Parse(jsonStr.c_str());

	if (doc.HasMember("etag") && doc["etag"].IsString())
		obj.etag = doc["etag"].GetString();
	if (doc.HasMember("lastModified") && doc["lastModified"].IsNumber())
		obj.lastModified = doc["lastModified"].GetDouble();
	if (doc.HasMember("tags") && doc["tags"].IsObject()) {
		for (auto& m : doc["tags"].GetObject()) {
			if (m.value.IsString())
				obj.tags[m.name.GetString()] = m.value.GetString();
		}
	}
}

static std::string serializeMultipartState(const MockS3GlobalStorage::MultipartUpload& upload) {
	using namespace rapidjson;
	Document doc;
	doc.SetObject();
	auto& allocator = doc.GetAllocator();

	doc.AddMember("uploadId", Value(upload.uploadId.c_str(), allocator), allocator);
	doc.AddMember("bucket", Value(upload.bucket.c_str(), allocator), allocator);
	doc.AddMember("object", Value(upload.object.c_str(), allocator), allocator);
	doc.AddMember("initiated", upload.initiated, allocator);

	Value partsArray(kArrayType);
	for (const auto& part : upload.parts) {
		Value partObj(kObjectType);
		partObj.AddMember("partNum", part.first, allocator);
		partObj.AddMember("etag", Value(part.second.first.c_str(), allocator), allocator);
		partsArray.PushBack(partObj, allocator);
	}
	doc.AddMember("parts", partsArray, allocator);

	StringBuffer buffer;
	PrettyWriter<StringBuffer> writer(buffer);
	doc.Accept(writer);
	return buffer.GetString();
}

static void deserializeMultipartState(const std::string& jsonStr, MockS3GlobalStorage::MultipartUpload& upload) {
	using namespace rapidjson;
	Document doc;
	doc.Parse(jsonStr.c_str());

	if (doc.HasMember("uploadId") && doc["uploadId"].IsString())
		upload.uploadId = doc["uploadId"].GetString();
	if (doc.HasMember("bucket") && doc["bucket"].IsString())
		upload.bucket = doc["bucket"].GetString();
	if (doc.HasMember("object") && doc["object"].IsString())
		upload.object = doc["object"].GetString();
	if (doc.HasMember("initiated") && doc["initiated"].IsNumber())
		upload.initiated = doc["initiated"].GetDouble();
	if (doc.HasMember("parts") && doc["parts"].IsArray()) {
		for (auto& partVal : doc["parts"].GetArray()) {
			if (partVal.HasMember("partNum") && partVal["partNum"].IsInt() && partVal.HasMember("etag") &&
			    partVal["etag"].IsString()) {
				int partNum = partVal["partNum"].GetInt();
				std::string etag = partVal["etag"].GetString();
				upload.parts[partNum] = { etag, "" }; // Content loaded separately from .part.N files
			}
		}
	}
}

// Forward declarations for state loading functions
ACTOR static Future<Void> loadPersistedObjects(std::string persistenceDir);
ACTOR static Future<Void> loadPersistedMultipartUploads(std::string persistenceDir);
ACTOR static Future<Void> loadMockS3PersistedStateImpl();
Future<Void> loadMockS3PersistedStateFuture();

static std::string serializePartMeta(const std::string& etag) {
	using namespace rapidjson;
	Document doc;
	doc.SetObject();
	auto& allocator = doc.GetAllocator();
	doc.AddMember("etag", Value(etag.c_str(), allocator), allocator);

	StringBuffer buffer;
	Writer<StringBuffer> writer(buffer); // Use Writer instead of PrettyWriter for compact output
	doc.Accept(writer);
	return buffer.GetString();
}

// ACTOR: Persist object data and metadata
ACTOR static Future<Void> persistObject(std::string bucket, std::string object) {
	auto& storage = getGlobalStorage();
	ASSERT(storage.persistenceEnabled); // Caller should check before calling

	auto bucketIter = storage.buckets.find(bucket);
	if (bucketIter == storage.buckets.end()) {
		return Void();
	}

	auto objectIter = bucketIter->second.find(object);
	if (objectIter == bucketIter->second.end()) {
		return Void();
	}

	// Copy data to state variables (needed across wait() boundaries)
	state std::string content = objectIter->second.content;
	state std::string metaJson = serializeObjectMeta(objectIter->second);

	try {

		// Compute paths before wait() calls
		state std::string dataPath = storage.getObjectDataPath(bucket, object);
		state std::string metaPath = storage.getObjectMetaPath(bucket, object);

		TraceEvent("MockS3PersistingObject")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("DataPath", dataPath)
		    .detail("Size", content.size());

		// Persist object content
		wait(atomicWriteFile(dataPath, content));

		// Persist object metadata
		wait(atomicWriteFile(metaPath, metaJson));

		TraceEvent("MockS3ObjectPersisted")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("Size", content.size());
	} catch (Error& e) {
		TraceEvent(SevError, "MockS3PersistObjectFailed").error(e).detail("Bucket", bucket).detail("Object", object);
	}

	return Void();
}

// ACTOR: Persist multipart upload state
ACTOR static Future<Void> persistMultipartState(std::string uploadId) {
	state std::string persistenceDir; // Declare state before any early returns
	state std::map<int, std::pair<std::string, std::string>> parts;

	auto& storage = getGlobalStorage();
	ASSERT(storage.persistenceEnabled); // Caller should check before calling

	auto uploadIter = storage.multipartUploads.find(uploadId);
	if (uploadIter == storage.multipartUploads.end()) {
		return Void();
	}

	const auto& upload = uploadIter->second;
	persistenceDir = storage.persistenceDir;
	parts = upload.parts;

	try {

		// Persist multipart state
		std::string statePath = persistenceDir + "/multipart/" + uploadId + ".state.json";
		std::string stateJson = serializeMultipartState(upload);
		wait(atomicWriteFile(statePath, stateJson));

		// Persist each part
		state std::map<int, std::pair<std::string, std::string>>::iterator partIter = parts.begin();
		while (partIter != parts.end()) {
			state int partNum = partIter->first;
			state std::string etag = partIter->second.first;
			state std::string partData = partIter->second.second;

			state std::string partPath = persistenceDir + "/multipart/" + uploadId + ".part." + std::to_string(partNum);
			wait(atomicWriteFile(partPath, partData));

			state std::string partMetaPath = partPath + ".meta.json";
			state std::string partMetaJson = serializePartMeta(etag);
			wait(atomicWriteFile(partMetaPath, partMetaJson));

			partIter++;
		}

		TraceEvent("MockS3MultipartPersisted").detail("UploadId", uploadId).detail("PartsCount", parts.size());
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3PersistMultipartFailed").error(e).detail("UploadId", uploadId);
	}

	return Void();
}

// ACTOR: Delete persisted object
ACTOR static Future<Void> deletePersistedObject(std::string bucket, std::string object) {
	state std::string dataPath; // Declare state before any early returns
	state std::string metaPath;

	auto& storage = getGlobalStorage();
	ASSERT(storage.persistenceEnabled); // Caller should check before calling

	dataPath = storage.getObjectDataPath(bucket, object);
	metaPath = storage.getObjectMetaPath(bucket, object);

	try {
		wait(deletePersistedFile(dataPath));
		wait(deletePersistedFile(metaPath));

		TraceEvent("MockS3ObjectDeleted").detail("Bucket", bucket).detail("Object", object);
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3DeletePersistedObjectFailed")
		    .error(e)
		    .detail("Bucket", bucket)
		    .detail("Object", object);
	}

	return Void();
}

// ACTOR: Delete persisted multipart upload
ACTOR static Future<Void> deletePersistedMultipart(std::string uploadId) {
	state int maxPart; // Declare state before any early returns
	state std::string persistenceDir;
	state int partNum;
	state std::string partPath;
	state std::string partMetaPath;

	auto& storage = getGlobalStorage();
	ASSERT(storage.persistenceEnabled); // Caller should check before calling

	try {
		// Get parts count before we delete
		auto uploadIter = storage.multipartUploads.find(uploadId);
		maxPart = 100; // Conservative estimate
		if (uploadIter != storage.multipartUploads.end()) {
			for (const auto& part : uploadIter->second.parts) {
				maxPart = std::max(maxPart, part.first);
			}
		}

		// Store persistence dir in state variable (needed for path construction after wait())
		persistenceDir = storage.persistenceDir;

		// Delete state file
		std::string statePath = persistenceDir + "/multipart/" + uploadId + ".state.json";
		wait(deletePersistedFile(statePath));

		// Delete all part files (try all possible part numbers)
		partNum = 1;
		while (partNum <= maxPart + 10) {
			partPath = persistenceDir + "/multipart/" + uploadId + ".part." + std::to_string(partNum);
			partMetaPath = partPath + ".meta.json";
			wait(deletePersistedFile(partPath));
			wait(deletePersistedFile(partMetaPath));
			partNum++;
		}

		TraceEvent("MockS3MultipartDeleted").detail("UploadId", uploadId);
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3DeletePersistedMultipartFailed").error(e).detail("UploadId", uploadId);
	}

	return Void();
}

// Mock S3 Server Implementation for deterministic testing
class MockS3ServerImpl {
public:
	using ObjectData = MockS3GlobalStorage::ObjectData;
	using MultipartUpload = MockS3GlobalStorage::MultipartUpload;

	MockS3ServerImpl() { TraceEvent("MockS3ServerImpl_Constructor").detail("Address", format("%p", this)); }

	~MockS3ServerImpl() { TraceEvent("MockS3ServerImpl_Destructor").detail("Address", format("%p", this)); }

	// S3 Operation Handlers
	ACTOR static Future<Void> handleRequest(MockS3ServerImpl* self,
	                                        Reference<HTTP::IncomingRequest> req,
	                                        Reference<HTTP::OutgoingResponse> response) {

		TraceEvent("MockS3Request")
		    .detail("Method", req->verb)
		    .detail("Resource", req->resource)
		    .detail("ContentLength", req->data.contentLen)
		    .detail("Headers", req->data.headers.size())
		    .detail("UserAgent",
		            req->data.headers.find("User-Agent") != req->data.headers.end() ? req->data.headers.at("User-Agent")
		                                                                            : "N/A")
		    .detail("Host",
		            req->data.headers.find("Host") != req->data.headers.end() ? req->data.headers.at("Host") : "N/A");

		try {
			// Parse S3 request components
			std::string bucket, object;
			std::map<std::string, std::string> queryParams;
			self->parseS3Request(req->resource, bucket, object, queryParams);

			TraceEvent("MockS3ParsedRequest")
			    .detail("Bucket", bucket)
			    .detail("Object", object)
			    .detail("QueryParamCount", queryParams.size());

			// Route to appropriate handler based on operation type
			if (queryParams.count("uploads")) {
				wait(self->handleMultipartStart(self, req, response, bucket, object));
			} else if (queryParams.count("uploadId")) {
				if (queryParams.count("partNumber")) {
					wait(self->handleUploadPart(self, req, response, bucket, object, queryParams));
				} else if (req->verb == "POST") {
					wait(self->handleMultipartComplete(self, req, response, bucket, object, queryParams));
				} else if (req->verb == "DELETE") {
					wait(self->handleMultipartAbort(self, req, response, bucket, object, queryParams));
				} else {
					self->sendError(
					    response, HTTP::HTTP_STATUS_CODE_BAD_GATEWAY, "InvalidRequest", "Unknown multipart operation");
				}
			} else if (queryParams.count("tagging")) {
				if (req->verb == "PUT") {
					wait(self->handlePutObjectTags(self, req, response, bucket, object));
				} else if (req->verb == "GET") {
					wait(self->handleGetObjectTags(self, req, response, bucket, object));
				} else {
					self->sendError(response,
					                HTTP::HTTP_STATUS_CODE_BAD_GATEWAY,
					                "MethodNotAllowed",
					                "Method not allowed for tagging");
				}
			} else if (queryParams.count("list-type") || (req->verb == "GET" && object.empty())) {
				// ListObjects operation (when GET request to bucket)
				wait(self->handleListObjects(self, req, response, bucket, queryParams));
			} else if (object.empty()) {
				// Bucket-level operations
				if (req->verb == "HEAD") {
					wait(self->handleHeadBucket(self, req, response, bucket));
				} else if (req->verb == "PUT") {
					wait(self->handlePutBucket(self, req, response, bucket));
				} else {
					self->sendError(response,
					                HTTP::HTTP_STATUS_CODE_BAD_GATEWAY,
					                "MethodNotAllowed",
					                "Bucket operation not supported");
				}
			} else {
				// Basic object operations
				if (req->verb == "PUT") {
					wait(self->handlePutObject(self, req, response, bucket, object));
				} else if (req->verb == "GET") {
					wait(self->handleGetObject(self, req, response, bucket, object));
				} else if (req->verb == "DELETE") {
					wait(self->handleDeleteObject(self, req, response, bucket, object));
				} else if (req->verb == "HEAD") {
					wait(self->handleHeadObject(self, req, response, bucket, object));
				} else {
					self->sendError(
					    response, HTTP::HTTP_STATUS_CODE_BAD_GATEWAY, "MethodNotAllowed", "Method not supported");
				}
			}

		} catch (Error& e) {
			TraceEvent(SevError, "MockS3RequestError").error(e).detail("Resource", req->resource);
			self->sendError(response, 500, "InternalError", "Internal server error");
		}

		return Void();
	}

	void parseS3Request(const std::string& resource,
	                    std::string& bucket,
	                    std::string& object,
	                    std::map<std::string, std::string>& queryParams) {

		// Split resource into path and query string
		size_t queryPos = resource.find('?');
		std::string path = (queryPos != std::string::npos) ? resource.substr(0, queryPos) : resource;
		std::string query = (queryPos != std::string::npos) ? resource.substr(queryPos + 1) : "";

		// Parse path: /bucket/object (like real S3)
		if (path.size() > 1) {
			path = path.substr(1); // Remove leading /
			size_t slashPos = path.find('/');
			if (slashPos != std::string::npos) {
				bucket = path.substr(0, slashPos);
				object = path.substr(slashPos + 1);
			} else {
				bucket = path;
				object = "";
			}
		}

		// Parse query parameters
		if (!query.empty()) {
			std::regex paramRegex("([^&=]+)=?([^&]*)");
			std::sregex_iterator iter(query.begin(), query.end(), paramRegex);
			std::sregex_iterator end;

			for (; iter != end; ++iter) {
				std::string key = iter->str(1);
				std::string value = iter->str(2);
				// URL decode the parameter value
				queryParams[key] = HTTP::urlDecode(value);
			}
		}

		// MockS3Server handles S3 HTTP requests where bucket is always the first path component
		// For bucket operations: HEAD /bucket_name
		// For object operations: HEAD /bucket_name/object_path
		if (bucket.empty()) {
			TraceEvent(SevWarn, "MockS3MissingBucketInPath").detail("Resource", resource).detail("QueryString", query);
			throw backup_invalid_url();
		}

		TraceEvent("MockS3ParsedPath")
		    .detail("OriginalResource", resource)
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("QueryString", query);
	}

	// Parse HTTP Range header: "bytes=start-end"
	// Returns true if parsing succeeded, false otherwise
	// Sets rangeStart and rangeEnd to the parsed values
	static bool parseRangeHeader(const std::string& rangeHeader, int64_t& rangeStart, int64_t& rangeEnd) {
		if (rangeHeader.empty()) {
			return false;
		}

		// Check for "bytes=" prefix
		if (rangeHeader.substr(0, 6) != "bytes=") {
			return false;
		}

		std::string range = rangeHeader.substr(6);
		size_t dashPos = range.find('-');
		if (dashPos == std::string::npos) {
			return false;
		}

		try {
			rangeStart = std::stoll(range.substr(0, dashPos));
			std::string endStr = range.substr(dashPos + 1);
			if (endStr.empty()) {
				// Open-ended range (e.g., "bytes=100-")
				rangeEnd = -1; // Indicates open-ended
			} else {
				rangeEnd = std::stoll(endStr);
			}
			return true;
		} catch (...) {
			return false;
		}
	}

	// Multipart Upload Operations
	ACTOR static Future<Void> handleMultipartStart(MockS3ServerImpl* self,
	                                               Reference<HTTP::IncomingRequest> req,
	                                               Reference<HTTP::OutgoingResponse> response,
	                                               std::string bucket,
	                                               std::string object) {

		TraceEvent("MockS3MultipartStart").detail("Bucket", bucket).detail("Object", object);

		// Create multipart upload
		MultipartUpload upload(bucket, object);
		state std::string uploadId = upload.uploadId;
		getGlobalStorage().multipartUploads[uploadId] = std::move(upload);

		// Persist multipart state
		wait(persistMultipartState(uploadId));

		// Generate XML response
		std::string xml = format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
		                         "<InitiateMultipartUploadResult>\n"
		                         "  <Bucket>%s</Bucket>\n"
		                         "  <Key>%s</Key>\n"
		                         "  <UploadId>%s</UploadId>\n"
		                         "</InitiateMultipartUploadResult>",
		                         bucket.c_str(),
		                         object.c_str(),
		                         uploadId.c_str());

		self->sendXMLResponse(response, 200, xml);

		TraceEvent("MockS3MultipartStarted").detail("UploadId", uploadId);

		return Void();
	}

	ACTOR static Future<Void> handleUploadPart(MockS3ServerImpl* self,
	                                           Reference<HTTP::IncomingRequest> req,
	                                           Reference<HTTP::OutgoingResponse> response,
	                                           std::string bucket,
	                                           std::string object,
	                                           std::map<std::string, std::string> queryParams) {

		state std::string uploadId = queryParams.at("uploadId");
		state int partNumber = std::stoi(queryParams.at("partNumber"));

		TraceEvent("MockS3UploadPart")
		    .detail("UploadId", uploadId)
		    .detail("PartNumber", partNumber)
		    .detail("ContentLength", req->data.contentLen)
		    .detail("ActualContentSize", req->data.content.size())
		    .detail("ContentPreview",
		            req->data.content.size() > 0
		                ? req->data.content.substr(0, std::min((size_t)20, req->data.content.size()))
		                : "EMPTY");

		auto uploadIter = getGlobalStorage().multipartUploads.find(uploadId);
		if (uploadIter == getGlobalStorage().multipartUploads.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchUpload", "Upload not found");
			return Void();
		}

		// Store part data
		state std::string etag = ObjectData::generateETag(req->data.content);
		uploadIter->second.parts[partNumber] = { etag, req->data.content };

		// Persist multipart state (includes all parts)
		wait(persistMultipartState(uploadId));

		// Return ETag in response
		response->code = 200;
		response->data.headers["ETag"] = etag;
		response->data.contentLen = 0;
		response->data.content->discardAll(); // Clear existing content

		TraceEvent("MockS3PartUploaded")
		    .detail("UploadId", uploadId)
		    .detail("PartNumber", partNumber)
		    .detail("ETag", etag);

		return Void();
	}

	ACTOR static Future<Void> handleMultipartComplete(MockS3ServerImpl* self,
	                                                  Reference<HTTP::IncomingRequest> req,
	                                                  Reference<HTTP::OutgoingResponse> response,
	                                                  std::string bucket,
	                                                  std::string object,
	                                                  std::map<std::string, std::string> queryParams) {

		state std::string uploadId = queryParams.at("uploadId");

		TraceEvent("MockS3MultipartComplete").detail("UploadId", uploadId);

		auto uploadIter = getGlobalStorage().multipartUploads.find(uploadId);
		if (uploadIter == getGlobalStorage().multipartUploads.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchUpload", "Upload not found");
			return Void();
		}

		// Combine all parts in order
		state std::string combinedContent;
		for (auto& part : uploadIter->second.parts) {
			combinedContent += part.second.second;
		}

		TraceEvent("MockS3MultipartDebug")
		    .detail("UploadId", uploadId)
		    .detail("PartsCount", uploadIter->second.parts.size())
		    .detail("CombinedSize", combinedContent.size())
		    .detail("CombinedPreview",
		            combinedContent.size() > 0 ? combinedContent.substr(0, std::min((size_t)20, combinedContent.size()))
		                                       : "EMPTY");

		// Create final object
		ObjectData obj(combinedContent);
		getGlobalStorage().buckets[bucket][object] = std::move(obj);

		TraceEvent("MockS3MultipartFinalObject")
		    .detail("UploadId", uploadId)
		    .detail("StoredSize", getGlobalStorage().buckets[bucket][object].content.size())
		    .detail("StoredPreview",
		            getGlobalStorage().buckets[bucket][object].content.size() > 0
		                ? getGlobalStorage().buckets[bucket][object].content.substr(
		                      0, std::min((size_t)20, getGlobalStorage().buckets[bucket][object].content.size()))
		                : "EMPTY");

		// Persist final object
		wait(persistObject(bucket, object));

		// Clean up multipart upload (in-memory and persisted)
		getGlobalStorage().multipartUploads.erase(uploadId);
		wait(deletePersistedMultipart(uploadId));

		// Generate completion XML response
		std::string xml = format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
		                         "<CompleteMultipartUploadResult>\n"
		                         "  <Bucket>%s</Bucket>\n"
		                         "  <Key>%s</Key>\n"
		                         "  <ETag>%s</ETag>\n"
		                         "</CompleteMultipartUploadResult>",
		                         bucket.c_str(),
		                         object.c_str(),
		                         getGlobalStorage().buckets[bucket][object].etag.c_str());

		self->sendXMLResponse(response, 200, xml);

		TraceEvent("MockS3MultipartCompleted").detail("UploadId", uploadId).detail("FinalSize", combinedContent.size());

		return Void();
	}

	ACTOR static Future<Void> handleMultipartAbort(MockS3ServerImpl* self,
	                                               Reference<HTTP::IncomingRequest> req,
	                                               Reference<HTTP::OutgoingResponse> response,
	                                               std::string bucket,
	                                               std::string object,
	                                               std::map<std::string, std::string> queryParams) {

		state std::string uploadId = queryParams.at("uploadId");

		TraceEvent("MockS3MultipartAbort").detail("UploadId", uploadId);

		auto uploadIter = getGlobalStorage().multipartUploads.find(uploadId);
		if (uploadIter == getGlobalStorage().multipartUploads.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchUpload", "Upload not found");
			return Void();
		}

		// Remove multipart upload (in-memory and persisted)
		getGlobalStorage().multipartUploads.erase(uploadId);
		wait(deletePersistedMultipart(uploadId));

		response->code = 204; // No Content
		response->data.contentLen = 0;
		response->data.content->discardAll(); // Clear existing content

		TraceEvent("MockS3MultipartAborted").detail("UploadId", uploadId);

		return Void();
	}

	// Object Tagging Operations
	static Future<Void> handlePutObjectTags(MockS3ServerImpl* self,
	                                        Reference<HTTP::IncomingRequest> req,
	                                        Reference<HTTP::OutgoingResponse> response,
	                                        std::string bucket,
	                                        std::string object) {

		TraceEvent("MockS3PutObjectTags")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("TagsXML", req->data.content);

		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter == getGlobalStorage().buckets.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		auto objectIter = bucketIter->second.find(object);
		if (objectIter == bucketIter->second.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchKey", "Object not found");
			return Void();
		}

		// Parse tags XML (simplified parser)
		std::map<std::string, std::string> tags = self->parseTagsXML(req->data.content);
		objectIter->second.tags = tags;

		response->code = 200;
		response->data.contentLen = 0;
		response->data.content->discardAll(); // Clear existing content

		TraceEvent("MockS3ObjectTagsSet")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("TagCount", tags.size());

		return Void();
	}

	static Future<Void> handleGetObjectTags(MockS3ServerImpl* self,
	                                        Reference<HTTP::IncomingRequest> req,
	                                        Reference<HTTP::OutgoingResponse> response,
	                                        std::string bucket,
	                                        std::string object) {

		TraceEvent("MockS3GetObjectTags").detail("Bucket", bucket).detail("Object", object);

		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter == getGlobalStorage().buckets.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		auto objectIter = bucketIter->second.find(object);
		if (objectIter == bucketIter->second.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchKey", "Object not found");
			return Void();
		}

		// Generate tags XML response
		std::string xml = self->generateTagsXML(objectIter->second.tags);
		self->sendXMLResponse(response, 200, xml);

		TraceEvent("MockS3ObjectTagsRetrieved")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("TagCount", objectIter->second.tags.size());

		return Void();
	}

	// Basic Object Operations
	ACTOR static Future<Void> handlePutObject(MockS3ServerImpl* self,
	                                          Reference<HTTP::IncomingRequest> req,
	                                          Reference<HTTP::OutgoingResponse> response,
	                                          std::string bucket,
	                                          std::string object) {

		TraceEvent("MockS3PutObject_Debug")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ContentLength", req->data.contentLen)
		    .detail("ContentSize", req->data.content.size())
		    .detail("ContentPreview", req->data.content.substr(0, std::min(100, (int)req->data.content.size())));

		ObjectData obj(req->data.content);
		state std::string etag = obj.etag;
		getGlobalStorage().buckets[bucket][object] = std::move(obj);

		TraceEvent("MockS3PutObject_Stored")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ETag", etag)
		    .detail("StoredSize", getGlobalStorage().buckets[bucket][object].content.size());

		// Persist object to disk
		wait(persistObject(bucket, object));

		response->code = 200;
		response->data.headers["ETag"] = etag;
		response->data.contentLen = 0;
		// Don't create UnsentPacketQueue for empty responses - let HTTP server handle it

		TraceEvent("MockS3PutObject_Response")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ResponseCode", response->code)
		    .detail("ContentLen", response->data.contentLen)
		    .detail("HasContent", response->data.content != nullptr);

		return Void();
	}

	static Future<Void> handleGetObject(MockS3ServerImpl* self,
	                                    Reference<HTTP::IncomingRequest> req,
	                                    Reference<HTTP::OutgoingResponse> response,
	                                    std::string bucket,
	                                    std::string object) {

		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter == getGlobalStorage().buckets.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		auto objectIter = bucketIter->second.find(object);
		if (objectIter == bucketIter->second.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchKey", "Object not found");
			return Void();
		}

		std::string content = objectIter->second.content;
		std::string etag = objectIter->second.etag;
		std::string contentMD5 = HTTP::computeMD5Sum(content);

		// Handle HTTP Range header for partial content requests
		// This is essential for AsyncFileEncrypted to read encrypted blocks correctly
		int64_t rangeStart = 0;
		int64_t rangeEnd = static_cast<int64_t>(content.size() - 1);
		bool isRangeRequest = false;

		auto rangeHeader = req->data.headers.find("Range");
		if (rangeHeader != req->data.headers.end()) {
			int64_t parsedStart, parsedEnd;
			if (parseRangeHeader(rangeHeader->second, parsedStart, parsedEnd)) {
				rangeStart = parsedStart;
				if (parsedEnd == -1) {
					// Open-ended range (e.g., "bytes=100-")
					rangeEnd = static_cast<int64_t>(content.size() - 1);
				} else {
					rangeEnd = parsedEnd;
				}
				// Clamp range to actual content size
				int64_t contentSize = static_cast<int64_t>(content.size() - 1);
				rangeEnd = std::min(rangeEnd, contentSize);
				rangeStart = std::min(rangeStart, contentSize);
				if (rangeStart <= rangeEnd) {
					isRangeRequest = true;
				}
			}
		}

		// Extract the requested range
		std::string responseContent;
		if (isRangeRequest && rangeStart <= rangeEnd) {
			responseContent =
			    content.substr(static_cast<size_t>(rangeStart), static_cast<size_t>(rangeEnd - rangeStart + 1));
			response->code = 206; // Partial Content
			response->data.headers["Content-Range"] =
			    format("bytes %lld-%lld/%zu", rangeStart, rangeEnd, content.size());
			// For range requests, calculate MD5 of the partial content, not full content
			contentMD5 = HTTP::computeMD5Sum(responseContent);
		} else {
			responseContent = content;
			response->code = 200;
		}

		response->data.headers["ETag"] = etag;
		response->data.headers["Content-Type"] = "binary/octet-stream";
		response->data.headers["Content-MD5"] = contentMD5;

		// Write content to response
		response->data.contentLen = responseContent.size();
		response->data.headers["Content-Length"] = std::to_string(responseContent.size());
		response->data.content->discardAll(); // Clear existing content

		if (!responseContent.empty()) {
			// Use the correct approach: getWriteBuffer from the UnsentPacketQueue
			PacketBuffer* buffer = response->data.content->getWriteBuffer(responseContent.size());
			PacketWriter pw(buffer, nullptr, Unversioned());
			pw.serializeBytes(responseContent);
			pw.finish();
		}

		return Void();
	}

	ACTOR static Future<Void> handleDeleteObject(MockS3ServerImpl* self,
	                                             Reference<HTTP::IncomingRequest> req,
	                                             Reference<HTTP::OutgoingResponse> response,
	                                             std::string bucket,
	                                             std::string object) {

		TraceEvent("MockS3DeleteObject").detail("Bucket", bucket).detail("Object", object);

		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter != getGlobalStorage().buckets.end()) {
			bucketIter->second.erase(object);
		}

		// Delete persisted object
		wait(deletePersistedObject(bucket, object));

		response->code = 204; // No Content
		response->data.contentLen = 0;
		response->data.content->discardAll(); // Clear existing content

		TraceEvent("MockS3ObjectDeleted").detail("Bucket", bucket).detail("Object", object);

		return Void();
	}

	static Future<Void> handleHeadObject(MockS3ServerImpl* self,
	                                     Reference<HTTP::IncomingRequest> req,
	                                     Reference<HTTP::OutgoingResponse> response,
	                                     std::string bucket,
	                                     std::string object) {

		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter == getGlobalStorage().buckets.end()) {
			TraceEvent("MockS3HeadObjectNoBucket")
			    .detail("Bucket", bucket)
			    .detail("Object", object)
			    .detail("AvailableBuckets", getGlobalStorage().buckets.size());
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		auto objectIter = bucketIter->second.find(object);
		if (objectIter == bucketIter->second.end()) {
			TraceEvent("MockS3HeadObjectNoObject")
			    .detail("Bucket", bucket)
			    .detail("Object", object)
			    .detail("ObjectsInBucket", bucketIter->second.size());
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchKey", "Object not found");
			return Void();
		}

		const ObjectData& obj = objectIter->second;
		std::string etag = obj.etag;
		size_t contentSize = obj.content.size();
		std::string preview = contentSize > 0 ? obj.content.substr(0, std::min((size_t)20, contentSize)) : "EMPTY";

		TraceEvent("MockS3HeadObjectFound")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("Size", contentSize)
		    .detail("Preview", preview);

		response->code = 200;
		response->data.headers["ETag"] = etag;
		response->data.headers["Content-Length"] = std::to_string(contentSize);
		response->data.headers["Content-Type"] = "binary/octet-stream";
		// HEAD requests need contentLen set to actual size for headers
		response->data.contentLen = contentSize; // This controls ResponseContentSize in HTTP logs

		return Void();
	}

	// S3 ListObjects Operation
	static Future<Void> handleListObjects(MockS3ServerImpl* self,
	                                      Reference<HTTP::IncomingRequest> req,
	                                      Reference<HTTP::OutgoingResponse> response,
	                                      std::string bucket,
	                                      std::map<std::string, std::string> queryParams) {

		TraceEvent("MockS3ListObjects").detail("Bucket", bucket).detail("QueryParamCount", queryParams.size());

		// Get query parameters for listing
		std::string prefix = queryParams.count("prefix") ? queryParams.at("prefix") : "";
		std::string delimiter = queryParams.count("delimiter") ? queryParams.at("delimiter") : "";
		std::string marker = queryParams.count("marker") ? queryParams.at("marker") : "";
		std::string continuationToken =
		    queryParams.count("continuation-token") ? queryParams.at("continuation-token") : "";
		int maxKeys = queryParams.count("max-keys") ? std::stoi(queryParams.at("max-keys")) : 1000;

		TraceEvent("MockS3ListObjectsDebug")
		    .detail("Bucket", bucket)
		    .detail("Prefix", prefix)
		    .detail("Delimiter", delimiter)
		    .detail("Marker", marker)
		    .detail("ContinuationToken", continuationToken)
		    .detail("MaxKeys", maxKeys);

		// Find bucket
		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter == getGlobalStorage().buckets.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		// Collect all matching objects first
		std::vector<std::pair<std::string, const ObjectData*>> matchingObjects;
		for (const auto& objectPair : bucketIter->second) {
			const std::string& objectName = objectPair.first;
			const ObjectData& objectData = objectPair.second;

			// Apply prefix filter
			if (!prefix.empty() && objectName.find(prefix) != 0) {
				continue;
			}

			matchingObjects.push_back({ objectName, &objectData });
		}

		// Sort objects by name for consistent pagination
		std::sort(matchingObjects.begin(), matchingObjects.end());

		// Find starting point for pagination
		size_t startIndex = 0;
		if (!marker.empty()) {
			for (size_t i = 0; i < matchingObjects.size(); i++) {
				if (matchingObjects[i].first > marker) {
					startIndex = i;
					break;
				}
			}
		} else if (!continuationToken.empty()) {
			// Simple continuation token implementation (just use the last object name)
			for (size_t i = 0; i < matchingObjects.size(); i++) {
				if (matchingObjects[i].first > continuationToken) {
					startIndex = i;
					break;
				}
			}
		}

		// Build list of objects for this page
		std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult>\n";
		xml += "<Name>" + bucket + "</Name>\n";
		xml += "<Prefix>" + prefix + "</Prefix>\n";
		xml += "<MaxKeys>" + std::to_string(maxKeys) + "</MaxKeys>\n";

		if (!marker.empty()) {
			xml += "<Marker>" + marker + "</Marker>\n";
		}

		int count = 0;
		std::string lastKey;
		size_t totalMatching = matchingObjects.size();

		for (size_t i = startIndex; i < matchingObjects.size() && count < maxKeys; i++) {
			const std::string& objectName = matchingObjects[i].first;
			const ObjectData* objectData = matchingObjects[i].second;

			xml += "<Contents>\n";
			xml += "<Key>" + objectName + "</Key>\n";
			xml += "<LastModified>" + std::to_string((int64_t)objectData->lastModified) + "</LastModified>\n";
			xml += "<ETag>" + objectData->etag + "</ETag>\n";
			xml += "<Size>" + std::to_string(objectData->content.size()) + "</Size>\n";
			xml += "<StorageClass>STANDARD</StorageClass>\n";
			xml += "</Contents>\n";

			lastKey = objectName;
			count++;
		}

		// Determine if there are more results
		bool isTruncated = (startIndex + count) < totalMatching;
		xml += "<IsTruncated>" + std::string(isTruncated ? "true" : "false") + "</IsTruncated>\n";

		if (isTruncated && !lastKey.empty()) {
			xml += "<NextMarker>" + lastKey + "</NextMarker>\n";
		}

		xml += "</ListBucketResult>";

		self->sendXMLResponse(response, 200, xml);

		TraceEvent("MockS3ListObjectsCompleted")
		    .detail("Bucket", bucket)
		    .detail("Prefix", prefix)
		    .detail("ObjectCount", count)
		    .detail("StartIndex", startIndex)
		    .detail("TotalMatching", totalMatching)
		    .detail("IsTruncated", isTruncated)
		    .detail("NextMarker", isTruncated ? lastKey : "");

		return Void();
	}

	// S3 Bucket Operations
	static Future<Void> handleHeadBucket(MockS3ServerImpl* self,
	                                     Reference<HTTP::IncomingRequest> req,
	                                     Reference<HTTP::OutgoingResponse> response,
	                                     std::string bucket) {

		TraceEvent("MockS3HeadBucket").detail("Bucket", bucket);

		// Ensure bucket exists in our storage (implicit creation like real S3)
		if (getGlobalStorage().buckets.find(bucket) == getGlobalStorage().buckets.end()) {
			getGlobalStorage().buckets[bucket] = std::map<std::string, ObjectData>();
		}

		response->code = 200;
		response->data.headers["Content-Type"] = "application/xml";
		response->data.contentLen = 0;
		response->data.content->discardAll(); // Clear existing content

		TraceEvent("MockS3BucketHead").detail("Bucket", bucket);

		return Void();
	}

	static Future<Void> handlePutBucket(MockS3ServerImpl* self,
	                                    Reference<HTTP::IncomingRequest> req,
	                                    Reference<HTTP::OutgoingResponse> response,
	                                    std::string bucket) {

		TraceEvent("MockS3PutBucket").detail("Bucket", bucket);

		// Ensure bucket exists in our storage (implicit creation)
		if (getGlobalStorage().buckets.find(bucket) == getGlobalStorage().buckets.end()) {
			getGlobalStorage().buckets[bucket] = std::map<std::string, ObjectData>();
		}

		response->code = 200;
		response->data.headers["Content-Type"] = "application/xml";
		response->data.contentLen = 0;
		response->data.content->discardAll(); // Clear existing content

		TraceEvent("MockS3BucketCreated").detail("Bucket", bucket);

		return Void();
	}

	// Utility Methods

	void sendError(Reference<HTTP::OutgoingResponse> response,
	               int code,
	               const std::string& errorCode,
	               const std::string& message) {

		TraceEvent("MockS3Error").detail("Code", code).detail("ErrorCode", errorCode).detail("Message", message);

		std::string xml = format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
		                         "<Error>\n"
		                         "  <Code>%s</Code>\n"
		                         "  <Message>%s</Message>\n"
		                         "</Error>",
		                         errorCode.c_str(),
		                         message.c_str());

		sendXMLResponse(response, code, xml);
	}

	void sendXMLResponse(Reference<HTTP::OutgoingResponse> response, int code, const std::string& xml) {
		TraceEvent("MockS3SendXMLResponse_Start")
		    .detail("Code", code)
		    .detail("XMLSize", xml.size())
		    .detail("XMLPreview", xml.size() > 0 ? xml.substr(0, std::min((size_t)50, xml.size())) : "EMPTY");

		response->code = code;
		response->data.headers["Content-Type"] = "application/xml";
		response->data.headers["Content-Length"] = std::to_string(xml.size());
		response->data.headers["Content-MD5"] = HTTP::computeMD5Sum(xml);

		// Actually put the XML content into the response
		if (xml.empty()) {
			response->data.contentLen = 0;
			TraceEvent("MockS3SendXMLResponse_Empty").detail("ResponseCode", response->code);
		} else {
			// Use the existing content queue instead of creating a new one
			// This prevents memory management issues and potential canBeSet() failures
			size_t contentSize = xml.size();
			response->data.contentLen = contentSize;

			// Clear any existing content and write the XML
			response->data.content->discardAll();
			PacketBuffer* buffer = response->data.content->getWriteBuffer(contentSize);
			PacketWriter pw(buffer, nullptr, Unversioned());
			pw.serializeBytes(xml);
			pw.finish();
		}

		TraceEvent("MockS3SendXMLResponse_Complete")
		    .detail("FinalCode", response->code)
		    .detail("FinalContentLen", response->data.contentLen)
		    .detail("XMLSize", xml.size());
	}

	std::map<std::string, std::string> parseTagsXML(const std::string& xml) {
		std::map<std::string, std::string> tags;

		// Simplified XML parsing for tags - this would need a proper XML parser in production
		std::regex tagRegex("<Tag><Key>([^<]+)</Key><Value>([^<]*)</Value></Tag>");
		std::sregex_iterator iter(xml.begin(), xml.end(), tagRegex);
		std::sregex_iterator end;

		for (; iter != end; ++iter) {
			std::string key = iter->str(1);
			std::string value = iter->str(2);
			tags[key] = value;

			TraceEvent("MockS3ParsedTag").detail("Key", key).detail("Value", value);
		}

		return tags;
	}

	std::string generateTagsXML(const std::map<std::string, std::string>& tags) {
		std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Tagging><TagSet>";

		for (const auto& tag : tags) {
			xml += "<Tag><Key>" + tag.first + "</Key><Value>" + tag.second + "</Value></Tag>";
		}

		xml += "</TagSet></Tagging>";
		return xml;
	}
};

// Global registry to track registered servers and avoid conflicts
static std::map<std::string, bool> registeredServers;

// Clear global storage state for clean test runs
static void clearSingletonState() {
	getGlobalStorage().buckets.clear();
	getGlobalStorage().multipartUploads.clear();
	TraceEvent("MockS3ServerImpl_StateCleared");
}

// Request Handler Implementation - Each handler instance works with global storage
Future<Void> MockS3RequestHandler::handleRequest(Reference<HTTP::IncomingRequest> req,
                                                 Reference<HTTP::OutgoingResponse> response) {
	// Guard against calling virtual functions during destruction
	if (destructing.load()) {
		TraceEvent(SevWarn, "MockS3RequestHandler_DestructingGuard")
		    .detail("Method", "handleRequest")
		    .detail("Resource", req->resource);
		return Void();
	}

	// Create a temporary instance just to use its static handleRequest method
	// All actual storage is in g_mockS3Storage which is truly global
	static MockS3ServerImpl serverInstance;
	return MockS3ServerImpl::handleRequest(&serverInstance, req, response);
}

Reference<HTTP::IRequestHandler> MockS3RequestHandler::clone() {
	// Guard against calling virtual functions during destruction
	if (destructing.load()) {
		TraceEvent(SevWarn, "MockS3RequestHandler_DestructingGuard").detail("Method", "clone");
		// Return nullptr - caller must handle this gracefully
		return Reference<HTTP::IRequestHandler>();
	}
	return makeReference<MockS3RequestHandler>();
}

// Safe server registration that prevents conflicts (internal implementation)
ACTOR Future<Void> registerMockS3Server_impl(std::string ip, std::string port) {
	state std::string serverKey = ip + ":" + port; // State variable before any early returns

	// DIAGNOSTIC: Enhanced registration logging
	TraceEvent("MockS3ServerDiagnostic")
	    .detail("Phase", "Registration Start")
	    .detail("IP", ip)
	    .detail("Port", port)
	    .detail("ServerKey", serverKey)
	    .detail("IsSimulated", g_network->isSimulated())
	    .detail("AlreadyRegistered", registeredServers.count(serverKey) > 0);

	// Check if server is already registered
	if (registeredServers.count(serverKey)) {
		TraceEvent(SevWarn, "MockS3ServerAlreadyRegistered").detail("Address", serverKey);
		return Void();
	}

	try {
		TraceEvent("MockS3ServerDiagnostic")
		    .detail("Phase", "Calling registerSimHTTPServer")
		    .detail("Address", serverKey);

		wait(g_simulator->registerSimHTTPServer(ip, port, makeReference<MockS3RequestHandler>()));
		registeredServers[serverKey] = true;

		// Enable persistence automatically for all MockS3 instances
		// This ensures all tests using MockS3 get persistence enabled
		if (!getGlobalStorage().persistenceEnabled) {
			std::string persistenceDir = "simfdb/mocks3";
			enableMockS3Persistence(persistenceDir);
			TraceEvent("MockS3ServerPersistenceEnabled")
			    .detail("Address", serverKey)
			    .detail("PersistenceDir", persistenceDir);

			// Load any previously persisted state (for crash recovery in simulation)
			wait(loadMockS3PersistedStateFuture());
		}

		TraceEvent("MockS3ServerRegistered").detail("Address", serverKey).detail("Success", true);

		TraceEvent("MockS3ServerDiagnostic")
		    .detail("Phase", "Registration Complete")
		    .detail("Address", serverKey)
		    .detail("TotalRegistered", registeredServers.size());
	} catch (Error& e) {
		TraceEvent(SevError, "MockS3ServerRegistrationFailed")
		    .error(e)
		    .detail("Address", serverKey)
		    .detail("ErrorCode", e.code())
		    .detail("ErrorName", e.name());
		throw;
	}

	return Void();
}

// Public Interface Implementation
ACTOR Future<Void> startMockS3Server(NetworkAddress listenAddress) {
	TraceEvent("MockS3ServerStarting").detail("ListenAddress", listenAddress.toString());

	try {
		TraceEvent("MockS3ServerRegistering")
		    .detail("IP", listenAddress.ip.toString())
		    .detail("Port", std::to_string(listenAddress.port))
		    .detail("IsSimulated", g_network->isSimulated());

		// Persistence is automatically enabled in registerMockS3Server_impl()
		wait(registerMockS3Server_impl(listenAddress.ip.toString(), std::to_string(listenAddress.port)));

		TraceEvent("MockS3ServerStarted")
		    .detail("ListenAddress", listenAddress.toString())
		    .detail("HandlerCreated", true);

	} catch (Error& e) {
		TraceEvent(SevError, "MockS3ServerStartError").error(e).detail("ListenAddress", listenAddress.toString());
		throw;
	}

	return Void();
}

// Clear all MockS3 global storage - called at the start of each simulation test
void clearMockS3Storage() {
	getGlobalStorage().clearStorage();
}

// Enable persistence for MockS3 storage
void enableMockS3Persistence(const std::string& persistenceDir) {
	getGlobalStorage().enablePersistence(persistenceDir);
	TraceEvent("MockS3PersistenceConfigured").detail("Directory", persistenceDir);
}

// ACTOR: Load persisted objects from disk
ACTOR static Future<Void> loadPersistedObjects(std::string persistenceDir) {
	state std::string objectsDir = persistenceDir + "/objects"; // State variable before any early returns

	if (!fileExists(objectsDir)) {
		TraceEvent("MockS3LoadObjects").detail("Status", "NoObjectsDir");
		return Void();
	}

	try {
		// Get list of bucket directories
		state std::vector<std::string> buckets = platform::listFiles(objectsDir, "");
		// Sort for deterministic load order (platform::listFiles returns OS-dependent order)
		std::sort(buckets.begin(), buckets.end());
		state int bucketIdx = 0;

		for (bucketIdx = 0; bucketIdx < buckets.size(); bucketIdx++) {
			state std::string bucket = buckets[bucketIdx];
			if (bucket == "." || bucket == "..")
				continue;

			state std::string bucketDir = objectsDir + "/" + bucket;
			if (!directoryExists(bucketDir))
				continue;

			// Get all files in the bucket directory (including nested paths)
			state std::vector<std::string> files = platform::listFiles(bucketDir, "");
			std::sort(files.begin(), files.end()); // Deterministic order
			state int fileIdx = 0;

			for (fileIdx = 0; fileIdx < files.size(); fileIdx++) {
				state std::string fileName = files[fileIdx];

				// Look for .meta.json files to identify objects
				if (fileName.size() > 10 && fileName.substr(fileName.size() - 10) == ".meta.json") {
					// Extract object name by removing .meta.json suffix
					state std::string objectName = fileName.substr(0, fileName.size() - 10);
					state std::string dataPath = bucketDir + "/" + objectName + ".data";
					state std::string metaPath = bucketDir + "/" + fileName;

					if (!fileExists(dataPath)) {
						TraceEvent(SevWarn, "MockS3LoadObjectSkipped")
						    .detail("Bucket", bucket)
						    .detail("Object", objectName)
						    .detail("Reason", "NoDataFile");
						continue;
					}

					// Read object content and metadata
					state std::string content = wait(readFileContent(dataPath));
					state std::string metaJson = wait(readFileContent(metaPath));

					// Parse metadata using rapidjson
					MockS3GlobalStorage::ObjectData obj(content);
					deserializeObjectMeta(metaJson, obj);
					getGlobalStorage().buckets[bucket][objectName] = std::move(obj);

					TraceEvent("MockS3ObjectRestored")
					    .detail("Bucket", bucket)
					    .detail("Object", objectName)
					    .detail("Size", content.size());
				}
			}
		}

		TraceEvent("MockS3ObjectsLoaded").detail("BucketsCount", getGlobalStorage().buckets.size());
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3LoadObjectsFailed").error(e);
	}

	return Void();
}

// ACTOR: Load persisted multipart uploads from disk
ACTOR static Future<Void> loadPersistedMultipartUploads(std::string persistenceDir) {
	state std::string multipartDir = persistenceDir + "/multipart"; // State variable before any early returns

	if (!fileExists(multipartDir)) {
		TraceEvent("MockS3LoadMultipart").detail("Status", "NoMultipartDir");
		return Void();
	}

	try {
		// Get all files in multipart directory
		state std::vector<std::string> files = platform::listFiles(multipartDir, "");
		std::sort(files.begin(), files.end()); // Deterministic order
		state int fileIdx = 0;

		for (fileIdx = 0; fileIdx < files.size(); fileIdx++) {
			state std::string fileName = files[fileIdx];

			// Look for .state.json files
			if (fileName.size() > 11 && fileName.substr(fileName.size() - 11) == ".state.json") {
				state std::string uploadId = fileName.substr(0, fileName.size() - 11);
				state std::string statePath = multipartDir + "/" + fileName;

				// Read state file
				state std::string stateJson = wait(readFileContent(statePath));
				if (stateJson.empty()) {
					TraceEvent(SevWarn, "MockS3LoadMultipartSkipped")
					    .detail("UploadId", uploadId)
					    .detail("Reason", "EmptyStateFile");
					continue;
				}

				// Parse multipart upload state using rapidjson
				state MockS3GlobalStorage::MultipartUpload upload("", "");
				upload.uploadId = uploadId;
				deserializeMultipartState(stateJson, upload);

				// Load all parts for this upload
				state int partNum = 1;
				state int maxAttempts = 10000; // Reasonable limit
				for (partNum = 1; partNum <= maxAttempts; partNum++) {
					state std::string partPath = multipartDir + "/" + uploadId + ".part." + std::to_string(partNum);
					state std::string partMetaPath = partPath + ".meta.json";

					if (!fileExists(partPath) || !fileExists(partMetaPath))
						break; // No more parts

					// Read part data and metadata
					state std::string partData = wait(readFileContent(partPath));
					state std::string partMetaJson = wait(readFileContent(partMetaPath));

					// Parse part metadata using rapidjson
					using namespace rapidjson;
					Document doc;
					doc.Parse(partMetaJson.c_str());
					std::string etag = doc.HasMember("etag") && doc["etag"].IsString() ? doc["etag"].GetString() : "";
					upload.parts[partNum] = { etag, partData };

					TraceEvent("MockS3MultipartPartRestored")
					    .detail("UploadId", uploadId)
					    .detail("PartNumber", partNum)
					    .detail("Size", partData.size());
				}

				// Store the restored upload
				TraceEvent("MockS3MultipartUploadRestored")
				    .detail("UploadId", uploadId)
				    .detail("Bucket", upload.bucket)
				    .detail("Object", upload.object)
				    .detail("PartsCount", upload.parts.size());

				getGlobalStorage().multipartUploads[uploadId] = std::move(upload);
			}
		}

		TraceEvent("MockS3MultipartUploadsLoaded").detail("UploadsCount", getGlobalStorage().multipartUploads.size());
	} catch (Error& e) {
		TraceEvent(SevWarn, "MockS3LoadMultipartFailed").error(e);
	}

	return Void();
}

// ACTOR: Load all persisted state from disk
ACTOR static Future<Void> loadMockS3PersistedStateImpl() {
	state std::string persistenceDir; // Declare state before any early returns

	if (!getGlobalStorage().persistenceEnabled || getGlobalStorage().persistenceLoaded) {
		return Void();
	}

	persistenceDir = getGlobalStorage().persistenceDir;
	TraceEvent("MockS3LoadPersistedStateStart").detail("PersistenceDir", persistenceDir);

	try {
		// Load objects
		wait(loadPersistedObjects(persistenceDir));

		// Load multipart uploads
		wait(loadPersistedMultipartUploads(persistenceDir));

		getGlobalStorage().persistenceLoaded = true;

		TraceEvent("MockS3LoadPersistedStateComplete")
		    .detail("ObjectsCount", getGlobalStorage().buckets.size())
		    .detail("MultipartUploadsCount", getGlobalStorage().multipartUploads.size());
	} catch (Error& e) {
		TraceEvent(SevError, "MockS3LoadPersistedStateFailed").error(e);
		throw;
	}

	return Void();
}

// Load persisted state from disk (called at server startup) - returns Future for use in ACTOR context
Future<Void> loadMockS3PersistedStateFuture() {
	if (getGlobalStorage().persistenceEnabled && !getGlobalStorage().persistenceLoaded) {
		return loadMockS3PersistedStateImpl();
	}
	return Void();
}

// Unit Tests for MockS3Server
TEST_CASE("/MockS3Server/parseS3Request/ValidBucketParameter") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket?region=us-east-1";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket");
	ASSERT(object == "");
	ASSERT(queryParams["region"] == "us-east-1");

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/MissingBucketParameter") {

	MockS3ServerImpl server;
	std::string resource = "/?region=us-east-1"; // Empty path - no bucket
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	try {
		server.parseS3Request(resource, bucket, object, queryParams);
		ASSERT(false); // Should not reach here
	} catch (Error& e) {
		ASSERT(e.code() == error_code_backup_invalid_url);
	}

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/EmptyQueryString") {

	MockS3ServerImpl server;
	std::string resource = "/"; // Empty path - no bucket
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	try {
		server.parseS3Request(resource, bucket, object, queryParams);
		ASSERT(false); // Should not reach here
	} catch (Error& e) {
		ASSERT(e.code() == error_code_backup_invalid_url);
	}

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/BucketParameterOverride") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket/testobject?region=us-east-1";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket"); // Should use path (like real S3)
	ASSERT(object == "testobject");
	ASSERT(queryParams["region"] == "us-east-1");

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/ComplexPath") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket/folder/subfolder/file.txt?region=us-east-1";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket"); // Should use path (like real S3)
	ASSERT(object == "folder/subfolder/file.txt");
	ASSERT(queryParams["region"] == "us-east-1");

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/URLEncodedParameters") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket?region=us-east-1&param=value%3Dtest";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket"); // Path components are not URL decoded (as per S3 spec)
	ASSERT(queryParams["region"] == "us-east-1");
	ASSERT(queryParams["param"] == "value=test"); // Query parameters ARE URL decoded

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/EmptyPath") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket?region=us-east-1";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket");
	ASSERT(object == "");
	ASSERT(queryParams["region"] == "us-east-1");

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/OnlyBucketInPath") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket?region=us-east-1";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket"); // Should use path (like real S3)
	ASSERT(object == "");
	ASSERT(queryParams["region"] == "us-east-1");

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/MultipleParameters") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket?region=us-east-1&version=1&encoding=utf8";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket");
	ASSERT(queryParams["region"] == "us-east-1");
	ASSERT(queryParams["version"] == "1");
	ASSERT(queryParams["encoding"] == "utf8");
	ASSERT(queryParams.size() == 3);

	return Void();
}

TEST_CASE("/MockS3Server/parseS3Request/ParametersWithoutValues") {

	MockS3ServerImpl server;
	std::string resource = "/testbucket?flag&region=us-east-1";
	std::string bucket, object;
	std::map<std::string, std::string> queryParams;

	server.parseS3Request(resource, bucket, object, queryParams);

	ASSERT(bucket == "testbucket");
	ASSERT(queryParams["flag"] == ""); // Parameter without value should be empty string
	ASSERT(queryParams["region"] == "us-east-1");

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/SimpleByteRange") {
	std::string rangeHeader = "bytes=0-99";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == true);
	ASSERT(rangeStart == 0);
	ASSERT(rangeEnd == 99);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/MiddleRange") {
	std::string rangeHeader = "bytes=100-199";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == true);
	ASSERT(rangeStart == 100);
	ASSERT(rangeEnd == 199);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/LargeOffsets") {
	std::string rangeHeader = "bytes=1000000-1999999";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == true);
	ASSERT(rangeStart == 1000000);
	ASSERT(rangeEnd == 1999999);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/InvalidFormat") {
	std::string rangeHeader = "invalid-range";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == false);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/MissingBytesPrefix") {
	std::string rangeHeader = "0-99";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == false);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/MissingDash") {
	std::string rangeHeader = "bytes=0";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == false);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/EmptyString") {
	std::string rangeHeader = "";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	ASSERT(result == false);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/NegativeStart") {
	std::string rangeHeader = "bytes=-100-200";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	// Suffix-byte-range-spec (last N bytes) is not currently supported
	ASSERT(result == false);

	return Void();
}

TEST_CASE("/MockS3Server/RangeHeader/StartGreaterThanEnd") {
	std::string rangeHeader = "bytes=200-100";
	int64_t rangeStart, rangeEnd;

	bool result = MockS3ServerImpl::parseRangeHeader(rangeHeader, rangeStart, rangeEnd);

	// Parser accepts this, but semantic validation happens in handleGetObject
	ASSERT(result == true);
	ASSERT(rangeStart == 200);
	ASSERT(rangeEnd == 100);

	return Void();
}

// Real HTTP Server Implementation for ctests
ACTOR Future<Void> startMockS3ServerReal_impl(NetworkAddress listenAddress, std::string persistenceDir) {
	TraceEvent("MockS3ServerRealStarting").detail("ListenAddress", listenAddress.toString());

	// Enable persistence for standalone MockS3Server
	if (!getGlobalStorage().persistenceEnabled) {
		// Use provided persistence directory or default to "simfdb/mocks3"
		if (persistenceDir.empty()) {
			persistenceDir = "simfdb/mocks3";
		}
		enableMockS3Persistence(persistenceDir);
		TraceEvent("MockS3ServerRealPersistenceEnabled")
		    .detail("ListenAddress", listenAddress.toString())
		    .detail("PersistenceDir", persistenceDir);

		// Load any previously persisted state (for crash recovery)
		wait(loadMockS3PersistedStateFuture());
	}

	state Reference<HTTP::SimServerContext> server = makeReference<HTTP::SimServerContext>();
	server->registerNewServer(listenAddress, makeReference<MockS3RequestHandler>());

	TraceEvent("MockS3ServerRealStarted")
	    .detail("ListenAddress", listenAddress.toString())
	    .detail("ServerPtr", format("%p", server.getPtr()));

	// Keep the server running indefinitely
	wait(Never());
	return Void();
}

Future<Void> startMockS3ServerReal(const NetworkAddress& listenAddress, const std::string& persistenceDir) {
	return startMockS3ServerReal_impl(listenAddress, persistenceDir);
}

// Wrapper for registerMockS3Server (calls the ACTOR implementation)
Future<Void> registerMockS3Server(std::string ip, std::string port) {
	return registerMockS3Server_impl(ip, port);
}
