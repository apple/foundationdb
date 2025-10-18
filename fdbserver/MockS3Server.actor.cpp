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
#include "flow/ActorCollection.h"
#include "flow/IRandom.h"
#include "flow/serialize.h"

#include <string>
#include <map>
#include <unordered_map>
#include <sstream>
#include <iomanip>
#include <regex>
#include <utility>
#include <iostream>

#include "flow/actorcompiler.h" // This must be the last #include.

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
};

// Accessor function - uses function-local static for lazy initialization
// In simulation, this static is shared across all simulated processes (same OS thread)
static MockS3GlobalStorage& getGlobalStorage() {
	static MockS3GlobalStorage storage;
	return storage;
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
	static Future<Void> handleMultipartStart(MockS3ServerImpl* self,
	                                         Reference<HTTP::IncomingRequest> req,
	                                         Reference<HTTP::OutgoingResponse> response,
	                                         std::string bucket,
	                                         std::string object) {

		TraceEvent("MockS3MultipartStart").detail("Bucket", bucket).detail("Object", object);

		// Create multipart upload
		MultipartUpload upload(bucket, object);
		std::string uploadId = upload.uploadId;
		getGlobalStorage().multipartUploads[uploadId] = std::move(upload);

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

	static Future<Void> handleUploadPart(MockS3ServerImpl* self,
	                                     Reference<HTTP::IncomingRequest> req,
	                                     Reference<HTTP::OutgoingResponse> response,
	                                     std::string bucket,
	                                     std::string object,
	                                     std::map<std::string, std::string> queryParams) {

		std::string uploadId = queryParams.at("uploadId");
		int partNumber = std::stoi(queryParams.at("partNumber"));

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
		std::string etag = ObjectData::generateETag(req->data.content);
		uploadIter->second.parts[partNumber] = { etag, req->data.content };

		// Return ETag in response
		response->code = 200;
		response->data.headers["ETag"] = etag;
		response->data.contentLen = 0;
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

		TraceEvent("MockS3PartUploaded")
		    .detail("UploadId", uploadId)
		    .detail("PartNumber", partNumber)
		    .detail("ETag", etag);

		return Void();
	}

	static Future<Void> handleMultipartComplete(MockS3ServerImpl* self,
	                                            Reference<HTTP::IncomingRequest> req,
	                                            Reference<HTTP::OutgoingResponse> response,
	                                            std::string bucket,
	                                            std::string object,
	                                            std::map<std::string, std::string> queryParams) {

		std::string uploadId = queryParams.at("uploadId");

		TraceEvent("MockS3MultipartComplete").detail("UploadId", uploadId);

		auto uploadIter = getGlobalStorage().multipartUploads.find(uploadId);
		if (uploadIter == getGlobalStorage().multipartUploads.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchUpload", "Upload not found");
			return Void();
		}

		// Combine all parts in order
		std::string combinedContent;
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

		// Clean up multipart upload
		getGlobalStorage().multipartUploads.erase(uploadId);

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

	static Future<Void> handleMultipartAbort(MockS3ServerImpl* self,
	                                         Reference<HTTP::IncomingRequest> req,
	                                         Reference<HTTP::OutgoingResponse> response,
	                                         std::string bucket,
	                                         std::string object,
	                                         std::map<std::string, std::string> queryParams) {

		std::string uploadId = queryParams.at("uploadId");

		TraceEvent("MockS3MultipartAbort").detail("UploadId", uploadId);

		auto uploadIter = getGlobalStorage().multipartUploads.find(uploadId);
		if (uploadIter == getGlobalStorage().multipartUploads.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchUpload", "Upload not found");
			return Void();
		}

		// Remove multipart upload
		getGlobalStorage().multipartUploads.erase(uploadId);

		response->code = 204; // No Content
		response->data.contentLen = 0;
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

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
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

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
	static Future<Void> handlePutObject(MockS3ServerImpl* self,
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
		std::string etag = obj.etag;
		getGlobalStorage().buckets[bucket][object] = std::move(obj);

		TraceEvent("MockS3PutObject_Stored")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ETag", etag)
		    .detail("StoredSize", getGlobalStorage().buckets[bucket][object].content.size());

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
		response->data.content = new UnsentPacketQueue();

		if (!responseContent.empty()) {
			// Use the correct approach: getWriteBuffer from the UnsentPacketQueue
			PacketBuffer* buffer = response->data.content->getWriteBuffer(responseContent.size());
			PacketWriter pw(buffer, nullptr, Unversioned());
			pw.serializeBytes(responseContent);
			pw.finish();
		}

		return Void();
	}

	static Future<Void> handleDeleteObject(MockS3ServerImpl* self,
	                                       Reference<HTTP::IncomingRequest> req,
	                                       Reference<HTTP::OutgoingResponse> response,
	                                       std::string bucket,
	                                       std::string object) {

		TraceEvent("MockS3DeleteObject").detail("Bucket", bucket).detail("Object", object);

		auto bucketIter = getGlobalStorage().buckets.find(bucket);
		if (bucketIter != getGlobalStorage().buckets.end()) {
			bucketIter->second.erase(object);
		}

		response->code = 204; // No Content
		response->data.contentLen = 0;
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

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
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

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
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

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
			// Use PacketWriter to properly populate the content
			// The previous approach created an empty UnsentPacketQueue, causing memory corruption
			size_t contentSize = xml.size();

			response->data.content = new UnsentPacketQueue();
			response->data.contentLen = contentSize;

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
	// Create a temporary instance just to use its static handleRequest method
	// All actual storage is in g_mockS3Storage which is truly global
	static MockS3ServerImpl serverInstance;
	return MockS3ServerImpl::handleRequest(&serverInstance, req, response);
}

Reference<HTTP::IRequestHandler> MockS3RequestHandler::clone() {
	// Prevent cloning during destruction to avoid "Pure virtual function called!" errors
	if (destructing) {
		TraceEvent(SevWarn, "MockS3RequestHandlerCloneDuringDestruction");
		return Reference<HTTP::IRequestHandler>();
	}
	return makeReference<MockS3RequestHandler>();
}

// Safe server registration that prevents conflicts
ACTOR Future<Void> registerMockS3Server(std::string ip, std::string port) {
	state std::string serverKey = ip + ":" + port;

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

		wait(registerMockS3Server(listenAddress.ip.toString(), std::to_string(listenAddress.port)));

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
ACTOR Future<Void> startMockS3ServerReal_impl(NetworkAddress listenAddress) {
	TraceEvent("MockS3ServerRealStarting").detail("ListenAddress", listenAddress.toString());

	state Reference<HTTP::SimServerContext> server = makeReference<HTTP::SimServerContext>();
	server->registerNewServer(listenAddress, makeReference<MockS3RequestHandler>());

	TraceEvent("MockS3ServerRealStarted")
	    .detail("ListenAddress", listenAddress.toString())
	    .detail("ServerPtr", format("%p", server.getPtr()));

	// Keep the server running indefinitely
	wait(Never());
	return Void();
}

Future<Void> startMockS3ServerReal(const NetworkAddress& listenAddress) {
	return startMockS3ServerReal_impl(listenAddress);
}
