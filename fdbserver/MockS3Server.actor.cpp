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
#include <mutex>
#include <iostream>

#include "flow/actorcompiler.h" // This must be the last #include.

// Mock S3 Server Implementation for deterministic testing
class MockS3ServerImpl {
public:
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

	// Storage
	std::map<std::string, std::map<std::string, ObjectData>> buckets;
	std::map<std::string, MultipartUpload> multipartUploads;

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

		// Parse path: /bucket/object
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
				queryParams[key] = urlDecode(value);
			}
		}

		TraceEvent("MockS3ParsedPath")
		    .detail("OriginalResource", resource)
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("QueryString", query);
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
		self->multipartUploads[uploadId] = std::move(upload);

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

		auto uploadIter = self->multipartUploads.find(uploadId);
		if (uploadIter == self->multipartUploads.end()) {
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

		auto uploadIter = self->multipartUploads.find(uploadId);
		if (uploadIter == self->multipartUploads.end()) {
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
		self->buckets[bucket][object] = std::move(obj);

		TraceEvent("MockS3MultipartFinalObject")
		    .detail("UploadId", uploadId)
		    .detail("StoredSize", self->buckets[bucket][object].content.size())
		    .detail("StoredPreview",
		            self->buckets[bucket][object].content.size() > 0
		                ? self->buckets[bucket][object].content.substr(
		                      0, std::min((size_t)20, self->buckets[bucket][object].content.size()))
		                : "EMPTY");

		// Clean up multipart upload
		self->multipartUploads.erase(uploadId);

		// Generate completion XML response
		std::string xml = format("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
		                         "<CompleteMultipartUploadResult>\n"
		                         "  <Bucket>%s</Bucket>\n"
		                         "  <Key>%s</Key>\n"
		                         "  <ETag>%s</ETag>\n"
		                         "</CompleteMultipartUploadResult>",
		                         bucket.c_str(),
		                         object.c_str(),
		                         self->buckets[bucket][object].etag.c_str());

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

		auto uploadIter = self->multipartUploads.find(uploadId);
		if (uploadIter == self->multipartUploads.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchUpload", "Upload not found");
			return Void();
		}

		// Remove multipart upload
		self->multipartUploads.erase(uploadId);

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

		auto bucketIter = self->buckets.find(bucket);
		if (bucketIter == self->buckets.end()) {
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

		auto bucketIter = self->buckets.find(bucket);
		if (bucketIter == self->buckets.end()) {
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

		TraceEvent("MockS3PutObject")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ContentLength", req->data.contentLen);

		// Create object
		ObjectData obj(req->data.content);
		self->buckets[bucket][object] = std::move(obj);

		response->code = 200;
		response->data.headers["ETag"] = self->buckets[bucket][object].etag;
		response->data.contentLen = 0;
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

		TraceEvent("MockS3ObjectStored")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ETag", self->buckets[bucket][object].etag);

		return Void();
	}

	static Future<Void> handleGetObject(MockS3ServerImpl* self,
	                                    Reference<HTTP::IncomingRequest> req,
	                                    Reference<HTTP::OutgoingResponse> response,
	                                    std::string bucket,
	                                    std::string object) {

		TraceEvent("MockS3GetObject").detail("Bucket", bucket).detail("Object", object);

		auto bucketIter = self->buckets.find(bucket);
		if (bucketIter == self->buckets.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		auto objectIter = bucketIter->second.find(object);
		if (objectIter == bucketIter->second.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchKey", "Object not found");
			return Void();
		}

		response->code = 200;
		response->data.headers["ETag"] = objectIter->second.etag;
		response->data.headers["Content-Type"] = "binary/octet-stream";
		response->data.headers["Content-MD5"] = HTTP::computeMD5Sum(objectIter->second.content);

		// Write content to response - CRITICAL FIX: Avoid PacketWriter to prevent malloc corruption
		TraceEvent("MockS3GetObjectWriting")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("ContentSize", objectIter->second.content.size());

		if (objectIter->second.content.empty()) {
			response->data.contentLen = 0;
		} else {
			// CORRUPTION FIX: Use PacketWriter with generous buffer allocation
			size_t contentSize = objectIter->second.content.size();
			size_t bufferSize = contentSize + 1024; // Generous padding to prevent overflow

			response->data.content = new UnsentPacketQueue();
			PacketBuffer* buffer = response->data.content->getWriteBuffer(bufferSize);
			PacketWriter pw(buffer, nullptr, Unversioned());

			TraceEvent("MockS3GetObject_SafePacketWriter")
			    .detail("ContentSize", contentSize)
			    .detail("BufferSize", bufferSize)
			    .detail("BufferPtr", format("%p", buffer))
			    .detail("ResponseCode", response->code);

			pw.serializeBytes(objectIter->second.content);
			pw.finish(); // CRITICAL: Finalize PacketWriter to make content available
			response->data.contentLen = contentSize;
			response->data.headers["Content-Length"] = std::to_string(contentSize);
		}

		TraceEvent("MockS3ObjectRetrieved").detail("Bucket", bucket).detail("Object", object);

		return Void();
	}

	static Future<Void> handleDeleteObject(MockS3ServerImpl* self,
	                                       Reference<HTTP::IncomingRequest> req,
	                                       Reference<HTTP::OutgoingResponse> response,
	                                       std::string bucket,
	                                       std::string object) {

		TraceEvent("MockS3DeleteObject").detail("Bucket", bucket).detail("Object", object);

		auto bucketIter = self->buckets.find(bucket);
		if (bucketIter != self->buckets.end()) {
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

		TraceEvent("MockS3HeadObject").detail("Bucket", bucket).detail("Object", object);

		auto bucketIter = self->buckets.find(bucket);
		if (bucketIter == self->buckets.end()) {
			TraceEvent("MockS3HeadObjectNoBucket")
			    .detail("Bucket", bucket)
			    .detail("Object", object)
			    .detail("AvailableBuckets", self->buckets.size());
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

		TraceEvent("MockS3HeadObjectFound")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("Size", obj.content.size())
		    .detail("Preview",
		            obj.content.size() > 0 ? obj.content.substr(0, std::min((size_t)20, obj.content.size())) : "EMPTY");

		response->code = 200;
		response->data.headers["ETag"] = obj.etag;
		response->data.headers["Content-Length"] = std::to_string(obj.content.size());
		response->data.headers["Content-Type"] = "binary/octet-stream";
		// CRITICAL FIX: HEAD requests need contentLen set to actual size for headers
		response->data.contentLen = obj.content.size(); // This controls ResponseContentSize in HTTP logs

		TraceEvent("MockS3ObjectHead")
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("Size", obj.content.size());

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
		int maxKeys = queryParams.count("max-keys") ? std::stoi(queryParams.at("max-keys")) : 1000;

		TraceEvent("MockS3ListObjectsDebug")
		    .detail("Bucket", bucket)
		    .detail("Prefix", prefix)
		    .detail("Delimiter", delimiter)
		    .detail("MaxKeys", maxKeys);

		// Find bucket
		auto bucketIter = self->buckets.find(bucket);
		if (bucketIter == self->buckets.end()) {
			self->sendError(response, HTTP::HTTP_STATUS_CODE_NOT_FOUND, "NoSuchBucket", "Bucket not found");
			return Void();
		}

		// Build list of matching objects
		std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult>\n";
		xml += "<Name>" + bucket + "</Name>\n";
		xml += "<Prefix>" + prefix + "</Prefix>\n";
		xml += "<MaxKeys>" + std::to_string(maxKeys) + "</MaxKeys>\n";
		xml += "<IsTruncated>false</IsTruncated>\n";

		int count = 0;
		for (const auto& objectPair : bucketIter->second) {
			const std::string& objectName = objectPair.first;
			const ObjectData& objectData = objectPair.second;

			// Apply prefix filter
			if (!prefix.empty() && objectName.find(prefix) != 0) {
				continue;
			}

			// Apply max-keys limit
			if (count >= maxKeys) {
				break;
			}

			xml += "<Contents>\n";
			xml += "<Key>" + objectName + "</Key>\n";
			xml += "<LastModified>" + std::to_string((int64_t)objectData.lastModified) + "</LastModified>\n";
			xml += "<ETag>" + objectData.etag + "</ETag>\n";
			xml += "<Size>" + std::to_string(objectData.content.size()) + "</Size>\n";
			xml += "<StorageClass>STANDARD</StorageClass>\n";
			xml += "</Contents>\n";

			count++;
		}

		xml += "</ListBucketResult>";

		self->sendXMLResponse(response, 200, xml);

		TraceEvent("MockS3ListObjectsCompleted")
		    .detail("Bucket", bucket)
		    .detail("Prefix", prefix)
		    .detail("ObjectCount", count);

		return Void();
	}

	// S3 Bucket Operations
	static Future<Void> handleHeadBucket(MockS3ServerImpl* self,
	                                     Reference<HTTP::IncomingRequest> req,
	                                     Reference<HTTP::OutgoingResponse> response,
	                                     std::string bucket) {

		TraceEvent("MockS3HeadBucket").detail("Bucket", bucket);

		// Ensure bucket exists in our storage (implicit creation like real S3)
		if (self->buckets.find(bucket) == self->buckets.end()) {
			self->buckets[bucket] = std::map<std::string, ObjectData>();
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
		if (self->buckets.find(bucket) == self->buckets.end()) {
			self->buckets[bucket] = std::map<std::string, ObjectData>();
		}

		response->code = 200;
		response->data.headers["Content-Type"] = "application/xml";
		response->data.contentLen = 0;
		response->data.content = new UnsentPacketQueue(); // Required for HTTP header transmission

		TraceEvent("MockS3BucketCreated").detail("Bucket", bucket);

		return Void();
	}

	// Utility Methods
	static std::string urlDecode(const std::string& encoded) {
		std::string decoded;
		for (size_t i = 0; i < encoded.length(); ++i) {
			if (encoded[i] == '%' && i + 2 < encoded.length()) {
				int value;
				std::istringstream is(encoded.substr(i + 1, 2));
				if (is >> std::hex >> value) {
					decoded += static_cast<char>(value);
					i += 2;
				} else {
					decoded += encoded[i];
				}
			} else if (encoded[i] == '+') {
				decoded += ' ';
			} else {
				decoded += encoded[i];
			}
		}
		return decoded;
	}

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

		// CORRUPTION FIX: Use PacketWriter with generous buffer allocation
		if (xml.empty()) {
			response->data.contentLen = 0;
			TraceEvent("MockS3SendXMLResponse_Empty").detail("ResponseCode", response->code);
		} else {
			// Use PacketWriter with generous buffer to prevent heap corruption
			size_t contentSize = xml.size();
			size_t bufferSize = contentSize + 1024; // Generous padding to prevent overflow

			response->data.content = new UnsentPacketQueue();
			PacketBuffer* buffer = response->data.content->getWriteBuffer(bufferSize);
			PacketWriter pw(buffer, nullptr, Unversioned());

			TraceEvent("MockS3SendXMLResponse_SafePacketWriter")
			    .detail("ContentSize", contentSize)
			    .detail("BufferSize", bufferSize)
			    .detail("BufferPtr", format("%p", buffer))
			    .detail("ResponseCode", response->code)
			    .detail("XMLPreview", xml.substr(0, std::min((size_t)50, xml.size())));

			pw.serializeBytes(xml);
			pw.finish(); // CRITICAL: Finalize PacketWriter to make content available
			response->data.contentLen = contentSize; // Set to actual content size
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

// Thread-safe singleton storage that avoids destruction order issues
// Use a static local variable to ensure it's never destroyed
static MockS3ServerImpl& getSingletonInstance() {
	static MockS3ServerImpl instance;
	return instance;
}

// Clear singleton state for clean test runs
static void clearSingletonState() {
	MockS3ServerImpl& instance = getSingletonInstance();
	instance.buckets.clear();
	instance.multipartUploads.clear();
	TraceEvent("MockS3ServerImpl_StateCleared");
}

// Request Handler Implementation - Uses singleton to preserve state
Future<Void> MockS3RequestHandler::handleRequest(Reference<HTTP::IncomingRequest> req,
                                                 Reference<HTTP::OutgoingResponse> response) {
	TraceEvent("MockS3RequestHandler_GetInstance").detail("Method", req->verb).detail("Resource", req->resource);

	// Use singleton instance to maintain state across requests while avoiding reference counting
	MockS3ServerImpl& serverInstance = getSingletonInstance();

	TraceEvent("MockS3RequestHandler_UsingInstance")
	    .detail("InstancePtr", format("%p", &serverInstance))
	    .detail("Method", req->verb)
	    .detail("Resource", req->resource);

	TraceEvent("MockS3RequestHandler").detail("Method", req->verb).detail("Resource", req->resource);

	return MockS3ServerImpl::handleRequest(&serverInstance, req, response);
}

Reference<HTTP::IRequestHandler> MockS3RequestHandler::clone() {
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
