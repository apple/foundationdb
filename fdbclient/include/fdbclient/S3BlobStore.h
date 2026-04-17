/*
 * S3BlobStore.h
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

#pragma once

#include <map>
#include <functional>
#include "flow/Net2Packet.h"
#include "fdbclient/IBlobStore.h"
#include "fdbrpc/HTTP.h"

class S3BlobStoreEndpoint : public IBlobStoreEndpoint, ReferenceCounted<S3BlobStoreEndpoint> {
public:
	void addref() override { ReferenceCounted<S3BlobStoreEndpoint>::addref(); }
	void delref() override { ReferenceCounted<S3BlobStoreEndpoint>::delref(); }

	struct Credentials {
		std::string key;
		std::string secret;
		std::string securityToken;
	};

	S3BlobStoreEndpoint(std::string const& host,
	                    std::string const& service,
	                    std::string region,
	                    Optional<std::string> const& proxyHost,
	                    Optional<std::string> const& proxyPort,
	                    Optional<StringRef> const& creds,
	                    BlobKnobs const& knobs = BlobKnobs(),
	                    HTTP::Headers extraHeaders = HTTP::Headers());

	// Infer the cloud region from a hostname by matching known service prefixes
	// (e.g. "s3.us-west-2.amazonaws.com" -> "us-west-2", "cos.ap-beijing.myqcloud.com" -> "ap-beijing").
	// Returns "" if no pattern matches.
	static std::string guessRegionFromDomain(std::string domain);

	// Convenience: parse URL and return as S3BlobStoreEndpoint (downcasts from IBlobStoreEndpoint::fromString).
	// Callers that only need the IBlobStoreEndpoint interface should use IBlobStoreEndpoint::fromString directly.
	static Reference<S3BlobStoreEndpoint> fromString(const std::string& url,
	                                                 const Optional<std::string>& proxy,
	                                                 std::string* resourceFromURL,
	                                                 std::string* error,
	                                                 ParametersT* ignored_parameters) {
		Reference<IBlobStoreEndpoint> base =
		    IBlobStoreEndpoint::fromString(url, proxy, resourceFromURL, error, ignored_parameters);
		auto* endpoint = dynamic_cast<S3BlobStoreEndpoint*>(base.getPtr());
		if (base && !endpoint) {
			if (error) {
				*error = "Parsed blob store endpoint is not an S3BlobStoreEndpoint";
			}
			return Reference<S3BlobStoreEndpoint>();
		}
		return Reference<S3BlobStoreEndpoint>::addRef(endpoint);
	}

	Future<Void> updateSecret() override;
	bool extractCredentialFields(JSONDoc& account) override;
	std::string credentialFileKey() const override;
	bool lookupSecretOnEachRequest() override;
	void setRequestHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers) override;
	std::string normalizeResourceForRequest(std::string const& resource) override;

	Optional<Credentials> credentials;
	bool lookupKey;
	bool lookupSecret;
	bool simulatedTokenError = false; // Set by simulateRequestFailure for BUGGIFY testing

	// Calculates the authentication string from the secret key
	static std::string hmac_sha1(Credentials const& creds, std::string const& msg);

	// Sets headers needed for Authorization (including Date which will be overwritten if present)
	void setAuthHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers);

	// Set headers in the AWS V4 authorization format. $date and $datestamp are used for unit testing
	void setV4AuthHeaders(const std::string& verb,
	                      const std::string& resource,
	                      HTTP::Headers& headers,
	                      std::string date = "",
	                      std::string datestamp = "");

	// doRequest hooks for S3 token error recovery
	void simulateRequestFailure(std::string const& verb,
	                            std::string const& resource,
	                            Reference<HTTP::IncomingResponse>& r) override;
	void processRequestFailure(Reference<HTTP::IncomingResponse> const& r,
	                           TraceEvent& event,
	                           bool& retryExtended) override;
	Future<bool> preRetryCheck(std::string const& verb,
	                           std::string const& resource,
	                           ReusableConnection& rconn,
	                           int requestTimeout,
	                           bool& retryExtended) override;

	// Get a normalized version of this URL with the given resource and any non-default BlobKnob values as URL
	// parameters in addition to the passed params string
	std::string getResourceURL(std::string resource, std::string params) const override;

	// Construct a resource path for S3 operations
	std::string constructResourcePath(const std::string& bucket, const std::string& object) const;

	// Get bucket contents via a stream, since listing large buckets will take many serial blob requests.
	// If a delimiter is passed then common prefixes will be read in parallel, recursively, depending on recurseFilter.
	// recurseFilter must be a function that takes a string and returns true if it passes.  The default behavior is
	// to assume true.
	Future<Void> listObjectsStream(std::string const& bucket,
	                               PromiseStream<ListResult> results,
	                               Optional<std::string> prefix = {},
	                               Optional<char> delimiter = {},
	                               int maxDepth = 0,
	                               std::function<bool(std::string const&)> recurseFilter = nullptr) override;

	// Get a list of all buckets
	AsyncResult<std::vector<std::string>> listBuckets() override;

	// Check if a bucket exists
	Future<bool> bucketExists(std::string const& bucket) override;

	// Check if an object exists in a bucket
	Future<bool> objectExists(std::string const& bucket, std::string const& object) override;

	// Get the size of an object in a bucket
	Future<int64_t> objectSize(std::string const& bucket, std::string const& object) override;

	// Read an arbitrary segment of an object
	Future<int> readObject(std::string const& bucket,
	                       std::string const& object,
	                       void* data,
	                       int length,
	                       int64_t offset) override;

	// Delete an object in a bucket
	Future<Void> deleteObject(std::string const& bucket, std::string const& object) override;

	// Create a bucket if it does not already exist.
	Future<Void> createBucket(std::string const& bucket) override;

	// Useful methods for working with tiny files
	AsyncResult<std::string> readEntireFile(std::string const& bucket, std::string const& object) override;
	Future<Void> writeEntireFileFromBuffer(std::string const& bucket,
	                                       std::string const& object,
	                                       UnsentPacketQueue* pContent,
	                                       int contentLen,
	                                       std::string const& contentHash) override;

	// MultiPart upload methods
	// Returns UploadID
	Future<std::string> beginMultiPartUpload(std::string const& bucket, std::string const& object) override;
	// Returns eTag
	Future<std::string> uploadPart(std::string const& bucket,
	                               std::string const& object,
	                               std::string const& uploadID,
	                               unsigned int partNumber,
	                               UnsentPacketQueue* pContent,
	                               int contentLen,
	                               std::string const& contentHash) override;
	Future<Optional<std::string>> finishMultiPartUpload(std::string const& bucket,
	                                                    std::string const& object,
	                                                    std::string const& uploadID,
	                                                    MultiPartSetT const& parts,
	                                                    int64_t totalSize = 0) override;

	Future<Void> abortMultiPartUpload(std::string const& bucket,
	                                  std::string const& object,
	                                  std::string const& uploadID);
	Future<Void> putObjectTags(std::string const& bucket,
	                           std::string const& object,
	                           std::map<std::string, std::string> const& tags);
	Future<std::map<std::string, std::string>> getObjectTags(std::string const& bucket, std::string const& object);
};
