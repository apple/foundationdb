/*
 * GCSBlobStore.h
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

#include "fdbclient/S3BlobStore.h"

// GCS blob store endpoint using the S3-compatible XML API with OAuth2 Bearer token authentication.
// Inherits all blob operations (list, read, write, multipart) from S3BlobStoreEndpoint since the
// GCS XML API uses the same protocol. Only auth headers and credential handling differ.
class GCSBlobStoreEndpoint : public S3BlobStoreEndpoint {
public:
	GCSBlobStoreEndpoint(std::string const& host,
	                     std::string const& service,
	                     Optional<std::string> const& proxyHost,
	                     Optional<std::string> const& proxyPort,
	                     Optional<StringRef> const& creds,
	                     std::string const& projectId,
	                     BlobKnobs const& knobs = BlobKnobs(),
	                     HTTP::Headers extraHeaders = HTTP::Headers());

	// OAuth2 Bearer token instead of SigV4
	void setRequestHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers) override;

	Future<Void> updateSecret() override;
	bool extractCredentialFields(JSONDoc& account) override;

	// Only consult credential files when the URL didn't supply an inline token.
	bool lookupSecretOnEachRequest() override;

	// GCS credential files are keyed by "@host" — no access-key prefix like S3's "accessKey@host".
	std::string credentialFileKey() const override;

	std::string getResourceURL(std::string resource, std::string params) const override;

	// GCS bucket creation needs x-goog-project-id header
	Future<Void> createBucket(std::string const& bucket) override;

	std::string projectId;
	std::string token;
	bool lookupToken;
};
