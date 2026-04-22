/*
 * GCSBlobStore.cpp
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

#include "fdbclient/GCSBlobStore.h"
#include "fdbclient/JSONDoc.h"
#include "flow/Trace.h"

GCSBlobStoreEndpoint::GCSBlobStoreEndpoint(std::string const& host,
                                           std::string const& service,
                                           Optional<std::string> const& proxyHost,
                                           Optional<std::string> const& proxyPort,
                                           Optional<StringRef> const& creds,
                                           std::string const& projectId,
                                           BlobKnobs const& knobs,
                                           HTTP::Headers extraHeaders)
  : S3BlobStoreEndpoint(host,
                        service,
                        "auto",
                        proxyHost,
                        proxyPort,
                        Optional<StringRef>(), // Don't pass creds to S3 — we handle auth ourselves
                        knobs,
                        extraHeaders),
    projectId(projectId) {
	if (creds.present()) {
		token = creds.get().toString();
	}
	// Mirror S3's lookupKey/lookupSecret pattern: if the URL didn't supply a token, we must load it from
	// credential files on each new connection and omit it from the serialized URL.
	lookupToken = token.empty();
}

void GCSBlobStoreEndpoint::setRequestHeaders(std::string const& verb,
                                             std::string const& resource,
                                             HTTP::Headers& headers) {
	headers["Accept"] = "application/xml";
	if (!token.empty()) {
		headers["Authorization"] = "Bearer " + token;
	}
	if (!projectId.empty()) {
		headers["x-goog-project-id"] = projectId;
	}
}

Future<Void> GCSBlobStoreEndpoint::updateSecret() {
	// S3BlobStoreEndpoint::updateSecret() short-circuits when credentials are absent, which is always the case for
	// GCS (we carry the bearer token in `token`, not in S3's `credentials`). Skip the S3 override and go straight to
	// the generic credential-file loop in the base class.
	return IBlobStoreEndpoint::updateSecret();
}

bool GCSBlobStoreEndpoint::extractCredentialFields(JSONDoc& account) {
	std::string newToken;
	if (!account.tryGet("token", newToken)) {
		return false;
	}
	token = newToken;
	return true;
}

bool GCSBlobStoreEndpoint::lookupSecretOnEachRequest() {
	return lookupToken;
}

std::string GCSBlobStoreEndpoint::credentialFileKey() const {
	return "@" + host;
}

std::string GCSBlobStoreEndpoint::getResourceURL(std::string resource, std::string params) const {
	if (!params.empty())
		params.append("&");
	params.append("p=gcs");
	if (!projectId.empty()) {
		params.append("&gcspid=").append(projectId);
	}
	std::string url = S3BlobStoreEndpoint::getResourceURL(resource, params);

	// Preserve an inline token in the URL so it round-trips through fromString. Tokens loaded from credential
	// files (lookupToken == true) are omitted to avoid baking a refreshed value into a serialized URL.
	if (!lookupToken && !token.empty()) {
		const std::string placeholder = "blobstore://@";
		size_t pos = url.find(placeholder);
		if (pos != std::string::npos) {
			url.replace(pos, placeholder.size(), "blobstore://" + token + "@");
		}
	}
	return url;
}

Future<Void> createBucket_gcs_impl(Reference<GCSBlobStoreEndpoint> b, std::string bucket) {
	co_await b->requestRateWrite->getAllowance(1);

	bool exists = co_await b->bucketExists(bucket);
	if (exists) {
		co_return;
	}

	if (b->projectId.empty()) {
		TraceEvent(SevError, "GCSBucketCreateMissingProject")
		    .detail("Bucket", bucket)
		    .detail("Hint", "Set gcs_project_id (or gcspid) URL parameter");
		throw backup_invalid_url();
	}

	// GCS XML API accepts the same PUT request as S3 for bucket creation.
	// x-goog-project-id is added by setRequestHeaders on every request.
	std::string resource = b->constructResourcePath(bucket, "");
	HTTP::Headers headers;
	co_await b->doRequest("PUT", resource, headers, nullptr, 0, { 200, 409 });
}

Future<Void> GCSBlobStoreEndpoint::createBucket(std::string const& bucket) {
	return createBucket_gcs_impl(Reference<GCSBlobStoreEndpoint>::addRef(this), bucket);
}

TEST_CASE("/backup/gcs/fromString") {
	// URL with p=gcs should produce a GCSBlobStoreEndpoint
	{
		std::string url =
		    "blobstore://my-token@storage.googleapis.com/resource?bucket=my-bucket&p=gcs&gcspid=my-project";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);
		ASSERT(endpoint.isValid());
		ASSERT(resource == "resource");

		auto* gcs = dynamic_cast<GCSBlobStoreEndpoint*>(endpoint.getPtr());
		ASSERT(gcs != nullptr);
		ASSERT(gcs->projectId == "my-project");
		ASSERT(gcs->token == "my-token");
	}

	// provider=gcs long form should also work
	{
		std::string url = "blobstore://tok@storage.googleapis.com/r?bucket=b&provider=gcs&gcs_project_id=proj";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);
		ASSERT(endpoint.isValid());

		auto* gcs = dynamic_cast<GCSBlobStoreEndpoint*>(endpoint.getPtr());
		ASSERT(gcs != nullptr);
		ASSERT(gcs->projectId == "proj");
		ASSERT(gcs->token == "tok");
	}

	// URL without p=gcs should NOT produce a GCSBlobStoreEndpoint
	{
		std::string url = "blobstore://s3.us-west-2.amazonaws.com/resource?bucket=b&sa=1";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);
		ASSERT(endpoint.isValid());
		ASSERT(dynamic_cast<GCSBlobStoreEndpoint*>(endpoint.getPtr()) == nullptr);
	}

	return Void();
}

TEST_CASE("/backup/gcs/setRequestHeaders") {
	// With token and projectId — all headers set
	{
		GCSBlobStoreEndpoint gcs("storage.googleapis.com", "443", {}, {}, "my-bearer-token"_sr, "my-project");
		HTTP::Headers headers;
		gcs.setRequestHeaders("GET", "/bucket/object", headers);

		ASSERT(headers["Authorization"] == "Bearer my-bearer-token");
		ASSERT(headers["Accept"] == "application/xml");
		ASSERT(headers["x-goog-project-id"] == "my-project");
	}

	// Empty token — no Authorization header
	{
		GCSBlobStoreEndpoint gcs("storage.googleapis.com", "443", {}, {}, Optional<StringRef>(), "my-project");
		HTTP::Headers headers;
		gcs.setRequestHeaders("PUT", "/bucket", headers);

		ASSERT(!headers.contains("Authorization"));
		ASSERT(headers["Accept"] == "application/xml");
		ASSERT(headers["x-goog-project-id"] == "my-project");
	}

	// Empty projectId — no x-goog-project-id header
	{
		GCSBlobStoreEndpoint gcs("storage.googleapis.com", "443", {}, {}, "tok"_sr, "");
		HTTP::Headers headers;
		gcs.setRequestHeaders("GET", "/bucket/object", headers);

		ASSERT(headers["Authorization"] == "Bearer tok");
		ASSERT(!headers.contains("x-goog-project-id"));
	}

	return Void();
}

TEST_CASE("/backup/gcs/getResourceURL") {
	// getResourceURL should include p=gcs and gcspid
	{
		std::string url = "blobstore://tok@storage.googleapis.com/resource?bucket=b&p=gcs&gcspid=my-project";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);
		ASSERT(endpoint.isValid());

		std::string resURL = endpoint->getResourceURL("test-resource", "");
		ASSERT(resURL.find("p=gcs") != std::string::npos);
		ASSERT(resURL.find("gcspid=my-project") != std::string::npos);
	}

	// Without projectId — no gcspid in URL
	{
		std::string url = "blobstore://tok@storage.googleapis.com/resource?bucket=b&p=gcs";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);
		ASSERT(endpoint.isValid());

		std::string resURL = endpoint->getResourceURL("test-resource", "");
		ASSERT(resURL.find("p=gcs") != std::string::npos);
		ASSERT(resURL.find("gcspid") == std::string::npos);
	}

	// Round-trip: an inline token survives parse → serialize → parse.
	{
		std::string url = "blobstore://tok@storage.googleapis.com/resource?bucket=b&p=gcs&gcspid=proj";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);

		std::string serialized = endpoint->getResourceURL("new-resource", "");
		ASSERT(serialized.find("tok@") != std::string::npos);

		std::string resource2;
		std::string error2;
		S3BlobStoreEndpoint::ParametersT params2;
		Reference<S3BlobStoreEndpoint> endpoint2 =
		    S3BlobStoreEndpoint::fromString(serialized, {}, &resource2, &error2, &params2);
		ASSERT(endpoint2.isValid());

		auto* gcs2 = dynamic_cast<GCSBlobStoreEndpoint*>(endpoint2.getPtr());
		ASSERT(gcs2 != nullptr);
		ASSERT(gcs2->projectId == "proj");
		ASSERT(gcs2->token == "tok");
		ASSERT(!gcs2->lookupToken);
	}

	// Tokens loaded from credential files are not embedded in the serialized URL.
	{
		std::string url = "blobstore://@storage.googleapis.com/resource?bucket=b&p=gcs&gcspid=proj";
		std::string resource;
		std::string error;
		S3BlobStoreEndpoint::ParametersT params;
		Reference<S3BlobStoreEndpoint> endpoint = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &params);

		auto* gcs = dynamic_cast<GCSBlobStoreEndpoint*>(endpoint.getPtr());
		ASSERT(gcs != nullptr);
		ASSERT(gcs->lookupToken);

		// Simulate a credential-file refresh populating the token.
		gcs->token = "refreshed-token";
		std::string serialized = gcs->getResourceURL("r", "");
		ASSERT(serialized.find("refreshed-token") == std::string::npos);
	}

	return Void();
}
