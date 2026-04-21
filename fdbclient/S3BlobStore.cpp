/*
 * S3BlobStore.cpp
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

#include "fdbclient/S3BlobStore.h"

#include <sstream>
#include "fdbrpc/HTTP.h"
#include "fdbclient/Knobs.h"
#include "flow/FastRef.h"
#include "flow/IConnection.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "md5/md5.h"
#include "libb64/encode.h"
#include "fdbclient/sha1/SHA1.h"
#include <climits>
#include <iostream>
#include <time.h>
#include <iomanip>
#include <openssl/sha.h>
#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/hex.hpp>
#include "flow/IAsyncFile.h"
#include "flow/Hostname.h"
#include "flow/UnitTest.h"
#include "rapidxml/rapidxml.hpp"
#ifdef WITH_AWS_BACKUP
#include "fdbclient/FDBAWSCredentialsProvider.h"
#endif

#include "flow/CoroUtils.h"

using namespace rapidxml;

std::string S3BlobStoreEndpoint::guessRegionFromDomain(std::string domain) {
	// Special case for localhost/127.0.0.1 to prevent basic_string exception
	if (domain == "127.0.0.1" || domain == "localhost") {
		return "us-east-1";
	}

	static const std::vector<const char*> knownServices = { "s3.", "cos.", "oss-", "obs." };
	boost::algorithm::to_lower(domain);

	for (const auto& service : knownServices) {
		std::size_t p = domain.find(service);
		if (p == std::string::npos || (p >= 1 && domain[p - 1] != '.')) {
			// eg. 127.0.0.1, example.com, s3-service.example.com, mys3.example.com
			continue;
		}

		StringRef h = StringRef(domain).substr(p);

		if (!h.startsWith("oss-"_sr)) {
			h.eat(service); // ignore s3 service
		}

		return h.eat(".").toString();
	}

	return "";
}

static Optional<S3BlobStoreEndpoint::Credentials> parseS3Credentials(Optional<StringRef> const& credString) {
	if (!credString.present()) {
		return Optional<S3BlobStoreEndpoint::Credentials>();
	}
	StringRef c = credString.get();
	StringRef key = c.eat(":");
	StringRef secret = c.eat(":");
	StringRef securityToken = c.eat();
	return S3BlobStoreEndpoint::Credentials{ key.toString(), secret.toString(), securityToken.toString() };
}

S3BlobStoreEndpoint::S3BlobStoreEndpoint(std::string const& host,
                                         std::string const& service,
                                         std::string region,
                                         Optional<std::string> const& proxyHost,
                                         Optional<std::string> const& proxyPort,
                                         Optional<StringRef> const& creds,
                                         BlobKnobs const& knobs,
                                         HTTP::Headers extraHeaders)
  : IBlobStoreEndpoint(host,
                       service,
                       region.empty() ? guessRegionFromDomain(host) : region,
                       proxyHost,
                       proxyPort,
                       knobs,
                       extraHeaders),
    credentials(parseS3Credentials(creds)), lookupKey(credentials.present() && credentials.get().key.empty()),
    lookupSecret(credentials.present() && credentials.get().secret.empty()) {

	if (this->region.empty() && CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER) {
		throw std::string(
		    "Failed to get region from host or parameter in url, region is required for aws v4 signature");
	}
}

std::string S3BlobStoreEndpoint::getResourceURL(std::string resource, std::string params) const {
	// The base class produces blobstore://@host/resource?params.
	// For S3, we replace the empty credential slot with explicit credentials when they weren't
	// looked up from credential files (i.e., they were passed in the URL).
	std::string url = IBlobStoreEndpoint::getResourceURL(resource, params);

	// Build the S3 credential prefix to replace the bare "@"
	std::string credsString;
	if (credentials.present()) {
		if (!lookupKey) {
			credsString = credentials.get().key;
		}
		if (!lookupSecret) {
			credsString += ":" + credentials.get().secret;
			if (!credentials.get().securityToken.empty()) {
				credsString += ":" + credentials.get().securityToken;
			}
		}
	}

	// Replace "blobstore://@" with "blobstore://<creds>@"
	const std::string placeholder = "blobstore://@";
	size_t pos = url.find(placeholder);
	if (pos != std::string::npos) {
		url.replace(pos, placeholder.size(), "blobstore://" + credsString + "@");
	}

	return url;
}

std::string S3BlobStoreEndpoint::constructResourcePath(const std::string& bucket, const std::string& object) const {
	std::string resource;

	if (host.find(bucket + ".") != 0) {
		resource += std::string("/") + bucket; // not virtual hosting mode
	}

	if (!object.empty()) {
		// S3 object keys should not start with '/'. Strip any leading slashes.
		std::string cleanedObject = object;
		while (!cleanedObject.empty() && cleanedObject[0] == '/') {
			cleanedObject = cleanedObject.substr(1);
		}
		if (!cleanedObject.empty()) {
			resource += "/";
			resource += cleanedObject;
		}
	}

	return resource;
}

Future<bool> bucketExists_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket) {
	co_await b->requestRateRead->getAllowance(1);

	std::string resource = b->constructResourcePath(bucket, "");
	HTTP::Headers headers;

	try {
		Reference<HTTP::IncomingResponse> r =
		    co_await b->doRequest("HEAD", resource, headers, nullptr, 0, { 200, 404 });
		co_return r->code == 200;
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled)
			throw;
		TraceEvent(SevError, "S3ClientBucketExistsError")
		    .detail("Bucket", bucket)
		    .detail("Host", b->host)
		    .errorUnsuppressed(e);
		throw;
	}
}

Future<bool> S3BlobStoreEndpoint::bucketExists(std::string const& bucket) {
	return bucketExists_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket);
}

Future<bool> objectExists_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket, std::string object) {
	co_await b->requestRateRead->getAllowance(1);

	std::string resource = b->constructResourcePath(bucket, object);
	HTTP::Headers headers;

	Reference<HTTP::IncomingResponse> r = co_await b->doRequest("HEAD", resource, headers, nullptr, 0, { 200, 404 });
	co_return r->code == 200;
}

Future<bool> S3BlobStoreEndpoint::objectExists(std::string const& bucket, std::string const& object) {
	return objectExists_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

Future<Void> deleteObject_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket, std::string object) {
	co_await b->requestRateDelete->getAllowance(1);

	std::string resource = b->constructResourcePath(bucket, object);
	HTTP::Headers headers;
	// 200 or 204 means object successfully deleted, 404 means it already doesn't exist, so any of those are considered
	// successful
	Reference<HTTP::IncomingResponse> r =
	    co_await b->doRequest("DELETE", resource, headers, nullptr, 0, { 200, 204, 404 });

	// But if the object already did not exist then the 'delete' is assumed to be successful but a warning is logged.
	if (r->code == 404) {
		TraceEvent(SevWarnAlways, "S3BlobStoreEndpointDeleteObjectMissing")
		    .detail("Host", b->host)
		    .detail("Bucket", bucket)
		    .detail("Object", object);
	}
}

Future<Void> S3BlobStoreEndpoint::deleteObject(std::string const& bucket, std::string const& object) {
	return deleteObject_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

Future<Void> createBucket_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket) {
	UnsentPacketQueue packets;
	co_await b->requestRateWrite->getAllowance(1);

	bool exists = co_await b->bucketExists(bucket);
	if (!exists) {
		std::string resource = b->constructResourcePath(bucket, "");
		HTTP::Headers headers;

		std::string region = b->getRegion();
		if (region.empty()) {
			Reference<HTTP::IncomingResponse> r =
			    co_await b->doRequest("PUT", resource, headers, nullptr, 0, { 200, 409 });
		} else {
			Standalone<StringRef> body(
			    format("<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
			           "  <LocationConstraint>%s</LocationConstraint>"
			           "</CreateBucketConfiguration>",
			           region.c_str()));
			PacketWriter pw(packets.getWriteBuffer(), nullptr, Unversioned());
			pw.serializeBytes(body);

			Reference<HTTP::IncomingResponse> r =
			    co_await b->doRequest("PUT", resource, headers, &packets, body.size(), { 200, 409 });
		}
	}
}

Future<Void> S3BlobStoreEndpoint::createBucket(std::string const& bucket) {
	return createBucket_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket);
}

Future<int64_t> objectSize_impl(Reference<S3BlobStoreEndpoint> b, std::string bucket, std::string object) {
	co_await b->requestRateRead->getAllowance(1);

	std::string resource = b->constructResourcePath(bucket, object);
	HTTP::Headers headers;

	Reference<HTTP::IncomingResponse> r = co_await b->doRequest("HEAD", resource, headers, nullptr, 0, { 200, 404 });
	if (r->code == 404)
		throw file_not_found();
	co_return r->data.contentLen;
}

Future<int64_t> S3BlobStoreEndpoint::objectSize(std::string const& bucket, std::string const& object) {
	return objectSize_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

// If the credentials expire, the connection will eventually fail and be discarded from the pool, and then a new
// connection will be constructed, which will call this again to get updated credentials
static S3BlobStoreEndpoint::Credentials getSecretSdk() {
#ifdef WITH_AWS_BACKUP
	double elapsed = -timer_monotonic();
	Aws::Auth::AWSCredentials awsCreds = FDBAWSCredentialsProvider::getAwsCredentials();
	elapsed += timer_monotonic();

	if (awsCreds.IsEmpty()) {
		TraceEvent(SevWarn, "S3BlobStoreAWSCredsEmpty");
		throw backup_auth_missing();
	}

	S3BlobStoreEndpoint::Credentials fdbCreds;
	fdbCreds.key = awsCreds.GetAWSAccessKeyId();
	fdbCreds.secret = awsCreds.GetAWSSecretKey();
	fdbCreds.securityToken = awsCreds.GetSessionToken();

	TraceEvent("S3BlobStoreGotSdkCredentials").suppressFor(60).detail("Duration", elapsed);

	return fdbCreds;
#else
	TraceEvent(SevError, "S3BlobStoreNoSDK").log();
	throw backup_auth_missing();
#endif
}

Future<Void> S3BlobStoreEndpoint::updateSecret() {
	if (knobs.sdk_auth) {
		credentials = getSecretSdk();
		co_return;
	}
	if (!credentials.present()) {
		co_return;
	}
	co_await IBlobStoreEndpoint::updateSecret();
}

bool S3BlobStoreEndpoint::extractCredentialFields(JSONDoc& account) {
	Credentials creds = credentials.get();
	if (lookupKey) {
		std::string apiKey;
		if (account.tryGet("api_key", apiKey))
			creds.key = apiKey;
		else
			return false;
	}
	if (lookupSecret) {
		std::string secret;
		if (account.tryGet("secret", secret))
			creds.secret = secret;
		else
			return false;
	}
	std::string token;
	if (account.tryGet("token", token))
		creds.securityToken = token;
	credentials = creds;
	return true;
}

std::string S3BlobStoreEndpoint::credentialFileKey() const {
	std::string accessKey = lookupKey ? "" : credentials.get().key;
	return accessKey + "@" + host;
}

bool S3BlobStoreEndpoint::lookupSecretOnEachRequest() {
	return lookupKey || lookupSecret || knobs.sdk_auth;
}

std::string awsCanonicalURI(const std::string& resource, std::vector<std::string>& queryParameters, bool isV4) {
	StringRef resourceRef(resource);
	resourceRef.eat("/");
	std::string canonicalURI("/" + resourceRef.toString());
	size_t q = canonicalURI.find_last_of('?');
	if (q != canonicalURI.npos)
		canonicalURI.resize(q);
	if (isV4) {
		canonicalURI = HTTP::awsV4URIEncode(canonicalURI, false);
	} else {
		canonicalURI = HTTP::urlEncode(canonicalURI);
	}

	// Create the canonical query string
	std::string queryString;
	q = resource.find_last_of('?');
	if (q != queryString.npos)
		queryString = resource.substr(q + 1);

	StringRef qStr(queryString);
	StringRef queryParameter;
	while (!(queryParameter = qStr.eat("&")).empty()) {
		StringRef param = queryParameter.eat("=");
		StringRef value = queryParameter.eat();

		if (isV4) {
			queryParameters.push_back(HTTP::awsV4URIEncode(param.toString(), true) + "=" +
			                          HTTP::awsV4URIEncode(value.toString(), true));
		} else {
			queryParameters.push_back(HTTP::urlEncode(param.toString()) + "=" + HTTP::urlEncode(value.toString()));
		}
	}

	return canonicalURI;
}

// ref: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
// Returns the S3 error code string from an XML error response, or "" if the response
// cannot be parsed. This is best-effort for logging/diagnostics only — many HTTP error
// responses (502/503 from load balancers, empty bodies, HTML responses) are not XML.
std::string parseErrorCodeFromS3(const std::string& response) {
	if (response.empty()) {
		return "";
	}
	try {
		std::vector<char> xmlBuffer(response.begin(), response.end());
		xmlBuffer.push_back('\0');
		xml_document<> doc;
		doc.parse<0>(&xmlBuffer[0]);
		xml_node<>* root = doc.first_node("Error");
		if (!root) {
			return "";
		}
		xml_node<>* codeNode = root->first_node("Code");
		if (!codeNode) {
			return "";
		}
		return std::string(codeNode->value());
	} catch (...) {
		// Parse failures are expected for non-XML responses (HTML error pages, empty bodies, etc.)
		TraceEvent("ParseS3ErrorCodeNonXML")
		    .suppressFor(60)
		    .detail("ResponseSize", response.size())
		    .detail("ResponseHead", response.substr(0, 200));
		return "";
	}
}

bool isS3TokenError(const std::string& s3Error) {
	return s3Error == "InvalidToken" || s3Error == "ExpiredToken";
}

void S3BlobStoreEndpoint::setRequestHeaders(std::string const& verb,
                                            std::string const& resource,
                                            HTTP::Headers& headers) {
	headers["Accept"] = "application/xml";
	if (credentials.present() && !credentials.get().securityToken.empty())
		headers["x-amz-security-token"] = credentials.get().securityToken;
	if (CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER) {
		setV4AuthHeaders(verb, resource, headers);
	} else {
		setAuthHeaders(verb, resource, headers);
	}
}

std::string S3BlobStoreEndpoint::normalizeResourceForRequest(std::string const& resource) {
	std::vector<std::string> queryParameters;
	std::string canonicalURI = awsCanonicalURI(resource, queryParameters, CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER);
	if (!queryParameters.empty()) {
		canonicalURI += "?";
		for (size_t i = 0; i < queryParameters.size(); ++i) {
			if (i > 0) {
				canonicalURI += "&";
			}
			canonicalURI += queryParameters[i];
		}
	}
	return canonicalURI;
}

void populateDryrunRequest(Reference<HTTP::OutgoingRequest> dryrunRequest,
                           Reference<S3BlobStoreEndpoint> bstore,
                           std::string bucket) {
	// dryrun with a check bucket exist request, to avoid sending duplicate data
	HTTP::Headers headers;
	dryrunRequest->verb = "GET";
	dryrunRequest->data.contentLen = 0;
	dryrunRequest->data.headers = headers;
	dryrunRequest->data.headers["Host"] = bstore->host;
	dryrunRequest->data.headers["Accept"] = "application/xml";

	dryrunRequest->resource = bstore->constructResourcePath(bucket, "");
}

bool isWriteRequest(std::string verb) {
	return verb == "POST" || verb == "PUT";
}

std::string parseBucketFromURI(std::string uri) {
	if (uri.size() <= 1 || uri[0] != '/') {
		// there is no bucket in the uri
		return "";
	}
	uri = uri.substr(1);
	size_t secondSlash = uri.find('/');
	if (secondSlash == std::string::npos) {
		return uri;
	}
	return uri.substr(0, secondSlash);
}

void S3BlobStoreEndpoint::simulateRequestFailure(std::string const& verb,
                                                 std::string const& resource,
                                                 Reference<HTTP::IncomingResponse>& r) {
	simulatedTokenError = false;
	if (!g_network->isSimulated() || !BUGGIFY || deterministicRandom()->random01() >= 0.1) {
		return;
	}
	// Don't simulate token errors for multipart complete operations (POST with uploadId but no partNumber)
	// because changing a successful 200 to 400 after the server has already completed and removed
	// the upload causes the client to infinitely retry with a phantom upload ID.
	bool isMultipartComplete = verb == "POST" && resource.find("uploadId=") != std::string::npos &&
	                           resource.find("partNumber=") == std::string::npos;
	if (!isMultipartComplete) {
		r->code = 400;
		simulatedTokenError = true;
	}
}

void S3BlobStoreEndpoint::processRequestFailure(Reference<HTTP::IncomingResponse> const& r,
                                                TraceEvent& event,
                                                bool& retryExtended) {
	// Only parse S3 error code for error responses (4xx/5xx), not successful responses (2xx).
	// Skip parsing for simulated errors where response content is still binary data.
	std::string s3Error;
	if (r->code >= 400 && !simulatedTokenError) {
		s3Error = parseErrorCodeFromS3(r->data.content);
	}
	event.detail("S3ErrorCode", s3Error);

	if (r->code == 400) {
		if (isS3TokenError(s3Error) || simulatedTokenError) {
			retryExtended = true;
		}
		TraceEvent(SevWarnAlways, "S3BlobStoreBadRequest")
		    .detail("HttpCode", r->code)
		    .detail("HttpResponseContent", r->data.content)
		    .detail("S3Error", s3Error);
	}
}

Future<bool> S3BlobStoreEndpoint::preRetryCheck(std::string const& verb,
                                                std::string const& resource,
                                                ReusableConnection& rconn,
                                                int requestTimeout,
                                                bool& retryExtended) {
	if (!isWriteRequest(verb) || !CLIENT_KNOBS->BACKUP_ALLOW_DRYRUN) {
		co_return true;
	}

	std::string bucket = parseBucketFromURI(resource);
	if (bucket.empty()) {
		TraceEvent(SevError, "EmptyBucketRequest").detail("Verb", verb).detail("Resource", resource);
		throw bucket_not_in_url();
	}

	// Send a cheap HEAD request to verify credentials before resending a potentially large upload
	UnsentPacketQueue dryrunContentCopy;
	auto dryrunRequest = makeReference<HTTP::OutgoingRequest>();
	dryrunRequest->data.content = &dryrunContentCopy;
	populateDryrunRequest(dryrunRequest, Reference<S3BlobStoreEndpoint>::addRef(this), bucket);
	setRequestHeaders(dryrunRequest->verb, dryrunRequest->resource, dryrunRequest->data.headers);
	dryrunRequest->resource = normalizeResourceForRequest(dryrunRequest->resource);

	TraceEvent("RetryS3RequestDueToTokenIssue")
	    .detail("OriginalResource", resource)
	    .detail("DryrunResource", dryrunRequest->resource)
	    .detail("Bucket", bucket)
	    .detail("V4", CLIENT_KNOBS->HTTP_REQUEST_AWS_V4_HEADER);

	co_await requestRate->getAllowance(1);
	Future<Reference<HTTP::IncomingResponse>> dryrunResponse =
	    HTTP::doRequest(rconn.conn, dryrunRequest, sendRate, &s_stats.bytes_sent, recvRate);
	Reference<HTTP::IncomingResponse> dryrunR = co_await timeoutError(dryrunResponse, requestTimeout);

	std::string s3Error;
	if (dryrunR->code >= 400) {
		s3Error = parseErrorCodeFromS3(dryrunR->data.content);
	}

	if (dryrunR->code == 400 && isS3TokenError(s3Error)) {
		// Token still bad — delay and skip this iteration
		co_await ::delay(knobs.max_delay_retryable_error);
		co_return false;
	} else if (dryrunR->code == 200 || dryrunR->code == 404) {
		// Credentials are good now — proceed with the real request
		TraceEvent("S3TokenIssueResolved").detail("HttpCode", dryrunR->code).detail("URI", dryrunRequest->resource);
		retryExtended = false;
		co_return true;
	} else {
		TraceEvent(SevError, "S3UnexpectedError")
		    .detail("HttpCode", dryrunR->code)
		    .detail("HttpResponseContent", dryrunR->data.content)
		    .detail("S3Error", s3Error)
		    .detail("URI", dryrunRequest->resource);
		throw http_bad_response();
	}
}

Future<Void> listObjectsStream_impl(Reference<S3BlobStoreEndpoint> bstore,
                                    std::string bucket,
                                    PromiseStream<S3BlobStoreEndpoint::ListResult> results,
                                    Optional<std::string> prefix,
                                    Optional<char> delimiter,
                                    int maxDepth,
                                    std::function<bool(std::string const&)> recurseFilterUnsafe) {
	// C++20 coroutine safety: copy const& params to survive across suspension.
	auto recurseFilter = recurseFilterUnsafe;

	std::string resource = bstore->constructResourcePath(bucket, "");
	// In virtual hosting mode, constructResourcePath returns "" for empty object.
	// We need to ensure the query string starts with "/" to form a valid HTTP request.
	// Commit 15dd76a7f9 switched from ListObjectsV1 to V2 and accidentally removed the leading "/".
	if (resource.empty()) {
		resource = "/";
	}
	resource.append("?list-type=2&max-keys=").append(std::to_string(CLIENT_KNOBS->BLOBSTORE_LIST_MAX_KEYS_PER_PAGE));

	if (prefix.present())
		resource.append("&prefix=").append(prefix.get());
	if (delimiter.present())
		resource.append("&delimiter=").append(std::string(1, delimiter.get()));

	std::string continuationToken;
	bool more = true;
	std::vector<Future<Void>> subLists;

	while (more) {
		co_await bstore->concurrentLists.take();
		FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		std::string fullResource = resource;
		if (!continuationToken.empty()) {
			fullResource.append("&continuation-token=").append(continuationToken);
		}

		Reference<HTTP::IncomingResponse> r =
		    co_await bstore->doRequest("GET", fullResource, headers, nullptr, 0, { 200, 404 });
		listReleaser.release();

		try {
			S3BlobStoreEndpoint::ListResult listResult;

			// If we got a 404, throw an error to indicate the resource doesn't exist
			if (r->code == 404) {
				TraceEvent(SevError, "S3BlobStoreResourceNotFound")
				    .detail("Bucket", bucket)
				    .detail("Prefix", prefix.present() ? prefix.get() : "")
				    .detail("Resource", fullResource);
				throw resource_not_found();
			}

			xml_document<> doc;

			// Copy content because rapidxml will modify it during parse
			std::string content = r->data.content;
			doc.parse<0>((char*)content.c_str());

			// There should be exactly one node
			xml_node<>* result = doc.first_node();
			if (result == nullptr || strcmp(result->name(), "ListBucketResult") != 0) {
				throw http_bad_response();
			}

			xml_node<>* n = result->first_node();
			more = false;
			continuationToken.clear();

			while (n != nullptr) {
				const char* name = n->name();
				if (strcmp(name, "IsTruncated") == 0) {
					const char* val = n->value();
					if (strcmp(val, "true") == 0) {
						more = true;
					} else if (strcmp(val, "false") == 0) {
						more = false;
					} else {
						throw http_bad_response();
					}
				} else if (strcmp(name, "NextContinuationToken") == 0) {
					if (n->value() != nullptr) {
						continuationToken = n->value();
					}
				} else if (strcmp(name, "Contents") == 0) {
					S3BlobStoreEndpoint::ObjectInfo object;

					xml_node<>* key = n->first_node("Key");
					if (key == nullptr) {
						throw http_bad_response();
					}
					// URL decode the object name since S3 XML responses contain URL-encoded names
					object.name = HTTP::urlDecode(key->value());

					xml_node<>* size = n->first_node("Size");
					if (size == nullptr) {
						throw http_bad_response();
					}
					object.size = strtoull(size->value(), nullptr, 10);

					listResult.objects.push_back(object);
				} else if (strcmp(name, "CommonPrefixes") == 0) {
					xml_node<>* prefixNode = n->first_node("Prefix");
					while (prefixNode != nullptr) {
						const char* prefix = prefixNode->value();
						// If recursing, queue a sub-request, otherwise add the common prefix to the result.
						if (maxDepth > 0) {
							if (!recurseFilter || recurseFilter(prefix)) {
								// For recursive listing, don't use delimiter in sub-requests to get individual files
								subLists.push_back(bstore->listObjectsStream(
								    bucket, results, prefix, Optional<char>(), maxDepth - 1, recurseFilter));
							}
						} else {
							listResult.commonPrefixes.push_back(prefix);
						}
						prefixNode = prefixNode->next_sibling("Prefix");
					}
				}
				n = n->next_sibling();
			}

			results.send(listResult);
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				TraceEvent(SevWarn, "S3BlobStoreEndpointListResultParseError")
				    .errorUnsuppressed(e)
				    .suppressFor(60)
				    .detail("Resource", fullResource);
			}
			throw http_bad_response();
		}
	}

	co_await waitForAll(subLists);
}

Future<Void> S3BlobStoreEndpoint::listObjectsStream(std::string const& bucket,
                                                    PromiseStream<ListResult> results,
                                                    Optional<std::string> prefix,
                                                    Optional<char> delimiter,
                                                    int maxDepth,
                                                    std::function<bool(std::string const&)> recurseFilter) {
	return listObjectsStream_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, results, prefix, delimiter, maxDepth, recurseFilter);
}

AsyncResult<std::vector<std::string>> listBuckets_impl(Reference<S3BlobStoreEndpoint> bstore) {
	std::string resource = "/?marker=";
	std::string lastName;
	bool more = true;
	std::vector<std::string> buckets;

	while (more) {
		co_await bstore->concurrentLists.take();
		FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		std::string fullResource = resource + lastName;
		Reference<HTTP::IncomingResponse> r =
		    co_await bstore->doRequest("GET", fullResource, headers, nullptr, 0, { 200 });
		listReleaser.release();

		try {
			xml_document<> doc;

			// Copy content because rapidxml will modify it during parse
			std::string content = r->data.content;
			doc.parse<0>((char*)content.c_str());

			// There should be exactly one node
			xml_node<>* result = doc.first_node();
			if (result == nullptr || strcmp(result->name(), "ListAllMyBucketsResult") != 0) {
				throw http_bad_response();
			}

			more = false;
			xml_node<>* truncated = result->first_node("IsTruncated");
			if (truncated != nullptr && strcmp(truncated->value(), "true") == 0) {
				more = true;
			}

			xml_node<>* bucketsNode = result->first_node("Buckets");
			if (bucketsNode != nullptr) {
				xml_node<>* bucketNode = bucketsNode->first_node("Bucket");
				while (bucketNode != nullptr) {
					xml_node<>* nameNode = bucketNode->first_node("Name");
					if (nameNode == nullptr) {
						throw http_bad_response();
					}
					const char* name = nameNode->value();
					buckets.push_back(name);

					bucketNode = bucketNode->next_sibling("Bucket");
				}
			}

			if (more) {
				lastName = buckets.back();
			}

		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "S3BlobStoreEndpointListBucketResultParseError")
				    .errorUnsuppressed(e)
				    .suppressFor(60)
				    .detail("Resource", fullResource);
			throw http_bad_response();
		}
	}

	co_return buckets;
}

AsyncResult<std::vector<std::string>> S3BlobStoreEndpoint::listBuckets() {
	return listBuckets_impl(Reference<S3BlobStoreEndpoint>::addRef(this));
}

std::string S3BlobStoreEndpoint::hmac_sha1(Credentials const& creds, std::string const& msg) {
	std::string key = creds.secret;

	// Hash key to shorten it if it is longer than SHA1 block size
	if (key.size() > 64) {
		key = SHA1::from_string(key);
	}

	// Pad key up to SHA1 block size if needed
	key.append(64 - key.size(), '\0');

	std::string kipad = key;
	for (int i = 0; i < 64; ++i)
		kipad[i] ^= '\x36';

	std::string kopad = key;
	for (int i = 0; i < 64; ++i)
		kopad[i] ^= '\x5c';

	kipad.append(msg);
	std::string hkipad = SHA1::from_string(kipad);
	kopad.append(hkipad);
	return SHA1::from_string(kopad);
}

static void sha256(const unsigned char* data, const size_t len, unsigned char* hash) {
	SHA256_CTX sha256;
	SHA256_Init(&sha256);
	SHA256_Update(&sha256, data, len);
	SHA256_Final(hash, &sha256);
}

std::string sha256_hex(std::string str) {
	unsigned char hash[SHA256_DIGEST_LENGTH];
	sha256((const unsigned char*)str.c_str(), str.size(), hash);
	std::stringstream ss;
	for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
		ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
	}
	return ss.str();
}

// Return base64'd SHA256 hash of input string.
std::string sha256_base64(std::string str) {
	unsigned char hash[SHA256_DIGEST_LENGTH];
	sha256((const unsigned char*)str.c_str(), str.size(), hash);
	std::string hashAsStr = std::string((char*)hash, SHA256_DIGEST_LENGTH);
	std::string sig = base64::encoder::from_string(hashAsStr);
	// base64 encoded blocks end in \n so remove last character.
	sig.resize(sig.size() - 1);
	return sig;
}

std::string hmac_sha256_hex(std::string key, std::string msg) {
	unsigned char hash[32];

	HMAC_CTX* hmac = HMAC_CTX_new();
	HMAC_Init_ex(hmac, &key[0], key.length(), EVP_sha256(), nullptr);
	HMAC_Update(hmac, (unsigned char*)&msg[0], msg.length());
	unsigned int len = 32;
	HMAC_Final(hmac, hash, &len);
	HMAC_CTX_free(hmac);

	std::stringstream ss;
	ss << std::hex << std::setfill('0');
	for (int i = 0; i < len; i++) {
		ss << std::hex << std::setw(2) << (unsigned int)hash[i];
	}
	return (ss.str());
}

std::string hmac_sha256(std::string key, std::string msg) {
	unsigned char hash[32];

	HMAC_CTX* hmac = HMAC_CTX_new();
	HMAC_Init_ex(hmac, &key[0], key.length(), EVP_sha256(), nullptr);
	HMAC_Update(hmac, (unsigned char*)&msg[0], msg.length());
	unsigned int len = 32;
	HMAC_Final(hmac, hash, &len);
	HMAC_CTX_free(hmac);

	std::stringstream ss;
	ss << std::setfill('0');
	for (int i = 0; i < len; i++) {
		ss << hash[i];
	}
	return (ss.str());
}

// Date and Time parameters are used for unit testing
void S3BlobStoreEndpoint::setV4AuthHeaders(std::string const& verb,
                                           std::string const& resource,
                                           HTTP::Headers& headers,
                                           std::string date,
                                           std::string datestamp) {
	if (!credentials.present()) {
		return;
	}
	Credentials creds = credentials.get();
	// std::cout << "========== Starting===========" << std::endl;
	std::string accessKey = creds.key;
	std::string secretKey = creds.secret;
	// Create a date for headers and the credential string
	std::string amzDate;
	std::string dateStamp;
	if (date.empty() || datestamp.empty()) {
		time_t ts;
		time(&ts);
		char dateBuf[20];
		// ISO 8601 format YYYYMMDD'T'HHMMSS'Z'
		strftime(dateBuf, 20, "%Y%m%dT%H%M%SZ", gmtime(&ts));
		amzDate = dateBuf;
		strftime(dateBuf, 20, "%Y%m%d", gmtime(&ts));
		dateStamp = dateBuf;
	} else {
		amzDate = date;
		dateStamp = datestamp;
	}

	// ************* TASK 1: CREATE A CANONICAL REQUEST *************
	// Create Create canonical URI--the part of the URI from domain to query string (use '/' if no path)
	std::vector<std::string> queryParameters;
	std::string canonicalURI = awsCanonicalURI(resource, queryParameters, true);

	std::string canonicalQueryString;
	if (!queryParameters.empty()) {
		std::sort(queryParameters.begin(), queryParameters.end());
		canonicalQueryString = boost::algorithm::join(queryParameters, "&");
	}

	using namespace boost::algorithm;
	// Create the canonical headers and signed headers
	ASSERT(!headers["Host"].empty());
	// Be careful. There is x-amz-content-sha256 for auth and then
	// x-amz-checksum-sha256 for object integrity check.
	headers["x-amz-content-sha256"] = "UNSIGNED-PAYLOAD";
	headers["x-amz-date"] = amzDate;
	std::vector<std::pair<std::string, std::string>> headersList;
	headersList.push_back({ "host", trim_copy(headers["Host"]) + "\n" });
	if (headers.contains("Content-Type"))
		headersList.push_back({ "content-type", trim_copy(headers["Content-Type"]) + "\n" });
	if (headers.contains("Content-MD5"))
		headersList.push_back({ "content-md5", trim_copy(headers["Content-MD5"]) + "\n" });
	for (const auto& [headerName, headerValue] : headers) {
		if (StringRef(headerName).startsWith("x-amz"_sr))
			headersList.push_back({ to_lower_copy(headerName), trim_copy(headerValue) + "\n" });
	}
	std::sort(headersList.begin(), headersList.end());
	std::string canonicalHeaders;
	std::string signedHeaders;
	for (const auto& [headerName, headerValue] : headersList) {
		canonicalHeaders += headerName + ":" + headerValue;
		signedHeaders += headerName + ";";
	}
	signedHeaders.pop_back();
	std::string canonicalRequest = verb + "\n" + canonicalURI + "\n" + canonicalQueryString + "\n" + canonicalHeaders +
	                               "\n" + signedHeaders + "\n" + headers["x-amz-content-sha256"];

	// ************* TASK 2: CREATE THE STRING TO SIGN*************
	std::string algorithm = "AWS4-HMAC-SHA256";
	std::string credentialScope = dateStamp + "/" + region + "/s3/" + "aws4_request";
	std::string stringToSign =
	    algorithm + "\n" + amzDate + "\n" + credentialScope + "\n" + sha256_hex(canonicalRequest);

	// ************* TASK 3: CALCULATE THE SIGNATURE *************
	// Create the signing key using the function defined above.
	std::string signingKey =
	    hmac_sha256(hmac_sha256(hmac_sha256(hmac_sha256("AWS4" + secretKey, dateStamp), region), "s3"), "aws4_request");
	// Sign the string_to_sign using the signing_key
	std::string signature = hmac_sha256_hex(signingKey, stringToSign);
	// ************* TASK 4: ADD SIGNING INFORMATION TO THE Header *************
	std::string authorizationHeader = algorithm + " " + "Credential=" + accessKey + "/" + credentialScope + ", " +
	                                  "SignedHeaders=" + signedHeaders + ", " + "Signature=" + signature;
	headers["Authorization"] = authorizationHeader;
}

void S3BlobStoreEndpoint::setAuthHeaders(std::string const& verb, std::string const& resource, HTTP::Headers& headers) {
	if (!credentials.present()) {
		return;
	}
	Credentials creds = credentials.get();

	std::string& date = headers["Date"];

	char dateBuf[64];
	time_t ts;
	time(&ts);
	strftime(dateBuf, 64, "%a, %d %b %Y %H:%M:%S GMT", gmtime(&ts));
	date = dateBuf;

	std::string msg;
	msg.append(verb);
	msg.append("\n");
	auto contentMD5 = headers.find("Content-MD5");
	if (contentMD5 != headers.end())
		msg.append(contentMD5->second);
	msg.append("\n");
	auto contentType = headers.find("Content-Type");
	if (contentType != headers.end())
		msg.append(contentType->second);
	msg.append("\n");
	msg.append(date);
	msg.append("\n");
	for (const auto& [headerName, headerValue] : headers) {
		StringRef name = headerName;
		if (name.startsWith("x-amz"_sr) || name.startsWith("x-icloud"_sr)) {
			msg.append(headerName);
			msg.append(":");
			msg.append(headerValue);
			msg.append("\n");
		}
	}

	msg.append(resource);
	if (verb == "GET") {
		size_t q = resource.find_last_of('?');
		if (q != resource.npos)
			msg.resize(msg.size() - (resource.size() - q));
	}

	std::string sig = base64::encoder::from_string(hmac_sha1(creds, msg));
	// base64 encoded blocks end in \n so remove it.
	sig.resize(sig.size() - 1);
	std::string auth = "AWS ";
	auth.append(creds.key);
	auth.append(":");
	auth.append(sig);
	headers["Authorization"] = auth;
}

AsyncResult<std::string> readEntireFile_impl(Reference<S3BlobStoreEndpoint> bstore,
                                             std::string bucket,
                                             std::string object) {
	co_await bstore->requestRateRead->getAllowance(1);

	std::string resource = bstore->constructResourcePath(bucket, object);
	HTTP::Headers headers;
	// Set this header on the GET for it to volunteer saved checksum in the response headers.
	// See https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html#API_GetObject_RequestSyntax
	headers["x-amz-checksum-mode"] = "ENABLED";
	Reference<HTTP::IncomingResponse> r =
	    co_await bstore->doRequest("GET", resource, headers, nullptr, 0, { 200, 404 });
	if (r->code == 404)
		throw file_not_found();
	if (bstore->knobs.enable_object_integrity_check) {
		// Verify the content. We set 'x-amz-checksum-mode' on the GET request above so
		// the server will return the sha256 checksum we set when we uploaded the object in the
		// GET response headers. See
		// https://stackoverflow.com/questions/36540234/what-is-the-difference-getcontentmd5-and-getetag-of-aws-s3-putobjectresult
		std::string checksumSHA256 = r->data.headers["x-amz-checksum-sha256"];
		if (checksumSHA256.empty()) {
			// This is what is thrown elsewhere when no expected etag.
			throw http_bad_response();
		}
		// Calculate the sha256 checksum of the content and compare it to the checksum returned by the server.
		std::string contentSHA256 = sha256_base64(r->data.content);
		if (checksumSHA256 != contentSHA256) {
			throw checksum_failed();
		}
	}
	co_return r->data.content;
}

AsyncResult<std::string> S3BlobStoreEndpoint::readEntireFile(std::string const& bucket, std::string const& object) {
	return readEntireFile_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

Future<Void> writeEntireFileFromBuffer_impl(Reference<S3BlobStoreEndpoint> bstore,
                                            std::string bucket,
                                            std::string object,
                                            UnsentPacketQueue* pContent,
                                            int contentLen,
                                            std::string contentHash) {
	if (contentLen > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	co_await bstore->requestRateWrite->getAllowance(1);
	co_await bstore->concurrentUploads.take();
	FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = bstore->constructResourcePath(bucket, object);
	HTTP::Headers headers;
	// contentHash is calculated by the caller. It is md5 or sha256 dependent on
	// enable_object_integrity_check setting. If the hash (md5 or sha256) we
	// volunteer doesn't match that calculated serverside, the upload fails with:
	// InvalidDigest</Code><Message>The Content-MD5 you specified was invalid.</Message>
	if (bstore->knobs.enable_object_integrity_check) {
		headers["x-amz-checksum-sha256"] = contentHash;
		headers["x-amz-checksum-algorithm"] = "SHA256";
	} else {
		headers["Content-MD5"] = contentHash;
	}
	if (!CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE.empty())
		headers["x-amz-server-side-encryption"] = CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE;
	Reference<HTTP::IncomingResponse> r =
	    co_await bstore->doRequest("PUT", resource, headers, pContent, contentLen, { 200 });
}

Future<Void> S3BlobStoreEndpoint::writeEntireFileFromBuffer(std::string const& bucket,
                                                            std::string const& object,
                                                            UnsentPacketQueue* pContent,
                                                            int contentLen,
                                                            std::string const& contentHash) {
	return writeEntireFileFromBuffer_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, pContent, contentLen, contentHash);
}

Future<int> readObject_impl(Reference<S3BlobStoreEndpoint> bstore,
                            std::string bucket,
                            std::string object,
                            void* data,
                            int length,
                            int64_t offset) {
	try {
		if (length <= 0) {
			TraceEvent(SevWarn, "S3BlobStoreReadObjectEmptyRead").detail("Length", length);
			co_return 0;
		}

		// Log rate limiter state
		co_await bstore->requestRateRead->getAllowance(1);

		std::string resource = bstore->constructResourcePath(bucket, object);
		HTTP::Headers headers;
		headers["Range"] = format("bytes=%lld-%lld", offset, offset + length - 1);

		// Attempt the request
		Reference<HTTP::IncomingResponse> r;
		Reference<HTTP::IncomingResponse> _r =
		    co_await bstore->doRequest("GET", resource, headers, nullptr, 0, { 200, 206, 404 });
		r = _r;

		if (r->code == 404) {
			throw file_not_found();
		}

		// Verify response has content
		if (r->data.contentLen != r->data.content.size()) {
			TraceEvent(SevWarn, "S3BlobStoreReadObjectContentLengthMismatch")
			    .detail("Expected", r->data.contentLen)
			    .detail("Actual", r->data.content.size());
			throw io_error();
		}

		// Copy the output bytes, server could have sent more or less bytes than requested so copy at most length
		// bytes
		int bytesToCopy = std::min<int64_t>(r->data.contentLen, length);
		memcpy(data, r->data.content.data(), bytesToCopy);
		// Return the number of bytes actually copied
		co_return bytesToCopy;
	} catch (Error& e) {
		TraceEvent(SevWarn, "S3BlobStoreEndpoint_ReadError")
		    .error(e)
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("Length", length)
		    .detail("Offset", offset);
		throw;
	}
}

Future<int> S3BlobStoreEndpoint::readObject(std::string const& bucket,
                                            std::string const& object,
                                            void* data,
                                            int length,
                                            int64_t offset) {
	return readObject_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, data, length, offset);
}

static Future<std::string> beginMultiPartUpload_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                     std::string bucket,
                                                     std::string object) {
	co_await bstore->requestRateWrite->getAllowance(1);

	std::string resource = bstore->constructResourcePath(bucket, object);
	resource += "?uploads";
	HTTP::Headers headers;
	if (!CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE.empty())
		headers["x-amz-server-side-encryption"] = CLIENT_KNOBS->BLOBSTORE_ENCRYPTION_TYPE;
	if (bstore->knobs.enable_object_integrity_check) {
		// CRITICAL: Adding x-amz-checksum-algorithm to multipart initiation means ALL parts
		// must include checksums and completion request must include ChecksumSHA256 tags.
		// AWS S3 will reject completion if any part checksum is missing.
		headers["x-amz-checksum-algorithm"] = "SHA256";
	}
	Reference<HTTP::IncomingResponse> r = co_await bstore->doRequest("POST", resource, headers, nullptr, 0, { 200 });

	try {
		xml_document<> doc;
		// Copy content because rapidxml will modify it during parse
		std::string content = r->data.content;

		doc.parse<0>((char*)content.c_str());

		// There should be exactly one node
		xml_node<>* result = doc.first_node();
		if (result != nullptr && strcmp(result->name(), "InitiateMultipartUploadResult") == 0) {
			xml_node<>* id = result->first_node("UploadId");
			if (id != nullptr) {
				co_return id->value();
			}
		}
	} catch (...) {
	}
	throw http_bad_response();
}

Future<std::string> S3BlobStoreEndpoint::beginMultiPartUpload(std::string const& bucket, std::string const& object) {
	return beginMultiPartUpload_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

Future<std::string> uploadPart_impl(Reference<S3BlobStoreEndpoint> bstore,
                                    std::string bucket,
                                    std::string object,
                                    std::string uploadID,
                                    unsigned int partNumber,
                                    UnsentPacketQueue* pContent,
                                    int contentLen,
                                    std::string contentMD5) {
	co_await bstore->requestRateWrite->getAllowance(1);
	co_await bstore->concurrentUploads.take();
	FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = bstore->constructResourcePath(bucket, object);
	resource += format("?partNumber=%d&uploadId=%s", partNumber, uploadID.c_str());
	HTTP::Headers headers;
	// Send hash for content so blobstore can verify it
	// Use SHA256 if integrity check is enabled, otherwise use MD5
	if (bstore->knobs.enable_object_integrity_check) {
		headers["x-amz-checksum-sha256"] = contentMD5;
		headers["x-amz-checksum-algorithm"] = "SHA256";
	} else {
		headers["Content-MD5"] = contentMD5;
	}
	Reference<HTTP::IncomingResponse> r =
	    co_await bstore->doRequest("PUT", resource, headers, pContent, contentLen, { 200 });
	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then
	// the next retry will see error 400.  That could be detected and handled gracefully by retrieving the etag for the
	// successful request.

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	// Only verify MD5 when not using integrity check (SHA256 verification is done by AWS)
	if (!bstore->knobs.enable_object_integrity_check) {
		if (!HTTP::verifyMD5(&r->data, false, contentMD5))
			throw checksum_failed();
	}
	// Note: When using SHA256, AWS handles the verification internally
	// and will return an error if the checksum doesn't match

	// No etag -> bad response.
	std::string etag = r->data.headers["ETag"];
	if (etag.empty())
		throw http_bad_response();

	co_return etag;
}

Future<std::string> S3BlobStoreEndpoint::uploadPart(std::string const& bucket,
                                                    std::string const& object,
                                                    std::string const& uploadID,
                                                    unsigned int partNumber,
                                                    UnsentPacketQueue* pContent,
                                                    int contentLen,
                                                    std::string const& contentMD5) {
	return uploadPart_impl(Reference<S3BlobStoreEndpoint>::addRef(this),
	                       bucket,
	                       object,
	                       uploadID,
	                       partNumber,
	                       pContent,
	                       contentLen,
	                       contentMD5);
}

Future<Optional<std::string>> finishMultiPartUpload_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                         std::string bucket,
                                                         std::string object,
                                                         std::string uploadID,
                                                         S3BlobStoreEndpoint::MultiPartSetT parts,
                                                         int64_t /*totalSize*/) {
	UnsentPacketQueue part_list; // NonCopyable state var so must be declared at top of actor
	co_await bstore->requestRateWrite->getAllowance(1);

	std::string manifest = "<CompleteMultipartUpload>";
	for (auto& p : parts) {
		manifest += format("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag>", p.first, p.second.etag.c_str());
		// Include checksum if integrity check is enabled and checksum is present
		if (bstore->knobs.enable_object_integrity_check && !p.second.checksum.empty()) {
			manifest += format("<ChecksumSHA256>%s</ChecksumSHA256>", p.second.checksum.c_str());
		}
		manifest += "</Part>\n";
	}
	manifest += "</CompleteMultipartUpload>";

	std::string resource = bstore->constructResourcePath(bucket, object);
	resource += format("?uploadId=%s", uploadID.c_str());
	HTTP::Headers headers;
	PacketWriter pw(part_list.getWriteBuffer(manifest.size()), nullptr, Unversioned());
	pw.serializeBytes(manifest);
	Reference<HTTP::IncomingResponse> r =
	    co_await bstore->doRequest("POST", resource, headers, &part_list, manifest.size(), { 200 });

	// The XML response contains a ChecksumSHA256 field, but this is just a hash of the multipart
	// structure, not the actual object content, so it's useless for integrity verification.
	// We skip parsing it to avoid wasted CPU cycles.

	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then
	// the next retry will see error 400.  That could be detected and handled gracefully by HEAD'ing the object before
	// upload to get its (possibly nonexistent) eTag, then if an error 400 is seen then retrieve the eTag again and if
	// it has changed then consider the finish complete.
	co_return Optional<std::string>();
}

Future<Optional<std::string>> S3BlobStoreEndpoint::finishMultiPartUpload(std::string const& bucket,
                                                                         std::string const& object,
                                                                         std::string const& uploadID,
                                                                         MultiPartSetT const& parts,
                                                                         int64_t totalSize) {
	return finishMultiPartUpload_impl(
	    Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, uploadID, parts, totalSize);
}

Future<Void> abortMultiPartUpload_impl(Reference<S3BlobStoreEndpoint> bstore,
                                       std::string bucket,
                                       std::string object,
                                       std::string uploadID) {
	co_await bstore->requestRateWrite->getAllowance(1);

	std::string resource = bstore->constructResourcePath(bucket, object);
	resource += format("?uploadId=%s", uploadID.c_str());

	HTTP::Headers headers;
	Reference<HTTP::IncomingResponse> r =
	    co_await bstore->doRequest("DELETE", resource, headers, nullptr, 0, { 200, 204 });
}

Future<Void> S3BlobStoreEndpoint::abortMultiPartUpload(std::string const& bucket,
                                                       std::string const& object,
                                                       std::string const& uploadID) {
	return abortMultiPartUpload_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, uploadID);
}

// Forward declarations
Future<std::map<std::string, std::string>> getObjectTags_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                              std::string bucket,
                                                              std::string object);

Future<Void> putObjectTags_impl(Reference<S3BlobStoreEndpoint> bstore,
                                std::string bucket,
                                std::string object,
                                std::map<std::string, std::string> tags) {
	UnsentPacketQueue packets;
	co_await bstore->requestRateWrite->getAllowance(1);
	std::string resource = bstore->constructResourcePath(bucket, object);
	resource += "?tagging";
	int maxRetries = 5;
	int retryCount = 0;
	double backoff = 1.0;
	double maxBackoff = 8.0;

	while (true) {
		Error err;
		try {
			std::string manifest = "<Tagging xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><TagSet>";
			for (auto itr = tags.begin(); itr != tags.end(); ++itr) {
				manifest += "<Tag><Key>" + itr->first + "</Key><Value>" + itr->second + "</Value></Tag>";
			}
			manifest += "</TagSet></Tagging>";

			PacketWriter pw(packets.getWriteBuffer(manifest.size()), nullptr, Unversioned());
			pw.serializeBytes(manifest);

			HTTP::Headers headers;
			headers["Content-Type"] = "application/xml";
			headers["Content-Length"] = format("%d", manifest.size());

			Reference<HTTP::IncomingResponse> r =
			    co_await bstore->doRequest("PUT", resource, headers, &packets, manifest.size(), { 200 });

			// Verify tags were written correctly
			std::map<std::string, std::string> verifyTags = co_await getObjectTags_impl(bstore, bucket, object);
			if (verifyTags == tags) {
				co_return;
			}

			if (++retryCount >= maxRetries) {
				TraceEvent(SevWarn, "S3BlobStorePutTagsMaxRetriesExceeded")
				    .detail("Bucket", bucket)
				    .detail("Object", object);
				throw operation_failed();
			}

			// Implement exponential backoff with jitter
			co_await delay(backoff * (0.9 + 0.2 * deterministicRandom()->random01()));
			backoff = std::min(backoff * 2, maxBackoff);
			continue;
		} catch (Error& e) {
			err = e;
		}

		if (err.code() == error_code_actor_cancelled) {
			throw err;
		}

		TraceEvent(SevWarn, "S3BlobStorePutTagsError")
		    .error(err)
		    .detail("Bucket", bucket)
		    .detail("Object", object)
		    .detail("RetryCount", retryCount);

		if (++retryCount >= maxRetries) {
			throw err;
		}

		// Implement exponential backoff with jitter for errors
		co_await delay(backoff * (0.9 + 0.2 * deterministicRandom()->random01()));
		backoff = std::min(backoff * 2, maxBackoff);
	}
}

Future<Void> S3BlobStoreEndpoint::putObjectTags(std::string const& bucket,
                                                std::string const& object,
                                                std::map<std::string, std::string> const& tags) {
	return putObjectTags_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object, tags);
}

Future<std::map<std::string, std::string>> getObjectTags_impl(Reference<S3BlobStoreEndpoint> bstore,
                                                              std::string bucket,
                                                              std::string object) {
	co_await bstore->requestRateRead->getAllowance(1);

	std::string resource = bstore->constructResourcePath(bucket, object);
	resource += "?tagging";
	HTTP::Headers headers;

	Reference<HTTP::IncomingResponse> r = co_await bstore->doRequest("GET", resource, headers, nullptr, 0, { 200 });

	rapidxml::xml_document<> doc;
	doc.parse<rapidxml::parse_default>((char*)r->data.content.c_str());

	std::map<std::string, std::string> tags;

	// Find the Tagging node (with or without namespace)
	rapidxml::xml_node<>* tagging = doc.first_node();
	while (tagging && strcmp(tagging->name(), "Tagging") != 0) {
		tagging = tagging->next_sibling();
	}

	if (tagging) {
		// Find TagSet node
		rapidxml::xml_node<>* tagSet = tagging->first_node();
		while (tagSet && strcmp(tagSet->name(), "TagSet") != 0) {
			tagSet = tagSet->next_sibling();
		}

		if (tagSet) {
			// Iterate through Tag nodes
			for (rapidxml::xml_node<>* tag = tagSet->first_node(); tag; tag = tag->next_sibling()) {
				if (strcmp(tag->name(), "Tag") == 0) {
					std::string key, value;
					// Find Key and Value nodes
					for (rapidxml::xml_node<>* node = tag->first_node(); node; node = node->next_sibling()) {
						if (strcmp(node->name(), "Key") == 0) {
							key = node->value();
						} else if (strcmp(node->name(), "Value") == 0) {
							value = node->value();
						}
					}
					if (!key.empty()) {
						tags[key] = value;
					}
				}
			}
		}
	}

	co_return tags;
}

Future<std::map<std::string, std::string>> S3BlobStoreEndpoint::getObjectTags(std::string const& bucket,
                                                                              std::string const& object) {
	return getObjectTags_impl(Reference<S3BlobStoreEndpoint>::addRef(this), bucket, object);
}

TEST_CASE("/backup/s3/v4headers") {
	std::string credStr = "AKIAIOSFODNN7EXAMPLE:wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY:";
	Optional<StringRef> creds = StringRef(credStr);
	// GET without query parameters
	{
		S3BlobStoreEndpoint s3("s3.amazonaws.com", "443", "amazonaws", "proxy", "port", creds);
		std::string verb("GET");
		std::string resource("/test.txt");
		HTTP::Headers headers;
		headers["Host"] = "s3.amazonaws.com";
		s3.setV4AuthHeaders(verb, resource, headers, "20130524T000000Z", "20130524");
		ASSERT(headers["Authorization"] ==
		       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/amazonaws/s3/aws4_request, "
		       "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
		       "Signature=c6037f4b174f2019d02d7085a611cef8adfe1efe583e220954dc85d59cd31ba3");
		ASSERT(headers["x-amz-date"] == "20130524T000000Z");
	}

	// GET with query parameters
	{
		S3BlobStoreEndpoint s3("s3.amazonaws.com", "443", "amazonaws", "proxy", "port", creds);
		std::string verb("GET");
		std::string resource("/test/examplebucket?Action=DescribeRegions&Version=2013-10-15");
		HTTP::Headers headers;
		headers["Host"] = "s3.amazonaws.com";
		s3.setV4AuthHeaders(verb, resource, headers, "20130524T000000Z", "20130524");
		ASSERT(headers["Authorization"] ==
		       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/amazonaws/s3/aws4_request, "
		       "SignedHeaders=host;x-amz-content-sha256;x-amz-date, "
		       "Signature=426f04e71e191fbc30096c306fe1b11ce8f026a7be374541862bbee320cce71c");
		ASSERT(headers["x-amz-date"] == "20130524T000000Z");
	}

	// POST
	{
		S3BlobStoreEndpoint s3("s3.us-west-2.amazonaws.com", "443", "us-west-2", "proxy", "port", creds);
		std::string verb("POST");
		std::string resource("/simple.json");
		HTTP::Headers headers;
		headers["Host"] = "s3.us-west-2.amazonaws.com";
		headers["Content-Type"] = "Application/x-amz-json-1.0";
		s3.setV4AuthHeaders(verb, resource, headers, "20130524T000000Z", "20130524");
		ASSERT(headers["Authorization"] ==
		       "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-west-2/s3/aws4_request, "
		       "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date, "
		       "Signature=cf095e36bed9cd3139c2e8b3e20c296a79d8540987711bf3a0d816b19ae00314");
		ASSERT(headers["x-amz-date"] == "20130524T000000Z");
		ASSERT(headers["Host"] == "s3.us-west-2.amazonaws.com");
		ASSERT(headers["Content-Type"] == "Application/x-amz-json-1.0");
	}

	return Void();
}

TEST_CASE("/backup/s3/guess_region") {
	std::string url = "blobstore://s3.us-west-2.amazonaws.com/resource_name?bucket=bucket_name&sa=1";

	std::string resource;
	std::string error;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);
	ASSERT(s3->getRegion() == "us-west-2");

	url = "blobstore://s3.us-west-2.amazonaws.com/resource_name?bucket=bucket_name&sc=922337203685477580700";
	try {
		s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);
		ASSERT(false); // not reached
	} catch (Error& e) {
		// conversion of 922337203685477580700 to long int will overflow
		ASSERT_EQ(e.code(), error_code_backup_invalid_url);
	}
	return Void();
}

TEST_CASE("/backup/s3/virtual_hosting_list_resource_path") {
	// Test that listObjectsStream_impl constructs valid HTTP resource paths
	// for virtual hosting mode URLs with path prefixes.
	//
	// Regression test for commit 15dd76a7f9 which accidentally removed the leading "/"
	// from the query string when switching from ListObjectsV1 to ListObjectsV2.
	//
	// For URL: blobstore://@bucket.s3.region.amazonaws.com/prefix/path?bucket=...
	// constructResourcePath(bucket, "") returns "" in virtual hosting mode.
	// We must ensure the final resource starts with "/" before the query string.

	std::string url = "blobstore://@test-bucket.s3.us-west-2.amazonaws.com/bulkload/path?bucket=test-bucket";
	std::string resource;
	std::string error;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);

	ASSERT(s3.isValid());
	ASSERT(resource == "bulkload/path"); // Path prefix extracted from URL (without leading '/')

	// In virtual hosting mode, constructResourcePath returns "" for empty object
	std::string listResource = s3->constructResourcePath("test-bucket", "");
	ASSERT(listResource.empty()); // Virtual hosting mode doesn't include bucket in path

	// listObjectsStream_impl should add "/" before query string to form valid HTTP request
	// Expected: GET /bulkload/path?list-type=2&... (but we can't test the full actor here)
	// We can only verify that constructResourcePath returns "" as expected.
	// The fix in listObjectsStream_impl checks if resource.empty() and sets it to "/" before
	// appending the query string, ensuring the final request is well-formed.

	// Test non-virtual hosting mode for comparison
	url = "blobstore://s3.us-west-2.amazonaws.com/resource_path?bucket=test-bucket";
	s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);
	ASSERT(s3.isValid());
	ASSERT(resource == "resource_path"); // Path extracted from URL (without leading '/')

	// In non-virtual hosting mode, constructResourcePath includes bucket
	listResource = s3->constructResourcePath("test-bucket", "");
	ASSERT(listResource == "/test-bucket"); // Bucket is part of path

	return Void();
}

TEST_CASE("/backup/s3/constructResourcePath") {
	// Test that leading slashes in object keys are properly stripped
	// S3 object keys should not start with '/', but if passed, we normalize them

	std::string url = "blobstore://s3.us-west-2.amazonaws.com?bucket=test-bucket";
	std::string resource;
	std::string error;
	S3BlobStoreEndpoint::ParametersT parameters;
	Reference<S3BlobStoreEndpoint> s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);

	// Test normal object key (no leading slash)
	ASSERT(s3->constructResourcePath("test-bucket", "normal/path/file.txt") == "/test-bucket/normal/path/file.txt");

	// Test single leading slash - should be stripped
	ASSERT(s3->constructResourcePath("test-bucket", "/leading/slash/file.txt") ==
	       "/test-bucket/leading/slash/file.txt");

	// Test multiple leading slashes - all should be stripped
	ASSERT(s3->constructResourcePath("test-bucket", "///multiple/slashes.txt") == "/test-bucket/multiple/slashes.txt");

	// Test object key that is only slashes - should result in empty object path
	ASSERT(s3->constructResourcePath("test-bucket", "///") == "/test-bucket");

	// Test empty object key
	ASSERT(s3->constructResourcePath("test-bucket", "") == "/test-bucket");

	// Test virtual hosting mode (bucket in hostname)
	url = "blobstore://test-bucket.s3.us-west-2.amazonaws.com";
	s3 = S3BlobStoreEndpoint::fromString(url, {}, &resource, &error, &parameters);

	// Virtual hosting mode doesn't include bucket in path
	ASSERT(s3->constructResourcePath("test-bucket", "normal/file.txt") == "/normal/file.txt");
	ASSERT(s3->constructResourcePath("test-bucket", "/leading/slash.txt") == "/leading/slash.txt");
	ASSERT(s3->constructResourcePath("test-bucket", "").empty());

	return Void();
}

TEST_CASE("/backup/s3/parseErrorCodeFromS3") {
	// Valid S3 error XML — should extract the error code
	{
		std::string xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
		                  "<Error><Code>AccessDenied</Code>"
		                  "<Message>Access Denied</Message></Error>";
		ASSERT(parseErrorCodeFromS3(xml) == "AccessDenied");
	}

	// InvalidToken error
	{
		std::string xml = "<Error><Code>InvalidToken</Code>"
		                  "<Message>The provided token is malformed or otherwise invalid.</Message></Error>";
		ASSERT(parseErrorCodeFromS3(xml) == "InvalidToken");
	}

	// ExpiredToken error
	{
		std::string xml = "<Error><Code>ExpiredToken</Code><Message>Token expired</Message></Error>";
		ASSERT(parseErrorCodeFromS3(xml) == "ExpiredToken");
	}

	// Missing <Code> node — should return ""
	{
		std::string xml = "<Error><Message>Something went wrong</Message></Error>";
		ASSERT(parseErrorCodeFromS3(xml).empty());
	}

	// No <Error> root — should return ""
	{
		std::string xml = "<ListBucketResult><Name>bucket</Name></ListBucketResult>";
		ASSERT(parseErrorCodeFromS3(xml).empty());
	}

	// Empty response — should return ""
	{
		ASSERT(parseErrorCodeFromS3("").empty());
	}

	// HTML error page from load balancer (502/503) — should return "", not throw
	{
		std::string html = "<html><body><h1>502 Bad Gateway</h1></body></html>";
		ASSERT(parseErrorCodeFromS3(html).empty());
	}

	// Completely invalid / garbage — should return "", not throw
	{
		ASSERT(parseErrorCodeFromS3("not xml at all {{{").empty());
	}

	// Partial XML — should return "", not throw
	{
		ASSERT(parseErrorCodeFromS3("<Error><Code>Incomple").empty());
	}

	// Plain text error — should return "", not throw
	{
		ASSERT(parseErrorCodeFromS3("Internal Server Error").empty());
	}

	return Void();
}
