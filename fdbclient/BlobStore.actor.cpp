/*
 * BlobStore.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/BlobStore.h"

#include "fdbclient/md5/md5.h"
#include "fdbclient/libb64/encode.h"
#include "fdbclient/sha1/SHA1.h"
#include <time.h>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "fdbrpc/IAsyncFile.h"
#include "fdbclient/rapidxml/rapidxml.hpp"
#include "flow/actorcompiler.h" // has to be last include

using namespace rapidxml;

json_spirit::mObject BlobStoreEndpoint::Stats::getJSON() {
	json_spirit::mObject o;

	o["requests_failed"] = requests_failed;
	o["requests_successful"] = requests_successful;
	o["bytes_sent"] = bytes_sent;

	return o;
}

BlobStoreEndpoint::Stats BlobStoreEndpoint::Stats::operator-(const Stats &rhs) {
	Stats r;
	r.requests_failed = requests_failed - rhs.requests_failed;
	r.requests_successful = requests_successful - rhs.requests_successful;
	r.bytes_sent = bytes_sent - rhs.bytes_sent;
	return r;
}

BlobStoreEndpoint::Stats BlobStoreEndpoint::s_stats;

BlobStoreEndpoint::BlobKnobs::BlobKnobs() {
	secure_connection = 1;
	connect_tries = CLIENT_KNOBS->BLOBSTORE_CONNECT_TRIES;
	connect_timeout = CLIENT_KNOBS->BLOBSTORE_CONNECT_TIMEOUT;
	max_connection_life = CLIENT_KNOBS->BLOBSTORE_MAX_CONNECTION_LIFE;
	request_tries = CLIENT_KNOBS->BLOBSTORE_REQUEST_TRIES;
	request_timeout_min = CLIENT_KNOBS->BLOBSTORE_REQUEST_TIMEOUT_MIN;
	requests_per_second = CLIENT_KNOBS->BLOBSTORE_REQUESTS_PER_SECOND;
	concurrent_requests = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS;
	list_requests_per_second = CLIENT_KNOBS->BLOBSTORE_LIST_REQUESTS_PER_SECOND;
	write_requests_per_second = CLIENT_KNOBS->BLOBSTORE_WRITE_REQUESTS_PER_SECOND;
	read_requests_per_second = CLIENT_KNOBS->BLOBSTORE_READ_REQUESTS_PER_SECOND;
	delete_requests_per_second = CLIENT_KNOBS->BLOBSTORE_DELETE_REQUESTS_PER_SECOND;
	multipart_max_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MAX_PART_SIZE;
	multipart_min_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	concurrent_uploads = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_UPLOADS;
	concurrent_lists = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_LISTS;
	concurrent_reads_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_READS_PER_FILE;
	concurrent_writes_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
	read_block_size = CLIENT_KNOBS->BLOBSTORE_READ_BLOCK_SIZE;
	read_ahead_blocks = CLIENT_KNOBS->BLOBSTORE_READ_AHEAD_BLOCKS;
	read_cache_blocks_per_file = CLIENT_KNOBS->BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	max_send_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	max_recv_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;
}

bool BlobStoreEndpoint::BlobKnobs::set(StringRef name, int value) {
	#define TRY_PARAM(n, sn) if(name == LiteralStringRef(#n) || name == LiteralStringRef(#sn)) { n = value; return true; }
	TRY_PARAM(secure_connection, sc)
	TRY_PARAM(connect_tries, ct);
	TRY_PARAM(connect_timeout, cto);
	TRY_PARAM(max_connection_life, mcl);
	TRY_PARAM(request_tries, rt);
	TRY_PARAM(request_timeout_min, rtom);
	// TODO: For backward compatibility because request_timeout was renamed to request_timeout_min
	if(name == LiteralStringRef("request_timeout") || name == LiteralStringRef("rto")) { request_timeout_min = value; return true; }
	TRY_PARAM(requests_per_second, rps);
	TRY_PARAM(list_requests_per_second, lrps);
	TRY_PARAM(write_requests_per_second, wrps);
	TRY_PARAM(read_requests_per_second, rrps);
	TRY_PARAM(delete_requests_per_second, drps);
	TRY_PARAM(concurrent_requests, cr);
	TRY_PARAM(multipart_max_part_size, maxps);
	TRY_PARAM(multipart_min_part_size, minps);
	TRY_PARAM(concurrent_uploads, cu);
	TRY_PARAM(concurrent_lists, cl);
	TRY_PARAM(concurrent_reads_per_file, crpf);
	TRY_PARAM(concurrent_writes_per_file, cwpf);
	TRY_PARAM(read_block_size, rbs);
	TRY_PARAM(read_ahead_blocks, rab);
	TRY_PARAM(read_cache_blocks_per_file, rcb);
	TRY_PARAM(max_send_bytes_per_second, sbps);
	TRY_PARAM(max_recv_bytes_per_second, rbps);
	#undef TRY_PARAM
	return false;
}

// Returns a Blob URL parameter string that specifies all of the non-default options for the endpoint using option short names.
std::string BlobStoreEndpoint::BlobKnobs::getURLParameters() const {
	static BlobKnobs defaults;
	std::string r;
	#define _CHECK_PARAM(n, sn) if(n != defaults. n) { r += format("%s%s=%d", r.empty() ? "" : "&", #sn, n); }
	_CHECK_PARAM(secure_connection, sc);
	_CHECK_PARAM(connect_tries, ct);
	_CHECK_PARAM(connect_timeout, cto);
	_CHECK_PARAM(max_connection_life, mcl);
	_CHECK_PARAM(request_tries, rt);
	_CHECK_PARAM(request_timeout_min, rto);
	_CHECK_PARAM(requests_per_second, rps);
	_CHECK_PARAM(list_requests_per_second, lrps);
	_CHECK_PARAM(write_requests_per_second, wrps);
	_CHECK_PARAM(read_requests_per_second, rrps);
	_CHECK_PARAM(delete_requests_per_second, drps);
	_CHECK_PARAM(concurrent_requests, cr);
	_CHECK_PARAM(multipart_max_part_size, maxps);
	_CHECK_PARAM(multipart_min_part_size, minps);
	_CHECK_PARAM(concurrent_uploads, cu);
	_CHECK_PARAM(concurrent_lists, cl);
	_CHECK_PARAM(concurrent_reads_per_file, crpf);
	_CHECK_PARAM(concurrent_writes_per_file, cwpf);
	_CHECK_PARAM(read_block_size, rbs);
	_CHECK_PARAM(read_ahead_blocks, rab);
	_CHECK_PARAM(read_cache_blocks_per_file, rcb);
	_CHECK_PARAM(max_send_bytes_per_second, sbps);
	_CHECK_PARAM(max_recv_bytes_per_second, rbps);
	#undef _CHECK_PARAM
	return r;
}

Reference<BlobStoreEndpoint> BlobStoreEndpoint::fromString(std::string const &url, std::string *resourceFromURL, std::string *error, ParametersT *ignored_parameters) {
	if(resourceFromURL)
		resourceFromURL->clear();

	try {
		StringRef t(url);
		StringRef prefix = t.eat("://");
		if(prefix != LiteralStringRef("blobstore"))
			throw format("Invalid blobstore URL prefix '%s'", prefix.toString().c_str());
		StringRef cred = t.eat("@");
		uint8_t foundSeparator = 0;
		StringRef hostPort = t.eatAny("/?", &foundSeparator);
		StringRef resource;
		if(foundSeparator == '/') {
			 resource = t.eat("?");
		}

		// hostPort is at least a host or IP address, optionally followed by :portNumber or :serviceName
		StringRef h(hostPort);
		StringRef host = h.eat(":");
		if(host.size() == 0)
			throw std::string("host cannot be empty");

		StringRef service = h.eat();

		BlobKnobs knobs;
		HTTP::Headers extraHeaders;
		while(1) {
			StringRef name = t.eat("=");
			if(name.size() == 0)
				break;
			StringRef value = t.eat("&");

			// Special case for header
			if(name == LiteralStringRef("header")) {
				StringRef originalValue = value;
				StringRef headerFieldName = value.eat(":");
				StringRef headerFieldValue = value;
				if(headerFieldName.size() == 0 || headerFieldValue.size() == 0) {
					throw format("'%s' is not a valid value for '%s' parameter.  Format is <FieldName>:<FieldValue> where strings are not empty.", originalValue.toString().c_str(), name.toString().c_str());
				}
				std::string &fieldValue = extraHeaders[headerFieldName.toString()];
				// RFC 2616 section 4.2 says header field names can repeat but only if it is valid to concatenate their values with comma separation
				if(!fieldValue.empty()) {
					fieldValue.append(",");
				}
				fieldValue.append(headerFieldValue.toString());
				continue;
			}

			// See if the parameter is a knob
			// First try setting a dummy value (all knobs are currently numeric) just to see if this parameter is known to BlobStoreEndpoint.
			// If it is, then we will set it to a good value or throw below, so the dummy set has no bad side effects.
			bool known = knobs.set(name, 0);

			// If the parameter is not known to BlobStoreEndpoint then throw unless there is an ignored_parameters set to add it to
			if(!known) {
				if(ignored_parameters == nullptr) {
					throw format("%s is not a valid parameter name", name.toString().c_str());
				}
				(*ignored_parameters)[name.toString()] = value.toString();
				continue;
			}

			// The parameter is known to BlobStoreEndpoint so it must be numeric and valid.
			char *valueEnd;
			int ivalue = strtol(value.toString().c_str(), &valueEnd, 10);
			if(*valueEnd || (ivalue == 0 && value.toString() != "0"))
				throw format("%s is not a valid value for %s", value.toString().c_str(), name.toString().c_str());

			// It should not be possible for this set to fail now since the dummy set above had to have worked.
			ASSERT(knobs.set(name, ivalue));
		}

		if(resourceFromURL != nullptr)
			*resourceFromURL = resource.toString();

		StringRef c(cred);
		StringRef key = c.eat(":");
		StringRef secret = c.eat();

		return Reference<BlobStoreEndpoint>(new BlobStoreEndpoint(host.toString(), service.toString(), key.toString(), secret.toString(), knobs, extraHeaders));

	} catch(std::string &err) {
		if(error != nullptr)
			*error = err;
		TraceEvent(SevWarnAlways, "BlobStoreEndpointBadURL").suppressFor(60).detail("Description", err).detail("Format", getURLFormat()).detail("URL", url);
		throw backup_invalid_url();
	}
}

std::string BlobStoreEndpoint::getResourceURL(std::string resource, std::string params) {
	std::string hostPort = host;
	if(!service.empty()) {
		hostPort.append(":");
		hostPort.append(service);
	}

	// If secret isn't being looked up from credentials files then it was passed explicitly in th URL so show it here.
	std::string s;
	if(!lookupSecret)
		s = std::string(":") + secret;

	std::string r = format("blobstore://%s%s@%s/%s", key.c_str(), s.c_str(), hostPort.c_str(), resource.c_str());

	// Get params that are deviations from knob defaults
	std::string knobParams = knobs.getURLParameters();
	if(!knobParams.empty()) {
		if(!params.empty()) {
			params.append("&");
		}
		params.append(knobParams);
	}

	for(auto &kv : extraHeaders) {
		if(!params.empty()) {
			params.append("&");
		}
		params.append("header=");
		params.append(HTTP::urlEncode(kv.first));
		params.append(":");
		params.append(HTTP::urlEncode(kv.second));
	}

	if(!params.empty())
		r.append("?").append(params);

	return r;
}

ACTOR Future<bool> bucketExists_impl(Reference<BlobStoreEndpoint> b, std::string bucket) {
	wait(b->requestRateRead->getAllowance(1));

	std::string resource = std::string("/") + bucket;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("HEAD", resource, headers, nullptr, 0, {200, 404}));
	return r->code == 200;
}

Future<bool> BlobStoreEndpoint::bucketExists(std::string const &bucket) {
	return bucketExists_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<bool> objectExists_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	wait(b->requestRateRead->getAllowance(1));

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("HEAD", resource, headers, nullptr, 0, {200, 404}));
	return r->code == 200;
}

Future<bool> BlobStoreEndpoint::objectExists(std::string const &bucket, std::string const &object) {
	return objectExists_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteObject_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	wait(b->requestRateDelete->getAllowance(1));

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	// 200 or 204 means object successfully deleted, 404 means it already doesn't exist, so any of those are considered successful
	Reference<HTTP::Response> r = wait(b->doRequest("DELETE", resource, headers, nullptr, 0, {200, 204, 404}));

	// But if the object already did not exist then the 'delete' is assumed to be successful but a warning is logged.
	if(r->code == 404) {
		TraceEvent(SevWarnAlways, "BlobStoreEndpointDeleteObjectMissing")
			.detail("Host", b->host)
			.detail("Bucket", bucket)
			.detail("Object", object);
	}

	return Void();
}

Future<Void> BlobStoreEndpoint::deleteObject(std::string const &bucket, std::string const &object) {
	return deleteObject_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteRecursively_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string prefix, int *pNumDeleted, int64_t *pBytesDeleted) {
	state PromiseStream<BlobStoreEndpoint::ListResult> resultStream;
	// Start a recursive parallel listing which will send results to resultStream as they are received
	state Future<Void> done = b->listObjectsStream(bucket, resultStream, prefix, '/', std::numeric_limits<int>::max());
	// Wrap done in an actor which will send end_of_stream since listObjectsStream() does not (so that many calls can write to the same stream)
	done = map(done, [=](Void) {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	state std::list<Future<Void>> deleteFutures;
	try {
		loop {
			choose {
				// Throw if done throws, otherwise don't stop until end_of_stream
				when(wait(done)) {
					done = Never();
				}

				when(BlobStoreEndpoint::ListResult list = waitNext(resultStream.getFuture())) {
					for(auto &object : list.objects) {
						deleteFutures.push_back(map(b->deleteObject(bucket, object.name), [=](Void) -> Void {
							if(pNumDeleted != nullptr) {
								++*pNumDeleted;
							}
							if(pBytesDeleted != nullptr) {
								*pBytesDeleted += object.size;
							}
							return Void();
						}));
					}
				}
			}

			// This is just a precaution to avoid having too many outstanding delete actors waiting to run
			while(deleteFutures.size() > CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS) {
				wait(deleteFutures.front());
				deleteFutures.pop_front();
			}
		}
	} catch(Error &e) {
		if(e.code() != error_code_end_of_stream)
			throw;
	}

	while(deleteFutures.size() > 0) {
		wait(deleteFutures.front());
		deleteFutures.pop_front();
	}

	return Void();
}

Future<Void> BlobStoreEndpoint::deleteRecursively(std::string const &bucket, std::string prefix, int *pNumDeleted, int64_t *pBytesDeleted) {
	return deleteRecursively_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, prefix, pNumDeleted, pBytesDeleted);
}

ACTOR Future<Void> createBucket_impl(Reference<BlobStoreEndpoint> b, std::string bucket) {
	wait(b->requestRateWrite->getAllowance(1));

	bool exists = wait(b->bucketExists(bucket));
	if(!exists) {
		std::string resource = std::string("/") + bucket;
		HTTP::Headers headers;
		Reference<HTTP::Response> r = wait(b->doRequest("PUT", resource, headers, nullptr, 0, {200, 409}));
	}
	return Void();
}

Future<Void> BlobStoreEndpoint::createBucket(std::string const &bucket) {
	return createBucket_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<int64_t> objectSize_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	wait(b->requestRateRead->getAllowance(1));

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("HEAD", resource, headers, nullptr, 0, {200, 404}));
	if(r->code == 404)
		throw file_not_found();
	return r->contentLen;
}

Future<int64_t> BlobStoreEndpoint::objectSize(std::string const &bucket, std::string const &object) {
	return objectSize_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

// Try to read a file, parse it as JSON, and return the resulting document.
// It will NOT throw if any errors are encountered, it will just return an empty
// JSON object and will log trace events for the errors encountered.
ACTOR Future<Optional<json_spirit::mObject>> tryReadJSONFile(std::string path) {
	state std::string content;

	// Event type to be logged in the event of an exception
	state const char *errorEventType = "BlobCredentialFileError";

	try {
		state Reference<IAsyncFile> f = wait(IAsyncFileSystem::filesystem()->open(path, IAsyncFile::OPEN_NO_AIO | IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0));
		state int64_t size = wait(f->size());
		state Standalone<StringRef> buf = makeString(size);
		int r = wait(f->read(mutateString(buf), size, 0));
		ASSERT(r == size);
		content = buf.toString();

		// Any exceptions from hehre forward are parse failures
		errorEventType = "BlobCredentialFileParseFailed";
		json_spirit::mValue json;
		json_spirit::read_string(content, json);
		if(json.type() == json_spirit::obj_type)
			return json.get_obj();
		else
			TraceEvent(SevWarn, "BlobCredentialFileNotJSONObject").suppressFor(60).detail("File", path);

	} catch(Error &e) {
		if(e.code() != error_code_actor_cancelled)
			TraceEvent(SevWarn, errorEventType).error(e).suppressFor(60).detail("File", path);
	}

	return Optional<json_spirit::mObject>();
}

ACTOR Future<Void> updateSecret_impl(Reference<BlobStoreEndpoint> b) {
	std::vector<std::string> *pFiles = (std::vector<std::string> *)g_network->global(INetwork::enBlobCredentialFiles);
	if(pFiles == nullptr)
		return Void();

	state std::vector<Future<Optional<json_spirit::mObject>>> reads;
	for(auto &f : *pFiles)
		reads.push_back(tryReadJSONFile(f));

	wait(waitForAll(reads));

	std::string key = b->key + "@" + b->host;

	int invalid = 0;

	for(auto &f : reads) {
		// If value not present then the credentials file wasn't readable or valid.  Continue to check other results.
		if(!f.get().present()) {
			++invalid;
			continue;
		}

		JSONDoc doc(f.get().get());
		if(doc.has("accounts") && doc.last().type() == json_spirit::obj_type) {
			JSONDoc accounts(doc.last().get_obj());
			if(accounts.has(key, false) && accounts.last().type() == json_spirit::obj_type) {
				JSONDoc account(accounts.last());
				std::string secret;
				// Once we find a matching account, use it.
				if(account.tryGet("secret", secret)) {
					b->secret = secret;
					return Void();
				}
			}
		}
	}

	// If any sources were invalid
	if(invalid > 0)
		throw backup_auth_unreadable();

	// All sources were valid but didn't contain the desired info
	throw backup_auth_missing();
}

Future<Void> BlobStoreEndpoint::updateSecret() {
	return updateSecret_impl(Reference<BlobStoreEndpoint>::addRef(this));
}

ACTOR Future<BlobStoreEndpoint::ReusableConnection> connect_impl(Reference<BlobStoreEndpoint> b) {
	// First try to get a connection from the pool
	while(!b->connectionPool.empty()) {
		BlobStoreEndpoint::ReusableConnection rconn = b->connectionPool.front();
		b->connectionPool.pop();

		// If the connection expires in the future then return it
		if(rconn.expirationTime > now()) {
		TraceEvent("BlobStoreEndpointReusingConnected").suppressFor(60)
			.detail("RemoteEndpoint", rconn.conn->getPeerAddress())
			.detail("ExpiresIn", rconn.expirationTime - now());
			return rconn;
		}
	}
	std::string service = b->service;
	if (service.empty())
		service = b->knobs.secure_connection ? "https" : "http";
	state Reference<IConnection> conn = wait(INetworkConnections::net()->connect(b->host, service, b->knobs.secure_connection ? true : false));
	wait(conn->connectHandshake());

	TraceEvent("BlobStoreEndpointNewConnection").suppressFor(60)
		.detail("RemoteEndpoint", conn->getPeerAddress())
		.detail("ExpiresIn", b->knobs.max_connection_life);

	if(b->lookupSecret)
		wait(b->updateSecret());

	return BlobStoreEndpoint::ReusableConnection({conn, now() + b->knobs.max_connection_life});
}

Future<BlobStoreEndpoint::ReusableConnection> BlobStoreEndpoint::connect() {
	return connect_impl(Reference<BlobStoreEndpoint>::addRef(this));
}

void BlobStoreEndpoint::returnConnection(ReusableConnection &rconn) {
	// If it expires in the future then add it to the pool in the front
	if(rconn.expirationTime > now())
		connectionPool.push(rconn);
	rconn.conn = Reference<IConnection>();
}

// Do a request, get a Response.
// Request content is provided as UnsentPacketQueue *pContent which will be depleted as bytes are sent but the queue itself must live for the life of this actor
// and be destroyed by the caller
ACTOR Future<Reference<HTTP::Response>> doRequest_impl(Reference<BlobStoreEndpoint> bstore, std::string verb, std::string resource, HTTP::Headers headers, UnsentPacketQueue *pContent, int contentLen, std::set<unsigned int> successCodes) {
	state UnsentPacketQueue contentCopy;

	headers["Content-Length"] = format("%d", contentLen);
	headers["Host"] = bstore->host;
	headers["Accept"] = "application/xml";

	// Merge extraHeaders into headers
	for(auto &kv : bstore->extraHeaders) {
		std::string &fieldValue = headers[kv.first];
		if(!fieldValue.empty()) {
			fieldValue.append(",");
		}
		fieldValue.append(kv.second);
	}

	// For requests with content to upload, the request timeout should be at least twice the amount of time
	// it would take to upload the content given the upload bandwidth and concurrency limits.
	int bandwidthThisRequest = 1 + bstore->knobs.max_send_bytes_per_second / bstore->knobs.concurrent_uploads;
	int contentUploadSeconds = contentLen / bandwidthThisRequest;
	state int requestTimeout = std::max(bstore->knobs.request_timeout_min, 3 * contentUploadSeconds);

	wait(bstore->concurrentRequests.take());
	state FlowLock::Releaser globalReleaser(bstore->concurrentRequests, 1);

	state int maxTries = std::min(bstore->knobs.request_tries, bstore->knobs.connect_tries);
	state int thisTry = 1;
	state double nextRetryDelay = 2.0;

	loop {
		state Optional<Error> err;
		state Optional<NetworkAddress> remoteAddress;
		state bool connectionEstablished = false;
		state Reference<HTTP::Response> r;

		try {
			// Start connecting
			Future<BlobStoreEndpoint::ReusableConnection> frconn = bstore->connect();

			// Make a shallow copy of the queue by calling addref() on each buffer in the chain and then prepending that chain to contentCopy
			contentCopy.discardAll();
			if(pContent != nullptr) {
				PacketBuffer *pFirst = pContent->getUnsent();
				PacketBuffer *pLast = nullptr;
				for(PacketBuffer *p = pFirst; p != nullptr; p = p->nextPacketBuffer()) {
					p->addref();
					// Also reset the sent count on each buffer
					p->bytes_sent = 0;
					pLast = p;
				}
				contentCopy.prependWriteBuffer(pFirst, pLast);
			}

			// Finish connecting, do request
			state BlobStoreEndpoint::ReusableConnection rconn = wait(timeoutError(frconn, bstore->knobs.connect_timeout));
			connectionEstablished = true;

			// Finish/update the request headers (which includes Date header)
			// This must be done AFTER the connection is ready because if credentials are coming from disk they are refreshed
			// when a new connection is established and setAuthHeaders() would need the updated secret.
			bstore->setAuthHeaders(verb, resource, headers);
			remoteAddress = rconn.conn->getPeerAddress();
			wait(bstore->requestRate->getAllowance(1));
			Reference<HTTP::Response> _r = wait(timeoutError(HTTP::doRequest(rconn.conn, verb, resource, headers, &contentCopy, contentLen, bstore->sendRate, &bstore->s_stats.bytes_sent, bstore->recvRate), requestTimeout));
			r = _r;

			// Since the response was parsed successfully (which is why we are here) reuse the connection unless we received the "Connection: close" header.
			if(r->headers["Connection"] != "close")
				bstore->returnConnection(rconn);
			rconn.conn.clear();

		} catch(Error &e) {
			if(e.code() == error_code_actor_cancelled)
				throw;
			err = e;
		}

		// If err is not present then r is valid.
		// If r->code is in successCodes then record the successful request and return r.
		if(!err.present() && successCodes.count(r->code) != 0) {
			bstore->s_stats.requests_successful++;
			return r;
		}

		// Otherwise, this request is considered failed.  Update failure count.
		bstore->s_stats.requests_failed++;

		// All errors in err are potentially retryable as well as certain HTTP response codes...
		bool retryable = err.present() || r->code == 500 || r->code == 502 || r->code == 503 || r->code == 429;

		// But only if our previous attempt was not the last allowable try.
		retryable = retryable && (thisTry < maxTries);

		TraceEvent event(SevWarn, retryable ? "BlobStoreEndpointRequestFailedRetryable" : "BlobStoreEndpointRequestFailed");

		// Attach err to trace event if present, otherwise extract some stuff from the response
		if(err.present()) {
			event.error(err.get());
		}
		event.suppressFor(60);
		if(!err.present()) {
			event.detail("ResponseCode", r->code);
		}

		event.detail("ConnectionEstablished", connectionEstablished);

		if(remoteAddress.present())
			event.detail("RemoteEndpoint", remoteAddress.get());
		else
			event.detail("RemoteHost", bstore->host);

		event.detail("Verb", verb)
			 .detail("Resource", resource)
			 .detail("ThisTry", thisTry);

		// If r is not valid or not code 429 then increment the try count.  429's will not count against the attempt limit.
		if(!r || r->code != 429)
			++thisTry;

		// We will wait delay seconds before the next retry, start with nextRetryDelay.
		double delay = nextRetryDelay;
		// Double but limit the *next* nextRetryDelay.
		nextRetryDelay = std::min(nextRetryDelay * 2, 60.0);

		if(retryable) {
			// If r is valid then obey the Retry-After response header if present.
			if(r) {
				auto iRetryAfter = r->headers.find("Retry-After");
				if(iRetryAfter != r->headers.end()) {
					event.detail("RetryAfterHeader", iRetryAfter->second);
					char *pEnd;
					double retryAfter = strtod(iRetryAfter->second.c_str(), &pEnd);
					if(*pEnd)  // If there were other characters then don't trust the parsed value, use a probably safe value of 5 minutes.
						retryAfter = 300;
					// Update delay
					delay = std::max(delay, retryAfter);
				}
			}

			// Log the delay then wait.
			event.detail("RetryDelay", delay);
			wait(::delay(delay));
		}
		else {
			// We can't retry, so throw something.

			// This error code means the authentication header was not accepted, likely the account or key is wrong.
			if(r && r->code == 406)
				throw http_not_accepted();

			if(r && r->code == 401)
				throw http_auth_failed();

			// Recognize and throw specific errors
			if(err.present()) {
				int code = err.get().code();

				// If we get a timed_out error during the the connect() phase, we'll call that connection_failed despite the fact that
				// there was technically never a 'connection' to begin with.  It differentiates between an active connection
				// timing out vs a connection timing out, though not between an active connection failing vs connection attempt failing.
				// TODO:  Add more error types?
				if(code == error_code_timed_out && !connectionEstablished)
					throw connection_failed();

				if(code == error_code_timed_out || code == error_code_connection_failed || code == error_code_lookup_failed)
					throw err.get();
			}

			throw http_request_failed();
		}
	}
}

Future<Reference<HTTP::Response>> BlobStoreEndpoint::doRequest(std::string const &verb, std::string const &resource, const HTTP::Headers &headers, UnsentPacketQueue *pContent, int contentLen, std::set<unsigned int> successCodes) {
	return doRequest_impl(Reference<BlobStoreEndpoint>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
}

ACTOR Future<Void> listObjectsStream_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, PromiseStream<BlobStoreEndpoint::ListResult> results, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	// Request 1000 keys at a time, the maximum allowed
	state std::string resource = "/";
	resource.append(bucket);
	resource.append("/?max-keys=1000");
	if(prefix.present())
		resource.append("&prefix=").append(HTTP::urlEncode(prefix.get()));
	if(delimiter.present())
		resource.append("&delimiter=").append(HTTP::urlEncode(std::string(1, delimiter.get())));
	resource.append("&marker=");
	state std::string lastFile;
	state bool more = true;

	state std::vector<Future<Void>> subLists;

	while(more) {
		wait(bstore->concurrentLists.take());
		state FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		state std::string fullResource = resource + HTTP::urlEncode(lastFile);
		lastFile.clear();
		Reference<HTTP::Response> r = wait(bstore->doRequest("GET", fullResource, headers, nullptr, 0, {200}));
		listReleaser.release();

		try {
			BlobStoreEndpoint::ListResult listResult;
			xml_document<> doc;

			// Copy content because rapidxml will modify it during parse
			std::string content = r->content;
			doc.parse<0>((char *)content.c_str());

			// There should be exactly one node
			xml_node<> *result = doc.first_node();
			if(result == nullptr || strcmp(result->name(), "ListBucketResult") != 0) {
				throw http_bad_response();
			}

			xml_node<> *n = result->first_node();
			while(n != nullptr) {
				const char *name = n->name();
				if(strcmp(name, "IsTruncated") == 0) {
					const char *val = n->value();
					if(strcmp(val, "true") == 0) {
						more = true;
					}
					else if(strcmp(val, "false") == 0) {
						more = false;
					}
					else {
						throw http_bad_response();
					}
				}
				else if(strcmp(name, "Contents") == 0) {
					BlobStoreEndpoint::ObjectInfo object;

					xml_node<> *key = n->first_node("Key");
					if(key == nullptr) {
						throw http_bad_response();
					}
					object.name = key->value();

					xml_node<> *size = n->first_node("Size");
					if(size == nullptr) {
						throw http_bad_response();
					}
					object.size = strtoull(size->value(), nullptr, 10);

					listResult.objects.push_back(object);
				}
				else if(strcmp(name, "CommonPrefixes") == 0) {
					xml_node<> *prefixNode = n->first_node("Prefix");
					while(prefixNode != nullptr) {
						const char *prefix = prefixNode->value();
						// If recursing, queue a sub-request, otherwise add the common prefix to the result.
						if(maxDepth > 0) {
							// If there is no recurse filter or the filter returns true then start listing the subfolder
							if(!recurseFilter || recurseFilter(prefix)) {
								subLists.push_back(bstore->listObjectsStream(bucket, results, prefix, delimiter, maxDepth - 1, recurseFilter));
							}
							// Since prefix will not be in the final listResult below we have to set lastFile here in case it's greater than the last object
							lastFile = prefix;
						}
						else {
							listResult.commonPrefixes.push_back(prefix);
						}

						prefixNode = prefixNode->next_sibling("Prefix");
					}
				}

				n = n->next_sibling();
			}

			results.send(listResult);

			if(more) {
				// lastFile will be the last commonprefix for which a sublist was started, if any.
				// If there are any objects and the last one is greater than lastFile then make it the new lastFile.
				if(!listResult.objects.empty() && lastFile < listResult.objects.back().name) {
					lastFile = listResult.objects.back().name;
				}
				// If there are any common prefixes and the last one is greater than lastFile then make it the new lastFile.
				if(!listResult.commonPrefixes.empty() && lastFile < listResult.commonPrefixes.back()) {
					lastFile = listResult.commonPrefixes.back();
				}

				// If lastFile is empty at this point, something has gone wrong.
				if(lastFile.empty()) {
					TraceEvent(SevWarn, "BlobStoreEndpointListNoNextMarker").suppressFor(60).detail("Resource", fullResource);
					throw http_bad_response();
				}
			}
		} catch(Error &e) {
			if(e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "BlobStoreEndpointListResultParseError").error(e).suppressFor(60).detail("Resource", fullResource);
			throw http_bad_response();
		}
	}

	wait(waitForAll(subLists));

	return Void();
}

Future<Void> BlobStoreEndpoint::listObjectsStream(std::string const &bucket, PromiseStream<ListResult> results, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	return listObjectsStream_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, results, prefix, delimiter, maxDepth, recurseFilter);
}

ACTOR Future<BlobStoreEndpoint::ListResult> listObjects_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	state BlobStoreEndpoint::ListResult results;
	state PromiseStream<BlobStoreEndpoint::ListResult> resultStream;
	state Future<Void> done = bstore->listObjectsStream(bucket, resultStream, prefix, delimiter, maxDepth, recurseFilter);
	// Wrap done in an actor which sends end_of_stream because list does not so that many lists can write to the same stream
	done = map(done, [=](Void) {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	try {
		loop {
			choose {
				// Throw if done throws, otherwise don't stop until end_of_stream
				when(wait(done)) {
					done = Never();
				}

				when(BlobStoreEndpoint::ListResult info = waitNext(resultStream.getFuture())) {
					results.commonPrefixes.insert(results.commonPrefixes.end(), info.commonPrefixes.begin(), info.commonPrefixes.end());
					results.objects.insert(results.objects.end(), info.objects.begin(), info.objects.end());
				}
			}
		}
	} catch(Error &e) {
		if(e.code() != error_code_end_of_stream)
			throw;
	}

	return results;
}

Future<BlobStoreEndpoint::ListResult> BlobStoreEndpoint::listObjects(std::string const &bucket, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	return listObjects_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, prefix, delimiter, maxDepth, recurseFilter);
}

ACTOR Future<std::vector<std::string>> listBuckets_impl(Reference<BlobStoreEndpoint> bstore) {
	state std::string resource = "/?marker=";
	state std::string lastName;
	state bool more = true;
	state std::vector<std::string> buckets;

	while(more) {
		wait(bstore->concurrentLists.take());
		state FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		state std::string fullResource = resource + HTTP::urlEncode(lastName);
		Reference<HTTP::Response> r = wait(bstore->doRequest("GET", fullResource, headers, nullptr, 0, {200}));
		listReleaser.release();

		try {
			xml_document<> doc;

			// Copy content because rapidxml will modify it during parse
			std::string content = r->content;
			doc.parse<0>((char *)content.c_str());

			// There should be exactly one node
			xml_node<> *result = doc.first_node();
			if(result == nullptr || strcmp(result->name(), "ListAllMyBucketsResult") != 0) {
				throw http_bad_response();
			}

			more = false;
			xml_node<> *truncated = result->first_node("IsTruncated");
			if(truncated != nullptr && strcmp(truncated->value(), "true") == 0) {
				more = true;
			}

			xml_node<> *bucketsNode = result->first_node("Buckets");
			if(bucketsNode != nullptr) {
				xml_node<> *bucketNode = bucketsNode->first_node("Bucket");
				while(bucketNode != nullptr) {
					xml_node<> *nameNode = bucketNode->first_node("Name");
					if(nameNode == nullptr) {
						throw http_bad_response();
					}
					const char *name = nameNode->value();
					buckets.push_back(name);

					bucketNode = bucketNode->next_sibling("Bucket");
				}
			}

			if(more) {
				lastName = buckets.back();
			}

		} catch(Error &e) {
			if(e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "BlobStoreEndpointListBucketResultParseError").error(e).suppressFor(60).detail("Resource", fullResource);
			throw http_bad_response();
		}
	}

	return buckets;
}

Future<std::vector<std::string>> BlobStoreEndpoint::listBuckets() {
	return listBuckets_impl(Reference<BlobStoreEndpoint>::addRef(this));
}

std::string BlobStoreEndpoint::hmac_sha1(std::string const &msg) {
	std::string key = secret;

	// First pad the key to 64 bytes.
	key.append(64 - key.size(), '\0');

	std::string kipad = key;
	for(int i = 0; i < 64; ++i)
		kipad[i] ^= '\x36';

	std::string kopad = key;
	for(int i = 0; i < 64; ++i)
		kopad[i] ^= '\x5c';

	kipad.append(msg);
	std::string hkipad = SHA1::from_string(kipad);
	kopad.append(hkipad);
	return SHA1::from_string(kopad);
}

void BlobStoreEndpoint::setAuthHeaders(std::string const &verb, std::string const &resource, HTTP::Headers& headers) {
	std::string &date = headers["Date"];

	char dateBuf[20];
	time_t ts;
	time(&ts);
	// ISO 8601 format YYYYMMDD'T'HHMMSS'Z'
	strftime(dateBuf, 20, "%Y%m%dT%H%M%SZ", gmtime(&ts));
	date = dateBuf;

	std::string msg;
	msg.append(verb);
	msg.append("\n");
	auto contentMD5 = headers.find("Content-MD5");
	if(contentMD5 != headers.end())
		msg.append(contentMD5->second);
	msg.append("\n");
	auto contentType = headers.find("Content-Type");
	if(contentType != headers.end())
		msg.append(contentType->second);
	msg.append("\n");
	msg.append(date);
	msg.append("\n");
	for(auto h : headers) {
		StringRef name = h.first;
		if(name.startsWith(LiteralStringRef("x-amz")) ||
		   name.startsWith(LiteralStringRef("x-icloud"))) {
			msg.append(h.first);
			msg.append(":");
			msg.append(h.second);
			msg.append("\n");
		}
	}

	msg.append(resource);
	if(verb == "GET") {
		size_t q = resource.find_last_of('?');
		if(q != resource.npos)
			msg.resize(msg.size() - (resource.size() - q));
	}

	std::string sig = base64::encoder::from_string(hmac_sha1(msg));
	// base64 encoded blocks end in \n so remove it.
	sig.resize(sig.size() - 1);
	std::string auth = "AWS ";
	auth.append(key);
	auth.append(":");
	auth.append(sig);
	headers["Authorization"] = auth;
}

ACTOR Future<std::string> readEntireFile_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	wait(bstore->requestRateRead->getAllowance(1));

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	Reference<HTTP::Response> r = wait(bstore->doRequest("GET", resource, headers, nullptr, 0, {200, 404}));
	if(r->code == 404)
		throw file_not_found();
	return r->content;
}

Future<std::string> BlobStoreEndpoint::readEntireFile(std::string const &bucket, std::string const &object) {
	return readEntireFile_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> writeEntireFileFromBuffer_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object, UnsentPacketQueue *pContent, int contentLen, std::string contentMD5) {
	if(contentLen > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	wait(bstore->requestRateWrite->getAllowance(1));
	wait(bstore->concurrentUploads.take());
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	// Send MD5 sum for content so blobstore can verify it
	headers["Content-MD5"] = contentMD5;
	state Reference<HTTP::Response> r = wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, {200}));

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	if (!r->verifyMD5(false, contentMD5))
		throw checksum_failed();

	return Void();
}

ACTOR Future<Void> writeEntireFile_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object, std::string content) {
	state UnsentPacketQueue packets;
	PacketWriter pw(packets.getWriteBuffer(content.size()), nullptr, Unversioned());
	pw.serializeBytes(content);
	if(content.size() > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	// Yield because we may have just had to copy several MB's into packet buffer chain and next we have to calculate an MD5 sum of it.
	// TODO:  If this actor is used to send large files then combine the summing and packetization into a loop with a yield() every 20k or so.
	wait(yield());

	MD5_CTX sum;
	::MD5_Init(&sum);
	::MD5_Update(&sum, content.data(), content.size());
	std::string sumBytes;
	sumBytes.resize(16);
	::MD5_Final((unsigned char *)sumBytes.data(), &sum);
	std::string contentMD5 = base64::encoder::from_string(sumBytes);
	contentMD5.resize(contentMD5.size() - 1);

	wait(writeEntireFileFromBuffer_impl(bstore, bucket, object, &packets, content.size(), contentMD5));
	return Void();
}

Future<Void> BlobStoreEndpoint::writeEntireFile(std::string const &bucket, std::string const &object, std::string const &content) {
	return writeEntireFile_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object, content);
}

Future<Void> BlobStoreEndpoint::writeEntireFileFromBuffer(std::string const &bucket, std::string const &object, UnsentPacketQueue *pContent, int contentLen, std::string const &contentMD5) {
	return writeEntireFileFromBuffer_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object, pContent, contentLen, contentMD5);
}

ACTOR Future<int> readObject_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object, void *data, int length, int64_t offset) {
	if(length <= 0)
		return 0;
	wait(bstore->requestRateRead->getAllowance(1));

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	headers["Range"] = format("bytes=%lld-%lld", offset, offset + length - 1);
	Reference<HTTP::Response> r = wait(bstore->doRequest("GET", resource, headers, nullptr, 0, {200, 206, 404}));
	if(r->code == 404)
		throw file_not_found();
	if(r->contentLen != r->content.size())  // Double check that this wasn't a header-only response, probably unnecessary
		throw io_error();
	// Copy the output bytes, server could have sent more or less bytes than requested so copy at most length bytes
	memcpy(data, r->content.data(), std::min<int64_t>(r->contentLen, length));
	return r->contentLen;
}

Future<int> BlobStoreEndpoint::readObject(std::string const &bucket, std::string const &object, void *data, int length, int64_t offset) {
	return readObject_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object, data, length, offset);
}

ACTOR static Future<std::string> beginMultiPartUpload_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object) {
	wait(bstore->requestRateWrite->getAllowance(1));

	std::string resource = std::string("/") + bucket + "/" + object + "?uploads";
	HTTP::Headers headers;
	Reference<HTTP::Response> r = wait(bstore->doRequest("POST", resource, headers, nullptr, 0, {200}));

	try {
		xml_document<> doc;
		// Copy content because rapidxml will modify it during parse
		std::string content = r->content;

		doc.parse<0>((char *)content.c_str());

		// There should be exactly one node
		xml_node<> *result = doc.first_node();
		if(result != nullptr && strcmp(result->name(), "InitiateMultipartUploadResult") == 0) {
			xml_node<> *id = result->first_node("UploadId");
			if(id != nullptr) {
				return id->value();
			}
		}
	} catch(...) {
	}
	throw http_bad_response();
}

Future<std::string> BlobStoreEndpoint::beginMultiPartUpload(std::string const &bucket, std::string const &object) {
	return beginMultiPartUpload_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<std::string> uploadPart_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object, std::string uploadID, unsigned int partNumber, UnsentPacketQueue *pContent, int contentLen, std::string contentMD5) {
	wait(bstore->requestRateWrite->getAllowance(1));
	wait(bstore->concurrentUploads.take());
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = format("/%s/%s?partNumber=%d&uploadId=%s", bucket.c_str(), object.c_str(), partNumber, uploadID.c_str());
	HTTP::Headers headers;
	// Send MD5 sum for content so blobstore can verify it
	headers["Content-MD5"] = contentMD5;
	state Reference<HTTP::Response> r = wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, {200}));
	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then the next retry
	// will see error 400.  That could be detected and handled gracefully by retrieving the etag for the successful request.

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	if (!r->verifyMD5(false, contentMD5))
		throw checksum_failed();

	// No etag -> bad response.
	std::string etag = r->headers["ETag"];
	if(etag.empty())
		throw http_bad_response();

	return etag;
}

Future<std::string> BlobStoreEndpoint::uploadPart(std::string const &bucket, std::string const &object, std::string const &uploadID, unsigned int partNumber, UnsentPacketQueue *pContent, int contentLen, std::string const &contentMD5) {
	return uploadPart_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object, uploadID, partNumber, pContent, contentLen, contentMD5);
}

ACTOR Future<Void> finishMultiPartUpload_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object, std::string uploadID, BlobStoreEndpoint::MultiPartSetT parts) {
	state UnsentPacketQueue part_list;  // NonCopyable state var so must be declared at top of actor
	wait(bstore->requestRateWrite->getAllowance(1));

	std::string manifest = "<CompleteMultipartUpload>";
	for(auto &p : parts)
		manifest += format("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n", p.first, p.second.c_str());
	manifest += "</CompleteMultipartUpload>";

	std::string resource = format("/%s/%s?uploadId=%s", bucket.c_str(), object.c_str(), uploadID.c_str());
	HTTP::Headers headers;
	PacketWriter pw(part_list.getWriteBuffer(manifest.size()), nullptr, Unversioned());
	pw.serializeBytes(manifest);
	Reference<HTTP::Response> r = wait(bstore->doRequest("POST", resource, headers, &part_list, manifest.size(), {200}));
	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then the next retry
	// will see error 400.  That could be detected and handled gracefully by HEAD'ing the object before upload to get its (possibly
	// nonexistent) eTag, then if an error 400 is seen then retrieve the eTag again and if it has changed then consider the finish complete.
	return Void();
}

Future<Void> BlobStoreEndpoint::finishMultiPartUpload(std::string const &bucket, std::string const &object, std::string const &uploadID, MultiPartSetT const &parts) {
	return finishMultiPartUpload_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object, uploadID, parts);
}
