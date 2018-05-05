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

#include "BlobStore.h"

#include "md5/md5.h"
#include "libb64/encode.h"
#include "sha1/SHA1.h"
#include "time.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include "IAsyncFile.h"

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
	connect_tries = CLIENT_KNOBS->BLOBSTORE_CONNECT_TRIES;
	connect_timeout = CLIENT_KNOBS->BLOBSTORE_CONNECT_TIMEOUT;
	max_connection_life = CLIENT_KNOBS->BLOBSTORE_MAX_CONNECTION_LIFE;
	request_tries = CLIENT_KNOBS->BLOBSTORE_REQUEST_TRIES;
	request_timeout = CLIENT_KNOBS->BLOBSTORE_REQUEST_TIMEOUT;
	requests_per_second = CLIENT_KNOBS->BLOBSTORE_REQUESTS_PER_SECOND;
	concurrent_requests = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS;
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
	TRY_PARAM(connect_tries, ct);
	TRY_PARAM(connect_timeout, cto);
	TRY_PARAM(max_connection_life, mcl);
	TRY_PARAM(request_tries, rt);
	TRY_PARAM(request_timeout, rto);
	TRY_PARAM(requests_per_second, rps);
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
	_CHECK_PARAM(connect_tries, ct);
	_CHECK_PARAM(connect_timeout, cto);
	_CHECK_PARAM(max_connection_life, mcl);
	_CHECK_PARAM(request_tries, rt);
	_CHECK_PARAM(request_timeout, rto);
	_CHECK_PARAM(requests_per_second, rps);
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

Reference<BlobStoreEndpoint> BlobStoreEndpoint::fromString(std::string const &url, std::string *resourceFromURL, std::string *error) {
	if(resourceFromURL)
		resourceFromURL->clear();

	try {
		StringRef t(url);
		StringRef prefix = t.eat("://");
		if(prefix != LiteralStringRef("blobstore"))
			throw std::string("Invalid blobstore URL.");
		StringRef cred   =   t.eat("@");
		StringRef hostPort = t.eat("/");
		StringRef resource = t.eat("?");

		// hostPort is at least a host or IP address, optionally followed by :portNumber or :serviceName
		StringRef h(hostPort);
		StringRef host = h.eat(":");
		if(host.size() == 0)
			throw std::string("host cannot be empty");

		StringRef service = h.eat();

		BlobKnobs knobs;
		while(1) {
			StringRef name = t.eat("=");
			if(name.size() == 0)
				break;
			StringRef value = t.eat("&");
			char *valueEnd;
			int ivalue = strtol(value.toString().c_str(), &valueEnd, 10);
			if(*valueEnd || ivalue == 0)
				throw format("%s is not a valid value for %s", value.toString().c_str(), name.toString().c_str());
			if(!knobs.set(name, ivalue))
				throw format("%s is not a valid parameter name", name.toString().c_str());
		}

		if(resourceFromURL != nullptr)
			*resourceFromURL = resource.toString();

		StringRef c(cred);
		StringRef key = c.eat(":");
		StringRef secret = c.eat();

		return Reference<BlobStoreEndpoint>(new BlobStoreEndpoint(host.toString(), service.toString(), key.toString(), secret.toString(), knobs));

	} catch(std::string &err) {
		if(error != nullptr)
			*error = err;
		TraceEvent(SevWarnAlways, "BlobStoreEndpointBadURL").detail("Description", err).detail("Format", getURLFormat()).detail("URL", url).suppressFor(60, true);
		throw backup_invalid_url();
	}
}

std::string BlobStoreEndpoint::getResourceURL(std::string resource) {
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
	std::string p = knobs.getURLParameters();
	if(!p.empty())
		r.append("?").append(p);
	return r;
}

ACTOR Future<bool> objectExists_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("HEAD", resource, headers, NULL, 0, {200, 404}));
	return r->code == 200;
}

Future<bool> BlobStoreEndpoint::objectExists(std::string const &bucket, std::string const &object) {
	return objectExists_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteObject_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	Reference<HTTP::Response> r = wait(b->doRequest("DELETE", resource, headers, NULL, 0, {200, 204, 404}));
	// 200 means object deleted, 404 means it doesn't exist already, so either success code passed above is fine.
	return Void();
}

Future<Void> BlobStoreEndpoint::deleteObject(std::string const &bucket, std::string const &object) {
	return deleteObject_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteRecursively_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string prefix, int *pNumDeleted) {
	state PromiseStream<BlobStoreEndpoint::ListResult> resultStream;
	// Start a recursive parallel listing which will send results to resultStream as they are received
	state Future<Void> done = b->listBucketStream(bucket, resultStream, prefix, '/', std::numeric_limits<int>::max());
	// Wrap done in an actor which will send end_of_stream since listBucketStream() does not (so that many calls can write to the same stream)
	done = map(done, [=](Void) {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	state std::list<Future<Void>> deleteFutures;
	try {
		loop {
			choose {
				// Throw if done throws, otherwise don't stop until end_of_stream
				when(Void _ = wait(done)) {
					done = Never();
				}

				when(BlobStoreEndpoint::ListResult list = waitNext(resultStream.getFuture())) {
					for(auto &object : list.objects) {
						int *pNumDeletedCopy = pNumDeleted;   // avoid capture of this
						deleteFutures.push_back(map(b->deleteObject(bucket, object.name), [pNumDeletedCopy](Void) -> Void {
							if(pNumDeletedCopy != nullptr)
								++*pNumDeletedCopy;
							return Void();
						}));
					}
				}
			}

			// This is just a precaution to avoid having too many outstanding delete actors waiting to run
			while(deleteFutures.size() > CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS) {
				Void _ = wait(deleteFutures.front());
				deleteFutures.pop_front();
			}
		}
	} catch(Error &e) {
		if(e.code() != error_code_end_of_stream)
			throw;
	}

	while(deleteFutures.size() > 0) {
		Void _ = wait(deleteFutures.front());
		deleteFutures.pop_front();
	}

	return Void();
}

Future<Void> BlobStoreEndpoint::deleteRecursively(std::string const &bucket, std::string prefix, int *pNumDeleted) {
	return deleteRecursively_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, prefix, pNumDeleted);
}

ACTOR Future<Void> createBucket_impl(Reference<BlobStoreEndpoint> b, std::string bucket) {
	std::string resource = std::string("/") + bucket;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("PUT", resource, headers, NULL, 0, {200, 409}));
	return Void();
}

Future<Void> BlobStoreEndpoint::createBucket(std::string const &bucket) {
	return createBucket_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket);
}

ACTOR Future<int64_t> objectSize_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("HEAD", resource, headers, NULL, 0, {200, 404}));
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
			TraceEvent(SevWarn, "BlobCredentialFileNotJSONObject").detail("File", path).suppressFor(60, true);

	} catch(Error &e) {
		if(e.code() != error_code_actor_cancelled)
			TraceEvent(SevWarn, errorEventType).detail("File", path).error(e).suppressFor(60, true);
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

	Void _ = wait(waitForAll(reads));

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
		TraceEvent("BlobStoreEndpointReusingConnected")
			.detail("RemoteEndpoint", rconn.conn->getPeerAddress())
			.detail("ExpiresIn", rconn.expirationTime - now())
			.suppressFor(60, true);
			return rconn;
		}
	}

	state Reference<IConnection> conn = wait(INetworkConnections::net()->connect(b->host, b->service.empty() ? "http" : b->service));

	TraceEvent("BlobStoreEndpointNewConnection")
		.detail("RemoteEndpoint", conn->getPeerAddress())
		.detail("ExpiresIn", b->knobs.max_connection_life)
		.suppressFor(60, true);

	if(b->lookupSecret)
		Void _ = wait(b->updateSecret());

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
	Void _ = wait(bstore->concurrentRequests.take());
	state FlowLock::Releaser globalReleaser(bstore->concurrentRequests, 1);

	state int maxTries = std::min(bstore->knobs.request_tries, bstore->knobs.connect_tries);
	state int thisTry = 1;
	state double nextRetryDelay = 2.0;

	loop {
		state Optional<Error> err;
		state Optional<NetworkAddress> remoteAddress;

		try {
			// Start connecting
			Future<BlobStoreEndpoint::ReusableConnection> frconn = bstore->connect();

			// Make a shallow copy of the queue by calling addref() on each buffer in the chain and then prepending that chain to contentCopy
			if(pContent != nullptr) {
				contentCopy.discardAll();
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

			// Finish/update the request headers (which includes Date header)
			// This must be done AFTER the connection is ready because if credentials are coming from disk they are refreshed
			// when a new connection is established and setAuthHeaders() would need the updated secret.
			bstore->setAuthHeaders(verb, resource, headers);
			remoteAddress = rconn.conn->getPeerAddress();
			Void _ = wait(bstore->requestRate->getAllowance(1));
			state Reference<HTTP::Response> r = wait(timeoutError(HTTP::doRequest(rconn.conn, verb, resource, headers, &contentCopy, contentLen, bstore->sendRate, &bstore->s_stats.bytes_sent, bstore->recvRate), bstore->knobs.request_timeout));
			r->convertToJSONifXML();

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

		if(remoteAddress.present())
			event.detail("RemoteEndpoint", remoteAddress.get());
		else
			event.detail("RemoteHost", bstore->host);

		event.detail("Verb", verb)
			 .detail("Resource", resource)
			 .detail("ThisTry", thisTry)
			 .suppressFor(60, true);

		// If r is not valid or not code 429 then increment the try count.  429's will not count against the attempt limit.
		if(!r || r->code != 429)
			++thisTry;

		// We will wait delay seconds before the next retry, start with nextRetryDelay.
		double delay = nextRetryDelay;
		// Double but limit the *next* nextRetryDelay.
		nextRetryDelay = std::min(nextRetryDelay * 2, 60.0);

		// Attach err to trace event if present, otherwise extract some stuff from the response
		if(err.present())
			event.error(err.get());
		else {
			event.detail("ResponseCode", r->code);
		}

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
			Void _ = wait(::delay(delay));
		}
		else {
			// We can't retry, so throw something.

			// This error code means the authentication header was not accepted, likely the account or key is wrong.
			if(r && r->code == 406)
				throw http_not_accepted();

			if(r && r->code == 401)
				throw http_auth_failed();

			throw http_request_failed();
		}
	}
}

Future<Reference<HTTP::Response>> BlobStoreEndpoint::doRequest(std::string const &verb, std::string const &resource, const HTTP::Headers &headers, UnsentPacketQueue *pContent, int contentLen, std::set<unsigned int> successCodes) {
	return doRequest_impl(Reference<BlobStoreEndpoint>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
}

ACTOR Future<Void> listBucketStream_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, PromiseStream<BlobStoreEndpoint::ListResult> results, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
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
		Void _ = wait(bstore->concurrentLists.take());
		state FlowLock::Releaser listReleaser(bstore->concurrentLists, 1);

		HTTP::Headers headers;
		state std::string fullResource = resource + HTTP::urlEncode(lastFile);
		lastFile.clear();
		Reference<HTTP::Response> r = wait(bstore->doRequest("GET", fullResource, headers, NULL, 0, {200}));
		listReleaser.release();

		try {
			BlobStoreEndpoint::ListResult result;
			// Parse the json assuming it is valid and contains the right stuff.  If any exceptions are thrown, throw http_bad_response
			json_spirit::mValue json;
			json_spirit::read_string(r->content, json);
			JSONDoc doc(json);

			std::string isTruncated;
			if (!doc.tryGet("truncated", more)) {
				doc.get("ListBucketResult.IsTruncated", isTruncated);
				more = isTruncated == "false" ? false : true;
			}

			if (doc.has("results")) {
				for (auto &jsonObject : doc.at("results").get_array()) {
					JSONDoc objectDoc(jsonObject);
					BlobStoreEndpoint::ObjectInfo object;
					objectDoc.get("size", object.size);
					objectDoc.get("key", object.name);
					result.objects.push_back(std::move(object));
				}
			}

			if(doc.has("ListBucketResult.Contents")) {
				if (doc.at("ListBucketResult.Contents").type() == json_spirit::array_type) {
					for (auto &jsonObject : doc.at("ListBucketResult.Contents").get_array()) {
						JSONDoc objectDoc(jsonObject);
						BlobStoreEndpoint::ObjectInfo object;
						std::string sizeVal;
						objectDoc.get("Size", sizeVal);
						object.size = strtoll(sizeVal.c_str(), NULL, 10);
						objectDoc.get("Key", object.name);
						result.objects.push_back(std::move(object));
					}
				}
				else {
					auto jsonObject = doc.at("ListBucketResult.Contents");
					JSONDoc objectDoc(jsonObject);
					BlobStoreEndpoint::ObjectInfo object;
					std::string sizeVal;
					objectDoc.get("Size", sizeVal);
					object.size = strtoll(sizeVal.c_str(), NULL, 10);
					objectDoc.get("Key", object.name);
					result.objects.push_back(std::move(object));
				}
			}

			if(doc.has("CommonPrefixes")) {
				for(auto &jsonObject : doc.at("CommonPrefixes").get_array()) {
					JSONDoc objectDoc(jsonObject);
					std::string p;
					objectDoc.get("Prefix", p);
					// If recursing, queue a sub-request, otherwise add the common prefix to the result.
					if(maxDepth > 0) {
						// If there is no recurse filter or the filter returns true then start listing the subfolder
						if(!recurseFilter || recurseFilter(p))
							subLists.push_back(bstore->listBucketStream(bucket, results, p, delimiter, maxDepth - 1, recurseFilter));
						if(more)
							lastFile = std::move(p);
					}
					else
						result.commonPrefixes.push_back(std::move(p));
				}
			}

			results.send(result);

			if(more) {
				// lastFile will be the last commonprefix for which a sublist was started, if any
				if(!result.objects.empty() && lastFile < result.objects.back().name)
					lastFile = result.objects.back().name;
				if(!result.commonPrefixes.empty() && lastFile < result.commonPrefixes.back())
					lastFile = result.commonPrefixes.back();

				if(lastFile.empty()) {
					TraceEvent(SevWarn, "BlobStoreEndpointListNoNextMarker").detail("Resource", fullResource).suppressFor(60, true);
					throw backup_error();
				}
			}
		} catch(Error &e) {
			if(e.code() != error_code_actor_cancelled)
				TraceEvent(SevWarn, "BlobStoreEndpointListResultParseError").detail("Resource", fullResource).error(e).suppressFor(60, true);
			throw http_bad_response();
		}
	}

	Void _ = wait(waitForAll(subLists));

	return Void();
}

Future<Void> BlobStoreEndpoint::listBucketStream(std::string const &bucket, PromiseStream<ListResult> results, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	return listBucketStream_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, results, prefix, delimiter, maxDepth, recurseFilter);
}

ACTOR Future<BlobStoreEndpoint::ListResult> listBucket_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	state BlobStoreEndpoint::ListResult results;
	state PromiseStream<BlobStoreEndpoint::ListResult> resultStream;
	state Future<Void> done = bstore->listBucketStream(bucket, resultStream, prefix, delimiter, maxDepth, recurseFilter);
	// Wrap done in an actor which sends end_of_stream because list does not so that many lists can write to the same stream
	done = map(done, [=](Void) {
		resultStream.sendError(end_of_stream());
		return Void();
	});

	try {
		loop {
			choose {
				// Throw if done throws, otherwise don't stop until end_of_stream
				when(Void _ = wait(done)) {
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

Future<BlobStoreEndpoint::ListResult> BlobStoreEndpoint::listBucket(std::string const &bucket, Optional<std::string> prefix, Optional<char> delimiter, int maxDepth, std::function<bool(std::string const &)> recurseFilter) {
	return listBucket_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, prefix, delimiter, maxDepth, recurseFilter);
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
	time_t ts;
	time(&ts);
	std::string &date = headers["Date"];
	date = std::string(asctime(gmtime(&ts)), 24) + " GMT";  // asctime() returns a 24 character string plus a \n and null terminator.
	std::string msg;
	StringRef x;
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
	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	Reference<HTTP::Response> r = wait(bstore->doRequest("GET", resource, headers, NULL, 0, {200, 404}));
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

	Void _ = wait(bstore->concurrentUploads.take());
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
	PacketWriter pw(packets.getWriteBuffer(), NULL, Unversioned());
	pw.serializeBytes(content);
	if(content.size() > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	// Yield because we may have just had to copy several MB's into packet buffer chain and next we have to calculate an MD5 sum of it.
	// TODO:  If this actor is used to send large files then combine the summing and packetization into a loop with a yield() every 20k or so.
	Void _ = wait(yield());

	MD5_CTX sum;
	::MD5_Init(&sum);
	::MD5_Update(&sum, content.data(), content.size());
	std::string sumBytes;
	sumBytes.resize(16);
	::MD5_Final((unsigned char *)sumBytes.data(), &sum);
	std::string contentMD5 = base64::encoder::from_string(sumBytes);
	contentMD5.resize(contentMD5.size() - 1);

	Void _ = wait(writeEntireFileFromBuffer_impl(bstore, bucket, object, &packets, content.size(), contentMD5));
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
	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	headers["Range"] = format("bytes=%lld-%lld", offset, offset + length - 1);
	Reference<HTTP::Response> r = wait(bstore->doRequest("GET", resource, headers, NULL, 0, {200, 206, 404}));
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
	std::string resource = std::string("/") + bucket + "/" + object + "?uploads";
	HTTP::Headers headers;
	Reference<HTTP::Response> r = wait(bstore->doRequest("POST", resource, headers, NULL, 0, {200}));
	int start = r->content.find("<UploadId>");
	if(start == std::string::npos)
		 throw http_bad_response();
	start += 10;
	int end = r->content.find("</UploadId>", start);
	if(end == std::string::npos)
		throw http_bad_response();
	return r->content.substr(start, end - start);
}

Future<std::string> BlobStoreEndpoint::beginMultiPartUpload(std::string const &bucket, std::string const &object) {
	return beginMultiPartUpload_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<std::string> uploadPart_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, std::string object, std::string uploadID, unsigned int partNumber, UnsentPacketQueue *pContent, int contentLen, std::string contentMD5) {
	Void _ = wait(bstore->concurrentUploads.take());
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
	state UnsentPacketQueue part_list();  // NonCopyable state var so must be declared at top of actor

	std::string manifest = "<CompleteMultipartUpload>";
	for(auto &p : parts)
		manifest += format("<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>\n", p.first, p.second.c_str());
	manifest += "</CompleteMultipartUpload>";

	std::string resource = format("/%s/%s?uploadId=%s", bucket.c_str(), object.c_str(), uploadID.c_str());
	HTTP::Headers headers;
	PacketWriter pw(part_list.getWriteBuffer(), NULL, Unversioned());
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
