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
#include "fdbclient/json_spirit/json_spirit_reader_template.h"
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

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
	request_tries = CLIENT_KNOBS->BLOBSTORE_REQUEST_TRIES;
	request_timeout = CLIENT_KNOBS->BLOBSTORE_REQUEST_TIMEOUT;
	requests_per_second = CLIENT_KNOBS->BLOBSTORE_REQUESTS_PER_SECOND;
	concurrent_requests = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_REQUESTS;
	multipart_max_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MAX_PART_SIZE;
	multipart_min_part_size = CLIENT_KNOBS->BLOBSTORE_MULTIPART_MIN_PART_SIZE;
	concurrent_uploads = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_UPLOADS;
	concurrent_reads_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_READS_PER_FILE;
	concurrent_writes_per_file = CLIENT_KNOBS->BLOBSTORE_CONCURRENT_WRITES_PER_FILE;
	read_block_size = CLIENT_KNOBS->BLOBSTORE_READ_BLOCK_SIZE;
	read_ahead_blocks = CLIENT_KNOBS->BLOBSTORE_READ_AHEAD_BLOCKS;
	read_cache_blocks_per_file = CLIENT_KNOBS->BLOBSTORE_READ_CACHE_BLOCKS_PER_FILE;
	max_send_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_RECV_BYTES_PER_SECOND;
	max_recv_bytes_per_second = CLIENT_KNOBS->BLOBSTORE_MAX_SEND_BYTES_PER_SECOND;
	buckets_to_span = CLIENT_KNOBS->BLOBSTORE_BACKUP_BUCKETS;
}

bool BlobStoreEndpoint::BlobKnobs::set(StringRef name, int value) {
	#define TRY_PARAM(n, sn) if(name == LiteralStringRef(#n) || name == LiteralStringRef(#sn)) { n = value; return true; }
	TRY_PARAM(buckets_to_span, bts);
	TRY_PARAM(connect_tries, ct);
	TRY_PARAM(connect_timeout, cto);
	TRY_PARAM(request_tries, rt);
	TRY_PARAM(request_timeout, rto);
	TRY_PARAM(requests_per_second, rps);
	TRY_PARAM(concurrent_requests, cr);
	TRY_PARAM(multipart_max_part_size, maxps);
	TRY_PARAM(multipart_min_part_size, minps);
	TRY_PARAM(concurrent_uploads, cu);
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
	_CHECK_PARAM(buckets_to_span, bts);
	_CHECK_PARAM(connect_tries, ct);
	_CHECK_PARAM(connect_timeout, cto);
	_CHECK_PARAM(request_tries, rt);
	_CHECK_PARAM(request_timeout, rto);
	_CHECK_PARAM(requests_per_second, rps);
	_CHECK_PARAM(concurrent_requests, cr);
	_CHECK_PARAM(multipart_max_part_size, maxps);
	_CHECK_PARAM(multipart_min_part_size, minps);
	_CHECK_PARAM(concurrent_uploads, cu);
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


struct tokenizer {
	tokenizer(StringRef s) : s(s) {}
	StringRef tok(StringRef sep) {
		for(int i = 0, iend = s.size() - sep.size(); i <= iend; ++i) {
			if(s.substr(i, sep.size()) == sep) {
				StringRef token = s.substr(0, i);
				s = s.substr(i + sep.size());
				return token;
			}
		}
		StringRef token = s;
		s = StringRef();
		return token;
	}
	StringRef tok(const char *sep) { return tok(StringRef((const uint8_t *)sep, strlen(sep))); }
	StringRef s;
};

Reference<BlobStoreEndpoint> BlobStoreEndpoint::fromString(std::string const &url, std::string *resourceFromURL, std::string *error) {
	if(resourceFromURL)
		resourceFromURL->clear();

	try {
		tokenizer t(url);
		StringRef prefix = t.tok("://");
		if(prefix != LiteralStringRef("blobstore"))
			throw std::string("Invalid blobstore URL.");
		StringRef key =      t.tok(":");
		StringRef secret =   t.tok("@");
		StringRef hosts =    t.tok(":");
		StringRef port =     t.tok("/");
		StringRef resource = t.tok("?");

		// The host/ip list can have up to one text hostname, which should be first, and then 0 or more IP addresses.
		// The items are comma separated.
		std::string host;
		std::vector<NetworkAddress> addrs;
		char *end;
		uint16_t portNum = (uint16_t)strtoul(port.toString().c_str(), &end, 10);
		if(*end) {
			throw format("%s is not a valid port", port.toString().c_str());
		}

		tokenizer h(hosts);
		while(1) {
			StringRef item = h.tok(",");
			if(item.size() == 0)
				break;
			// Try to parse item as an IP with the given port, if it fails it will throw so then store it as host
			try {
				// Use an integer port number so the parse doesn't fail due to the port number being garbage
				NetworkAddress na = NetworkAddress::parse(format("%s:%d", item.toString().c_str(), (int)portNum));
				addrs.push_back(na);
			} catch(Error &e) {
				host = item.toString();
			}
		}

		BlobKnobs knobs;
		while(1) {
			StringRef name = t.tok("=");
			if(name.size() == 0)
				break;
			StringRef value = t.tok("&");
			char *valueEnd;
			int ivalue = strtol(value.toString().c_str(), &valueEnd, 10);
			if(*valueEnd || ivalue == 0)
				throw format("%s is not a valid value for %s", value.toString().c_str(), name.toString().c_str());
			if(!knobs.set(name, ivalue))
				throw format("%s is not a valid parameter name", name.toString().c_str());
		}

		if(resourceFromURL != nullptr)
			*resourceFromURL = resource.toString();

		return Reference<BlobStoreEndpoint>(new BlobStoreEndpoint(host, addrs, portNum, key.toString(), secret.toString(), knobs));

	} catch(std::string &err) {
		if(error != nullptr)
			*error = err;
		TraceEvent(SevWarnAlways, "BlobStoreEndpointBadURL").detail("Description", err).detail("Format", getURLFormat()).detail("URL", url);
		throw file_not_found();
	}
}

std::string BlobStoreEndpoint::getResourceURL(std::string resource) {
	std::string hosts = host;
	for(auto &na : addresses) {
		if(hosts.size() != 0)
			hosts.append(",");
		hosts.append(toIPString(na.ip));
	}
	std::string r = format("blobstore://%s:%s@%s:%d/%s", key.c_str(), secret.c_str(), hosts.c_str(), (int)port, resource.c_str());
	std::string p = knobs.getURLParameters();
	if(!p.empty())
		r.append("?").append(p);
	return r;
}

ACTOR Future<Void> resolveHostname_impl(Reference<BlobStoreEndpoint> bstore) {
	state std::vector<uint32_t> ip_addresses;

	// TODO:  Resolve host to get list of IPs into ip_addresses.  Ideally this should be done using
	// boost asio so that backup agents can re-resolve a blobstore endpoint if none of the IP addresses
	// are reachable for some long amount of time that exceeds time-to-live for the hostname.
	// However, if it is done using blocking calls instead then the command line tools that use
	// blobstore URLs will have to resolve them at a safe time during input parsing / checking.
	//

	// Don't modify the existing IP address list if resolution did not work
	if(ip_addresses.empty())
		return Void();

	// Resolve was successful so replace addresses with the new IPs found.
	bstore->addresses.clear();
	for(auto &ip : ip_addresses)
		bstore->addresses.push_back(NetworkAddress(ip, bstore->port));

	return Void();
}

Future<Void> BlobStoreEndpoint::resolveHostname(bool only_if_unresolved) {
	if(only_if_unresolved && !addresses.empty())
		return Void();

	return resolveHostname_impl(Reference<BlobStoreEndpoint>::addRef(this));
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
	Reference<HTTP::Response> r = wait(b->doRequest("DELETE", resource, headers, NULL, 0, {200, 404}));
	// 200 means object deleted, 404 means it doesn't exist already, so either success code passed above is fine.
	return Void();
}

Future<Void> BlobStoreEndpoint::deleteObject(std::string const &bucket, std::string const &object) {
	return deleteObject_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

ACTOR Future<Void> deleteBucket_impl(Reference<BlobStoreEndpoint> b, std::string bucket, int *pNumDeleted) {
	state PromiseStream<BlobStoreEndpoint::ObjectInfo> resultStream;
	state Future<Void> done = b->getBucketContentsStream(bucket, resultStream);
	state std::vector<Future<Void>> deleteFutures;
	loop {
		choose {
			when(Void _ = wait(done)) {
				break;
			}
			when(BlobStoreEndpoint::ObjectInfo info = waitNext(resultStream.getFuture())) {
				if(pNumDeleted == nullptr)
					deleteFutures.push_back(b->deleteObject(bucket, info.name));
				else
					deleteFutures.push_back(map(b->deleteObject(bucket, info.name), [this](Void) -> Void { ++*pNumDeleted; return Void(); }));
			}
		}
	}

	Void _ = wait(waitForAll(deleteFutures));
	return Void();
}

Future<Void> BlobStoreEndpoint::deleteBucket(std::string const &bucket, int *pNumDeleted) {
	return deleteBucket_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, pNumDeleted);
}

ACTOR Future<int64_t> objectSize_impl(Reference<BlobStoreEndpoint> b, std::string bucket, std::string object) {
	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;

	Reference<HTTP::Response> r = wait(b->doRequest("HEAD", resource, headers, NULL, 0, {200}));
	return r->contentLen;
}

Future<int64_t> BlobStoreEndpoint::objectSize(std::string const &bucket, std::string const &object) {
	return objectSize_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, object);
}

Future<Reference<IConnection>> BlobStoreEndpoint::connect( NetworkAddress address ) {
	auto &pool = connectionPool[address];

	while(!pool.empty()) {
		BlobStoreEndpoint::ConnPoolEntry c = pool.front();
		pool.pop_front();

		// If the connection was placed in the pool less than 10 seconds ago, reuse it.
		if(c.second > now() - 10) {
			//printf("Reusing blob store connection\n");
			return c.first;
		}
	}

	return INetworkConnections::net()->connect(address);
}

// Do a request, get a Response.
// Request content is provided as UnsentPacketQueue *pContent which will be depleted as bytes are sent but the queue itself must live for the life of this actor
// and be destroyed by the caller
ACTOR Future<Reference<HTTP::Response>> doRequest_impl(Reference<BlobStoreEndpoint> bstore, std::string verb, std::string resource, HTTP::Headers headers, UnsentPacketQueue *pContent, int contentLen, std::set<unsigned int> successCodes) {
	state UnsentPacketQueue contentCopy;

	// Set content length header if there is content
	if(contentLen > 0)
		headers["Content-Length"] = format("%d", contentLen);

	Void _ = wait(bstore->concurrentRequests.take(1));
	state FlowLock::Releaser globalReleaser(bstore->concurrentRequests, 1);

	state int maxTries = std::min(bstore->knobs.request_tries, bstore->knobs.connect_tries);
	state int thisTry = 1;
	state double nextRetryDelay = 2.0;
	state NetworkAddress address;
	loop {
		state Optional<Error> err;
		try {
			// Pick an adress
			address = bstore->addresses[g_random->randomInt(0, bstore->addresses.size())];
			state FlowLock::Releaser perAddressReleaser;
			Void _ = wait(bstore->concurrentRequestsPerAddress[address]->take(1));
			perAddressReleaser = FlowLock::Releaser(*bstore->concurrentRequestsPerAddress[address], 1);

			// Start connecting
			Future<Reference<IConnection>> fconn = bstore->connect(address);

			// Finish/update the request headers (which includes Date header)
			bstore->setAuthHeaders(verb, resource, headers);

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
			state Reference<IConnection> conn = wait(timeoutError(fconn, bstore->knobs.connect_timeout));
			Void _ = wait(bstore->requestRate->getAllowance(1));
			state Reference<HTTP::Response> r = wait(timeoutError(HTTP::doRequest(conn, verb, resource, headers, &contentCopy, contentLen, bstore->sendRate, &bstore->s_stats.bytes_sent, bstore->recvRate), bstore->knobs.request_timeout));

			// Since the response was parsed successfully (which is why we are here) reuse the connection unless we received the "Connection: close" header.
			if(r->headers["Connection"] != "close")
				bstore->connectionPool[address].push_back(BlobStoreEndpoint::ConnPoolEntry(conn, now()));
			conn.clear();
		} catch(Error &e) {
			// For timeouts, conn failure, or bad reponse reported by HTTP:doRequest, save the error and handle it / possibly retry below.
			// Any other error is rethrown.
			if(e.code() == error_code_connection_failed || e.code() == error_code_timed_out || e.code() == error_code_http_bad_response)
				err = e;
			else
				throw;
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

		event.detail("RemoteEndpoint", address)
			 .detail("Verb", verb)
			 .detail("Resource", resource)
			 .detail("ThisTry", thisTry)
			 .suppressFor(5, true);

		// We will wait delay seconds before the next retry, start with nextRetryDelay.
		double delay = nextRetryDelay;
		// Double but limit the *next* nextRetryDelay.
		nextRetryDelay = std::min(nextRetryDelay * 2, 60.0);

		// Attach err to trace event if present, otherwise extract some stuff from the response
		if(err.present())
			event.error(err.get());
		else {
			event.detail("ResponseCode", r->code);

			// Check for the Retry-After header which is present with certain types of errors
			auto iRetryAfter = r->headers.find("Retry-After");
			if(iRetryAfter != r->headers.end()) {
				event.detail("RetryAfterHeader", iRetryAfter->second);
				char *pEnd;
				double retryAfter = strtod(iRetryAfter->second.c_str(), &pEnd);
				if(*pEnd)  // If there were other characters then don't trust the parsed value, use a probably safe value of 5 minutes.
					retryAfter = 300;
				delay = std::max(delay, retryAfter);
			}
		}

		// For retryable errors, log the delay then wait.
		if(retryable) {
			event.detail("RetryDelay", delay);
			Void _ = wait(::delay(delay));
		}
		else {
			// We can't retry, so throw something.

			// This error code means the authentication header was not accepted, likely the account or key is wrong.
			if(r->code == 406)
				throw http_not_accepted();

			throw http_request_failed();
		}
	}
}

Future<Reference<HTTP::Response>> BlobStoreEndpoint::doRequest(std::string const &verb, std::string const &resource, const HTTP::Headers &headers, UnsentPacketQueue *pContent, int contentLen, std::set<unsigned int> successCodes) {
	return doRequest_impl(Reference<BlobStoreEndpoint>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
}

ACTOR Future<Void> getBucketContentsStream_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket, PromiseStream<BlobStoreEndpoint::ObjectInfo> results) {
	// Request 1000 keys at a time, the maximum allowed
	state std::string resource = std::string("/") + bucket + "/?max-keys=1000&marker=";
	state std::string lastFile;
	state bool more = true;

	while(more) {
		HTTP::Headers headers;
		Reference<HTTP::Response> r = wait(bstore->doRequest("GET", resource + HTTP::urlEncode(lastFile), headers, NULL, 0, {200}));

		try {
			// Parse the json assuming it is valid and contains the right stuff.  If any exceptions are thrown, throw http_bad_response
			json_spirit::Value json;
			json_spirit::read_string(r->content, json);
			for(auto &i : json.get_obj()) {
				if(i.name_ == "truncated") {
					more = i.value_.get_bool();
				}
				else if(i.name_ == "results") {
					BlobStoreEndpoint::ObjectInfo info;
					info.bucket = bucket;
					for(auto &o : i.value_.get_array()) {
						info.size = -1;
						info.name.clear();
						for(auto &f : o.get_obj()) {
							if(f.name_ == "size")
								info.size = f.value_.get_int();
							else if(f.name_ == "key")
								info.name = f.value_.get_str();
						}
						if(info.size >= 0 && !info.name.empty()) {
							lastFile = info.name;
							results.send(std::move(info));
						}
					}
				}
			}
		} catch(Error &e) {
			throw http_bad_response();
		}
	}

	return Void();
}

Future<Void> BlobStoreEndpoint::getBucketContentsStream(std::string const &bucket, PromiseStream<BlobStoreEndpoint::ObjectInfo> results) {
	return getBucketContentsStream_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket, results);
}

ACTOR Future<BlobStoreEndpoint::BucketContentsT> getBucketContents_impl(Reference<BlobStoreEndpoint> bstore, std::string bucket) {
	state BlobStoreEndpoint::BucketContentsT results;
	state PromiseStream<BlobStoreEndpoint::ObjectInfo> resultStream;
	state Future<Void> done = bstore->getBucketContentsStream(bucket, resultStream);
	loop {
		choose {
			when(Void _ = wait(done)) {
				break;
			}
			when(BlobStoreEndpoint::ObjectInfo info = waitNext(resultStream.getFuture())) {
				results.push_back(info);
			}
		}
	}
	return results;
}

Future<BlobStoreEndpoint::BucketContentsT> BlobStoreEndpoint::getBucketContents(std::string const &bucket) {
	return getBucketContents_impl(Reference<BlobStoreEndpoint>::addRef(this), bucket);
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
	std::string auth = key;
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
	if(contentLen == 0)
		throw file_not_writable();

	if(contentLen > bstore->knobs.multipart_max_part_size)
		throw file_too_large();

	Void _ = wait(bstore->concurrentUploads.take(1));
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = std::string("/") + bucket + "/" + object;
	HTTP::Headers headers;
	// Send MD5 sum for content so blobstore can verify it
	headers["Content-MD5"] = contentMD5;
	state Reference<HTTP::Response> r = wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, {200}));

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	if(r->headers["Content-MD5"] != contentMD5)
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
	Reference<HTTP::Response> r = wait(bstore->doRequest("GET", resource, headers, NULL, 0, {200, 206}));
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
	Void _ = wait(bstore->concurrentUploads.take(1));
	state FlowLock::Releaser uploadReleaser(bstore->concurrentUploads, 1);

	std::string resource = format("/%s/%s?partNumber=%d&uploadId=%s", bucket.c_str(), object.c_str(), partNumber, uploadID.c_str());
	HTTP::Headers headers;
	// Send MD5 sum for content so blobstore can verify it
	headers["Content-MD5"] = contentMD5;
	state Reference<HTTP::Response> r = wait(bstore->doRequest("PUT", resource, headers, pContent, contentLen, {200}));
	// TODO:  In the event that the client times out just before the request completes (so the client is unaware) then the next retry
	// will see error 400.  That could be detected and handled gracefully by retrieving the etag for the successful request.

	// For uploads, Blobstore returns an MD5 sum of uploaded content so check it.
	if(r->headers["Content-MD5"] != contentMD5)
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
