/*
 * RESTClient.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/RESTClient.h"

#include "fdbclient/Knobs.h"
#include "fdbrpc/IRateControl.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Net2Packet.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

#include <boost/algorithm/string.hpp>
#include <memory>
#include <unordered_map>

#include "flow/actorcompiler.h" // always the last include

namespace {
std::unordered_set<std::string> protocols = { "http", "https" };

bool isProtocolSupported(const std::string& protocol) {
	return protocols.find(protocol) != protocols.end();
}

bool isSecurePrototol(const std::string& protocol) {
	return protocol.compare("https") == 0;
}
} // namespace

json_spirit::mObject RESTClient::Stats::getJSON() {
	json_spirit::mObject o;

	o["host:service"] = hostService;
	o["requests_failed"] = requests_failed;
	o["requests_successful"] = requests_successful;
	o["bytes_sent"] = bytes_sent;

	return o;
}

RESTClient::Stats RESTClient::Stats::operator-(const Stats& rhs) {
	Stats r(hostService);

	r.requests_failed = requests_failed - rhs.requests_failed;
	r.requests_successful = requests_successful - rhs.requests_successful;
	r.bytes_sent = bytes_sent - rhs.bytes_sent;

	return r;
}

RESTClient::Knobs::Knobs() {
	secure_connection = RESTClient::Knobs::SECURE_CONNECTION;
	connection_pool_size = CLIENT_KNOBS->REST_CLIENT_MAX_CONNECTIONPOOL_SIZE;
	connect_tries = CLIENT_KNOBS->REST_CLIENT_CONNECT_TRIES;
	connect_timeout = CLIENT_KNOBS->REST_CLIENT_CONNECT_TIMEOUT;
	max_connection_life = CLIENT_KNOBS->REST_CLIENT_MAX_CONNECTION_LIFE;
	request_tries = CLIENT_KNOBS->REST_CLIENT_REQUEST_TRIES;
	request_timeout_secs = CLIENT_KNOBS->REST_CLIENT_REQUEST_TIMEOUT_SEC;

	knobMap["connection_pool_size"] = std::addressof(connection_pool_size);
	knobMap["pz"] = std::addressof(connection_pool_size);
	knobMap["secure_connection"] = std::addressof(secure_connection);
	knobMap["sc"] = std::addressof(secure_connection);
	knobMap["connect_tries"] = std::addressof(connect_tries);
	knobMap["ct"] = std::addressof(connect_tries);
	knobMap["connect_timeout"] = std::addressof(connect_timeout);
	knobMap["cto"] = std::addressof(connect_timeout);
	knobMap["max_connection_life"] = std::addressof(max_connection_life);
	knobMap["mcl"] = std::addressof(max_connection_life);
	knobMap["request_tries"] = std::addressof(request_tries);
	knobMap["rt"] = std::addressof(request_tries);
	knobMap["request_timeout_secs"] = std::addressof(request_timeout_secs);
	knobMap["rtom"] = std::addressof(request_timeout_secs);
}

void RESTClient::Knobs::set(const std::unordered_map<std::string, int>& knobSettings) {
	TraceEvent trace = TraceEvent("RESTClient_SetKnobs");

	for (const auto& itr : knobSettings) {
		const auto& kItr = RESTClient::Knobs::knobMap.find(itr.first);
		if (kItr == RESTClient::Knobs::knobMap.end()) {
			trace.detail("RESTClient_InvalidKnobName", itr.first);
			throw rest_invalid_rest_client_knob();
		}
		*(kItr->second) = itr.second;
		trace.detail(itr.first.c_str(), itr.second);
	}
}

std::unordered_map<std::string, int> RESTClient::Knobs::get() const {
	std::unordered_map<std::string, int> details = {
		{ "connection_pool_size", connection_pool_size },
		{ "secure_connection", secure_connection },
		{ "connect_tries", connect_tries },
		{ "connect_timeout", connect_timeout },
		{ "max_connection_life", max_connection_life },
		{ "request_tries", request_tries },
		{ "request_timeout_secs", request_timeout_secs },
	};

	return details;
}

RESTClient::RESTClient(const std::string& uri) {
	parseUri(uri);
	stats.hostService = host + ":" + service;
}

RESTClient::RESTClient(const std::string& uri, std::unordered_map<std::string, int>& knobSettings) {
	knobs.set(knobSettings);
	parseUri(uri);
	stats.hostService = host + ":" + service;
}

void RESTClient::parseUri(const std::string& uri) {
	// Sample valid URIs
	// 1. With 'host' & 'resource' := '<protocol>://<host>/<resource>'
	// 2. With 'host', 'service' & 'resource' := '<protocol>://<host>:port/<resource>'
	// 3. With 'host', 'service', 'resource' & 'reqParameters' := '<protocol>://<host>:port/<resource>?<parameter-list>'

	try {
		StringRef t(uri);
		StringRef p = t.eat("://");
		std::string protocol = p.toString();
		boost::algorithm::to_lower(protocol);
		if (!isProtocolSupported(protocol)) {
			throw format("Invalid REST URI protocol '%s'", protocol.c_str());
		}

		// Ensure connection secure knob setting matches with the input URI
		if ((isSecurePrototol(protocol) && knobs.secure_connection != RESTClient::Knobs::SECURE_CONNECTION) ||
		    (!isSecurePrototol(protocol) && knobs.secure_connection != RESTClient::Knobs::NOT_SECURE_CONNECTION)) {
			throw format("Invalid REST URI protocol secure knob '%s'", uri.c_str());
		}

		// extract 'resource' and optional 'parameter list' if supplied in the URL
		uint8_t foundSeparator = 0;
		StringRef hostPort = t.eatAny("/?", &foundSeparator);
		if (foundSeparator == '/') {
			resource = t.eat("?").toString();
			reqParameters = t.eat().toString();
		}

		// hostPort is at least a host or IP address, optionally followed by :portNumber or :serviceName
		StringRef hRef(hostPort);
		StringRef h = hRef.eat(":");
		if (h.size() == 0) {
			throw std::string("host cannot be empty");
		}
		host = h.toString();
		service = hRef.eat().toString();

		TraceEvent("RESTClient_ParseURI")
		    .detail("URI", uri)
		    .detail("Host", host)
		    .detail("Service", service)
		    .detail("Resource", resource)
		    .detail("ReqParameters", reqParameters);
	} catch (std::string& err) {
		TraceEvent("RESTClient_ParseError").detail("URI", uri).detail("Error", err);
		throw rest_invalid_uri();
	}
}

ACTOR Future<RESTClient::ReusableConnection> connect_impl(Reference<RESTClient> client) {
	while (!client->connectionPool.empty()) {
		RESTClient::ReusableConnection rconn = client->connectionPool.front();
		client->connectionPool.pop();

		if (rconn.expirationTime > now()) {
			TraceEvent("RESTClient_ReusableConnection")
			    .suppressFor(60)
			    .detail("RemoteEndpoint", rconn.conn->getPeerAddress())
			    .detail("ExpireIn", rconn.expirationTime - now());
			return rconn;
		}
	}

	state Reference<IConnection> conn = wait(INetworkConnections::net()->connect(
	    client->host, client->service, client->knobs.secure_connection ? true : false));
	wait(conn->connectHandshake());

	return RESTClient::ReusableConnection({ conn, now() + client->knobs.max_connection_life });
}

Future<RESTClient::ReusableConnection> RESTClient::connect() {
	return connect_impl(Reference<RESTClient>::addRef(this));
}

void RESTClient::returnConnection(ReusableConnection& rconn) {
	// If it expires in the future then add it to the pool in the front iff connection pool size is not maxed
	if (rconn.expirationTime > now() && connectionPool.size() < knobs.connection_pool_size)
		connectionPool.push(rconn);
	rconn.conn = Reference<IConnection>();
}

void RESTClient::setKnobs(const std::unordered_map<std::string, int>& knobSettings) {
	knobs.set(knobSettings);
}

std::unordered_map<std::string, int> RESTClient::getKnobs() const {
	return knobs.get();
}

ACTOR Future<Reference<HTTP::Response>> doRequest_impl(Reference<RESTClient> client,
                                                       std::string verb,
                                                       std::string resource,
                                                       HTTP::Headers headers,
                                                       UnsentPacketQueue* pContent,
                                                       int contentLen,
                                                       std::set<unsigned int> successCodes) {
	state UnsentPacketQueue contentCopy;

	headers["Content-Length"] = format("%d", contentLen);
	headers["Host"] = client->host;

	state int maxTries = std::min(client->knobs.request_tries, client->knobs.connect_tries);
	state int thisTry = 1;
	state double nextRetryDelay = 2.0;
	state Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	state double reqTimeout = (client->knobs.request_timeout_secs * 1.0) / 60;

	loop {
		state Optional<Error> err;
		state Optional<NetworkAddress> remoteAddress;
		state bool connectionEstablished = false;
		state Reference<HTTP::Response> r;

		try {
			// Start connecting
			Future<RESTClient::ReusableConnection> frconn = client->connect();

			// Make a shallow copy of the queue by calling addref() on each buffer in the chain and then prepending that
			// chain to contentCopy
			contentCopy.discardAll();
			if (pContent != nullptr) {
				PacketBuffer* pFirst = pContent->getUnsent();
				PacketBuffer* pLast = nullptr;
				for (PacketBuffer* p = pFirst; p != nullptr; p = p->nextPacketBuffer()) {
					p->addref();
					// Also reset the sent count on each buffer
					p->bytes_sent = 0;
					pLast = p;
				}
				contentCopy.prependWriteBuffer(pFirst, pLast);
			}

			// Finish connecting, do request
			state RESTClient::ReusableConnection rconn = wait(timeoutError(frconn, client->knobs.connect_timeout));
			connectionEstablished = true;

			remoteAddress = rconn.conn->getPeerAddress();
			Reference<HTTP::Response> _r = wait(timeoutError(HTTP::doRequest(rconn.conn,
			                                                                 verb,
			                                                                 resource,
			                                                                 headers,
			                                                                 &contentCopy,
			                                                                 contentLen,
			                                                                 sendReceiveRate,
			                                                                 &client->stats.bytes_sent,
			                                                                 sendReceiveRate),
			                                                 reqTimeout));
			r = _r;

			// Since the response was parsed successfully (which is why we are here) reuse the connection unless we
			// received the "Connection: close" header.
			if (r->headers["Connection"] != "close") {
				client->returnConnection(rconn);
			}
			rconn.conn.clear();
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			err = e;
		}

		// If err is not present then r is valid.
		// If r->code is in successCodes then record the successful request and return r.
		if (!err.present() && successCodes.count(r->code) != 0) {
			client->stats.requests_successful++;
			return r;
		}

		// Otherwise, this request is considered failed.  Update failure count.
		client->stats.requests_failed++;

		// All errors in err are potentially retryable as well as certain HTTP response codes...
		bool retryable = err.present() || r->code == HTTP::HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR ||
		                 r->code == HTTP::HTTP_STATUS_CODE_BAD_GATEWAY ||
		                 r->code == HTTP::HTTP_STATUS_CODE_SERVICE_UNAVAILABLE ||
		                 r->code == HTTP::HTTP_STATUS_CODE_TOO_MANY_REQUESTS;

		// But only if our previous attempt was not the last allowable try.
		retryable = retryable && (thisTry < maxTries);

		TraceEvent event(SevWarn, retryable ? "RESTClient_FailedRetryable" : "RESTClient_RequestFailed");

		// Attach err to trace event if present, otherwise extract some stuff from the response
		if (err.present()) {
			event.errorUnsuppressed(err.get());
		}
		event.suppressFor(60);
		if (!err.present()) {
			event.detail("ResponseCode", r->code);
		}

		event.detail("ConnectionEstablished", connectionEstablished);

		if (remoteAddress.present())
			event.detail("RemoteEndpoint", remoteAddress.get());
		else
			event.detail("RemoteHost", client->host);

		event.detail("Verb", verb).detail("Resource", resource).detail("ThisTry", thisTry);

		// If r is not valid or not code TOO_MANY_REQUESTS then increment the try count.
		// TOO_MANY_REQUEST's will not count against the attempt limit.
		if (!r || r->code != HTTP::HTTP_STATUS_CODE_TOO_MANY_REQUESTS) {
			++thisTry;
		}

		// We will wait delay seconds before the next retry, start with nextRetryDelay.
		double delay = nextRetryDelay;
		// Double but limit the *next* nextRetryDelay.
		nextRetryDelay = std::min(nextRetryDelay * 2, 60.0);

		if (retryable) {
			// If r is valid then obey the Retry-After response header if present.
			if (r) {
				auto iRetryAfter = r->headers.find("Retry-After");
				if (iRetryAfter != r->headers.end()) {
					event.detail("RetryAfterHeader", iRetryAfter->second);
					char* pEnd;
					double retryAfter = strtod(iRetryAfter->second.c_str(), &pEnd);
					if (*pEnd) {
						// If there were other characters then don't trust the parsed value
						retryAfter = HTTP::HTTP_RETRYAFTER_DELAY_SECS;
					}
					// Update delay
					delay = std::max(delay, retryAfter);
				}
			}

			// Log the delay then wait.
			event.detail("RetryDelay", delay);
			wait(::delay(delay));
		} else {
			// We can't retry, so throw something.

			// This error code means the authentication header was not accepted, likely the account or key is wrong.
			if (r && r->code == HTTP::HTTP_STATUS_CODE_NOT_ACCEPTABLE) {
				throw http_not_accepted();
			}

			if (r && r->code == HTTP::HTTP_STATUS_CODE_UNAUTHORIZED) {
				throw http_auth_failed();
			}

			// Recognize and throw specific errors
			if (err.present()) {
				int code = err.get().code();

				// If we get a timed_out error during the the connect() phase, we'll call that connection_failed despite
				// the fact that there was technically never a 'connection' to begin with.  It differentiates between an
				// active connection timing out vs a connection timing out, though not between an active connection
				// failing vs connection attempt failing.
				// TODO:  Add more error types?
				if (code == error_code_timed_out && !connectionEstablished) {
					throw connection_failed();
				}

				if (code == error_code_timed_out || code == error_code_connection_failed ||
				    code == error_code_lookup_failed) {
					throw err.get();
				}
			}

			throw http_request_failed();
		}
	}
}

Future<Reference<HTTP::Response>> RESTClient::doRequest(std::string verb,
                                                        std::string resource,
                                                        HTTP::Headers headers,
                                                        UnsentPacketQueue* pContent,
                                                        int contentLen,
                                                        std::set<unsigned int> successCodes) {
	return doRequest_impl(
	    Reference<RESTClient>::addRef(this), verb, resource, headers, pContent, contentLen, successCodes);
}

// Only used to link unit tests
void forceLinkRESTClientTests() {}

TEST_CASE("fdbclient/RESTClient") {
	// invalid protocol
	try {
		std::string uri("httpx://foo/bar");
		RESTClient r(uri);
		ASSERT(false);
	} catch (Error& e) {
		if (e.code() != error_code_rest_invalid_uri) {
			throw e;
		}
	}

	// mismatch protocol and knob values
	try {
		std::string uri("http://foo/bar");
		RESTClient r(uri);
		ASSERT(false);
	} catch (Error& e) {
		if (e.code() != error_code_rest_invalid_uri) {
			throw e;
		}
	}

	// missing host
	try {
		std::string uri("https://:/bar");
		RESTClient r(uri);
		ASSERT(false);
	} catch (Error& e) {
		if (e.code() != error_code_rest_invalid_uri) {
			throw e;
		}
	}

	// valid URI with service
	try {
		std::string uri("https://host:80/foo/bar");
		RESTClient r(uri);
		ASSERT_EQ(r.host.compare("host"), 0);
		ASSERT_EQ(r.service.compare("80"), 0);
		ASSERT_EQ(r.resource.compare("foo/bar"), 0);
	} catch (Error& e) {
		throw e;
	}

	// valid URI with-out service
	try {
		std::string uri("https://host/foo/bar");
		RESTClient r(uri);
		ASSERT_EQ(r.host.compare("host"), 0);
		ASSERT(r.service.empty());
		ASSERT_EQ(r.resource.compare("foo/bar"), 0);
	} catch (Error& e) {
		throw e;
	}

	// valid URI with parameters
	try {
		std::string uri("https://host/foo/bar?param1,param2");
		RESTClient r(uri);
		ASSERT_EQ(r.host.compare("host"), 0);
		ASSERT(r.service.empty());
		ASSERT_EQ(r.resource.compare("foo/bar"), 0);
		ASSERT_EQ(r.reqParameters.compare("param1,param2"), 0);
	} catch (Error& e) {
		throw e;
	}

	// ensure RESTClient::Knob default values and updates
	{
		RESTClient r("https://localhost:8080/foo");
		std::unordered_map<std::string, int> knobs = r.getKnobs();
		ASSERT_EQ(knobs["secure_connection"], RESTClient::Knobs::SECURE_CONNECTION);
		ASSERT_EQ(knobs["connection_pool_size"], CLIENT_KNOBS->REST_CLIENT_MAX_CONNECTIONPOOL_SIZE);
		ASSERT_EQ(knobs["connect_tries"], CLIENT_KNOBS->REST_CLIENT_CONNECT_TRIES);
		ASSERT_EQ(knobs["connect_timeout"], CLIENT_KNOBS->REST_CLIENT_CONNECT_TIMEOUT);
		ASSERT_EQ(knobs["max_connection_life"], CLIENT_KNOBS->REST_CLIENT_MAX_CONNECTION_LIFE);
		ASSERT_EQ(knobs["request_tries"], CLIENT_KNOBS->REST_CLIENT_REQUEST_TRIES);
		ASSERT_EQ(knobs["request_timeout_secs"], CLIENT_KNOBS->REST_CLIENT_REQUEST_TIMEOUT_SEC);

		for (auto& itr : knobs) {
			itr.second++;
		}
		r.setKnobs(knobs);

		std::unordered_map<std::string, int> updated = r.getKnobs();
		for (auto& itr : updated) {
			ASSERT_EQ(knobs[itr.first], itr.second);
		}

		// invalid client knob
		knobs["foo"] = 100;
		try {
			r.setKnobs(knobs);
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() != error_code_rest_invalid_rest_client_knob) {
				throw e;
			}
		}
	}

	return Void();
}