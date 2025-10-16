/*
 * RESTClient.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/HTTP.h"
#include "flow/IRateControl.h"
#include "fdbclient/RESTUtils.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/FastRef.h"
#include "flow/Knobs.h"
#include "flow/Net2Packet.h"
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/serialize.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/IConnection.h"

#include <memory>
#include <unordered_map>

#include "flow/actorcompiler.h" // always the last include

#define TRACE_REST_OP(opName, url)                                                                                     \
	do {                                                                                                               \
		if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::DEBUG) {                                                    \
			const std::string urlStr = url.toString();                                                                 \
			TraceEvent("RESTClientOp")                                                                                 \
			    .detail("Op", #opName)                                                                                 \
			    .detail("Url", urlStr)                                                                                 \
			    .detail("IsSecure", url.connType.secure);                                                              \
		}                                                                                                              \
	} while (0);

json_spirit::mObject RESTClient::Stats::getJSON() {
	json_spirit::mObject o;

	o["host_service"] = host_service;
	o["requests_failed"] = requests_failed;
	o["requests_successful"] = requests_successful;
	o["bytes_sent"] = bytes_sent;

	return o;
}

RESTClient::Stats RESTClient::Stats::operator-(const Stats& rhs) {
	Stats r(host_service);

	r.requests_failed = requests_failed - rhs.requests_failed;
	r.requests_successful = requests_successful - rhs.requests_successful;
	r.bytes_sent = bytes_sent - rhs.bytes_sent;

	return r;
}

RESTClient::RESTClient() : conectionPool(makeReference<RESTConnectionPool>(knobs.connection_pool_size)) {}

RESTClient::RESTClient(std::unordered_map<std::string, int>& knobSettings)
  : conectionPool(makeReference<RESTConnectionPool>(knobs.connection_pool_size)) {
	knobs.set(knobSettings);
}

void RESTClient::setKnobs(const std::unordered_map<std::string, int>& knobSettings) {
	knobs.set(knobSettings);
}

std::unordered_map<std::string, int> RESTClient::getKnobs() const {
	return knobs.get();
}

bool isErrorRetryable(const Error& e) {
	// Server if unreachable or timing out requests, bubble the error to the caller to decide continue using the same
	// server OR attempt connecting to a different server instance

	return e.code() != error_code_timed_out && e.code() != error_code_connection_failed;
}

ACTOR Future<Reference<HTTP::IncomingResponse>> doRequest_impl(Reference<RESTClient> client,
                                                               std::string verb,
                                                               HTTP::Headers headers,
                                                               RESTUrl url,
                                                               std::set<unsigned int> successCodes) {

	state Reference<HTTP::OutgoingRequest> req = makeReference<HTTP::OutgoingRequest>();
	state UnsentPacketQueue content;
	req->data.content = &content;
	req->data.contentLen = url.body.size();
	req->data.headers = headers;
	req->data.headers["Host"] = url.host;
	req->verb = verb;
	req->resource = url.resource;

	if (FLOW_KNOBS->REST_LOG_LEVEL >= RESTLogSeverity::VERBOSE) {
		TraceEvent("RESTDoRequestImpl").detail("Url", url.toString());
	}

	if (url.body.size() > 0) {
		PacketWriter pw(req->data.content->getWriteBuffer(url.body.size()), nullptr, Unversioned());
		pw.serializeBytes(url.body);
	}

	std::string statsKey = RESTClient::getStatsKey(url.host, url.service);
	auto sItr = client->statsMap.find(statsKey);
	if (sItr == client->statsMap.end()) {
		client->statsMap.emplace(statsKey, std::make_unique<RESTClient::Stats>(statsKey));
	}

	state int maxTries = std::min(client->knobs.request_tries, client->knobs.connect_tries);
	state int thisTry = 1;
	state double nextRetryDelay = 2.0;
	state Reference<IRateControl> sendReceiveRate = makeReference<Unlimited>();
	state double reqTimeout = (client->knobs.request_timeout_secs * 1.0);
	state RESTConnectionPoolKey connectPoolKey = RESTConnectionPool::getConnectionPoolKey(url.host, url.service);
	state RESTClient::Stats* statsPtr = client->statsMap[statsKey].get();

	loop {
		state Optional<Error> err;
		state Optional<NetworkAddress> remoteAddress;
		state bool connectionEstablished = false;

		state Reference<HTTP::IncomingResponse> r;
		state RESTConnectionPool::ReusableConnection rconn;

		try {
			// Start connecting
			Future<RESTConnectionPool::ReusableConnection> frconn =
			    client->conectionPool->connect(connectPoolKey, url.connType.secure, client->knobs.max_connection_life);

			// Finish connecting, do request
			wait(store(rconn, timeoutError(frconn, client->knobs.connect_timeout)));
			connectionEstablished = true;

			remoteAddress = rconn.conn->getPeerAddress();
			wait(store(
			    r,
			    timeoutError(HTTP::doRequest(rconn.conn, req, sendReceiveRate, &statsPtr->bytes_sent, sendReceiveRate),
			                 reqTimeout)));

			// Since the response was parsed successfully (which is why we are here) reuse the connection unless we
			// received the "Connection: close" header.
			if (r->data.headers["Connection"] != "close") {
				client->conectionPool->returnConnection(connectPoolKey, rconn, client->knobs.connection_pool_size);
			}
			rconn.conn.clear();
		} catch (Error& e) {
			// Close the connection on error to satisfy Sim2Conn's assertion in simulation.
			// If rconn holds a connection that hasn't been returned to the pool,
			// we must close it before it's destroyed (when rconn goes out of scope).
			// This sets closedByCaller = true in Sim2Conn, preventing assertion failures.
			if (connectionEstablished && rconn.conn.isValid()) {
				rconn.conn->close();
				rconn.conn.clear();
			}
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			err = e;
		}

		// If err is not present then r is valid.
		// If r->code is in successCodes then record the successful request and return r.
		if (!err.present() && successCodes.count(r->code) != 0) {
			statsPtr->requests_successful++;
			return r;
		}

		// Otherwise, this request is considered failed.  Update failure count.
		statsPtr->requests_failed++;

		// All errors in err are potentially (except 'timed_out' and/or 'connection_failed') retryable as well as
		// certain HTTP response codes...
		bool retryable =
		    (err.present() && isErrorRetryable(err.get())) ||
		    (r.isValid() &&
		     (r->code == HTTP::HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR ||
		      r->code == HTTP::HTTP_STATUS_CODE_BAD_GATEWAY || r->code == HTTP::HTTP_STATUS_CODE_BAD_GATEWAY ||
		      r->code == HTTP::HTTP_STATUS_CODE_SERVICE_UNAVAILABLE ||
		      r->code == HTTP::HTTP_STATUS_CODE_TOO_MANY_REQUESTS || r->code == HTTP::HTTP_STATUS_CODE_TIMEOUT));

		// But only if our previous attempt was not the last allowable try.
		retryable = retryable && (thisTry < maxTries);

		TraceEvent event(SevWarn, retryable ? "RESTClientFailedRetryable" : "RESTClientRequestFailed");

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
			event.detail("RemoteHost", url.host);

		event.detail("Verb", verb).detail("Resource", url.resource).detail("ThisTry", thisTry);

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
				auto iRetryAfter = r->data.headers.find("Retry-After");
				if (iRetryAfter != r->data.headers.end()) {
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

				// If we get a timed_out error during the the connect() phase, we'll call that connection_failed
				// despite the fact that there was technically never a 'connection' to begin with.  It
				// differentiates between an active connection timing out vs a connection timing out, though not
				// between an active connection failing vs connection attempt failing.
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

Future<Reference<HTTP::IncomingResponse>> RESTClient::doPutOrPost(const std::string& verb,
                                                                  Optional<HTTP::Headers> optHeaders,
                                                                  RESTUrl& url,
                                                                  std::set<unsigned int> successCodes) {
	HTTP::Headers headers;
	if (optHeaders.present()) {
		headers = optHeaders.get();
	}

	return doRequest_impl(Reference<RESTClient>::addRef(this), verb, headers, url, successCodes);
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doPost(const std::string& fullUrl,
                                                             const std::string& requestBody,
                                                             Optional<HTTP::Headers> optHeaders) {
	RESTUrl url(fullUrl, requestBody);
	TRACE_REST_OP("DoPost", url);
	return doPutOrPost(HTTP::HTTP_VERB_POST, optHeaders, url, { HTTP::HTTP_STATUS_CODE_OK });
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doPut(const std::string& fullUrl,
                                                            const std::string& requestBody,
                                                            Optional<HTTP::Headers> optHeaders) {
	RESTUrl url(fullUrl, requestBody);
	TRACE_REST_OP("DoPut", url);
	return doPutOrPost(
	    HTTP::HTTP_VERB_PUT,
	    optHeaders,
	    url,
	    // 201 - on successful resource create
	    // 200 / 204 - if target resource representation was successfully modified with the desired state
	    { HTTP::HTTP_STATUS_CODE_OK, HTTP::HTTP_STATUS_CODE_CREATED, HTTP::HTTP_STATUS_CODE_NO_CONTENT });
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doGetHeadDeleteOrTrace(const std::string& verb,
                                                                             Optional<HTTP::Headers> optHeaders,
                                                                             RESTUrl& url,
                                                                             std::set<unsigned int> successCodes) {
	HTTP::Headers headers;
	if (optHeaders.present()) {
		headers = optHeaders.get();
	}

	return doRequest_impl(Reference<RESTClient>::addRef(this), HTTP::HTTP_VERB_GET, headers, url, successCodes);
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doGet(const std::string& fullUrl,
                                                            Optional<HTTP::Headers> optHeaders) {
	RESTUrl url(fullUrl);
	TRACE_REST_OP("DoGet", url);
	return doGetHeadDeleteOrTrace(HTTP::HTTP_VERB_GET, optHeaders, url, { HTTP::HTTP_STATUS_CODE_OK });
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doHead(const std::string& fullUrl,
                                                             Optional<HTTP::Headers> optHeaders) {
	RESTUrl url(fullUrl);
	TRACE_REST_OP("DoHead", url);
	return doGetHeadDeleteOrTrace(HTTP::HTTP_VERB_HEAD, optHeaders, url, { HTTP::HTTP_STATUS_CODE_OK });
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doDelete(const std::string& fullUrl,
                                                               Optional<HTTP::Headers> optHeaders) {
	RESTUrl url(fullUrl);
	TRACE_REST_OP("DoDelete", url);
	return doGetHeadDeleteOrTrace(
	    HTTP::HTTP_VERB_DELETE,
	    optHeaders,
	    url,
	    // 200 - action has been enacted.
	    // 202 - action will likely succeed, but, has not yet been enacted.
	    // 204 - action has been enated, no further information is to supplied.
	    { HTTP::HTTP_STATUS_CODE_OK, HTTP::HTTP_STATUS_CODE_NO_CONTENT, HTTP::HTTP_STATUS_CODE_ACCEPTED });
}

Future<Reference<HTTP::IncomingResponse>> RESTClient::doTrace(const std::string& fullUrl,
                                                              Optional<HTTP::Headers> optHeaders) {
	RESTUrl url(fullUrl);
	TRACE_REST_OP("DoTrace", url);
	return doGetHeadDeleteOrTrace(HTTP::HTTP_VERB_TRACE, optHeaders, url, { HTTP::HTTP_STATUS_CODE_OK });
}

// Only used to link unit tests
void forceLinkRESTClientTests() {}

TEST_CASE("fdbrpc/RESTClient") {
	RESTClient r;
	std::unordered_map<std::string, int> knobs = r.getKnobs();
	ASSERT_EQ(knobs["connection_pool_size"], FLOW_KNOBS->RESTCLIENT_MAX_CONNECTIONPOOL_SIZE);
	ASSERT_EQ(knobs["connect_tries"], FLOW_KNOBS->RESTCLIENT_CONNECT_TRIES);
	ASSERT_EQ(knobs["connect_timeout"], FLOW_KNOBS->RESTCLIENT_CONNECT_TIMEOUT);
	ASSERT_EQ(knobs["max_connection_life"], FLOW_KNOBS->RESTCLIENT_MAX_CONNECTION_LIFE);
	ASSERT_EQ(knobs["request_tries"], FLOW_KNOBS->RESTCLIENT_REQUEST_TRIES);
	ASSERT_EQ(knobs["request_timeout_secs"], FLOW_KNOBS->RESTCLIENT_REQUEST_TIMEOUT_SEC);

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

	return Void();
}
