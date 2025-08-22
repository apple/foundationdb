/*
 * HTTP.h
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

#ifndef FDBRPC_HTTP_H
#define FDBRPC_HTTP_H

#include "flow/NetworkAddress.h"
#pragma once

#include "flow/flow.h"
#include "flow/ActorCollection.h"
#include "flow/IConnection.h"
#include "flow/IRateControl.h"
#include "flow/Net2Packet.h"

namespace HTTP {
struct is_iless {
	bool operator()(const std::string& a, const std::string& b) const { return strcasecmp(a.c_str(), b.c_str()) < 0; }
};

constexpr int HTTP_STATUS_CODE_OK = 200;
constexpr int HTTP_STATUS_CODE_CREATED = 201;
constexpr int HTTP_STATUS_CODE_ACCEPTED = 202;
constexpr int HTTP_STATUS_CODE_NO_CONTENT = 204;
constexpr int HTTP_STATUS_CODE_BAD_REQUEST = 400;
constexpr int HTTP_STATUS_CODE_UNAUTHORIZED = 401;
constexpr int HTTP_STATUS_CODE_NOT_FOUND = 404;
constexpr int HTTP_STATUS_CODE_NOT_ACCEPTABLE = 406;
constexpr int HTTP_STATUS_CODE_TIMEOUT = 408;
constexpr int HTTP_STATUS_CODE_TOO_MANY_REQUESTS = 429;
constexpr int HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR = 500;
constexpr int HTTP_STATUS_CODE_BAD_GATEWAY = 502;
constexpr int HTTP_STATUS_CODE_SERVICE_UNAVAILABLE = 503;
constexpr int HTTP_STATUS_CODE_GATEWAY_TIMEOUT = 504;

constexpr int HTTP_RETRYAFTER_DELAY_SECS = 300;

const std::string HTTP_VERB_GET = "GET";
const std::string HTTP_VERB_HEAD = "HEAD";
const std::string HTTP_VERB_DELETE = "DELETE";
const std::string HTTP_VERB_TRACE = "TRACE";
const std::string HTTP_VERB_PUT = "PUT";
const std::string HTTP_VERB_POST = "POST";
const std::string HTTP_VERB_CONNECT = "CONNECT";

typedef std::map<std::string, std::string, is_iless> Headers;

std::string urlEncode(const std::string& s);
std::string urlDecode(const std::string& s);
std::string awsV4URIEncode(const std::string& s, bool encodeSlash);

template <class T>
struct HTTPData {
	Headers headers;
	int64_t contentLen;
	T content;
};

// computes the sum in base-64 for http
std::string computeMD5Sum(std::string content);

// class methods on template type classes are weird
bool verifyMD5(HTTPData<std::string>* data,
               bool fail_if_header_missing,
               Optional<std::string> content_sum = Optional<std::string>());

template <class T>
struct RequestBase : ReferenceCounted<RequestBase<T>> {
	RequestBase() {}
	std::string verb;
	std::string resource;
	HTTPData<T> data;

	bool isHeaderOnlyResponse() {
		return verb == HTTP_VERB_HEAD || verb == HTTP_VERB_DELETE || verb == HTTP_VERB_CONNECT;
	}
};

// TODO: utility for constructing packet buffer from string OutgoingRequest
struct IncomingRequest : RequestBase<std::string> {
	Future<Void> read(Reference<IConnection> conn, bool header_only = false);
};
struct OutgoingRequest : RequestBase<UnsentPacketQueue*> {};

template <class T>
struct ResponseBase : ReferenceCounted<ResponseBase<T>> {
	ResponseBase() : code(200) {} // Initialize code to 200 (OK) by default
	float version;
	int code;
	HTTPData<T> data;

	std::string getCodeDescription();
};

struct IncomingResponse : ResponseBase<std::string> {
	std::string toString() const; // for debugging
	Future<Void> read(Reference<IConnection> conn, bool header_only = false);
};
struct OutgoingResponse : ResponseBase<UnsentPacketQueue*> {
	Future<Void> write(Reference<IConnection> conn);
	void reset();
};

// Do an HTTP request to the blob store, parse the response.
Future<Reference<IncomingResponse>> doRequest(Reference<IConnection> conn,
                                              Reference<OutgoingRequest> request,
                                              Reference<IRateControl> sendRate,
                                              int64_t* pSent,
                                              Reference<IRateControl> recvRate);

// Connect to proxy, send CONNECT command, and connect to the remote host.
Future<Reference<IConnection>> proxyConnect(const std::string& remoteHost,
                                            const std::string& remoteService,
                                            const std::string& proxyHost,
                                            const std::string& proxyService);

// HTTP server stuff

// Registers an http handler that just asserts false if it ever gets an http request.
// This is used to validate that clients only talk to the http servers they're supposed to talk to
Future<Void> registerAlwaysFailHTTPHandler();

// Implementation of http server that handles http requests
// TODO: could change to factory pattern instead of clone pattern
struct IRequestHandler {
	// Sets up state for each instance of the handler. Provides default stateless implementation, but a stateful handler
	// must override this.
	virtual Future<Void> init() { return Future<Void>(Void()); };

	// Actual callback implementation. Fills out the response object based on the request.
	virtual Future<Void> handleRequest(Reference<IncomingRequest>, Reference<OutgoingResponse>) = 0;

	// If each instance has a mix of global state provided in the type-specific constructor, but then also local state
	// instantiated in init, the default instance passed to registerSimHTTPServer is cloned for each process to copy the
	// global state, but before init is called. You may optionally clone after init, but the contract is that clone must
	// not copy or share the non-global state between instances.
	virtual Reference<IRequestHandler> clone() = 0;

	// for reference counting an interface - don't implement ReferenceCounted<T>
	virtual void addref() = 0;
	virtual void delref() = 0;
};

struct SimRegisteredHandlerContext : ReferenceCounted<SimRegisteredHandlerContext>, NonCopyable {
public:
	std::string hostname;
	std::string service;
	int port;
	Reference<IRequestHandler> requestHandler;

	SimRegisteredHandlerContext(std::string hostname,
	                            std::string service,
	                            int port,
	                            Reference<IRequestHandler> requestHandler)
	  : hostname(hostname), service(service), port(port), requestHandler(requestHandler) {}

	void addAddress(NetworkAddress addr);
	void removeIp(IPAddress addr);

private:
	std::vector<NetworkAddress> addresses;
	void updateDNS();
};

struct SimServerContext : ReferenceCounted<SimServerContext>, NonCopyable {
	UID dbgid;
	bool running;
	ActorCollection actors;
	std::vector<NetworkAddress> listenAddresses;
	std::vector<Future<Void>> listenBinds;
	std::vector<Reference<IListener>> listeners;

	SimServerContext() : dbgid(deterministicRandom()->randomUniqueID()), running(true), actors(false) {}

	void registerNewServer(NetworkAddress addr, Reference<IRequestHandler> server);

	void stop() {
		running = false;
		actors = ActorCollection(false);
		listenAddresses.clear();
		listenBinds.clear();
		listeners.clear();
	}
};

} // namespace HTTP

#endif
