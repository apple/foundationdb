/*
 * HTTP.h
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

#ifndef FDBRPC_HTTP_H
#define FDBRPC_HTTP_H

#pragma once

#include "flow/flow.h"
#include "flow/Net2Packet.h"
#include "flow/IRateControl.h"

namespace HTTP {
struct is_iless {
	bool operator()(const std::string& a, const std::string& b) const { return strcasecmp(a.c_str(), b.c_str()) < 0; }
};

typedef std::map<std::string, std::string, is_iless> Headers;

std::string urlEncode(const std::string& s);
std::string awsV4URIEncode(const std::string& s, bool encodeSlash);

struct Response : ReferenceCounted<Response> {
	Response() {}
	Future<Void> read(Reference<IConnection> conn, bool header_only);
	std::string toString();
	float version;
	int code;
	Headers headers;
	std::string content;
	int64_t contentLen;

	bool verifyMD5(bool fail_if_header_missing, Optional<std::string> content_sum = Optional<std::string>());
};

// Prepend the HTTP request header to the given PacketBuffer, returning the new head of the buffer chain
PacketBuffer* writeRequestHeader(std::string const& verb,
                                 std::string const& resource,
                                 HTTP::Headers const& headers,
                                 PacketBuffer* dest);

// Do an HTTP request to the blob store, parse the response.
Future<Reference<Response>> doRequest(Reference<IConnection> const& conn,
                                      std::string const& verb,
                                      std::string const& resource,
                                      HTTP::Headers const& headers,
                                      UnsentPacketQueue* const& pContent,
                                      int const& contentLen,
                                      Reference<IRateControl> const& sendRate,
                                      int64_t* const& pSent,
                                      Reference<IRateControl> const& recvRate,
                                      const std::string& requestHeader = std::string());

// Connect to proxy, send CONNECT command, and connect to the remote host.
Future<Reference<IConnection>> proxyConnect(const std::string& remoteHost,
                                            const std::string& remoteService,
                                            const std::string& proxyHost,
                                            const std::string& proxyService);

constexpr int HTTP_STATUS_CODE_OK = 200;
constexpr int HTTP_STATUS_CODE_CREATED = 201;
constexpr int HTTP_STATUS_CODE_ACCEPTED = 202;
constexpr int HTTP_STATUS_CODE_NO_CONTENT = 204;
constexpr int HTTP_STATUS_CODE_UNAUTHORIZED = 401;
constexpr int HTTP_STATUS_CODE_NOT_ACCEPTABLE = 406;
constexpr int HTTP_STATUS_CODE_TOO_MANY_REQUESTS = 429;
constexpr int HTTP_STATUS_CODE_INTERNAL_SERVER_ERROR = 500;
constexpr int HTTP_STATUS_CODE_BAD_GATEWAY = 502;
constexpr int HTTP_STATUS_CODE_SERVICE_UNAVAILABLE = 503;

constexpr int HTTP_RETRYAFTER_DELAY_SECS = 300;

const std::string HTTP_VERB_GET = "GET";
const std::string HTTP_VERB_HEAD = "HEAD";
const std::string HTTP_VERB_DELETE = "DELETE";
const std::string HTTP_VERB_TRACE = "TRACE";
const std::string HTTP_VERB_PUT = "PUT";
const std::string HTTP_VERB_POST = "POST";

} // namespace HTTP

#endif
