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

#include "flow/flow.h"
#include "flow/Net2Packet.h"
#include "fdbrpc/IRateControl.h"
#include "fdbclient/Knobs.h"

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
} // namespace HTTP
