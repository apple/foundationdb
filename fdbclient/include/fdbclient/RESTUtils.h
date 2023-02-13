/*
 * RESTUtils.h
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

#ifndef FDRPC_REST_UTILS_H
#define FDRPC_REST_UTILS_H

#pragma once

#include "flow/flow.h"
#include "flow/FastRef.h"
#include "flow/Net2Packet.h"

#include <unordered_map>
#include <utility>

// Util interface managing REST active connection pool.
// The interface internally constructs and maintains map {"host:service" -> activeConnection}; any new connection
// request would first access cached connection if possible (not expired), if none exists, it would establish a new
// connection and return to the caller. Caller on accomplishing the task at-hand, should return the connection back to
// the pool.

using RESTConnectionPoolKey = std::pair<std::string, std::string>;

class IConnection;

class RESTConnectionPool : public ReferenceCounted<RESTConnectionPool> {
public:
	struct ReusableConnection {
		Reference<IConnection> conn;
		double expirationTime;
	};

	// Maximum number of connections cached in the connection-pool.
	int maxConnPerConnectKey;
	std::map<RESTConnectionPoolKey, std::queue<ReusableConnection>> connectionPoolMap;

	RESTConnectionPool(const int maxConnsPerKey) : maxConnPerConnectKey(maxConnsPerKey) {}

	// Routine is responsible to provide an usable TCP connection object; it reuses an active connection from
	// connection-pool if availalbe, otherwise, establish a new TCP connection
	Future<ReusableConnection> connect(RESTConnectionPoolKey connectKey, const bool isSecure, const int maxConnLife);
	void returnConnection(RESTConnectionPoolKey connectKey, ReusableConnection& conn, const int maxConnections);

	static RESTConnectionPoolKey getConnectionPoolKey(const std::string& host, const std::string& service) {
		return std::make_pair(host, service);
	}
};

// Util interface facilitating management and update for RESTClient knob parameters
struct RESTClientKnobs {
	int connection_pool_size, secure_connection, connect_timeout, connect_tries, max_connection_life, request_tries,
	    request_timeout_secs;

	constexpr static int SECURE_CONNECTION = 1;
	constexpr static int NOT_SECURE_CONNECTION = 0;

	RESTClientKnobs();

	void set(const std::unordered_map<std::string, int>& knobSettings);
	std::unordered_map<std::string, int> get() const;
	std::unordered_map<std::string, int*> knobMap;

	static std::vector<std::string> getKnobDescriptions() {
		return {
			"connection_pool_size (pz)             Maximum numbers of active connections in the connection-pool",
			"secure_connection (or sc)             Set 1 for secure connection and 0 for insecure connection.",
			"connect_tries (or ct)                 Number of times to try to connect for each request.",
			"connect_timeout (or cto)              Number of seconds to wait for a connect request to succeed.",
			"max_connection_life (or mcl)          Maximum number of seconds to use a single TCP connection.",
			"request_tries (or rt)                 Number of times to try each request until a parsable HTTP "
			"response other than 429 is received.",
			"request_timeout_secs (or rtom)        Number of seconds to wait for a request to succeed after a "
			"connection is established.",
		};
	}
};

// Util interface facilitating parsing of an input REST 'full_url'
struct RESTUrl {
public:
	// Connection resources - host and port details
	std::string host;
	std::string service;
	// resource identified by URI
	std::string resource;
	// optional REST request parameters
	std::string reqParameters;
	// Request 'body' payload
	std::string body;

	explicit RESTUrl(const std::string& fullUrl, const bool isSecure);
	explicit RESTUrl(const std::string& fullUrl, const std::string& body, const bool isSecure);

private:
	void parseUrl(const std::string& fullUrl, bool isSecure);
};

#endif