/*
 * RESTClient.h
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

#ifndef FDBCLIENT_RESTCLIENT_H
#define FDBCLIENT_RESTCLIENT_H

#pragma once

#include "fdbclient/HTTP.h"
#include "fdbclient/JSONDoc.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/Net2Packet.h"

// This interface enables sending REST HTTP requests and receiving REST HTTP responses from a resource identified by a
// URI.

class RESTClient : public ReferenceCounted<RESTClient> {
public:
	struct Stats {
		Stats() : requests_successful(0), requests_failed(0), bytes_sent(0) {}
		explicit Stats(std::string& hs) : hostService(hs), requests_successful(0), requests_failed(0), bytes_sent(0) {}
		Stats operator-(const Stats& rhs);
		void clear() { requests_failed = requests_successful = bytes_sent = 0; }
		json_spirit::mObject getJSON();

		std::string hostService;
		int64_t requests_successful;
		int64_t requests_failed;
		int64_t bytes_sent;
	};

	// Client connection pool parameters
	struct ReusableConnection {
		Reference<IConnection> conn;
		double expirationTime;
	};

	// RESTClient knobs
	struct Knobs {
		Knobs();

		int connection_pool_size, secure_connection, connect_timeout, connect_tries, max_connection_life, request_tries,
		    request_timeout_secs;
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

		constexpr static int SECURE_CONNECTION = 1;
		constexpr static int NOT_SECURE_CONNECTION = 0;
	};

	// Maximum number of connections cached in the connection-pool.
	int maxConnections;
	std::queue<ReusableConnection> connectionPool;
	// Connection resources - host and port details
	std::string host;
	std::string service;
	// resource identified by URI
	std::string resource;
	// optional REST request parameters
	std::string reqParameters;

	RESTClient::Knobs knobs;
	Stats stats;

	explicit RESTClient(const std::string& uri);
	explicit RESTClient(const std::string& uri, std::unordered_map<std::string, int>& params);

	void setKnobs(const std::unordered_map<std::string, int>& knobSettings);
	std::unordered_map<std::string, int> getKnobs() const;

	// Allows clients to intiaite REST request to the specified remote-address; it would do the following:
	// 1. Establish a new connection to the remote-address.
	// 2. Leverage HTTP::doRequest to perfrom request REST operation.
	// 3. Handle error responses including request retries. Request retries implements exponential back-off to avoid
	//    flooding too many requests to the remote end within a short period of time.
	Future<Reference<HTTP::Response>> doRequest(std::string verb,
	                                            std::string resource,
	                                            HTTP::Headers headers,
	                                            UnsentPacketQueue* pContent,
	                                            int contentLen,
	                                            std::set<unsigned int> successCodes);

	// Routine is responsible to provide an usable TCP connection object; it reuses an active connection from
	// connection-pool if availalbe, otherwise, establish a new TCP connection
	Future<ReusableConnection> connect();
	void returnConnection(ReusableConnection& conn);

private:
	void parseUri(const std::string& uri);
};

#endif
