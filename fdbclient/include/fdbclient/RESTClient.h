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

#ifndef FDBRPC_RESTCLIENT_H
#define FDBRPC_RESTCLIENT_H

#pragma once

#include "fdbclient/JSONDoc.h"
#include "fdbrpc/HTTP.h"
#include "fdbclient/RESTUtils.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/flow.h"
#include "flow/Net2Packet.h"

#include <memory>

// This interface enables sending REST HTTP requests and receiving REST HTTP responses from a resource identified by a
// URI.

class RESTClient : public ReferenceCounted<RESTClient> {
public:
	struct Stats {
		explicit Stats(const std::string& hService)
		  : host_service(hService), requests_successful(0), requests_failed(0), bytes_sent(0) {}
		Stats operator-(const Stats& rhs);
		void clear() { requests_failed = requests_successful = bytes_sent = 0; }
		json_spirit::mObject getJSON();

		std::string host_service;
		int64_t requests_successful;
		int64_t requests_failed;
		int64_t bytes_sent;
	};

	RESTClientKnobs knobs;
	Reference<RESTConnectionPool> conectionPool;
	// Connection stats maintained per "host:service"
	std::unordered_map<std::string, std::unique_ptr<Stats>> statsMap;

	RESTClient();
	explicit RESTClient(std::unordered_map<std::string, int>& params);

	void setKnobs(const std::unordered_map<std::string, int>& knobSettings);
	std::unordered_map<std::string, int> getKnobs() const;

	// Supports common REST APIs.
	// On invocation of below methods, input 'fullUrl' is parsed using RESTUrl interface,
	// RESTConnectionPool is used to leverage cached connection if any for 'host:service' pair. API then leverage
	// HTTP::doRequest to accomplish the specified operation

	Future<Reference<HTTP::IncomingResponse>> doGet(const std::string& fullUrl,
	                                                Optional<HTTP::Headers> optHeaders = Optional<HTTP::Headers>());
	Future<Reference<HTTP::IncomingResponse>> doHead(const std::string& fullUrl,
	                                                 Optional<HTTP::Headers> optHeaders = Optional<HTTP::Headers>());
	Future<Reference<HTTP::IncomingResponse>> doDelete(const std::string& fullUrl,
	                                                   Optional<HTTP::Headers> optHeaders = Optional<HTTP::Headers>());
	Future<Reference<HTTP::IncomingResponse>> doTrace(const std::string& fullUrl,
	                                                  Optional<HTTP::Headers> optHeaders = Optional<HTTP::Headers>());
	Future<Reference<HTTP::IncomingResponse>> doPut(const std::string& fullUrl,
	                                                const std::string& requestBody,
	                                                Optional<HTTP::Headers> optHeaders = Optional<HTTP::Headers>());
	Future<Reference<HTTP::IncomingResponse>> doPost(const std::string& fullUrl,
	                                                 const std::string& requestBody,
	                                                 Optional<HTTP::Headers> optHeaders = Optional<HTTP::Headers>());

	static std::string getStatsKey(const std::string& host, const std::string& service) { return host + ":" + service; }

private:
	Future<Reference<HTTP::IncomingResponse>> doGetHeadDeleteOrTrace(const std::string& verb,
	                                                                 Optional<HTTP::Headers> optHeaders,
	                                                                 RESTUrl& url,
	                                                                 std::set<unsigned int> successCodes);
	Future<Reference<HTTP::IncomingResponse>> doPutOrPost(const std::string& verb,
	                                                      Optional<HTTP::Headers> headers,
	                                                      RESTUrl& url,
	                                                      std::set<unsigned int> successCodes);
};

#endif
