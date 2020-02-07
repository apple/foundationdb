/*
 * TLSPolicy.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#ifndef _FLOW_TLSPOLICY_H_
#define _FLOW_TLSPOLICY_H_
#pragma once

#include <map>
#include <string>
#include <vector>
#include <openssl/x509.h>
#include "flow/FastRef.h"

typedef int NID;

enum class MatchType {
	EXACT,
	PREFIX,
	SUFFIX,
};

enum class X509Location {
	// This NID is located within a X509_NAME
	NAME,
	// This NID is an X509 extension, and should be parsed accordingly
	EXTENSION,
};

struct Criteria {
	Criteria( const std::string& s )
		: criteria(s), match_type(MatchType::EXACT), location(X509Location::NAME) {}
	Criteria( const std::string& s, MatchType mt )
		: criteria(s), match_type(mt), location(X509Location::NAME) {}
	Criteria( const std::string& s, X509Location loc)
		: criteria(s), match_type(MatchType::EXACT), location(loc) {}
	Criteria( const std::string& s, MatchType mt, X509Location loc)
		: criteria(s), match_type(mt), location(loc) {}

	std::string criteria;
	MatchType match_type;
	X509Location location;

	bool operator==(const Criteria& c) const {
		return criteria == c.criteria && match_type == c.match_type && location == c.location;
	}
};

class TLSPolicy : ReferenceCounted<TLSPolicy> {
public:
	enum class Is {
		CLIENT,
		SERVER
	};

	bool set_verify_peers(std::vector<std::string> verify_peers);
	bool verify_peer(X509_STORE_CTX* store_ctx);

	TLSPolicy(Is client) : is_client(client == Is::CLIENT) {}
	virtual ~TLSPolicy();

	virtual void addref() { ReferenceCounted<TLSPolicy>::addref(); }
	virtual void delref() { ReferenceCounted<TLSPolicy>::delref(); }

	struct Rule {
		explicit Rule(std::string input);

		std::map< NID, Criteria > subject_criteria;
		std::map< NID, Criteria > issuer_criteria;
		std::map< NID, Criteria > root_criteria;

		bool verify_cert = true;
		bool verify_time = true;
	};

	std::vector<Rule> rules;
	bool is_client;
};

#endif
