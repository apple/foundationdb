/*
 * FDBLibTLSVerify.h
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

#ifndef FDB_LIBTLS_VERIFY_H
#define FDB_LIBTLS_VERIFY_H

#pragma once

#include <stdint.h>

#include "flow/FastRef.h"

#include <map>
#include <string>
#include <utility>

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
	Criteria(const std::string& s) : criteria(s), match_type(MatchType::EXACT), location(X509Location::NAME) {}
	Criteria(const std::string& s, MatchType mt) : criteria(s), match_type(mt), location(X509Location::NAME) {}
	Criteria(const std::string& s, X509Location loc) : criteria(s), match_type(MatchType::EXACT), location(loc) {}
	Criteria(const std::string& s, MatchType mt, X509Location loc) : criteria(s), match_type(mt), location(loc) {}

	std::string criteria;
	MatchType match_type;
	X509Location location;

	bool operator==(const Criteria& c) const {
		return criteria == c.criteria && match_type == c.match_type && location == c.location;
	}
};

struct FDBLibTLSVerify : ReferenceCounted<FDBLibTLSVerify> {
	FDBLibTLSVerify(std::string verify);
	virtual ~FDBLibTLSVerify();

	virtual void addref() { ReferenceCounted<FDBLibTLSVerify>::addref(); }
	virtual void delref() { ReferenceCounted<FDBLibTLSVerify>::delref(); }

	void parse_verify(std::string input);

	bool verify_cert;
	bool verify_time;

	std::map<NID, Criteria> subject_criteria;
	std::map<NID, Criteria> issuer_criteria;
	std::map<NID, Criteria> root_criteria;
};

#endif /* FDB_LIBTLS_VERIFY_H */
