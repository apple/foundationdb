/*
 * FDBLibTLSVerify.cpp
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

#include "FDBLibTLS/FDBLibTLSVerify.h"

#include <openssl/objects.h>

#include <algorithm>
#include <exception>
#include <cstring>

static int hexValue(char c) {
	static char const digits[] = "0123456789ABCDEF";

	if (c >= 'a' && c <= 'f')
		c -= ('a' - 'A');

	int value = std::find(digits, digits + 16, c) - digits;
	if (value >= 16) {
		throw std::runtime_error("hexValue");
	}
	return value;
}

// Does not handle "raw" form (e.g. #28C4D1), only escaped text
static std::string de4514(std::string const& input, int start, int& out_end) {
	std::string output;

	if (input[start] == '#' || input[start] == ' ') {
		out_end = start;
		return output;
	}

	int space_count = 0;

	for (int p = start; p < input.size();) {
		switch (input[p]) {
		case '\\': // Handle escaped sequence

			// Backslash escaping nothing!
			if (p == input.size() - 1) {
				out_end = p;
				goto FIN;
			}

			switch (input[p + 1]) {
			case ' ':
			case '"':
			case '#':
			case '+':
			case ',':
			case ';':
			case '<':
			case '=':
			case '>':
			case '|':
			case '\\':
				output += input[p + 1];
				p += 2;
				space_count = 0;
				continue;

			default:
				// Backslash escaping pair of hex digits requires two characters
				if (p == input.size() - 2) {
					out_end = p;
					goto FIN;
				}

				try {
					output += hexValue(input[p + 1]) * 16 + hexValue(input[p + 2]);
					p += 3;
					space_count = 0;
					continue;
				} catch (...) {
					out_end = p;
					goto FIN;
				}
			}

		case '"':
		case '+':
		case ',':
		case ';':
		case '<':
		case '>':
		case 0:
			// All of these must have been escaped
			out_end = p;
			goto FIN;

		default:
			// Character is what it is
			output += input[p];
			if (input[p] == ' ')
				space_count++;
			else
				space_count = 0;
			p++;
		}
	}

	out_end = input.size();

FIN:
	out_end -= space_count;
	output.resize(output.size() - space_count);

	return output;
}

static std::pair<std::string, std::string> splitPair(std::string const& input, char c) {
	int p = input.find_first_of(c);
	if (p == input.npos) {
		throw std::runtime_error("splitPair");
	}
	return std::make_pair(input.substr(0, p), input.substr(p + 1, input.size()));
}

static NID abbrevToNID(std::string const& sn) {
	NID nid = NID_undef;

	if (sn == "C" || sn == "CN" || sn == "L" || sn == "ST" || sn == "O" || sn == "OU" || sn == "UID" || sn == "DC" ||
	    sn == "subjectAltName")
		nid = OBJ_sn2nid(sn.c_str());
	if (nid == NID_undef)
		throw std::runtime_error("abbrevToNID");

	return nid;
}

static X509Location locationForNID(NID nid) {
	const char* name = OBJ_nid2ln(nid);
	if (name == nullptr) {
		throw std::runtime_error("locationForNID");
	}
	if (strncmp(name, "X509v3", 6) == 0) {
		return X509Location::EXTENSION;
	} else {
		// It probably isn't true that all other NIDs live in the NAME, but it is for now...
		return X509Location::NAME;
	}
}

FDBLibTLSVerify::FDBLibTLSVerify(std::string verify_config) : verify_cert(true), verify_time(true) {
	parse_verify(verify_config);
}

FDBLibTLSVerify::~FDBLibTLSVerify() {}

void FDBLibTLSVerify::parse_verify(std::string input) {
	int s = 0;

	while (s < input.size()) {
		int eq = input.find('=', s);

		if (eq == input.npos)
			throw std::runtime_error("parse_verify");

		MatchType mt = MatchType::EXACT;
		if (input[eq - 1] == '>')
			mt = MatchType::PREFIX;
		if (input[eq - 1] == '<')
			mt = MatchType::SUFFIX;
		std::string term = input.substr(s, eq - s - (mt == MatchType::EXACT ? 0 : 1));

		if (term.find("Check.") == 0) {
			if (eq + 2 > input.size())
				throw std::runtime_error("parse_verify");
			if (eq + 2 != input.size() && input[eq + 2] != ',')
				throw std::runtime_error("parse_verify");
			if (mt != MatchType::EXACT)
				throw std::runtime_error("parse_verify: cannot prefix match Check");

			bool* flag;

			if (term == "Check.Valid")
				flag = &verify_cert;
			else if (term == "Check.Unexpired")
				flag = &verify_time;
			else
				throw std::runtime_error("parse_verify");

			if (input[eq + 1] == '0')
				*flag = false;
			else if (input[eq + 1] == '1')
				*flag = true;
			else
				throw std::runtime_error("parse_verify");

			s = eq + 3;
		} else {
			std::map<int, Criteria>* criteria = &subject_criteria;

			if (term.find('.') != term.npos) {
				auto scoped = splitPair(term, '.');

				if (scoped.first == "S" || scoped.first == "Subject")
					criteria = &subject_criteria;
				else if (scoped.first == "I" || scoped.first == "Issuer")
					criteria = &issuer_criteria;
				else if (scoped.first == "R" || scoped.first == "Root")
					criteria = &root_criteria;
				else
					throw std::runtime_error("parse_verify");

				term = scoped.second;
			}

			int remain;
			auto unesc = de4514(input, eq + 1, remain);

			if (remain == eq + 1)
				throw std::runtime_error("parse_verify");

			NID termNID = abbrevToNID(term);
			const X509Location loc = locationForNID(termNID);
			criteria->insert(std::make_pair(termNID, Criteria(unesc, mt, loc)));

			if (remain != input.size() && input[remain] != ',')
				throw std::runtime_error("parse_verify");

			s = remain + 1;
		}
	}
}
