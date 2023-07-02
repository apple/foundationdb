/*
 * verify-test.cpp
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

#include <iostream>
#include <string>
#include <vector>

#include <string.h>
#include <boost/lexical_cast.hpp>

#include <openssl/objects.h>

#include "fdbrpc/ITLSPlugin.h"

#include "FDBLibTLS/FDBLibTLSPlugin.h"
#include "FDBLibTLS/FDBLibTLSPolicy.h"

struct FDBLibTLSVerifyTest {
	FDBLibTLSVerifyTest(std::string input)
	  : input(input), valid(false), verify_cert(true), verify_time(true), subject_criteria({}), issuer_criteria({}),
	    root_criteria({}){};
	FDBLibTLSVerifyTest(std::string input,
	                    bool verify_cert,
	                    bool verify_time,
	                    std::map<int, Criteria> subject,
	                    std::map<int, Criteria> issuer,
	                    std::map<int, Criteria> root)
	  : input(input), valid(true), verify_cert(verify_cert), verify_time(verify_time), subject_criteria(subject),
	    issuer_criteria(issuer), root_criteria(root){};
	~FDBLibTLSVerifyTest(){};

	int run();

	std::string input;

	bool valid;
	bool verify_cert;
	bool verify_time;

	std::map<int, Criteria> subject_criteria;
	std::map<int, Criteria> issuer_criteria;
	std::map<int, Criteria> root_criteria;
};

static std::string criteriaToString(std::map<int, Criteria> const& criteria) {
	std::string s;
	for (auto& pair : criteria) {
		s += "{" + std::to_string(pair.first) + ":(" + printable(pair.second.criteria) + ", " +
		     boost::lexical_cast<std::string>((int)pair.second.match_type) + ", " +
		     boost::lexical_cast<std::string>((int)pair.second.location) + ")}";
	}
	return "{" + s + "}";
}

static void logf(const char* event, void* uid, bool is_error, ...) {}

int FDBLibTLSVerifyTest::run() {
	Reference<FDBLibTLSVerify> verify;
	try {
		verify = makeReference<FDBLibTLSVerify>(input);
	} catch (const std::runtime_error& e) {
		if (valid) {
			std::cerr << "FAIL: Verify test failed, but should have succeeded - '" << input << "'\n";
			return 1;
		}
		return 0;
	}
	if (!valid) {
		std::cerr << "FAIL: Verify test should have failed, but succeeded - '" << input << "'\n";
		return 1;
	}
	if (verify->verify_cert != verify_cert) {
		std::cerr << "FAIL: Got verify cert " << verify->verify_cert << ", want " << verify_cert << "\n";
		return 1;
	}
	if (verify->verify_time != verify_time) {
		std::cerr << "FAIL: Got verify time " << verify->verify_time << ", want " << verify_time << "\n";
		return 1;
	}
	if (verify->subject_criteria != subject_criteria) {
		std::cerr << "FAIL: Got subject criteria " << criteriaToString(verify->subject_criteria) << ", want "
		          << criteriaToString(subject_criteria) << "\n";
		return 1;
	}
	if (verify->issuer_criteria != issuer_criteria) {
		std::cerr << "FAIL: Got issuer criteria " << criteriaToString(verify->issuer_criteria) << ", want "
		          << criteriaToString(issuer_criteria) << "\n";
		return 1;
	}
	if (verify->root_criteria != root_criteria) {
		std::cerr << "FAIL: Got root criteria " << criteriaToString(verify->root_criteria) << ", want "
		          << criteriaToString(root_criteria) << "\n";
		return 1;
	}
	return 0;
}

static int policy_verify_test() {
	auto plugin = makeReference<FDBLibTLSPlugin>();
	auto policy = makeReference<FDBLibTLSPolicy>(plugin, (ITLSLogFunc)logf);

	const char* verify_peers[] = {
		"S.CN=abc",
		"I.CN=def",
		"R.CN=xyz,Check.Unexpired=0",
	};
	int verify_peers_len[] = {
		(int)strlen(verify_peers[0]),
		(int)strlen(verify_peers[1]),
		(int)strlen(verify_peers[2]),
	};
	Reference<FDBLibTLSVerify> verify_rules[] = {
		makeReference<FDBLibTLSVerify>(std::string(verify_peers[0], verify_peers_len[0])),
		makeReference<FDBLibTLSVerify>(std::string(verify_peers[1], verify_peers_len[1])),
		makeReference<FDBLibTLSVerify>(std::string(verify_peers[2], verify_peers_len[2])),
	};

	if (!policy->set_verify_peers(3, (const uint8_t**)verify_peers, verify_peers_len)) {
		std::cerr << "FAIL: Policy verify test failed, but should have succeeded\n";
		return 1;
	}
	if (policy->verify_rules.size() != 3) {
		std::cerr << "FAIL: Got " << policy->verify_rules.size() << " verify rule, want 3\n";
		return 1;
	}

	int i = 0;
	for (auto& verify_rule : policy->verify_rules) {
		if (verify_rule->verify_cert != verify_rules[i]->verify_cert) {
			std::cerr << "FAIL: Got verify cert " << verify_rule->verify_cert << ", want "
			          << verify_rules[i]->verify_cert << "\n";
			return 1;
		}
		if (verify_rule->verify_time != verify_rules[i]->verify_time) {
			std::cerr << "FAIL: Got verify time " << verify_rule->verify_time << ", want "
			          << verify_rules[i]->verify_time << "\n";
			return 1;
		}
		if (verify_rule->subject_criteria != verify_rules[i]->subject_criteria) {
			std::cerr << "FAIL: Got subject criteria " << criteriaToString(verify_rule->subject_criteria) << ", want "
			          << criteriaToString(verify_rules[i]->subject_criteria) << "\n";
			return 1;
		}
		if (verify_rule->issuer_criteria != verify_rules[i]->issuer_criteria) {
			std::cerr << "FAIL: Got issuer criteria " << criteriaToString(verify_rule->issuer_criteria) << ", want "
			          << criteriaToString(verify_rules[i]->issuer_criteria) << "\n";
			return 1;
		}
		if (verify_rule->root_criteria != verify_rules[i]->root_criteria) {
			std::cerr << "FAIL: Got root criteria " << criteriaToString(verify_rule->root_criteria) << ", want "
			          << criteriaToString(verify_rules[i]->root_criteria) << "\n";
			return 1;
		}
		i++;
	}
	return 0;
}

int main(int argc, char** argv) {
	int failed = 0;

#define EXACT(x) Criteria(x, MatchType::EXACT, X509Location::NAME)
#define PREFIX(x) Criteria(x, MatchType::PREFIX, X509Location::NAME)
#define SUFFIX(x) Criteria(x, MatchType::SUFFIX, X509Location::NAME)

	std::vector<FDBLibTLSVerifyTest> tests = {
		FDBLibTLSVerifyTest("", true, true, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Valid=1", true, true, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Valid=0", false, true, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=1", true, true, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0", true, false, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Valid=1,Check.Unexpired=0", true, false, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,Check.Valid=0", false, false, {}, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp\\, LLC",
		                    true,
		                    false,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp, LLC") } },
		                    { { NID_countryName, EXACT("US") } },
		                    {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp\\= LLC",
		                    true,
		                    false,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp= LLC") } },
		                    { { NID_countryName, EXACT("US") } },
		                    {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,R.C=US,C=US,S.O=XYZCorp\\= LLC",
		                    true,
		                    false,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp= LLC") } },
		                    {},
		                    { { NID_countryName, EXACT("US") } }),
		FDBLibTLSVerifyTest("Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp=LLC",
		                    true,
		                    false,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp=LLC") } },
		                    { { NID_countryName, EXACT("US") } },
		                    {}),
		FDBLibTLSVerifyTest("I.C=US,C=US,Check.Unexpired=0,S.O=XYZCorp=LLC",
		                    true,
		                    false,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp=LLC") } },
		                    { { NID_countryName, EXACT("US") } },
		                    {}),
		FDBLibTLSVerifyTest("I.C=US,C=US,S.O=XYZCorp\\, LLC",
		                    true,
		                    true,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp, LLC") } },
		                    { { NID_countryName, EXACT("US") } },
		                    {}),
		FDBLibTLSVerifyTest("I.C=US,C=US,S.O=XYZCorp\\, LLC,R.CN=abc",
		                    true,
		                    true,
		                    { { NID_countryName, EXACT("US") }, { NID_organizationName, EXACT("XYZCorp, LLC") } },
		                    { { NID_countryName, EXACT("US") } },
		                    { { NID_commonName, EXACT("abc") } }),
		FDBLibTLSVerifyTest("C=\\,S=abc", true, true, { { NID_countryName, EXACT(",S=abc") } }, {}, {}),
		FDBLibTLSVerifyTest("CN=\\61\\62\\63", true, true, { { NID_commonName, EXACT("abc") } }, {}, {}),
		FDBLibTLSVerifyTest("CN=a\\62c", true, true, { { NID_commonName, EXACT("abc") } }, {}, {}),
		FDBLibTLSVerifyTest("CN=a\\01c", true, true, { { NID_commonName, EXACT("a\001c") } }, {}, {}),
		FDBLibTLSVerifyTest("S.subjectAltName=XYZCorp",
		                    true,
		                    true,
		                    { { NID_subject_alt_name, { "XYZCorp", MatchType::EXACT, X509Location::EXTENSION } } },
		                    {},
		                    {}),
		FDBLibTLSVerifyTest("S.O>=XYZ", true, true, { { NID_organizationName, PREFIX("XYZ") } }, {}, {}),
		FDBLibTLSVerifyTest("S.O<=LLC", true, true, { { NID_organizationName, SUFFIX("LLC") } }, {}, {}),

		// Invalid cases.
		FDBLibTLSVerifyTest("Check.Invalid=0"),
		FDBLibTLSVerifyTest("Valid=1"),
		FDBLibTLSVerifyTest("C= US,S=abc"),
		FDBLibTLSVerifyTest("C=#US,S=abc"),
		FDBLibTLSVerifyTest("C=abc,S=\\"),
		FDBLibTLSVerifyTest("XYZ=abc"),
		FDBLibTLSVerifyTest("GN=abc"),
		FDBLibTLSVerifyTest("CN=abc,Check.Expired=1"),
	};

#undef EXACT
#undef PREFIX
#undef SUFFIX

	for (auto& test : tests)
		failed |= test.run();

	failed |= policy_verify_test();

	return (failed);
}
