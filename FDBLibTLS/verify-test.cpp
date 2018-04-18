#include <iostream>
#include <string>
#include <vector>

#include <openssl/obj_mac.h>

#include "ITLSPlugin.h"
#include "ReferenceCounted.h"

#include "FDBLibTLSPlugin.h"
#include "FDBLibTLSPolicy.h"

struct FDBLibTLSVerifyTest {
	FDBLibTLSVerifyTest(std::string input):
		input(input), valid(false), verify_cert(true), verify_time(true), subject_criteria({}), issuer_criteria({}) {};
        FDBLibTLSVerifyTest(std::string input, bool verify_cert, bool verify_time, std::map<int, std::string> subject, std::map<int, std::string> issuer):
		input(input), valid(true), verify_cert(verify_cert), verify_time(verify_time), subject_criteria(subject), issuer_criteria(issuer) {};
        ~FDBLibTLSVerifyTest() {};

	int run();

	std::string input;

	bool valid;
	bool verify_cert;
	bool verify_time;

	std::map<int, std::string> subject_criteria;
	std::map<int, std::string> issuer_criteria;
};

static std::string printable( std::string const& val ) {
	static char const digits[] = "0123456789ABCDEF";
	std::string s;

	for ( int i = 0; i < val.size(); i++ ) {
		uint8_t b = val[i];
		if (b >= 32 && b < 127 && b != '\\')
			s += (char)b;
		else if (b == '\\')
			s += "\\\\";
		else {
			s += "\\x";
			s += digits[(b >> 4) & 15];
			s += digits[b & 15];
		}
	}
	return s;
}

static std::string criteriaToString(std::map<int, std::string> const& criteria) {
	std::string s;
	for (auto &pair: criteria) {
		s += "{" + std::to_string(pair.first) + ":" + printable(pair.second) + "}";
	}
	return "{" + s + "}";
}

static void logf(const char* event, void* uid, int is_error, ...) {
}

int FDBLibTLSVerifyTest::run() {
	FDBLibTLSPlugin *plugin = new FDBLibTLSPlugin();
	FDBLibTLSPolicy *policy = new FDBLibTLSPolicy(Reference<FDBLibTLSPlugin>::addRef(plugin), (ITLSLogFunc)logf);

	bool rc = policy->set_verify_peers((const uint8_t *)input.c_str(), input.size());
	if (rc != valid) {
		if (valid) {
			std::cerr << "FAIL: Verify test failed, but should have succeeded - '" << input << "'\n";
			return 1;
		} else {
			std::cerr << "FAIL: Verify test should have failed, but succeeded - '" << input << "'\n";
			return 1;
		}
	}
	if (policy->verify_cert != verify_cert) {
		std::cerr << "FAIL: Got verify cert " << policy->verify_cert << ", want " << verify_cert << "\n";
		return 1;
	}
	if (policy->verify_time != verify_time) {
		std::cerr << "FAIL: Got verify time " << policy->verify_time << ", want " << verify_time << "\n";
		return 1;
	}
	if (policy->subject_criteria != subject_criteria) {
		std::cerr << "FAIL: Got subject criteria " << criteriaToString(policy->subject_criteria) << ", want " << criteriaToString(subject_criteria) << "\n";
		return 1;
	}
	if (policy->issuer_criteria != issuer_criteria) {
		std::cerr << "FAIL: Got issuer criteria " << criteriaToString(policy->issuer_criteria) << ", want " << criteriaToString(issuer_criteria) << "\n";
		return 1;
	}
	return 0;
}

int main(int argc, char **argv)
{
	int failed = 0;

	std::vector<FDBLibTLSVerifyTest> tests = {
		FDBLibTLSVerifyTest("", true, true, {}, {}),
		FDBLibTLSVerifyTest("Check.Valid=1", true, true, {}, {}),
		FDBLibTLSVerifyTest("Check.Valid=0", false, true, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=1", true, true, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0", true, false, {}, {}),
		FDBLibTLSVerifyTest("Check.Valid=1,Check.Unexpired=0", true, false, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,Check.Valid=0", false, false, {}, {}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp\\, LLC", true, false,
			{{NID_countryName, "US"}, {NID_organizationName, "XYZCorp, LLC"}}, {{NID_countryName, "US"}}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp\\= LLC", true, false,
			{{NID_countryName, "US"}, {NID_organizationName, "XYZCorp= LLC"}}, {{NID_countryName, "US"}}),
		FDBLibTLSVerifyTest("Check.Unexpired=0,I.C=US,C=US,S.O=XYZCorp=LLC", true, false,
			{{NID_countryName, "US"}, {NID_organizationName, "XYZCorp=LLC"}}, {{NID_countryName, "US"}}),
		FDBLibTLSVerifyTest("I.C=US,C=US,Check.Unexpired=0,S.O=XYZCorp=LLC", true, false,
			{{NID_countryName, "US"}, {NID_organizationName, "XYZCorp=LLC"}}, {{NID_countryName, "US"}}),
		FDBLibTLSVerifyTest("I.C=US,C=US,S.O=XYZCorp\\, LLC", true, true,
			{{NID_countryName, "US"}, {NID_organizationName, "XYZCorp, LLC"}}, {{NID_countryName, "US"}}),
		FDBLibTLSVerifyTest("C=\\,S=abc", true, true, {{NID_countryName, ",S=abc"}}, {}),
		FDBLibTLSVerifyTest("CN=\\61\\62\\63", true, true, {{NID_commonName, "abc"}}, {}),
		FDBLibTLSVerifyTest("CN=a\\62c", true, true, {{NID_commonName, "abc"}}, {}),
		FDBLibTLSVerifyTest("CN=a\\01c", true, true, {{NID_commonName, "a\001c"}}, {}),

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

	for (auto &test: tests)
		failed |= test.run();

	return (failed);
}
