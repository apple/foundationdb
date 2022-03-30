/*
 * TLSConfig.actor.cpp
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

#define PRIVATE_EXCEPT_FOR_TLSCONFIG_CPP
#include "flow/TLSConfig.actor.h"
#undef PRIVATE_EXCEPT_FOR_TLSCONFIG_CPP

// To force typeinfo to only be emitted once.
TLSPolicy::~TLSPolicy() {}

#ifdef TLS_DISABLED

void LoadedTLSConfig::print(FILE* fp) {
	fprintf(fp, "Cannot print LoadedTLSConfig.  TLS support is not enabled.\n");
}

#else // TLS is enabled

#include <algorithm>
#include <cstring>
#include <exception>
#include <map>
#include <set>
#include <openssl/objects.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/x509_vfy.h>
#include <stdint.h>
#include <string>
#include <sstream>
#include <utility>
#include <boost/asio/ssl/context.hpp>

// This include breaks module dependencies, but we need to do async file reads.
// So either we include fdbrpc here, or this file is moved to fdbrpc/, and then
// Net2, which depends on us, includes fdbrpc/.
//
// Either way, the only way to break this dependency cycle is to move all of
// AsyncFile to flow/
#include "fdbrpc/IAsyncFile.h"
#include "flow/Platform.h"

#include "flow/FastRef.h"
#include "flow/Trace.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

std::vector<std::string> LoadedTLSConfig::getVerifyPeers() const {
	if (tlsVerifyPeers.size()) {
		return tlsVerifyPeers;
	}

	std::string envVerifyPeers;
	if (platform::getEnvironmentVar("FDB_TLS_VERIFY_PEERS", envVerifyPeers)) {
		return { envVerifyPeers };
	}

	return { "Check.Valid=1" };
}

std::string LoadedTLSConfig::getPassword() const {
	if (tlsPassword.size()) {
		return tlsPassword;
	}

	std::string envPassword;
	platform::getEnvironmentVar("FDB_TLS_PASSWORD", envPassword);
	return envPassword;
}

void LoadedTLSConfig::print(FILE* fp) {
	int num_certs = 0;
	boost::asio::ssl::context context(boost::asio::ssl::context::tls);
	try {
		ConfigureSSLContext(*this, &context);
	} catch (Error& e) {
		fprintf(fp, "There was an error in loading the certificate chain.\n");
		throw;
	}

	X509_STORE* store = SSL_CTX_get_cert_store(context.native_handle());
	X509_STORE_CTX* store_ctx = X509_STORE_CTX_new();
	X509* cert = SSL_CTX_get0_certificate(context.native_handle());
	X509_STORE_CTX_init(store_ctx, store, cert, nullptr);

	X509_verify_cert(store_ctx);
	STACK_OF(X509)* chain = X509_STORE_CTX_get0_chain(store_ctx);

	X509_print_fp(fp, cert);

	num_certs = sk_X509_num(chain);
	if (num_certs) {
		for (int i = 0; i < num_certs; i++) {
			printf("\n");
			X509* cert = sk_X509_value(chain, i);
			X509_print_fp(fp, cert);
		}
	}

	X509_STORE_CTX_free(store_ctx);
}

void ConfigureSSLContext(const LoadedTLSConfig& loaded,
                         boost::asio::ssl::context* context,
                         std::function<void()> onPolicyFailure) {
	try {
		context->set_options(boost::asio::ssl::context::default_workarounds);
		context->set_verify_mode(boost::asio::ssl::context::verify_peer |
		                         boost::asio::ssl::verify_fail_if_no_peer_cert);

		if (loaded.isTLSEnabled()) {
			auto tlsPolicy = makeReference<TLSPolicy>(loaded.getEndpointType());
			tlsPolicy->set_verify_peers({ loaded.getVerifyPeers() });

			context->set_verify_callback(
			    [policy = tlsPolicy, onPolicyFailure](bool preverified, boost::asio::ssl::verify_context& ctx) {
				    bool success = policy->verify_peer(preverified, ctx.native_handle());
				    if (!success) {
					    onPolicyFailure();
				    }
				    return success;
			    });
		} else {
			// Insecurely always except if TLS is not enabled.
			context->set_verify_callback([](bool, boost::asio::ssl::verify_context&) { return true; });
		}

		context->set_password_callback([password = loaded.getPassword()](
		                                   size_t, boost::asio::ssl::context::password_purpose) { return password; });

		const std::string& CABytes = loaded.getCABytes();
		if (CABytes.size()) {
			context->add_certificate_authority(boost::asio::buffer(CABytes.data(), CABytes.size()));
		}

		const std::string& keyBytes = loaded.getKeyBytes();
		if (keyBytes.size()) {
			context->use_private_key(boost::asio::buffer(keyBytes.data(), keyBytes.size()),
			                         boost::asio::ssl::context::pem);
		}

		const std::string& certBytes = loaded.getCertificateBytes();
		if (certBytes.size()) {
			context->use_certificate_chain(boost::asio::buffer(certBytes.data(), certBytes.size()));
		}
	} catch (boost::system::system_error& e) {
		TraceEvent("TLSConfigureError")
		    .detail("What", e.what())
		    .detail("Value", e.code().value())
		    .detail("WhichMeans", TLSPolicy::ErrorString(e.code()));
		throw tls_error();
	}
}

std::string TLSConfig::getCertificatePathSync() const {
	if (tlsCertPath.size()) {
		return tlsCertPath;
	}

	std::string envCertPath;
	if (platform::getEnvironmentVar("FDB_TLS_CERTIFICATE_FILE", envCertPath)) {
		return envCertPath;
	}

	const char* defaultCertFileName = "fdb.pem";
	if (fileExists(defaultCertFileName)) {
		return defaultCertFileName;
	}

	if (fileExists(joinPath(platform::getDefaultConfigPath(), defaultCertFileName))) {
		return joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
	}

	return std::string();
}

std::string TLSConfig::getKeyPathSync() const {
	if (tlsKeyPath.size()) {
		return tlsKeyPath;
	}

	std::string envKeyPath;
	if (platform::getEnvironmentVar("FDB_TLS_KEY_FILE", envKeyPath)) {
		return envKeyPath;
	}

	const char* defaultCertFileName = "fdb.pem";
	if (fileExists(defaultCertFileName)) {
		return defaultCertFileName;
	}

	if (fileExists(joinPath(platform::getDefaultConfigPath(), defaultCertFileName))) {
		return joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
	}

	return std::string();
}

std::string TLSConfig::getCAPathSync() const {
	if (tlsCAPath.size()) {
		return tlsCAPath;
	}

	std::string envCAPath;
	platform::getEnvironmentVar("FDB_TLS_CA_FILE", envCAPath);
	return envCAPath;
}

LoadedTLSConfig TLSConfig::loadSync() const {
	LoadedTLSConfig loaded;

	const std::string certPath = getCertificatePathSync();
	if (certPath.size()) {
		try {
			loaded.tlsCertBytes = readFileBytes(certPath, FLOW_KNOBS->CERT_FILE_MAX_SIZE);
		} catch (Error& e) {
			fprintf(stderr, "Error reading TLS Certificate [%s]: %s\n", certPath.c_str(), e.what());
			throw;
		}
	} else {
		loaded.tlsCertBytes = tlsCertBytes;
	}

	const std::string keyPath = getKeyPathSync();
	if (keyPath.size()) {
		try {
			loaded.tlsKeyBytes = readFileBytes(keyPath, FLOW_KNOBS->CERT_FILE_MAX_SIZE);
		} catch (Error& e) {
			fprintf(stderr, "Error reading TLS Key [%s]: %s\n", keyPath.c_str(), e.what());
			throw;
		}
	} else {
		loaded.tlsKeyBytes = tlsKeyBytes;
	}

	const std::string CAPath = getCAPathSync();
	if (CAPath.size()) {
		try {
			loaded.tlsCABytes = readFileBytes(CAPath, FLOW_KNOBS->CERT_FILE_MAX_SIZE);
		} catch (Error& e) {
			fprintf(stderr, "Error reading TLS CA [%s]: %s\n", CAPath.c_str(), e.what());
			throw;
		}
	} else {
		loaded.tlsCABytes = tlsCABytes;
	}

	loaded.tlsPassword = tlsPassword;
	loaded.tlsVerifyPeers = tlsVerifyPeers;
	loaded.endpointType = endpointType;

	return loaded;
}

// And now do the same thing, but async...

ACTOR static Future<Void> readEntireFile(std::string filename, std::string* destination) {
	state Reference<IAsyncFile> file =
	    wait(IAsyncFileSystem::filesystem()->open(filename, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0));
	state int64_t filesize = wait(file->size());
	if (filesize > FLOW_KNOBS->CERT_FILE_MAX_SIZE) {
		throw file_too_large();
	}
	destination->resize(filesize);
	wait(success(file->read(&((*destination)[0]), filesize, 0)));
	return Void();
}

ACTOR Future<LoadedTLSConfig> TLSConfig::loadAsync(const TLSConfig* self) {
	state LoadedTLSConfig loaded;
	state std::vector<Future<Void>> reads;

	state int32_t certIdx = -1;
	state int32_t keyIdx = -1;
	state int32_t caIdx = -1;

	state std::string certPath = self->getCertificatePathSync();
	if (certPath.size()) {
		reads.push_back(readEntireFile(certPath, &loaded.tlsCertBytes));
		certIdx = reads.size() - 1;
	} else {
		loaded.tlsCertBytes = self->tlsCertBytes;
	}

	state std::string keyPath = self->getKeyPathSync();
	if (keyPath.size()) {
		reads.push_back(readEntireFile(keyPath, &loaded.tlsKeyBytes));
		keyIdx = reads.size() - 1;
	} else {
		loaded.tlsKeyBytes = self->tlsKeyBytes;
	}

	state std::string CAPath = self->getCAPathSync();
	if (CAPath.size()) {
		reads.push_back(readEntireFile(CAPath, &loaded.tlsCABytes));
		caIdx = reads.size() - 1;
	} else {
		loaded.tlsCABytes = self->tlsCABytes;
	}

	try {
		wait(waitForAll(reads));
	} catch (Error& e) {
		if (certIdx != -1 && reads[certIdx].isError()) {
			fprintf(stderr, "Failure reading TLS Certificate [%s]: %s\n", certPath.c_str(), e.what());
		} else if (keyIdx != -1 && reads[keyIdx].isError()) {
			fprintf(stderr, "Failure reading TLS Key [%s]: %s\n", keyPath.c_str(), e.what());
		} else if (caIdx != -1 && reads[caIdx].isError()) {
			fprintf(stderr, "Failure reading TLS Key [%s]: %s\n", CAPath.c_str(), e.what());
		} else {
			fprintf(stderr, "Failure reading TLS needed file: %s\n", e.what());
		}

		throw;
	}

	loaded.tlsPassword = self->tlsPassword;
	loaded.tlsVerifyPeers = self->tlsVerifyPeers;
	loaded.endpointType = self->endpointType;

	return loaded;
}

std::string TLSPolicy::ErrorString(boost::system::error_code e) {
	char* str = ERR_error_string(e.value(), nullptr);
	return std::string(str);
}

std::string TLSPolicy::toString() const {
	std::stringstream ss;
	ss << "TLSPolicy{ Rules=[";
	for (const auto& r : rules) {
		ss << " " << r.toString() << ",";
	}
	ss << " ] }";
	return ss.str();
}

std::string TLSPolicy::Rule::toString() const {
	std::stringstream ss;

	ss << "Rule{ verify_cert=" << verify_cert << ", verify_time=" << verify_time;
	ss << ", Subject=[";
	for (const auto& s : subject_criteria) {
		ss << " { NID=" << s.first << ", Criteria=" << s.second.criteria << "},";
	}
	ss << " ], Issuer=[";
	for (const auto& s : issuer_criteria) {
		ss << " { NID=" << s.first << ", Criteria=" << s.second.criteria << "},";
	}
	ss << " ], Root=[";
	for (const auto& s : root_criteria) {
		ss << " { NID=" << s.first << ", Criteria=" << s.second.criteria << "},";
	}
	ss << " ] }";

	return ss.str();
}

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

void TLSPolicy::set_verify_peers(std::vector<std::string> verify_peers) {
	for (int i = 0; i < verify_peers.size(); i++) {
		try {
			std::string& verifyString = verify_peers[i];
			int start = 0;
			while (start < verifyString.size()) {
				int split = verifyString.find('|', start);
				if (split == std::string::npos) {
					break;
				}
				if (split == start || verifyString[split - 1] != '\\') {
					rules.emplace_back(verifyString.substr(start, split - start));
					start = split + 1;
				}
			}
			rules.emplace_back(verifyString.substr(start));
		} catch (const std::runtime_error& e) {
			rules.clear();
			std::string& verifyString = verify_peers[i];
			TraceEvent(SevError, "FDBLibTLSVerifyPeersParseError").detail("Config", verifyString);
			throw tls_error();
		}
	}
}

TLSPolicy::Rule::Rule(std::string input) {
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

bool match_criteria_entry(const std::string& criteria, ASN1_STRING* entry, MatchType mt) {
	bool rc = false;
	ASN1_STRING* asn_criteria = nullptr;
	unsigned char* criteria_utf8 = nullptr;
	int criteria_utf8_len = 0;
	unsigned char* entry_utf8 = nullptr;
	int entry_utf8_len = 0;

	if ((asn_criteria = ASN1_IA5STRING_new()) == nullptr)
		goto err;
	if (ASN1_STRING_set(asn_criteria, criteria.c_str(), criteria.size()) != 1)
		goto err;
	if ((criteria_utf8_len = ASN1_STRING_to_UTF8(&criteria_utf8, asn_criteria)) < 1)
		goto err;
	if ((entry_utf8_len = ASN1_STRING_to_UTF8(&entry_utf8, entry)) < 1)
		goto err;
	if (mt == MatchType::EXACT) {
		if (criteria_utf8_len == entry_utf8_len && memcmp(criteria_utf8, entry_utf8, criteria_utf8_len) == 0)
			rc = true;
	} else if (mt == MatchType::PREFIX) {
		if (criteria_utf8_len <= entry_utf8_len && memcmp(criteria_utf8, entry_utf8, criteria_utf8_len) == 0)
			rc = true;
	} else if (mt == MatchType::SUFFIX) {
		if (criteria_utf8_len <= entry_utf8_len &&
		    memcmp(criteria_utf8, entry_utf8 + (entry_utf8_len - criteria_utf8_len), criteria_utf8_len) == 0)
			rc = true;
	}

err:
	ASN1_STRING_free(asn_criteria);
	free(criteria_utf8);
	free(entry_utf8);
	return rc;
}

bool match_name_criteria(X509_NAME* name, NID nid, const std::string& criteria, MatchType mt) {
	X509_NAME_ENTRY* name_entry;
	int idx;

	// If name does not exist, or has multiple of this RDN, refuse to proceed.
	if ((idx = X509_NAME_get_index_by_NID(name, nid, -1)) < 0)
		return false;
	if (X509_NAME_get_index_by_NID(name, nid, idx) != -1)
		return false;
	if ((name_entry = X509_NAME_get_entry(name, idx)) == nullptr)
		return false;

	return match_criteria_entry(criteria, X509_NAME_ENTRY_get_data(name_entry), mt);
}

bool match_extension_criteria(X509* cert, NID nid, const std::string& value, MatchType mt) {
	if (nid != NID_subject_alt_name && nid != NID_issuer_alt_name) {
		// I have no idea how other extensions work.
		return false;
	}
	auto pos = value.find(':');
	if (pos == value.npos) {
		return false;
	}
	std::string value_gen = value.substr(0, pos);
	std::string value_val = value.substr(pos + 1, value.npos);
	STACK_OF(GENERAL_NAME)* sans =
	    reinterpret_cast<STACK_OF(GENERAL_NAME)*>(X509_get_ext_d2i(cert, nid, nullptr, nullptr));
	if (sans == nullptr) {
		return false;
	}
	int num_sans = sk_GENERAL_NAME_num(sans);
	bool rc = false;
	for (int i = 0; i < num_sans && !rc; ++i) {
		GENERAL_NAME* altname = sk_GENERAL_NAME_value(sans, i);
		std::string matchable;
		switch (altname->type) {
		case GEN_OTHERNAME:
			break;
		case GEN_EMAIL:
			if (value_gen == "EMAIL" && match_criteria_entry(value_val, altname->d.rfc822Name, mt)) {
				rc = true;
				break;
			}
		case GEN_DNS:
			if (value_gen == "DNS" && match_criteria_entry(value_val, altname->d.dNSName, mt)) {
				rc = true;
				break;
			}
		case GEN_X400:
		case GEN_DIRNAME:
		case GEN_EDIPARTY:
			break;
		case GEN_URI:
			if (value_gen == "URI" && match_criteria_entry(value_val, altname->d.uniformResourceIdentifier, mt)) {
				rc = true;
				break;
			}
		case GEN_IPADD:
			if (value_gen == "IP" && match_criteria_entry(value_val, altname->d.iPAddress, mt)) {
				rc = true;
				break;
			}
		case GEN_RID:
			break;
		}
	}
	sk_GENERAL_NAME_pop_free(sans, GENERAL_NAME_free);
	return rc;
}

bool match_criteria(X509* cert,
                    X509_NAME* subject,
                    NID nid,
                    const std::string& criteria,
                    MatchType mt,
                    X509Location loc) {
	switch (loc) {
	case X509Location::NAME: {
		return match_name_criteria(subject, nid, criteria, mt);
	}
	case X509Location::EXTENSION: {
		return match_extension_criteria(cert, nid, criteria, mt);
	}
	}
	// Should never be reachable.
	return false;
}

std::tuple<bool, std::string> check_verify(const TLSPolicy::Rule* verify, X509_STORE_CTX* store_ctx, bool is_client) {
	X509_NAME *subject, *issuer;
	bool rc = false;
	X509* cert = nullptr;
	// if returning false, give a reason string
	std::string reason = "";

	// Check subject criteria.
	cert = sk_X509_value(X509_STORE_CTX_get0_chain(store_ctx), 0);
	if ((subject = X509_get_subject_name(cert)) == nullptr) {
		reason = "Cert subject error";
		goto err;
	}
	for (auto& pair : verify->subject_criteria) {
		if (!match_criteria(
		        cert, subject, pair.first, pair.second.criteria, pair.second.match_type, pair.second.location)) {
			reason = "Cert subject match failure";
			goto err;
		}
	}

	// Check issuer criteria.
	if ((issuer = X509_get_issuer_name(cert)) == nullptr) {
		reason = "Cert issuer error";
		goto err;
	}
	for (auto& pair : verify->issuer_criteria) {
		if (!match_criteria(
		        cert, issuer, pair.first, pair.second.criteria, pair.second.match_type, pair.second.location)) {
			reason = "Cert issuer match failure";
			goto err;
		}
	}

	// Check root criteria - this is the subject of the final certificate in the stack.
	cert = sk_X509_value(X509_STORE_CTX_get0_chain(store_ctx), sk_X509_num(X509_STORE_CTX_get0_chain(store_ctx)) - 1);
	if ((subject = X509_get_subject_name(cert)) == nullptr) {
		reason = "Root subject error";
		goto err;
	}
	for (auto& pair : verify->root_criteria) {
		if (!match_criteria(
		        cert, subject, pair.first, pair.second.criteria, pair.second.match_type, pair.second.location)) {
			reason = "Root subject match failure";
			goto err;
		}
	}

	// If we got this far, everything checked out...
	rc = true;

err:
	return std::make_tuple(rc, reason);
}

bool TLSPolicy::verify_peer(bool preverified, X509_STORE_CTX* store_ctx) {
	bool rc = false;
	std::set<std::string> verify_failure_reasons;
	bool verify_success;
	std::string verify_failure_reason;

	// If certificate verification is disabled, there's nothing more to do.
	if (std::any_of(rules.begin(), rules.end(), [](const Rule& r) { return !r.verify_cert; })) {
		return true;
	}

	if (!preverified) {
		TraceEvent(SevWarn, "TLSPolicyFailure")
		    .suppressFor(1.0)
		    .detail("Reason", "preverification failed")
		    .detail("VerifyError", X509_verify_cert_error_string(X509_STORE_CTX_get_error(store_ctx)));
		return false;
	}

	if (!rules.size()) {
		return true;
	}

	// Any matching rule is sufficient.
	for (auto& verify_rule : rules) {
		std::tie(verify_success, verify_failure_reason) = check_verify(&verify_rule, store_ctx, is_client);
		if (verify_success) {
			rc = true;
			break;
		} else {
			if (verify_failure_reason.length() > 0)
				verify_failure_reasons.insert(verify_failure_reason);
		}
	}

	if (!rc) {
		// log the various failure reasons
		for (std::string reason : verify_failure_reasons) {
			TraceEvent(SevWarn, "TLSPolicyFailure").suppressFor(1.0).detail("Reason", reason);
		}
	}
	return rc;
}
#endif
