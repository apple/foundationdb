/*
 * TLSConfig.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include <algorithm>
#include <cstring>
#include <exception>
#include <map>
#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <string_view>
#include <sstream>
#include <utility>
#include <vector>
#include <filesystem>

#include <boost/asio/ssl/context.hpp>
#include <boost/lexical_cast.hpp>

#include <openssl/crypto.h>
#include <openssl/objects.h>
#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/x509_vfy.h>

#include <fmt/core.h>

#include "flow/Platform.h"
#include "flow/IAsyncFile.h"
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

bool LoadedTLSConfig::getDisablePlainTextConnection() const {
	return tlsDisablePlainTextConnection;
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
		ConfigureSSLContext(*this, context);
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

void ConfigureSSLContext(const LoadedTLSConfig& loaded, boost::asio::ssl::context& context) {
	try {
		context.set_options(boost::asio::ssl::context::default_workarounds);
		auto verifyFailIfNoPeerCert = boost::asio::ssl::verify_fail_if_no_peer_cert;
		// Servers get to accept connections without peer certs as "untrusted" clients
		if (loaded.getEndpointType() == TLSEndpointType::SERVER)
			verifyFailIfNoPeerCert = 0;
		context.set_verify_mode(boost::asio::ssl::context::verify_peer | verifyFailIfNoPeerCert);

		context.set_password_callback([password = loaded.getPassword()](
		                                  size_t, boost::asio::ssl::context::password_purpose) { return password; });

		const std::string& CABytes = loaded.getCABytes();
		if (CABytes.size()) {
			context.add_certificate_authority(boost::asio::buffer(CABytes.data(), CABytes.size()));
		}

		const std::string& keyBytes = loaded.getKeyBytes();
		if (keyBytes.size()) {
			context.use_private_key(boost::asio::buffer(keyBytes.data(), keyBytes.size()),
			                        boost::asio::ssl::context::pem);
		}

		const std::string& certBytes = loaded.getCertificateBytes();
		if (certBytes.size()) {
			context.use_certificate_chain(boost::asio::buffer(certBytes.data(), certBytes.size()));
		}
	} catch (boost::system::system_error& e) {
		TraceEvent("TLSContextConfigureError")
		    .detail("What", e.what())
		    .detail("Value", e.code().value())
		    .detail("WhichMeans", TLSPolicy::ErrorString(e.code()));
		throw tls_error();
	}
}

void ConfigureSSLStream(Reference<TLSPolicy> policy,
                        boost::asio::ssl::stream<boost::asio::ip::tcp::socket&>& stream,
                        const NetworkAddress& peerAddress,
                        std::function<void(bool)> callback) {
	try {
		stream.set_verify_callback(
		    [policy, peerAddress, callback](bool preverified, boost::asio::ssl::verify_context& ctx) {
			    bool success = policy->verify_peer(preverified, ctx.native_handle(), peerAddress);
			    if (!success) {
				    if (policy->on_failure)
					    policy->on_failure();
			    }
			    if (callback) {
				    callback(success);
			    }
			    return success;
		    });
	} catch (boost::system::system_error& e) {
		TraceEvent("TLSStreamConfigureError")
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

	const char* defaultCertFileName = "cert.pem";
	if (fileExists(platform::getDefaultConfigPath() / defaultCertFileName)) {
		return platform::getDefaultConfigPath() / defaultCertFileName;
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

	const char* defaultKeyFileName = "key.pem";
	if (fileExists(platform::getDefaultConfigPath() / defaultKeyFileName)) {
		return platform::getDefaultConfigPath() / defaultKeyFileName;
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

bool TLSConfig::getDisablePlainTextConnection() const {
	std::string envDisablePlainTextConnection;
	if (platform::getEnvironmentVar("FDB_TLS_DISABLE_PLAINTEXT_CONNECTION", envDisablePlainTextConnection)) {
		try {
			return boost::lexical_cast<bool>(envDisablePlainTextConnection);
		} catch (boost::bad_lexical_cast& e) {
			fprintf(stderr,
			        "Warning: Ignoring invalid FDB_TLS_DISABLE_PLAINTEXT_CONNECTION [%s]: %s\n",
			        envDisablePlainTextConnection.c_str(),
			        e.what());
		}
	}

	return tlsDisablePlainTextConnection;
}

LoadedTLSConfig TLSConfig::loadSync() const {
	LoadedTLSConfig loaded;

	const std::string certPath = getCertificatePathSync();
	if (certPath.size()) {
		try {
			loaded.tlsCertBytes = readFileBytes(certPath, FLOW_KNOBS->CERT_FILE_MAX_SIZE);
		} catch (Error& e) {
			fprintf(stderr, "Warning: Error reading TLS Certificate [%s]: %s\n", certPath.c_str(), e.what());
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
			fprintf(stderr, "Warning: Error reading TLS Key [%s]: %s\n", keyPath.c_str(), e.what());
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
			fprintf(stderr, "Warning: Error reading TLS CA [%s]: %s\n", CAPath.c_str(), e.what());
			throw;
		}
	} else {
		loaded.tlsCABytes = tlsCABytes;
	}

	loaded.tlsPassword = tlsPassword;
	loaded.tlsVerifyPeers = tlsVerifyPeers;
	loaded.endpointType = endpointType;
	loaded.tlsDisablePlainTextConnection = tlsDisablePlainTextConnection;

	return loaded;
}

TLSPolicy::TLSPolicy(const LoadedTLSConfig& loaded, std::function<void()> on_failure)
  : rules(), on_failure(std::move(on_failure)), is_client(loaded.getEndpointType() == TLSEndpointType::CLIENT) {
	set_verify_peers(loaded.getVerifyPeers());
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
			fprintf(stderr, "Warning: Error reading TLS Certificate [%s]: %s\n", certPath.c_str(), e.what());
		} else if (keyIdx != -1 && reads[keyIdx].isError()) {
			fprintf(stderr, "Warning: Error reading TLS Key [%s]: %s\n", keyPath.c_str(), e.what());
		} else if (caIdx != -1 && reads[caIdx].isError()) {
			fprintf(stderr, "Warning: Error reading TLS Key [%s]: %s\n", CAPath.c_str(), e.what());
		} else {
			fprintf(stderr, "Warning: Error reading TLS needed file: %s\n", e.what());
		}

		throw;
	}

	loaded.tlsPassword = self->tlsPassword;
	loaded.tlsVerifyPeers = self->tlsVerifyPeers;
	loaded.endpointType = self->endpointType;
	loaded.tlsDisablePlainTextConnection = self->tlsDisablePlainTextConnection;

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

namespace {

// Free an object that allocated by OpenSSL.
// Since in openssl/crypto.h the OPENSSL_free function is actuall a macro expanded
// to CRYPTO_free, to enable RAII on OpenSSL allocated resources, it is necessary
// to wrap the CRYPTO_free;
inline void OPENSSL_free_impl(void* ptr) {
	CRYPTO_free(ptr, OPENSSL_FILE, OPENSSL_LINE);
}
// Free an object of GENERAL_NAME
inline void GENERAL_NAME_free_impl(struct stack_st_GENERAL_NAME* ptr) {
	sk_GENERAL_NAME_pop_free(ptr, GENERAL_NAME_free);
}

bool match_criteria_entry(const std::string_view criteria, const ASN1_STRING* entry, const MatchType match_type) {
	// Well, ScopeExit.h:ScopeExit should also work but unique_ptr is easier
	std::unique_ptr<ASN1_STRING, decltype(&ASN1_STRING_free)> asn_criteria(ASN1_IA5STRING_new(), ASN1_STRING_free);
	if (!asn_criteria) {
		return false;
	}
	if (ASN1_STRING_set(asn_criteria.get(), &criteria[0], criteria.size()) != 1) {
		return false;
	}

	unsigned char* criteria_utf8_ptr;
	int criteria_utf8_len = ASN1_STRING_to_UTF8(&criteria_utf8_ptr, asn_criteria.get());
	if (criteria_utf8_len < 1) {
		return false;
	}
	std::unique_ptr<unsigned char, decltype(&OPENSSL_free_impl)> criteria_utf8(criteria_utf8_ptr, OPENSSL_free_impl);

	unsigned char* entry_utf8_ptr = nullptr;
	int entry_utf8_len = ASN1_STRING_to_UTF8(&entry_utf8_ptr, entry);
	if (entry_utf8_len < 1) {
		return false;
	}
	std::unique_ptr<unsigned char, decltype(&OPENSSL_free_impl)> entry_utf8(entry_utf8_ptr, OPENSSL_free_impl);

	switch (match_type) {
	case MatchType::EXACT:
		return (criteria_utf8_len == entry_utf8_len &&
		        memcmp(criteria_utf8.get(), entry_utf8.get(), criteria_utf8_len) == 0);
	case MatchType::PREFIX:
		return (criteria_utf8_len <= entry_utf8_len &&
		        memcmp(criteria_utf8.get(), entry_utf8.get(), criteria_utf8_len) == 0);
	case MatchType::SUFFIX:
		return (criteria_utf8_len <= entry_utf8_len && memcmp(criteria_utf8.get(),
		                                                      entry_utf8.get() + (entry_utf8_len - criteria_utf8_len),
		                                                      criteria_utf8_len) == 0);
	default:
		UNREACHABLE();
	}
}

class PeerVerifier {
	const std::vector<TLSPolicy::Rule>& m_rules;
	X509_STORE_CTX* m_storeCtx;
	bool m_verified;

	std::string_view m_successReason;

	std::vector<std::string_view> m_verifyState;
	std::string m_currentName;
	std::string m_currentExtension;
	std::vector<std::string> m_failureReasons;

	bool matchNameCriteria(X509_NAME* name, const NID nid, std::string_view criteria, const MatchType matchType);

	bool matchExtensionCriteria(const X509* cert, const NID nid, std::string_view criteria, const MatchType matchType);

	bool matchCriteria(const X509* cert, X509_NAME* name, const NID nid, const Criteria& criteria);

	// Verify a set of criteria
	bool verifyCriterias(const X509* cert, X509_NAME* name, const TLSPolicy::Rule::CriteriaMap& criterias);

	// Verify a single rule
	bool verifyRule(const TLSPolicy::Rule&);

	// Verify the TLSPolicy peer
	bool verify();

public:
	PeerVerifier(const std::vector<TLSPolicy::Rule>& rules, X509_STORE_CTX* store_ctx)
	  : m_rules(rules), m_storeCtx(store_ctx), m_successReason(), m_verifyState() {
		ASSERT(m_storeCtx != nullptr);

		// Prealloc 32 * sizeof(const char*) to avoid reallocation, this should be sufficient
		m_verifyState.reserve(32);

		m_verified = verify();
	}

	bool isOk() const noexcept { return m_verified; }
	bool isErr() const noexcept { return !isOk(); }
	const std::vector<std::string>& getFailureReasons() const noexcept { return m_failureReasons; }
	const std::string_view getSuccessReason() const noexcept { return m_successReason; }
};

std::string getX509Name(const X509_NAME* name) {
	std::unique_ptr<BIO, decltype(&BIO_free)> out(BIO_new(BIO_s_mem()), BIO_free);
	if (out == nullptr) {
		throw internal_error_msg("Unable to allocate OpenSSL BIO");
	}
	X509_NAME_print_ex(out.get(), name, /* indent= */ 0, /* flags */ XN_FLAG_ONELINE);
	unsigned char* rawName = nullptr;
	long length = BIO_get_mem_data(out.get(), &rawName);
	ASSERT(length > 0);
	std::string result((const char*)rawName, length);
	return result;
}

// From v3_genn.c, GENERAL_NAME->type is int
using GeneralNameType = int;

using namespace std::literals::string_view_literals;

const std::unordered_map<GeneralNameType, std::string_view> UNSUPPORTED_GENERAL_NAME_TYPES = {
	{ GEN_OTHERNAME, "GEN_OTHERNAME"sv },
	{ GEN_X400, "GEN_X400"sv },
	{ GEN_DIRNAME, "GEN_DIRNAME"sv },
	{ GEN_EDIPARTY, "GEN_EDIPARTY"sv },
	{ GEN_RID, "GEN_RID"sv }
};

bool PeerVerifier::matchExtensionCriteria(const X509* cert,
                                          const NID nid,
                                          std::string_view criteria,
                                          const MatchType matchType) {

	// Only support NID_subject_alt_name and NID_issuer_alt_name
	if (nid != NID_subject_alt_name && nid != NID_issuer_alt_name) {
		m_verifyState.emplace_back("UnsupportedNIDExtensionType"sv);
		return false;
	}

	std::unique_ptr<STACK_OF(GENERAL_NAME), decltype(&GENERAL_NAME_free_impl)> sans(
	    static_cast<STACK_OF(GENERAL_NAME)*>(X509_get_ext_d2i(cert, nid, nullptr, nullptr)), GENERAL_NAME_free_impl);
	if (sans == nullptr) {
		m_verifyState.emplace_back("EmptySans"sv);
		return false;
	}
	int numSans = sk_GENERAL_NAME_num(sans.get());

	auto checkCriteriaEntry = [matchType, &criteria](const std::string_view prefix, const ASN1_STRING* entry) -> bool {
		return criteria.starts_with(prefix) && match_criteria_entry(criteria.substr(prefix.size()), entry, matchType);
	};

	for (int i = 0; i < numSans; ++i) {
		GENERAL_NAME* altName = sk_GENERAL_NAME_value(sans.get(), i);
		// See openssl/include/openssl/x509v3.h.in for more details about GENERAL_NAME
		const auto altNameType = altName->type;
		if (UNSUPPORTED_GENERAL_NAME_TYPES.contains(altNameType)) {
			m_verifyState.emplace_back(UNSUPPORTED_GENERAL_NAME_TYPES.at(altNameType));
			return false;
		}

		if (altNameType == GEN_EMAIL) {
			if (checkCriteriaEntry("EMAIL:", altName->d.rfc822Name)) {
				return true;
			}
		} else if (altNameType == GEN_DNS) {
			if (checkCriteriaEntry("DNS:", altName->d.dNSName)) {
				return true;
			}
		} else if (altNameType == GEN_URI) {
			if (checkCriteriaEntry("URI:", altName->d.uniformResourceIdentifier)) {
				return true;
			}
		} else if (altNameType == GEN_IPADD) {
			if (checkCriteriaEntry("IP:", altName->d.iPAddress)) {
				return true;
			}
		}

		// Ignore other types
	}

	// No entry matches
	m_verifyState.emplace_back("NoMatchingEntry"sv);
	return false;
}

bool PeerVerifier::matchNameCriteria(X509_NAME* name,
                                     const NID nid,
                                     std::string_view criteria,
                                     const MatchType matchType) {
	if (int index = X509_NAME_get_index_by_NID(name, nid, -1); index >= 0) {
		if (X509_NAME_get_index_by_NID(name, nid, index) != -1) {
			m_verifyState.emplace_back("MultipleNames"sv);
		} else {
			if (X509_NAME_ENTRY* nameEntry = X509_NAME_get_entry(name, index); nameEntry == nullptr) {
				m_verifyState.emplace_back("MissingNameEntry"sv);
			} else {
				m_currentName = getX509Name(name);
				if (match_criteria_entry(criteria, X509_NAME_ENTRY_get_data(nameEntry), matchType)) {
					return true;
				}
				m_verifyState.emplace_back("NameMismatch"sv);
			}
		}
	} else {
		m_verifyState.emplace_back("Missing"sv);
	}

	return false;
}

bool PeerVerifier::matchCriteria(const X509* cert, X509_NAME* name, const NID nid, const Criteria& criteria) {
	bool result = false;

	switch (criteria.location) {
	case X509Location::NAME:
		m_verifyState.emplace_back("Name"sv);
		result = matchNameCriteria(name, nid, criteria.criteria, criteria.match_type);
		break;

	case X509Location::EXTENSION:
		m_verifyState.emplace_back("Extension"sv);
		m_currentExtension = criteria.criteria;
		result = matchExtensionCriteria(cert, nid, criteria.criteria, criteria.match_type);
		break;

	default:
		// Base on the enum this should NEVER happen
		m_verifyState.emplace_back("UnsupportedX509LocationValue"sv);
	}

	if (result) {
		m_currentName.clear();
		m_currentExtension.clear();

		m_verifyState.pop_back();
	}
	return result;
}

bool PeerVerifier::verifyCriterias(const X509* cert, X509_NAME* name, const TLSPolicy::Rule::CriteriaMap& criterias) {
	for (const auto& [nid, criteria] : criterias) {
		if (!matchCriteria(cert, name, nid, criteria)) {
			return false;
		}
	}
	return true;
}

bool PeerVerifier::verifyRule(const TLSPolicy::Rule& rule) {
	m_verifyState.emplace_back("Rule"sv);

	{
		m_verifyState.emplace_back("Cert");

		const X509* cert = sk_X509_value(X509_STORE_CTX_get0_chain(m_storeCtx), 0);
		ASSERT(cert != nullptr);

		m_verifyState.emplace_back("Subject"sv);
		if (auto subject = X509_get_subject_name(cert); subject != nullptr) {
			if (!verifyCriterias(cert, subject, rule.subject_criteria)) {
				return false;
			}
		} else {
			m_verifyState.emplace_back("Missing"sv);
			return false;
		}
		m_verifyState.pop_back();

		m_verifyState.emplace_back("Issuer");
		if (auto issuer = X509_get_issuer_name(cert); issuer != nullptr) {
			if (!verifyCriterias(cert, issuer, rule.issuer_criteria)) {
				return false;
			}
		} else {
			m_verifyState.emplace_back("Missing"sv);
			return false;
		}
		m_verifyState.pop_back();

		m_verifyState.pop_back();
	}

	{
		const auto chain = X509_STORE_CTX_get0_chain(m_storeCtx);
		const auto numItems = sk_X509_num(chain);
		const X509* rootCert = sk_X509_value(chain, numItems - 1);
		ASSERT(rootCert != nullptr);

		m_verifyState.emplace_back("RootCert.Subject");
		if (auto subject = X509_get_subject_name(rootCert); subject != nullptr) {
			if (!verifyCriterias(rootCert, subject, rule.root_criteria)) {
				return false;
			}
		} else {
			m_verifyState.emplace_back("Missing"sv);
			return false;
		}
		m_verifyState.pop_back();
	}

	m_verifyState.pop_back();

	return true;
}

bool PeerVerifier::verify() {
	if (m_rules.size() == 0) {
		m_successReason = "No rule defined"sv;
		return true;
	}

	if (std::any_of(std::begin(m_rules), std::end(m_rules), [](const auto& rule) { return !rule.verify_cert; })) {
		m_successReason = "At least one certificate verfications rule disabled"sv;
		return true;
	}

	for (const auto& rule : m_rules) {
		if (verifyRule(rule)) {
			m_successReason = "Rule matched successfully"sv;
			ASSERT(m_currentName.empty() && m_currentExtension.empty() && m_verifyState.empty());
			return true;
		} else {
			std::string failureReason;

			// Prevent realloc
			failureReason.reserve(1024);

			for (const auto& item : m_verifyState) {
				failureReason.append(item);
				failureReason.push_back('.');
			}
			failureReason.pop_back();
			m_verifyState.clear();

			if (!m_currentName.empty()) {
				failureReason.append("[Name="sv);
				failureReason.append(m_currentName);
				failureReason.append("]"sv);

				m_currentName.clear();
			}
			if (!m_currentExtension.empty()) {
				failureReason.append("[Extension="sv);
				failureReason.append(m_currentExtension);
				failureReason.append("]"sv);

				m_currentExtension.clear();
			}

			m_failureReasons.push_back(std::move(failureReason));
		}
	}

	// No rule matched
	return false;
}

} // anonymous namespace

bool TLSPolicy::verify_peer(bool preverified, X509_STORE_CTX* store_ctx, const NetworkAddress& peerAddress) {
	// Preverification
	if (!preverified) {
		TraceEvent(SevWarn, "TLSPolicyFailure")
		    .suppressFor(1.0)
		    .detail("PeerAddress", peerAddress)
		    .detail("Reason", "preverification failed")
		    .detail("VerifyError", X509_verify_cert_error_string(X509_STORE_CTX_get_error(store_ctx)));
		return false;
	}

	PeerVerifier verifier(rules, store_ctx);

	if (verifier.isErr()) {
		// Must have all rules tried with failure
		ASSERT_EQ(verifier.getFailureReasons().size(), rules.size());

		for (size_t i = 0; i < rules.size(); ++i) {
			const auto& failureReason = verifier.getFailureReasons()[i];
			const auto& rule = rules[i];
			std::string eventName = fmt::format("TLSPolicyFailure{:02d}", i);

			TraceEvent(SevWarn, eventName.c_str())
			    .suppressFor(1.0)
			    .detail("PeerAddress", peerAddress)
			    .detail("Reason", failureReason)
			    .detail("Rule", rule.toString());
		}
	} else {
		TraceEvent(SevInfo, "TLSPolicySuccess")
		    .suppressFor(1.0)
		    .detail("PeerAddress", peerAddress)
		    .detail("Reason", verifier.getSuccessReason());
	}

	return verifier.isOk();
}
