/*
 * TLSConfig.actor.h
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

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#if defined(NO_INTELLISENSE) && !defined(FLOW_TLS_CONFIG_ACTOR_G_H)
#define FLOW_TLS_CONFIG_ACTOR_G_H
#include "flow/TLSConfig.actor.g.h"
#elif !defined(FLOW_TLS_CONFIG_ACTOR_H)
#define FLOW_TLS_CONFIG_ACTOR_H

#pragma once

#include <cstdio>
#include <map>
#include <string>
#include <vector>
#include <boost/system/system_error.hpp>
#include "flow/FastRef.h"
#include "flow/Knobs.h"
#include "flow/flow.h"

#ifndef TLS_DISABLED

#include <openssl/x509.h>
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
#endif

#include "flow/actorcompiler.h" // This must be the last #include.

enum class TLSEndpointType { UNSET = 0, CLIENT, SERVER };

class TLSConfig;
template <typename T>
class LoadAsyncActorState;

class LoadedTLSConfig {
public:
	std::string getCertificateBytes() const { return tlsCertBytes; }

	std::string getKeyBytes() const { return tlsKeyBytes; }

	std::string getCABytes() const { return tlsCABytes; }

	// Return the explicitly set verify peers string.
	// If no verify peers string was set, return the environment setting
	// If no environment setting exists, return "Check.Valid=1"
	std::vector<std::string> getVerifyPeers() const;

	// Return the explicitly set password.
	// If no password was set, return the environment setting
	// If no environment setting exists, return an empty string
	std::string getPassword() const;

	TLSEndpointType getEndpointType() const { return endpointType; }

	bool isTLSEnabled() const { return endpointType != TLSEndpointType::UNSET; }

	void print(FILE* fp);

#ifndef PRIVATE_EXCEPT_FOR_TLSCONFIG_CPP
private:
#endif

	std::string tlsCertBytes, tlsKeyBytes, tlsCABytes;
	std::string tlsPassword;
	std::vector<std::string> tlsVerifyPeers;
	TLSEndpointType endpointType = TLSEndpointType::UNSET;

	friend class TLSConfig;
	template <typename T>
	friend class LoadAsyncActorState;
};

class TLSConfig {
public:
	enum {
		OPT_TLS = 100000,
		OPT_TLS_PLUGIN,
		OPT_TLS_CERTIFICATES,
		OPT_TLS_KEY,
		OPT_TLS_VERIFY_PEERS,
		OPT_TLS_CA_FILE,
		OPT_TLS_PASSWORD
	};

	TLSConfig() = default;
	explicit TLSConfig(TLSEndpointType endpointType) : endpointType(endpointType) {}

	void setCertificatePath(const std::string& path) {
		tlsCertPath = path;
		tlsCertBytes = "";
	}

	void setCertificateBytes(const std::string& bytes) {
		tlsCertBytes = bytes;
		tlsCertPath = "";
	}

	void setKeyPath(const std::string& path) {
		tlsKeyPath = path;
		tlsKeyBytes = "";
	}

	void setKeyBytes(const std::string& bytes) {
		tlsKeyBytes = bytes;
		tlsKeyPath = "";
	}

	void setCAPath(const std::string& path) {
		tlsCAPath = path;
		tlsCABytes = "";
	}

	void setCABytes(const std::string& bytes) {
		tlsCABytes = bytes;
		tlsCAPath = "";
	}

	void setPassword(const std::string& password) { tlsPassword = password; }

	void clearVerifyPeers() { tlsVerifyPeers.clear(); }

	void addVerifyPeers(const std::string& verifyPeers) { tlsVerifyPeers.push_back(verifyPeers); }

	// Load all specified certificates into memory, and return an object that
	// allows access to them.
	// If self has any certificates by path, they will be *synchronously* loaded from disk.
	LoadedTLSConfig loadSync() const;

	// Load all specified certificates into memory, and return an object that
	// allows access to them.
	// If self has any certificates by path, they will be *asynchronously* loaded from disk.
	Future<LoadedTLSConfig> loadAsync() const { return loadAsync(this); }

	// Return the explicitly set path.
	// If one was not set, return the path from the environment.
	// (Cert and Key only) If neither exist, check for fdb.pem in cwd
	// (Cert and Key only) If fdb.pem doesn't exist, check for it in default config dir
	// Otherwise return the empty string.
	// Theoretically, fileExists() can block, so these functions are labelled as synchronous
	// TODO: make an easy to use Future<bool> fileExists, and port lots of code over to it.
	std::string getCertificatePathSync() const;
	std::string getKeyPathSync() const;
	std::string getCAPathSync() const;

private:
	ACTOR static Future<LoadedTLSConfig> loadAsync(const TLSConfig* self);
	template <typename T>
	friend class LoadAsyncActorState;

	std::string tlsCertPath, tlsKeyPath, tlsCAPath;
	std::string tlsCertBytes, tlsKeyBytes, tlsCABytes;
	std::string tlsPassword;
	std::vector<std::string> tlsVerifyPeers;
	TLSEndpointType endpointType = TLSEndpointType::UNSET;
};

#ifndef TLS_DISABLED
namespace boost {
namespace asio {
namespace ssl {
struct context;
}
} // namespace asio
} // namespace boost
void ConfigureSSLContext(
    const LoadedTLSConfig& loaded,
    boost::asio::ssl::context* context,
    std::function<void()> onPolicyFailure = []() {});
#endif

class TLSPolicy : ReferenceCounted<TLSPolicy> {
public:
	TLSPolicy(TLSEndpointType client) : is_client(client == TLSEndpointType::CLIENT) {}
	virtual ~TLSPolicy();

	virtual void addref() { ReferenceCounted<TLSPolicy>::addref(); }
	virtual void delref() { ReferenceCounted<TLSPolicy>::delref(); }

#ifndef TLS_DISABLED
	static std::string ErrorString(boost::system::error_code e);

	void set_verify_peers(std::vector<std::string> verify_peers);
	bool verify_peer(bool preverified, X509_STORE_CTX* store_ctx);

	std::string toString() const;

	struct Rule {
		explicit Rule(std::string input);

		std::string toString() const;

		std::map<NID, Criteria> subject_criteria;
		std::map<NID, Criteria> issuer_criteria;
		std::map<NID, Criteria> root_criteria;

		bool verify_cert = true;
		bool verify_time = true;
	};

	std::vector<Rule> rules;
#endif
	bool is_client;
};

#define TLS_PLUGIN_FLAG "--tls-plugin"
#define TLS_CERTIFICATE_FILE_FLAG "--tls-certificate-file"
#define TLS_KEY_FILE_FLAG "--tls-key-file"
#define TLS_VERIFY_PEERS_FLAG "--tls-verify-peers"
#define TLS_CA_FILE_FLAG "--tls-ca-file"
#define TLS_PASSWORD_FLAG "--tls-password"

#define TLS_OPTION_FLAGS                                                                                               \
	{ TLSConfig::OPT_TLS_PLUGIN, TLS_PLUGIN_FLAG, SO_REQ_SEP },                                                        \
	    { TLSConfig::OPT_TLS_CERTIFICATES, TLS_CERTIFICATE_FILE_FLAG, SO_REQ_SEP },                                    \
	    { TLSConfig::OPT_TLS_KEY, TLS_KEY_FILE_FLAG, SO_REQ_SEP },                                                     \
	    { TLSConfig::OPT_TLS_VERIFY_PEERS, TLS_VERIFY_PEERS_FLAG, SO_REQ_SEP },                                        \
	    { TLSConfig::OPT_TLS_PASSWORD, TLS_PASSWORD_FLAG, SO_REQ_SEP },                                                \
	    { TLSConfig::OPT_TLS_CA_FILE, TLS_CA_FILE_FLAG, SO_REQ_SEP },

#define TLS_HELP                                                                                                       \
	"  " TLS_CERTIFICATE_FILE_FLAG " CERTFILE\n"                                                                       \
	"                 The path of a file containing the TLS certificate and CA\n"                                      \
	"                 chain.\n"                                                                                        \
	"  " TLS_CA_FILE_FLAG " CERTAUTHFILE\n"                                                                            \
	"                 The path of a file containing the CA certificates chain.\n"                                      \
	"  " TLS_KEY_FILE_FLAG " KEYFILE\n"                                                                                \
	"                 The path of a file containing the private key corresponding\n"                                   \
	"                 to the TLS certificate.\n"                                                                       \
	"  " TLS_PASSWORD_FLAG " PASSCODE\n"                                                                               \
	"                 The passphrase of encrypted private key\n"                                                       \
	"  " TLS_VERIFY_PEERS_FLAG " CONSTRAINTS\n"                                                                        \
	"                 The constraints by which to validate TLS peers. The contents\n"                                  \
	"                 and format of CONSTRAINTS are plugin-specific.\n"

#include "flow/unactorcompiler.h"
#endif