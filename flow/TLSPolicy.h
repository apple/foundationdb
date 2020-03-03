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
#include <boost/system/system_error.hpp>
#include "flow/FastRef.h"

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
#endif

struct TLSParams {
	enum { OPT_TLS = 100000, OPT_TLS_PLUGIN, OPT_TLS_CERTIFICATES, OPT_TLS_KEY, OPT_TLS_VERIFY_PEERS, OPT_TLS_CA_FILE, OPT_TLS_PASSWORD };

	std::string tlsCertPath, tlsKeyPath, tlsCAPath, tlsPassword;
	std::string tlsCertBytes, tlsKeyBytes, tlsCABytes;
};

class TLSPolicy : ReferenceCounted<TLSPolicy> {
public:
	enum class Is {
		CLIENT,
		SERVER
	};

	TLSPolicy(Is client) : is_client(client == Is::CLIENT) {}
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

		std::map< NID, Criteria > subject_criteria;
		std::map< NID, Criteria > issuer_criteria;
		std::map< NID, Criteria > root_criteria;

		bool verify_cert = true;
		bool verify_time = true;
	};

	std::vector<Rule> rules;
#endif
	bool is_client;
};

#define TLS_PLUGIN_FLAG "--tls_plugin"
#define TLS_CERTIFICATE_FILE_FLAG "--tls_certificate_file"
#define TLS_KEY_FILE_FLAG "--tls_key_file"
#define TLS_VERIFY_PEERS_FLAG "--tls_verify_peers"
#define TLS_CA_FILE_FLAG "--tls_ca_file"
#define TLS_PASSWORD_FLAG "--tls_password"

#define TLS_OPTION_FLAGS \
	{ TLSParams::OPT_TLS_PLUGIN,       TLS_PLUGIN_FLAG,           SO_REQ_SEP }, \
	{ TLSParams::OPT_TLS_CERTIFICATES, TLS_CERTIFICATE_FILE_FLAG, SO_REQ_SEP }, \
	{ TLSParams::OPT_TLS_KEY,          TLS_KEY_FILE_FLAG,         SO_REQ_SEP }, \
	{ TLSParams::OPT_TLS_VERIFY_PEERS, TLS_VERIFY_PEERS_FLAG,     SO_REQ_SEP }, \
	{ TLSParams::OPT_TLS_PASSWORD,     TLS_PASSWORD_FLAG,         SO_REQ_SEP }, \
	{ TLSParams::OPT_TLS_CA_FILE,      TLS_CA_FILE_FLAG,          SO_REQ_SEP },

#define TLS_HELP \
	"  " TLS_CERTIFICATE_FILE_FLAG " CERTFILE\n" \
	"                 The path of a file containing the TLS certificate and CA\n" \
	"                 chain.\n"											\
	"  " TLS_CA_FILE_FLAG " CERTAUTHFILE\n" \
	"                 The path of a file containing the CA certificates chain.\n"	\
	"  " TLS_KEY_FILE_FLAG " KEYFILE\n" \
	"                 The path of a file containing the private key corresponding\n" \
	"                 to the TLS certificate.\n"						\
	"  " TLS_PASSWORD_FLAG " PASSCODE\n" \
	"                 The passphrase of encrypted private key\n" \
	"  " TLS_VERIFY_PEERS_FLAG " CONSTRAINTS\n" \
	"                 The constraints by which to validate TLS peers. The contents\n" \
	"                 and format of CONSTRAINTS are plugin-specific.\n"

#endif
