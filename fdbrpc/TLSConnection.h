/*
 * TLSConnection.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FLOW_TLSCONNECTION_H
#define FLOW_TLSCONNECTION_H

#pragma once

#include "flow/Platform.h"

#include "fdbrpc/ITLSPlugin.h"

struct TLSConnection : IConnection, ReferenceCounted<TLSConnection> {
	Reference<IConnection> conn;
	Reference<ITLSSession> session;

	Future<Void> handshook;

	int write_wants, read_wants;

	UID uid;

	virtual void addref() { ReferenceCounted<TLSConnection>::addref(); }
	virtual void delref() { ReferenceCounted<TLSConnection>::delref(); }

	TLSConnection( Reference<IConnection> const& conn, Reference<ITLSPolicy> const& policy, bool is_client, std::string host);
	~TLSConnection() {
		// Here for ordering to make sure we delref the ITLSSession
		// which has a pointer to this object
		session.clear();
	}

	virtual void close() { conn->close(); }

	virtual Future<Void> onWritable();

	virtual Future<Void> onReadable();

	virtual int read( uint8_t* begin, uint8_t* end );

	virtual int write( SendBuffer const* buffer, int limit);

	virtual NetworkAddress getPeerAddress() {
		NetworkAddress a = conn->getPeerAddress();
		return NetworkAddress(a.ip, a.port, a.isPublic(), true);
	}

	virtual UID getDebugID() { return uid; }
};

struct TLSOptions : ReferenceCounted<TLSOptions> {
	enum { OPT_TLS = 100000, OPT_TLS_PLUGIN, OPT_TLS_CERTIFICATES, OPT_TLS_KEY, OPT_TLS_VERIFY_PEERS, OPT_TLS_CA_FILE, OPT_TLS_PASSWORD };
	enum PolicyType { POLICY_VERIFY_PEERS = 1, POLICY_NO_VERIFY_PEERS };
	TLSOptions() : certs_set(false), key_set(false), verify_peers_set(false), ca_set(false) {
#ifndef TLS_DISABLED
		init_plugin( );
#endif
	}

	void set_cert_file( std::string const& cert_file );
	void set_cert_data( std::string const& cert_data );
	void set_ca_file(std::string const& ca_file);
	void set_ca_data(std::string const& ca_data);
	// If there is a passphrase, this api should be called prior to setting key for the passphrase to be used
	void set_key_password( std::string const& password );
	void set_key_file( std::string const& key_file );
	void set_key_data( std::string const& key_data );
	void set_verify_peers( std::vector<std::string> const& verify_peers );

	void register_network();

	Reference<ITLSPolicy> get_policy(PolicyType type);
	bool enabled();

	struct PolicyInfo {
		std::string ca_path;
		Standalone<StringRef> ca_contents;
		std::string key_path;
		std::string keyPassword;
		Standalone<StringRef> key_contents;
		std::string cert_path;
		Standalone<StringRef> cert_contents;
		std::vector<std::string> verify_peers;
	};

private:
	void init_plugin();

	Reference<ITLSPlugin> plugin;
	PolicyInfo policyInfo;
	AsyncVar<Reference<ITLSPolicy>> policyVerifyPeersSet;
	AsyncVar<Reference<ITLSPolicy>> policyVerifyPeersNotSet;
	Optional<Future<Void>> configurationReloader;

	bool certs_set, key_set, verify_peers_set, ca_set;
};

struct TLSListener : IListener, ReferenceCounted<TLSListener> {
	Reference<IListener> listener;
	Reference<TLSOptions> options;

	TLSListener( Reference<TLSOptions> options, Reference<IListener> listener ) : options(options), listener(listener) {}

	virtual void addref() { ReferenceCounted<TLSListener>::addref(); }
	virtual void delref() { ReferenceCounted<TLSListener>::delref(); }

	virtual Future<Reference<IConnection>> accept();

	virtual NetworkAddress getListenAddress() { return listener->getListenAddress(); }
};

struct TLSNetworkConnections : INetworkConnections {
	INetworkConnections *network;

	explicit TLSNetworkConnections( Reference<TLSOptions> options );

	virtual Future<Reference<IConnection>> connect( NetworkAddress toAddr, std::string host );
	virtual Future<std::vector<NetworkAddress>> resolveTCPEndpoint( std::string host, std::string service);

	virtual Reference<IListener> listen( NetworkAddress localAddr );

private:
	Reference<TLSOptions> options;
};

#define TLS_PLUGIN_FLAG "--tls_plugin"
#define TLS_CERTIFICATE_FILE_FLAG "--tls_certificate_file"
#define TLS_KEY_FILE_FLAG "--tls_key_file"
#define TLS_VERIFY_PEERS_FLAG "--tls_verify_peers"
#define TLS_CA_FILE_FLAG "--tls_ca_file"
#define TLS_PASSWORD_FLAG "--tls_password"

#define TLS_OPTION_FLAGS \
	{ TLSOptions::OPT_TLS_PLUGIN,       TLS_PLUGIN_FLAG,           SO_REQ_SEP }, \
	{ TLSOptions::OPT_TLS_CERTIFICATES, TLS_CERTIFICATE_FILE_FLAG, SO_REQ_SEP }, \
	{ TLSOptions::OPT_TLS_KEY,          TLS_KEY_FILE_FLAG,         SO_REQ_SEP }, \
	{ TLSOptions::OPT_TLS_VERIFY_PEERS, TLS_VERIFY_PEERS_FLAG,     SO_REQ_SEP }, \
	{ TLSOptions::OPT_TLS_PASSWORD,     TLS_PASSWORD_FLAG,         SO_REQ_SEP }, \
	{ TLSOptions::OPT_TLS_CA_FILE,      TLS_CA_FILE_FLAG,          SO_REQ_SEP },

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

#endif /* FLOW_TLSCONNECTION_H */
