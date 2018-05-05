/*
 * TLSConnection.actor.cpp
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

#include "flow/actorcompiler.h"
#include "flow/network.h"

#include "TLSConnection.h"

#include "ITLSPlugin.h"
#include "LoadPlugin.h"
#include "Platform.h"

// Must not throw an exception from this function!
static int send_func(void* ctx, const uint8_t* buf, int len) {
	TLSConnection* conn = (TLSConnection*)ctx;

	try {
		SendBuffer sb;
		sb.bytes_sent = 0;
		sb.bytes_written = len;
		sb.data = buf;
		sb.next = 0;

		int w = conn->conn->write( &sb );
		return w;
	} catch ( Error& e ) {
		TraceEvent("TLSConnectionSendError", conn->getDebugID()).error(e);
		return -1;
	} catch ( ... ) {
		TraceEvent("TLSConnectionSendError", conn->getDebugID()).error( unknown_error() );
		return -1;
	}
}

// Must not throw an exception from this function!
static int recv_func(void* ctx, uint8_t* buf, int len) {
	TLSConnection* conn = (TLSConnection*)ctx;

	try {
		int r = conn->conn->read( buf, buf + len );
		return r;
	} catch ( Error& e ) {
		TraceEvent("TLSConnectionRecvError", conn->getDebugID()).error(e);
		return -1;
	} catch ( ... ) {
		TraceEvent("TLSConnectionRecvError", conn->getDebugID()).error( unknown_error() );
		return -1;
	}
}

ACTOR static Future<Void> handshake( TLSConnection* self ) {
	loop {
		int r = self->session->handshake();
		if ( r == ITLSSession::SUCCESS ) break;
		if ( r == ITLSSession::FAILED ) {
			TraceEvent("TLSConnectionHandshakeError", self->getDebugID());
			throw connection_failed();
		}
		ASSERT( r == ITLSSession::WANT_WRITE || r == ITLSSession::WANT_READ );
		Void _ = wait( r == ITLSSession::WANT_WRITE ? self->conn->onWritable() : self->conn->onReadable() );
	}

	TraceEvent("TLSConnectionHandshakeSuccessful", self->getDebugID())
		.detail("Peer", self->getPeerAddress());

	return Void();
}

TLSConnection::TLSConnection( Reference<IConnection> const& conn, Reference<ITLSPolicy> const& policy, bool is_client ) : conn(conn), write_wants(0), read_wants(0), uid(conn->getDebugID()) {
	session = Reference<ITLSSession>( policy->create_session(is_client, send_func, this, recv_func, this, (void*)&uid) );
	if ( !session ) {
		// If session is NULL, we're trusting policy->create_session
		// to have used its provided logging function to have logged
		// the error
		throw internal_error();
	}
	handshook = handshake(this);
}

Future<Void> TLSConnection::onWritable() {
	if ( !handshook.isReady() )
		return handshook;
	return
		write_wants == ITLSSession::WANT_READ ? conn->onReadable() :
		write_wants == ITLSSession::WANT_WRITE ? conn->onWritable() :
		Void();
}

Future<Void> TLSConnection::onReadable() {
	if ( !handshook.isReady() )
		return handshook;
	return
		read_wants == ITLSSession::WANT_READ ? conn->onReadable() :
		read_wants == ITLSSession::WANT_WRITE ? conn->onWritable() :
		Void();
}

int TLSConnection::read( uint8_t* begin, uint8_t* end ) {
	if ( !handshook.isReady() ) return 0;
	handshook.get();

	read_wants = 0;
	int r = session->read( begin, end - begin );
	if ( r > 0 )
		return r;

	if ( r == ITLSSession::FAILED ) throw connection_failed();

	ASSERT( r == ITLSSession::WANT_WRITE || r == ITLSSession::WANT_READ );

	read_wants = r;
	return 0;
}

int TLSConnection::write( SendBuffer const* buffer, int limit ) {
	ASSERT(limit > 0);

	if ( !handshook.isReady() ) return 0;
	handshook.get();

	write_wants = 0;
	int toSend = std::min(limit, buffer->bytes_written - buffer->bytes_sent);
	ASSERT(toSend);
	int w = session->write( buffer->data + buffer->bytes_sent, toSend );
	if ( w > 0 )
		return w;

	if ( w == ITLSSession::FAILED ) throw connection_failed();

	ASSERT( w == ITLSSession::WANT_WRITE || w == ITLSSession::WANT_READ );

	write_wants = w;
	return 0;
}

ACTOR Future<Reference<IConnection>> wrap( Reference<ITLSPolicy> policy, bool is_client, Future<Reference<IConnection>> c ) {
	Reference<IConnection> conn = wait(c);
	return Reference<IConnection>(new TLSConnection( conn, policy, is_client ));
}

Future<Reference<IConnection>> TLSListener::accept() {
	return wrap( policy, false, listener->accept() );
}

TLSNetworkConnections::TLSNetworkConnections( Reference<TLSOptions> options ) : options(options) {
	network = INetworkConnections::net();
	g_network->setGlobal(INetwork::enumGlobal::enNetworkConnections, (flowGlobalType) this);
}

Future<Reference<IConnection>> TLSNetworkConnections::connect( NetworkAddress toAddr ) {
	if ( toAddr.isTLS() ) {
		NetworkAddress clearAddr( toAddr.ip, toAddr.port, toAddr.isPublic(), false );
		TraceEvent("TLSConnectionConnecting").detail("ToAddr", toAddr);
		return wrap( options->get_policy(), true, network->connect( clearAddr ) );
	}
	return network->connect( toAddr );
}

Future<std::vector<NetworkAddress>> TLSNetworkConnections::resolveTCPEndpoint( std::string host, std::string service) {
	return network->resolveTCPEndpoint( host, service );
}

Reference<IListener> TLSNetworkConnections::listen( NetworkAddress localAddr ) {
	if ( localAddr.isTLS() ) {
		NetworkAddress clearAddr( localAddr.ip, localAddr.port, localAddr.isPublic(), false );
		TraceEvent("TLSConnectionListening").detail("OnAddr", localAddr);
		return Reference<IListener>(new TLSListener( options->get_policy(), network->listen( clearAddr ) ));
	}
	return network->listen( localAddr );
}

// 5MB for loading files into memory
#define CERT_FILE_MAX_SIZE (5 * 1024 * 1024)

void TLSOptions::set_plugin_name_or_path( std::string const& plugin_name_or_path ) {
	if ( plugin )
		throw invalid_option();

	init_plugin( plugin_name_or_path );
}

void TLSOptions::set_cert_file( std::string const& cert_file ) {
	try {
		TraceEvent("TLSConnectionSettingCertFile").detail("CertFilePath", cert_file);
		set_cert_data( readFileBytes( cert_file, CERT_FILE_MAX_SIZE ) );
	} catch ( Error& ) {
		TraceEvent(SevError, "TLSOptionsSetCertFileError").detail("Filename", cert_file);
		throw;
	}
}

void TLSOptions::set_cert_data( std::string const& cert_data ) {
	if ( !policy )
		init_plugin();

	TraceEvent("TLSConnectionSettingCertData").detail("CertDataSize", cert_data.size());
	if ( !policy->set_cert_data( (const uint8_t*)&cert_data[0], cert_data.size() ) )
		throw tls_error();

	certs_set = true;
}

void TLSOptions::set_key_file( std::string const& key_file ) {
	try {
		TraceEvent("TLSConnectionSettingKeyFile").detail("KeyFilePath", key_file);
		set_key_data( readFileBytes( key_file, CERT_FILE_MAX_SIZE ) );
	} catch ( Error& ) {
		TraceEvent(SevError, "TLSOptionsSetKeyFileError").detail("Filename", key_file);
		throw;
	}
}

void TLSOptions::set_key_data( std::string const& key_data ) {
	if ( !policy )
		init_plugin();

	TraceEvent("TLSConnectionSettingKeyData").detail("KeyDataSize", key_data.size());
	if ( !policy->set_key_data( (const uint8_t*)&key_data[0], key_data.size() ) )
		throw tls_error();

	key_set = true;
}

void TLSOptions::set_verify_peers( std::string const& verify_peers ) {
	if ( !policy )
		init_plugin();

	TraceEvent("TLSConnectionSettingVerifyPeers").detail("Value", verify_peers);
	if ( !policy->set_verify_peers( (const uint8_t*)&verify_peers[0], verify_peers.size() ) )
		throw tls_error();

	verify_peers_set = true;
}

void TLSOptions::register_network() {
	new TLSNetworkConnections( Reference<TLSOptions>::addRef( this ) );
}

const char *defaultCertFileName = "fdb.pem";

Reference<ITLSPolicy> TLSOptions::get_policy() {
	if ( !certs_set ) {
		std::string certFile;
		if ( !platform::getEnvironmentVar( "FDB_TLS_CERTIFICATE_FILE", certFile ) )
			certFile = fileExists(defaultCertFileName) ? defaultCertFileName : joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
		set_cert_file( certFile );
	}
	if ( !key_set ) {
		std::string keyFile;
		if ( !platform::getEnvironmentVar( "FDB_TLS_KEY_FILE", keyFile ) )
			keyFile = fileExists(defaultCertFileName) ? defaultCertFileName : joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
		set_key_file( keyFile );
	}
	if( !verify_peers_set ) {
		std::string verifyPeerString;
		if ( platform::getEnvironmentVar( "FDB_TLS_VERIFY_PEERS", verifyPeerString ) )
			set_verify_peers( verifyPeerString );
	}

	return policy;
}

static void TLSConnectionLogFunc( const char* event, void* uid_ptr, int is_error, ... ) {
	UID uid;

	if ( uid_ptr )
		uid = *(UID*)uid_ptr;

	Severity s = SevInfo;
	if ( is_error )
		s = SevError;

	auto t = TraceEvent( s, event, uid );

	va_list ap;
	char* field;

	va_start( ap, is_error );
	while ( (field = va_arg( ap, char* )) ) {
		t.detail( field, va_arg( ap, char* ) );
	}
	va_end( ap );
}

void TLSOptions::init_plugin( std::string const& plugin_path ) {
	std::string path;

	if ( plugin_path.length() ) {
		path = plugin_path;
	} else {
		if ( !platform::getEnvironmentVar( "FDB_TLS_PLUGIN", path ) )
			// FIXME: should there be other fallbacks?
			path = platform::getDefaultPluginPath("FDBLibTLS");
	}

	TraceEvent("TLSConnectionLoadingPlugin").detail("PluginPath", path);
	plugin = loadPlugin<ITLSPlugin>( path.c_str() );
	if ( !plugin ) {
		// FIXME: allow?
		TraceEvent(SevError, "TLSConnectionPluginInitError").detail("Plugin", path).GetLastError();
		throw tls_error();
	}

	policy = Reference<ITLSPolicy>( plugin->create_policy( TLSConnectionLogFunc ) );
	if ( !policy ) {
		// Hopefully create_policy logged something with the log func
		TraceEvent(SevError, "TLSConnectionCreatePolicyError");
		throw tls_error();
	}
}

bool TLSOptions::enabled() {
	return !!policy;
}
