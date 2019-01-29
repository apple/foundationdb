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

#include <memory>
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/Knobs.h"
#include "fdbrpc/TLSConnection.h"
#include "fdbrpc/ITLSPlugin.h"
#include "fdbrpc/LoadPlugin.h"
#include "fdbrpc/Platform.h"
#include "fdbrpc/IAsyncFile.h"
#include "flow/actorcompiler.h"  // This must be the last #include.

// Name of specialized TLS Plugin
const char* tlsPluginName = "fdb-libressl-plugin";

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
		TraceEvent("TLSConnectionSendError", conn->getDebugID()).error(e).suppressFor(1.0);
		return -1;
	} catch ( ... ) {
		TraceEvent("TLSConnectionSendError", conn->getDebugID()).error( unknown_error() ).suppressFor(1.0);
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
		TraceEvent("TLSConnectionRecvError", conn->getDebugID()).error(e).suppressFor(1.0);
		return -1;
	} catch ( ... ) {
		TraceEvent("TLSConnectionRecvError", conn->getDebugID()).error( unknown_error() ).suppressFor(1.0);
		return -1;
	}
}

ACTOR static Future<Void> handshake( TLSConnection* self ) {
	loop {
		int r = self->session->handshake();
		if ( r == ITLSSession::SUCCESS ) break;
		if ( r == ITLSSession::FAILED ) {
			TraceEvent("TLSConnectionHandshakeError", self->getDebugID()).suppressFor(1.0);
			throw connection_failed();
		}
		ASSERT( r == ITLSSession::WANT_WRITE || r == ITLSSession::WANT_READ );
		wait( r == ITLSSession::WANT_WRITE ? self->conn->onWritable() : self->conn->onReadable() );
	}

	TraceEvent("TLSConnectionHandshakeSuccessful", self->getDebugID()).suppressFor(1.0).detail("Peer", self->getPeerAddress());

	return Void();
}

TLSConnection::TLSConnection( Reference<IConnection> const& conn, Reference<ITLSPolicy> const& policy, bool is_client, std::string host) : conn(conn), write_wants(0), read_wants(0), uid(conn->getDebugID()) {
	const char * serverName = host.empty() ? NULL : host.c_str();
	session = Reference<ITLSSession>( policy->create_session(is_client, serverName, send_func, this, recv_func, this, (void*)&uid) );
	if ( !session ) {
		// If session is NULL, we're trusting policy->create_session
		// to have used its provided logging function to have logged
		// the error
		throw tls_error();
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

ACTOR Future<Reference<IConnection>> wrap( Reference<ITLSPolicy> policy, bool is_client, Future<Reference<IConnection>> c, std::string host) {
	Reference<IConnection> conn = wait(c);
	return Reference<IConnection>(new TLSConnection( conn, policy, is_client, host ));
}

Future<Reference<IConnection>> TLSListener::accept() {
	return wrap( options->get_policy(TLSOptions::POLICY_VERIFY_PEERS), false, listener->accept(), "");
}

TLSNetworkConnections::TLSNetworkConnections( Reference<TLSOptions> options ) : options(options) {
	network = INetworkConnections::net();
	g_network->setGlobal(INetwork::enumGlobal::enNetworkConnections, (flowGlobalType) this);
}

Future<Reference<IConnection>> TLSNetworkConnections::connect( NetworkAddress toAddr, std::string host) {
	if ( toAddr.isTLS() ) {
		NetworkAddress clearAddr( toAddr.ip, toAddr.port, toAddr.isPublic(), false );
		TraceEvent("TLSConnectionConnecting").suppressFor(1.0).detail("ToAddr", toAddr);
		// For FDB<->FDB connections, we don't have hostnames and can't verify IP
		// addresses against certificates, so we have our own peer verifying logic
		// to use. For FDB<->external system connections, we can use the standard
		// hostname-based certificate verification logic.
		if (host.empty() || host == toIPString(toAddr.ip))
			return wrap(options->get_policy(TLSOptions::POLICY_VERIFY_PEERS), true, network->connect(clearAddr), std::string(""));
		else
			return wrap( options->get_policy(TLSOptions::POLICY_NO_VERIFY_PEERS), true, network->connect( clearAddr ), host );
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
		return Reference<IListener>(new TLSListener( options, network->listen( clearAddr ) ));
	}
	return network->listen( localAddr );
}

// 5MB for loading files into memory
#define CERT_FILE_MAX_SIZE (5 * 1024 * 1024)

void TLSOptions::set_cert_file( std::string const& cert_file ) {
	try {
		TraceEvent("TLSConnectionSettingCertFile").detail("CertFilePath", cert_file);
		policyInfo.cert_path = cert_file;
		set_cert_data( readFileBytes( cert_file, CERT_FILE_MAX_SIZE ) );
	} catch ( Error& ) {
		TraceEvent(SevError, "TLSOptionsSetCertFileError").detail("Filename", cert_file);
		throw;
	}
}

void TLSOptions::set_ca_file(std::string const& ca_file) {
	try {
		TraceEvent("TLSConnectionSettingCAFile").detail("CAPath", ca_file);
		policyInfo.ca_path = ca_file;
		set_ca_data(readFileBytes(ca_file, CERT_FILE_MAX_SIZE));
	}
	catch (Error&) {
		TraceEvent(SevError, "TLSOptionsSetCertAError").detail("Filename", ca_file);
		throw;
	}
}

void TLSOptions::set_ca_data(std::string const& ca_data) {
	if (!policyVerifyPeersSet.get() || !policyVerifyPeersNotSet.get())
		init_plugin();

	TraceEvent("TLSConnectionSettingCAData").detail("CADataSize", ca_data.size());
	policyInfo.ca_contents = Standalone<StringRef>(ca_data);
	if (!policyVerifyPeersSet.get()->set_ca_data((const uint8_t*)&ca_data[0], ca_data.size()))
		throw tls_error();
	if (!policyVerifyPeersNotSet.get()->set_ca_data((const uint8_t*)&ca_data[0], ca_data.size()))
		throw tls_error();

	ca_set = true;
}

void TLSOptions::set_cert_data( std::string const& cert_data ) {
	if (!policyVerifyPeersSet.get() || !policyVerifyPeersNotSet.get())
		init_plugin();

	TraceEvent("TLSConnectionSettingCertData").detail("CertDataSize", cert_data.size());
	policyInfo.cert_contents = Standalone<StringRef>(cert_data);
	if ( !policyVerifyPeersSet.get()->set_cert_data( (const uint8_t*)&cert_data[0], cert_data.size() ) )
		throw tls_error();
	if (!policyVerifyPeersNotSet.get()->set_cert_data((const uint8_t*)&cert_data[0], cert_data.size()))
		throw tls_error();

	certs_set = true;
}

void TLSOptions::set_key_password(std::string const& password) {
	TraceEvent("TLSConnectionSettingPassword");
	policyInfo.keyPassword = password;
}

void TLSOptions::set_key_file( std::string const& key_file ) {
	try {
		TraceEvent("TLSConnectionSettingKeyFile").detail("KeyFilePath", key_file);
		policyInfo.key_path = key_file;
		set_key_data( readFileBytes( key_file, CERT_FILE_MAX_SIZE ) );
	} catch ( Error& ) {
		TraceEvent(SevError, "TLSOptionsSetKeyFileError").detail("Filename", key_file);
		throw;
	}
}

void TLSOptions::set_key_data( std::string const& key_data ) {
	if (!policyVerifyPeersSet.get() || !policyVerifyPeersNotSet.get())
		init_plugin();
	const char *passphrase = policyInfo.keyPassword.empty() ? NULL : policyInfo.keyPassword.c_str();
	TraceEvent("TLSConnectionSettingKeyData").detail("KeyDataSize", key_data.size());
	policyInfo.key_contents = Standalone<StringRef>(key_data);
	if ( !policyVerifyPeersSet.get()->set_key_data( (const uint8_t*)&key_data[0], key_data.size(), passphrase) )
		throw tls_error();
	if (!policyVerifyPeersNotSet.get()->set_key_data((const uint8_t*)&key_data[0], key_data.size(), passphrase))
		throw tls_error();

	key_set = true;
}

void TLSOptions::set_verify_peers( std::vector<std::string> const& verify_peers ) {
	if (!policyVerifyPeersSet.get())
		init_plugin();
	{
		TraceEvent e("TLSConnectionSettingVerifyPeers");
		for (int i = 0; i < verify_peers.size(); i++)
			e.detail(std::string("Value" + std::to_string(i)).c_str(), verify_peers[i].c_str());
	}
	std::unique_ptr<const uint8_t *[]> verify_peers_arr(new const uint8_t*[verify_peers.size()]);
	std::unique_ptr<int[]> verify_peers_len(new int[verify_peers.size()]);
	for (int i = 0; i < verify_peers.size(); i++) {
		verify_peers_arr[i] = (const uint8_t *)&verify_peers[i][0];
		verify_peers_len[i] = verify_peers[i].size();
	}

	if (!policyVerifyPeersSet.get()->set_verify_peers(verify_peers.size(), verify_peers_arr.get(), verify_peers_len.get()))
		throw tls_error();

	policyInfo.verify_peers = verify_peers;
	verify_peers_set = true;
}

void TLSOptions::register_network() {
	// Simulation relies upon being able to call this multiple times, and have it override g_network
	// each time it's called.
	new TLSNetworkConnections( Reference<TLSOptions>::addRef( this ) );
}

ACTOR static Future<ErrorOr<Standalone<StringRef>>> readEntireFile( std::string filename ) {
	state Reference<IAsyncFile> file = wait(IAsyncFileSystem::filesystem()->open(filename, IAsyncFile::OPEN_READONLY | IAsyncFile::OPEN_UNCACHED, 0));
	state int64_t filesize = wait(file->size());
	state Standalone<StringRef> buf = makeString(filesize);
	int rc = wait(file->read(mutateString(buf), filesize, 0));
	if (rc != filesize) {
		// File modified during read, probably.  The mtime should change, and thus we'll be called again.
		return tls_error();
	}
	return buf;
}

ACTOR static Future<Void> watchFileForChanges( std::string filename, AsyncVar<Standalone<StringRef>> *contents_var ) {
	state std::time_t lastModTime = wait(IAsyncFileSystem::filesystem()->lastWriteTime(filename));
	loop {
		wait(delay(FLOW_KNOBS->TLS_CERT_REFRESH_DELAY_SECONDS));
		std::time_t modtime = wait(IAsyncFileSystem::filesystem()->lastWriteTime(filename));
		if (lastModTime != modtime) {
			lastModTime = modtime;
			ErrorOr<Standalone<StringRef>> contents = wait(readEntireFile(filename));
			if (contents.present()) {
				contents_var->set(contents.get());
			}
		}
	}
}

ACTOR static Future<Void> reloadConfigurationOnChange( TLSOptions::PolicyInfo *pci, Reference<ITLSPlugin> plugin, AsyncVar<Reference<ITLSPolicy>> *realVerifyPeersPolicy, AsyncVar<Reference<ITLSPolicy>> *realNoVerifyPeersPolicy ) {
	if (FLOW_KNOBS->TLS_CERT_REFRESH_DELAY_SECONDS <= 0) {
		return Void();
	}
	loop {
		// Early in bootup, the filesystem might not be initialized yet.  Wait until it is.
		if (IAsyncFileSystem::filesystem() != nullptr) {
			break;
		}
		wait(delay(1.0));
	}
	state int mismatches = 0;
	state AsyncVar<Standalone<StringRef>> ca_var;
	state AsyncVar<Standalone<StringRef>> key_var;
	state AsyncVar<Standalone<StringRef>> cert_var;
	state std::vector<Future<Void>> lifetimes;
	if (!pci->ca_path.empty()) lifetimes.push_back(watchFileForChanges(pci->ca_path, &ca_var));
	if (!pci->key_path.empty()) lifetimes.push_back(watchFileForChanges(pci->key_path, &key_var));
	if (!pci->cert_path.empty()) lifetimes.push_back(watchFileForChanges(pci->cert_path, &cert_var));
	loop {
		state Future<Void> ca_changed = ca_var.onChange();
		state Future<Void> key_changed = key_var.onChange();
		state Future<Void> cert_changed = cert_var.onChange();
		wait( ca_changed || key_changed || cert_changed );
		if (ca_changed.isReady()) {
			TraceEvent(SevInfo, "TLSRefreshCAChanged").detail("path", pci->ca_path).detail("length", ca_var.get().size());
			pci->ca_contents = ca_var.get();
		}
		if (key_changed.isReady()) {
			TraceEvent(SevInfo, "TLSRefreshKeyChanged").detail("path", pci->key_path).detail("length", key_var.get().size());
			pci->key_contents = key_var.get();
		}
		if (cert_changed.isReady()) {
			TraceEvent(SevInfo, "TLSRefreshCertChanged").detail("path", pci->cert_path).detail("length", cert_var.get().size());
			pci->cert_contents = cert_var.get();
		}
		bool rc = true;
		Reference<ITLSPolicy> verifypeers = Reference<ITLSPolicy>(plugin->create_policy());
		Reference<ITLSPolicy> noverifypeers = Reference<ITLSPolicy>(plugin->create_policy());
		loop {
			// Don't actually loop.  We're just using loop/break as a `goto err`.
			// This loop always ends with an unconditional break.
			rc = verifypeers->set_ca_data(pci->ca_contents.begin(), pci->ca_contents.size());
			if (!rc) break;
			rc = verifypeers->set_key_data(pci->key_contents.begin(), pci->key_contents.size(), pci->keyPassword.c_str());
			if (!rc) break;
			rc = verifypeers->set_cert_data(pci->cert_contents.begin(), pci->cert_contents.size());
			if (!rc) break;
			{
				std::unique_ptr<const uint8_t *[]> verify_peers_arr(new const uint8_t*[pci->verify_peers.size()]);
				std::unique_ptr<int[]> verify_peers_len(new int[pci->verify_peers.size()]);
				for (int i = 0; i < pci->verify_peers.size(); i++) {
					verify_peers_arr[i] = (const uint8_t *)&pci->verify_peers[i][0];
					verify_peers_len[i] = pci->verify_peers[i].size();
				}
				rc = verifypeers->set_verify_peers(pci->verify_peers.size(), verify_peers_arr.get(), verify_peers_len.get());
				if (!rc) break;
			}
			rc = noverifypeers->set_ca_data(pci->ca_contents.begin(), pci->ca_contents.size());
			if (!rc) break;
			rc = noverifypeers->set_key_data(pci->key_contents.begin(), pci->key_contents.size(), pci->keyPassword.c_str());
			if (!rc) break;
			rc = noverifypeers->set_cert_data(pci->cert_contents.begin(), pci->cert_contents.size());
			if (!rc) break;
			break;
		}

		if (rc) {
			TraceEvent(SevInfo, "TLSCertificateRefreshSucceeded");
			realVerifyPeersPolicy->set(verifypeers);
			realNoVerifyPeersPolicy->set(noverifypeers);
			mismatches = 0;
		} else {
			// Some files didn't match up, they should in the future, and we'll retry then.
			mismatches++;
			TraceEvent(SevWarn, "TLSCertificateRefreshMismatch").detail("mismatches", mismatches);
		}
	}
}

const char *defaultCertFileName = "fdb.pem";

Reference<ITLSPolicy> TLSOptions::get_policy(PolicyType type) {
	if ( !certs_set ) {
		if ( !platform::getEnvironmentVar( "FDB_TLS_CERTIFICATE_FILE", policyInfo.cert_path ) )
			policyInfo.cert_path = fileExists(defaultCertFileName) ? defaultCertFileName : joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
		set_cert_file( policyInfo.cert_path );
	}
	if ( !key_set ) {
		if ( policyInfo.keyPassword.empty() )
			platform::getEnvironmentVar( "FDB_TLS_PASSWORD", policyInfo.keyPassword );
		if ( !platform::getEnvironmentVar( "FDB_TLS_KEY_FILE", policyInfo.key_path ) )
			policyInfo.key_path = fileExists(defaultCertFileName) ? defaultCertFileName : joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
		set_key_file( policyInfo.key_path );
	}
	if( !verify_peers_set ) {
		std::string verify_peers;
		if (platform::getEnvironmentVar("FDB_TLS_VERIFY_PEERS", verify_peers))
			set_verify_peers({ verify_peers });
		else
			set_verify_peers({ std::string("Check.Valid=1")});
	}
	if (!ca_set) {
		if (platform::getEnvironmentVar("FDB_TLS_CA_FILE", policyInfo.ca_path))
			set_ca_file(policyInfo.ca_path);
	}

	if (!configurationReloader.present()) {
		configurationReloader = reloadConfigurationOnChange(&policyInfo, plugin, &policyVerifyPeersSet, &policyVerifyPeersNotSet);
	}

	Reference<ITLSPolicy> policy;
	switch (type) {
	case POLICY_VERIFY_PEERS:
		policy = policyVerifyPeersSet.get();
		break;
	case POLICY_NO_VERIFY_PEERS:
		policy = policyVerifyPeersNotSet.get();
		break;
	default:
		ASSERT_ABORT(0);
	}
	return policy;
}

void TLSOptions::init_plugin() {

	TraceEvent("TLSConnectionLoadingPlugin").detail("Plugin", tlsPluginName);

	plugin = loadPlugin<ITLSPlugin>( tlsPluginName );

	if ( !plugin ) {
		TraceEvent(SevError, "TLSConnectionPluginInitError").detail("Plugin", tlsPluginName).GetLastError();
		throw tls_error();
	}

	policyVerifyPeersSet = AsyncVar<Reference<ITLSPolicy>>(Reference<ITLSPolicy>(plugin->create_policy()));
	if ( !policyVerifyPeersSet.get()) {
		// Hopefully create_policy logged something with the log func
		TraceEvent(SevError, "TLSConnectionCreatePolicyVerifyPeersSetError");
		throw tls_error();
	}

	policyVerifyPeersNotSet = AsyncVar<Reference<ITLSPolicy>>(Reference<ITLSPolicy>(plugin->create_policy()));
	if (!policyVerifyPeersNotSet.get()) {
		// Hopefully create_policy logged something with the log func
		TraceEvent(SevError, "TLSConnectionCreatePolicyVerifyPeersNotSetError");
		throw tls_error();
	}
}

bool TLSOptions::enabled() {
	return policyVerifyPeersSet.get().isValid() && policyVerifyPeersNotSet.get().isValid();
}
