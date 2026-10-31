/*
 * TlsHandshakePoolTest.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

// Checks that TLS peers which connect but never complete a handshake do not starve Net2's handshake capacity:
// - while such silent peers are pending, healthy handshakes still succeed, and none runs on the network thread;
// - silent peers are disconnected at the handshake timeout (CONNECTION_MONITOR_TIMEOUT);
// - healthy handshakes still succeed afterwards.
// There are as many silent peers as handshake threads, so the first check passes only if a handshake thread keeps
// serving other handshakes while a silent one is pending on it, i.e. only if the handshake workers do not block.
//
// The server is production code: a Net2 TLS listener and SSLConnection::acceptHandshake(), the calls FlowTransport
// makes for every incoming connection, over real sockets and OpenSSL. FlowTransport itself is not involved, and the
// clients are plain Boost.Asio. Simulation fakes TLS handshakes, so this can only be checked natively.
//
// `--legacy` runs with the non-blocking handshake workers off, to show the old behavior. It is expected to fail: the
// silent peers tie up both blocking handshake threads, so healthy handshakes fall back to the network thread, and the
// silent peers are not disconnected at the timeout because their threads stay blocked reading from them.

#include <chrono>
#include <iostream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <fmt/core.h>

#include "flow/Arena.h"
#include "flow/flow.h"
#include "flow/IConnection.h"
#include "flow/Knobs.h"
#include "flow/MkCert.h"
#include "flow/SimpleCounter.h"
#include "flow/TLSConfig.h"
#include "flow/network.h"

namespace {

using boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;

constexpr int kHandshakeThreads = 2;
// As many silent peers as handshake threads: enough to tie up every blocking handshake thread.
constexpr int kSilentPeers = kHandshakeThreads;
constexpr int kHealthyHandshakes = 5;

// A self-signed certificate, so it is also its own CA.
struct Creds {
	std::string cert;
	std::string key;
};

Creds makeCreds() {
	Arena arena;
	auto cert = mkcert::makeCertChain(arena, 1, mkcert::ESide::Server).front();
	return Creds{ cert.certPem.toString(), cert.privateKeyPem.toString() };
}

int64_t mainThreadHandshakes() {
	int64_t total = 0;
	for (auto* counter : SimpleCounter<int64_t>::getCounters()) {
		if (counter->name() == "/Net2/TLS/ServerTLSHandshakesOnMainThread") {
			total += counter->get();
		}
	}
	return total;
}

// Runs one TLS client handshake with a deadline. Returns true on success.
bool healthyHandshake(const Creds& creds, const tcp::endpoint& server) {
	boost::asio::io_context ioc;
	ssl::context ctx(ssl::context::tls_client);
	ctx.use_certificate_chain(boost::asio::buffer(creds.cert));
	ctx.use_private_key(boost::asio::buffer(creds.key), ssl::context::pem);
	ctx.set_verify_mode(ssl::verify_none);
	ssl::stream<tcp::socket> stream(ioc, ctx);

	boost::system::error_code ec;
	// NOLINTNEXTLINE(bugprone-unused-return-value): the error is reported through ec.
	stream.next_layer().connect(server, ec);
	if (ec) {
		return false;
	}
	bool ok = false;
	boost::asio::steady_timer deadline(ioc, std::chrono::seconds(5));
	stream.async_handshake(ssl::stream_base::client, [&](const boost::system::error_code& e) {
		ok = !e;
		deadline.cancel();
	});
	deadline.async_wait([&](const boost::system::error_code& e) {
		if (!e) {
			stream.next_layer().close();
		}
	});
	ioc.run();
	return ok;
}

// Runs kHealthyHandshakes handshakes and reports them. Passes if all succeed and none ran on the network thread.
bool runHealthyPhase(const char* phase, const Creds& creds, const tcp::endpoint& server) {
	int64_t before = mainThreadHandshakes();
	int succeeded = 0;
	for (int i = 0; i < kHealthyHandshakes; ++i) {
		succeeded += healthyHandshake(creds, server);
	}
	int64_t onNetworkThread = mainThreadHandshakes() - before;
	std::cout << fmt::format("{}: {}/{} healthy handshakes succeeded, {} ran on the network thread\n",
	                         phase,
	                         succeeded,
	                         kHealthyHandshakes,
	                         onNetworkThread);
	return succeeded == kHealthyHandshakes && onNetworkThread == 0;
}

// True if the server has closed (or reset) the connection.
bool closedByServer(tcp::socket& socket) {
	boost::system::error_code ec;
	// NOLINTNEXTLINE(bugprone-unused-return-value): the error is reported through ec.
	socket.non_blocking(true, ec);
	char byte;
	socket.read_some(boost::asio::buffer(&byte, 1), ec);
	return ec == boost::asio::error::eof || ec == boost::asio::error::connection_reset;
}

// Starts the TLS handshake of every accepted connection without waiting for it, so silent and healthy handshakes are in
// progress at the same time.
Future<Void> acceptLoop(Reference<IListener> listener, std::vector<Future<Void>>* handshakes) {
	while (true) {
		Reference<IConnection> conn = co_await listener->accept();
		// The future keeps the connection alive; a failed handshake just leaves its error in it.
		handshakes->push_back(conn->acceptHandshake());
	}
}

// Server: starts Net2 as a TLS server listening on a free loopback port. Clients present the same self-signed
// certificate, so it is also the CA.
Reference<IListener> startServer(const Creds& creds) {
	TLSConfig tlsConfig(TLSEndpointType::SERVER);
	tlsConfig.setCertificateBytes(creds.cert);
	tlsConfig.setKeyBytes(creds.key);
	tlsConfig.setCABytes(creds.cert);
	g_network = newNet2(tlsConfig);
	openTraceFile({}, 10 << 20, 10 << 20, ".", "tls_handshake_pool_test");
	return INetworkConnections::net()->listen(NetworkAddress::parse("127.0.0.1:0:tls"));
}

// Server: accepts connections and starts each one's TLS handshake, which Net2 runs where the knobs direct it. Runs
// Net2's network thread on the calling thread until g_network->stop().
void runServer(Reference<IListener> listener) {
	std::vector<Future<Void>> handshakes;
	Future<Void> accepting = acceptLoop(listener, &handshakes);
	g_network->run();
}

// Client: opens the silent peers, then checks that healthy handshakes succeed while they are pending, that the server
// disconnects them at the handshake timeout, and that healthy handshakes succeed after it. Returns true if all pass.
bool runClient(const Creds& creds, const tcp::endpoint& server) {
	boost::asio::io_context ioc;
	std::vector<tcp::socket> silent;
	for (int i = 0; i < kSilentPeers; ++i) {
		silent.emplace_back(ioc).connect(server);
	}
	// Let the server accept the silent peers and start their handshakes.
	std::this_thread::sleep_for(std::chrono::milliseconds(500));
	bool whileSilentPending = runHealthyPhase("While silent peers are pending", creds, server);

	std::this_thread::sleep_for(std::chrono::duration<double>(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT + 1.5));
	int silentClosed = 0;
	for (auto& socket : silent) {
		silentClosed += closedByServer(socket);
	}
	std::cout << fmt::format(
	    "Silent peers disconnected after the handshake timeout: {}/{}\n", silentClosed, kSilentPeers);
	bool afterTimeout = runHealthyPhase("After the handshake timeout", creds, server);

	return whileSilentPending && afterTimeout && silentClosed == kSilentPeers;
}

// Must run before startServer(): Net2 reads these when it creates its handshake threads.
void setKnobs(bool legacy) {
	bootstrapGlobalFlowKnobs.TLS_SERVER_HANDSHAKE_THREADS = kHandshakeThreads;
	bootstrapGlobalFlowKnobs.TLS_HANDSHAKE_NONBLOCKING_WORKERS = !legacy;
	bootstrapGlobalFlowKnobs.DISABLE_MAINTHREAD_TLS_HANDSHAKE = !legacy;
	// Silent and healthy peers share 127.0.0.1, so per-IP throttling would refuse the healthy ones after the silent
	// ones time out. Disable it here to observe handshake capacity alone.
	bootstrapGlobalFlowKnobs.TLS_SERVER_CONNECTION_THROTTLE_ATTEMPTS = 1000000;
}

bool runTest(bool legacy) {
	setKnobs(legacy);
	Creds creds = makeCreds();
	Reference<IListener> listener = startServer(creds);
	tcp::endpoint server(boost::asio::ip::make_address("127.0.0.1"), listener->getListenAddress().port);

	// The client uses blocking sockets, so it runs on its own thread while the server runs on this one.
	bool ok = false;
	std::thread client([&] {
		ok = runClient(creds, server);
		g_network->stop();
	});
	runServer(listener);
	client.join();
	return ok;
}

} // namespace

int main(int argc, char** argv) {
	bool legacy = argc == 2 && std::string_view(argv[1]) == "--legacy";
	if (argc > 1 && !legacy) {
		std::cerr << "Usage: " << argv[0] << " [--legacy]\n";
		return 2;
	}
	if (legacy) {
		std::cout << "Legacy behavior: non-blocking handshake workers off\n";
	}

	int rc = 1;
	try {
		bool ok = runTest(legacy);
		std::cout << (ok ? "Test OK\n" : "Test FAILED\n");
		rc = ok ? 0 : 1;
	} catch (Error& e) {
		std::cerr << "Unexpected error: " << e.name() << " (" << e.code() << ")\n";
	}
	// Skip static destruction; the network and its handshake threads are still alive.
	flushTraceFileVoid();
	_exit(rc);
}
