/*
 * TLSHandshakeWorkerTest.cpp
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

// Native tests for TLSHandshakeWorker: real OpenSSL handshakes over socketpairs (no listening ports needed). Simulation
// fakes TLS handshakes, so these run only natively ("noSim/").

#include "flow/TLSHandshakeWorker.h"

#ifndef _WIN32

#include <sys/socket.h>

#include "flow/MkCert.h"
#include "flow/Platform.h"
#include "flow/UnitTest.h"

namespace {

using boost::asio::ip::tcp;
namespace ssl = boost::asio::ssl;

// A handshake's result: success, or the job's error.
using Outcome = Future<boost::system::error_code>;

// Longer than any test runs, for handshakes that must not time out.
constexpr double kNoTimeout = 30.0;

struct TestCreds {
	std::string certChain;
	std::string key;
	std::string caCert;
};

TestCreds makeTestCreds(mkcert::ESide side) {
	Arena arena;
	auto chain = mkcert::makeCertChain(arena, mkcert::makeCertChainSpec(arena, 3, side), {});
	auto nonRoot = chain;
	nonRoot.pop_back();
	return TestCreds{ mkcert::concatCertChain(arena, nonRoot).toString(),
		              chain.front().privateKeyPem.toString(),
		              chain.back().certPem.toString() };
}

std::unique_ptr<ssl::context> makeContext(const TestCreds& own, const std::string& trustedCa) {
	auto ctx = std::make_unique<ssl::context>(ssl::context::tls);
	ctx->use_certificate_chain(boost::asio::buffer(own.certChain));
	ctx->use_private_key(boost::asio::buffer(own.key), ssl::context::pem);
	ctx->add_certificate_authority(boost::asio::buffer(trustedCa));
	ctx->set_verify_mode(ssl::verify_peer | ssl::verify_fail_if_no_peer_cert);
	return ctx;
}

// Two connected, non-blocking stream sockets, one TLS stream per end. The client end can stay silent.
struct TestPair {
	boost::asio::io_context ioc;
	tcp::socket serverSocket{ ioc };
	tcp::socket clientSocket{ ioc };
	std::unique_ptr<TLSHandshakeSocket> server;
	std::unique_ptr<TLSHandshakeSocket> client;

	TestPair(ssl::context& serverCtx, ssl::context& clientCtx) {
		int fds[2];
		ASSERT(::socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
		serverSocket.assign(tcp::v4(), fds[0]);
		clientSocket.assign(tcp::v4(), fds[1]);
		serverSocket.non_blocking(true);
		clientSocket.non_blocking(true);
		server = std::make_unique<TLSHandshakeSocket>(serverSocket, serverCtx);
		client = std::make_unique<TLSHandshakeSocket>(clientSocket, clientCtx);
	}

	std::shared_ptr<TLSHandshakeJob> serverJob(double timeout) {
		return std::make_shared<TLSHandshakeJob>(*server, TLSHandshakeSocket::server, NetworkAddress(), timeout);
	}
	std::shared_ptr<TLSHandshakeJob> clientJob(double timeout) {
		return std::make_shared<TLSHandshakeJob>(*client, TLSHandshakeSocket::client, NetworkAddress(), timeout);
	}
};

struct TestTLS {
	TestCreds serverCreds = makeTestCreds(mkcert::ESide::Server);
	TestCreds clientCreds = makeTestCreds(mkcert::ESide::Client);
	std::unique_ptr<ssl::context> serverCtx = makeContext(serverCreds, clientCreds.caCert);
	std::unique_ptr<ssl::context> clientCtx = makeContext(clientCreds, serverCreds.caCert);
};

// Waits for the job and returns its error, or success. Call before submitting the job: the future must be taken on
// this thread.
Outcome outcome(std::shared_ptr<TLSHandshakeJob> job) {
	try {
		co_await job->done.getFuture();
		co_return boost::system::error_code();
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		ASSERT_EQ(e.code(), error_code_connection_failed);
		co_return job->error;
	}
}

// Starts both ends of `pair`'s handshake, each on the given worker. Returns the server's and the client's outcome.
std::pair<Outcome, Outcome> startHandshake(TestPair& pair,
                                           TLSHandshakeWorker& serverWorker,
                                           TLSHandshakeWorker& clientWorker) {
	auto serverJob = pair.serverJob(kNoTimeout);
	auto clientJob = pair.clientJob(kNoTimeout);
	std::pair<Outcome, Outcome> outcomes(outcome(serverJob), outcome(clientJob));
	serverWorker.submitJob(serverJob);
	clientWorker.submitJob(clientJob);
	return outcomes;
}

} // namespace

TEST_CASE("noSim/flow/TLSHandshakeWorker/Success") {
	noUnseed = true;
	TestTLS tls;
	TestPair pair(*tls.serverCtx, *tls.clientCtx);
	TLSHandshakeWorker serverWorker, clientWorker;

	auto [serverResult, clientResult] = startHandshake(pair, serverWorker, clientWorker);
	ASSERT(!(co_await serverResult));
	ASSERT(!(co_await clientResult));
	ASSERT_EQ(serverWorker.activeJobCount(), 0);
	ASSERT_EQ(clientWorker.activeJobCount(), 0);
	co_return;
}

TEST_CASE("noSim/flow/TLSHandshakeWorker/SilentPeerTimesOut") {
	noUnseed = true;
	TestTLS tls;
	TestPair pair(*tls.serverCtx, *tls.clientCtx);
	TLSHandshakeWorker worker;

	double start = timer_monotonic();
	auto job = pair.serverJob(0.3);
	Outcome result = outcome(job);
	worker.submitJob(job);

	boost::system::error_code ec = co_await result;
	double elapsed = timer_monotonic() - start;
	ASSERT(ec == boost::asio::error::timed_out);
	ASSERT(elapsed >= 0.25 && elapsed < 5.0);
	// The worker leaves the socket to its owner.
	ASSERT(pair.serverSocket.is_open());
	co_return;
}

// The property the blocking handshake pool lacks: silent peers must not delay other handshakes on the same thread.
TEST_CASE("noSim/flow/TLSHandshakeWorker/SilentPeersDoNotBlockOthers") {
	noUnseed = true;
	// Two descriptors per peer; stay well under macOS's default limit of 256 open files.
	constexpr int silentPeers = 32;
	TestTLS tls;
	TestPair healthy(*tls.serverCtx, *tls.clientCtx);
	std::vector<std::unique_ptr<TestPair>> silent;
	std::vector<std::shared_ptr<TLSHandshakeJob>> silentJobs;
	std::vector<Outcome> silentResults;
	// Declared after the sockets so the workers stop, abandoning any handshakes, before the sockets are destroyed.
	TLSHandshakeWorker worker, clientWorker;

	for (int i = 0; i < silentPeers; ++i) {
		silent.push_back(std::make_unique<TestPair>(*tls.serverCtx, *tls.clientCtx));
		silentJobs.push_back(silent.back()->serverJob(kNoTimeout));
		silentResults.push_back(outcome(silentJobs.back()));
		worker.submitJob(silentJobs.back());
	}

	double start = timer_monotonic();
	auto [serverResult, clientResult] = startHandshake(healthy, worker, clientWorker);
	ASSERT(!(co_await serverResult));
	ASSERT(!(co_await clientResult));
	ASSERT(timer_monotonic() - start < 10.0);
	for (auto& r : silentResults) {
		ASSERT(!r.isReady());
	}
	ASSERT_EQ(worker.activeJobCount(), silentPeers);

	for (auto& job : silentJobs) {
		worker.cancelJob(*job);
	}
	for (auto& r : silentResults) {
		ASSERT(co_await r == boost::asio::error::operation_aborted);
	}
	ASSERT_EQ(worker.activeJobCount(), 0);
	co_return;
}

TEST_CASE("noSim/flow/TLSHandshakeWorker/Cancel") {
	noUnseed = true;
	TestTLS tls;
	TestPair pair(*tls.serverCtx, *tls.clientCtx);
	TLSHandshakeWorker worker;

	auto job = pair.serverJob(kNoTimeout);
	Outcome result = outcome(job);
	worker.submitJob(job);
	co_await delay(0.1);
	ASSERT(!result.isReady());

	double start = timer_monotonic();
	worker.cancelJob(*job);
	ASSERT(co_await result == boost::asio::error::operation_aborted);
	ASSERT(timer_monotonic() - start < 5.0);
	ASSERT(pair.serverSocket.is_open());
	co_return;
}

TEST_CASE("noSim/flow/TLSHandshakeWorker/UntrustedPeerFailsFast") {
	noUnseed = true;
	TestTLS tls;
	// A client whose chain is signed by a CA the server does not trust.
	TestCreds strangerCreds = makeTestCreds(mkcert::ESide::Client);
	auto strangerCtx = makeContext(strangerCreds, tls.serverCreds.caCert);
	TestPair pair(*tls.serverCtx, *strangerCtx);
	TLSHandshakeWorker serverWorker, clientWorker;

	double start = timer_monotonic();
	auto [serverResult, clientResult] = startHandshake(pair, serverWorker, clientWorker);
	boost::system::error_code ec = co_await serverResult;
	ASSERT(ec);
	ASSERT(ec.category() == boost::asio::error::get_ssl_category());
	ASSERT(timer_monotonic() - start < 10.0);
	// In TLS 1.3 the client finishes its side before the server checks the client certificate, so the client may
	// report success; it only has to finish too.
	co_await clientResult;
	co_return;
}

#endif // _WIN32
