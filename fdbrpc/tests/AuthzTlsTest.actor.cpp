/*
 * AuthzTlsTest.cpp
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

#ifndef _WIN32
#include <algorithm>
#include <cstring>
#include <fmt/format.h>
#include <unistd.h>
#include <string_view>
#include <signal.h>
#include <sys/wait.h>
#include "flow/Arena.h"
#include "flow/MkCert.h"
#include "flow/ScopeExit.h"
#include "flow/TLSConfig.actor.h"
#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowTransport.h"
#include "flow/actorcompiler.h" // This must be the last #include.

std::FILE* outp = stdout;

template <class... Args>
void log(Args&&... args) {
	auto buf = fmt::memory_buffer{};
	fmt::format_to(std::back_inserter(buf), std::forward<Args>(args)...);
	fmt::print(outp, "{}\n", std::string_view(buf.data(), buf.size()));
}

template <class... Args>
void logc(Args&&... args) {
	auto buf = fmt::memory_buffer{};
	fmt::format_to(std::back_inserter(buf), "[CLIENT] ");
	fmt::format_to(std::back_inserter(buf), std::forward<Args>(args)...);
	fmt::print(outp, "{}\n", std::string_view(buf.data(), buf.size()));
}

template <class... Args>
void logs(Args&&... args) {
	auto buf = fmt::memory_buffer{};
	fmt::format_to(std::back_inserter(buf), "[SERVER] ");
	fmt::format_to(std::back_inserter(buf), std::forward<Args>(args)...);
	fmt::print(outp, "{}\n", std::string_view(buf.data(), buf.size()));
}

template <class... Args>
void logm(Args&&... args) {
	auto buf = fmt::memory_buffer{};
	fmt::format_to(std::back_inserter(buf), "[ MAIN ] ");
	fmt::format_to(std::back_inserter(buf), std::forward<Args>(args)...);
	fmt::print(outp, "{}\n", std::string_view(buf.data(), buf.size()));
}

struct TLSCreds {
	std::string certBytes;
	std::string keyBytes;
	std::string caBytes;
};

TLSCreds makeCreds(int chainLen, mkcert::ESide side) {
	if (chainLen == 0)
		return {};
	auto arena = Arena();
	auto ret = TLSCreds{};
	auto specs = mkcert::makeCertChainSpec(arena, std::labs(chainLen), side);
	if (chainLen < 0) {
		specs[0].offsetNotBefore = -60l * 60 * 24 * 365;
		specs[0].offsetNotAfter = -10l; // cert that expired 10 seconds ago
	}
	auto chain = mkcert::makeCertChain(arena, specs, {} /* create root CA cert from spec*/);
	if (chain.size() == 1) {
		ret.certBytes = concatCertChain(arena, chain).toString();
	} else {
		auto nonRootChain = chain;
		nonRootChain.pop_back();
		ret.certBytes = concatCertChain(arena, nonRootChain).toString();
	}
	ret.caBytes = chain.back().certPem.toString();
	ret.keyBytes = chain.front().privateKeyPem.toString();
	return ret;
}

enum class Result : int {
	TRUSTED = 0,
	UNTRUSTED,
	ERROR,
};

template <>
struct fmt::formatter<Result> {
	constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

	template <class FormatContext>
	auto format(const Result& r, FormatContext& ctx) -> decltype(ctx.out()) {
		if (r == Result::TRUSTED)
			return fmt::format_to(ctx.out(), "TRUSTED");
		else if (r == Result::UNTRUSTED)
			return fmt::format_to(ctx.out(), "UNTRUSTED");
		else
			return fmt::format_to(ctx.out(), "ERROR");
	}
};

ACTOR template <class T>
Future<T> stopNetworkAfter(Future<T> what) {
	T t = wait(what);
	g_network->stop();
	return t;
}

// Reflective struct containing information about the requester from a server PoV
struct SessionInfo {
	constexpr static FileIdentifier file_identifier = 1578312;
	bool isPeerTrusted = false;
	NetworkAddress peerAddress;

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, isPeerTrusted, peerAddress);
	}
};

struct SessionProbeRequest {
	constexpr static FileIdentifier file_identifier = 1559713;
	ReplyPromise<SessionInfo> reply{ PeerCompatibilityPolicy{ RequirePeer::AtLeast,
		                                                      ProtocolVersion::withStableInterfaces() } };

	bool verify() const { return true; }

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, reply);
	}
};

struct SessionProbeReceiver final : NetworkMessageReceiver {
	SessionProbeReceiver() {}
	void receive(ArenaObjectReader& reader) override {
		SessionProbeRequest req;
		reader.deserialize(req);
		SessionInfo res;
		res.isPeerTrusted = FlowTransport::transport().currentDeliveryPeerIsTrusted();
		res.peerAddress = FlowTransport::transport().currentDeliveryPeerAddress();
		req.reply.send(res);
	}
	PeerCompatibilityPolicy peerCompatibilityPolicy() const override {
		return PeerCompatibilityPolicy{ RequirePeer::AtLeast, ProtocolVersion::withStableInterfaces() };
	}
	bool isPublic() const override { return true; }
};

Future<Void> runServer(Future<Void> listenFuture, const Endpoint& endpoint, int addrPipe, int completionPipe) {
	auto realAddr = FlowTransport::transport().getLocalAddresses().address;
	logs("Listening at {}", realAddr.toString());
	logs("Endpoint token is {}", endpoint.token.toString());
	// below writes/reads would block, but this is good enough for a test.
	if (sizeof(realAddr) != ::write(addrPipe, &realAddr, sizeof(realAddr))) {
		logs("Failed to write server addr to pipe: {}", strerror(errno));
		return Void();
	}
	if (sizeof(endpoint.token) != ::write(addrPipe, &endpoint.token, sizeof(endpoint.token))) {
		logs("Failed to write server endpoint to pipe: {}", strerror(errno));
		return Void();
	}
	auto done = false;
	if (sizeof(done) != ::read(completionPipe, &done, sizeof(done))) {
		logs("Failed to read completion flag from pipe: {}", strerror(errno));
		return Void();
	}
	return Void();
}

ACTOR Future<Void> waitAndPrintResponse(Future<SessionInfo> response, Result* rc) {
	try {
		SessionInfo info = wait(response);
		logc("Probe response: trusted={} peerAddress={}", info.isPeerTrusted, info.peerAddress.toString());
		*rc = info.isPeerTrusted ? Result::TRUSTED : Result::UNTRUSTED;
	} catch (Error& err) {
		logc("Error: {}", err.what());
		*rc = Result::ERROR;
	}
	return Void();
}

template <bool IsServer>
int runHost(TLSCreds creds, int addrPipe, int completionPipe, Result expect) {
	auto tlsConfig = TLSConfig(IsServer ? TLSEndpointType::SERVER : TLSEndpointType::CLIENT);
	tlsConfig.setCertificateBytes(creds.certBytes);
	tlsConfig.setCABytes(creds.caBytes);
	tlsConfig.setKeyBytes(creds.keyBytes);
	g_network = newNet2(tlsConfig);
	openTraceFile(NetworkAddress(),
	              10 << 20,
	              10 << 20,
	              ".",
	              IsServer ? "authz_tls_unittest_server" : "authz_tls_unittest_client");
	FlowTransport::createInstance(!IsServer, 1, WLTOKEN_RESERVED_COUNT);
	auto& transport = FlowTransport::transport();
	if constexpr (IsServer) {
		auto addr = NetworkAddress::parse("127.0.0.1:0:tls");
		auto thread = std::thread([]() {
			g_network->run();
			flushTraceFileVoid();
		});
		auto endpoint = Endpoint();
		auto receiver = SessionProbeReceiver();
		transport.addEndpoint(endpoint, &receiver, TaskPriority::ReadSocket);
		runServer(transport.bind(addr, addr), endpoint, addrPipe, completionPipe);
		auto cleanupGuard = ScopeExit([&thread]() {
			g_network->stop();
			thread.join();
		});
	} else {
		auto dest = Endpoint();
		auto& serverAddr = dest.addresses.address;
		if (sizeof(serverAddr) != ::read(addrPipe, &serverAddr, sizeof(serverAddr))) {
			logc("Failed to read server addr from pipe: {}", strerror(errno));
			return 1;
		}
		auto& token = dest.token;
		if (sizeof(token) != ::read(addrPipe, &token, sizeof(token))) {
			logc("Failed to read server endpoint token from pipe: {}", strerror(errno));
			return 2;
		}
		logc("Server address is {}", serverAddr.toString());
		logc("Server endpoint token is {}", token.toString());
		auto sessionProbeReq = SessionProbeRequest{};
		transport.sendUnreliable(SerializeSource(sessionProbeReq), dest, true /*openConnection*/);
		logc("Request is sent");
		auto probeResponse = sessionProbeReq.reply.getFuture();
		auto result = Result::TRUSTED;
		auto timeout = delay(5);
		auto complete = waitAndPrintResponse(probeResponse, &result);
		auto f = stopNetworkAfter(complete || timeout);
		auto rc = 0;
		g_network->run();
		if (!complete.isReady()) {
			logc("Error: Probe request timed out");
			rc = 3;
		}
		auto done = true;
		if (sizeof(done) != ::write(completionPipe, &done, sizeof(done))) {
			logc("Failed to signal server to terminate: {}", strerror(errno));
			rc = 4;
		}
		if (rc == 0) {
			if (expect != result) {
				logc("Test failed: expected {}, got {}", expect, result);
				rc = 5;
			} else {
				logc("Response OK: got {} as expected", result);
			}
		}
		return rc;
	}
	return 0;
}

int runTlsTest(int serverChainLen, int clientChainLen) {
	log("==== BEGIN TESTCASE ====");
	auto expect = Result::ERROR;
	if (serverChainLen > 0) {
		if (clientChainLen > 0)
			expect = Result::TRUSTED;
		else if (clientChainLen == 0)
			expect = Result::UNTRUSTED;
	}
	log("Cert chain length: server={} client={}", serverChainLen, clientChainLen);
	auto arena = Arena();
	auto serverCreds = makeCreds(serverChainLen, mkcert::ESide::Server);
	auto clientCreds = makeCreds(clientChainLen, mkcert::ESide::Client);
	// make server and client trust each other
	std::swap(serverCreds.caBytes, clientCreds.caBytes);
	auto clientPid = pid_t{};
	auto serverPid = pid_t{};
	int addrPipe[2];
	int completionPipe[2];
	if (::pipe(addrPipe) || ::pipe(completionPipe)) {
		logm("Pipe open failed: {}", strerror(errno));
		return 1;
	}
	auto pipeCleanup = ScopeExit([&addrPipe, &completionPipe]() {
		::close(addrPipe[0]);
		::close(addrPipe[1]);
		::close(completionPipe[0]);
		::close(completionPipe[1]);
	});
	serverPid = fork();
	if (serverPid == 0) {
		_exit(runHost<true>(std::move(serverCreds), addrPipe[1], completionPipe[0], expect));
	}
	clientPid = fork();
	if (clientPid == 0) {
		_exit(runHost<false>(std::move(clientCreds), addrPipe[0], completionPipe[1], expect));
	}
	auto pid = pid_t{};
	auto status = int{};
	pid = waitpid(clientPid, &status, 0);
	auto ok = true;
	if (pid < 0) {
		logm("waitpid() for client failed with {}", strerror(errno));
		ok = false;
	} else {
		if (status != 0) {
			logm("Client error: rc={}", status);
			ok = false;
		} else {
			logm("Client OK");
		}
	}
	pid = waitpid(serverPid, &status, 0);
	if (pid < 0) {
		logm("waitpid() for server failed with {}", strerror(errno));
		ok = false;
	} else {
		if (status != 0) {
			logm("Server error: rc={}", status);
			ok = false;
		} else {
			logm("Server OK");
		}
	}
	log(ok ? "OK" : "FAILED");
	return 0;
}

int main() {
	std::pair<int, int> inputs[] = { { 3, 2 }, { 4, 0 }, { 1, 3 }, { 1, 0 }, { 2, 0 }, { 3, 3 }, { 3, 0 } };
	for (auto input : inputs) {
		auto [serverChainLen, clientChainLen] = input;
		if (auto rc = runTlsTest(serverChainLen, clientChainLen))
			return rc;
	}
	return 0;
}
#else // _WIN32

int main() {
	return 0;
}
#endif // _WIN32
