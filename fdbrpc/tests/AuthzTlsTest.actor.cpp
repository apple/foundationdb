/*
 * AuthzTlsTest.cpp
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

#ifndef _WIN32

#include <algorithm>
#include <array>
#include <cstring>
#include <ctime>
#include <iostream>
#include <string_view>
#include <thread>
#include <type_traits>

#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <fmt/core.h>

#include "fdbrpc/fdbrpc.h"
#include "fdbrpc/FlowTransport.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/MkCert.h"
#include "flow/ScopeExit.h"
#include "flow/TLSConfig.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

using namespace std::literals::string_view_literals;

enum Role : uint8_t { MAIN, CLIENT, SERVER, UNDETERMINED, LAST };

constexpr std::array<std::string_view, Role::LAST> ROLE_STRING{ "MAIN"sv, "CLIENT"sv, "SERVER"sv, "UNDETERMINED"sv };

Role role = Role::MAIN;

template <>
struct fmt::formatter<Role> : fmt::formatter<std::string> {
	auto format(Role role, fmt::format_context& ctx) const {
		return fmt::format_to(ctx.out(), "{:^10}", ROLE_STRING[static_cast<int>(role)]);
	}
};

template <class... Args>
void logRaw(const fmt::format_string<Args...>& fmt_str, Args&&... args) {
	std::cout << fmt::format(fmt_str, std::forward<Args>(args)...);
	std::cout.flush();
}

template <class... Args>
void log(const fmt::format_string<Args...>& fmt_str, Args&&... args) {
	// NOTE: The fmt::formatter<Role> can do the padding, but not this fmt::format expression
	std::cout << fmt::format("[{}] ", role);
	logRaw(fmt_str, std::forward<Args>(args)...);
	std::cout << std::endl;
}

enum ChainLength : int { NO_TLS = -1 };

template <>
struct fmt::formatter<ChainLength> : fmt::formatter<std::string> {
	auto format(ChainLength value, fmt::format_context& ctx) const {
		if (value == NO_TLS)
			return fmt::format_to(ctx.out(), "NO_TLS");
		else
			return fmt::format_to(ctx.out(), "{}", static_cast<std::underlying_type_t<ChainLength>>(value));
	}
};

template <>
struct fmt::formatter<std::vector<std::pair<ChainLength, ChainLength>>> : fmt::formatter<std::string> {
	auto format(const std::vector<std::pair<ChainLength, ChainLength>>& entries, fmt::format_context& ctx) const {
		fmt::format_to(ctx.out(), "[");
		bool first = true;
		for (const auto& entry : entries) {
			fmt::format_to(ctx.out(), "{}{{ {}, {} }}", (first ? "" : ", "), entry.first, entry.second);
			first = false;
		}
		return fmt::format_to(ctx.out(), "]");
	}
};

std::string drainPipe(const int pipeFd) {
	int readRc = 0;
	std::string ret;
	char buf[PIPE_BUF];
	while ((readRc = ::read(pipeFd, buf, PIPE_BUF)) > 0) {
		ret.append(buf, readRc);
	}
	if (readRc != 0) {
		log("Unexpected error draining pipe: {}", strerror(errno));
		throw std::runtime_error("pipe read error");
	}
	return ret;
}

struct TLSCreds {
	bool noTls = false;
	std::string certBytes;
	std::string keyBytes;
	std::string caBytes;
};

TLSCreds makeCreds(const ChainLength chainLen, const mkcert::ESide side) {
	if (chainLen == 0 || chainLen == NO_TLS) {
		return TLSCreds{ chainLen == NO_TLS, "", "", "" };
	}
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

enum class Result : int { ERROR = 0, TRUSTED, UNTRUSTED, TIMEOUT, LAST };

constexpr std::array<std::string_view, static_cast<size_t>(Result::LAST)> RESULT_STRING{ "ERROR",
	                                                                                     "TRUSTED",
	                                                                                     "UNTRUSTED",
	                                                                                     "TIMEOUT" };
template <>
struct fmt::formatter<Result> : fmt::formatter<std::string> {
	auto format(const Result& r, fmt::format_context& ctx) const {
		return fmt::format_to(ctx.out(), "{}", RESULT_STRING[static_cast<int>(r)]);
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

void runServer(const Endpoint& endpoint, int addrPipe, int completionPipe) {
	auto realAddr = FlowTransport::transport().getLocalAddresses().address;
	log("Listening at {}", realAddr.toString());
	log("Endpoint token is {}", endpoint.token.toString());
	static_assert(std::is_trivially_destructible_v<NetworkAddress>,
	              "NetworkAddress cannot be directly put on wire; need proper (de-)serialization");
	// below writes/reads would block, but this is good enough for a test.
	if (sizeof(realAddr) != ::write(addrPipe, &realAddr, sizeof(realAddr))) {
		log("Failed to write server addr to pipe: {}", strerror(errno));
		return;
	}
	if (sizeof(endpoint.token) != ::write(addrPipe, &endpoint.token, sizeof(endpoint.token))) {
		log("Failed to write server endpoint to pipe: {}", strerror(errno));
		return;
	}
	auto done = false;
	if (sizeof(done) != ::read(completionPipe, &done, sizeof(done))) {
		log("Failed to read completion flag from pipe: {}", strerror(errno));
		return;
	}
	return;
}

ACTOR Future<Void> waitAndPrintResponse(Future<SessionInfo> response, Result* rc) {
	try {
		SessionInfo info = wait(response);
		log("Probe response: trusted={} peerAddress={}", info.isPeerTrusted, info.peerAddress.toString());
		*rc = info.isPeerTrusted ? Result::TRUSTED : Result::UNTRUSTED;
	} catch (Error& err) {
		if (err.code() != error_code_operation_cancelled) {
			log("Unexpected error: {}", err.what());
			*rc = Result::ERROR;
		} else {
			log("Timed out");
			*rc = Result::TIMEOUT;
		}
	}
	return Void();
}

// int runAsServer(TLSCreds creds, int addrPipe, int completionPipe, Result expect) {}

template <bool IsServer>
int runHost(TLSCreds creds, int addrPipe, int completionPipe, Result expect) {
	auto tlsConfig = TLSConfig(IsServer ? TLSEndpointType::SERVER : TLSEndpointType::CLIENT);
	bool const noTls = creds.noTls;
	if (!noTls) {
		tlsConfig.setCertificateBytes(creds.certBytes);
		tlsConfig.setCABytes(creds.caBytes);
		tlsConfig.setKeyBytes(creds.keyBytes);
	}
	g_network = newNet2(tlsConfig);
	openTraceFile({}, 10 << 20, 10 << 20, ".", IsServer ? "authz_tls_unittest_server" : "authz_tls_unittest_client");
	FlowTransport::createInstance(!IsServer, 1, WLTOKEN_RESERVED_COUNT);
	auto& transport = FlowTransport::transport();
	if constexpr (IsServer) {
		auto addr = NetworkAddress::parse(noTls ? "127.0.0.1:0" : "127.0.0.1:0:tls");
		auto endpoint = Endpoint();
		auto receiver = SessionProbeReceiver();
		auto listenFuture = transport.bind(addr, addr);
		transport.addEndpoint(endpoint, &receiver, TaskPriority::ReadSocket);
		auto thread = std::thread([]() {
			g_network->run();
			flushTraceFileVoid();
		});
		runServer(endpoint, addrPipe, completionPipe);
		auto cleanupGuard = ScopeExit([&thread]() {
			g_network->stop();
			thread.join();
		});
		return 0;
	} else {
		auto dest = Endpoint();
		auto& serverAddr = dest.addresses.address;
		if (sizeof(serverAddr) != ::read(addrPipe, &serverAddr, sizeof(serverAddr))) {
			log("Failed to read server addr from pipe: {}", strerror(errno));
			return 1;
		}
		if (noTls)
			serverAddr.flags &= ~NetworkAddress::FLAG_TLS;
		else
			serverAddr.flags |= NetworkAddress::FLAG_TLS;
		auto& token = dest.token;
		if (sizeof(token) != ::read(addrPipe, &token, sizeof(token))) {
			log("Failed to read server endpoint token from pipe: {}", strerror(errno));
			return 2;
		}
		log("Server address is {}{}", serverAddr.toString(), noTls ? " (TLS suffix removed)" : "");
		log("Server endpoint token is {}", token.toString());
		auto sessionProbeReq = SessionProbeRequest{};
		transport.sendUnreliable(SerializeSource(sessionProbeReq), dest, true /*openConnection*/);
		log("Request is sent");
		auto rc = 0;
		auto result = Result::ERROR;
		{
			auto timeout = delay(expect == Result::TIMEOUT ? 0.5 : 5);
			auto complete = waitAndPrintResponse(sessionProbeReq.reply.getFuture(), &result);
			auto f = stopNetworkAfter(complete || timeout);
			g_network->run();
		}
		auto done = true;
		if (sizeof(done) != ::write(completionPipe, &done, sizeof(done))) {
			log("Failed to signal server to terminate: {}", strerror(errno));
			rc = 4;
		}
		if (rc == 0) {
			if (expect != result) {
				log("Test failed: expected {}, got {}", expect, result);
				rc = 5;
			} else {
				log("Response OK: got {} as expected", result);
			}
		}
		return rc;
	}
}

Result getExpectedResult(ChainLength serverChainLen, ChainLength clientChainLen) {
	auto expect = Result::ERROR;
	if (serverChainLen > 0) {
		if (clientChainLen == NO_TLS || clientChainLen < 0) {
			expect = Result::TIMEOUT;
		} else if (clientChainLen > 0) {
			expect = Result::TRUSTED;
		} else if (clientChainLen == 0) {
			expect = Result::UNTRUSTED;
		}
	} else if (serverChainLen == NO_TLS && clientChainLen == NO_TLS) {
		expect = Result::TRUSTED;
	} else {
		expect = Result::TIMEOUT;
	}
	return expect;
}

std::pair<bool, std::string> waitPidStatusInterpreter(const char* procName, const int status) {
	std::string prefix = fmt::format("{} subprocess ", procName);
	std::string message;
	if (WIFEXITED(status)) {
		const auto exitStatus = WEXITSTATUS(status);
		if (exitStatus == 0) {
			return { true, fmt::format("{} waitpid() OK", prefix) };
		}
		message = fmt::format("{} exited with status {}", prefix, exitStatus);
	} else if (WIFSIGNALED(status)) {
		const auto signal = WTERMSIG(status);
		message = fmt::format("{} killed by signal {} - {}", prefix, signal, strsignal(signal));
#ifdef WCOREDUMP
		const auto coreDumped = WCOREDUMP(status);
		if (coreDumped)
			message.append(std::string_view(" (core dumped)"));
#endif // WCOREDUMP
	} else if (WIFSTOPPED(status)) {
		const auto signal = WSTOPSIG(status);
		message = fmt::format("{} stopped by signal {} - {}", prefix, signal, strsignal(signal));
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wparentheses"
	} else if (WIFCONTINUED(status)) {
#pragma clang diagnostic pop
		message = fmt::format("{} continued by signal SIGCONT", prefix);
	}

	if (message.empty()) {
		message = fmt::format("{} Unrecognized status {} (Check man 2 waitpid for more details)", prefix, status);
	}

	return { false, message };
}

bool waitPid(pid_t subProcPid, const char* procName) {
	auto status = int{};
	auto pid = ::waitpid(subProcPid, &status, 0);

	if (pid < 0) {
		log("{} subprocess waitpid() failed with {}", procName, strerror(errno));

		return false;
	} else {
		auto [ok, message] = waitPidStatusInterpreter(procName, status);
		log("{}", message);

		return ok;
	}
}

int runTlsTest(ChainLength serverChainLen, ChainLength clientChainLen) {
	log("==== BEGIN TESTCASE ====");
	auto const expect = getExpectedResult(serverChainLen, clientChainLen);
	using namespace std::literals::string_literals;
	log("Cert chain length: server={} client={}", serverChainLen, clientChainLen);
	auto arena = Arena();
	auto serverCreds = makeCreds(serverChainLen, mkcert::ESide::Server);
	auto clientCreds = makeCreds(clientChainLen, mkcert::ESide::Client);
	// make server and client trust each other
	std::swap(serverCreds.caBytes, clientCreds.caBytes);
	auto clientPid = pid_t{};
	auto serverPid = pid_t{};
	int addrPipe[2], completionPipe[2], serverStdoutPipe[2], clientStdoutPipe[2];
	if (::pipe(addrPipe) || ::pipe(completionPipe) || ::pipe(serverStdoutPipe) || ::pipe(clientStdoutPipe)) {
		log("Pipe open failed: {}", strerror(errno));
		return 1;
	}
	auto ok = true;
	{
		serverPid = fork();
		if (serverPid == -1) {
			log("fork() for server subprocess failed: {}", strerror(errno));
			return 1;
		} else if (serverPid == 0) {
			role = Role::SERVER;
			// server subprocess
			::close(addrPipe[0]); // close address-in pipe (server writes its own address for client)
			::close(
			    completionPipe[1]); // close completion-flag-out pipe (server awaits/reads completion flag from client)
			::close(clientStdoutPipe[0]);
			::close(clientStdoutPipe[1]);
			::close(serverStdoutPipe[0]);
			auto pipeCleanup = ScopeExit([&addrPipe, &completionPipe]() {
				::close(addrPipe[1]);
				::close(completionPipe[0]);
			});
			if (-1 == ::dup2(serverStdoutPipe[1], STDOUT_FILENO)) {
				log("Failed to redirect server stdout to pipe: {}", strerror(errno));
				::close(serverStdoutPipe[1]);
				return 1;
			}
			_exit(runHost<true>(std::move(serverCreds), addrPipe[1], completionPipe[0], expect));
		}
		auto serverProcCleanup = ScopeExit([&ok, serverPid]() {
			if (!waitPid(serverPid, "Server"))
				ok = false;
		});
		clientPid = fork();
		if (clientPid == -1) {
			log("fork() for client subprocess failed: {}", strerror(errno));
			return 1;
		} else if (clientPid == 0) {
			role = Role::CLIENT;
			::close(addrPipe[1]);
			::close(completionPipe[0]);
			::close(serverStdoutPipe[0]);
			::close(serverStdoutPipe[1]);
			::close(clientStdoutPipe[0]);
			auto pipeCleanup = ScopeExit([&addrPipe, &completionPipe]() {
				::close(addrPipe[0]);
				::close(completionPipe[1]);
			});
			if (-1 == ::dup2(clientStdoutPipe[1], STDOUT_FILENO)) {
				log("Failed to redirect client stdout to pipe: {}", strerror(errno));
				::close(clientStdoutPipe[1]);
				return 1;
			}
			_exit(runHost<false>(std::move(clientCreds), addrPipe[0], completionPipe[1], expect));
		}
		auto clientProcCleanup = ScopeExit([&ok, clientPid]() {
			if (!waitPid(clientPid, "Client"))
				ok = false;
		});
	}
	// main process
	::close(addrPipe[0]);
	::close(addrPipe[1]);
	::close(completionPipe[0]);
	::close(completionPipe[1]);
	::close(serverStdoutPipe[1]);
	::close(clientStdoutPipe[1]);
	auto pipeCleanup = ScopeExit([&]() {
		::close(serverStdoutPipe[0]);
		::close(clientStdoutPipe[0]);
	});
	std::string const clientStdout = drainPipe(clientStdoutPipe[0]);
	log("/// Begin Client STDOUT ///");
	logRaw(fmt::runtime(clientStdout));
	log("/// End Client STDOUT ///");
	std::string const serverStdout = drainPipe(serverStdoutPipe[0]);
	log("/// Begin Server STDOUT ///");
	logRaw(fmt::runtime(serverStdout));
	log("/// End Server STDOUT ///");
	log(fmt::runtime(ok ? "OK" : "FAILED"));
	return !ok;
}

int main(int argc, char** argv) {
	unsigned seed = std::time(nullptr);
	if (argc > 1)
		seed = std::stoul(argv[1]);
	std::srand(seed);
	log("Seed: {}", seed);
	auto categoryToValue = [](int category) -> ChainLength {
		if (category == 2 || category == -2) {
			return static_cast<ChainLength>(category + std::rand() % 3);
		} else {
			return static_cast<ChainLength>(category);
		}
	};
	std::vector<std::pair<ChainLength, ChainLength>> inputs;
	std::vector<int> categories{ 0, NO_TLS, 1, -1, 2, -2 };
	for (auto lhs : categories) {
		for (auto rhs : categories) {
			auto input = std::pair(categoryToValue(lhs), categoryToValue(rhs));
			inputs.push_back(input);
		}
	}
	std::vector<std::pair<ChainLength, ChainLength>> failed;
	for (auto input : inputs) {
		auto [serverChainLen, clientChainLen] = input;
		if (runTlsTest(serverChainLen, clientChainLen))
			failed.push_back({ serverChainLen, clientChainLen });
	}
	if (!failed.empty()) {
		log("Test Failed: {}/{} cases: {}", failed.size(), inputs.size(), failed);
		return 1;
	} else {
		log("Test OK: {}/{} cases passed", inputs.size(), inputs.size());
		return 0;
	}
}
#else // _WIN32

#include <iostream>

int main() {
	std::cerr << "TLS test is not supported in Windows" << std::endl;
	return -1;
}

#endif // _WIN32
