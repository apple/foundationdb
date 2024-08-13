/*
 * TLSTest.cpp
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

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <fmt/format.h>
#include <algorithm>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include "flow/Arena.h"
#include "flow/MkCert.h"

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

using namespace boost::asio;
using ip::tcp;

using ec_type = boost::system::error_code;

using socket_type = ssl::stream<tcp::socket&>;
using work_guard_type = executor_work_guard<io_context::executor_type>;

const_buffer toBuffer(StringRef s) {
	ASSERT(!s.empty());
	return const_buffer(s.begin(), s.size());
}

void trustRootCaCert(ssl::context& ctx, StringRef certPem) {
	if (!certPem.empty())
		ctx.add_certificate_authority(const_buffer(certPem.begin(), certPem.size()));
}

void useChain(ssl::context& ctx, mkcert::CertChainRef chain) {
	auto arena = Arena();
	auto chainStr = concatCertChain(arena, chain);
	if (!chainStr.empty())
		ctx.use_certificate_chain(toBuffer(chainStr));
	auto keyPem = chain.front().privateKeyPem;
	if (!keyPem.empty())
		ctx.use_private_key(toBuffer(keyPem), ssl::context::pem);
}

void initCerts(ssl::context& ctx, mkcert::CertChainRef myChain, StringRef peerRootPem) {
	trustRootCaCert(ctx, peerRootPem);
	if (myChain.size() > 1)
		myChain.pop_back();
	if (!myChain.empty())
		useChain(ctx, myChain);
}

void initSslContext(ssl::context& ctx,
                    mkcert::CertChainRef myChain,
                    mkcert::CertChainRef peerChain,
                    mkcert::ESide side) {
	ctx.set_options(ssl::context::default_workarounds);
	ctx.set_verify_mode(ssl::context::verify_peer |
	                    (side == mkcert::ESide::Server ? 0 : ssl::verify_fail_if_no_peer_cert));
	initCerts(ctx, myChain, peerChain.empty() ? StringRef() : peerChain.back().certPem);
}

template <>
struct fmt::formatter<tcp::endpoint> {
	constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

	template <class FormatContext>
	auto format(const tcp::endpoint& ep, FormatContext& ctx) -> decltype(ctx.out()) {
		return fmt::format_to(ctx.out(), "{}:{}", ep.address().to_string(), ep.port());
	}
};

void runTlsTest(int serverChainLen, int clientChainLen) {
	log("==== BEGIN TESTCASE ====");
	auto clientSsl = ssl::context(ssl::context::tls);
	auto serverSsl = ssl::context(ssl::context::tls);
	auto const expectHandshakeOk = clientChainLen >= 0 && serverChainLen > 0;
	auto const expectTrusted = clientChainLen != 0;
	log("cert chain length: server {}, client {}", serverChainLen, clientChainLen);
	auto arena = Arena();
	auto serverChain = mkcert::CertChainRef{};
	auto clientChain = mkcert::CertChainRef{};
	if (serverChainLen) {
		auto tmpArena = Arena();
		auto specs = mkcert::makeCertChainSpec(tmpArena, std::labs(serverChainLen), mkcert::ESide::Server);
		if (serverChainLen < 0) {
			specs[0].offsetNotBefore = -60l * 60 * 24 * 365;
			specs[0].offsetNotAfter = -10l; // cert that expired 10 seconds ago
		}
		serverChain = mkcert::makeCertChain(arena, specs, {} /* create root CA cert from spec*/);
	}
	if (clientChainLen) {
		auto tmpArena = Arena();
		auto specs = mkcert::makeCertChainSpec(tmpArena, std::labs(clientChainLen), mkcert::ESide::Client);
		if (clientChainLen < 0) {
			specs[0].offsetNotBefore = -60l * 60 * 24 * 365;
			specs[0].offsetNotAfter = -10l; // cert that expired 10 seconds ago
		}
		clientChain = mkcert::makeCertChain(arena, specs, {} /* create root CA cert from spec*/);
	}
	initSslContext(clientSsl, clientChain, serverChain, mkcert::ESide::Client);
	log("client SSL contexts initialized");
	initSslContext(serverSsl, serverChain, clientChain, mkcert::ESide::Server);
	log("server SSL contexts initialized");
	auto io = io_context();
	auto serverWorkGuard = work_guard_type(io.get_executor());
	auto clientWorkGuard = work_guard_type(io.get_executor());
	auto const ip = ip::address::from_string("127.0.0.1");
	auto acceptor = tcp::acceptor(io, tcp::endpoint(ip, 0));
	auto const serverAddr = acceptor.local_endpoint();
	logs("server listening at {}", serverAddr);
	auto serverSock = tcp::socket(io);
	auto serverSslSock = socket_type(serverSock, serverSsl);
	enum class ESockState { AssumedUntrusted, Trusted };
	auto serverSockState = ESockState::AssumedUntrusted;
	auto clientSockState = ESockState::AssumedUntrusted;
	auto handshakeOk = true;
	serverSslSock.set_verify_callback([&serverSockState, &handshakeOk](bool preverify, ssl::verify_context&) {
		logs("client preverify: {}", preverify);
		switch (serverSockState) {
		case ESockState::AssumedUntrusted:
			if (!preverify)
				return handshakeOk = false;
			serverSockState = ESockState::Trusted;
			break;
		case ESockState::Trusted:
			if (!preverify)
				return handshakeOk = false;
			break;
		default:
			break;
		}
		// if untrusted connection passes preverify, they are considered trusted
		return true;
	});
	acceptor.async_accept(serverSock, [&serverSslSock, &serverWorkGuard, &handshakeOk](const ec_type& ec) {
		if (ec) {
			logs("accept error: {}", ec.message());
			handshakeOk = false;
			serverWorkGuard.reset();
		} else {
			logs("accepted connection from {}", serverSslSock.next_layer().remote_endpoint());
			serverSslSock.async_handshake(ssl::stream_base::handshake_type::server,
			                              [&serverWorkGuard, &handshakeOk](const ec_type& ec) {
				                              if (ec) {
					                              logs("server handshake returned {}", ec.message());
					                              handshakeOk = false;
				                              } else {
					                              logs("handshake OK");
				                              }
				                              serverWorkGuard.reset();
			                              });
		}
	});
	auto clientSock = tcp::socket(io);
	auto clientSslSock = socket_type(clientSock, clientSsl);
	clientSslSock.set_verify_callback([&clientSockState](bool preverify, ssl::verify_context&) {
		logc("server preverify: {}", preverify);
		switch (clientSockState) {
		case ESockState::AssumedUntrusted:
			if (!preverify)
				return false;
			clientSockState = ESockState::Trusted;
			break;
		case ESockState::Trusted:
			if (!preverify)
				return false;
			break;
		default:
			break;
		}
		// if untrusted connection passes preverify, they are considered trusted
		return true;
	});
	clientSock.async_connect(serverAddr,
	                         [&clientWorkGuard, &clientSock, &clientSslSock, &handshakeOk](const ec_type& ec) {
		                         if (ec) {
			                         logc("connect error: {}", ec.message());
			                         handshakeOk = false;
			                         clientWorkGuard.reset();
		                         } else {
			                         logc("connected to {}", clientSock.remote_endpoint());
			                         clientSslSock.async_handshake(ssl::stream_base::handshake_type::client,
			                                                       [&clientWorkGuard, &handshakeOk](const ec_type& ec) {
				                                                       if (ec) {
					                                                       logc("handshake returned: {}", ec.message());
					                                                       handshakeOk = false;
				                                                       } else {
					                                                       logc("handshake OK");
				                                                       }
				                                                       clientWorkGuard.reset();
			                                                       });
		                         }
	                         });
	io.run();
	ASSERT_EQ(expectHandshakeOk, handshakeOk);
	if (expectHandshakeOk) {
		ASSERT_EQ(expectTrusted, (serverSockState == ESockState::Trusted));
		log("Test OK: Handshake passed and connection {} as expected",
		    serverSockState == ESockState::Trusted ? "trusted" : "untrusted");
	} else {
		log("Test OK: Handshake failed as expected");
	}
}

int main() {
	std::pair<int, int> inputs[] = { { 3, 2 }, { 4, 0 }, { -3, 1 }, { 3, -2 },  { -3, 0 },
		                             { 0, 0 }, { 0, 1 }, { 1, 3 },  { -1, -3 }, { 1, 0 } };
	for (auto input : inputs) {
		auto [serverChainLen, clientChainLen] = input;
		runTlsTest(serverChainLen, clientChainLen);
	}
	return 0;
}
