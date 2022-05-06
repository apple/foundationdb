/*
 * TLSTest.cpp
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

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/bind.hpp>
#include <fmt/format.h>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include "flow/Arena.h"
#include "flow/MkCert.h"

std::FILE* outp = stderr;

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

auto client_ssl = ssl::context(ssl::context::tls);
auto server_ssl = ssl::context(ssl::context::tls);

mkcert::CertChainRef server_chain;
mkcert::CertChainRef client_chain;

void trust_root_cacert(ssl::context& ctx, StringRef certPem) {
	ctx.add_certificate_authority(const_buffer(certPem.begin(), certPem.size()));
}

void use_chain(ssl::context& ctx, mkcert::CertChainRef chain) {
	auto arena = Arena();
	auto chain_str = concatCertChain(arena, chain);
	ctx.use_certificate_chain(const_buffer(chain_str.begin(), chain_str.size()));
	auto keyPem = chain.front().privateKeyPem;
	ctx.use_private_key(const_buffer(keyPem.begin(), keyPem.size()), ssl::context::pem);
}

void init_certs(ssl::context& ctx, mkcert::CertChainRef my_chain, StringRef peerRootPem) {
	if (!peerRootPem.empty())
		trust_root_cacert(ctx, peerRootPem);
	if (my_chain.size() > 1)
		my_chain.pop_back();
	if (my_chain.size() > 0)
		use_chain(ctx, my_chain);
}

void init_client_ssl_context() {
	auto& ctx = client_ssl;
	ctx.set_options(ssl::context::default_workarounds);
	ctx.set_verify_mode(ssl::context::verify_peer | ssl::verify_fail_if_no_peer_cert);
	ctx.set_verify_callback([](bool preverify, ssl::verify_context&) {
		logc("context preverify: {}", preverify);
		return preverify;
	});
	init_certs(ctx, client_chain, server_chain.empty() ? StringRef() : server_chain.back().certPem);
}

void init_server_ssl_context() {
	auto& ctx = server_ssl;
	ctx.set_options(ssl::context::default_workarounds);
	ctx.set_verify_mode(ssl::context::verify_peer | (client_chain.empty() ? 0 : ssl::verify_fail_if_no_peer_cert));
	ctx.set_verify_callback([](bool preverify, ssl::verify_context&) {
		logs("context preverify: {}", preverify);
		return preverify;
	});
	init_certs(ctx, server_chain, client_chain.empty() ? StringRef() : client_chain.back().certPem);
}

template <>
struct fmt::formatter<tcp::endpoint> {
	constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.begin(); }

	template <class FormatContext>
	auto format(const tcp::endpoint& ep, FormatContext& ctx) -> decltype(ctx.out()) {
		return fmt::format_to(ctx.out(), "{}:{}", ep.address().to_string(), ep.port());
	}
};

int main(int argc, char** argv) {
	auto const server_chain_len = (argc > 1 ? std::strtoul(argv[1], nullptr, 10) : 3ul);
	auto const client_chain_len = (argc > 2 ? std::strtoul(argv[2], nullptr, 10) : 3ul);
	auto const expect_trusted = client_chain_len != 0;
	log("cert chain length: server {}, client {}", server_chain_len, client_chain_len);
	[[maybe_unused]] auto print_chain = [](mkcert::CertChainRef chain) -> void {
		if (chain.empty()) {
			log("EMPTY");
			return;
		}
		for (auto certAndKey : chain) {
			certAndKey.printCert(outp);
			log("===========");
			certAndKey.printPrivateKey(outp);
			log("===========");
		}
	};
	auto arena = Arena();
	if (server_chain_len > 0)
		server_chain = mkcert::makeCertChain(arena, server_chain_len, mkcert::ESide::Server);
	if (client_chain_len > 0)
		client_chain = mkcert::makeCertChain(arena, client_chain_len, mkcert::ESide::Client);
	/*
	log("=========== SERVER CHAIN");
	print_chain(server_chain);
	auto concat = concatCertChain(arena, server_chain);
	if (!concat.empty())
	    log(concat.toString());
	log("=========== CLIENT CHAIN");
	print_chain(client_chain);
	concat = concatCertChain(arena, client_chain);
	if (!concat.empty())
	    log(concat.toString());
	*/
	init_client_ssl_context();
	log("client SSL contexts initialized");
	init_server_ssl_context();
	log("server SSL contexts initialized");
	auto io = io_context();
	auto wg_server = work_guard_type(io.get_executor());
	auto wg_client = work_guard_type(io.get_executor());
	auto const ip = ip::address::from_string("127.0.0.1");
	auto acceptor = tcp::acceptor(io, tcp::endpoint(ip, 0));
	auto const server_addr = acceptor.local_endpoint();
	logs("server listening at {}", server_addr);
	auto server_sock = tcp::socket(io);
	auto server_ssl_sock = socket_type(server_sock, server_ssl);
	enum class ESockState { AssumedUntrusted, Trusted };
	auto server_sock_state = ESockState::AssumedUntrusted;
	auto client_sock_state = ESockState::AssumedUntrusted;
	server_ssl_sock.set_verify_callback([&server_sock_state](bool preverify, ssl::verify_context&) {
		logs("client preverify: {}", preverify);
		switch (server_sock_state) {
		case ESockState::AssumedUntrusted:
			if (!preverify)
				return false;
			server_sock_state = ESockState::Trusted;
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
	acceptor.async_accept(server_sock, [&server_ssl_sock, &wg_server](const ec_type& ec) {
		if (ec) {
			logs("accept error: {}", ec.message());
			wg_server.reset();
		} else {
			logs("accepted connection from {}", server_ssl_sock.next_layer().remote_endpoint());
			server_ssl_sock.async_handshake(ssl::stream_base::handshake_type::server, [&wg_server](const ec_type& ec) {
				if (ec) {
					logs("server handshake returned {}", ec.message());
				} else {
					logs("handshake OK");
				}
				wg_server.reset();
			});
		}
	});
	auto client_sock = tcp::socket(io);
	auto client_ssl_sock = socket_type(client_sock, client_ssl);
	client_ssl_sock.set_verify_callback([&client_sock_state](bool preverify, ssl::verify_context&) {
		logc("server preverify: {}", preverify);
		switch (client_sock_state) {
		case ESockState::AssumedUntrusted:
			if (!preverify)
				return false;
			client_sock_state = ESockState::Trusted;
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
	client_sock.async_connect(server_addr, [&wg_client, &client_sock, &client_ssl_sock](const ec_type& ec) {
		if (ec) {
			logc("connect error: {}", ec.message());
			wg_client.reset();
		} else {
			logc("connected to {}", client_sock.remote_endpoint());
			client_ssl_sock.async_handshake(ssl::stream_base::handshake_type::client, [&wg_client](const ec_type& ec) {
				if (ec) {
					logc("client handshake returned {}", ec.message());
				} else {
					logc("handshake OK");
				}
				wg_client.reset();
			});
		}
	});
	io.run();
	ASSERT_EQ(expect_trusted, (server_sock_state == ESockState::Trusted));
	log("Test OK: Connection considered {}", server_sock_state == ESockState::Trusted ? "trusted" : "untrusted");
	return 0;
}
