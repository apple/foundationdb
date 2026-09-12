/*
 * prometheus_server.hpp
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

#ifndef MAKO_PROMETHEUS_SERVER_HPP
#define MAKO_PROMETHEUS_SERVER_HPP

#include <boost/asio.hpp>
#include <fmt/format.h>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <utility>

namespace mako {

class NativeMetricsServer {
	using Tcp = boost::asio::ip::tcp;
	struct Session : std::enable_shared_from_this<Session> {
		Tcp::socket socket;
		std::function<std::string()> const& render;
		std::string request;
		std::string response;

		Session(Tcp::socket socket, std::function<std::string()> const& render)
		  : socket(std::move(socket)), render(render) {}

		void start() {
			auto self = shared_from_this();
			boost::asio::async_read_until(socket,
			                              boost::asio::dynamic_buffer(request, 4096),
			                              "\r\n\r\n",
			                              [self](boost::system::error_code error, size_t) {
				                              if (error) {
					                              return;
				                              }
				                              const bool metrics = self->request.rfind("GET /metrics HTTP/1.", 0) == 0;
				                              const auto body = metrics ? self->render() : std::string("Not Found\n");
				                              self->response = fmt::format(
				                                  "HTTP/1.1 {}\r\nContent-Type: text/plain; version=0.0.4; "
				                                  "charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
				                                  metrics ? "200 OK" : "404 Not Found",
				                                  body.size(),
				                                  body);
				                              boost::asio::async_write(self->socket,
				                                                       boost::asio::buffer(self->response),
				                                                       [self](boost::system::error_code, size_t) {});
			                              });
		}
	};

	boost::asio::io_context io;
	Tcp::acceptor acceptor;
	std::function<std::string()> render;
	std::thread thread;

	void accept() {
		acceptor.async_accept([this](boost::system::error_code error, Tcp::socket socket) {
			if (!error) {
				std::make_shared<Session>(std::move(socket), render)->start();
			}
			if (acceptor.is_open()) {
				accept();
			}
		});
	}

public:
	NativeMetricsServer(uint16_t port, std::function<std::string()> render) : acceptor(io), render(std::move(render)) {
		acceptor.open(Tcp::v4());
		acceptor.set_option(Tcp::acceptor::reuse_address(true));
		acceptor.bind(Tcp::endpoint(Tcp::v4(), port));
		acceptor.listen();
		accept();
		thread = std::thread([this]() { io.run(); });
	}

	uint16_t port() const { return acceptor.local_endpoint().port(); }

	~NativeMetricsServer() {
		io.stop();
		if (thread.joinable()) {
			thread.join();
		}
	}
};

} // namespace mako

#endif /* MAKO_PROMETHEUS_SERVER_HPP */
