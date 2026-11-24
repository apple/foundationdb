/*
 * TLSHandshakeSocket.h
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

#ifndef FLOW_TLSHANDSHAKESOCKET_H
#define FLOW_TLSHANDSHAKESOCKET_H

#include <cstddef>
#include <cstdint>
#include <utility>
#include <vector>

#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/stream.hpp>

#ifndef _WIN32
class TLSHandshakeTransport : public boost::asio::ip::tcp::socket {
	using Socket = boost::asio::ip::tcp::socket;

public:
	using Socket::Socket;
	using Socket::read_some;
	using Socket::write_some;

	explicit TLSHandshakeTransport(Socket&& socket) : Socket(std::move(socket)) {}

	// Boost's synchronous SSL handshake consumes its output even if the socket would block. In a worker's
	// handshake call, keep the unsent suffix here. The worker flushes it before advancing the handshake again.
	void bufferHandshakeWrites(std::vector<uint8_t>* output) { handshakeOutput = output; }

	template <class MutableBufferSequence>
	std::size_t read_some(const MutableBufferSequence& buffers, boost::system::error_code& ec) {
		if (handshakeOutput && !handshakeOutput->empty()) {
			ec = boost::asio::error::would_block;
			return 0;
		}
		return Socket::read_some(buffers, ec);
	}

	template <class ConstBufferSequence>
	std::size_t write_some(const ConstBufferSequence& buffers, boost::system::error_code& ec) {
		if (!handshakeOutput) {
			return Socket::write_some(buffers, ec);
		}
		if (handshakeOutput->empty()) {
			std::size_t written = Socket::write_some(buffers, ec);
			if (ec != boost::asio::error::would_block && ec != boost::asio::error::try_again) {
				return written;
			}
		}
		ec.clear();
		std::size_t offset = handshakeOutput->size();
		std::size_t size = boost::asio::buffer_size(buffers);
		handshakeOutput->resize(offset + size);
		return boost::asio::buffer_copy(boost::asio::buffer(handshakeOutput->data() + offset, size), buffers);
	}

private:
	std::vector<uint8_t>* handshakeOutput = nullptr;
};
#else
using TLSHandshakeTransport = boost::asio::ip::tcp::socket;
#endif

using TLSHandshakeSocket = boost::asio::ssl::stream<TLSHandshakeTransport&>;

#endif // FLOW_TLSHANDSHAKESOCKET_H
