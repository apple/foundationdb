/*
 * IUDPSocket.h
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

#ifndef FLOW_IUDPSOCKET_H
#define FLOW_IUDPSOCKET_H

#include <boost/asio/ip/udp.hpp>

#include "flow/network.h"

class IUDPSocket {
public:
	//  see https://en.wikipedia.org/wiki/User_Datagram_Protocol - the max size of a UDP packet
	// This is enforced in simulation
	constexpr static size_t MAX_PACKET_SIZE = 65535;
	virtual ~IUDPSocket();
	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual void close() = 0;
	virtual Future<int> send(uint8_t const* begin, uint8_t const* end) = 0;
	virtual Future<int> sendTo(uint8_t const* begin, uint8_t const* end, NetworkAddress const& peer) = 0;
	virtual Future<int> receive(uint8_t* begin, uint8_t* end) = 0;
	virtual Future<int> receiveFrom(uint8_t* begin, uint8_t* end, NetworkAddress* sender) = 0;
	virtual void bind(NetworkAddress const& addr) = 0;

	virtual UID getDebugID() const = 0;
	virtual NetworkAddress localAddress() const = 0;
	virtual boost::asio::ip::udp::socket::native_handle_type native_handle() = 0;
};

#endif // FLOW_IUDPSOCKET_H
