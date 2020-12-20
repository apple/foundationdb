/*
 * SimExternalConnection.actor.cpp
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

#include <boost/asio.hpp>
#include <boost/range.hpp>
#include <chrono>
#include <thread>

#include "fdbrpc/SimExternalConnection.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace boost::asio;

static io_service ios;

void SimExternalConnection::close() {
	socket.close();
}

Future<Void> SimExternalConnection::acceptHandshake() {
	ASSERT(false);
	return Void();
}

Future<Void> SimExternalConnection::connectHandshake() {
	return Void();
}

Future<Void> SimExternalConnection::onWritable() {
	return Void();
}

Future<Void> SimExternalConnection::onReadable() {
	return Void();
}

int SimExternalConnection::read(uint8_t* begin, uint8_t* end) {
	size_t toRead = end - begin;
	ASSERT(toRead <= readBuffer.size());
	// TODO: Improve performance
	for (int i = 0; i < toRead; ++i) {
		*(begin + i) = readBuffer.front();
		readBuffer.pop_front();
	}
	return toRead;
}

int SimExternalConnection::write(SendBuffer const* buffer, int limit) {
	boost::system::error_code err;
	int bytesSent = socket.write_some(
	    boost::iterator_range<SendBufferIterator>(SendBufferIterator(buffer, limit), SendBufferIterator()), err);
	ASSERT(!err);
	ASSERT(bytesSent > 0);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	const auto bytesReadable = socket.available();
	std::vector<uint8_t> tempReadBuffer(bytesReadable);
	for (int index = 0; index < bytesReadable;) {
		index += socket.read_some(mutable_buffers_1(&tempReadBuffer[index], bytesReadable), err);
	}
	std::copy(tempReadBuffer.begin(), tempReadBuffer.end(), std::inserter(readBuffer, readBuffer.end()));
	ASSERT(!err);
	ASSERT(socket.available() == 0);
	return bytesSent;
}

NetworkAddress SimExternalConnection::getPeerAddress() const {
	auto endpoint = socket.remote_endpoint();
	auto addr = endpoint.address();
	if (addr.is_v6()) {
		return NetworkAddress(IPAddress(addr.to_v6().to_bytes()), endpoint.port());
	} else {
		return NetworkAddress(addr.to_v4().to_ulong(), endpoint.port());
	}
}

UID SimExternalConnection::getDebugID() const {
	return dbgid;
}

Future<std::vector<NetworkAddress>> SimExternalConnection::resolveTCPEndpoint(const std::string& host,
                                                                              const std::string& service) {
	ip::tcp::resolver resolver(ios);
	ip::tcp::resolver::query query(host, service);
	auto iter = resolver.resolve(query);
	decltype(iter) end;
	std::vector<NetworkAddress> addrs;
	while (iter != end) {
		auto endpoint = iter->endpoint();
		auto addr = endpoint.address();
		if (addr.is_v6()) {
			addrs.emplace_back(IPAddress(addr.to_v6().to_bytes()), endpoint.port());
		} else {
			addrs.emplace_back(addr.to_v4().to_ulong(), endpoint.port());
		}
		++iter;
	}
	return addrs;
}

Future<Reference<IConnection>> SimExternalConnection::connect(NetworkAddress toAddr) {
	ip::tcp::socket socket(ios);
	auto ip = toAddr.ip;
	ip::address address;
	if (ip.isV6()) {
		address = boost::asio::ip::address_v6(ip.toV6());
	} else {
		address = boost::asio::ip::address_v4(ip.toV4());
	}
	socket.connect(ip::tcp::endpoint(address, toAddr.port));
	return Reference<IConnection>(new SimExternalConnection(std::move(socket)));
}

SimExternalConnection::SimExternalConnection(ip::tcp::socket&& socket)
  : socket(std::move(socket)), dbgid(deterministicRandom()->randomUniqueID()) {}
