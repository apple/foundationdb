/*
 * SimExternalConnection.cpp
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

#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include <boost/asio.hpp>
#include <boost/range.hpp>
#include <thread>

#include "SimExternalConnection.h"
#include "flow/Hostname.h"
#include "flow/IConnection.h"
#include "flow/Net2Packet.h"
#include "flow/Platform.h"
#include "flow/SendBufferIterator.h"
#include "flow/UnitTest.h"
#include "flow/network.h"

using namespace boost::asio;

static io_service ios;

class SimExternalConnectionImpl {
public:
	static Future<Void> onReadable(SimExternalConnection* self) {
		co_await delayJittered(0.1);
		if (self->readBuffer.empty()) {
			co_await self->onReadableTrigger.onTrigger();
		}
	}

	static Future<Reference<IConnection>> connect(NetworkAddress toAddr) {
		co_await delayJittered(0.1);
		ip::tcp::socket socket(ios);
		auto ip = toAddr.ip;
		ip::address address;
		if (ip.isV6()) {
			address = boost::asio::ip::address_v6(ip.toV6());
		} else {
			address = boost::asio::ip::address_v4(ip.toV4());
		}
		boost::system::error_code err;
		socket.connect(ip::tcp::endpoint(address, toAddr.port), err);
		if (err) {
			co_return Reference<IConnection>();
		} else {
			co_return Reference<IConnection>(new SimExternalConnection(std::move(socket)));
		}
	}
};

void SimExternalConnection::close() {
	socket.close();
}

Future<Void> SimExternalConnection::acceptHandshake() {
	return Void();
}

Future<Void> SimExternalConnection::connectHandshake() {
	return Void();
}

Future<Void> SimExternalConnection::onWritable() {
	return Void();
}

Future<Void> SimExternalConnection::onReadable() {
	return SimExternalConnectionImpl::onReadable(this);
}

int SimExternalConnection::read(uint8_t* begin, uint8_t* end) {
	auto toRead = std::min<int>(end - begin, readBuffer.size());
	std::copy(readBuffer.begin(), readBuffer.begin() + toRead, begin);
	readBuffer.erase(readBuffer.begin(), readBuffer.begin() + toRead);
	return toRead;
}

int SimExternalConnection::write(SendBuffer const* buffer, int limit) {
	boost::system::error_code err;
	bool triggerReaders = (socket.available() == 0);
	int bytesSent = socket.write_some(
	    boost::iterator_range<SendBufferIterator>(SendBufferIterator(buffer, limit), SendBufferIterator()), err);
	ASSERT(!err);
	ASSERT(bytesSent > 0);
	threadSleep(0.1);
	const auto bytesReadable = socket.available();
	std::vector<uint8_t> tempReadBuffer(bytesReadable);
	for (int index = 0; index < bytesReadable;) {
		index += socket.read_some(mutable_buffers_1(&tempReadBuffer[index], bytesReadable), err);
	}
	std::copy(tempReadBuffer.begin(), tempReadBuffer.end(), std::inserter(readBuffer, readBuffer.end()));
	ASSERT(!err);
	ASSERT(socket.available() == 0);
	if (triggerReaders) {
		onReadableTrigger.trigger();
	}
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

bool SimExternalConnection::hasTrustedPeer() const {
	return true;
}

UID SimExternalConnection::getDebugID() const {
	return dbgid;
}

std::vector<NetworkAddress> SimExternalConnection::resolveTCPEndpointBlocking(const std::string& host,
                                                                              const std::string& service,
                                                                              DNSCache* dnsCache) {
	ip::tcp::resolver resolver(ios);
	try {
		auto iter = resolver.resolve(host, service);
		decltype(iter) end;
		std::vector<NetworkAddress> addrs;
		while (iter != end) {
			auto endpoint = iter->endpoint();
			auto addr = endpoint.address();
			// register the endpoint as public so that if it does happen to be an fdb process, we can connect to it
			// successfully
			if (addr.is_v6()) {
				addrs.emplace_back(IPAddress(addr.to_v6().to_bytes()), endpoint.port(), true, false);
			} else {
				addrs.emplace_back(addr.to_v4().to_ulong(), endpoint.port(), true, false);
			}
			++iter;
		}
		if (addrs.empty()) {
			throw lookup_failed();
		}
		dnsCache->add(host, service, addrs);
		return addrs;
	} catch (...) {
		throw lookup_failed();
	}
}

static Future<std::vector<NetworkAddress>> resolveTCPEndpointImpl(std::string host,
                                                                  std::string service,
                                                                  DNSCache* dnsCache) {
	co_await delayJittered(0.1);
	co_return SimExternalConnection::resolveTCPEndpointBlocking(host, service, dnsCache);
}

Future<std::vector<NetworkAddress>> SimExternalConnection::resolveTCPEndpoint(const std::string& host,
                                                                              const std::string& service,
                                                                              DNSCache* dnsCache) {
	return resolveTCPEndpointImpl(host, service, dnsCache);
}

Future<Reference<IConnection>> SimExternalConnection::connect(NetworkAddress toAddr) {
	return SimExternalConnectionImpl::connect(toAddr);
}

SimExternalConnection::SimExternalConnection(ip::tcp::socket&& socket)
  : socket(std::move(socket)), dbgid(deterministicRandom()->randomUniqueID()) {}

static constexpr auto testEchoServerPort = 8000;

static void testEchoServer() {
	static constexpr auto readBufferSize = 1000;
	io_service ios;
	ip::tcp::acceptor acceptor(ios, ip::tcp::endpoint(ip::tcp::v4(), testEchoServerPort));
	ip::tcp::socket socket(ios);
	acceptor.accept(socket);
	while (true) {
		char readBuffer[readBufferSize];
		boost::system::error_code err;
		auto length = socket.read_some(mutable_buffers_1(readBuffer, readBufferSize), err);
		if (err == boost::asio::error::eof) {
			return;
		}
		ASSERT(!err);
		write(socket, buffer(readBuffer, length));
	}
}

TEST_CASE("fdbrpc/SimExternalClient") {
	const size_t maxDataLength = 10000;
	std::thread serverThread([] { return testEchoServer(); });
	UnsentPacketQueue packetQueue;
	Reference<IConnection> externalConn;
	while (true) {
		Reference<IConnection> connected =
		    co_await INetworkConnections::net()->connect("localhost", std::to_string(testEchoServerPort));
		if (connected.isValid()) {
			externalConn = std::move(connected);
			break;
		}
		// Wait until server is ready
		threadSleep(0.01);
	}
	Standalone<StringRef> data(
	    deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(0, maxDataLength + 1)));
	PacketWriter packetWriter(packetQueue.getWriteBuffer(data.size()), nullptr, Unversioned());
	packetWriter.serializeBytes(data);
	co_await externalConn->onWritable();
	externalConn->write(packetQueue.getUnsent());
	co_await externalConn->onReadable();
	std::vector<uint8_t> vec(data.size());
	if (!vec.empty()) {
		externalConn->read(vec.data(), vec.data() + vec.size());
	}
	externalConn->close();
	StringRef echo(vec.empty() ? reinterpret_cast<const uint8_t*>("") : vec.data(), vec.size());
	ASSERT(echo.toString() == data.toString());
	serverThread.join();
}

TEST_CASE("fdbrpc/MockDNS") {
	std::vector<NetworkAddress> networkAddresses;
	NetworkAddress address1(IPAddress(0x13131313), 1);
	networkAddresses.push_back(address1);
	INetworkConnections::net()->addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	std::vector<NetworkAddress> resolvedNetworkAddresses =
	    co_await INetworkConnections::net()->resolveTCPEndpoint("testhost1", "port1");
	ASSERT(resolvedNetworkAddresses.size() == 1);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address1) !=
	       resolvedNetworkAddresses.end());
	INetworkConnections::net()->removeMockTCPEndpoint("testhost1", "port1");
	NetworkAddress address2(IPAddress(0x14141414), 2);
	networkAddresses.push_back(address2);
	INetworkConnections::net()->addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	resolvedNetworkAddresses = co_await INetworkConnections::net()->resolveTCPEndpoint("testhost1", "port1");
	ASSERT(resolvedNetworkAddresses.size() == 2);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address2) !=
	       resolvedNetworkAddresses.end());
}

TEST_CASE("/fdbrpc/Hostname/hostname") {
	if (!g_network->isSimulated()) {
		co_return;
	}

	Hostname hostname = Hostname::parse("host-name:1234");
	NetworkAddress addressSource = NetworkAddress::parse("127.0.0.0:1234");
	INetworkConnections::net()->addMockTCPEndpoint(hostname.host, hostname.service, { addressSource });

	Optional<NetworkAddress> optionalAddress = co_await hostname.resolve();
	ASSERT(optionalAddress.present() && optionalAddress.get() == addressSource);

	optionalAddress = hostname.resolveBlocking();
	ASSERT(optionalAddress.present() && optionalAddress.get() == addressSource);

	NetworkAddress address = co_await hostname.resolveWithRetry();
	ASSERT(address == addressSource);
}

void forceLinkSimExternalConnectionTests() {}
