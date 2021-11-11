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

#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include <boost/asio.hpp>
#include <boost/range.hpp>
#include <thread>

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/SimExternalConnection.h"
#include "flow/Net2Packet.h"
#include "flow/Platform.h"
#include "flow/SendBufferIterator.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

using namespace boost::asio;

static io_service ios;

class SimExternalConnectionImpl {
public:
	ACTOR static Future<Void> onReadable(SimExternalConnection* self) {
		wait(delayJittered(0.1));
		if (self->readBuffer.empty()) {
			wait(self->onReadableTrigger.onTrigger());
		}
		return Void();
	}

	ACTOR static Future<Reference<IConnection>> connect(NetworkAddress toAddr) {
		wait(delayJittered(0.1));
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
			return Reference<IConnection>();
		} else {
			return Reference<IConnection>(new SimExternalConnection(std::move(socket)));
		}
	}
};

bool MockDNS::findMockTCPEndpoint(const std::string& host, const std::string& service) {
	std::string hostname = host + ":" + service;
	return hostnameToAddresses.find(hostname) != hostnameToAddresses.end();
}

void MockDNS::addMockTCPEndpoint(const std::string& host,
                                 const std::string& service,
                                 const std::vector<NetworkAddress>& addresses) {
	if (findMockTCPEndpoint(host, service)) {
		throw operation_failed();
	}
	hostnameToAddresses[host + ":" + service] = addresses;
}

void MockDNS::updateMockTCPEndpoint(const std::string& host,
                                    const std::string& service,
                                    const std::vector<NetworkAddress>& addresses) {
	if (!findMockTCPEndpoint(host, service)) {
		throw operation_failed();
	}
	hostnameToAddresses[host + ":" + service] = addresses;
}

void MockDNS::removeMockTCPEndpoint(const std::string& host, const std::string& service) {
	if (!findMockTCPEndpoint(host, service)) {
		throw operation_failed();
	}
	hostnameToAddresses.erase(host + ":" + service);
}

std::vector<NetworkAddress> MockDNS::getTCPEndpoint(const std::string& host, const std::string& service) {
	if (!findMockTCPEndpoint(host, service)) {
		throw operation_failed();
	}
	return hostnameToAddresses[host + ":" + service];
}

void MockDNS::clearMockTCPEndpoints() {
	hostnameToAddresses.clear();
}

MockDNS SimExternalConnection::mockDNS;

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

UID SimExternalConnection::getDebugID() const {
	return dbgid;
}

ACTOR static Future<std::vector<NetworkAddress>> resolveTCPEndpointImpl(std::string host, std::string service) {
	wait(delayJittered(0.1));
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

Future<std::vector<NetworkAddress>> SimExternalConnection::resolveTCPEndpoint(const std::string& host,
                                                                              const std::string& service) {
	if (mockDNS.findMockTCPEndpoint(host, service)) {
		return mockDNS.getTCPEndpoint(host, service);
	}
	return resolveTCPEndpointImpl(host, service);
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
	loop {
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
	state const size_t maxDataLength = 10000;
	state std::thread serverThread([] { return testEchoServer(); });
	state UnsentPacketQueue packetQueue;
	state Reference<IConnection> externalConn;
	loop {
		Reference<IConnection> _externalConn =
		    wait(INetworkConnections::net()->connect("localhost", std::to_string(testEchoServerPort)));
		if (_externalConn.isValid()) {
			externalConn = std::move(_externalConn);
			break;
		}
		// Wait until server is ready
		threadSleep(0.01);
	}
	state Key data = deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(0, maxDataLength + 1));
	PacketWriter packetWriter(packetQueue.getWriteBuffer(data.size()), nullptr, Unversioned());
	packetWriter.serializeBytes(data);
	wait(externalConn->onWritable());
	externalConn->write(packetQueue.getUnsent());
	wait(externalConn->onReadable());
	std::vector<uint8_t> vec(data.size());
	externalConn->read(&vec[0], &vec[0] + vec.size());
	externalConn->close();
	StringRef echo(&vec[0], vec.size());
	ASSERT(echo.toString() == data.toString());
	serverThread.join();
	return Void();
}

TEST_CASE("fdbrpc/MockTCPEndpoints") {
	state MockDNS mockDNS;
	state std::vector<NetworkAddress> networkAddresses;
	state NetworkAddress address1(IPAddress(0x13131313), 1);
	state NetworkAddress address2(IPAddress(0x14141414), 2);
	networkAddresses.push_back(address1);
	networkAddresses.push_back(address2);
	mockDNS.addMockTCPEndpoint("testhost1", "testport1", networkAddresses);
	ASSERT(mockDNS.findMockTCPEndpoint("testhost1", "testport1"));
	ASSERT(mockDNS.findMockTCPEndpoint("testhost1", "testport2") == false);
	std::vector<NetworkAddress> resolvedNetworkAddresses = mockDNS.getTCPEndpoint("testhost1", "testport1");
	ASSERT(resolvedNetworkAddresses.size() == 2);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address1) !=
	       resolvedNetworkAddresses.end());
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address2) !=
	       resolvedNetworkAddresses.end());
	// Adding a hostname twice should fail.
	try {
		mockDNS.addMockTCPEndpoint("testhost1", "testport1", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	// Updating an unexisted hostname should fail.
	try {
		mockDNS.updateMockTCPEndpoint("testhost2", "testport2", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	// Removing an unexisted hostname should fail.
	try {
		mockDNS.removeMockTCPEndpoint("testhost2", "testport2");
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	mockDNS.clearMockTCPEndpoints();
	// Updating any hostname right after clearing endpoints should fail.
	try {
		mockDNS.updateMockTCPEndpoint("testhost1", "testport1", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}

	return Void();
}

void forceLinkSimExternalConnectionTests() {}
