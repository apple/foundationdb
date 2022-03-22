/*
 * SimExternalConnection.actor.cpp
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

std::string MockDNS::toString() {
	std::string ret;
	for (auto it = hostnameToAddresses.begin(); it != hostnameToAddresses.end(); ++it) {
		if (it != hostnameToAddresses.begin()) {
			ret += ';';
		}
		ret += it->first + ',';
		const std::vector<NetworkAddress>& addresses = it->second;
		for (int i = 0; i < addresses.size(); ++i) {
			ret += addresses[i].toString();
			if (i != addresses.size() - 1) {
				ret += ',';
			}
		}
	}
	return ret;
}

MockDNS MockDNS::parseFromString(const std::string& s) {
	std::map<std::string, std::vector<NetworkAddress>> mockDNS;

	for (int p = 0; p < s.length();) {
		int pSemiColumn = s.find_first_of(';', p);
		if (pSemiColumn == s.npos) {
			pSemiColumn = s.length();
		}
		std::string oneMapping = s.substr(p, pSemiColumn - p);

		std::string hostname;
		std::vector<NetworkAddress> addresses;
		for (int i = 0; i < oneMapping.length();) {
			int pComma = oneMapping.find_first_of(',', i);
			if (pComma == oneMapping.npos) {
				pComma = oneMapping.length();
			}
			if (!i) {
				// The first part is hostname
				hostname = oneMapping.substr(i, pComma - i);
			} else {
				addresses.push_back(NetworkAddress::parse(oneMapping.substr(i, pComma - i)));
			}
			i = pComma + 1;
		}
		mockDNS[hostname] = addresses;
		p = pSemiColumn + 1;
	}

	return MockDNS(mockDNS);
}

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

std::vector<NetworkAddress> SimExternalConnection::resolveTCPEndpointBlocking(const std::string& host,
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

ACTOR static Future<std::vector<NetworkAddress>> resolveTCPEndpointImpl(std::string host, std::string service) {
	wait(delayJittered(0.1));
	return SimExternalConnection::resolveTCPEndpointBlocking(host, service);
}

Future<std::vector<NetworkAddress>> SimExternalConnection::resolveTCPEndpoint(const std::string& host,
                                                                              const std::string& service) {
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

TEST_CASE("fdbrpc/MockDNS") {
	state MockDNS mockDNS;
	state std::vector<NetworkAddress> networkAddresses;
	state NetworkAddress address1(IPAddress(0x13131313), 1);
	state NetworkAddress address2(IPAddress(0x14141414), 2);
	networkAddresses.push_back(address1);
	networkAddresses.push_back(address2);
	mockDNS.addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	ASSERT(mockDNS.findMockTCPEndpoint("testhost1", "port1"));
	ASSERT(!mockDNS.findMockTCPEndpoint("testhost1", "port2"));
	std::vector<NetworkAddress> resolvedNetworkAddresses = mockDNS.getTCPEndpoint("testhost1", "port1");
	ASSERT(resolvedNetworkAddresses.size() == 2);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address1) !=
	       resolvedNetworkAddresses.end());
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address2) !=
	       resolvedNetworkAddresses.end());
	// Adding a hostname twice should fail.
	try {
		mockDNS.addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	// Updating an unexisted hostname should fail.
	try {
		mockDNS.updateMockTCPEndpoint("testhost2", "port2", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	// Removing an unexisted hostname should fail.
	try {
		mockDNS.removeMockTCPEndpoint("testhost2", "port2");
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	mockDNS.clearMockTCPEndpoints();
	// Updating any hostname right after clearing endpoints should fail.
	try {
		mockDNS.updateMockTCPEndpoint("testhost1", "port1", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}

	return Void();
}

TEST_CASE("fdbrpc/MockTCPEndpoints") {
	state std::vector<NetworkAddress> networkAddresses;
	state NetworkAddress address1(IPAddress(0x13131313), 1);
	networkAddresses.push_back(address1);
	INetworkConnections::net()->addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	state std::vector<NetworkAddress> resolvedNetworkAddresses =
	    wait(INetworkConnections::net()->resolveTCPEndpoint("testhost1", "port1"));
	ASSERT(resolvedNetworkAddresses.size() == 1);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address1) !=
	       resolvedNetworkAddresses.end());
	// Adding a hostname twice should fail.
	try {
		INetworkConnections::net()->addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	// Removing an unexisted hostname should fail.
	try {
		INetworkConnections::net()->removeMockTCPEndpoint("testhost2", "port2");
	} catch (Error& e) {
		ASSERT(e.code() == error_code_operation_failed);
	}
	INetworkConnections::net()->removeMockTCPEndpoint("testhost1", "port1");
	state NetworkAddress address2(IPAddress(0x14141414), 2);
	networkAddresses.push_back(address2);
	INetworkConnections::net()->addMockTCPEndpoint("testhost1", "port1", networkAddresses);
	wait(store(resolvedNetworkAddresses, INetworkConnections::net()->resolveTCPEndpoint("testhost1", "port1")));
	ASSERT(resolvedNetworkAddresses.size() == 2);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address2) !=
	       resolvedNetworkAddresses.end());

	return Void();
}

TEST_CASE("fdbrpc/MockDNSParsing") {
	std::string mockDNSString;
	INetworkConnections::net()->parseMockDNSFromString(mockDNSString);
	ASSERT(INetworkConnections::net()->convertMockDNSToString() == mockDNSString);

	mockDNSString = "testhost1:port1,[::1]:4800:tls(fromHostname)";
	INetworkConnections::net()->parseMockDNSFromString(mockDNSString);
	ASSERT(INetworkConnections::net()->convertMockDNSToString() == mockDNSString);

	mockDNSString = "testhost1:port1,[::1]:4800,[2001:db8:85a3::8a2e:370:7334]:4800;testhost2:port2,[2001:"
	                "db8:85a3::8a2e:370:7334]:4800:tls(fromHostname),8.8.8.8:12";
	INetworkConnections::net()->parseMockDNSFromString(mockDNSString);
	ASSERT(INetworkConnections::net()->convertMockDNSToString() == mockDNSString);

	return Void();
}

void forceLinkSimExternalConnectionTests() {}
