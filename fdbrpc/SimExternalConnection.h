/*
 * SimExternalConnection.h
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

#ifndef FDBRPC_SIM_EXTERNAL_CONNECTION_H
#define FDBRPC_SIM_EXTERNAL_CONNECTION_H
#pragma once

#include "flow/FastRef.h"
#include "flow/network.h"
#include "flow/flow.h"

#include <boost/asio.hpp>

// MockDNS is a class maintaining a <hostname, vector<NetworkAddress>> mapping, mocking a DNS in simulation.
class MockDNS {
public:
	MockDNS() {}
	explicit MockDNS(const std::map<std::string, std::vector<NetworkAddress>>& mockDNS)
	  : hostnameToAddresses(mockDNS) {}

	bool findMockTCPEndpoint(const std::string& host, const std::string& service);
	void addMockTCPEndpoint(const std::string& host,
	                        const std::string& service,
	                        const std::vector<NetworkAddress>& addresses);
	void updateMockTCPEndpoint(const std::string& host,
	                           const std::string& service,
	                           const std::vector<NetworkAddress>& addresses);
	void removeMockTCPEndpoint(const std::string& host, const std::string& service);
	void clearMockTCPEndpoints();
	std::vector<NetworkAddress> getTCPEndpoint(const std::string& host, const std::string& service);

	void operator=(MockDNS const& rhs) { hostnameToAddresses = rhs.hostnameToAddresses; }
	// Convert hostnameToAddresses to string. The format is:
	// hostname1,host1Address1,host1Address2;hostname2,host2Address1,host2Address2...
	std::string toString();
	static MockDNS parseFromString(const std::string& s);

private:
	std::map<std::string, std::vector<NetworkAddress>> hostnameToAddresses;
};

class SimExternalConnection final : public IConnection, public ReferenceCounted<SimExternalConnection> {
	boost::asio::ip::tcp::socket socket;
	SimExternalConnection(boost::asio::ip::tcp::socket&& socket);
	UID dbgid;
	std::deque<uint8_t> readBuffer;
	AsyncTrigger onReadableTrigger;
	friend class SimExternalConnectionImpl;

public:
	void addref() override { return ReferenceCounted<SimExternalConnection>::addref(); }
	void delref() override { return ReferenceCounted<SimExternalConnection>::delref(); }
	void close() override;
	Future<Void> acceptHandshake() override;
	Future<Void> connectHandshake() override;
	Future<Void> onWritable() override;
	Future<Void> onReadable() override;
	int read(uint8_t* begin, uint8_t* end) override;
	int write(SendBuffer const* buffer, int limit) override;
	NetworkAddress getPeerAddress() const override;
	UID getDebugID() const override;
	static Future<std::vector<NetworkAddress>> resolveTCPEndpoint(const std::string& host, const std::string& service);
	static std::vector<NetworkAddress> resolveTCPEndpointBlocking(const std::string& host, const std::string& service);
	static Future<Reference<IConnection>> connect(NetworkAddress toAddr);
};

#endif
