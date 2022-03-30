/*
 * network.cpp
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

#include "Arena.h"
#include "boost/asio.hpp"

#include "flow/network.h"
#include "flow/flow.h"
#include "flow/UnitTest.h"

bool IPAddress::operator==(const IPAddress& rhs) const {
	return addr == rhs.addr;
}

bool IPAddress::operator!=(const IPAddress& addr) const {
	return !(*this == addr);
}

bool IPAddress::operator<(const IPAddress& rhs) const {
	return addr < rhs.addr;
}

std::string IPAddress::toString() const {
	if (isV6()) {
		return boost::asio::ip::address_v6(std::get<IPAddressStore>(addr)).to_string();
	} else {
		auto ip = std::get<uint32_t>(addr);
		return format("%d.%d.%d.%d", (ip >> 24) & 0xff, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff);
	}
}

Optional<IPAddress> IPAddress::parse(std::string const& str) {
	try {
		auto addr = boost::asio::ip::address::from_string(str);
		return addr.is_v6() ? IPAddress(addr.to_v6().to_bytes()) : IPAddress(addr.to_v4().to_ulong());
	} catch (...) {
		return Optional<IPAddress>();
	}
}

bool IPAddress::isValid() const {
	if (isV6()) {
		const auto& ip = std::get<IPAddressStore>(addr);
		return std::any_of(ip.begin(), ip.end(), [](uint8_t part) { return part != 0; });
	}
	return std::get<uint32_t>(addr) != 0;
}

Hostname Hostname::parse(const std::string& s) {
	if (s.empty()) {
		throw connection_string_invalid();
	}

	bool isTLS = false;
	std::string f;
	if (s.size() > 4 && strcmp(s.c_str() + s.size() - 4, ":tls") == 0) {
		isTLS = true;
		f = s.substr(0, s.size() - 4);
	} else {
		f = s;
	}
	auto colonPos = f.find_first_of(":");
	return Hostname(f.substr(0, colonPos), f.substr(colonPos + 1), isTLS);
}

FDB_DEFINE_BOOLEAN_PARAM(NetworkAddressFromHostname);

NetworkAddress NetworkAddress::parse(std::string const& s) {
	if (s.empty()) {
		throw connection_string_invalid();
	}

	bool isTLS = false;
	NetworkAddressFromHostname fromHostname = NetworkAddressFromHostname::False;
	std::string f = s;
	const auto& pos = f.find("(fromHostname)");
	if (pos != std::string::npos) {
		fromHostname = NetworkAddressFromHostname::True;
		f = f.substr(0, pos);
	}
	if (f.size() > 4 && strcmp(f.c_str() + f.size() - 4, ":tls") == 0) {
		isTLS = true;
		f = f.substr(0, f.size() - 4);
	}

	if (f[0] == '[') {
		// IPv6 address/port pair is represented as "[ip]:port"
		auto addrEnd = f.find_first_of(']');
		if (addrEnd == std::string::npos || f[addrEnd + 1] != ':') {
			throw connection_string_invalid();
		}

		auto port = std::stoi(f.substr(addrEnd + 2));
		auto addr = IPAddress::parse(f.substr(1, addrEnd - 1));
		if (!addr.present()) {
			throw connection_string_invalid();
		}
		return NetworkAddress(addr.get(), port, true, isTLS, fromHostname);
	} else {
		// TODO: Use IPAddress::parse
		int a, b, c, d, port, count = -1;
		if (sscanf(f.c_str(), "%d.%d.%d.%d:%d%n", &a, &b, &c, &d, &port, &count) < 5 || count != f.size())
			throw connection_string_invalid();
		return NetworkAddress((a << 24) + (b << 16) + (c << 8) + d, port, true, isTLS, fromHostname);
	}
}

Optional<NetworkAddress> NetworkAddress::parseOptional(std::string const& s) {
	try {
		return NetworkAddress::parse(s);
	} catch (Error& e) {
		ASSERT(e.code() == error_code_connection_string_invalid);
		return Optional<NetworkAddress>();
	}
}

std::vector<NetworkAddress> NetworkAddress::parseList(std::string const& addrs) {
	// Split addrs on ',' and parse them individually
	std::vector<NetworkAddress> coord;
	for (int p = 0; p < addrs.length();) {
		int pComma = addrs.find_first_of(',', p);
		if (pComma == addrs.npos) {
			pComma = addrs.length();
		}
		coord.push_back(NetworkAddress::parse(addrs.substr(p, pComma - p)));
		p = pComma + 1;
	}
	return coord;
}

std::string NetworkAddress::toString() const {
	std::string ipString = formatIpPort(ip, port) + (isTLS() ? ":tls" : "");
	if (fromHostname) {
		return ipString + "(fromHostname)";
	}
	return ipString;
}

std::string toIPVectorString(const std::vector<uint32_t>& ips) {
	std::string output;
	const char* space = "";
	for (const auto& ip : ips) {
		output += format("%s%d.%d.%d.%d", space, (ip >> 24) & 0xff, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff);
		space = " ";
	}
	return output;
}

std::string toIPVectorString(const std::vector<IPAddress>& ips) {
	std::string output;
	const char* space = "";
	for (auto ip : ips) {
		output += format("%s%s", space, ip.toString().c_str());
		space = " ";
	}
	return output;
}

std::string formatIpPort(const IPAddress& ip, uint16_t port) {
	const char* patt = ip.isV6() ? "[%s]:%d" : "%s:%d";
	return format(patt, ip.toString().c_str(), port);
}

Future<Reference<IConnection>> INetworkConnections::connect(const std::string& host,
                                                            const std::string& service,
                                                            bool isTLS) {
	// Use map to create an actor that returns an endpoint or throws
	Future<NetworkAddress> pickEndpoint =
	    map(resolveTCPEndpoint(host, service), [=](std::vector<NetworkAddress> const& addresses) -> NetworkAddress {
		    NetworkAddress addr = addresses[deterministicRandom()->randomInt(0, addresses.size())];
		    addr.fromHostname = true;
		    if (isTLS) {
			    addr.flags = NetworkAddress::FLAG_TLS;
		    }
		    return addr;
	    });

	// Wait for the endpoint to return, then wait for connect(endpoint) and return it.
	// Template types are being provided explicitly because they can't be automatically deduced for some reason.
	return mapAsync<NetworkAddress,
	                std::function<Future<Reference<IConnection>>(NetworkAddress const&)>,
	                Reference<IConnection>>(
	    pickEndpoint,
	    [=](NetworkAddress const& addr) -> Future<Reference<IConnection>> { return connectExternal(addr, host); });
}

IUDPSocket::~IUDPSocket() {}

const std::vector<int> NetworkMetrics::starvationBins = { 1, 3500, 7000, 7500, 8500, 8900, 10500 };

TEST_CASE("/flow/network/ipaddress") {
	ASSERT(NetworkAddress::parse("[::1]:4800").toString() == "[::1]:4800");

	{
		auto addr = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800";
		auto addrParsed = NetworkAddress::parse(addr);
		auto addrCompressed = "[2001:db8:85a3::8a2e:370:7334]:4800";
		ASSERT(addrParsed.isV6());
		ASSERT(!addrParsed.isTLS());
		ASSERT(addrParsed.fromHostname == false);
		ASSERT(addrParsed.toString() == addrCompressed);
		ASSERT(addrParsed.toString() == addrCompressed);
	}

	{
		auto addr = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800:tls(fromHostname)";
		auto addrParsed = NetworkAddress::parse(addr);
		auto addrCompressed = "[2001:db8:85a3::8a2e:370:7334]:4800:tls(fromHostname)";
		ASSERT(addrParsed.isV6());
		ASSERT(addrParsed.isTLS());
		ASSERT(addrParsed.fromHostname == true);
		ASSERT(addrParsed.toString() == addrCompressed);
	}

	{
		auto addr = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
		auto addrCompressed = "2001:db8:85a3::8a2e:370:7334";
		auto addrParsed = IPAddress::parse(addr);
		ASSERT(addrParsed.present());
		ASSERT(addrParsed.get().toString() == addrCompressed);
	}

	{
		auto addr = "2001";
		auto addrParsed = IPAddress::parse(addr);
		ASSERT(!addrParsed.present());
	}

	{
		auto addr = "8.8.8.8:12";
		auto addrParsed = IPAddress::parse(addr);
		ASSERT(!addrParsed.present());
	}

	return Void();
}

TEST_CASE("/flow/network/hostname") {
	std::string hn1s = "localhost:1234";
	std::string hn2s = "host-name:1234";
	std::string hn3s = "host.name:1234";
	std::string hn4s = "host-name_part1.host-name_part2:1234:tls";

	std::string hn5s = "127.0.0.1:1234";
	std::string hn6s = "127.0.0.1:1234:tls";
	std::string hn7s = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800";
	std::string hn8s = "[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:4800:tls";
	std::string hn9s = "2001:0db8:85a3:0000:0000:8a2e:0370:7334";
	std::string hn10s = "2001:0db8:85a3:0000:0000:8a2e:0370:7334:tls";
	std::string hn11s = "[::1]:4800";
	std::string hn12s = "[::1]:4800:tls";
	std::string hn13s = "1234";

	auto hn1 = Hostname::parse(hn1s);
	ASSERT(hn1.toString() == hn1s);
	ASSERT(hn1.host == "localhost");
	ASSERT(hn1.service == "1234");
	ASSERT(!hn1.isTLS);

	auto hn2 = Hostname::parse(hn2s);
	ASSERT(hn2.toString() == hn2s);
	ASSERT(hn2.host == "host-name");
	ASSERT(hn2.service == "1234");
	ASSERT(!hn2.isTLS);

	auto hn3 = Hostname::parse(hn3s);
	ASSERT(hn3.toString() == hn3s);
	ASSERT(hn3.host == "host.name");
	ASSERT(hn3.service == "1234");
	ASSERT(!hn3.isTLS);

	auto hn4 = Hostname::parse(hn4s);
	ASSERT(hn4.toString() == hn4s);
	ASSERT(hn4.host == "host-name_part1.host-name_part2");
	ASSERT(hn4.service == "1234");
	ASSERT(hn4.isTLS);

	ASSERT(Hostname::isHostname(hn1s));
	ASSERT(Hostname::isHostname(hn2s));
	ASSERT(Hostname::isHostname(hn3s));
	ASSERT(Hostname::isHostname(hn4s));

	ASSERT(!Hostname::isHostname(hn5s));
	ASSERT(!Hostname::isHostname(hn6s));
	ASSERT(!Hostname::isHostname(hn7s));
	ASSERT(!Hostname::isHostname(hn8s));
	ASSERT(!Hostname::isHostname(hn9s));
	ASSERT(!Hostname::isHostname(hn10s));
	ASSERT(!Hostname::isHostname(hn11s));
	ASSERT(!Hostname::isHostname(hn12s));
	ASSERT(!Hostname::isHostname(hn13s));

	return Void();
}

NetworkInfo::NetworkInfo() : handshakeLock(new FlowLock(FLOW_KNOBS->TLS_HANDSHAKE_LIMIT)) {}
