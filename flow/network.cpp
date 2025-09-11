/*
 * network.cpp
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
#include <boost/asio.hpp>

#include "flow/Arena.h"
#include "flow/network.h"
#include "flow/IUDPSocket.h"
#include "flow/flow.h"
#include "flow/ChaosMetrics.h"
#include "flow/UnitTest.h"
#include "flow/IConnection.h"

ChaosMetrics::ChaosMetrics() {
	clear();
}

void ChaosMetrics::clear() {
	std::memset(this, 0, sizeof(ChaosMetrics));
	startTime = g_network ? g_network->now() : 0;
}

void ChaosMetrics::getFields(TraceEvent* e) {
	std::pair<const char*, unsigned int> metrics[] = { { "DiskDelays", diskDelays }, { "BitFlips", bitFlips } };
	if (e != nullptr) {
		for (auto& m : metrics) {
			char c = m.first[0];
			if (c != 0) {
				e->detail(m.first, m.second);
			}
		}
	}
}

DiskFailureInjector* DiskFailureInjector::injector() {
	auto res = g_network->global(INetwork::enDiskFailureInjector);
	if (!res) {
		res = new DiskFailureInjector();
		g_network->setGlobal(INetwork::enDiskFailureInjector, res);
	}
	return static_cast<DiskFailureInjector*>(res);
}

void DiskFailureInjector::setDiskFailure(double interval, double stallFor, double throttleFor) {
	stallInterval = interval;
	stallPeriod = stallFor;
	stallUntil = std::max(stallUntil, g_network->now() + stallFor);
	// random stall duration in ms (chosen once)
	// TODO: make this delay configurable
	stallDuration = 0.001 * deterministicRandom()->randomInt(1, 5);
	throttlePeriod = throttleFor;
	throttleUntil = std::max(throttleUntil, g_network->now() + throttleFor);
	TraceEvent("SetDiskFailure")
	    .detail("Now", g_network->now())
	    .detail("StallInterval", interval)
	    .detail("StallPeriod", stallFor)
	    .detail("StallUntil", stallUntil)
	    .detail("ThrottlePeriod", throttleFor)
	    .detail("ThrottleUntil", throttleUntil);
}

double DiskFailureInjector::getStallDelay() const {
	// If we are in a stall period and a stallInterval was specified, determine the
	// delay to be inserted
	if (((stallUntil - g_network->now()) > 0.0) && stallInterval) {
		auto timeElapsed = fmod(g_network->now(), stallInterval);
		return std::max(0.0, stallDuration - timeElapsed);
	}
	return 0.0;
}

double DiskFailureInjector::getThrottleDelay() const {
	// If we are in the throttle period, insert a random delay (in ms)
	// TODO: make this delay configurable
	if ((throttleUntil - g_network->now()) > 0.0)
		return (0.001 * deterministicRandom()->randomInt(1, 3));

	return 0.0;
}

double DiskFailureInjector::getDiskDelay() const {
	return getStallDelay() + getThrottleDelay();
}

BitFlipper* BitFlipper::flipper() {
	auto res = g_network->global(INetwork::enBitFlipper);
	if (!res) {
		res = new BitFlipper();
		g_network->setGlobal(INetwork::enBitFlipper, res);
	}
	return static_cast<BitFlipper*>(res);
}

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

Optional<std::vector<NetworkAddress>> DNSCache::find(const std::string& host, const std::string& service) {
	auto it = hostnameToAddresses.find(host + ":" + service);
	if (it != hostnameToAddresses.end()) {
		return it->second;
	}
	return {};
}

void DNSCache::add(const std::string& host, const std::string& service, const std::vector<NetworkAddress>& addresses) {
	hostnameToAddresses[host + ":" + service] = addresses;
}

void DNSCache::remove(const std::string& host, const std::string& service) {
	auto it = hostnameToAddresses.find(host + ":" + service);
	if (it != hostnameToAddresses.end()) {
		hostnameToAddresses.erase(it);
	}
}

void DNSCache::clear() {
	hostnameToAddresses.clear();
}

std::string DNSCache::toString() {
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

DNSCache DNSCache::parseFromString(const std::string& s) {
	std::map<std::string, std::vector<NetworkAddress>> dnsCache;

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
		dnsCache[hostname] = addresses;
		p = pSemiColumn + 1;
	}

	return DNSCache(dnsCache);
}

TEST_CASE("/flow/DNSCache") {
	DNSCache dnsCache;
	std::vector<NetworkAddress> networkAddresses;
	NetworkAddress address1(IPAddress(0x13131313), 1), address2(IPAddress(0x14141414), 2);
	networkAddresses.push_back(address1);
	networkAddresses.push_back(address2);
	dnsCache.add("testhost1", "port1", networkAddresses);
	ASSERT(dnsCache.find("testhost1", "port1").present());
	ASSERT(!dnsCache.find("testhost1", "port2").present());
	std::vector<NetworkAddress> resolvedNetworkAddresses = dnsCache.find("testhost1", "port1").get();
	ASSERT(resolvedNetworkAddresses.size() == 2);
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address1) !=
	       resolvedNetworkAddresses.end());
	ASSERT(std::find(resolvedNetworkAddresses.begin(), resolvedNetworkAddresses.end(), address2) !=
	       resolvedNetworkAddresses.end());
	dnsCache.remove("testhost1", "port1");
	ASSERT(!dnsCache.find("testhost1", "port1").present());
	dnsCache.add("testhost1", "port2", networkAddresses);
	ASSERT(dnsCache.find("testhost1", "port2").present());
	dnsCache.clear();
	ASSERT(!dnsCache.find("testhost1", "port2").present());

	return Void();
}

TEST_CASE("/flow/DNSCacheParsing") {
	std::string dnsCacheString;
	ASSERT(DNSCache::parseFromString(dnsCacheString).toString() == dnsCacheString);

	dnsCacheString = "testhost1:port1,[::1]:4800:tls(fromHostname)";
	ASSERT(DNSCache::parseFromString(dnsCacheString).toString() == dnsCacheString);

	dnsCacheString = "testhost1:port1,[::1]:4800,[2001:db8:85a3::8a2e:370:7334]:4800;testhost2:port2,[2001:db8:85a3::"
	                 "8a2e:370:7334]:4800:tls(fromHostname),8.8.8.8:12";
	ASSERT(DNSCache::parseFromString(dnsCacheString).toString() == dnsCacheString);

	return Void();
}

Future<Reference<IConnection>> INetworkConnections::connect(const std::string& host,
                                                            const std::string& service,
                                                            bool isTLS) {
	// Use map to create an actor that returns an endpoint or throws
	Future<NetworkAddress> pickEndpoint =
	    map(resolveTCPEndpoint(host, service), [=](std::vector<NetworkAddress> const& addresses) -> NetworkAddress {
		    NetworkAddress addr = INetworkConnections::pickOneAddress(addresses);
		    addr.fromHostname = true;
		    if (isTLS) {
			    addr.flags = NetworkAddress::FLAG_TLS;
		    }
		    return addr;
	    });

	// Wait for the endpoint to return, then wait for connect(endpoint) and return it.
	// Template types are being provided explicitly because they can't be automatically deduced for some reason.
	return mapAsync(pickEndpoint, [=](NetworkAddress const& addr) -> Future<Reference<IConnection>> {
		// Pass the original hostname for SNI if this is a TLS connection from hostname
		if (addr.isTLS() && addr.fromHostname) {
			return connectExternalWithHostname(addr, host);
		}
		return connectExternal(addr);
	});
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

TEST_CASE("/flow/network/ipV6Preferred") {
	std::vector<NetworkAddress> addresses;
	for (int i = 0; i < 50; ++i) {
		std::string s = fmt::format("{}.{}.{}.{}:{}", i, i, i, i, i);
		addresses.push_back(NetworkAddress::parse(s));
	}
	std::string ipv6 = "[2001:db8:85a3::8a2e:370:7334]:4800";
	addresses.push_back(NetworkAddress::parse(ipv6));
	for (int i = 50; i < 100; ++i) {
		std::string s = fmt::format("{}.{}.{}.{}:{}", i, i, i, i, i);
		addresses.push_back(NetworkAddress::parse(s));
	}
	// Confirm IPv6 is always preferred.
	ASSERT((INetworkConnections::pickOneAddress(addresses).toString() == ipv6) ==
	       !FLOW_KNOBS->RESOLVE_PREFER_IPV4_ADDR);

	return Void();
}

NetworkInfo::NetworkInfo() : handshakeLock(new FlowLock(FLOW_KNOBS->TLS_HANDSHAKE_LIMIT)) {}
NetworkInfo::~NetworkInfo() {
	delete handshakeLock;
}
