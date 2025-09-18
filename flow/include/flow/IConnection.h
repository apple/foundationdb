/*
 * IConnection.h
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

#ifndef FLOW_ICONNECTION_H
#define FLOW_ICONNECTION_H

#include <cstdint>
#include <limits>

#include <boost/asio/ip/tcp.hpp>

#include "flow/Knobs.h"
#include "flow/NetworkAddress.h"
#include "flow/network.h"

class Void;

template <typename T>
class Future;

// forward declare SendBuffer, defined in serialize.h
class SendBuffer;

class IConnection {
public:
	// IConnection is reference-counted (use Reference<IConnection>), but the caller must explicitly call close()
	virtual void addref() = 0;
	virtual void delref() = 0;

	// Closes the underlying connection eventually if it is not already closed.
	virtual void close() = 0;

	virtual Future<Void> acceptHandshake() = 0;

	virtual Future<Void> connectHandshake() = 0;

	// Precondition: write() has been called and last returned 0
	// returns when write() can write at least one byte (or may throw an error if the connection dies)
	virtual Future<Void> onWritable() = 0;

	// Precondition: read() has been called and last returned 0
	// returns when read() can read at least one byte (or may throw an error if the connection dies)
	virtual Future<Void> onReadable() = 0;

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might
	// be 0) (or may throw an error if the connection dies)
	virtual int read(uint8_t* begin, uint8_t* end) = 0;

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of
	// bytes written (might be 0) (or may throw an error if the connection dies) The SendBuffer chain cannot be empty,
	// and the limit must be positive. Important non-obvious behavior:  The caller is committing to write the contents
	// of the buffer chain up to the limit.  If all of those bytes could not be sent in this call to write() then
	// further calls must be made to write the remainder.  An IConnection implementation can make decisions based on the
	// entire byte set that the caller was attempting to write even if it is unable to write all of it immediately. Due
	// to limitations of TLSConnection, callers must also avoid reallocations that reduce the amount of written data in
	// the first buffer in the chain.
	virtual int write(SendBuffer const* buffer, int limit = std::numeric_limits<int>::max()) = 0;

	// Returns the network address and port of the other end of the connection.  In the case of an incoming connection,
	// this may not be an address we can connect to!
	virtual NetworkAddress getPeerAddress() const = 0;

	// Returns whether the peer is trusted.
	// For TLS-enabled connections, this is true if the peer has presented a valid chain of certificates trusted by the
	// local endpoint. For non-TLS connections this is always true for any valid open connection.
	virtual bool hasTrustedPeer() const = 0;

	virtual UID getDebugID() const = 0;

	// At present, implemented by Sim2Conn where we want to disable bits flip for connections between parent process and
	// child process, also reduce latency for this kind of connection
	virtual bool isStableConnection() const { throw unsupported_operation(); }

	virtual boost::asio::ip::tcp::socket& getSocket() = 0;
};

class IListener {
public:
	virtual void addref() = 0;
	virtual void delref() = 0;

	// Returns one incoming connection when it is available.  Do not cancel unless you are done with the listener!
	virtual Future<Reference<IConnection>> accept() = 0;

	virtual NetworkAddress getListenAddress() const = 0;
};

// DNSCache is a class maintaining a <hostname, vector<NetworkAddress>> mapping.
class DNSCache {
public:
	DNSCache() = default;
	explicit DNSCache(const std::map<std::string, std::vector<NetworkAddress>>& dnsCache)
	  : hostnameToAddresses(dnsCache) {}

	Optional<std::vector<NetworkAddress>> find(const std::string& host, const std::string& service);
	void add(const std::string& host, const std::string& service, const std::vector<NetworkAddress>& addresses);
	void remove(const std::string& host, const std::string& service);
	void clear();

	// Convert hostnameToAddresses to string. The format is:
	// hostname1,host1Address1,host1Address2;hostname2,host2Address1,host2Address2...
	std::string toString();
	static DNSCache parseFromString(const std::string& s);

private:
	std::map<std::string, std::vector<NetworkAddress>> hostnameToAddresses;
};

class IUDPSocket;

class INetworkConnections {
public:
	// Methods for making and accepting network connections.  Logically this is part of the INetwork abstraction
	// that abstracts all interaction with the physical world; it is separated out to make it easy for e.g. transport
	// security to override only these operations without having to delegate everything in INetwork.

	// Make an outgoing connection to the given address.  May return an error or block indefinitely in case of
	// connection problems!
	virtual Future<Reference<IConnection>> connect(NetworkAddress toAddr,
	                                               boost::asio::ip::tcp::socket* existingSocket = nullptr) = 0;

	virtual Future<Reference<IConnection>> connectExternal(NetworkAddress toAddr) = 0;

	// Make an outgoing connection to the given address with hostname for SNI (TLS Server Name Indication)
	virtual Future<Reference<IConnection>> connectExternalWithHostname(NetworkAddress toAddr,
	                                                                   const std::string& hostname) {
		// Default implementation ignores hostname - subclasses can override for SNI support
		return connectExternal(toAddr);
	}

	// Make an outgoing udp connection and connect to the passed address.
	virtual Future<Reference<IUDPSocket>> createUDPSocket(NetworkAddress toAddr) = 0;
	// Make an outgoing udp connection without establishing a connection
	virtual Future<Reference<IUDPSocket>> createUDPSocket(bool isV6 = false) = 0;

	virtual void addMockTCPEndpoint(const std::string& host,
	                                const std::string& service,
	                                const std::vector<NetworkAddress>& addresses) = 0;
	virtual void removeMockTCPEndpoint(const std::string& host, const std::string& service) = 0;
	virtual void parseMockDNSFromString(const std::string& s) = 0;
	virtual std::string convertMockDNSToString() = 0;
	// Resolve host name and service name (such as "http" or can be a plain number like "80") to a list of 1 or more
	// NetworkAddresses
	virtual Future<std::vector<NetworkAddress>> resolveTCPEndpoint(const std::string& host,
	                                                               const std::string& service) = 0;
	// Similar to resolveTCPEndpoint(), except that this one uses DNS cache.
	virtual Future<std::vector<NetworkAddress>> resolveTCPEndpointWithDNSCache(const std::string& host,
	                                                                           const std::string& service) = 0;
	// Resolve host name and service name. This one should only be used when resolving asynchronously is impossible. For
	// all other cases, resolveTCPEndpoint() should be preferred.
	virtual std::vector<NetworkAddress> resolveTCPEndpointBlocking(const std::string& host,
	                                                               const std::string& service) = 0;
	// Resolve host name and service name with DNS cache. This one should only be used when resolving asynchronously is
	// impossible. For all other cases, resolveTCPEndpointWithDNSCache() should be preferred.
	virtual std::vector<NetworkAddress> resolveTCPEndpointBlockingWithDNSCache(const std::string& host,
	                                                                           const std::string& service) = 0;

	// Convenience function to resolve host/service and connect to one of its NetworkAddresses randomly
	// isTLS has to be a parameter here because it is passed to connect() as part of the toAddr object.
	virtual Future<Reference<IConnection>> connect(const std::string& host,
	                                               const std::string& service,
	                                               bool isTLS = false);

	// Listen for connections on the given local address
	virtual Reference<IListener> listen(NetworkAddress localAddr) = 0;

	static INetworkConnections* net() {
		return static_cast<INetworkConnections*>((void*)g_network->global(INetwork::enNetworkConnections));
	}

	// If a DNS name can be resolved to both and IPv4 and IPv6 addresses, we want IPv6 addresses when running the
	// clusters on IPv6.
	// This function takes a vector of addresses and return a random one, preferring IPv6 over IPv4.
	// To prefer IPv4 addresses instead, set knob RESOLVE_PREFER_IPV4_ADDR to true.
	static NetworkAddress pickOneAddress(const std::vector<NetworkAddress>& addresses) {
		std::vector<NetworkAddress> ipV6Addresses;
		std::vector<NetworkAddress> ipV4Addresses;
		for (const NetworkAddress& addr : addresses) {
			if (addr.isV6()) {
				ipV6Addresses.push_back(addr);
			} else {
				ipV4Addresses.push_back(addr);
			}
		}
		if (ipV4Addresses.size() > 0 && FLOW_KNOBS->RESOLVE_PREFER_IPV4_ADDR) {
			return ipV4Addresses[deterministicRandom()->randomInt(0, ipV4Addresses.size())];
		}
		if (ipV6Addresses.size() > 0) {
			return ipV6Addresses[deterministicRandom()->randomInt(0, ipV6Addresses.size())];
		}
		return addresses[deterministicRandom()->randomInt(0, addresses.size())];
	}

	void removeCachedDNS(const std::string& host, const std::string& service) { dnsCache.remove(host, service); }

	DNSCache dnsCache;

	// Returns the interface that should be used to make and accept socket connections
};

#endif // FLOW_ICONNECTION_H
