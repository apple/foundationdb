/*
 * NetworkAddress.h
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

#ifndef FLOW_NETWORKADDRESS_H
#define FLOW_NETWORKADDRESS_H

#include "flow/BooleanParam.h"
#include "flow/Trace.h"
#include "flow/IPAddress.h"

FDB_BOOLEAN_PARAM(NetworkAddressFromHostname);

struct NetworkAddress {
	constexpr static FileIdentifier file_identifier = 14155727;
	// A NetworkAddress identifies a particular running server (i.e. a TCP endpoint).
	IPAddress ip;
	uint16_t port;
	uint16_t flags;
	bool fromHostname;

	enum { FLAG_PRIVATE = 1, FLAG_TLS = 2 };

	NetworkAddress()
	  : ip(IPAddress(0)), port(0), flags(FLAG_PRIVATE), fromHostname(NetworkAddressFromHostname::False) {}
	NetworkAddress(const IPAddress& address,
	               uint16_t port,
	               bool isPublic,
	               bool isTLS,
	               NetworkAddressFromHostname fromHostname = NetworkAddressFromHostname::False)
	  : ip(address), port(port), flags((isPublic ? 0 : FLAG_PRIVATE) | (isTLS ? FLAG_TLS : 0)),
	    fromHostname(fromHostname) {}
	NetworkAddress(uint32_t ip,
	               uint16_t port,
	               bool isPublic,
	               bool isTLS,
	               NetworkAddressFromHostname fromHostname = NetworkAddressFromHostname::False)
	  : NetworkAddress(IPAddress(ip), port, isPublic, isTLS, fromHostname) {}

	NetworkAddress(uint32_t ip, uint16_t port)
	  : NetworkAddress(ip, port, false, false, NetworkAddressFromHostname::False) {}
	NetworkAddress(const IPAddress& ip, uint16_t port)
	  : NetworkAddress(ip, port, false, false, NetworkAddressFromHostname::False) {}

	bool operator==(NetworkAddress const& r) const { return ip == r.ip && port == r.port && flags == r.flags; }
	bool operator!=(NetworkAddress const& r) const { return !(*this == r); }
	bool operator<(NetworkAddress const& r) const {
		if (flags != r.flags)
			return flags < r.flags;
		else if (ip != r.ip)
			return ip < r.ip;
		return port < r.port;
	}
	bool operator>(NetworkAddress const& r) const { return r < *this; }
	bool operator<=(NetworkAddress const& r) const { return !(*this > r); }
	bool operator>=(NetworkAddress const& r) const { return !(*this < r); }

	bool isValid() const { return ip.isValid() || port != 0; }
	bool isPublic() const { return !(flags & FLAG_PRIVATE); }
	bool isTLS() const { return (flags & FLAG_TLS) != 0; }
	bool isV6() const { return ip.isV6(); }

	size_t hash() const {
		size_t result = 0;
		if (ip.isV6()) {
			uint16_t* ptr = (uint16_t*)ip.toV6().data();
			result = ((size_t)ptr[5] << 32) | ((size_t)ptr[6] << 16) | ptr[7];
		} else {
			result = ip.toV4();
		}
		return (result << 16) + port;
	}

	static NetworkAddress parse(std::string const&); // May throw connection_string_invalid
	static Optional<NetworkAddress> parseOptional(std::string const&);
	static std::vector<NetworkAddress> parseList(std::string const&);
	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (is_fb_function<Ar>) {
			serializer(ar, ip, port, flags, fromHostname);
		} else {
			if (ar.isDeserializing && !ar.protocolVersion().hasIPv6()) {
				uint32_t ipV4;
				serializer(ar, ipV4, port, flags);
				ip = IPAddress(ipV4);
			} else {
				serializer(ar, ip, port, flags);
			}
			if (ar.protocolVersion().hasNetworkAddressHostnameFlag()) {
				serializer(ar, fromHostname);
			}
		}
	}
};

template <>
struct Traceable<NetworkAddress> : std::true_type {
	static std::string toString(const NetworkAddress& value) { return value.toString(); }
};

namespace std {
template <>
struct hash<NetworkAddress> {
	size_t operator()(const NetworkAddress& na) const { return na.hash(); }
};
} // namespace std

struct NetworkAddressList {
	NetworkAddress address;
	Optional<NetworkAddress> secondaryAddress{};

	bool operator==(NetworkAddressList const& r) const {
		return address == r.address && secondaryAddress == r.secondaryAddress;
	}
	bool operator!=(NetworkAddressList const& r) const {
		return address != r.address || secondaryAddress != r.secondaryAddress;
	}
	bool operator<(NetworkAddressList const& r) const {
		if (address != r.address)
			return address < r.address;
		return secondaryAddress < r.secondaryAddress;
	}

	NetworkAddress getTLSAddress() const {
		if (!secondaryAddress.present() || address.isTLS()) {
			return address;
		}
		return secondaryAddress.get();
	}

	std::string toString() const {
		if (!secondaryAddress.present()) {
			return address.toString();
		}
		return address.toString() + ", " + secondaryAddress.get().toString();
	}

	bool contains(const NetworkAddress& r) const {
		return address == r || (secondaryAddress.present() && secondaryAddress.get() == r);
	}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, address, secondaryAddress);
	}
};

extern std::string formatIpPort(const IPAddress& ip, uint16_t port);

struct AddressExclusion {
	IPAddress ip;
	int port;

	AddressExclusion() : ip(0), port(0) {}
	explicit AddressExclusion(const IPAddress& ip) : ip(ip), port(0) {}
	explicit AddressExclusion(const IPAddress& ip, int port) : ip(ip), port(port) {}

	bool operator<(AddressExclusion const& r) const {
		if (ip != r.ip)
			return ip < r.ip;
		return port < r.port;
	}
	bool operator==(AddressExclusion const& r) const { return ip == r.ip && port == r.port; }

	bool isWholeMachine() const { return port == 0; }
	bool isValid() const { return ip.isValid() || port != 0; }

	bool excludes(NetworkAddress const& addr) const {
		if (isWholeMachine())
			return ip == addr.ip;
		return ip == addr.ip && port == addr.port;
	}

	// This is for debugging and IS NOT to be used for serialization to persistent state
	std::string toString() const {
		if (!isWholeMachine())
			return formatIpPort(ip, port);
		return ip.toString();
	}

	static AddressExclusion parse(StringRef const&);

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, ip, port);
	}
};

#endif // FLOW_NETWORKADDRESS_H
