#ifndef FLOW_IPADDRESS_H
#define FLOW_IPADDRESS_H

#include <array>
#include <variant>

#include "flow/Optional.h"

struct IPAddress {
	// It should be the same to boost::asio::ip::address_v6::bytes_type. This is enforced by static_assert in
	// Platform.actor.cpp.
	using IPAddressStore = std::array<uint8_t, 16>;

public:
	// Represents both IPv4 and IPv6 address. For IPv4 addresses,
	// only the first 32bits are relevant and rest are initialized to 0.
	IPAddress() : addr(uint32_t(0)) {}
	explicit IPAddress(const IPAddressStore& v6addr) : addr(v6addr) {}
	explicit IPAddress(uint32_t v4addr) : addr(v4addr) {}

	bool isV6() const { return std::holds_alternative<IPAddressStore>(addr); }
	bool isV4() const { return !isV6(); }
	bool isValid() const;

	// Returns raw v4/v6 representation of address. Caller is responsible
	// to call these functions safely.
	uint32_t toV4() const { return std::get<uint32_t>(addr); }
	const IPAddressStore& toV6() const { return std::get<IPAddressStore>(addr); }

	std::string toString() const;
	static Optional<IPAddress> parse(std::string const& str);

	bool operator==(const IPAddress& addr) const;
	bool operator!=(const IPAddress& addr) const;
	bool operator<(const IPAddress& addr) const;

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (is_fb_function<Ar>) {
			serializer(ar, addr);
		} else {
			if (Ar::isDeserializing) {
				bool v6;
				serializer(ar, v6);
				if (v6) {
					IPAddressStore store;
					serializer(ar, store);
					addr = store;
				} else {
					uint32_t res;
					serializer(ar, res);
					addr = res;
				}
			} else {
				bool v6 = isV6();
				serializer(ar, v6);
				if (v6) {
					auto res = toV6();
					serializer(ar, res);
				} else {
					auto res = toV4();
					serializer(ar, res);
				}
			}
		}
	}

private:
	std::variant<uint32_t, IPAddressStore> addr;
};

template <>
struct Traceable<IPAddress> : std::true_type {
	static std::string toString(const IPAddress& value) { return value.toString(); }
};

#endif // FLOW_IPADDRESS_H