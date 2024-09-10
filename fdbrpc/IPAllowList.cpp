/*
 * IPAllowList.cpp
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

#include "flow/UnitTest.h"
#include "flow/Error.h"
#include "fdbrpc/IPAllowList.h"

#include <fmt/format.h>
#include <fmt/printf.h>
#include <fmt/ranges.h>
#include <bitset>

namespace {

template <std::size_t C>
std::string binRep(std::array<unsigned char, C> const& addr) {
	return fmt::format("{:02x}", fmt::join(addr, ":"));
}

template <std::size_t C>
void printIP(std::array<unsigned char, C> const& addr) {
	fmt::print(" {}", binRep(addr));
}

template <size_t Sz>
int netmaskWeightImpl(std::array<unsigned char, Sz> const& addr) {
	int count = 0;
	for (int i = 0; i < addr.size() && addr[i] != 0xff; ++i) {
		std::bitset<8> b(addr[i]);
		count += 8 - b.count();
	}
	return count;
}

} // namespace

AuthAllowedSubnet::AuthAllowedSubnet(IPAddress const& baseAddress, IPAddress const& addressMask)
  : baseAddress(baseAddress), addressMask(addressMask) {
	ASSERT(baseAddress.isV4() == addressMask.isV4());
}

IPAddress AuthAllowedSubnet::netmask() const {
	if (addressMask.isV4()) {
		uint32_t res = 0xffffffff ^ addressMask.toV4();
		return IPAddress(res);
	} else {
		std::array<unsigned char, 16> res;
		res.fill(0xff);
		auto mask = addressMask.toV6();
		for (int i = 0; i < mask.size(); ++i) {
			res[i] ^= mask[i];
		}
		return IPAddress(res);
	}
}

int AuthAllowedSubnet::netmaskWeight() const {
	if (addressMask.isV4()) {
		boost::asio::ip::address_v4 addr(netmask().toV4());
		return netmaskWeightImpl(addr.to_bytes());
	} else {
		return netmaskWeightImpl(netmask().toV6());
	}
}

AuthAllowedSubnet AuthAllowedSubnet::fromString(std::string_view addressString) {
	auto pos = addressString.find('/');
	if (pos == std::string_view::npos) {
		fmt::print("ERROR: {} is not a valid (use Network-Prefix/netmaskWeight syntax)\n", addressString);
		throw invalid_option();
	}
	auto address = addressString.substr(0, pos);
	auto netmaskWeight = std::stoi(std::string(addressString.substr(pos + 1)));
	auto addr = boost::asio::ip::make_address(address);
	if (addr.is_v4()) {
		auto bM = createBitMask(addr.to_v4().to_bytes(), netmaskWeight);
		// we typically would expect a base address has been passed, but to be safe we still
		// will make the last bits 0
		auto mask = boost::asio::ip::address_v4(bM).to_uint();
		auto baseAddress = addr.to_v4().to_uint() & mask;
		return AuthAllowedSubnet(IPAddress(baseAddress), IPAddress(mask));
	} else {
		auto mask = createBitMask(addr.to_v6().to_bytes(), netmaskWeight);
		auto baseAddress = addr.to_v6().to_bytes();
		for (int i = 0; i < mask.size(); ++i) {
			baseAddress[i] &= mask[i];
		}
		return AuthAllowedSubnet(IPAddress(baseAddress), IPAddress(mask));
	}
}

void AuthAllowedSubnet::printIP(std::string_view txt, IPAddress const& address) {
	fmt::print("{}:", txt);
	if (address.isV4()) {
		::printIP(boost::asio::ip::address_v4(address.toV4()).to_bytes());
	} else {
		::printIP(address.toV6());
	}
	fmt::print("\n");
}

template <std::size_t sz>
std::array<unsigned char, sz> AuthAllowedSubnet::createBitMask(std::array<unsigned char, sz> const& addr,
                                                               int netmaskWeight) {
	std::array<unsigned char, sz> res;
	res.fill((unsigned char)0xff);
	int idx = netmaskWeight / 8;
	if (netmaskWeight % 8 > 0) {
		// 2^(netmaskWeight % 8) - 1 sets the last (netmaskWeight % 8) number of bits to 1
		// everything else will be zero. For example: 2^3 - 1 == 7 == 0b111
		unsigned char bitmask = (1 << (8 - (netmaskWeight % 8))) - ((unsigned char)1);
		res[idx] ^= bitmask;
		++idx;
	}
	for (; idx < res.size(); ++idx) {
		res[idx] = (unsigned char)0;
	}
	return res;
}

template std::array<unsigned char, 4> AuthAllowedSubnet::createBitMask<4>(const std::array<unsigned char, 4>& addr,
                                                                          int netmaskWeight);
template std::array<unsigned char, 16> AuthAllowedSubnet::createBitMask<16>(const std::array<unsigned char, 16>& addr,
                                                                            int netmaskWeight);

// helpers for testing
namespace {
using boost::asio::ip::address_v4;
using boost::asio::ip::address_v6;

void traceAddress(TraceEvent& evt, const char* name, IPAddress address) {
	evt.detail(name, address);
	std::string bin;
	if (address.isV4()) {
		boost::asio::ip::address_v4 a(address.toV4());
		bin = binRep(a.to_bytes());
	} else {
		bin = binRep(address.toV6());
	}
	evt.detail(fmt::format("{}Binary", name).c_str(), bin);
}

void subnetAssert(IPAllowList const& allowList, IPAddress addr, bool expectAllowed) {
	if (allowList(addr) == expectAllowed) {
		return;
	}
	TraceEvent evt(SevError, expectAllowed ? "ExpectedAddressToBeTrusted" : "ExpectedAddressToBeUntrusted");
	traceAddress(evt, "Address", addr);
	auto const& subnets = allowList.subnets();
	for (int i = 0; i < subnets.size(); ++i) {
		traceAddress(evt, fmt::format("SubnetBase{}", i).c_str(), subnets[i].baseAddress);
		traceAddress(evt, fmt::format("SubnetMask{}", i).c_str(), subnets[i].addressMask);
	}
}

IPAddress parseAddr(std::string const& str) {
	auto res = IPAddress::parse(str);
	ASSERT(res.present());
	return res.get();
}

struct SubNetTest {
	AuthAllowedSubnet subnet;
	SubNetTest(AuthAllowedSubnet&& subnet) : subnet(std::move(subnet)) {}
	SubNetTest(AuthAllowedSubnet const& subnet) : subnet(subnet) {}
	template <bool V4>
	static SubNetTest randomSubNetImpl() {
		constexpr int width = V4 ? 4 : 16;
		std::array<unsigned char, width> binAddr;
		unsigned char rnd[4];
		for (int i = 0; i < binAddr.size(); ++i) {
			if (i % 4 == 0) {
				auto tmp = deterministicRandom()->randomUInt32();
				::memcpy(rnd, &tmp, 4);
			}
			binAddr[i] = rnd[i % 4];
		}
		auto netmaskWeight = deterministicRandom()->randomInt(1, width);
		std::string address;
		if constexpr (V4) {
			address_v4 a(binAddr);
			address = a.to_string();
		} else {
			address_v6 a(binAddr);
			address = a.to_string();
		}
		return SubNetTest(AuthAllowedSubnet::fromString(fmt::format("{}/{}", address, netmaskWeight)));
	}
	static SubNetTest randomSubNet() {
		if (deterministicRandom()->coinflip()) {
			return randomSubNetImpl<true>();
		} else {
			return randomSubNetImpl<false>();
		}
	}

	template <bool V4>
	static IPAddress intArrayToAddress(uint32_t* arr) {
		if constexpr (V4) {
			return IPAddress(arr[0]);
		} else {
			std::array<unsigned char, 16> res;
			memcpy(res.data(), arr, 16);
			return IPAddress(res);
		}
	}

	template <class I>
	I transformIntToSubnet(I val, I subnetMask, I baseAddress) {
		return (val & subnetMask) ^ baseAddress;
	}

	template <bool V4>
	static IPAddress randomAddress() {
		constexpr int width = V4 ? 4 : 16;
		uint32_t rnd[width / 4];
		for (int i = 0; i < width / 4; ++i) {
			rnd[i] = deterministicRandom()->randomUInt32();
		}
		return intArrayToAddress<V4>(rnd);
	}

	template <bool V4>
	IPAddress randomAddress(bool inSubnet) {
		ASSERT(V4 == subnet.baseAddress.isV4() || !inSubnet);
		for (;;) {
			auto res = randomAddress<V4>();
			if (V4 != subnet.baseAddress.isV4()) {
				return res;
			}
			if (!inSubnet) {
				if (!subnet(res)) {
					return res;
				} else {
					continue;
				}
			}
			// first we make sure the address is in the subnet
			if constexpr (V4) {
				auto a = res.toV4();
				auto base = subnet.baseAddress.toV4();
				auto netmask = subnet.netmask().toV4();
				auto validAddress = transformIntToSubnet(a, netmask, base);
				res = IPAddress(validAddress);
			} else {
				auto a = res.toV6();
				auto base = subnet.baseAddress.toV6();
				auto netmask = subnet.netmask().toV6();
				for (int i = 0; i < a.size(); ++i) {
					a[i] = transformIntToSubnet(a[i], netmask[i], base[i]);
				}
				res = IPAddress(a);
			}
			return res;
		}
	}

	IPAddress randomAddress(bool inSubnet) {
		if (!inSubnet && deterministicRandom()->random01() < 0.1) {
			// return an address of a different type
			if (subnet.baseAddress.isV4()) {
				return randomAddress<false>(false);
			} else {
				return randomAddress<true>(false);
			}
		}
		if (subnet.addressMask.isV4()) {
			return randomAddress<true>(inSubnet);
		} else {
			return randomAddress<false>(inSubnet);
		}
	}
};

} // namespace

TEST_CASE("/fdbrpc/allow_list") {
	// test correct weight calculation
	// IPv4
	for (int i = 0; i < 33; ++i) {
		auto str = fmt::format("0.0.0.0/{}", i);
		auto subnet = AuthAllowedSubnet::fromString(str);
		if (i != subnet.netmaskWeight()) {
			fmt::print("Wrong calculated weight {} for {}\n", subnet.netmaskWeight(), str);
			fmt::print("\tBase address: {}\n", subnet.baseAddress.toString());
			fmt::print("\tAddress Mask: {}\n", subnet.addressMask.toString());
			fmt::print("\tNetmask: {}\n", subnet.netmask().toString());
			ASSERT_EQ(i, subnet.netmaskWeight());
		}
	}
	// IPv6
	for (int i = 0; i < 129; ++i) {
		auto subnet = AuthAllowedSubnet::fromString(fmt::format("0::/{}", i));
		ASSERT_EQ(i, subnet.netmaskWeight());
	}
	IPAllowList allowList;
	// Simulated v4 addresses
	allowList.addTrustedSubnet("1.0.0.0/8");
	allowList.addTrustedSubnet("2.0.0.0/4");
	::subnetAssert(allowList, parseAddr("1.0.1.1"), true);
	::subnetAssert(allowList, parseAddr("1.1.2.2"), true);
	::subnetAssert(allowList, parseAddr("2.2.1.1"), true);
	::subnetAssert(allowList, parseAddr("128.0.1.1"), false);
	allowList = IPAllowList();
	allowList.addTrustedSubnet("0.0.0.0/2");
	allowList.addTrustedSubnet("abcd::/16");
	::subnetAssert(allowList, parseAddr("1.0.1.1"), true);
	::subnetAssert(allowList, parseAddr("1.1.2.2"), true);
	::subnetAssert(allowList, parseAddr("2.2.1.1"), true);
	::subnetAssert(allowList, parseAddr("4.0.1.2"), true);
	::subnetAssert(allowList, parseAddr("5.2.1.1"), true);
	::subnetAssert(allowList, parseAddr("128.0.1.1"), false);
	::subnetAssert(allowList, parseAddr("192.168.3.1"), false);
	// Simulated v6 addresses
	::subnetAssert(allowList, parseAddr("abcd::1:2:3:4"), true);
	::subnetAssert(allowList, parseAddr("abcd::2:3:3:4"), true);
	::subnetAssert(allowList, parseAddr("abcd:ab:ab:fdb:2:3:3:4"), true);
	::subnetAssert(allowList, parseAddr("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:12"), false);
	::subnetAssert(allowList, parseAddr("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:1"), false);
	::subnetAssert(allowList, parseAddr("2001:fdb1:fdb2:fdb3:fdb4:fdb5:fdb6:fdb"), false);
	// Corner Cases
	allowList = IPAllowList();
	allowList.addTrustedSubnet("0.0.0.0/0");
	// Random address tests
	SubNetTest subnetTest(allowList.subnets()[0]);
	for (int i = 0; i < 10; ++i) {
		// All IPv4 addresses are in the allow list
		::subnetAssert(allowList, subnetTest.randomAddress<true>(), true);
		// No IPv6 addresses are in the allow list
		::subnetAssert(allowList, subnetTest.randomAddress<false>(), false);
	}
	allowList = IPAllowList();
	allowList.addTrustedSubnet("::/0");
	subnetTest = SubNetTest(allowList.subnets()[0]);
	for (int i = 0; i < 10; ++i) {
		// All IPv6 addresses are in the allow list
		::subnetAssert(allowList, subnetTest.randomAddress<false>(), true);
		// No IPv4 addresses are ub the allow list
		::subnetAssert(allowList, subnetTest.randomAddress<true>(), false);
	}
	allowList = IPAllowList();
	IPAddress baseAddress = SubNetTest::randomAddress<true>();
	allowList.addTrustedSubnet(fmt::format("{}/32", baseAddress.toString()));
	for (int i = 0; i < 10; ++i) {
		auto rnd = SubNetTest::randomAddress<true>();
		::subnetAssert(allowList, rnd, rnd == baseAddress);
		rnd = SubNetTest::randomAddress<false>();
		::subnetAssert(allowList, rnd, false);
	}
	allowList = IPAllowList();
	baseAddress = SubNetTest::randomAddress<false>();
	allowList.addTrustedSubnet(fmt::format("{}/128", baseAddress.toString()));
	for (int i = 0; i < 10; ++i) {
		auto rnd = SubNetTest::randomAddress<false>();
		::subnetAssert(allowList, rnd, rnd == baseAddress);
		rnd = SubNetTest::randomAddress<true>();
		::subnetAssert(allowList, rnd, false);
	}
	for (int i = 0; i < 100; ++i) {
		SubNetTest subnetTest(SubNetTest::randomSubNet());
		allowList = IPAllowList();
		allowList.addTrustedSubnet(subnetTest.subnet);
		for (int j = 0; j < 10; ++j) {
			bool inSubnet = deterministicRandom()->random01() < 0.7;
			auto addr = subnetTest.randomAddress(inSubnet);
			::subnetAssert(allowList, addr, inSubnet);
		}
	}
	return Void();
}
