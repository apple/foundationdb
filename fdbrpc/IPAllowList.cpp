/*
 * IPAllowList.h
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

#include "fdbrpc/IPAllowList.h"
#include "flow/UnitTest.h"

#include <fmt/printf.h>
#include <fmt/format.h>

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
int hostCountImpl(std::array<unsigned char, Sz> const& addr) {
	int count = 0;
	for (int i = 0; i < addr.size() && addr[i] != 0xff; ++i) {
		std::bitset<8> b(addr[i]);
		count += 8 - b.count();
	}
	return count;
}

} // namespace

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


int AuthAllowedSubnet::hostCount() const {
	if (addressMask.isV4()) {
		boost::asio::ip::address_v4 addr(addressMask.toV4());
		return hostCountImpl(addr.to_bytes());
	} else {
		return hostCountImpl(addressMask.toV6());
	}
}

AuthAllowedSubnet AuthAllowedSubnet::fromString(std::string_view addressString) {
	auto pos = addressString.find('/');
	if (pos == std::string_view::npos) {
		fmt::print("ERROR: {} is not a valid (use Network-Prefix/hostcount syntax)\n");
		throw invalid_option();
	}
	auto address = addressString.substr(0, pos);
	auto hostCount = std::stoi(std::string(addressString.substr(pos + 1)));
	auto addr = boost::asio::ip::make_address(address);
	if (addr.is_v4()) {
		auto bM = createBitMask(addr.to_v4().to_bytes(), hostCount);
		// we typically would expect a base address has been passed, but to be safe we still
		// will make the last bits 0
		auto mask = boost::asio::ip::address_v4(bM).to_uint();
		auto baseAddress = addr.to_v4().to_uint() & mask;
		fmt::print("For address {}:", addressString);
		printIP("Base Address", IPAddress(baseAddress));
		printIP("Mask:", IPAddress(mask));
		return AuthAllowedSubnet(IPAddress(baseAddress), IPAddress(mask));
	} else {
		auto mask = createBitMask(addr.to_v6().to_bytes(), hostCount);
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
	SubNetTest(AuthAllowedSubnet&& subnet)
		: subnet(subnet)
	{
	}
	template<bool V4>
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
		auto hostCount = deterministicRandom()->randomInt(1, width);
		std::string address;
		if constexpr (V4) {
			address_v4 a(binAddr);
			address = a.to_string();
		} else {
			address_v6 a(binAddr);
			address = a.to_string();
		}
		return SubNetTest(AuthAllowedSubnet::fromString(fmt::format("{}/{}", address, hostCount)));
	}
	static SubNetTest randomSubNet() {
		if (deterministicRandom()->coinflip()) {
			return randomSubNetImpl<true>();
		} else {
			return randomSubNetImpl<false>();
		}
	}

	template<bool V4>
	IPAddress intArrayToAddress(uint32_t* arr) {
		if constexpr (V4) {
			return IPAddress(arr[0]);
		} else {
			std::array<unsigned char, 16> res;
			memcpy(res.data(), arr, 4);
			return IPAddress(res);
		}
	}

	template<class I>
	I transformIntToSubnet(I val, I subnetMask, I baseAddress) {
		return (val & subnetMask) ^ baseAddress;
	}

	template <bool V4>
	IPAddress randomAddress(bool inSubnet) {
		ASSERT(V4 == subnet.baseAddress.isV4() || !inSubnet);
		constexpr int width = V4 ? 4 : 16;
		for (;;) {
			uint32_t rnd[width / 4];
			for (int i = 0; i < width / 4; ++i) {
				rnd[i] = deterministicRandom()->randomUInt32();
			}
			auto res = intArrayToAddress<V4>(rnd);
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
			return randomAddress<true>(inSubnet);
		}
	}
};

} // namespace

TEST_CASE("/fdbrpc/allow_list") {
	IPAllowList allowList;
	allowList.addTrustedSubnet("1.0.0.0/8");
	allowList.addTrustedSubnet("2.0.0.0/4");
	::subnetAssert(allowList, parseAddr("1.0.1.1"), true);
	::subnetAssert(allowList, parseAddr("1.1.2.2"), true);
	::subnetAssert(allowList, parseAddr("2.2.1.1"), true);
	::subnetAssert(allowList, parseAddr("128.0.1.1"), false);
	allowList = IPAllowList();
	allowList.addTrustedSubnet("0.0.0.0/2");
	::subnetAssert(allowList, parseAddr("1.0.1.1"), true);
	::subnetAssert(allowList, parseAddr("1.1.2.2"), true);
	::subnetAssert(allowList, parseAddr("2.2.1.1"), true);
	::subnetAssert(allowList, parseAddr("5.2.1.1"), true);
	::subnetAssert(allowList, parseAddr("128.0.1.1"), false);
	::subnetAssert(allowList, parseAddr("192.168.3.1"), false);
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
