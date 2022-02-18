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

} // namespace

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
	return Void();
}
