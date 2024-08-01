/*
 * IPAllowList.h
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

#pragma once
#ifndef FDBRPC_IP_ALLOW_LIST_H
#define FDBRPC_IP_ALLOW_LIST_H

#include "flow/network.h"
#include "flow/Arena.h"

struct AuthAllowedSubnet {
	IPAddress baseAddress;
	IPAddress addressMask;

	AuthAllowedSubnet(IPAddress const& baseAddress, IPAddress const& addressMask);

	static AuthAllowedSubnet fromString(std::string_view addressString);

	template <std::size_t sz>
	static std::array<unsigned char, sz> createBitMask(std::array<unsigned char, sz> const& addr, int netmaskWeight);

	bool operator()(IPAddress const& address) const {
		if (addressMask.isV4() != address.isV4()) {
			return false;
		}
		if (addressMask.isV4()) {
			return (addressMask.toV4() & address.toV4()) == baseAddress.toV4();
		} else {
			auto res = address.toV6();
			auto const& mask = addressMask.toV6();
			for (int i = 0; i < res.size(); ++i) {
				res[i] &= mask[i];
			}
			return res == baseAddress.toV6();
		}
	}

	IPAddress netmask() const;

	int netmaskWeight() const;

	// some useful helper functions if we need to debug ip masks etc
	static void printIP(std::string_view txt, IPAddress const& address);
};

class IPAllowList {
	std::vector<AuthAllowedSubnet> subnetList;

public:
	void addTrustedSubnet(std::string_view str) { subnetList.push_back(AuthAllowedSubnet::fromString(str)); }

	void addTrustedSubnet(AuthAllowedSubnet const& subnet) { subnetList.push_back(subnet); }

	std::vector<AuthAllowedSubnet> const& subnets() const { return subnetList; }

	bool operator()(IPAddress address) const {
		if (subnetList.empty()) {
			return true;
		}
		for (auto const& subnet : subnetList) {
			if (subnet(address)) {
				return true;
			}
		}
		return false;
	}
};

#endif // FDBRPC_IP_ALLOW_LIST_H
