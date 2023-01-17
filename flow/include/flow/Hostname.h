/*
 * Hostname.h
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

#ifndef FLOW_HOSTNAME_H
#define FLOW_HOSTNAME_H
#pragma once

#include "flow/network.h"
#include "flow/genericactors.actor.h"

struct Hostname {
	std::string host;
	std::string service; // decimal port number
	bool isTLS;

	Hostname(const std::string& host, const std::string& service, bool isTLS)
	  : host(host), service(service), isTLS(isTLS) {}
	Hostname() : host(""), service(""), isTLS(false) {}

	bool operator==(const Hostname& r) const { return host == r.host && service == r.service && isTLS == r.isTLS; }
	bool operator!=(const Hostname& r) const { return !(*this == r); }
	bool operator<(const Hostname& r) const {
		if (isTLS != r.isTLS)
			return isTLS < r.isTLS;
		else if (host != r.host)
			return host < r.host;
		return service < r.service;
	}
	bool operator>(const Hostname& r) const { return r < *this; }
	bool operator<=(const Hostname& r) const { return !(*this > r); }
	bool operator>=(const Hostname& r) const { return !(*this < r); }

	// Allow hostnames in forms like following:
	//    hostname:1234
	//    host.name:1234
	//    host-name:1234
	//    host-name_part1.host-name_part2:1234:tls
	static bool isHostname(const std::string& s) {
		std::regex validation(R"(^([\w\-]+\.?)+:([\d]+){1,}(:tls)?$)");
		std::regex ipv4Validation(R"(^([\d]{1,3}\.?){4,}:([\d]+){1,}(:tls)?$)");
		return !std::regex_match(s, ipv4Validation) && std::regex_match(s, validation);
	}

	static Hostname parse(const std::string& s);

	std::string toString() const { return host + ":" + service + (isTLS ? ":tls" : ""); }

	// The resolve functions below use DNS cache.
	Future<Optional<NetworkAddress>> resolve();
	Future<NetworkAddress> resolveWithRetry();
	Optional<NetworkAddress> resolveBlocking(); // This one should only be used when resolving asynchronously is
	                                            // impossible. For all other cases, resolve() should be preferred.

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, host, service, isTLS);
	}
};

#endif
