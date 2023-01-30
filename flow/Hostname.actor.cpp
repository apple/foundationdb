/*
 * Hostname.actor.cpp
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

#include "flow/Hostname.h"

#include <regex>

#include "flow/UnitTest.h"
#include "flow/IConnection.h"
#include "flow/actorcompiler.h" // has to be last include

namespace {

const static std::regex validation("^([\\w\\-]+\\.?)+:([\\d]+){1,}(:tls)?$");
const static std::regex ipv4Validation("^([\\d]{1,3}\\.?){4,}:([\\d]+){1,}(:tls)?$");

} // anonymous namespace

bool Hostname::isHostname(const std::string& str) {
	return !std::regex_match(str, ipv4Validation) && std::regex_match(str, validation);
}

Hostname Hostname::parse(const std::string& s) {
	if (s.empty() || !Hostname::isHostname(s)) {
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

ACTOR Future<Optional<NetworkAddress>> resolveImpl(Hostname* self) {
	try {
		std::vector<NetworkAddress> addresses =
		    wait(INetworkConnections::net()->resolveTCPEndpointWithDNSCache(self->host, self->service));
		NetworkAddress address = INetworkConnections::pickOneAddress(addresses);
		address.flags = 0; // Reset the parsed address to public
		address.fromHostname = NetworkAddressFromHostname::True;
		if (self->isTLS) {
			address.flags |= NetworkAddress::FLAG_TLS;
		}
		return address;
	} catch (...) {
		return Optional<NetworkAddress>();
	}
}

ACTOR Future<NetworkAddress> resolveWithRetryImpl(Hostname* self) {
	state double resolveInterval = FLOW_KNOBS->HOSTNAME_RESOLVE_INIT_INTERVAL;
	loop {
		try {
			Optional<NetworkAddress> address = wait(resolveImpl(self));
			if (address.present()) {
				return address.get();
			}
			wait(delay(resolveInterval));
			resolveInterval = std::min(2 * resolveInterval, FLOW_KNOBS->HOSTNAME_RESOLVE_MAX_INTERVAL);
		} catch (Error& e) {
			ASSERT(e.code() == error_code_actor_cancelled);
			throw;
		}
	}
}

Future<Optional<NetworkAddress>> Hostname::resolve() {
	return resolveImpl(this);
}

Future<NetworkAddress> Hostname::resolveWithRetry() {
	return resolveWithRetryImpl(this);
}

Optional<NetworkAddress> Hostname::resolveBlocking() {
	try {
		std::vector<NetworkAddress> addresses =
		    INetworkConnections::net()->resolveTCPEndpointBlockingWithDNSCache(host, service);
		NetworkAddress address = INetworkConnections::pickOneAddress(addresses);
		address.flags = 0; // Reset the parsed address to public
		address.fromHostname = NetworkAddressFromHostname::True;
		if (isTLS) {
			address.flags |= NetworkAddress::FLAG_TLS;
		}
		return address;
	} catch (...) {
		return Optional<NetworkAddress>();
	}
}

TEST_CASE("/flow/Hostname/hostname") {
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

	state Hostname hn2 = Hostname::parse(hn2s);
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

	ASSERT(!Hostname::isHostname(hn5s));
	ASSERT(!Hostname::isHostname(hn6s));
	ASSERT(!Hostname::isHostname(hn7s));
	ASSERT(!Hostname::isHostname(hn8s));
	ASSERT(!Hostname::isHostname(hn9s));
	ASSERT(!Hostname::isHostname(hn10s));
	ASSERT(!Hostname::isHostname(hn11s));
	ASSERT(!Hostname::isHostname(hn12s));
	ASSERT(!Hostname::isHostname(hn13s));

	state Optional<NetworkAddress> optionalAddress = wait(hn2.resolve());
	ASSERT(!optionalAddress.present());

	optionalAddress = hn2.resolveBlocking();
	ASSERT(!optionalAddress.present());

	state NetworkAddress address;
	try {
		wait(timeoutError(store(address, hn2.resolveWithRetry()), 1));
	} catch (Error& e) {
		ASSERT(e.code() == error_code_timed_out);
	}
	ASSERT(address == NetworkAddress());

	state NetworkAddress addressSource = NetworkAddress::parse("127.0.0.0:1234");
	INetworkConnections::net()->addMockTCPEndpoint("host-name", "1234", { addressSource });

	// Test resolve.
	wait(store(optionalAddress, hn2.resolve()));
	ASSERT(optionalAddress.present() && optionalAddress.get() == addressSource);
	optionalAddress = Optional<NetworkAddress>();

	// Test resolveBlocking.
	optionalAddress = hn2.resolveBlocking();
	ASSERT(optionalAddress.present() && optionalAddress.get() == addressSource);
	optionalAddress = Optional<NetworkAddress>();

	// Test resolveWithRetry.
	wait(store(address, hn2.resolveWithRetry()));
	ASSERT(address == addressSource);

	return Void();
}
