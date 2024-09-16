/*
 * AutoPublicAddress.cpp
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

#include "flow/Platform.h"
#include <algorithm>

#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include "boost/asio.hpp"

#include "fdbclient/CoordinationInterface.h"

IPAddress ClusterConnectionString::determineLocalSourceIP() const {
	int size = coords.size() + hostnames.size();
	int index = 0;
	loop {
		try {
			using namespace boost::asio;

			io_service ioService;
			ip::udp::socket socket(ioService);

			NetworkAddress coordAddr;
			// Try coords first, because they don't need to be resolved.
			if (index < coords.size()) {
				coordAddr = coords[index];
			} else {
				const Hostname& h = hostnames[index - coords.size()];
				Optional<NetworkAddress> resolvedAddr = h.resolveBlocking();
				if (!resolvedAddr.present()) {
					throw lookup_failed();
				}
				coordAddr = resolvedAddr.get();
			}
			const auto boostIp = coordAddr.ip.isV6() ? ip::address(ip::address_v6(coordAddr.ip.toV6()))
			                                         : ip::address(ip::address_v4(coordAddr.ip.toV4()));

			ip::udp::endpoint endpoint(boostIp, coordAddr.port);
			socket.connect(endpoint);
			IPAddress ip = coordAddr.ip.isV6() ? IPAddress(socket.local_endpoint().address().to_v6().to_bytes())
			                                   : IPAddress(socket.local_endpoint().address().to_v4().to_ulong());
			socket.close();

			return ip;
		} catch (...) {
			++index;
			if (index == size) {
				fprintf(stderr, "Error determining public address.\n");
				throw bind_failed();
			}
		}
	}
}
