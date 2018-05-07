/*
 * AutoPublicAddress.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include "boost/asio.hpp"

#include "CoordinationInterface.h"

uint32_t determinePublicIPAutomatically( ClusterConnectionString const& ccs ) {
	try {
		boost::asio::io_service ioService;
		boost::asio::ip::udp::socket socket(ioService);
		boost::asio::ip::udp::endpoint endpoint(boost::asio::ip::address_v4(ccs.coordinators()[0].ip), ccs.coordinators()[0].port);
		socket.connect(endpoint);
		auto ip = socket.local_endpoint().address().to_v4().to_ulong();
		socket.close();

		return ip;
	}
	catch(boost::system::system_error e) {
		fprintf(stderr, "Error determining public address: %s\n", e.what());
		throw bind_failed();
	}
}