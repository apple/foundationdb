/*
 * network.cpp
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

#include "network.h"
#include "flow.h"

NetworkAddress NetworkAddress::parse( std::string const& s ) {
	bool isTLS = false;
	std::string f;
	if( s.size() > 4 && strcmp(s.c_str() + s.size() - 4, ":tls") == 0 ) {
		isTLS = true;
		f = s.substr(0, s.size() - 4);
	} else
		f = s;
	int a,b,c,d,port,count=-1;
	if (sscanf(f.c_str(), "%d.%d.%d.%d:%d%n", &a,&b,&c,&d, &port, &count)<5 || count != f.size())
		throw connection_string_invalid();
	return NetworkAddress( (a<<24)+(b<<16)+(c<<8)+d, port, true, isTLS );
}

std::vector<NetworkAddress> NetworkAddress::parseList( std::string const& addrs ) {
	// Split addrs on ',' and parse them individually
	std::vector<NetworkAddress> coord;
	for(int p = 0; p <= addrs.size(); ) {
		int pComma = addrs.find_first_of(',', p);
		if (pComma == addrs.npos) pComma = addrs.size();
		NetworkAddress parsedAddress = NetworkAddress::parse( addrs.substr(p, pComma-p) );
		coord.push_back( parsedAddress );
		p = pComma + 1;
	}
	return coord;
}

std::string NetworkAddress::toString() const {
	return format( "%d.%d.%d.%d:%d%s", (ip>>24)&0xff, (ip>>16)&0xff, (ip>>8)&0xff, ip&0xff, port, isTLS() ? ":tls" : "" );
}

std::string toIPString(uint32_t ip) {
	return format( "%d.%d.%d.%d", (ip>>24)&0xff, (ip>>16)&0xff, (ip>>8)&0xff, ip&0xff );
}

std::string toIPVectorString(std::vector<uint32_t> ips) {
	std::string output;
	const char* space = "";
	for (auto ip : ips) {
		output += format("%s%d.%d.%d.%d", space, (ip >> 24) & 0xff, (ip >> 16) & 0xff, (ip >> 8) & 0xff, ip & 0xff);
		space = " ";
	}
	return output;
}

Future<Reference<IConnection>> INetworkConnections::connect( std::string host, std::string service, bool useTLS ) {
	// Use map to create an actor that returns an endpoint or throws
	Future<NetworkAddress> pickEndpoint = map(resolveTCPEndpoint(host, service), [=](std::vector<NetworkAddress> const &addresses) -> NetworkAddress {
		NetworkAddress addr = addresses[g_random->randomInt(0, addresses.size())];
		if(useTLS)
			addr.flags = NetworkAddress::FLAG_TLS;
		return addr;
	});

	// Wait for the endpoint to return, then wait for connect(endpoint) and return it.
	// Template types are being provided explicitly because they can't be automatically deduced for some reason.
	return mapAsync<NetworkAddress, std::function<Future<Reference<IConnection>>(NetworkAddress const &)>, Reference<IConnection> >
		(pickEndpoint, [=](NetworkAddress const &addr) -> Future<Reference<IConnection>> {
		return connect(addr);
	});
}
