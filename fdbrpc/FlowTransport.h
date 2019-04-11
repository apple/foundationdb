/*
 * FlowTransport.h
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

#ifndef FLOW_TRANSPORT_H
#define FLOW_TRANSPORT_H
#pragma once

#include <algorithm>
#include "flow/network.h"

#pragma pack(push, 4)
class Endpoint {
public:
	// Endpoint represents a particular service (e.g. a serialized Promise<T> or PromiseStream<T>)
	// An endpoint is either "local" (used for receiving data) or "remote" (used for sending data)
	typedef UID Token;
	NetworkAddressList addresses;
	Token token;

	Endpoint() {}
	Endpoint(const NetworkAddressList& addresses, Token token) : addresses(addresses), token(token) {
		choosePrimaryAddress();
	}

	void choosePrimaryAddress() {
		if(addresses.secondaryAddress.present() && !g_network->getLocalAddresses().secondaryAddress.present() && (addresses.address.isTLS() != g_network->getLocalAddresses().address.isTLS())) {
			std::swap(addresses.address, addresses.secondaryAddress.get());
		}	
	}

	bool isValid() const { return token.isValid(); }
	bool isLocal() const;

	// Return the primary network address, which is the first network address among
	// all addresses this endpoint listens to.
	const NetworkAddress& getPrimaryAddress() const {
		return addresses.address;
	}

	bool operator == (Endpoint const& r) const {
		return getPrimaryAddress() == r.getPrimaryAddress() && token == r.token;
	}
	bool operator != (Endpoint const& r) const {
		return !(*this == r);
	}

	bool operator < (Endpoint const& r) const {
		const NetworkAddress& left = getPrimaryAddress();
		const NetworkAddress& right = r.getPrimaryAddress();
		if (left != right)
			return left < right;
		else
			return token < r.token;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.isDeserializing && ar.protocolVersion() < 0x0FDB00B061020001LL) {
			addresses.secondaryAddress = Optional<NetworkAddress>();
			serializer(ar, addresses.address, token);
		} else {
			serializer(ar, addresses, token);
			if (ar.isDeserializing) {
				choosePrimaryAddress();
			}
		}
	}
};
#pragma pack(pop)

class NetworkMessageReceiver {
public:
	virtual void receive( ArenaReader& ) = 0;
	virtual bool isStream() const { return false; }
};

typedef struct NetworkPacket* PacketID;

class FlowTransport {
public:
	FlowTransport(uint64_t transportId);
	~FlowTransport();

	static void createInstance(uint64_t transportId = 0);
	// Creates a new FlowTransport and makes FlowTransport::transport() return it.  This uses g_network->global() variables,
	// so it will be private to a simulation.

	void initMetrics();
	// Metrics must be initialized after FlowTransport::createInstance has been called

	Future<Void> bind( NetworkAddress publicAddress, NetworkAddress listenAddress );
	// Starts a server listening on the given listenAddress, and sets publicAddress to be the public
	// address of this server.  Returns only errors.

	NetworkAddress getLocalAddress() const;
	// Returns first local NetworkAddress.

	NetworkAddressList getLocalAddresses() const;
	// Returns all local NetworkAddress.

	std::map<NetworkAddress, std::pair<uint64_t, double>>* getIncompatiblePeers();
	// Returns the same of all peers that have attempted to connect, but have incompatible protocol versions

	void addPeerReference( const Endpoint&, NetworkMessageReceiver* );
	// Signal that a peer connection is being used, even if no messages are currently being sent to the peer

	void removePeerReference( const Endpoint&, NetworkMessageReceiver* );
	// Signal that a peer connection is no longer being used

	void addEndpoint( Endpoint& endpoint, NetworkMessageReceiver*, uint32_t taskID );
	// Sets endpoint to be a new local endpoint which delivers messages to the given receiver

	void removeEndpoint( const Endpoint&, NetworkMessageReceiver* );
	// The given local endpoint no longer delivers messages to the given receiver or uses resources

	void addWellKnownEndpoint( Endpoint& endpoint, NetworkMessageReceiver*, uint32_t taskID );
	// Sets endpoint to a new local endpoint (without changing its token) which delivers messages to the given receiver
	// Implementations may have limitations on when this function is called and what endpoint.token may be!

	PacketID sendReliable( ISerializeSource const& what, const Endpoint& destination );
	// sendReliable will keep trying to deliver the data to the destination until cancelReliable is
	//   called.  It will retry sending if the connection is closed or the failure manager reports
	//   the destination become available (edge triggered).

	void cancelReliable( PacketID );
	// Makes PacketID "unreliable" (either the data or a connection close event will be delivered
	//   eventually).  It can still be used safely to send a reply to a "reliable" request.

	Reference<AsyncVar<bool>> getDegraded();
	// This async var will be set to true when the process cannot connect to a public network address that the failure monitor thinks is healthy.

	void sendUnreliable( ISerializeSource const& what, const Endpoint& destination, bool openConnection = true );// { cancelReliable(sendReliable(what,destination)); }

	int getEndpointCount();
	// for tracing only

	bool incompatibleOutgoingConnectionsPresent();

	static FlowTransport& transport() { return *static_cast<FlowTransport*>((void*) g_network->global(INetwork::enFlowTransport)); }
	static NetworkAddress getGlobalLocalAddress() { return transport().getLocalAddress(); }
	static NetworkAddressList getGlobalLocalAddresses() { return transport().getLocalAddresses(); }

	Endpoint loadedEndpoint(const UID& token);

private:
	class TransportData* self;
};

inline bool Endpoint::isLocal() const {
	const auto& localAddrs = FlowTransport::transport().getLocalAddresses();
	return addresses.address == localAddrs.address || (localAddrs.secondaryAddress.present() && addresses.address == localAddrs.secondaryAddress.get());
}

#endif
