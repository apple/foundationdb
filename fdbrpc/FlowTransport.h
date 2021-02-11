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
#include "flow/genericactors.actor.h"
#include "flow/network.h"
#include "flow/FileIdentifier.h"
#include "flow/Net2Packet.h"
#include "fdbrpc/ContinuousSample.h"

#pragma pack(push, 4)
class Endpoint {
public:
	// Endpoint represents a particular service (e.g. a serialized Promise<T> or PromiseStream<T>)
	// An endpoint is either "local" (used for receiving data) or "remote" (used for sending data)
	constexpr static FileIdentifier file_identifier = 10618805;
	typedef UID Token;
	NetworkAddressList addresses;
	Token token;

	Endpoint() {}
	Endpoint(const NetworkAddressList& addresses, Token token) : addresses(addresses), token(token) {
		choosePrimaryAddress();
	}

	void choosePrimaryAddress() {
		if (addresses.secondaryAddress.present() && !g_network->getLocalAddresses().secondaryAddress.present() &&
		    (addresses.address.isTLS() != g_network->getLocalAddresses().address.isTLS())) {
			std::swap(addresses.address, addresses.secondaryAddress.get());
		}
	}

	bool isValid() const { return token.isValid(); }
	bool isLocal() const;

	// Return the primary network address, which is the first network address among
	// all addresses this endpoint listens to.
	const NetworkAddress& getPrimaryAddress() const { return addresses.address; }

	bool operator==(Endpoint const& r) const {
		return getPrimaryAddress() == r.getPrimaryAddress() && token == r.token;
	}
	bool operator!=(Endpoint const& r) const { return !(*this == r); }

	bool operator<(Endpoint const& r) const {
		const NetworkAddress& left = getPrimaryAddress();
		const NetworkAddress& right = r.getPrimaryAddress();
		if (left != right)
			return left < right;
		else
			return token < r.token;
	}

	template <class Ar>
	void serialize(Ar& ar) {
		if constexpr (is_fb_function<Ar>) {
			serializer(ar, addresses, token);
			if constexpr (Ar::isDeserializing) {
				choosePrimaryAddress();
			}
		} else {
			if (ar.isDeserializing && !ar.protocolVersion().hasEndpointAddrList()) {
				addresses.secondaryAddress = Optional<NetworkAddress>();
				serializer(ar, addresses.address, token);
			} else {
				serializer(ar, addresses, token);
				if (ar.isDeserializing) {
					choosePrimaryAddress();
				}
			}
		}
	}
};
#pragma pack(pop)

class ArenaObjectReader;
class NetworkMessageReceiver {
public:
	virtual void receive(ArenaReader&) = 0;
	virtual void receive(ArenaObjectReader&) = 0;
	virtual bool isStream() const { return false; }
};

struct TransportData;

struct Peer : public ReferenceCounted<Peer> {
	TransportData* transport;
	NetworkAddress destination;
	UnsentPacketQueue unsent;
	ReliablePacketList reliable;
	AsyncTrigger dataToSend; // Triggered when unsent.empty() becomes false
	Future<Void> connect;
	AsyncTrigger resetPing;
	AsyncTrigger resetConnection;
	bool compatible;
	bool outgoingConnectionIdle; // We don't actually have a connection open and aren't trying to open one because we
	                             // don't have anything to send
	double lastConnectTime;
	double reconnectionDelay;
	int peerReferences;
	bool incompatibleProtocolVersionNewer;
	int64_t bytesReceived;
	int64_t bytesSent;
	double lastDataPacketSentTime;
	int outstandingReplies;
	ContinuousSample<double> pingLatencies;
	double lastLoggedTime;
	int64_t lastLoggedBytesReceived;
	int64_t lastLoggedBytesSent;

	// Cleared every time stats are logged for this peer.
	int connectOutgoingCount;
	int connectIncomingCount;
	int connectFailedCount;
	ContinuousSample<double> connectLatencies;

	explicit Peer(TransportData* transport, NetworkAddress const& destination)
	  : transport(transport), destination(destination), outgoingConnectionIdle(true), lastConnectTime(0.0),
	    reconnectionDelay(FLOW_KNOBS->INITIAL_RECONNECTION_TIME), compatible(true), outstandingReplies(0),
	    incompatibleProtocolVersionNewer(false), peerReferences(-1), bytesReceived(0), lastDataPacketSentTime(now()),
	    pingLatencies(destination.isPublic() ? FLOW_KNOBS->PING_SAMPLE_AMOUNT : 1), lastLoggedBytesReceived(0),
	    bytesSent(0), lastLoggedBytesSent(0), lastLoggedTime(0.0), connectOutgoingCount(0), connectIncomingCount(0),
	    connectFailedCount(0),
	    connectLatencies(destination.isPublic() ? FLOW_KNOBS->NETWORK_CONNECT_SAMPLE_AMOUNT : 1) {}

	void send(PacketBuffer* pb, ReliablePacket* rp, bool firstUnsent);

	void prependConnectPacket();

	void discardUnreliablePackets();

	void onIncomingConnection(Reference<Peer> self, Reference<IConnection> conn, Future<Void> reader);
};

class FlowTransport {
public:
	FlowTransport(uint64_t transportId);
	~FlowTransport();

	static void createInstance(bool isClient, uint64_t transportId);
	// Creates a new FlowTransport and makes FlowTransport::transport() return it.  This uses g_network->global()
	// variables, so it will be private to a simulation.

	static bool isClient() { return g_network->global(INetwork::enClientFailureMonitor) != nullptr; }

	void initMetrics();
	// Metrics must be initialized after FlowTransport::createInstance has been called

	Future<Void> bind(NetworkAddress publicAddress, NetworkAddress listenAddress);
	// Starts a server listening on the given listenAddress, and sets publicAddress to be the public
	// address of this server.  Returns only errors.

	NetworkAddress getLocalAddress() const;
	// Returns first local NetworkAddress.

	NetworkAddressList getLocalAddresses() const;
	// Returns all local NetworkAddress.

	std::map<NetworkAddress, std::pair<uint64_t, double>>* getIncompatiblePeers();
	// Returns the same of all peers that have attempted to connect, but have incompatible protocol versions

	void addPeerReference(const Endpoint&, bool isStream);
	// Signal that a peer connection is being used, even if no messages are currently being sent to the peer

	void removePeerReference(const Endpoint&, bool isStream);
	// Signal that a peer connection is no longer being used

	void addEndpoint(Endpoint& endpoint, NetworkMessageReceiver*, TaskPriority taskID);
	// Sets endpoint to be a new local endpoint which delivers messages to the given receiver

	void removeEndpoint(const Endpoint&, NetworkMessageReceiver*);
	// The given local endpoint no longer delivers messages to the given receiver or uses resources

	void addWellKnownEndpoint(Endpoint& endpoint, NetworkMessageReceiver*, TaskPriority taskID);
	// Sets endpoint to a new local endpoint (without changing its token) which delivers messages to the given receiver
	// Implementations may have limitations on when this function is called and what endpoint.token may be!

	ReliablePacket* sendReliable(ISerializeSource const& what, const Endpoint& destination);
	// sendReliable will keep trying to deliver the data to the destination until cancelReliable is
	//   called.  It will retry sending if the connection is closed or the failure manager reports
	//   the destination become available (edge triggered).

	void cancelReliable(ReliablePacket*);
	// Makes Packet "unreliable" (either the data or a connection close event will be delivered
	//   eventually).  It can still be used safely to send a reply to a "reliable" request.

	Reference<AsyncVar<bool>> getDegraded();
	// This async var will be set to true when the process cannot connect to a public network address that the failure
	// monitor thinks is healthy.

	void resetConnection(NetworkAddress address);
	// Forces the connection with this address to be reset

	Reference<Peer> sendUnreliable(ISerializeSource const& what, const Endpoint& destination,
	                               bool openConnection); // { cancelReliable(sendReliable(what,destination)); }

	int getEndpointCount();
	// for tracing only

	bool incompatibleOutgoingConnectionsPresent();

	static FlowTransport& transport() {
		return *static_cast<FlowTransport*>((void*)g_network->global(INetwork::enFlowTransport));
	}
	static NetworkAddress getGlobalLocalAddress() { return transport().getLocalAddress(); }
	static NetworkAddressList getGlobalLocalAddresses() { return transport().getLocalAddresses(); }

	Endpoint loadedEndpoint(const UID& token);

private:
	class TransportData* self;
};

inline bool Endpoint::isLocal() const {
	const auto& localAddrs = FlowTransport::transport().getLocalAddresses();
	return addresses.address == localAddrs.address ||
	       (localAddrs.secondaryAddress.present() && addresses.address == localAddrs.secondaryAddress.get());
}

#endif
