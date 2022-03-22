/*
 * FlowTransport.h
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

#ifndef FLOW_TRANSPORT_H
#define FLOW_TRANSPORT_H
#pragma once

#include <algorithm>
#include "fdbrpc/HealthMonitor.h"
#include "flow/genericactors.actor.h"
#include "flow/network.h"
#include "flow/FileIdentifier.h"
#include "flow/ProtocolVersion.h"
#include "flow/Net2Packet.h"
#include "fdbrpc/ContinuousSample.h"

enum { WLTOKEN_ENDPOINT_NOT_FOUND = 0, WLTOKEN_PING_PACKET, WLTOKEN_FIRST_AVAILABLE };

#pragma pack(push, 4)
class Endpoint {
public:
	// Endpoint represents a particular service (e.g. a serialized Promise<T> or PromiseStream<T>)
	// An endpoint is either "local" (used for receiving data) or "remote" (used for sending data)
	constexpr static FileIdentifier file_identifier = 10618805;
	using Token = UID;
	NetworkAddressList addresses;
	Token token{};

	Endpoint() {}
	Endpoint(const NetworkAddressList& addresses, Token token) : addresses(addresses), token(token) {
		choosePrimaryAddress();
	}

	static Token wellKnownToken(int wlTokenID) { return UID(-1, wlTokenID); }

	static Endpoint wellKnown(const NetworkAddressList& addresses, int wlTokenID) {
		return Endpoint(addresses, wellKnownToken(wlTokenID));
	}

	void choosePrimaryAddress() {
		if (addresses.secondaryAddress.present() &&
		    ((!g_network->getLocalAddresses().secondaryAddress.present() &&
		      (addresses.address.isTLS() != g_network->getLocalAddresses().address.isTLS())) ||
		     (g_network->getLocalAddresses().secondaryAddress.present() && !addresses.address.isTLS()))) {
			std::swap(addresses.address, addresses.secondaryAddress.get());
		}
	}

	bool isValid() const { return token.isValid(); }
	bool isLocal() const;

	// Return the primary network address, which is the first network address among
	// all addresses this endpoint listens to.
	const NetworkAddress& getPrimaryAddress() const { return addresses.address; }

	NetworkAddress getStableAddress() const { return addresses.getTLSAddress(); }

	Endpoint getAdjustedEndpoint(uint32_t index) const {
		uint32_t newIndex = token.second();
		newIndex += index;
		return Endpoint(
		    addresses,
		    UID(token.first() + (uint64_t(index) << 32), (token.second() & 0xffffffff00000000LL) | newIndex));
	}

	bool operator==(Endpoint const& r) const {
		return token == r.token && getPrimaryAddress() == r.getPrimaryAddress();
	}
	bool operator!=(Endpoint const& r) const { return !(*this == r); }
	bool operator<(Endpoint const& r) const {
		return addresses.address < r.addresses.address || (addresses.address == r.addresses.address && token < r.token);
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

namespace std {
template <>
struct hash<Endpoint> {
	size_t operator()(const Endpoint& ep) const { return ep.token.hash() + ep.addresses.address.hash(); }
};
} // namespace std

enum class RequirePeer { Exactly, AtLeast };

struct PeerCompatibilityPolicy {
	RequirePeer requirement;
	ProtocolVersion version;
};

class ArenaObjectReader;
class NetworkMessageReceiver {
public:
	virtual void receive(ArenaObjectReader&) = 0;
	virtual bool isStream() const { return false; }
	virtual PeerCompatibilityPolicy peerCompatibilityPolicy() const {
		return { RequirePeer::Exactly, g_network->protocolVersion() };
	}
};

class TransportData;

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
	int timeoutCount;

	Reference<AsyncVar<Optional<ProtocolVersion>>> protocolVersion;

	// Cleared every time stats are logged for this peer.
	int connectOutgoingCount;
	int connectIncomingCount;
	int connectFailedCount;
	ContinuousSample<double> connectLatencies;
	Promise<Void> disconnect;

	explicit Peer(TransportData* transport, NetworkAddress const& destination);

	void send(PacketBuffer* pb, ReliablePacket* rp, bool firstUnsent);

	void prependConnectPacket();

	void discardUnreliablePackets();

	void onIncomingConnection(Reference<Peer> self, Reference<IConnection> conn, Future<Void> reader);
};

class FlowTransport {
public:
	FlowTransport(uint64_t transportId, int maxWellKnownEndpoints);
	~FlowTransport();

	// Creates a new FlowTransport and makes FlowTransport::transport() return it.  This uses g_network->global()
	// variables, so it will be private to a simulation.
	static void createInstance(bool isClient, uint64_t transportId, int maxWellKnownEndpoints);

	static bool isClient() { return g_network->global(INetwork::enClientFailureMonitor) != nullptr; }

	// Metrics must be initialized after FlowTransport::createInstance has been called
	void initMetrics();

	// Starts a server listening on the given listenAddress, and sets publicAddress to be the public
	// address of this server.  Returns only errors.
	Future<Void> bind(NetworkAddress publicAddress, NetworkAddress listenAddress);

	// Returns first local NetworkAddress.
	NetworkAddress getLocalAddress() const;

	// Returns all local NetworkAddress.
	NetworkAddressList getLocalAddresses() const;

	// Returns all peers that the FlowTransport is monitoring.
	const std::unordered_map<NetworkAddress, Reference<Peer>>& getAllPeers() const;

	// Returns the same of all peers that have attempted to connect, but have incompatible protocol versions
	std::map<NetworkAddress, std::pair<uint64_t, double>>* getIncompatiblePeers();

	// Returns when getIncompatiblePeers has at least one peer which is incompatible.
	Future<Void> onIncompatibleChanged();

	// Signal that a peer connection is being used, even if no messages are currently being sent to the peer
	void addPeerReference(const Endpoint&, bool isStream);

	// Signal that a peer connection is no longer being used
	void removePeerReference(const Endpoint&, bool isStream);

	// Sets endpoint to be a new local endpoint which delivers messages to the given receiver
	void addEndpoint(Endpoint& endpoint, NetworkMessageReceiver*, TaskPriority taskID);

	void addEndpoints(std::vector<std::pair<struct FlowReceiver*, TaskPriority>> const& streams);

	// The given local endpoint no longer delivers messages to the given receiver or uses resources
	void removeEndpoint(const Endpoint&, NetworkMessageReceiver*);

	// Sets endpoint to a new local endpoint (without changing its token) which delivers messages to the given receiver
	// Implementations may have limitations on when this function is called and what endpoint.token may be!
	void addWellKnownEndpoint(Endpoint& endpoint, NetworkMessageReceiver*, TaskPriority taskID);

	// sendReliable will keep trying to deliver the data to the destination until cancelReliable is called. It will
	// retry sending if the connection is closed or the failure manager reports the destination become available (edge
	// triggered).
	ReliablePacket* sendReliable(ISerializeSource const& what, const Endpoint& destination);

	// Makes Packet "unreliable" (either the data or a connection close event will be delivered eventually). It can
	// still be used safely to send a reply to a "reliable" request.
	void cancelReliable(ReliablePacket*);

	// This async var will be set to true when the process cannot connect to a public network address that the failure
	// monitor thinks is healthy.
	Reference<AsyncVar<bool>> getDegraded();

	// Forces the connection with this address to be reset
	void resetConnection(NetworkAddress address);

	Reference<Peer> sendUnreliable(ISerializeSource const& what,
	                               const Endpoint& destination,
	                               bool openConnection); // { cancelReliable(sendReliable(what,destination)); }

	bool incompatibleOutgoingConnectionsPresent();

	// Returns the protocol version of the peer at the specified address. The result is returned as an AsyncVar that
	// can be used to monitor for changes of a peer's protocol. The protocol version will be unset in the event that
	// there is no connection established to the peer.
	//
	// Note that this function does not establish a connection to the peer. In order to obtain a peer's protocol
	// version, some other mechanism should be used to connect to that peer.
	Reference<AsyncVar<Optional<ProtocolVersion>> const> getPeerProtocolAsyncVar(NetworkAddress addr);

	static FlowTransport& transport() {
		return *static_cast<FlowTransport*>((void*)g_network->global(INetwork::enFlowTransport));
	}
	static NetworkAddress getGlobalLocalAddress() { return transport().getLocalAddress(); }
	static NetworkAddressList getGlobalLocalAddresses() { return transport().getLocalAddresses(); }

	Endpoint loadedEndpoint(const UID& token);
	Future<Void> loadedDisconnect();

	HealthMonitor* healthMonitor();

private:
	class TransportData* self;
};

inline bool Endpoint::isLocal() const {
	const auto& localAddrs = FlowTransport::transport().getLocalAddresses();
	return addresses.address == localAddrs.address ||
	       (localAddrs.secondaryAddress.present() && addresses.address == localAddrs.secondaryAddress.get());
}

#endif
