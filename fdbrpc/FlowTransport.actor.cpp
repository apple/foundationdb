/*
 * FlowTransport.actor.cpp
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

#include "FlowTransport.h"
#include "genericactors.actor.h"
#include "fdbrpc.h"
#include "flow/Net2Packet.h"
#include "flow/ActorCollection.h"
#include "flow/TDMetric.actor.h"
#include "FailureMonitor.h"
#include "crc32c.h"
#include "simulator.h"

#if VALGRIND
#include <memcheck.h>
#endif

static NetworkAddress g_currentDeliveryPeerAddress;

const UID WLTOKEN_ENDPOINT_NOT_FOUND(-1, 0);
const UID WLTOKEN_PING_PACKET(-1, 1);
const UID TOKEN_IGNORE_PACKET(0, 2);
const uint64_t TOKEN_STREAM_FLAG = 1;


class EndpointMap : NonCopyable {
public:
	EndpointMap();
	void insert( NetworkMessageReceiver* r, Endpoint::Token& token, uint32_t priority );
	NetworkMessageReceiver* get( Endpoint::Token const& token );
	uint32_t getPriority( Endpoint::Token const& token );
	void remove( Endpoint::Token const& token, NetworkMessageReceiver* r );

private:
	void realloc();

	struct Entry {
		union {
			uint64_t uid[2];  // priority packed into lower 32 bits; actual lower 32 bits of token are the index in data[]
			uint32_t nextFree;
		};
		NetworkMessageReceiver* receiver;
		Endpoint::Token& token() { return *(Endpoint::Token*)uid; }
	};
	std::vector<Entry> data;
	uint32_t firstFree;
};

EndpointMap::EndpointMap() 
 : firstFree(-1) 
{
}

void EndpointMap::realloc() {
	int oldSize = data.size();
	data.resize( std::max(128, oldSize*2) );
	for(int i=oldSize; i<data.size(); i++) {
		data[i].receiver = 0;
		data[i].nextFree = i+1;
	}
	data[data.size()-1].nextFree = firstFree;
	firstFree = oldSize;
}

void EndpointMap::insert( NetworkMessageReceiver* r, Endpoint::Token& token, uint32_t priority ) {
	if (firstFree == uint32_t(-1)) realloc();
	int index = firstFree;
	firstFree = data[index].nextFree;
	token = Endpoint::Token( token.first(), (token.second()&0xffffffff00000000LL) | index );
	data[index].token() = Endpoint::Token( token.first(), (token.second()&0xffffffff00000000LL) | priority );
	data[index].receiver = r;
}

NetworkMessageReceiver* EndpointMap::get( Endpoint::Token const& token ) {
	uint32_t index = token.second();
	if ( index < data.size() && data[index].token().first() == token.first() && ((data[index].token().second()&0xffffffff00000000LL)|index)==token.second() )
		return data[index].receiver;
	return 0;
}

uint32_t EndpointMap::getPriority( Endpoint::Token const& token ) {
	uint32_t index = token.second();
	if ( index < data.size() && data[index].token().first() == token.first() && ((data[index].token().second()&0xffffffff00000000LL)|index)==token.second() )
		return data[index].token().second();
	return TaskUnknownEndpoint;
}

void EndpointMap::remove( Endpoint::Token const& token, NetworkMessageReceiver* r ) {
	uint32_t index = token.second();
	if ( index < data.size() && data[index].token().first() == token.first() && ((data[index].token().second()&0xffffffff00000000LL)|index)==token.second() && data[index].receiver == r ) {
		data[index].receiver = 0;
		data[index].nextFree = firstFree;
		firstFree = index;
	}
}

struct EndpointNotFoundReceiver : NetworkMessageReceiver {
	EndpointNotFoundReceiver(EndpointMap& endpoints) {
		//endpoints[WLTOKEN_ENDPOINT_NOT_FOUND] = this;
		Endpoint::Token e = WLTOKEN_ENDPOINT_NOT_FOUND;
		endpoints.insert(this, e, TaskDefaultEndpoint);
		ASSERT( e == WLTOKEN_ENDPOINT_NOT_FOUND );
	}
	virtual void receive( ArenaReader& reader ) {
		// Remote machine tells us it doesn't have endpoint e
		Endpoint e; reader >> e;
		IFailureMonitor::failureMonitor().endpointNotFound(e);
	}
};

struct PingReceiver : NetworkMessageReceiver {
	PingReceiver(EndpointMap& endpoints) {
		Endpoint::Token e = WLTOKEN_PING_PACKET;
		endpoints.insert(this, e, TaskReadSocket);
		ASSERT( e == WLTOKEN_PING_PACKET );
	}
	virtual void receive( ArenaReader& reader ) {
		ReplyPromise<Void> reply; reader >> reply;
		reply.send(Void());
	}
};

class TransportData {
public:
	TransportData(uint64_t transportId) 
	  : endpointNotFoundReceiver(endpoints),
		pingReceiver(endpoints),
		warnAlwaysForLargePacket(true),
		lastIncompatibleMessage(0),
		transportId(transportId),
		numIncompatibleConnections(0)
	{}

	void initMetrics() {
		bytesSent.init(LiteralStringRef("Net2.BytesSent"));
		countPacketsReceived.init(LiteralStringRef("Net2.CountPacketsReceived"));
		countPacketsGenerated.init(LiteralStringRef("Net2.CountPacketsGenerated"));
		countConnEstablished.init(LiteralStringRef("Net2.CountConnEstablished"));
		countConnClosedWithError.init(LiteralStringRef("Net2.CountConnClosedWithError"));
		countConnClosedWithoutError.init(LiteralStringRef("Net2.CountConnClosedWithoutError"));
	}

	struct Peer* getPeer( NetworkAddress const& address, bool doConnect = true );
	
	NetworkAddress localAddress;
	std::map<NetworkAddress, struct Peer*> peers;
	Future<Void> listen;
	bool warnAlwaysForLargePacket;

	// These declarations must be in exactly this order
	EndpointMap endpoints;
	EndpointNotFoundReceiver endpointNotFoundReceiver;
	PingReceiver pingReceiver;
	// End ordered declarations

	Int64MetricHandle bytesSent;
	Int64MetricHandle countPacketsReceived;
	Int64MetricHandle countPacketsGenerated;
	Int64MetricHandle countConnEstablished;
	Int64MetricHandle countConnClosedWithError;
	Int64MetricHandle countConnClosedWithoutError;

	std::map<NetworkAddress, std::pair<uint64_t, double>> incompatiblePeers;
	uint32_t numIncompatibleConnections;
	std::map<uint64_t, double> multiVersionConnections;
	double lastIncompatibleMessage;
	uint64_t transportId;

	Future<Void> multiVersionCleanup;
};

#define CONNECT_PACKET_V0 0x0FDB00A444020001LL
#define CONNECT_PACKET_V1 0x0FDB00A446030001LL
#define CONNECT_PACKET_V0_SIZE 14
#define CONNECT_PACKET_V1_SIZE 22
#define CONNECT_PACKET_V2_SIZE 26

#pragma pack( push, 1 )
struct ConnectPacket {
	uint32_t connectPacketLength;  // sizeof(ConnectPacket)-sizeof(uint32_t), or perhaps greater in later protocol versions
	uint64_t protocolVersion;      // Expect currentProtocolVersion
	uint16_t canonicalRemotePort;  // Port number to reconnect to the originating process
	uint64_t connectionId;         // Multi-version clients will use the same Id for both connections, other connections will set this to zero. Added at protocol Version 0x0FDB00A444020001.
	uint32_t canonicalRemoteIp;    // IP Address to reconnect to the originating process

	size_t minimumSize() {
		if (protocolVersion < CONNECT_PACKET_V0) return CONNECT_PACKET_V0_SIZE;
		if (protocolVersion < CONNECT_PACKET_V1) return CONNECT_PACKET_V1_SIZE;
		return CONNECT_PACKET_V2_SIZE;
	}
};

static_assert( sizeof(ConnectPacket) == CONNECT_PACKET_V2_SIZE, "ConnectPacket packed incorrectly" );
#pragma pack( pop )

static Future<Void> connectionReader( TransportData* const& transport, Reference<IConnection> const& conn, Peer* const& peer, Promise<Peer*> const& onConnected );

static PacketID sendPacket( TransportData* self, ISerializeSource const& what, const Endpoint& destination, bool reliable );

struct Peer : NonCopyable {
	// FIXME: Peers don't die!

	TransportData* transport;
	NetworkAddress destination;
	UnsentPacketQueue unsent;
	ReliablePacketList reliable;
	AsyncTrigger dataToSend;  // Triggered when unsent.empty() becomes false
	Future<Void> connect;
	AsyncTrigger incompatibleDataRead;
	bool compatible;
	bool outgoingConnectionIdle;  // We don't actually have a connection open and aren't trying to open one because we don't have anything to send
	double lastConnectTime;
	double reconnectionDelay;

	explicit Peer( TransportData* transport, NetworkAddress const& destination, bool doConnect = true ) 
		: transport(transport), destination(destination), outgoingConnectionIdle(!doConnect), lastConnectTime(0.0), reconnectionDelay(FLOW_KNOBS->INITIAL_RECONNECTION_TIME), compatible(true)
	{
		if(doConnect) {
			connect = connectionKeeper(this);
		}
	}

	void send(PacketBuffer* pb, ReliablePacket* rp, bool firstUnsent) {
		unsent.setWriteBuffer(pb);
		if (rp) reliable.insert(rp);
		if (firstUnsent) dataToSend.trigger();
	}

	void prependConnectPacket() {
		// Send the ConnectPacket expected at the beginning of a new connection
		ConnectPacket pkt;
		if (transport->localAddress.isTLS() != destination.isTLS()) {
			pkt.canonicalRemotePort = 0;   // a "mixed" TLS/non-TLS connection is like a client/server connection - there's no way to reverse it
			pkt.canonicalRemoteIp = 0;
		}
		else {
			pkt.canonicalRemotePort = transport->localAddress.port;
			pkt.canonicalRemoteIp = transport->localAddress.ip;
		}
		pkt.connectPacketLength = sizeof(pkt)-sizeof(pkt.connectPacketLength);
		pkt.protocolVersion = currentProtocolVersion;
		pkt.connectionId = transport->transportId;

		PacketBuffer* pb_first = new PacketBuffer;
		PacketWriter wr( pb_first, NULL, Unversioned() );
		wr.serializeBinaryItem(pkt);
		unsent.prependWriteBuffer(pb_first, wr.finish());
	}

	void discardUnreliablePackets() {
		// Throw away the current unsent list, dropping the reference count on each PacketBuffer that accounts for presence in the unsent list
		unsent.discardAll();

		// Compact reliable packets into a new unsent range
		PacketBuffer* pb = unsent.getWriteBuffer();
		pb = reliable.compact(pb, NULL);
		unsent.setWriteBuffer(pb);
	}

	void onIncomingConnection( Reference<IConnection> conn, Future<Void> reader ) {
		// In case two processes are trying to connect to each other simultaneously, the process with the larger canonical NetworkAddress
		// gets to keep its outgoing connection.
		if ( !destination.isPublic() && !outgoingConnectionIdle ) throw address_in_use();
		if ( !destination.isPublic() || outgoingConnectionIdle || destination > transport->localAddress ) {
			// Keep the new connection
			TraceEvent("IncomingConnection", conn->getDebugID())
				.detail("FromAddr", conn->getPeerAddress())
				.detail("CanonicalAddr", destination)
				.detail("IsPublic", destination.isPublic());

			connect.cancel();
			prependConnectPacket();
			connect = connectionKeeper( this, conn, reader );
		} else {
			TraceEvent("RedundantConnection", conn->getDebugID())
				.detail("FromAddr", conn->getPeerAddress().toString())
				.detail("CanonicalAddr", destination);

			// Keep our prior connection
			reader.cancel();
			conn->close();

			// Send an (ignored) packet to make sure that, if our outgoing connection died before the peer made this connection attempt,
			// we eventually find out that our connection is dead, close it, and then respond to the next connection reattempt from peer.
			//sendPacket( self, SerializeSourceRaw(StringRef()), Endpoint(peer->address(), TOKEN_IGNORE_PACKET), false );
		}
	}

	ACTOR static Future<Void> connectionMonitor( Peer *peer ) {
		state RequestStream< ReplyPromise<Void> > remotePing( Endpoint( peer->destination, WLTOKEN_PING_PACKET ) );

		loop {
			Void _ = wait( delayJittered( FLOW_KNOBS->CONNECTION_MONITOR_LOOP_TIME ) );

			// SOMEDAY: Stop monitoring and close the connection after a long period of inactivity with no reliable or onDisconnect requests outstanding

			state ReplyPromise<Void> reply;
			FlowTransport::transport().sendUnreliable( SerializeSource<ReplyPromise<Void>>(reply), remotePing.getEndpoint() );

			choose {
				when (Void _ = wait( delay( FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT ) )) { TraceEvent("ConnectionTimeout").detail("WithAddr", peer->destination); throw connection_failed(); }
				when (Void _ = wait( reply.getFuture() )) {}
				when (Void _ = wait( peer->incompatibleDataRead.onTrigger())) {}
			}
		}
	}

	ACTOR static Future<Void> connectionWriter( Peer* self, Reference<IConnection> conn ) {
		state double lastWriteTime = now();
		loop {
			//Void _ = wait( delay(0, TaskWriteSocket) );
			Void _ = wait( delayJittered(std::max<double>(FLOW_KNOBS->MIN_COALESCE_DELAY, FLOW_KNOBS->MAX_COALESCE_DELAY - (now() - lastWriteTime)), TaskWriteSocket) );
			//Void _ = wait( delay(500e-6, TaskWriteSocket) );
			//Void _ = wait( yield(TaskWriteSocket) );

			// Send until there is nothing left to send
			loop {
				lastWriteTime = now();

				int sent = conn->write( self->unsent.getUnsent() );
				if (sent) {
					self->transport->bytesSent += sent;
					self->unsent.sent(sent);
				}
				if (self->unsent.empty()) break;

				TEST(true); // We didn't write everything, so apparently the write buffer is full.  Wait for it to be nonfull.
				Void _ = wait( conn->onWritable() );
				Void _ = wait( yield(TaskWriteSocket) );
			}

			// Wait until there is something to send
			while ( self->unsent.empty() )
				Void _ = wait( self->dataToSend.onTrigger() );
		}
	}

	ACTOR static Future<Void> connectionKeeper( Peer* self, 
			Reference<IConnection> conn = Reference<IConnection>(), 
			Future<Void> reader = Void()) {
		TraceEvent(SevDebug, "ConnKeeper", conn ? conn->getDebugID() : UID())
			.detail("PeerAddr", self->destination)
			.detail("ConnSet", (bool)conn);
		loop {
			try {
				if (!conn) {  // Always, except for the first loop with an incoming connection
					self->outgoingConnectionIdle = true;
					// Wait until there is something to send
					while ( self->unsent.empty() )
						Void _ = wait( self->dataToSend.onTrigger() );
					ASSERT( self->destination.isPublic() );
					self->outgoingConnectionIdle = false;
					Void _ = wait( delayJittered( std::max(0.0, self->lastConnectTime+self->reconnectionDelay - now()) ) );  // Don't connect() to the same peer more than once per 2 sec
					self->lastConnectTime = now();

					TraceEvent("ConnectingTo", conn ? conn->getDebugID() : UID()).detail("PeerAddr", self->destination).suppressFor(1.0);
					Reference<IConnection> _conn = wait( timeout( INetworkConnections::net()->connect(self->destination), FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT, Reference<IConnection>() ) );
					if (_conn) {
						conn = _conn;
						TraceEvent("ConnectionExchangingConnectPacket", conn->getDebugID()).detail("PeerAddr", self->destination).suppressFor(1.0);
						self->prependConnectPacket();
					} else {
						TraceEvent("ConnectionTimedOut", conn ? conn->getDebugID() : UID()).detail("PeerAddr", self->destination).suppressFor(1.0);
						throw connection_failed();
					}

					reader = connectionReader( self->transport, conn, self, Promise<Peer*>());
				} else {
					self->outgoingConnectionIdle = false;
				}

				try {
					self->transport->countConnEstablished++;
					Void _ = wait( connectionWriter( self, conn ) || reader || connectionMonitor(self) );
				} catch (Error& e) {
					 if (e.code() == error_code_connection_failed || e.code() == error_code_actor_cancelled || ( g_network->isSimulated() && e.code() == error_code_checksum_failed ))
						self->transport->countConnClosedWithoutError++;
					else
						self->transport->countConnClosedWithError++;
					throw e;
				}

				ASSERT( false );
			} catch (Error& e) {
				if(now() - self->lastConnectTime > FLOW_KNOBS->RECONNECTION_RESET_TIME) {
					self->reconnectionDelay = FLOW_KNOBS->INITIAL_RECONNECTION_TIME;
				} else {
					self->reconnectionDelay = std::min(FLOW_KNOBS->MAX_RECONNECTION_TIME, self->reconnectionDelay * FLOW_KNOBS->RECONNECTION_TIME_GROWTH_RATE);
				}
				self->discardUnreliablePackets();
				reader = Future<Void>();
				bool ok = e.code() == error_code_connection_failed || e.code() == error_code_actor_cancelled || ( g_network->isSimulated() && e.code() == error_code_checksum_failed );

				if(self->compatible) {
					TraceEvent(ok ? SevInfo : SevWarnAlways, "ConnectionClosed", conn ? conn->getDebugID() : UID()).detail("PeerAddr", self->destination).error(e, true).suppressFor(1.0);
				}
				else {
					TraceEvent(ok ? SevInfo : SevError, "IncompatibleConnectionClosed", conn ? conn->getDebugID() : UID()).detail("PeerAddr", self->destination).error(e, true);
				}

				if (conn) {
					conn->close();
					conn = Reference<IConnection>();
				}
				IFailureMonitor::failureMonitor().notifyDisconnect( self->destination );  //< Clients might send more packets in response, which needs to go out on the next connection
				if (e.code() == error_code_actor_cancelled) throw;
				// Try to recover, even from serious errors, by retrying
			}
		}
	}
};

ACTOR static void deliver( TransportData* self, Endpoint destination, ArenaReader reader, bool inReadSocket ) {
	int priority = self->endpoints.getPriority(destination.token);
	if (priority < TaskReadSocket || !inReadSocket) {
		Void _ = wait( delay(0, priority) );
	} else {
		g_network->setCurrentTask( priority );
	}

	auto receiver = self->endpoints.get(destination.token);
	if (receiver) {
		try {
			g_currentDeliveryPeerAddress = destination.address;
			receiver->receive( reader );
			g_currentDeliveryPeerAddress = NetworkAddress();
		} catch (Error& e) {
			g_currentDeliveryPeerAddress = NetworkAddress();
			TraceEvent(SevError, "ReceiverError").error(e).detail("Token", destination.token.toString()).detail("Peer", destination.address);
			throw;
		}
	} else if (destination.token.first() & TOKEN_STREAM_FLAG) {
		// We don't have the (stream) endpoint 'token', notify the remote machine
		if (destination.token.first() != -1)
			sendPacket( self, 
				SerializeSource<Endpoint>( Endpoint( self->localAddress, destination.token ) ), 
				Endpoint( destination.address, WLTOKEN_ENDPOINT_NOT_FOUND), 
				false );
	}

	if( inReadSocket )
		g_network->setCurrentTask( TaskReadSocket );
}

static void scanPackets( TransportData* transport, uint8_t*& unprocessed_begin, uint8_t* e, Arena& arena, NetworkAddress const& peerAddress, uint64_t peerProtocolVersion ) {
	// Find each complete packet in the given byte range and queue a ready task to deliver it.
	// Remove the complete packets from the range by increasing unprocessed_begin.
	// There won't be more than 64K of data plus one packet, so this shouldn't take a long time.
	uint8_t* p = unprocessed_begin;

	bool checksumEnabled = true;
	if (transport->localAddress.isTLS() || peerAddress.isTLS()) {
		checksumEnabled = false;
	}

	loop {
		uint32_t packetLen, packetChecksum;

		//Retrieve packet length and checksum
		if (checksumEnabled) {
			if (e-p < sizeof(uint32_t) * 2) break;
			packetLen = *(uint32_t*)p; p += sizeof(uint32_t);
			packetChecksum = *(uint32_t*)p; p += sizeof(uint32_t);
		} else {
			if (e-p < sizeof(uint32_t)) break;
			packetLen = *(uint32_t*)p; p += sizeof(uint32_t);
		}

		if (packetLen > FLOW_KNOBS->PACKET_LIMIT) {
			TraceEvent(SevError, "Net2_PacketLimitExceeded").detail("FromPeer", peerAddress.toString()).detail("Length", (int)packetLen);
			throw platform_error();
		}
		else if (packetLen > FLOW_KNOBS->PACKET_WARNING) {
			TraceEvent(transport->warnAlwaysForLargePacket ? SevWarnAlways : SevWarn, "Net2_LargePacket")
				.detail("FromPeer", peerAddress.toString())
				.detail("Length", (int)packetLen)
				.suppressFor(1.0);

			if(g_network->isSimulated())
				transport->warnAlwaysForLargePacket = false;
		}

		if (e-p<packetLen) break;
		ASSERT( packetLen >= sizeof(UID) );

		if (checksumEnabled) {
			bool isBuggifyEnabled = false;
			if(g_network->isSimulated() && g_network->now() - g_simulator.lastConnectionFailure > g_simulator.connectionFailuresDisableDuration && BUGGIFY_WITH_PROB(0.0001)) {
				g_simulator.lastConnectionFailure = g_network->now();
				isBuggifyEnabled = true;
				TraceEvent(SevInfo, "BitsFlip");
				int flipBits = 32 - (int) floor(log2(g_random->randomUInt32()));

				uint32_t firstFlipByteLocation = g_random->randomUInt32() % packetLen;
				int firstFlipBitLocation = g_random->randomInt(0, 8);
				*(p + firstFlipByteLocation) ^= 1 << firstFlipBitLocation;
				flipBits--;

				for (int i = 0; i < flipBits; i++) {
					uint32_t byteLocation = g_random->randomUInt32() % packetLen;
					int bitLocation = g_random->randomInt(0, 8);
					if (byteLocation != firstFlipByteLocation || bitLocation != firstFlipBitLocation) {
						*(p + byteLocation) ^= 1 << bitLocation;
					}
				}
			}

			uint32_t calculatedChecksum = crc32c_append(0, p, packetLen);
			if (calculatedChecksum != packetChecksum) {
				if (isBuggifyEnabled) {
					TraceEvent(SevInfo, "ChecksumMismatchExp").detail("packetChecksum", (int)packetChecksum).detail("calculatedChecksum", (int)calculatedChecksum);
				} else {
					TraceEvent(SevWarnAlways, "ChecksumMismatchUnexp").detail("packetChecksum", (int)packetChecksum).detail("calculatedChecksum", (int)calculatedChecksum);
				}
				throw checksum_failed();
			} else {
				if (isBuggifyEnabled) {
					TraceEvent(SevError, "ChecksumMatchUnexp").detail("packetChecksum", (int)packetChecksum).detail("calculatedChecksum", (int)calculatedChecksum);
				}
			}
		}

#if VALGRIND
		VALGRIND_CHECK_MEM_IS_DEFINED(p, packetLen);
#endif
		ArenaReader reader( arena, StringRef(p, packetLen), AssumeVersion(peerProtocolVersion) );
		UID token; reader >> token;

		++transport->countPacketsReceived;

		deliver( transport, Endpoint( peerAddress, token ), std::move(reader), true );

		unprocessed_begin = p = p + packetLen;
	}
}

ACTOR static Future<Void> connectionReader(
		TransportData* transport,
		Reference<IConnection> conn, 
		Peer *peer,
		Promise<Peer*> onConnected) 
{
	// This actor exists whenever there is an open or opening connection, whether incoming or outgoing
	// For incoming connections conn is set and peer is initially NULL; for outgoing connections it is the reverse

	state Arena arena;
	state uint8_t* unprocessed_begin = NULL;
	state uint8_t* unprocessed_end = NULL;
	state uint8_t* buffer_end = NULL;
	state bool expectConnectPacket = true;
	state bool compatible = false;
	state NetworkAddress peerAddress;
	state uint64_t peerProtocolVersion = 0;

	peerAddress = conn->getPeerAddress();
	if (peer == nullptr) { 
		ASSERT( !peerAddress.isPublic() );
	}
	try {
		loop {
			loop {
				int readAllBytes = buffer_end - unprocessed_end;
				if (readAllBytes < 4096) {
					Arena newArena;
					int unproc_len = unprocessed_end - unprocessed_begin;
					int len = std::max( 65536, unproc_len*2 );
					uint8_t* newBuffer = new (newArena) uint8_t[ len ];
					memcpy( newBuffer, unprocessed_begin, unproc_len );
					arena = newArena;
					unprocessed_begin = newBuffer;
					unprocessed_end = newBuffer + unproc_len;
					buffer_end = newBuffer + len;
					readAllBytes = buffer_end - unprocessed_end;
				}

				int readBytes = conn->read( unprocessed_end, buffer_end );
				if (!readBytes) break;
				state bool readWillBlock = readBytes != readAllBytes;
				unprocessed_end += readBytes;
			
				if (expectConnectPacket && unprocessed_end-unprocessed_begin>=CONNECT_PACKET_V0_SIZE) {
					// At the beginning of a connection, we expect to receive a packet containing the protocol version and the listening port of the remote process
					ConnectPacket* p = (ConnectPacket*)unprocessed_begin;
				
					uint64_t connectionId = 0;
					int32_t connectPacketSize = p->minimumSize();
					if ( unprocessed_end-unprocessed_begin >= connectPacketSize ) {
						if(p->protocolVersion >= 0x0FDB00A444020001) {
							connectionId = p->connectionId;
						}
						
						if( (p->protocolVersion&compatibleProtocolVersionMask) != (currentProtocolVersion&compatibleProtocolVersionMask) ) {
							NetworkAddress addr = p->canonicalRemotePort ? NetworkAddress( p->canonicalRemoteIp, p->canonicalRemotePort ) : conn->getPeerAddress();
							if(connectionId != 1) addr.port = 0;
						
							if(!transport->multiVersionConnections.count(connectionId)) {
								if(now() - transport->lastIncompatibleMessage > FLOW_KNOBS->CONNECTION_REJECTED_MESSAGE_DELAY) {
									TraceEvent(SevWarn, "ConnectionRejected", conn->getDebugID())
										.detail("Reason", "IncompatibleProtocolVersion")
										.detail("LocalVersion", currentProtocolVersion)
										.detail("RejectedVersion", p->protocolVersion)
										.detail("VersionMask", compatibleProtocolVersionMask)
										.detail("Peer", p->canonicalRemotePort ? NetworkAddress( p->canonicalRemoteIp, p->canonicalRemotePort ) : conn->getPeerAddress())
										.detail("ConnectionId", connectionId);
									transport->lastIncompatibleMessage = now();
								}
								if(!transport->incompatiblePeers.count(addr)) {
									transport->incompatiblePeers[ addr ] = std::make_pair(connectionId, now());
								}
							} else if(connectionId > 1) {
								transport->multiVersionConnections[connectionId] = now() + FLOW_KNOBS->CONNECTION_ID_TIMEOUT;
							}

							compatible = false;
							if(p->protocolVersion < 0x0FDB00A551000000LL) {
								// Older versions expected us to hang up. It may work even if we don't hang up here, but it's safer to keep the old behavior.
								throw incompatible_protocol_version();
							}
						}
						else {
							compatible = true;
							TraceEvent("ConnectionEstablished", conn->getDebugID())
								.detail("Peer", conn->getPeerAddress())
								.detail("ConnectionId", connectionId).suppressFor(1.0);
						}

						if(connectionId > 1) {
							transport->multiVersionConnections[connectionId] = now() + FLOW_KNOBS->CONNECTION_ID_TIMEOUT;
						}
						unprocessed_begin += connectPacketSize;
						expectConnectPacket = false;

						peerProtocolVersion = p->protocolVersion;
						if (peer != nullptr) {
							// Outgoing connection; port information should be what we expect
							TraceEvent("ConnectedOutgoing").detail("PeerAddr", NetworkAddress( p->canonicalRemoteIp, p->canonicalRemotePort ) ).suppressFor(1.0);
							peer->compatible = compatible;
							if (!compatible)
								peer->transport->numIncompatibleConnections++;
							ASSERT( p->canonicalRemotePort == peerAddress.port );
						} else {
							if (p->canonicalRemotePort) {
								peerAddress = NetworkAddress( p->canonicalRemoteIp, p->canonicalRemotePort, true, peerAddress.isTLS() );
							}
							peer = transport->getPeer(peerAddress);
							peer->compatible = compatible;
							if (!compatible)
								peer->transport->numIncompatibleConnections++;
							onConnected.send( peer );
							Void _ = wait( delay(0) );  // Check for cancellation
						}
					}
				}
				if (compatible) {
					scanPackets( transport, unprocessed_begin, unprocessed_end, arena, peerAddress, peerProtocolVersion );
				}
				else if(!expectConnectPacket) {
					unprocessed_begin = unprocessed_end;
					peer->incompatibleDataRead.trigger();
				}

				if (readWillBlock)
					break;

				Void _ = wait(yield(TaskReadSocket));
			}

			Void _ = wait( conn->onReadable() );
			Void _ = wait(delay(0, TaskReadSocket));  // We don't want to call conn->read directly from the reactor - we could get stuck in the reactor reading 1 packet at a time
		}
	}
	catch (Error& e) {
		if (peer && !peer->compatible) {
			ASSERT(peer->transport->numIncompatibleConnections > 0);
			peer->transport->numIncompatibleConnections--;
		}
		throw;
	}
}

ACTOR static Future<Void> connectionIncoming( TransportData* self, Reference<IConnection> conn ) {
	try {
		state Promise<Peer*> onConnected;
		state Future<Void> reader = connectionReader( self, conn, nullptr, onConnected );
		choose {
			when( Void _ = wait( reader ) ) { ASSERT(false); return Void(); }
			when( Peer *p = wait( onConnected.getFuture() ) ) {
				p->onIncomingConnection( conn, reader );
			}
			when( Void _ = wait( delayJittered(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT) ) ) {
				TEST(true);  // Incoming connection timed out
				throw timed_out();
			}
		}
		return Void();
	} catch (Error& e) {
		TraceEvent("IncomingConnectionError", conn->getDebugID()).error(e).detail("FromAddress", conn->getPeerAddress()).suppressFor(1.0);
		conn->close();
		return Void();
	}
}

ACTOR static Future<Void> listen( TransportData* self, NetworkAddress listenAddr ) {
	state ActorCollectionNoErrors incoming;  // Actors monitoring incoming connections that haven't yet been associated with a peer
	state Reference<IListener> listener = INetworkConnections::net()->listen( listenAddr );
	try {
		loop {
			Reference<IConnection> conn = wait( listener->accept() );
			TraceEvent("ConnectionFrom", conn->getDebugID()).detail("FromAddress", conn->getPeerAddress()).suppressFor(1.0);
			incoming.add( connectionIncoming(self, conn) );
		}
	} catch (Error& e) {
		TraceEvent(SevError, "ListenError").error(e);
		throw;
	}
}

Peer* TransportData::getPeer( NetworkAddress const& address, bool doConnect ) {
	auto& peer = peers[address];
	if (!peer) peer = new Peer(this, address, doConnect);
	return peer;
}

ACTOR static Future<Void> multiVersionCleanupWorker( TransportData* self ) {
	loop {
		Void _ = wait(delay(FLOW_KNOBS->CONNECTION_CLEANUP_DELAY));
		for(auto it = self->incompatiblePeers.begin(); it != self->incompatiblePeers.end();) {
			if( self->multiVersionConnections.count(it->second.first) ) {
				it = self->incompatiblePeers.erase(it);
			} else {
				it++;
			}
		}

		for(auto it = self->multiVersionConnections.begin(); it != self->multiVersionConnections.end();) {
			if( it->second < now() ) {
				it = self->multiVersionConnections.erase(it);
			} else {
				it++;
			}
		}
	}
}

FlowTransport::FlowTransport( uint64_t transportId ) : self(new TransportData(transportId)) {
	self->multiVersionCleanup = multiVersionCleanupWorker(self);
}

FlowTransport::~FlowTransport() { delete self; }

void FlowTransport::initMetrics() { self->initMetrics(); }

NetworkAddress FlowTransport::getLocalAddress() { return self->localAddress; }

std::map<NetworkAddress, std::pair<uint64_t, double>>* FlowTransport::getIncompatiblePeers() { 
	for(auto it = self->incompatiblePeers.begin(); it != self->incompatiblePeers.end();) {
		if( self->multiVersionConnections.count(it->second.first) ) {
			it = self->incompatiblePeers.erase(it);
		} else {
			it++;
		}
	}
	return &self->incompatiblePeers; 
}

Future<Void> FlowTransport::bind( NetworkAddress publicAddress, NetworkAddress listenAddress ) {
	ASSERT( publicAddress.isPublic() );
	self->localAddress = publicAddress;
	TraceEvent("Binding").detail("PublicAddress", publicAddress).detail("ListenAddress", listenAddress);
	self->listen = listen( self, listenAddress );
	return self->listen;
}

void FlowTransport::loadedEndpoint( Endpoint& endpoint ) {
	if (endpoint.address.isValid()) return;
	ASSERT( !(endpoint.token.first() & TOKEN_STREAM_FLAG) );  // Only reply promises are supposed to be unaddressed
	ASSERT( g_currentDeliveryPeerAddress.isValid() );
	endpoint.address = g_currentDeliveryPeerAddress;
}

void FlowTransport::addEndpoint( Endpoint& endpoint, NetworkMessageReceiver* receiver, uint32_t taskID ) {
	endpoint.token = g_random->randomUniqueID();
	if (receiver->isStream()) {
		endpoint.address = getLocalAddress();
		endpoint.token = UID( endpoint.token.first() | TOKEN_STREAM_FLAG, endpoint.token.second() );
	} else {
		endpoint.address = NetworkAddress();
		endpoint.token = UID( endpoint.token.first() & ~TOKEN_STREAM_FLAG, endpoint.token.second() );
	}
	self->endpoints.insert( receiver, endpoint.token, taskID );
}

void FlowTransport::removeEndpoint( const Endpoint& endpoint, NetworkMessageReceiver* receiver ) {
	self->endpoints.remove(endpoint.token, receiver);
}

void FlowTransport::addWellKnownEndpoint( Endpoint& endpoint, NetworkMessageReceiver* receiver, uint32_t taskID ) {
	endpoint.address = getLocalAddress();
	ASSERT( ((endpoint.token.first() & TOKEN_STREAM_FLAG)!=0) == receiver->isStream() );
	Endpoint::Token otoken = endpoint.token;
	self->endpoints.insert( receiver, endpoint.token, taskID );
	ASSERT( endpoint.token == otoken );
}

static PacketID sendPacket( TransportData* self, ISerializeSource const& what, const Endpoint& destination, bool reliable ) {
	if (destination.address == self->localAddress) {
		TEST(true); // "Loopback" delivery
		// SOMEDAY: Would it be better to avoid (de)serialization by doing this check in flow?

		BinaryWriter wr( AssumeVersion(currentProtocolVersion) );
		what.serializeBinaryWriter(wr);
		Standalone<StringRef> copy = wr.toStringRef();
#if VALGRIND
		VALGRIND_CHECK_MEM_IS_DEFINED(copy.begin(), copy.size());
#endif

		deliver( self, destination, ArenaReader(copy.arena(), copy, AssumeVersion(currentProtocolVersion)), false );

		return (PacketID)NULL;
	} else {
		bool checksumEnabled = true;
		if (self->localAddress.isTLS() || destination.address.isTLS()) {
			checksumEnabled = false;
		}

		++self->countPacketsGenerated;

		Peer* peer = self->getPeer(destination.address);

		// If there isn't an open connection, a public address, or the peer isn't compatible, we can't send
		if ((peer->outgoingConnectionIdle && !destination.address.isPublic()) || (!peer->compatible && destination.token != WLTOKEN_PING_PACKET)) {
			TEST(true);  // Can't send to private address without a compatible open connection
			return (PacketID)NULL;
		}

		bool firstUnsent = peer->unsent.empty();

		PacketBuffer* pb = peer->unsent.getWriteBuffer();
		ReliablePacket* rp = reliable ? new ReliablePacket : 0;

		void*p = pb->data+pb->bytes_written;
		int prevBytesWritten = pb->bytes_written;
		PacketBuffer* checksumPb = pb;

		PacketWriter wr(pb,rp,AssumeVersion(currentProtocolVersion));  // SOMEDAY: Can we downgrade to talk to older peers?

		// Reserve some space for packet length and checksum, write them after serializing data
		SplitBuffer packetInfoBuffer;
		uint32_t len, checksum = 0;
		int packetInfoSize = sizeof(len);
		if (checksumEnabled) {
			packetInfoSize += sizeof(checksum);
		}

		wr.writeAhead(packetInfoSize , &packetInfoBuffer);
		wr << destination.token;
		what.serializePacketWriter(wr);
		pb = wr.finish();
		len = wr.size() - packetInfoSize;

		if (checksumEnabled) {
			// Find the correct place to start calculating checksum
			uint32_t checksumUnprocessedLength = len;
			prevBytesWritten += packetInfoSize;
			if (prevBytesWritten >= PacketBuffer::DATA_SIZE) {
				prevBytesWritten -= PacketBuffer::DATA_SIZE;
				checksumPb = checksumPb->nextPacketBuffer();
			}

			// Checksum calculation
			while (checksumUnprocessedLength > 0) {
				uint32_t processLength = std::min(checksumUnprocessedLength, (uint32_t)(PacketBuffer::DATA_SIZE - prevBytesWritten));
				checksum = crc32c_append(checksum, checksumPb->data + prevBytesWritten, processLength);
				checksumUnprocessedLength -= processLength;
				checksumPb = checksumPb->nextPacketBuffer();
				prevBytesWritten = 0;
			}
		}

		// Write packet length and checksum into packet buffer
		packetInfoBuffer.write(&len, sizeof(len));
		if (checksumEnabled) {
			packetInfoBuffer.write(&checksum, sizeof(checksum), sizeof(len));
		}

		if (len > FLOW_KNOBS->PACKET_LIMIT) {
			TraceEvent(SevError, "Net2_PacketLimitExceeded").detail("ToPeer", destination.address).detail("Length", (int)len);
			// throw platform_error();  // FIXME: How to recover from this situation?
		} 
		else if (len > FLOW_KNOBS->PACKET_WARNING) {
			TraceEvent(self->warnAlwaysForLargePacket ? SevWarnAlways : SevWarn, "Net2_LargePacket")
				.detail("ToPeer", destination.address)
				.detail("Length", (int)len)
				.suppressFor(1.0);

			if(g_network->isSimulated())
				self->warnAlwaysForLargePacket = false;
		}

#if VALGRIND
		SendBuffer *checkbuf = pb;
		while (checkbuf) {
			int size = checkbuf->bytes_written;
			const uint8_t* data = checkbuf->data;
			VALGRIND_CHECK_MEM_IS_DEFINED(data, size);
			checkbuf = checkbuf -> next;
		}
#endif

		peer->send(pb, rp, firstUnsent);

		return (PacketID)rp;
	}
}

PacketID FlowTransport::sendReliable( ISerializeSource const& what, const Endpoint& destination ) {
	return sendPacket( self, what, destination, true );
}

void FlowTransport::cancelReliable( PacketID pid ) {
	ReliablePacket* p = (ReliablePacket*)pid;
	if (p) p->remove();
	// SOMEDAY: Call reliable.compact() if a lot of memory is wasted in PacketBuffers by formerly reliable packets mixed with a few reliable ones.  Don't forget to delref the new PacketBuffers since they are unsent.
}

void FlowTransport::sendUnreliable( ISerializeSource const& what, const Endpoint& destination ) {
	sendPacket( self, what, destination, false );
}

int FlowTransport::getEndpointCount() { 
	return -1; 
}

bool FlowTransport::incompatibleOutgoingConnectionsPresent() {
	return self->numIncompatibleConnections;
}

void FlowTransport::createInstance( uint64_t transportId )
{
	g_network->setGlobal(INetwork::enFailureMonitor, (flowGlobalType) new SimpleFailureMonitor());
	g_network->setGlobal(INetwork::enFlowTransport, (flowGlobalType) new FlowTransport(transportId));
	g_network->setGlobal(INetwork::enNetworkAddressFunc, (flowGlobalType) &FlowTransport::getGlobalLocalAddress);
}
