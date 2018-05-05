/*
 * network.h
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

#ifndef FLOW_OPENNETWORK_H
#define FLOW_OPENNETWORK_H
#pragma once

#include <string>
#include <stdint.h>
#include "serialize.h"
#include "IRandom.h"

enum {
	TaskMaxPriority = 1000000,
	TaskRunCycleFunction = 20000,
	TaskFlushTrace = 10500,
	TaskWriteSocket = 10000,
	TaskPollEIO = 9900,
	TaskDiskIOComplete = 9150, 
	TaskLoadBalancedEndpoint = 9000,
	TaskReadSocket = 9000,
	TaskCoordinationReply = 8810,
	TaskCoordination = 8800,
	TaskFailureMonitor = 8700,
	TaskResolutionMetrics = 8700,
	TaskClusterController = 8650,
	TaskTLogQueuingMetrics = 8620,
	TaskTLogPop = 8610,
	TaskTLogPeekReply = 8600,
	TaskTLogPeek = 8590,
	TaskTLogCommitReply = 8580,
	TaskTLogCommit = 8570,
	TaskProxyGetRawCommittedVersion = 8565,
	TaskProxyResolverReply = 8560,
	TaskProxyCommitBatcher = 8550,
	TaskProxyCommit = 8540,
	TaskTLogConfirmRunningReply = 8530,
	TaskTLogConfirmRunning = 8520,
	TaskProxyGetKeyServersLocations = 8515,
	TaskProxyGRVTimer = 8510,
	TaskProxyGetConsistentReadVersion = 8500,
	TaskDefaultPromiseEndpoint = 8000,
	TaskDefaultOnMainThread = 7500,
	TaskDefaultDelay = 7010,
	TaskDefaultYield = 7000,
	TaskDiskRead = 5010,
	TaskDefaultEndpoint = 5000,
	TaskUnknownEndpoint = 4000,
	TaskMoveKeys = 3550,
	TaskDataDistributionLaunch = 3530,
	TaskDataDistribution = 3500,
	TaskDiskWrite = 3010,
	TaskUpdateStorage = 3000,
	TaskBatchCopy = 2900,
	TaskLowPriority = 2000,

	TaskMinPriority = 1000
};

class Void;

struct NetworkAddress {
	// A NetworkAddress identifies a particular running server (i.e. a TCP endpoint).
	uint32_t ip;
	uint16_t port;
	uint16_t flags;

	enum { FLAG_PRIVATE = 1, FLAG_TLS = 2 };

	NetworkAddress() : ip(0), port(0), flags(FLAG_PRIVATE) {}
	NetworkAddress( uint32_t ip, uint16_t port ) : ip(ip), port(port), flags(FLAG_PRIVATE) {}
	NetworkAddress( uint32_t ip, uint16_t port, bool isPublic, bool isTLS ) : ip(ip), port(port),
		flags( (isPublic ? 0 : FLAG_PRIVATE) | (isTLS ? FLAG_TLS : 0 ) ) {}

	bool operator == (NetworkAddress const& r) const { return ip==r.ip && port==r.port && flags==r.flags; }
	bool operator != (NetworkAddress const& r) const { return ip!=r.ip || port!=r.port || flags!=r.flags; }
	bool operator< (NetworkAddress const& r) const { if (flags != r.flags) return flags < r.flags; if (ip != r.ip) return ip < r.ip; return port<r.port; }
	bool isValid() const { return ip != 0 || port != 0; }
	bool isPublic() const { return !(flags & FLAG_PRIVATE); }
	bool isTLS() const { return (flags & FLAG_TLS) != 0; }

	static NetworkAddress parse( std::string const& );
	static std::vector<NetworkAddress> parseList( std::string const& );
	std::string toString() const;

	template <class Ar>
	void serialize(Ar& ar) {
		ar.serializeBinaryItem(*this);
	}
};

std::string toIPString(uint32_t ip);
std::string toIPVectorString(std::vector<uint32_t> ips);

template <class T> class Future;
template <class T> class Promise;

struct NetworkMetrics {
	enum { SLOW_EVENT_BINS = 16 };
	uint64_t countSlowEvents[SLOW_EVENT_BINS];

	enum { PRIORITY_BINS = 9 };
	int priorityBins[ PRIORITY_BINS ];
	double secSquaredPriorityBlocked[PRIORITY_BINS];

	double oldestAlternativesFailure;
	double newestAlternativesFailure;
	double lastSync;

	double secSquaredSubmit;
	double secSquaredDiskStall;

	NetworkMetrics() { memset(this, 0, sizeof(*this)); }
};

class IEventFD : public ReferenceCounted<IEventFD> {
public:
	virtual ~IEventFD() {}
	virtual int getFD() = 0;
	virtual Future<int64_t> read() = 0;
};

class IConnection {
public:
	// IConnection is reference-counted (use Reference<IConnection>), but the caller must explicitly call close()
	virtual void addref() = 0;
	virtual void delref() = 0;

	// Closes the underlying connection eventually if it is not already closed.
	virtual void close() = 0;
	
	// returns when write() can write at least one byte (or may throw an error if the connection dies)
	virtual Future<Void> onWritable() = 0;

	// returns when read() can read at least one byte (or may throw an error if the connection dies)
	virtual Future<Void> onReadable() = 0;

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might be 0)
	// (or may throw an error if the connection dies)
	virtual int read( uint8_t* begin, uint8_t* end ) = 0;

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of bytes written (might be 0)
	// (or may throw an error if the connection dies)
	// The SendBuffer chain cannot be empty, and the limit must be positive.
	// Important non-obvious behavior:  The caller is committing to write the contents of the buffer chain up to the limit.  If all of those bytes could 
	// not be sent in this call to write() then further calls must be made to write the remainder.  An IConnection implementation can make decisions
	// based on the entire byte set that the caller was attempting to write even if it is unable to write all of it immediately.
	// Due to limitations of TLSConnection, callers must also avoid reallocations that reduce the amount of written data in the first buffer in the chain.
	virtual int write( SendBuffer const* buffer, int limit = std::numeric_limits<int>::max()) = 0;

	// Returns the network address and port of the other end of the connection.  In the case of an incoming connection, this may not
	// be an address we can connect to!
	virtual NetworkAddress getPeerAddress() = 0;

	virtual UID getDebugID() = 0;
};

class IListener {
public:
	virtual void addref() = 0;
	virtual void delref() = 0;

	// Returns one incoming connection when it is available.  Do not cancel unless you are done with the listener!
	virtual Future<Reference<IConnection>> accept() = 0;

	virtual NetworkAddress getListenAddress() = 0;
};

typedef void*	flowGlobalType;
typedef NetworkAddress (*NetworkAddressFuncPtr)();

class INetwork;
extern INetwork* g_network;
extern INetwork* newNet2(NetworkAddress localAddress, bool useThreadPool = false, bool useMetrics = false);

class INetwork {
public:
	// This interface abstracts the physical or simulated network, event loop and hardware that FoundationDB is running on.
	// Note that there are tools for disk access, scheduling, etc as well as networking, and that almost all access
	//   to the network should be through FlowTransport, not directly through these low level interfaces!

	enum enumGlobal {
		enFailureMonitor = 0, enFlowTransport = 1, enTDMetrics = 2, enNetworkConnections = 3,
		enNetworkAddressFunc = 4, enFileSystem = 5, enASIOService = 6, enEventFD = 7, enRunCycleFunc = 8, enASIOTimedOut = 9, enBlobCredentialFiles = 10
	};

	virtual void longTaskCheck( const char* name ) {}

	virtual double now() = 0;
	// Provides a clock that advances at a similar rate on all connected endpoints
	// FIXME: Return a fixed point Time class

	virtual Future<class Void> delay( double seconds, int taskID ) = 0;
	// The given future will be set after seconds have elapsed

	virtual Future<class Void> yield( int taskID ) = 0;
	// The given future will be set immediately or after higher-priority tasks have executed

	virtual bool check_yield( int taskID ) = 0;
	// Returns true if a call to yield would result in a delay

	virtual int getCurrentTask() = 0;
	// Gets the taskID/priority of the current task

	virtual void setCurrentTask(int taskID ) = 0;
	// Sets the taskID/priority of the current task, without yielding

	virtual flowGlobalType global(int id) = 0;
	virtual void setGlobal(size_t id, flowGlobalType v) = 0;

	virtual void stop() = 0;
	// Terminate the program

	virtual bool isSimulated() const = 0;
	// Returns true if this network is a local simulation

	virtual void onMainThread( Promise<Void>&& signal, int taskID ) = 0;
	// Executes signal.send(Void()) on a/the thread belonging to this network

	virtual THREAD_HANDLE startThread( THREAD_FUNC_RETURN (*func) (void *), void *arg) = 0;
	// Starts a thread and returns a handle to it

	virtual void run() = 0;
	// Devotes this thread to running the network (generally until stop())

	virtual void initMetrics() {}
	// Metrics must be initialized after FlowTransport::createInstance has been called

	virtual void getDiskBytes( std::string const& directory, int64_t& free, int64_t& total) = 0;
	//Gets the number of free and total bytes available on the disk which contains directory

	virtual bool isAddressOnThisHost( NetworkAddress const& addr ) = 0;
	// Returns true if it is reasonably certain that a connection to the given address would be a fast loopback connection

	// Shorthand for transport().getLocalAddress()
	static NetworkAddress getLocalAddress()
	{
		flowGlobalType netAddressFuncPtr = reinterpret_cast<flowGlobalType>(g_network->global(INetwork::enNetworkAddressFunc));
		return (netAddressFuncPtr) ? reinterpret_cast<NetworkAddressFuncPtr>(netAddressFuncPtr)() : NetworkAddress();
	}

	NetworkMetrics networkMetrics;
protected:
	INetwork() {}

	~INetwork() {}	// Please don't try to delete through this interface!
};

class INetworkConnections {
public:
	// Methods for making and accepting network connections.  Logically this is part of the INetwork abstraction
	// that abstracts all interaction with the physical world; it is separated out to make it easy for e.g. transport
	// security to override only these operations without having to delegate everything in INetwork.

	// Make an outgoing connection to the given address.  May return an error or block indefinitely in case of connection problems!
	virtual Future<Reference<IConnection>> connect( NetworkAddress toAddr ) = 0;

	// Resolve host name and service name (such as "http" or can be a plain number like "80") to a list of 1 or more NetworkAddresses
	virtual Future<std::vector<NetworkAddress>> resolveTCPEndpoint( std::string host, std::string service ) = 0;

	// Convenience function to resolve host/service and connect to one of its NetworkAddresses randomly
	// useTLS has to be a parameter here because it is passed to connect() as part of the toAddr object.
	virtual Future<Reference<IConnection>> connect( std::string host, std::string service, bool useTLS = false);

	// Listen for connections on the given local address
	virtual Reference<IListener> listen( NetworkAddress localAddr ) = 0;

	static INetworkConnections* net() { return static_cast<INetworkConnections*>((void*) g_network->global(INetwork::enNetworkConnections)); }
	// Returns the interface that should be used to make and accept socket connections
};

#endif
