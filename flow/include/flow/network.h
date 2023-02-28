/*
 * network.h
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

#ifndef FLOW_OPENNETWORK_H
#define FLOW_OPENNETWORK_H
#pragma once

#include "flow/NetworkAddress.h"
#include "flow/IPAddress.h"
#include "flow/TaskPriority.h"

#include <string>
#include <stdint.h>
#include <atomic>
#include <unordered_map>
#include "flow/IRandom.h"
#include "flow/ProtocolVersion.h"
#include "flow/WriteOnlySet.h"

class Void;

std::string toIPVectorString(std::vector<uint32_t> ips);
std::string toIPVectorString(const std::vector<IPAddress>& ips);
std::string formatIpPort(const IPAddress& ip, uint16_t port);

template <class T>
class Future;
template <class T>
class Promise;

// Metrics which represent various network properties
struct NetworkMetrics {
	enum { SLOW_EVENT_BINS = 16 };
	uint64_t countSlowEvents[SLOW_EVENT_BINS] = {};

	double secSquaredSubmit = 0;
	double secSquaredDiskStall = 0;

	struct PriorityStats {
		TaskPriority priority;

		bool active = false;
		double duration = 0;
		double timer = 0;
		double windowedTimer = 0;
		double maxDuration = 0;

		PriorityStats(TaskPriority priority) : priority(priority) {}
	};

	std::unordered_map<TaskPriority, struct PriorityStats> activeTrackers;
	double lastRunLoopBusyness; // network thread busyness (measured every 5s by default)
	std::atomic<double>
	    networkBusyness; // network thread busyness which is returned to the the client (measured every 1s by default)

	// starvation trackers which keeps track of different task priorities
	std::vector<struct PriorityStats> starvationTrackers;
	struct PriorityStats starvationTrackerNetworkBusyness;

	static const std::vector<int> starvationBins;

	NetworkMetrics()
	  : lastRunLoopBusyness(0), networkBusyness(0),
	    starvationTrackerNetworkBusyness(PriorityStats(static_cast<TaskPriority>(starvationBins.at(0)))) {
		for (int priority : starvationBins) { // initalize starvation trackers with given priorities
			starvationTrackers.emplace_back(static_cast<TaskPriority>(priority));
		}
	}

	// Since networkBusyness is atomic we need to redefine copy assignment operator
	NetworkMetrics& operator=(const NetworkMetrics& rhs) {
		for (int i = 0; i < SLOW_EVENT_BINS; i++) {
			countSlowEvents[i] = rhs.countSlowEvents[i];
		}
		secSquaredSubmit = rhs.secSquaredSubmit;
		secSquaredDiskStall = rhs.secSquaredDiskStall;
		activeTrackers = rhs.activeTrackers;
		lastRunLoopBusyness = rhs.lastRunLoopBusyness;
		networkBusyness = rhs.networkBusyness.load();
		starvationTrackers = rhs.starvationTrackers;
		starvationTrackerNetworkBusyness = rhs.starvationTrackerNetworkBusyness;
		return *this;
	}
};

struct FlowLock;

struct NetworkInfo {
	NetworkMetrics metrics;
	double oldestAlternativesFailure = 0;
	double newestAlternativesFailure = 0;
	double lastAlternativesFailureSkipDelay = 0;

	std::map<std::pair<IPAddress, uint16_t>, std::pair<int, double>> serverTLSConnectionThrottler;
	FlowLock* handshakeLock;

	NetworkInfo();
};

class IEventFD : public ReferenceCounted<IEventFD> {
public:
	virtual ~IEventFD() {}
	virtual int getFD() = 0;
	virtual Future<int64_t> read() = 0;
};

typedef void* flowGlobalType;
typedef NetworkAddress (*NetworkAddressFuncPtr)();
typedef NetworkAddressList (*NetworkAddressesFuncPtr)();

class TLSConfig;
class INetwork;
extern INetwork* g_network;
extern INetwork* newNet2(const TLSConfig& tlsConfig, bool useThreadPool = false, bool useMetrics = false);

class INetwork {
public:
	// This interface abstracts the physical or simulated network, event loop and hardware that FoundationDB is running
	// on. Note that there are tools for disk access, scheduling, etc as well as networking, and that almost all access
	//   to the network should be through FlowTransport, not directly through these low level interfaces!

	// Time instants (e.g. from now()) within TIME_EPS are considered to be equal.
	static constexpr double TIME_EPS = 1e-7; // 100ns

	enum enumGlobal {
		enFailureMonitor = 0,
		enFlowTransport = 1,
		enTDMetrics = 2,
		enNetworkConnections = 3,
		enNetworkAddressFunc = 4,
		enFileSystem = 5,
		enASIOService = 6,
		enEventFD = 7,
		enRunCycleFunc = 8,
		enASIOTimedOut = 9,
		enBlobCredentialFiles = 10,
		enNetworkAddressesFunc = 11,
		enClientFailureMonitor = 12,
		enSQLiteInjectedError = 13,
		enGlobalConfig = 14,
		enChaosMetrics = 15,
		enDiskFailureInjector = 16,
		enBitFlipper = 17,
		enHistogram = 18,
		enTokenCache = 19,
		enMetrics = 20,
		COUNT // Add new fields before this enumerator
	};

	virtual void longTaskCheck(const char* name) {}

	virtual double now() const = 0;
	// Provides a clock that advances at a similar rate on all connected endpoints
	// FIXME: Return a fixed point Time class

	virtual double timer() = 0;
	// A wrapper for directly getting the system time. The time returned by now() only updates in the run loop,
	// so it cannot be used to measure times of functions that do not have wait statements.

	// Simulation version of timer_int for convenience, based on timer()
	// Returns epoch nanoseconds
	uint64_t timer_int() { return (uint64_t)(g_network->timer() * 1e9); }

	virtual double timer_monotonic() = 0;
	// Similar to timer, but monotonic

	virtual Future<class Void> delay(double seconds, TaskPriority taskID) = 0;
	// The given future will be set after seconds have elapsed

	virtual Future<class Void> orderedDelay(double seconds, TaskPriority taskID) = 0;
	// The given future will be set after seconds have elapsed, delays with the same time and TaskPriority will be
	// executed in the order they were issues

	virtual Future<class Void> yield(TaskPriority taskID) = 0;
	// The given future will be set immediately or after higher-priority tasks have executed

	virtual bool check_yield(TaskPriority taskID) = 0;
	// Returns true if a call to yield would result in a delay

	virtual TaskPriority getCurrentTask() const = 0;
	// Gets the taskID/priority of the current task

	virtual void setCurrentTask(TaskPriority taskID) = 0;
	// Sets the taskID/priority of the current task, without yielding

	virtual flowGlobalType global(int id) const = 0;
	virtual void setGlobal(size_t id, flowGlobalType v) = 0;

	virtual void stop() = 0;
	// Terminate the program

	virtual void addStopCallback(std::function<void()> fn) = 0;
	// Calls `fn` when stop() is called.
	// addStopCallback can be called more than once, and each added `fn` will be run once.

	virtual bool isSimulated() const = 0;
	// Returns true if this network is a local simulation

	virtual bool isOnMainThread() const = 0;
	// Returns true if the current thread is the main thread

	virtual void onMainThread(Promise<Void>&& signal, TaskPriority taskID) = 0;
	// Executes signal.send(Void()) on a/the thread belonging to this network in FIFO order

	virtual THREAD_HANDLE startThread(THREAD_FUNC_RETURN (*func)(void*),
	                                  void* arg,
	                                  int stackSize = 0,
	                                  const char* name = nullptr) = 0;
	// Starts a thread and returns a handle to it

	virtual void run() = 0;
	// Devotes this thread to running the network (generally until stop())

	virtual void initMetrics() {}
	// Metrics must be initialized after FlowTransport::createInstance has been called

	// TLS must be initialized before using the network
	enum ETLSInitState { NONE = 0, CONFIG = 1, CONNECT = 2, LISTEN = 3 };
	virtual void initTLS(ETLSInitState targetState = CONFIG) {}

	virtual const TLSConfig& getTLSConfig() const = 0;
	// Return the TLS Configuration

	virtual void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) = 0;
	// Gets the number of free and total bytes available on the disk which contains directory

	virtual bool isAddressOnThisHost(NetworkAddress const& addr) const = 0;
	// Returns true if it is reasonably certain that a connection to the given address would be a fast loopback
	// connection

	// If the network has not been run and this function has not been previously called, returns true. Otherwise,
	// returns false.
	virtual bool checkRunnable() = 0;

#ifdef ENABLE_SAMPLING
	// Returns the shared memory data structure used to store actor lineages.
	virtual ActorLineageSet& getActorLineageSet() = 0;
#endif

	virtual ProtocolVersion protocolVersion() const = 0;

	// Shorthand for transport().getLocalAddress()
	static NetworkAddress getLocalAddress() {
		flowGlobalType netAddressFuncPtr =
		    reinterpret_cast<flowGlobalType>(g_network->global(INetwork::enNetworkAddressFunc));
		return (netAddressFuncPtr) ? reinterpret_cast<NetworkAddressFuncPtr>(netAddressFuncPtr)() : NetworkAddress();
	}

	// Shorthand for transport().getLocalAddresses()
	static NetworkAddressList getLocalAddresses() {
		flowGlobalType netAddressesFuncPtr =
		    reinterpret_cast<flowGlobalType>(g_network->global(INetwork::enNetworkAddressesFunc));
		return (netAddressesFuncPtr) ? reinterpret_cast<NetworkAddressesFuncPtr>(netAddressesFuncPtr)()
		                             : NetworkAddressList();
	}

	NetworkInfo networkInfo;

protected:
	INetwork() {}

	~INetwork() {} // Please don't try to delete through this interface!
};

#endif
