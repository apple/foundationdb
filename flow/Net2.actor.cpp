/*
 * Net2.actor.cpp
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

#include "boost/asio/buffer.hpp"
#include "boost/asio/ip/address.hpp"
#include "boost/system/system_error.hpp"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include <algorithm>
#include <memory>
#define BOOST_SYSTEM_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#define BOOST_REGEX_NO_LIB
#include <boost/asio.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/range.hpp>
#include <boost/algorithm/string/join.hpp>
#include "flow/network.h"
#include "flow/IThreadPool.h"

#include "flow/ActorCollection.h"
#include "flow/ThreadSafeQueue.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/TDMetric.actor.h"
#include "flow/AsioReactor.h"
#include "flow/Profiler.h"
#include "flow/ProtocolVersion.h"
#include "flow/SendBufferIterator.h"
#include "flow/TLSConfig.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/Util.h"

#ifdef ADDRESS_SANITIZER
#include <sanitizer/lsan_interface.h>
#endif

// See the comment in TLSConfig.actor.h for the explanation of why this module breaking include was done.
#include "fdbrpc/IAsyncFile.h"

#ifdef WIN32
#include <mmsystem.h>
#endif
#include "flow/actorcompiler.h" // This must be the last #include.

// Defined to track the stack limit
extern "C" intptr_t g_stackYieldLimit;
intptr_t g_stackYieldLimit = 0;

using namespace boost::asio::ip;

#if defined(__linux__) || defined(__FreeBSD__)
#include <execinfo.h>

std::atomic<int64_t> net2RunLoopIterations(0);
std::atomic<int64_t> net2RunLoopSleeps(0);

volatile size_t net2backtraces_max = 10000;
volatile void** volatile net2backtraces = nullptr;
volatile size_t net2backtraces_offset = 0;
volatile bool net2backtraces_overflow = false;
volatile int net2backtraces_count = 0;

volatile void** other_backtraces = nullptr;
sigset_t sigprof_set;

void initProfiling() {
	net2backtraces = new volatile void*[net2backtraces_max];
	other_backtraces = new volatile void*[net2backtraces_max];

	// According to folk wisdom, calling this once before setting up the signal handler makes
	// it async signal safe in practice :-/
	backtrace(const_cast<void**>(other_backtraces), net2backtraces_max);

	sigemptyset(&sigprof_set);
	sigaddset(&sigprof_set, SIGPROF);
}
#endif

DESCR struct SlowTask {
	int64_t clocks; // clocks
	int64_t duration; // ns
	int64_t priority; // priority level
	int64_t numYields; // count
};

namespace N2 { // No indent, it's the whole file

class Net2;
class Peer;
class Connection;

// Outlives main
Net2* g_net2 = nullptr;

class Task {
public:
	virtual void operator()() = 0;
};

struct OrderedTask {
	int64_t priority;
	TaskPriority taskID;
	Task* task;
	OrderedTask(int64_t priority, TaskPriority taskID, Task* task) : priority(priority), taskID(taskID), task(task) {}
	bool operator<(OrderedTask const& rhs) const { return priority < rhs.priority; }
};

template <class T>
class ReadyQueue : public std::priority_queue<T, std::vector<T>> {
public:
	typedef typename std::priority_queue<T, std::vector<T>>::size_type size_type;
	ReadyQueue(size_type capacity = 0) { reserve(capacity); };
	void reserve(size_type capacity) { this->c.reserve(capacity); }
};

thread_local INetwork* thread_network = 0;

class Net2 final : public INetwork, public INetworkConnections {

private:
	void updateStarvationTracker(struct NetworkMetrics::PriorityStats& binStats,
	                             TaskPriority priority,
	                             TaskPriority lastPriority,
	                             double now);

public:
	Net2(const TLSConfig& tlsConfig, bool useThreadPool, bool useMetrics);
	void initTLS(ETLSInitState targetState) override;
	void run() override;
	void initMetrics() override;

	// INetworkConnections interface
	Future<Reference<IConnection>> connect(NetworkAddress toAddr, const std::string& host) override;
	Future<Reference<IConnection>> connectExternal(NetworkAddress toAddr, const std::string& host) override;
	Future<Reference<IUDPSocket>> createUDPSocket(NetworkAddress toAddr) override;
	Future<Reference<IUDPSocket>> createUDPSocket(bool isV6) override;
	// The mock DNS methods should only be used in simulation.
	void addMockTCPEndpoint(const std::string& host,
	                        const std::string& service,
	                        const std::vector<NetworkAddress>& addresses) override {
		throw operation_failed();
	}
	// The mock DNS methods should only be used in simulation.
	void removeMockTCPEndpoint(const std::string& host, const std::string& service) override {
		throw operation_failed();
	}
	void parseMockDNSFromString(const std::string& s) override { throw operation_failed(); }
	std::string convertMockDNSToString() override { throw operation_failed(); }

	Future<std::vector<NetworkAddress>> resolveTCPEndpoint(const std::string& host,
	                                                       const std::string& service) override;
	std::vector<NetworkAddress> resolveTCPEndpointBlocking(const std::string& host,
	                                                       const std::string& service) override;
	Reference<IListener> listen(NetworkAddress localAddr) override;

	// INetwork interface
	double now() const override { return currentTime; };
	double timer() override { return ::timer(); };
	double timer_monotonic() override { return ::timer_monotonic(); };
	Future<Void> delay(double seconds, TaskPriority taskId) override;
	Future<Void> orderedDelay(double seconds, TaskPriority taskId) override;
	Future<class Void> yield(TaskPriority taskID) override;
	bool check_yield(TaskPriority taskId) override;
	TaskPriority getCurrentTask() const override { return currentTaskID; }
	void setCurrentTask(TaskPriority taskID) override {
		currentTaskID = taskID;
		priorityMetric = (int64_t)taskID;
	}
	void onMainThread(Promise<Void>&& signal, TaskPriority taskID) override;
	bool isOnMainThread() const override { return thread_network == this; }
	void stop() override {
		if (thread_network == this)
			stopImmediately();
		else
			onMainThreadVoid([this] { this->stopImmediately(); }, nullptr);
	}
	void addStopCallback(std::function<void()> fn) override {
		if (thread_network == this)
			stopCallbacks.emplace_back(std::move(fn));
		else
			onMainThreadVoid([this, fn] { this->stopCallbacks.emplace_back(std::move(fn)); }, nullptr);
	}

	bool isSimulated() const override { return false; }
	THREAD_HANDLE startThread(THREAD_FUNC_RETURN (*func)(void*), void* arg, int stackSize, const char* name) override;

	void getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) override;
	bool isAddressOnThisHost(NetworkAddress const& addr) const override;
	void updateNow() { currentTime = timer_monotonic(); }

	flowGlobalType global(int id) const override { return (globals.size() > id) ? globals[id] : nullptr; }
	void setGlobal(size_t id, flowGlobalType v) override {
		ASSERT(id < globals.size());
		globals[id] = v;
	}

	ProtocolVersion protocolVersion() const override { return currentProtocolVersion; }

	std::vector<flowGlobalType> globals;

	const TLSConfig& getTLSConfig() const override { return tlsConfig; }

	bool checkRunnable() override;

#ifdef ENABLE_SAMPLING
	ActorLineageSet& getActorLineageSet() override;
#endif

	bool useThreadPool;

	// private:

	ASIOReactor reactor;
#ifndef TLS_DISABLED
	AsyncVar<Reference<ReferencedObject<boost::asio::ssl::context>>> sslContextVar;
	Reference<IThreadPool> sslHandshakerPool;
	int sslHandshakerThreadsStarted;
	int sslPoolHandshakesInProgress;
#endif
	TLSConfig tlsConfig;
	Future<Void> backgroundCertRefresh;
	ETLSInitState tlsInitializedState;

	INetworkConnections* network; // initially this, but can be changed

	int64_t tscBegin, tscEnd;
	double taskBegin;
	TaskPriority currentTaskID;
	uint64_t tasksIssued;
	TDMetricCollection tdmetrics;
	ChaosMetrics chaosMetrics;
	// we read now() from a different thread. On Intel, reading a double is atomic anyways, but on other platforms it's
	// not. For portability this should be atomic
	std::atomic<double> currentTime;
	// May be accessed off the network thread, e.g. by onMainThread
	std::atomic<bool> stopped;
	mutable std::map<IPAddress, bool> addressOnHostCache;

#ifdef ENABLE_SAMPLING
	ActorLineageSet actorLineageSet;
#endif

	std::atomic<bool> started;

	uint64_t numYields;

	NetworkMetrics::PriorityStats* lastPriorityStats;

	ReadyQueue<OrderedTask> ready;
	ThreadSafeQueue<OrderedTask> threadReady;

	struct DelayedTask : OrderedTask {
		double at;
		DelayedTask(double at, int64_t priority, TaskPriority taskID, Task* task)
		  : OrderedTask(priority, taskID, task), at(at) {}
		bool operator<(DelayedTask const& rhs) const { return at > rhs.at; } // Ordering is reversed for priority_queue
	};
	std::priority_queue<DelayedTask, std::vector<DelayedTask>> timers;

	void checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, TaskPriority priority);
	bool check_yield(TaskPriority taskId, int64_t tscNow);
	void processThreadReady();
	void trackAtPriority(TaskPriority priority, double now);
	void stopImmediately() {
#ifdef ADDRESS_SANITIZER
		// Do leak check before intentionally leaking a bunch of memory
		__lsan_do_leak_check();
#endif
		stopped = true;
		decltype(ready) _1;
		ready.swap(_1);
		decltype(timers) _2;
		timers.swap(_2);
	}

	Future<Void> timeOffsetLogger;
	Future<Void> logTimeOffset();

	Int64MetricHandle bytesReceived;
	Int64MetricHandle udpBytesReceived;
	Int64MetricHandle countWriteProbes;
	Int64MetricHandle countReadProbes;
	Int64MetricHandle countReads;
	Int64MetricHandle countUDPReads;
	Int64MetricHandle countWouldBlock;
	Int64MetricHandle countWrites;
	Int64MetricHandle countUDPWrites;
	Int64MetricHandle countRunLoop;
	Int64MetricHandle countCantSleep;
	Int64MetricHandle countWontSleep;
	Int64MetricHandle countTimers;
	Int64MetricHandle countTasks;
	Int64MetricHandle countYields;
	Int64MetricHandle countYieldBigStack;
	Int64MetricHandle countYieldCalls;
	Int64MetricHandle countYieldCallsTrue;
	Int64MetricHandle countASIOEvents;
	Int64MetricHandle countRunLoopProfilingSignals;
	Int64MetricHandle countTLSPolicyFailures;
	Int64MetricHandle priorityMetric;
	DoubleMetricHandle countLaunchTime;
	DoubleMetricHandle countReactTime;
	BoolMetricHandle awakeMetric;

	EventMetricHandle<SlowTask> slowTaskMetric;

	std::vector<std::string> blobCredentialFiles;
	std::vector<std::function<void()>> stopCallbacks;
};

static boost::asio::ip::address tcpAddress(IPAddress const& n) {
	if (n.isV6()) {
		return boost::asio::ip::address_v6(n.toV6());
	} else {
		return boost::asio::ip::address_v4(n.toV4());
	}
}

static IPAddress toIPAddress(boost::asio::ip::address const& addr) {
	if (addr.is_v4()) {
		return IPAddress(addr.to_v4().to_uint());
	} else {
		return IPAddress(addr.to_v6().to_bytes());
	}
}

static tcp::endpoint tcpEndpoint(NetworkAddress const& n) {
	return tcp::endpoint(tcpAddress(n.ip), n.port);
}

static udp::endpoint udpEndpoint(NetworkAddress const& n) {
	return udp::endpoint(tcpAddress(n.ip), n.port);
}

class BindPromise {
	Promise<Void> p;
	const char* errContext;
	UID errID;

public:
	BindPromise(const char* errContext, UID errID) : errContext(errContext), errID(errID) {}
	BindPromise(BindPromise const& r) : p(r.p), errContext(r.errContext), errID(r.errID) {}
	BindPromise(BindPromise&& r) noexcept : p(std::move(r.p)), errContext(r.errContext), errID(r.errID) {}

	Future<Void> getFuture() { return p.getFuture(); }

	void operator()(const boost::system::error_code& error, size_t bytesWritten = 0) {
		try {
			if (error) {
				// Log the error...
				{
					TraceEvent evt(SevWarn, errContext, errID);
					evt.suppressFor(1.0).detail("ErrorCode", error.value()).detail("Message", error.message());
#ifndef TLS_DISABLED
					// There is no function in OpenSSL to use to check if an error code is from OpenSSL,
					// but all OpenSSL errors have a non-zero "library" code set in bits 24-32, and linux
					// error codes should never go that high.
					if (error.value() >= (1 << 24L)) {
						evt.detail("WhichMeans", TLSPolicy::ErrorString(error));
					}
#endif
				}

				p.sendError(connection_failed());
			} else
				p.send(Void());
		} catch (Error& e) {
			p.sendError(e);
		} catch (...) {
			p.sendError(unknown_error());
		}
	}
};

class Connection final : public IConnection, ReferenceCounted<Connection> {
public:
	void addref() override { ReferenceCounted<Connection>::addref(); }
	void delref() override { ReferenceCounted<Connection>::delref(); }

	void close() override { closeSocket(); }

	explicit Connection(boost::asio::io_service& io_service)
	  : id(nondeterministicRandom()->randomUniqueID()), socket(io_service) {}

	// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
	ACTOR static Future<Reference<IConnection>> connect(boost::asio::io_service* ios, NetworkAddress addr) {
		state Reference<Connection> self(new Connection(*ios));

		self->peer_address = addr;
		try {
			auto to = tcpEndpoint(addr);
			BindPromise p("N2_ConnectError", self->id);
			Future<Void> onConnected = p.getFuture();
			self->socket.async_connect(to, std::move(p));

			wait(onConnected);
			self->init();
			return self;
		} catch (Error&) {
			// Either the connection failed, or was cancelled by the caller
			self->closeSocket();
			throw;
		}
	}

	// This is not part of the IConnection interface, because it is wrapped by IListener::accept()
	void accept(NetworkAddress peerAddr) {
		this->peer_address = peerAddr;
		init();
	}

	Future<Void> acceptHandshake() override { return Void(); }

	Future<Void> connectHandshake() override { return Void(); }

	// returns when write() can write at least one byte
	Future<Void> onWritable() override {
		++g_net2->countWriteProbes;
		BindPromise p("N2_WriteProbeError", id);
		auto f = p.getFuture();
		socket.async_write_some(boost::asio::null_buffers(), std::move(p));
		return f;
	}

	// returns when read() can read at least one byte
	Future<Void> onReadable() override {
		++g_net2->countReadProbes;
		BindPromise p("N2_ReadProbeError", id);
		auto f = p.getFuture();
		socket.async_read_some(boost::asio::null_buffers(), std::move(p));
		return f;
	}

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might
	// be 0)
	int read(uint8_t* begin, uint8_t* end) override {
		boost::system::error_code err;
		++g_net2->countReads;
		size_t toRead = end - begin;
		size_t size = socket.read_some(boost::asio::mutable_buffers_1(begin, toRead), err);
		g_net2->bytesReceived += size;
		//TraceEvent("ConnRead", this->id).detail("Bytes", size);
		if (err) {
			if (err == boost::asio::error::would_block) {
				++g_net2->countWouldBlock;
				return 0;
			}
			onReadError(err);
			throw connection_failed();
		}
		ASSERT(size); // If the socket is closed, we expect an 'eof' error, not a zero return value

		return size;
	}

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of
	// bytes written (might be 0)
	int write(SendBuffer const* data, int limit) override {
		boost::system::error_code err;
		++g_net2->countWrites;

		size_t sent = socket.write_some(
		    boost::iterator_range<SendBufferIterator>(SendBufferIterator(data, limit), SendBufferIterator()), err);

		if (err) {
			// Since there was an error, sent's value can't be used to infer that the buffer has data and the limit is
			// positive so check explicitly.
			ASSERT(limit > 0);
			bool notEmpty = false;
			for (auto p = data; p; p = p->next)
				if (p->bytes_written - p->bytes_sent > 0) {
					notEmpty = true;
					break;
				}
			ASSERT(notEmpty);

			if (err == boost::asio::error::would_block) {
				++g_net2->countWouldBlock;
				return 0;
			}
			onWriteError(err);
			throw connection_failed();
		}

		ASSERT(sent); // Make sure data was sent, and also this check will fail if the buffer chain was empty or the
		              // limit was not > 0.
		return sent;
	}

	NetworkAddress getPeerAddress() const override { return peer_address; }

	UID getDebugID() const override { return id; }

	tcp::socket& getSocket() { return socket; }

private:
	UID id;
	tcp::socket socket;
	NetworkAddress peer_address;

	void init() {
		// Socket settings that have to be set after connect or accept succeeds
		socket.non_blocking(true);
		if (FLOW_KNOBS->FLOW_TCP_NODELAY & 1) {
			socket.set_option(boost::asio::ip::tcp::no_delay(true));
		}
		if (FLOW_KNOBS->FLOW_TCP_QUICKACK & 1) {
#ifdef __linux__
			socket.set_option(boost::asio::detail::socket_option::boolean<IPPROTO_TCP, TCP_QUICKACK>(true));
#else
			TraceEvent(SevWarn, "N2_InitWarn").detail("Message", "TCP_QUICKACK not supported");
#endif
		}
		platform::setCloseOnExec(socket.native_handle());
	}

	void closeSocket() {
		boost::system::error_code error;
		socket.close(error);
		if (error)
			TraceEvent(SevWarn, "N2_CloseError", id)
			    .suppressFor(1.0)
			    .detail("ErrorCode", error.value())
			    .detail("Message", error.message());
	}

	void onReadError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_ReadError", id)
		    .suppressFor(1.0)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
	void onWriteError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_WriteError", id)
		    .suppressFor(1.0)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
};

class ReadPromise {
	Promise<int> p;
	const char* errContext;
	UID errID;
	std::shared_ptr<udp::endpoint> endpoint = nullptr;

public:
	ReadPromise(const char* errContext, UID errID) : errContext(errContext), errID(errID) {}
	ReadPromise(ReadPromise const& other) = default;
	ReadPromise(ReadPromise&& other) : p(std::move(other.p)), errContext(other.errContext), errID(other.errID) {}

	std::shared_ptr<udp::endpoint>& getEndpoint() { return endpoint; }

	Future<int> getFuture() { return p.getFuture(); }
	void operator()(const boost::system::error_code& error, size_t bytesWritten) {
		try {
			if (error) {
				TraceEvent evt(SevWarn, errContext, errID);
				evt.suppressFor(1.0).detail("ErrorCode", error.value()).detail("Message", error.message());
				p.sendError(connection_failed());
			} else {
				p.send(int(bytesWritten));
			}
		} catch (Error& e) {
			p.sendError(e);
		} catch (...) {
			p.sendError(unknown_error());
		}
	}
};

class UDPSocket : public IUDPSocket, ReferenceCounted<UDPSocket> {
	UID id;
	Optional<NetworkAddress> toAddress;
	udp::socket socket;
	bool isPublic = false;

public:
	ACTOR static Future<Reference<IUDPSocket>> connect(boost::asio::io_service* io_service,
	                                                   Optional<NetworkAddress> toAddress,
	                                                   bool isV6) {
		state Reference<UDPSocket> self(new UDPSocket(*io_service, toAddress, isV6));
		ASSERT(!toAddress.present() || toAddress.get().ip.isV6() == isV6);
		if (!toAddress.present()) {
			return self;
		}
		try {
			if (toAddress.present()) {
				auto to = udpEndpoint(toAddress.get());
				BindPromise p("N2_UDPConnectError", self->id);
				Future<Void> onConnected = p.getFuture();
				self->socket.async_connect(to, std::move(p));

				wait(onConnected);
			}
			self->init();
			return self;
		} catch (...) {
			self->closeSocket();
			throw;
		}
	}

	void close() override { closeSocket(); }

	Future<int> receive(uint8_t* begin, uint8_t* end) override {
		++g_net2->countUDPReads;
		ReadPromise p("N2_UDPReadError", id);
		auto res = p.getFuture();
		socket.async_receive(boost::asio::mutable_buffer(begin, end - begin), std::move(p));
		return fmap(
		    [](int bytes) {
			    g_net2->udpBytesReceived += bytes;
			    return bytes;
		    },
		    res);
	}

	Future<int> receiveFrom(uint8_t* begin, uint8_t* end, NetworkAddress* sender) override {
		++g_net2->countUDPReads;
		ReadPromise p("N2_UDPReadFromError", id);
		p.getEndpoint() = std::make_shared<udp::endpoint>();
		auto endpoint = p.getEndpoint().get();
		auto res = p.getFuture();
		socket.async_receive_from(boost::asio::mutable_buffer(begin, end - begin), *endpoint, std::move(p));
		return fmap(
		    [endpoint, sender](int bytes) {
			    if (sender) {
				    sender->port = endpoint->port();
				    sender->ip = toIPAddress(endpoint->address());
			    }
			    g_net2->udpBytesReceived += bytes;
			    return bytes;
		    },
		    res);
	}

	Future<int> send(uint8_t const* begin, uint8_t const* end) override {
		++g_net2->countUDPWrites;
		ReadPromise p("N2_UDPWriteError", id);
		auto res = p.getFuture();
		socket.async_send(boost::asio::const_buffer(begin, end - begin), std::move(p));
		return res;
	}

	Future<int> sendTo(uint8_t const* begin, uint8_t const* end, NetworkAddress const& peer) override {
		++g_net2->countUDPWrites;
		ReadPromise p("N2_UDPWriteError", id);
		auto res = p.getFuture();
		udp::endpoint toEndpoint = udpEndpoint(peer);
		socket.async_send_to(boost::asio::const_buffer(begin, end - begin), toEndpoint, std::move(p));
		return res;
	}

	void bind(NetworkAddress const& addr) override {
		boost::system::error_code ec;
		socket.bind(udpEndpoint(addr), ec);
		if (ec) {
			Error x;
			if (ec.value() == EADDRINUSE)
				x = address_in_use();
			else if (ec.value() == EADDRNOTAVAIL)
				x = invalid_local_address();
			else
				x = bind_failed();
			TraceEvent(SevWarnAlways, "Net2UDPBindError").error(x);
			throw x;
		}
		isPublic = true;
	}

	UID getDebugID() const override { return id; }

	void addref() override { ReferenceCounted<UDPSocket>::addref(); }
	void delref() override { ReferenceCounted<UDPSocket>::delref(); }

	NetworkAddress localAddress() const override {
		auto endpoint = socket.local_endpoint();
		return NetworkAddress(toIPAddress(endpoint.address()), endpoint.port(), isPublic, false);
	}

	boost::asio::ip::udp::socket::native_handle_type native_handle() override { return socket.native_handle(); }

private:
	UDPSocket(boost::asio::io_service& io_service, Optional<NetworkAddress> toAddress, bool isV6)
	  : id(nondeterministicRandom()->randomUniqueID()), socket(io_service, isV6 ? udp::v6() : udp::v4()) {}

	void closeSocket() {
		boost::system::error_code error;
		socket.close(error);
		if (error)
			TraceEvent(SevWarn, "N2_CloseError", id)
			    .suppressFor(1.0)
			    .detail("ErrorCode", error.value())
			    .detail("Message", error.message());
	}

	void onReadError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_UDPReadError", id)
		    .suppressFor(1.0)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
	void onWriteError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_UDPWriteError", id)
		    .suppressFor(1.0)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}

	void init() {
		socket.non_blocking(true);
		platform::setCloseOnExec(socket.native_handle());
	}
};

class Listener final : public IListener, ReferenceCounted<Listener> {
	boost::asio::io_context& io_service;
	NetworkAddress listenAddress;
	tcp::acceptor acceptor;

public:
	Listener(boost::asio::io_context& io_service, NetworkAddress listenAddress)
	  : io_service(io_service), listenAddress(listenAddress), acceptor(io_service, tcpEndpoint(listenAddress)) {
		platform::setCloseOnExec(acceptor.native_handle());
	}

	void addref() override { ReferenceCounted<Listener>::addref(); }
	void delref() override { ReferenceCounted<Listener>::delref(); }

	// Returns one incoming connection when it is available
	Future<Reference<IConnection>> accept() override { return doAccept(this); }

	NetworkAddress getListenAddress() const override { return listenAddress; }

private:
	ACTOR static Future<Reference<IConnection>> doAccept(Listener* self) {
		state Reference<Connection> conn(new Connection(self->io_service));
		state tcp::acceptor::endpoint_type peer_endpoint;
		try {
			BindPromise p("N2_AcceptError", UID());
			auto f = p.getFuture();
			self->acceptor.async_accept(conn->getSocket(), peer_endpoint, std::move(p));
			wait(f);
			auto peer_address = peer_endpoint.address().is_v6() ? IPAddress(peer_endpoint.address().to_v6().to_bytes())
			                                                    : IPAddress(peer_endpoint.address().to_v4().to_ulong());
			conn->accept(NetworkAddress(peer_address, peer_endpoint.port()));

			return conn;
		} catch (...) {
			conn->close();
			throw;
		}
	}
};

#ifndef TLS_DISABLED
typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket&> ssl_socket;

struct SSLHandshakerThread final : IThreadPoolReceiver {
	SSLHandshakerThread() {}
	void init() override {}

	struct Handshake final : TypedAction<SSLHandshakerThread, Handshake> {
		Handshake(ssl_socket& socket, ssl_socket::handshake_type type) : socket(socket), type(type) {}
		double getTimeEstimate() const override { return 0.001; }

		ThreadReturnPromise<Void> done;
		ssl_socket& socket;
		ssl_socket::handshake_type type;
		boost::system::error_code err;
	};

	void action(Handshake& h) {
		try {
			h.socket.next_layer().non_blocking(false, h.err);
			if (!h.err.failed()) {
				h.socket.handshake(h.type, h.err);
			}
			if (!h.err.failed()) {
				h.socket.next_layer().non_blocking(true, h.err);
			}
			if (h.err.failed()) {
				TraceEvent(SevWarn,
				           h.type == ssl_socket::handshake_type::client ? "N2_ConnectHandshakeError"
				                                                        : "N2_AcceptHandshakeError")
				    .detail("ErrorCode", h.err.value())
				    .detail("ErrorMsg", h.err.message().c_str())
				    .detail("BackgroundThread", true);
				h.done.sendError(connection_failed());
			} else {
				h.done.send(Void());
			}
		} catch (...) {
			TraceEvent(SevWarn,
			           h.type == ssl_socket::handshake_type::client ? "N2_ConnectHandshakeUnknownError"
			                                                        : "N2_AcceptHandshakeUnknownError")
			    .detail("BackgroundThread", true);
			h.done.sendError(connection_failed());
		}
	}
};

class SSLConnection final : public IConnection, ReferenceCounted<SSLConnection> {
public:
	void addref() override { ReferenceCounted<SSLConnection>::addref(); }
	void delref() override { ReferenceCounted<SSLConnection>::delref(); }

	void close() override { closeSocket(); }

	explicit SSLConnection(boost::asio::io_service& io_service,
	                       Reference<ReferencedObject<boost::asio::ssl::context>> context)
	  : id(nondeterministicRandom()->randomUniqueID()), socket(io_service), ssl_sock(socket, context->mutate()),
	    sslContext(context) {}

	// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
	ACTOR static Future<Reference<IConnection>> connect(boost::asio::io_service* ios,
	                                                    Reference<ReferencedObject<boost::asio::ssl::context>> context,
	                                                    NetworkAddress addr) {
		std::pair<IPAddress, uint16_t> peerIP = std::make_pair(addr.ip, addr.port);
		auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
		if (iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
			if (now() < iter->second.second) {
				if (iter->second.first >= FLOW_KNOBS->TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS) {
					TraceEvent("TLSOutgoingConnectionThrottlingWarning").suppressFor(1.0).detail("PeerIP", addr);
					wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT));
					throw connection_failed();
				}
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler.erase(peerIP);
			}
		}

		state Reference<SSLConnection> self(new SSLConnection(*ios, context));
		self->peer_address = addr;

		try {
			auto to = tcpEndpoint(self->peer_address);
			BindPromise p("N2_ConnectError", self->id);
			Future<Void> onConnected = p.getFuture();
			self->socket.async_connect(to, std::move(p));

			wait(onConnected);
			self->init();
			return self;
		} catch (Error& e) {
			// Either the connection failed, or was cancelled by the caller
			self->closeSocket();
			throw;
		}
	}

	// This is not part of the IConnection interface, because it is wrapped by IListener::accept()
	void accept(NetworkAddress peerAddr) {
		this->peer_address = peerAddr;
		init();
	}

	ACTOR static void doAcceptHandshake(Reference<SSLConnection> self, Promise<Void> connected) {
		state Hold<int> holder;

		try {
			Future<Void> onHandshook;

			// If the background handshakers are not all busy, use one
			if (N2::g_net2->sslPoolHandshakesInProgress < N2::g_net2->sslHandshakerThreadsStarted) {
				holder = Hold(&N2::g_net2->sslPoolHandshakesInProgress);
				auto handshake =
				    new SSLHandshakerThread::Handshake(self->ssl_sock, boost::asio::ssl::stream_base::server);
				onHandshook = handshake->done.getFuture();
				N2::g_net2->sslHandshakerPool->post(handshake);
			} else {
				// Otherwise use flow network thread
				BindPromise p("N2_AcceptHandshakeError", UID());
				onHandshook = p.getFuture();
				self->ssl_sock.async_handshake(boost::asio::ssl::stream_base::server, std::move(p));
			}
			wait(onHandshook);
			wait(delay(0, TaskPriority::Handshake));
			connected.send(Void());
		} catch (...) {
			self->closeSocket();
			connected.sendError(connection_failed());
		}
	}

	ACTOR static Future<Void> acceptHandshakeWrapper(Reference<SSLConnection> self) {
		state std::pair<IPAddress, uint16_t> peerIP;
		peerIP = std::make_pair(self->getPeerAddress().ip, static_cast<uint16_t>(0));
		auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
		if (iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
			if (now() < iter->second.second) {
				if (iter->second.first >= FLOW_KNOBS->TLS_SERVER_CONNECTION_THROTTLE_ATTEMPTS) {
					TraceEvent("TLSIncomingConnectionThrottlingWarning")
					    .suppressFor(1.0)
					    .detail("PeerIP", peerIP.first.toString());
					wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT));
					self->closeSocket();
					throw connection_failed();
				}
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler.erase(peerIP);
			}
		}

		wait(g_network->networkInfo.handshakeLock->take());
		state FlowLock::Releaser releaser(*g_network->networkInfo.handshakeLock);

		Promise<Void> connected;
		doAcceptHandshake(self, connected);
		try {
			choose {
				when(wait(connected.getFuture())) { return Void(); }
				when(wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT))) { throw connection_failed(); }
			}
		} catch (Error& e) {
			if (e.code() != error_code_actor_cancelled) {
				auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
				if (iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
					iter->second.first++;
				} else {
					g_network->networkInfo.serverTLSConnectionThrottler[peerIP] =
					    std::make_pair(0, now() + FLOW_KNOBS->TLS_SERVER_CONNECTION_THROTTLE_TIMEOUT);
				}
			}
			// Either the connection failed, or was cancelled by the caller
			self->closeSocket();
			throw;
		}
	}

	Future<Void> acceptHandshake() override { return acceptHandshakeWrapper(Reference<SSLConnection>::addRef(this)); }

	ACTOR static void doConnectHandshake(Reference<SSLConnection> self, Promise<Void> connected) {
		state Hold<int> holder;

		try {
			Future<Void> onHandshook;
			// If the background handshakers are not all busy, use one
			if (N2::g_net2->sslPoolHandshakesInProgress < N2::g_net2->sslHandshakerThreadsStarted) {
				holder = Hold(&N2::g_net2->sslPoolHandshakesInProgress);
				auto handshake =
				    new SSLHandshakerThread::Handshake(self->ssl_sock, boost::asio::ssl::stream_base::client);
				onHandshook = handshake->done.getFuture();
				N2::g_net2->sslHandshakerPool->post(handshake);
			} else {
				// Otherwise use flow network thread
				BindPromise p("N2_ConnectHandshakeError", self->id);
				onHandshook = p.getFuture();
				self->ssl_sock.async_handshake(boost::asio::ssl::stream_base::client, std::move(p));
			}
			wait(onHandshook);
			wait(delay(0, TaskPriority::Handshake));
			connected.send(Void());
		} catch (...) {
			self->closeSocket();
			connected.sendError(connection_failed());
		}
	}

	ACTOR static Future<Void> connectHandshakeWrapper(Reference<SSLConnection> self) {
		wait(g_network->networkInfo.handshakeLock->take());
		state FlowLock::Releaser releaser(*g_network->networkInfo.handshakeLock);

		Promise<Void> connected;
		doConnectHandshake(self, connected);
		try {
			choose {
				when(wait(connected.getFuture())) { return Void(); }
				when(wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT))) { throw connection_failed(); }
			}
		} catch (Error& e) {
			// Either the connection failed, or was cancelled by the caller
			if (e.code() != error_code_actor_cancelled) {
				std::pair<IPAddress, uint16_t> peerIP = std::make_pair(self->peer_address.ip, self->peer_address.port);
				auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
				if (iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
					iter->second.first++;
				} else {
					g_network->networkInfo.serverTLSConnectionThrottler[peerIP] =
					    std::make_pair(0, now() + FLOW_KNOBS->TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT);
				}
			}
			self->closeSocket();
			throw;
		}
	}

	Future<Void> connectHandshake() override { return connectHandshakeWrapper(Reference<SSLConnection>::addRef(this)); }

	// returns when write() can write at least one byte
	Future<Void> onWritable() override {
		++g_net2->countWriteProbes;
		BindPromise p("N2_WriteProbeError", id);
		auto f = p.getFuture();
		socket.async_write_some(boost::asio::null_buffers(), std::move(p));
		return f;
	}

	// returns when read() can read at least one byte
	Future<Void> onReadable() override {
		++g_net2->countReadProbes;
		BindPromise p("N2_ReadProbeError", id);
		auto f = p.getFuture();
		socket.async_read_some(boost::asio::null_buffers(), std::move(p));
		return f;
	}

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might
	// be 0)
	int read(uint8_t* begin, uint8_t* end) override {
		boost::system::error_code err;
		++g_net2->countReads;
		size_t toRead = end - begin;
		size_t size = ssl_sock.read_some(boost::asio::mutable_buffers_1(begin, toRead), err);
		g_net2->bytesReceived += size;
		//TraceEvent("ConnRead", this->id).detail("Bytes", size);
		if (err) {
			if (err == boost::asio::error::would_block) {
				++g_net2->countWouldBlock;
				return 0;
			}
			onReadError(err);
			throw connection_failed();
		}
		ASSERT(size); // If the socket is closed, we expect an 'eof' error, not a zero return value

		return size;
	}

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of
	// bytes written (might be 0)
	int write(SendBuffer const* data, int limit) override {
#ifdef __APPLE__
		// For some reason, writing ssl_sock with more than 2016 bytes when socket is writeable sometimes results in a
		// broken pipe error.
		limit = std::min(limit, 2016);
#endif
		boost::system::error_code err;
		++g_net2->countWrites;

		size_t sent = ssl_sock.write_some(
		    boost::iterator_range<SendBufferIterator>(SendBufferIterator(data, limit), SendBufferIterator()), err);

		if (err) {
			// Since there was an error, sent's value can't be used to infer that the buffer has data and the limit is
			// positive so check explicitly.
			ASSERT(limit > 0);
			bool notEmpty = false;
			for (auto p = data; p; p = p->next)
				if (p->bytes_written - p->bytes_sent > 0) {
					notEmpty = true;
					break;
				}
			ASSERT(notEmpty);

			if (err == boost::asio::error::would_block) {
				++g_net2->countWouldBlock;
				return 0;
			}
			onWriteError(err);
			throw connection_failed();
		}

		ASSERT(sent); // Make sure data was sent, and also this check will fail if the buffer chain was empty or the
		              // limit was not > 0.
		return sent;
	}

	NetworkAddress getPeerAddress() const override { return peer_address; }

	UID getDebugID() const override { return id; }

	tcp::socket& getSocket() { return socket; }

	ssl_socket& getSSLSocket() { return ssl_sock; }

private:
	UID id;
	tcp::socket socket;
	ssl_socket ssl_sock;
	NetworkAddress peer_address;
	Reference<ReferencedObject<boost::asio::ssl::context>> sslContext;

	void init() {
		// Socket settings that have to be set after connect or accept succeeds
		socket.non_blocking(true);
		socket.set_option(boost::asio::ip::tcp::no_delay(true));
		platform::setCloseOnExec(socket.native_handle());
	}

	void closeSocket() {
		boost::system::error_code cancelError;
		socket.cancel(cancelError);
		boost::system::error_code closeError;
		socket.close(closeError);
		boost::system::error_code shutdownError;
		ssl_sock.shutdown(shutdownError);
	}

	void onReadError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_ReadError", id)
		    .suppressFor(1.0)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
	void onWriteError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_WriteError", id)
		    .suppressFor(1.0)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
};

class SSLListener final : public IListener, ReferenceCounted<SSLListener> {
	boost::asio::io_context& io_service;
	NetworkAddress listenAddress;
	tcp::acceptor acceptor;
	AsyncVar<Reference<ReferencedObject<boost::asio::ssl::context>>>* contextVar;

public:
	SSLListener(boost::asio::io_context& io_service,
	            AsyncVar<Reference<ReferencedObject<boost::asio::ssl::context>>>* contextVar,
	            NetworkAddress listenAddress)
	  : io_service(io_service), listenAddress(listenAddress), acceptor(io_service, tcpEndpoint(listenAddress)),
	    contextVar(contextVar) {
		platform::setCloseOnExec(acceptor.native_handle());
	}

	void addref() override { ReferenceCounted<SSLListener>::addref(); }
	void delref() override { ReferenceCounted<SSLListener>::delref(); }

	// Returns one incoming connection when it is available
	Future<Reference<IConnection>> accept() override { return doAccept(this); }

	NetworkAddress getListenAddress() const override { return listenAddress; }

private:
	ACTOR static Future<Reference<IConnection>> doAccept(SSLListener* self) {
		state Reference<SSLConnection> conn(new SSLConnection(self->io_service, self->contextVar->get()));
		state tcp::acceptor::endpoint_type peer_endpoint;
		try {
			BindPromise p("N2_AcceptError", UID());
			auto f = p.getFuture();
			self->acceptor.async_accept(conn->getSocket(), peer_endpoint, std::move(p));
			wait(f);
			auto peer_address = peer_endpoint.address().is_v6() ? IPAddress(peer_endpoint.address().to_v6().to_bytes())
			                                                    : IPAddress(peer_endpoint.address().to_v4().to_ulong());

			conn->accept(NetworkAddress(peer_address, peer_endpoint.port(), false, true));

			return conn;
		} catch (...) {
			conn->close();
			throw;
		}
	}
};
#endif

struct PromiseTask final : public Task, public FastAllocated<PromiseTask> {
	Promise<Void> promise;
	PromiseTask() {}
	explicit PromiseTask(Promise<Void>&& promise) noexcept : promise(std::move(promise)) {}

	void operator()() override {
		promise.send(Void());
		delete this;
	}
};

// 5MB for loading files into memory

Net2::Net2(const TLSConfig& tlsConfig, bool useThreadPool, bool useMetrics)
  : globals(enumGlobal::COUNT), useThreadPool(useThreadPool), reactor(this),
#ifndef TLS_DISABLED
    sslContextVar({ ReferencedObject<boost::asio::ssl::context>::from(
        boost::asio::ssl::context(boost::asio::ssl::context::tls)) }),
    sslHandshakerThreadsStarted(0), sslPoolHandshakesInProgress(0),
#endif
    tlsConfig(tlsConfig), tlsInitializedState(ETLSInitState::NONE), network(this), tscBegin(0), tscEnd(0), taskBegin(0),
    currentTaskID(TaskPriority::DefaultYield), tasksIssued(0), stopped(false), started(false), numYields(0),
    lastPriorityStats(nullptr), ready(FLOW_KNOBS->READY_QUEUE_RESERVED_SIZE) {
	// Until run() is called, yield() will always yield
	TraceEvent("Net2Starting").log();

	// Set the global members
	if (useMetrics) {
		setGlobal(INetwork::enTDMetrics, (flowGlobalType)&tdmetrics);
	}
	if (FLOW_KNOBS->ENABLE_CHAOS_FEATURES) {
		setGlobal(INetwork::enChaosMetrics, (flowGlobalType)&chaosMetrics);
	}
	setGlobal(INetwork::enNetworkConnections, (flowGlobalType)network);
	setGlobal(INetwork::enASIOService, (flowGlobalType)&reactor.ios);
	setGlobal(INetwork::enBlobCredentialFiles, &blobCredentialFiles);

#ifdef __linux__
	setGlobal(INetwork::enEventFD, (flowGlobalType)N2::ASIOReactor::newEventFD(reactor));
#endif

	updateNow();
}

#ifndef TLS_DISABLED
ACTOR static Future<Void> watchFileForChanges(std::string filename, AsyncTrigger* fileChanged) {
	if (filename == "") {
		return Never();
	}
	state bool firstRun = true;
	state bool statError = false;
	state std::time_t lastModTime = 0;
	loop {
		try {
			std::time_t modtime = wait(IAsyncFileSystem::filesystem()->lastWriteTime(filename));
			if (firstRun) {
				lastModTime = modtime;
				firstRun = false;
			}
			if (lastModTime != modtime || statError) {
				lastModTime = modtime;
				statError = false;
				fileChanged->trigger();
			}
		} catch (Error& e) {
			if (e.code() == error_code_io_error) {
				// EACCES, ELOOP, ENOENT all come out as io_error(), but are more of a system
				// configuration issue than an FDB problem.  If we managed to load valid
				// certificates, then there's no point in crashing, but we should complain
				// loudly.  IAsyncFile will log the error, but not necessarily as a warning.
				TraceEvent(SevWarnAlways, "TLSCertificateRefreshStatError").detail("File", filename);
				statError = true;
			} else {
				throw;
			}
		}
		wait(delay(FLOW_KNOBS->TLS_CERT_REFRESH_DELAY_SECONDS));
	}
}

ACTOR static Future<Void> reloadCertificatesOnChange(
    TLSConfig config,
    std::function<void()> onPolicyFailure,
    AsyncVar<Reference<ReferencedObject<boost::asio::ssl::context>>>* contextVar) {
	if (FLOW_KNOBS->TLS_CERT_REFRESH_DELAY_SECONDS <= 0) {
		return Void();
	}
	loop {
		// Early in bootup, the filesystem might not be initialized yet.  Wait until it is.
		if (IAsyncFileSystem::filesystem() != nullptr) {
			break;
		}
		wait(delay(1.0));
	}
	state int mismatches = 0;
	state AsyncTrigger fileChanged;
	state std::vector<Future<Void>> lifetimes;
	lifetimes.push_back(watchFileForChanges(config.getCertificatePathSync(), &fileChanged));
	lifetimes.push_back(watchFileForChanges(config.getKeyPathSync(), &fileChanged));
	lifetimes.push_back(watchFileForChanges(config.getCAPathSync(), &fileChanged));
	loop {
		wait(fileChanged.onTrigger());
		TraceEvent("TLSCertificateRefreshBegin").log();

		try {
			LoadedTLSConfig loaded = wait(config.loadAsync());
			boost::asio::ssl::context context(boost::asio::ssl::context::tls);
			ConfigureSSLContext(loaded, &context, onPolicyFailure);
			TraceEvent(SevInfo, "TLSCertificateRefreshSucceeded").log();
			mismatches = 0;
			contextVar->set(ReferencedObject<boost::asio::ssl::context>::from(std::move(context)));
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) {
				throw;
			}
			// Some files didn't match up, they should in the future, and we'll retry then.
			mismatches++;
			TraceEvent(SevWarn, "TLSCertificateRefreshMismatch").error(e).detail("mismatches", mismatches);
		}
	}
}
#endif

void Net2::initTLS(ETLSInitState targetState) {
	if (tlsInitializedState >= targetState) {
		return;
	}
#ifndef TLS_DISABLED
	// Any target state must be higher than NONE so if the current state is NONE
	// then initialize the TLS config
	if (tlsInitializedState == ETLSInitState::NONE) {
		auto onPolicyFailure = [this]() { this->countTLSPolicyFailures++; };
		try {
			boost::asio::ssl::context newContext(boost::asio::ssl::context::tls);
			const LoadedTLSConfig& loaded = tlsConfig.loadSync();
			TraceEvent("Net2TLSConfig")
			    .detail("CAPath", tlsConfig.getCAPathSync())
			    .detail("CertificatePath", tlsConfig.getCertificatePathSync())
			    .detail("KeyPath", tlsConfig.getKeyPathSync())
			    .detail("HasPassword", !loaded.getPassword().empty())
			    .detail("VerifyPeers", boost::algorithm::join(loaded.getVerifyPeers(), "|"));
			ConfigureSSLContext(tlsConfig.loadSync(), &newContext, onPolicyFailure);
			sslContextVar.set(ReferencedObject<boost::asio::ssl::context>::from(std::move(newContext)));
		} catch (Error& e) {
			TraceEvent("Net2TLSInitError").error(e);
		}
		backgroundCertRefresh = reloadCertificatesOnChange(tlsConfig, onPolicyFailure, &sslContextVar);
	}

	// If a TLS connection is actually going to be used then start background threads if configured
	if (targetState > ETLSInitState::CONFIG) {
		int threadsToStart;
		switch (targetState) {
		case ETLSInitState::CONNECT:
			threadsToStart = FLOW_KNOBS->TLS_CLIENT_HANDSHAKE_THREADS;
			break;
		case ETLSInitState::LISTEN:
			threadsToStart = FLOW_KNOBS->TLS_SERVER_HANDSHAKE_THREADS;
			break;
		default:
			threadsToStart = 0;
		};
		threadsToStart -= sslHandshakerThreadsStarted;

		if (threadsToStart > 0) {
			if (sslHandshakerThreadsStarted == 0) {
#if defined(__linux__)
				if (mallopt(M_ARENA_MAX, FLOW_KNOBS->TLS_MALLOC_ARENA_MAX) != 1) {
					TraceEvent(SevWarn, "TLSMallocSetMaxArenasFailure")
					    .detail("MaxArenas", FLOW_KNOBS->TLS_MALLOC_ARENA_MAX);
				};
#endif
				sslHandshakerPool = createGenericThreadPool(FLOW_KNOBS->TLS_HANDSHAKE_THREAD_STACKSIZE);
			}

			for (int i = 0; i < threadsToStart; ++i) {
				++sslHandshakerThreadsStarted;
				sslHandshakerPool->addThread(new SSLHandshakerThread(), "fdb-ssl-connect");
			}
		}
	}
#endif

	tlsInitializedState = targetState;
}

ACTOR Future<Void> Net2::logTimeOffset() {
	loop {
		double processTime = timer_monotonic();
		double systemTime = timer();
		TraceEvent("ProcessTimeOffset")
		    .detailf("ProcessTime", "%lf", processTime)
		    .detailf("SystemTime", "%lf", systemTime)
		    .detailf("OffsetFromSystemTime", "%lf", processTime - systemTime);
		wait(::delay(FLOW_KNOBS->TIME_OFFSET_LOGGING_INTERVAL));
	}
}

void Net2::initMetrics() {
	bytesReceived.init(LiteralStringRef("Net2.BytesReceived"));
	countWriteProbes.init(LiteralStringRef("Net2.CountWriteProbes"));
	countReadProbes.init(LiteralStringRef("Net2.CountReadProbes"));
	countReads.init(LiteralStringRef("Net2.CountReads"));
	countWouldBlock.init(LiteralStringRef("Net2.CountWouldBlock"));
	countWrites.init(LiteralStringRef("Net2.CountWrites"));
	countRunLoop.init(LiteralStringRef("Net2.CountRunLoop"));
	countCantSleep.init(LiteralStringRef("Net2.CountCantSleep"));
	countWontSleep.init(LiteralStringRef("Net2.CountWontSleep"));
	countTimers.init(LiteralStringRef("Net2.CountTimers"));
	countTasks.init(LiteralStringRef("Net2.CountTasks"));
	countYields.init(LiteralStringRef("Net2.CountYields"));
	countYieldBigStack.init(LiteralStringRef("Net2.CountYieldBigStack"));
	countYieldCalls.init(LiteralStringRef("Net2.CountYieldCalls"));
	countASIOEvents.init(LiteralStringRef("Net2.CountASIOEvents"));
	countYieldCallsTrue.init(LiteralStringRef("Net2.CountYieldCallsTrue"));
	countRunLoopProfilingSignals.init(LiteralStringRef("Net2.CountRunLoopProfilingSignals"));
	countTLSPolicyFailures.init(LiteralStringRef("Net2.CountTLSPolicyFailures"));
	priorityMetric.init(LiteralStringRef("Net2.Priority"));
	awakeMetric.init(LiteralStringRef("Net2.Awake"));
	slowTaskMetric.init(LiteralStringRef("Net2.SlowTask"));
	countLaunchTime.init(LiteralStringRef("Net2.CountLaunchTime"));
	countReactTime.init(LiteralStringRef("Net2.CountReactTime"));
}

bool Net2::checkRunnable() {
	return !started.exchange(true);
}

#ifdef ENABLE_SAMPLING
ActorLineageSet& Net2::getActorLineageSet() {
	return actorLineageSet;
}
#endif

void Net2::run() {
	TraceEvent::setNetworkThread();
	TraceEvent("Net2Running").log();

	thread_network = this;

#ifdef WIN32
	if (timeBeginPeriod(1) != TIMERR_NOERROR)
		TraceEvent(SevError, "TimeBeginPeriodError").log();
#endif

	timeOffsetLogger = logTimeOffset();
	const char* flow_profiler_enabled = getenv("FLOW_PROFILER_ENABLED");
	if (flow_profiler_enabled != nullptr && *flow_profiler_enabled != '\0') {
		// The empty string check is to allow running `FLOW_PROFILER_ENABLED= ./fdbserver` to force disabling flow
		// profiling at startup.
		startProfiling(this);
	}

	// Get the address to the launch function
	typedef void (*runCycleFuncPtr)();
	runCycleFuncPtr runFunc = reinterpret_cast<runCycleFuncPtr>(
	    reinterpret_cast<flowGlobalType>(g_network->global(INetwork::enRunCycleFunc)));

	started.store(true);
	double nnow = timer_monotonic();

	while (!stopped) {
		FDB_TRACE_PROBE(run_loop_begin);
		++countRunLoop;

		if (runFunc) {
			tscBegin = timestampCounter();
			taskBegin = nnow;
			trackAtPriority(TaskPriority::RunCycleFunction, taskBegin);
			runFunc();
			double taskEnd = timer_monotonic();
			trackAtPriority(TaskPriority::RunLoop, taskEnd);
			countLaunchTime += taskEnd - taskBegin;
			checkForSlowTask(tscBegin, timestampCounter(), taskEnd - taskBegin, TaskPriority::RunCycleFunction);
		}

		double sleepTime = 0;
		bool b = ready.empty();
		if (b) {
			b = threadReady.canSleep();
			if (!b)
				++countCantSleep;
		} else
			++countWontSleep;
		if (b) {
			sleepTime = 1e99;
			double sleepStart = timer_monotonic();
			if (!timers.empty()) {
				sleepTime = timers.top().at - sleepStart; // + 500e-6?
			}
			if (sleepTime > 0) {
#if defined(__linux__)
				// notify the run loop monitoring thread that we have gone idle
				net2RunLoopSleeps.fetch_add(1);
#endif

				trackAtPriority(TaskPriority::Zero, sleepStart);
				awakeMetric = false;
				priorityMetric = 0;
				reactor.sleep(sleepTime);
				awakeMetric = true;
			}
		}

		tscBegin = timestampCounter();
		taskBegin = timer_monotonic();
		trackAtPriority(TaskPriority::ASIOReactor, taskBegin);
		reactor.react();

		updateNow();
		double now = this->currentTime;
		trackAtPriority(TaskPriority::RunLoop, now);

		countReactTime += now - taskBegin;
		checkForSlowTask(tscBegin, timestampCounter(), now - taskBegin, TaskPriority::ASIOReactor);

		if ((now - nnow) > FLOW_KNOBS->SLOW_LOOP_CUTOFF &&
		    nondeterministicRandom()->random01() < (now - nnow) * FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopTop").detail("Elapsed", now - nnow);

		int numTimers = 0;
		while (!timers.empty() && timers.top().at < now) {
			++numTimers;
			++countTimers;
			ready.push(timers.top());
			timers.pop();
		}
		// FIXME: Is this double counting?
		countTimers += numTimers;
		FDB_TRACE_PROBE(run_loop_ready_timers, numTimers);

		processThreadReady();

		tscBegin = timestampCounter();
		tscEnd = tscBegin + FLOW_KNOBS->TSC_YIELD_TIME;
		taskBegin = timer_monotonic();
		numYields = 0;
		TaskPriority minTaskID = TaskPriority::Max;
		[[maybe_unused]] int queueSize = ready.size();

		FDB_TRACE_PROBE(run_loop_tasks_start, queueSize);
		while (!ready.empty()) {
			++countTasks;
			currentTaskID = ready.top().taskID;
			priorityMetric = static_cast<int64_t>(currentTaskID);
			Task* task = ready.top().task;
			ready.pop();

			try {
				(*task)();
			} catch (Error& e) {
				TraceEvent(SevError, "TaskError").error(e);
			} catch (...) {
				TraceEvent(SevError, "TaskError").error(unknown_error());
			}

			if (currentTaskID < minTaskID) {
				trackAtPriority(currentTaskID, taskBegin);
				minTaskID = currentTaskID;
			}

			// attempt to empty out the IO backlog
			if (ready.size() % FLOW_KNOBS->ITERATIONS_PER_REACTOR_CHECK == 1) {
				if (runFunc) {
					runFunc();
				}
				reactor.react();
			}

			double tscNow = timestampCounter();
			double newTaskBegin = timer_monotonic();
			if (check_yield(TaskPriority::Max, tscNow)) {
				checkForSlowTask(tscBegin, tscNow, newTaskBegin - taskBegin, currentTaskID);
				taskBegin = newTaskBegin;
				FDB_TRACE_PROBE(run_loop_yield);
				++countYields;
				break;
			}

			taskBegin = newTaskBegin;
			tscBegin = tscNow;
		}

		trackAtPriority(TaskPriority::RunLoop, taskBegin);

		queueSize = ready.size();
		FDB_TRACE_PROBE(run_loop_done, queueSize);

#if defined(__linux__)
		if (FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL > 0) {
			sigset_t orig_set;
			pthread_sigmask(SIG_BLOCK, &sigprof_set, &orig_set);

			size_t other_offset = net2backtraces_offset;
			bool was_overflow = net2backtraces_overflow;
			int signal_count = net2backtraces_count;

			countRunLoopProfilingSignals += signal_count;

			if (other_offset) {
				volatile void** _traces = net2backtraces;
				net2backtraces = other_backtraces;
				other_backtraces = _traces;

				net2backtraces_offset = 0;
			}

			net2backtraces_overflow = false;
			net2backtraces_count = 0;

			pthread_sigmask(SIG_SETMASK, &orig_set, nullptr);

			if (was_overflow) {
				TraceEvent("Net2RunLoopProfilerOverflow")
				    .detail("SignalsReceived", signal_count)
				    .detail("BackTraceHarvested", other_offset != 0);
			}
			if (other_offset) {
				size_t iter_offset = 0;
				while (iter_offset < other_offset) {
					ProfilingSample* ps = (ProfilingSample*)(other_backtraces + iter_offset);
					TraceEvent(SevWarn, "Net2RunLoopTrace")
					    .detailf("TraceTime", "%.6f", ps->timestamp)
					    .detail("Trace", platform::format_backtrace(ps->frames, ps->length));
					iter_offset += ps->length + 2;
				}
			}

			// notify the run loop monitoring thread that we are making progress
			net2RunLoopIterations.fetch_add(1);
		}
#endif
		nnow = timer_monotonic();

		if ((nnow - now) > FLOW_KNOBS->SLOW_LOOP_CUTOFF &&
		    nondeterministicRandom()->random01() < (nnow - now) * FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopBottom")
			    .detail("Elapsed", nnow - now); // This includes the time spent running tasks
	}

	for (auto& fn : stopCallbacks) {
		fn();
	}

#ifdef WIN32
	timeEndPeriod(1);
#endif
} // Net2::run

// Updates the PriorityStats found in NetworkMetrics
void Net2::updateStarvationTracker(struct NetworkMetrics::PriorityStats& binStats,
                                   TaskPriority priority,
                                   TaskPriority lastPriority,
                                   double now) {

	// Busy -> idle at binStats.priority
	if (binStats.priority > priority && binStats.priority <= lastPriority) {
		binStats.active = false;
		binStats.duration += now - binStats.windowedTimer;
		binStats.maxDuration = std::max(binStats.maxDuration, now - binStats.timer);
	}

	// Idle -> busy at binStats.priority
	else if (binStats.priority <= priority && binStats.priority > lastPriority) {
		binStats.active = true;
		binStats.timer = now;
		binStats.windowedTimer = now;
	}
}

// Update both vectors of starvation trackers (one that updates every 5s and the other every 1s)
void Net2::trackAtPriority(TaskPriority priority, double now) {
	if (lastPriorityStats == nullptr || priority != lastPriorityStats->priority) {
		// Start tracking current priority
		auto activeStatsItr = networkInfo.metrics.activeTrackers.try_emplace(priority, priority);
		activeStatsItr.first->second.active = true;
		activeStatsItr.first->second.windowedTimer = now;

		if (lastPriorityStats != nullptr) {
			// Stop tracking previous priority
			lastPriorityStats->active = false;
			lastPriorityStats->duration += now - lastPriorityStats->windowedTimer;
		}

		// Update starvation trackers
		TaskPriority lastPriority = (lastPriorityStats == nullptr) ? TaskPriority::Zero : lastPriorityStats->priority;
		for (auto& binStats : networkInfo.metrics.starvationTrackers) {
			if (binStats.priority > lastPriority && binStats.priority > priority) {
				break;
			}
			updateStarvationTracker(binStats, priority, lastPriority, now);
		}

		// Update starvation trackers for network busyness
		updateStarvationTracker(networkInfo.metrics.starvationTrackerNetworkBusyness, priority, lastPriority, now);

		lastPriorityStats = &activeStatsItr.first->second;
	}
}

void Net2::processThreadReady() {
	int numReady = 0;
	while (true) {
		Optional<OrderedTask> t = threadReady.pop();
		if (!t.present())
			break;
		t.get().priority -= ++tasksIssued;
		ASSERT(t.get().task != 0);
		ready.push(t.get());
		++numReady;
	}
	FDB_TRACE_PROBE(run_loop_thread_ready, numReady);
}

void Net2::checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, TaskPriority priority) {
	int64_t elapsed = tscEnd - tscBegin;
	if (elapsed > FLOW_KNOBS->TSC_YIELD_TIME && tscBegin > 0) {
		int i = std::min<double>(NetworkMetrics::SLOW_EVENT_BINS - 1, log(elapsed / 1e6) / log(2.));
		++networkInfo.metrics.countSlowEvents[i];
		int64_t warnThreshold = g_network->isSimulated() ? 10e9 : 500e6;

		// printf("SlowTask: %d, %d yields\n", (int)(elapsed/1e6), numYields);

		slowTaskMetric->clocks = elapsed;
		slowTaskMetric->duration = (int64_t)(duration * 1e9);
		slowTaskMetric->priority = static_cast<int64_t>(priority);
		slowTaskMetric->numYields = numYields;
		slowTaskMetric->log();

		double sampleRate = std::min(1.0, (elapsed > warnThreshold) ? 1.0 : elapsed / 10e9);
		double slowTaskProfilingLogInterval =
		    std::max(FLOW_KNOBS->RUN_LOOP_PROFILING_INTERVAL, FLOW_KNOBS->SLOWTASK_PROFILING_LOG_INTERVAL);
		if (slowTaskProfilingLogInterval > 0 && duration > slowTaskProfilingLogInterval) {
			sampleRate = 1; // Always include slow task events that could show up in our slow task profiling.
		}

		if (!DEBUG_DETERMINISM && (nondeterministicRandom()->random01() < sampleRate))
			TraceEvent(elapsed > warnThreshold ? SevWarnAlways : SevInfo, "SlowTask")
			    .detail("TaskID", priority)
			    .detail("MClocks", elapsed / 1e6)
			    .detail("Duration", duration)
			    .detail("SampleRate", sampleRate)
			    .detail("NumYields", numYields);
	}
}

bool Net2::check_yield(TaskPriority taskID, int64_t tscNow) {
	// SOMEDAY: Yield if there are lots of higher priority tasks queued?
	if ((g_stackYieldLimit) && ((intptr_t)&taskID < g_stackYieldLimit)) {
		++countYieldBigStack;
		return true;
	}

	processThreadReady();

	if (taskID == TaskPriority::DefaultYield)
		taskID = currentTaskID;
	if (!ready.empty() && ready.top().priority > int64_t(taskID) << 32) {
		return true;
	}

	if (tscNow < tscBegin) {
		return true;
	}

	if (tscNow > tscEnd) {
		++numYields;
		return true;
	}

	return false;
}

bool Net2::check_yield(TaskPriority taskID) {
	if (numYields > 0) {
		++numYields;
		return true;
	}

	return check_yield(taskID, timestampCounter());
}

Future<class Void> Net2::yield(TaskPriority taskID) {
	++countYieldCalls;
	if (taskID == TaskPriority::DefaultYield)
		taskID = currentTaskID;
	if (check_yield(taskID)) {
		++countYieldCallsTrue;
		return delay(0, taskID);
	}
	g_network->setCurrentTask(taskID);
	return Void();
}

Future<Void> Net2::delay(double seconds, TaskPriority taskId) {
	if (seconds <= 0.) {
		PromiseTask* t = new PromiseTask;
		this->ready.push(OrderedTask((int64_t(taskId) << 32) - (++tasksIssued), taskId, t));
		return t->promise.getFuture();
	}
	if (seconds >=
	    4e12) // Intervals that overflow an int64_t in microseconds (more than 100,000 years) are treated as infinite
		return Never();

	double at = now() + seconds;
	PromiseTask* t = new PromiseTask;
	this->timers.push(DelayedTask(at, (int64_t(taskId) << 32) - (++tasksIssued), taskId, t));
	return t->promise.getFuture();
}

Future<Void> Net2::orderedDelay(double seconds, TaskPriority taskId) {
	// The regular delay already provides the required ordering property
	return delay(seconds, taskId);
}

void Net2::onMainThread(Promise<Void>&& signal, TaskPriority taskID) {
	if (stopped)
		return;
	PromiseTask* p = new PromiseTask(std::move(signal));
	int64_t priority = int64_t(taskID) << 32;

	if (thread_network == this) {
		processThreadReady();
		this->ready.push(OrderedTask(priority - (++tasksIssued), taskID, p));
	} else {
		if (threadReady.push(OrderedTask(priority, taskID, p)))
			reactor.wake();
	}
}

THREAD_HANDLE Net2::startThread(THREAD_FUNC_RETURN (*func)(void*), void* arg, int stackSize, const char* name) {
	return ::startThread(func, arg, stackSize, name);
}

Future<Reference<IConnection>> Net2::connect(NetworkAddress toAddr, const std::string& host) {
#ifndef TLS_DISABLED
	if (toAddr.isTLS()) {
		initTLS(ETLSInitState::CONNECT);
		return SSLConnection::connect(&this->reactor.ios, this->sslContextVar.get(), toAddr);
	}
#endif

	return Connection::connect(&this->reactor.ios, toAddr);
}

Future<Reference<IConnection>> Net2::connectExternal(NetworkAddress toAddr, const std::string& host) {
	return connect(toAddr, host);
}

ACTOR static Future<std::vector<NetworkAddress>> resolveTCPEndpoint_impl(Net2* self,
                                                                         std::string host,
                                                                         std::string service) {
	state tcp::resolver tcpResolver(self->reactor.ios);
	Promise<std::vector<NetworkAddress>> promise;
	state Future<std::vector<NetworkAddress>> result = promise.getFuture();

	tcpResolver.async_resolve(tcp::resolver::query(host, service),
	                          [=](const boost::system::error_code& ec, tcp::resolver::iterator iter) {
		                          if (ec) {
			                          promise.sendError(lookup_failed());
			                          return;
		                          }

		                          std::vector<NetworkAddress> addrs;

		                          tcp::resolver::iterator end;
		                          while (iter != end) {
			                          auto endpoint = iter->endpoint();
			                          auto addr = endpoint.address();
			                          if (addr.is_v6()) {
				                          addrs.emplace_back(IPAddress(addr.to_v6().to_bytes()), endpoint.port());
			                          } else {
				                          addrs.emplace_back(addr.to_v4().to_ulong(), endpoint.port());
			                          }
			                          ++iter;
		                          }

		                          if (addrs.empty()) {
			                          promise.sendError(lookup_failed());
		                          } else {
			                          promise.send(addrs);
		                          }
	                          });

	wait(ready(result));
	tcpResolver.cancel();

	return result.get();
}

Future<Reference<IUDPSocket>> Net2::createUDPSocket(NetworkAddress toAddr) {
	return UDPSocket::connect(&reactor.ios, toAddr, toAddr.ip.isV6());
}

Future<Reference<IUDPSocket>> Net2::createUDPSocket(bool isV6) {
	return UDPSocket::connect(&reactor.ios, Optional<NetworkAddress>(), isV6);
}

Future<std::vector<NetworkAddress>> Net2::resolveTCPEndpoint(const std::string& host, const std::string& service) {
	return resolveTCPEndpoint_impl(this, host, service);
}

std::vector<NetworkAddress> Net2::resolveTCPEndpointBlocking(const std::string& host, const std::string& service) {
	tcp::resolver tcpResolver(reactor.ios);
	tcp::resolver::query query(host, service);
	auto iter = tcpResolver.resolve(query);
	decltype(iter) end;
	std::vector<NetworkAddress> addrs;
	while (iter != end) {
		auto endpoint = iter->endpoint();
		auto addr = endpoint.address();
		if (addr.is_v6()) {
			addrs.emplace_back(IPAddress(addr.to_v6().to_bytes()), endpoint.port());
		} else {
			addrs.emplace_back(addr.to_v4().to_ulong(), endpoint.port());
		}
		++iter;
	}
	return addrs;
}

bool Net2::isAddressOnThisHost(NetworkAddress const& addr) const {
	auto it = addressOnHostCache.find(addr.ip);
	if (it != addressOnHostCache.end())
		return it->second;

	if (addressOnHostCache.size() > 50000)
		addressOnHostCache.clear(); // Bound cache memory; should not really happen

	try {
		boost::asio::io_service ioService;
		boost::asio::ip::udp::socket socket(ioService);
		boost::asio::ip::udp::endpoint endpoint(tcpAddress(addr.ip), 1);
		socket.connect(endpoint);
		bool local = addr.ip.isV6() ? socket.local_endpoint().address().to_v6().to_bytes() == addr.ip.toV6()
		                            : socket.local_endpoint().address().to_v4().to_ulong() == addr.ip.toV4();
		socket.close();
		if (local)
			TraceEvent(SevInfo, "AddressIsOnHost").detail("Address", addr);
		return addressOnHostCache[addr.ip] = local;
	} catch (boost::system::system_error e) {
		TraceEvent(SevWarnAlways, "IsAddressOnHostError")
		    .detail("Address", addr)
		    .detail("ErrDesc", e.what())
		    .detail("ErrCode", e.code().value());
		return addressOnHostCache[addr.ip] = false;
	}
}

Reference<IListener> Net2::listen(NetworkAddress localAddr) {
	try {
#ifndef TLS_DISABLED
		if (localAddr.isTLS()) {
			initTLS(ETLSInitState::LISTEN);
			return Reference<IListener>(new SSLListener(reactor.ios, &this->sslContextVar, localAddr));
		}
#endif
		return Reference<IListener>(new Listener(reactor.ios, localAddr));
	} catch (boost::system::system_error const& e) {
		Error x;
		if (e.code().value() == EADDRINUSE)
			x = address_in_use();
		else if (e.code().value() == EADDRNOTAVAIL)
			x = invalid_local_address();
		else
			x = bind_failed();
		TraceEvent("Net2ListenError").error(x).detail("Message", e.what());
		throw x;
	} catch (std::exception const& e) {
		Error x = unknown_error();
		TraceEvent("Net2ListenError").error(x).detail("Message", e.what());
		throw x;
	} catch (Error& e) {
		TraceEvent("Net2ListenError").error(e);
		throw e;
	} catch (...) {
		Error x = unknown_error();
		TraceEvent("Net2ListenError").error(x);
		throw x;
	}
}

void Net2::getDiskBytes(std::string const& directory, int64_t& free, int64_t& total) {
	return ::getDiskBytes(directory, free, total);
}

#ifdef __linux__
#include <sys/prctl.h>
#include <pthread.h>
#include <sched.h>
#endif

ASIOReactor::ASIOReactor(Net2* net) : do_not_stop(ios), network(net), firstTimer(ios) {
#ifdef __linux__
	// Reactor flags are used only for experimentation, and are platform-specific
	if (FLOW_KNOBS->REACTOR_FLAGS & 1) {
		prctl(PR_SET_TIMERSLACK, 1, 0, 0, 0);
		printf("Set timerslack to 1ns\n");
	}

	if (FLOW_KNOBS->REACTOR_FLAGS & 2) {
		int ret;
		pthread_t this_thread = pthread_self();
		struct sched_param params;
		params.sched_priority = sched_get_priority_max(SCHED_FIFO);
		ret = pthread_setschedparam(this_thread, SCHED_FIFO, &params);
		if (ret != 0)
			printf("Error setting priority (%d %d)\n", ret, errno);
		else
			printf("Set scheduler mode to SCHED_FIFO\n");
	}
#endif
}

void ASIOReactor::sleep(double sleepTime) {
	if (sleepTime > FLOW_KNOBS->BUSY_WAIT_THRESHOLD) {
		if (FLOW_KNOBS->REACTOR_FLAGS & 4) {
#ifdef __linux
			timespec tv;
			tv.tv_sec = 0;
			tv.tv_nsec = 20000;
			nanosleep(&tv, nullptr);
#endif
		} else {
			sleepTime -= FLOW_KNOBS->BUSY_WAIT_THRESHOLD;
			if (sleepTime < 4e12) {
				this->firstTimer.expires_from_now(boost::posix_time::microseconds(int64_t(sleepTime * 1e6)));
				this->firstTimer.async_wait(&nullWaitHandler);
			}
			setProfilingEnabled(0); // The following line generates false positives for slow task profiling
			ios.run_one();
			setProfilingEnabled(1);
			this->firstTimer.cancel();
		}
		++network->countASIOEvents;
	} else if (sleepTime > 0) {
		if (!(FLOW_KNOBS->REACTOR_FLAGS & 8))
			threadYield();
	}
}

void ASIOReactor::react() {
	while (ios.poll_one())
		++network->countASIOEvents; // Make this a task?
}

void ASIOReactor::wake() {
	ios.post(nullCompletionHandler);
}

} // namespace N2

SendBufferIterator::SendBufferIterator(SendBuffer const* p, int limit) : p(p), limit(limit) {
	ASSERT(limit > 0);
}

void SendBufferIterator::operator++() {
	limit -= p->bytes_written - p->bytes_sent;
	if (limit > 0)
		p = p->next;
	else
		p = nullptr;
}

boost::asio::const_buffer SendBufferIterator::operator*() const {
	return boost::asio::const_buffer(p->data() + p->bytes_sent, std::min(limit, p->bytes_written - p->bytes_sent));
}

INetwork* newNet2(const TLSConfig& tlsConfig, bool useThreadPool, bool useMetrics) {
	try {
		N2::g_net2 = new N2::Net2(tlsConfig, useThreadPool, useMetrics);
	} catch (boost::system::system_error e) {
		TraceEvent("Net2InitError").detail("Message", e.what());
		throw unknown_error();
	} catch (std::exception const& e) {
		TraceEvent("Net2InitError").detail("Message", e.what());
		throw unknown_error();
	}

	return N2::g_net2;
}

struct TestGVR {
	Standalone<StringRef> key;
	int64_t version;
	Optional<std::pair<UID, UID>> debugID;
	Promise<Optional<Standalone<StringRef>>> reply;

	TestGVR() {}

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, key, version, debugID, reply);
	}
};

template <class F>
void startThreadF(F&& func) {
	struct Thing {
		F f;
		Thing(F&& f) : f(std::move(f)) {}
		THREAD_FUNC start(void* p) {
			Thing* self = (Thing*)p;
			self->f();
			delete self;
			THREAD_RETURN;
		}
	};
	Thing* t = new Thing(std::move(func));
	startThread(Thing::start, t);
}

void net2_test(){
	/*printf("ThreadSafeQueue test\n");
	printf("  Interface: ");
	ThreadSafeQueue<int> tq;
	ASSERT( tq.canSleep() == true );

	ASSERT( tq.push( 1 ) == true ) ;
	ASSERT( tq.push( 2 ) == false );
	ASSERT( tq.push( 3 ) == false );

	ASSERT( tq.pop().get() == 1 );
	ASSERT( tq.pop().get() == 2 );
	ASSERT( tq.push( 4 ) == false );
	ASSERT( tq.pop().get() == 3 );
	ASSERT( tq.pop().get() == 4 );
	ASSERT( !tq.pop().present() );
	printf("OK\n");

	printf("Threaded: ");
	Event finished, finished2;
	int thread1Iterations = 1000000, thread2Iterations = 100000;

	if (thread1Iterations)
	    startThreadF([&](){
	        printf("Thread1\n");
	        for(int i=0; i<thread1Iterations; i++)
	            tq.push(i);
	        printf("T1Done\n");
	        finished.set();
	    });
	if (thread2Iterations)
	    startThreadF([&](){
	        printf("Thread2\n");
	        for(int i=0; i<thread2Iterations; i++)
	            tq.push(i + (1<<20));
	        printf("T2Done\n");
	        finished2.set();
	    });
	int c = 0, mx[2]={0, 1<<20}, p = 0;
	while (c < thread1Iterations + thread2Iterations)
	{
	    Optional<int> i = tq.pop();
	    if (i.present()) {
	        int v = i.get();
	        ++c;
	        if (mx[v>>20] != v)
	            printf("Wrong value dequeued!\n");
	        ASSERT( mx[v>>20] == v );
	        mx[v>>20] = v + 1;
	    } else {
	        ++p;
	        _mm_pause();
	    }
	    if ((c&3)==0) tq.canSleep();
	}
	printf("%d %d %x %x %s\n", c, p, mx[0], mx[1], mx[0]==thread1Iterations && mx[1]==(1<<20)+thread2Iterations ? "OK" :
	"FAIL");

	finished.block();
	finished2.block();


	g_network = newNet2();  // for promise serialization below

	Endpoint destination;

	printf("  Used: %lld\n", FastAllocator<4096>::getTotalMemory());

	char junk[100];

	double before = timer();

	std::vector<TestGVR> reqs;
	reqs.reserve( 10000 );

	int totalBytes = 0;
	for(int j=0; j<1000; j++) {
	    UnsentPacketQueue unsent;
	    ReliablePacketList reliable;

	    reqs.resize(10000);
	    for(int i=0; i<10000; i++) {
	        TestGVR &req = reqs[i];
	        req.key = LiteralStringRef("Foobar");

	        SerializeSource<TestGVR> what(req);

	        SendBuffer* pb = unsent.getWriteBuffer();
	        ReliablePacket* rp = new ReliablePacket;  // 0

	        PacketWriter wr(pb,rp,AssumeVersion(g_network->protocolVersion()));
	        //BinaryWriter wr;
	        SplitBuffer packetLen;
	        uint32_t len = 0;
	        wr.writeAhead(sizeof(len), &packetLen);
	        wr << destination.token;
	        //req.reply.getEndpoint();
	        what.serializePacketWriter(wr);
	        //wr.serializeBytes(junk, 43);

	        unsent.setWriteBuffer(wr.finish());
	        len = wr.size() - sizeof(len);
	        packetLen.write(&len, sizeof(len));

	        //totalBytes += wr.getLength();
	        totalBytes += wr.size();

	        if (rp) reliable.insert(rp);
	    }
	    reqs.clear();
	    unsent.discardAll();
	    reliable.discardAll();
	}

	printf("SimSend x 1Kx10K: %0.2f sec\n", timer()-before);
	printf("  Bytes: %d\n", totalBytes);
	printf("  Used: %lld\n", FastAllocator<4096>::getTotalMemory());
	*/
};
