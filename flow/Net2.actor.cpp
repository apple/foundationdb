/*
 * Net2.actor.cpp
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

#include "boost/asio/buffer.hpp"
#include "boost/asio/ip/address.hpp"
#include "boost/system/system_error.hpp"
#include "flow/Arena.h"
#include "flow/Knobs.h"
#include "flow/Platform.h"
#include "flow/Trace.h"
#include "flow/swift.h"
#include "flow/swift_concurrency_hooks.h"
#include <algorithm>
#include <memory>
#include <string_view>
#ifndef BOOST_SYSTEM_NO_LIB
#define BOOST_SYSTEM_NO_LIB
#endif
#ifndef BOOST_DATE_TIME_NO_LIB
#define BOOST_DATE_TIME_NO_LIB
#endif
#ifndef BOOST_REGEX_NO_LIB
#define BOOST_REGEX_NO_LIB
#endif
#include <boost/asio.hpp>
#include "boost/asio/ssl.hpp"
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/range.hpp>
#include <boost/algorithm/string/join.hpp>
#include "flow/network.h"
#include "flow/IThreadPool.h"

#include "flow/IAsyncFile.h"
#include "flow/ActorCollection.h"
#include "flow/TaskQueue.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/ChaosMetrics.h"
#include "flow/TDMetric.actor.h"
#include "flow/AsioReactor.h"
#include "flow/Profiler.h"
#include "flow/ProtocolVersion.h"
#include "flow/SendBufferIterator.h"
#include "flow/TLSConfig.actor.h"
#include "flow/WatchFile.actor.h"
#include "flow/genericactors.actor.h"
#include "flow/Util.h"
#include "flow/UnitTest.h"
#include "flow/ScopeExit.h"
#include "flow/IUDPSocket.h"
#include "flow/IConnection.h"

#ifdef ADDRESS_SANITIZER
#include <sanitizer/lsan_interface.h>
#endif

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

class Peer;
class Connection;

// Outlives main
Net2* g_net2 = nullptr;

thread_local INetwork* thread_network = nullptr;

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
	Future<Reference<IConnection>> connect(NetworkAddress toAddr, tcp::socket* existingSocket = nullptr) override;
	Future<Reference<IConnection>> connectExternal(NetworkAddress toAddr) override;
	Future<Reference<IConnection>> connectExternalWithHostname(NetworkAddress toAddr,
	                                                           const std::string& hostname) override;
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
	Future<std::vector<NetworkAddress>> resolveTCPEndpointWithDNSCache(const std::string& host,
	                                                                   const std::string& service) override;
	std::vector<NetworkAddress> resolveTCPEndpointBlocking(const std::string& host,
	                                                       const std::string& service) override;
	std::vector<NetworkAddress> resolveTCPEndpointBlockingWithDNSCache(const std::string& host,
	                                                                   const std::string& service) override;
	Reference<IListener> listen(NetworkAddress localAddr) override;

	// INetwork interface
	double now() const override { return currentTime; };
	double timer() override { return ::timer(); };
	double timer_monotonic() override { return ::timer_monotonic(); };
	Future<Void> delay(double seconds, TaskPriority taskId) override;
	Future<Void> orderedDelay(double seconds, TaskPriority taskId) override;
	void _swiftEnqueue(void* task) override;
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
			onMainThreadVoid([this] { this->stopImmediately(); });
	}
	void addStopCallback(std::function<void()> fn) override {
		if (thread_network == this)
			stopCallbacks.emplace_back(std::move(fn));
		else
			onMainThreadVoid([this, fn] { this->stopCallbacks.emplace_back(std::move(fn)); });
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

	ProtocolVersion protocolVersion() const override { return currentProtocolVersion(); }

	std::vector<flowGlobalType> globals;

	const TLSConfig& getTLSConfig() const override { return tlsConfig; }

	bool checkRunnable() override;

#ifdef ENABLE_SAMPLING
	ActorLineageSet& getActorLineageSet() override;
#endif

	bool useThreadPool;

	// private:

	ASIOReactor reactor;
	AsyncVar<Reference<ReferencedObject<boost::asio::ssl::context>>> sslContextVar;
	Reference<IThreadPool> sslHandshakerPool;
	int sslHandshakerThreadsStarted;
	int sslPoolHandshakesInProgress;
	TLSConfig tlsConfig;
	Reference<TLSPolicy> activeTlsPolicy;
	Future<Void> backgroundCertRefresh;
	ETLSInitState tlsInitializedState;

	INetworkConnections* network; // initially this, but can be changed

	int64_t tscBegin, tscEnd;
	double taskBegin;
	TaskPriority currentTaskID;
	TDMetricCollection tdmetrics;
	MetricCollection metrics;
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

	struct PromiseTask final : public FastAllocated<PromiseTask> {
		Promise<Void> promise;
		swift::Job* _Nullable swiftJob = nullptr;
		PromiseTask() {}
		explicit PromiseTask(Promise<Void>&& promise) noexcept : promise(std::move(promise)) {}
		explicit PromiseTask(swift::Job* swiftJob) : swiftJob(swiftJob) {}

		void operator()() {
#ifdef WITH_SWIFT
			if (auto job = swiftJob) {
				swift_job_run(job, ExecutorRef::generic());
			} else {
				promise.send(Void());
			}
#else
			promise.send(Void());
#endif
			delete this;
		}
	};

	TaskQueue<PromiseTask> taskQueue;

	void checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, TaskPriority priority);
	bool check_yield(TaskPriority taskId, int64_t tscNow);
	void trackAtPriority(TaskPriority priority, double now);
	void stopImmediately() {
#ifdef ADDRESS_SANITIZER
		// Do leak check before intentionally leaking a bunch of memory
		__lsan_do_leak_check();
#endif
		stopped = true;
		taskQueue.clear();
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
	Optional<std::string> proxy;
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
	std::variant<const char*, AuditedEvent> errContext;
	UID errID;
	NetworkAddress peerAddr;

public:
	BindPromise(const char* errContext, UID errID) : errContext(errContext), errID(errID) {}
	BindPromise(AuditedEvent auditedEvent, UID errID) : errContext(auditedEvent), errID(errID) {}
	BindPromise(BindPromise const& r) : p(r.p), errContext(r.errContext), errID(r.errID), peerAddr(r.peerAddr) {}
	BindPromise(BindPromise&& r) noexcept
	  : p(std::move(r.p)), errContext(r.errContext), errID(r.errID), peerAddr(r.peerAddr) {}

	Future<Void> getFuture() const { return p.getFuture(); }

	NetworkAddress getPeerAddr() const { return peerAddr; }

	void setPeerAddr(const NetworkAddress& addr) { peerAddr = addr; }

	void operator()(const boost::system::error_code& error, size_t bytesWritten = 0) {
		try {
			if (error) {
				// Log the error...
				{
					std::optional<TraceEvent> traceEvent;
					if (std::holds_alternative<AuditedEvent>(errContext))
						traceEvent.emplace(SevWarn, std::get<AuditedEvent>(errContext), errID);
					else
						traceEvent.emplace(SevWarn, std::get<const char*>(errContext), errID);
					TraceEvent& evt = *traceEvent;
					evt.suppressFor(1.0)
					    .detail("ErrorCode", error.value())
					    .detail("ErrorMsg", error.message())
					    .detail("BackgroundThread", false);
					// There is no function in OpenSSL to use to check if an error code is from OpenSSL,
					// but all OpenSSL errors have a non-zero "library" code set in bits 24-32, and linux
					// error codes should never go that high.
					if (error.value() >= (1 << 24L)) {
						evt.detail("WhichMeans", TLSPolicy::ErrorString(error));
					}

					if (peerAddr.isValid()) {
						evt.detail("PeerAddr", peerAddr);
						evt.detail("PeerAddress", peerAddr);
					}
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

	bool hasTrustedPeer() const override { return true; }

	UID getDebugID() const override { return id; }

	tcp::socket& getSocket() override { return socket; }

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
			    .detail("PeerAddr", peer_address)
			    .detail("PeerAddress", peer_address)
			    .detail("ErrorCode", error.value())
			    .detail("Message", error.message());
	}

	void onReadError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_ReadError", id)
		    .suppressFor(1.0)
		    .detail("PeerAddr", peer_address)
		    .detail("PeerAddress", peer_address)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
	void onWriteError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_WriteError", id)
		    .suppressFor(1.0)
		    .detail("PeerAddr", peer_address)
		    .detail("PeerAddress", peer_address)
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
		// when port 0 is passed in, a random port will be opened
		// set listenAddress as the address with the actual port opened instead of port 0
		if (listenAddress.port == 0) {
			this->listenAddress =
			    NetworkAddress::parse(acceptor.local_endpoint().address().to_string().append(":").append(
			        std::to_string(acceptor.local_endpoint().port())));
		}
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

typedef boost::asio::ssl::stream<boost::asio::ip::tcp::socket&> ssl_socket;

struct SSLHandshakerThread final : IThreadPoolReceiver {
	SSLHandshakerThread() {}
	void init() override {}

	struct Handshake final : TypedAction<SSLHandshakerThread, Handshake> {
		Handshake(ssl_socket& socket, ssl_socket::handshake_type type) : socket(socket), type(type) {}
		double getTimeEstimate() const override { return 0.001; }

		std::string getPeerEndPointAddress() const {
			std::ostringstream o;
			boost::system::error_code ec;
			auto addr = socket.lowest_layer().remote_endpoint(ec);
			o << (!ec.failed() ? addr.address().to_string() : std::string_view("0.0.0.0"));
			return std::move(o).str();
		}

		NetworkAddress getPeerAddress() const { return peerAddr; }

		void setPeerAddr(const NetworkAddress& addr) { peerAddr = addr; }

		ThreadReturnPromise<Void> done;
		ssl_socket& socket;
		ssl_socket::handshake_type type;
		boost::system::error_code err;
		NetworkAddress peerAddr;
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
				           h.type == ssl_socket::handshake_type::client ? "N2_ConnectHandshakeError"_audit
				                                                        : "N2_AcceptHandshakeError"_audit)
				    .detail("PeerAddr", h.getPeerAddress())
				    .detail("PeerAddress", h.getPeerAddress())
				    .detail("PeerEndPoint", h.getPeerEndPointAddress())
				    .detail("ErrorCode", h.err.value())
				    .detail("ErrorMsg", h.err.message().c_str())
				    .detail("BackgroundThread", true);
				h.done.sendError(connection_failed());
			} else {
				h.done.send(Void());
			}
		} catch (...) {
			TraceEvent(SevWarn,
			           h.type == ssl_socket::handshake_type::client ? "N2_ConnectHandshakeUnknownError"_audit
			                                                        : "N2_AcceptHandshakeUnknownError"_audit)
			    .detail("PeerAddr", h.getPeerAddress())
			    .detail("PeerAddress", h.getPeerAddress())
			    .detail("PeerEndPoint", h.getPeerEndPointAddress())
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
	    sslContext(context), has_trusted_peer(false) {}

	explicit SSLConnection(Reference<ReferencedObject<boost::asio::ssl::context>> context, tcp::socket* existingSocket)
	  : id(nondeterministicRandom()->randomUniqueID()), socket(std::move(*existingSocket)),
	    ssl_sock(socket, context->mutate()), sslContext(context) {}

	// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
	ACTOR static Future<Reference<IConnection>> connect(boost::asio::io_service* ios,
	                                                    Reference<ReferencedObject<boost::asio::ssl::context>> context,
	                                                    NetworkAddress addr,
	                                                    tcp::socket* existingSocket = nullptr) {
		std::pair<IPAddress, uint16_t> peerIP = std::make_pair(addr.ip, addr.port);
		auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
		if (iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
			if (now() < iter->second.second) {
				if (iter->second.first >= FLOW_KNOBS->TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS) {
					TraceEvent("TLSOutgoingConnectionThrottlingWarning").suppressFor(1.0).detail("PeerIP", addr);
					wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT));
					static SimpleCounter<int64_t>* countClientTLSHandshakeThrottled =
					    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ClientTLSHandshakeThrottled");
					countClientTLSHandshakeThrottled->increment(1);
					throw connection_failed();
				}
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler.erase(peerIP);
			}
		}

		if (existingSocket != nullptr) {
			Reference<SSLConnection> self(new SSLConnection(context, existingSocket));
			self->peer_address = addr;
			self->init();
			return self;
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
		} catch (Error&) {
			// Either the connection failed, or was cancelled by the caller
			self->closeSocket();
			throw;
		}
	}

	// Connect with hostname for SNI (Server Name Indication) support
	ACTOR static Future<Reference<IConnection>> connectWithHostname(
	    boost::asio::io_service* ios,
	    Reference<ReferencedObject<boost::asio::ssl::context>> context,
	    NetworkAddress addr,
	    std::string hostname) {
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
		self->sni_hostname = hostname; // Store hostname for SNI during handshake

		// Store hostname for SNI use during handshake

		try {
			auto to = tcpEndpoint(self->peer_address);
			BindPromise p("N2_ConnectError", self->id);
			Future<Void> onConnected = p.getFuture();
			self->socket.async_connect(to, std::move(p));

			wait(onConnected);

			// SNI will be set later in doConnectHandshake before SSL handshake

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

	ACTOR static void doAcceptHandshake(Reference<SSLConnection> self, Promise<Void> connected) {
		state Hold<int> holder;

		try {
			Future<Void> onHandshook;
			ConfigureSSLStream(N2::g_net2->activeTlsPolicy,
			                   self->ssl_sock,
			                   self->peer_address,
			                   [conn = self.getPtr()](bool verifyOk) { conn->has_trusted_peer = verifyOk; });

			// If the background handshakers are not all busy, use one
			// FIXME: see comment elsewhere about making this the only path.
			if ((FLOW_KNOBS->DISABLE_MAINTHREAD_TLS_HANDSHAKE && N2::g_net2->sslHandshakerThreadsStarted > 0) ||
			    N2::g_net2->sslPoolHandshakesInProgress < N2::g_net2->sslHandshakerThreadsStarted) {
				static SimpleCounter<int64_t>* countServerTLSHandshakesOnSideThreads =
				    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ServerTLSHandshakesOnSideThreads");
				countServerTLSHandshakesOnSideThreads->increment(1);
				holder = Hold(&N2::g_net2->sslPoolHandshakesInProgress);
				auto handshake =
				    new SSLHandshakerThread::Handshake(self->ssl_sock, boost::asio::ssl::stream_base::server);
				handshake->setPeerAddr(self->getPeerAddress());
				onHandshook = handshake->done.getFuture();
				N2::g_net2->sslHandshakerPool->post(handshake);
			} else {
				// Otherwise use flow network thread
				static SimpleCounter<int64_t>* countServerTLSHandshakesOnMainThread =
				    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ServerTLSHandshakesOnMainThread");
				countServerTLSHandshakesOnMainThread->increment(1);
				BindPromise p("N2_AcceptHandshakeError"_audit, self->id);
				p.setPeerAddr(self->getPeerAddress());
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
					static SimpleCounter<int64_t>* countServerTLSHandshakeThrottled =
					    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ServerTLSHandshakeThrottled");
					countServerTLSHandshakeThrottled->increment(1);
					throw connection_failed();
				}
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler.erase(peerIP);
			}
		}

		wait(g_network->networkInfo.handshakeLock->take(
		    getTaskPriorityFromInt(FLOW_KNOBS->TLS_HANDSHAKE_FLOWLOCK_PRIORITY)));
		state FlowLock::Releaser releaser(*g_network->networkInfo.handshakeLock);
		static SimpleCounter<int64_t>* countServerTLSHandshakeLocked =
		    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ServerTLSHandshakeLocked");
		countServerTLSHandshakeLocked->increment(1);

		Promise<Void> connected;
		doAcceptHandshake(self, connected);
		try {
			choose {
				when(wait(connected.getFuture())) {
					static SimpleCounter<int64_t>* countServerTLSHandshakesSucceed =
					    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ServerTLSHandshakesSucceed");
					countServerTLSHandshakesSucceed->increment(1);
					return Void();
				}
				when(wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT))) {
					static SimpleCounter<int64_t>* countServerTLSHandshakesTimedout =
					    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ServerTLSHandshakesTimedout");
					countServerTLSHandshakesTimedout->increment(1);
					throw connection_failed();
				}
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
			ConfigureSSLStream(N2::g_net2->activeTlsPolicy,
			                   self->ssl_sock,
			                   self->peer_address,
			                   [conn = self.getPtr()](bool verifyOk) { conn->has_trusted_peer = verifyOk; });

			// Set SNI hostname if we have one (for connections made with hostname)
			if (!self->sni_hostname.empty()) {
				int result = SSL_set_tlsext_host_name(self->ssl_sock.native_handle(), self->sni_hostname.c_str());
				TraceEvent("SSLSetSNIResult")
				    .detail("Hostname", self->sni_hostname)
				    .detail("Result", result)
				    .detail("Addr", self->peer_address);
			}

			// If the background handshakers are not all busy, use one

			// FIXME: this should probably be changed never to use the
			// main thread, as waiting for potentially high-RTT TLS
			// handshakes can delay execution of everything else that
			// runs on the main thread.  The cost of that (in terms of
			// unpredictable system performance and reliability) is
			// much, much higher than the cost a few hundred or
			// thousand incremental threads.
			if ((FLOW_KNOBS->DISABLE_MAINTHREAD_TLS_HANDSHAKE && N2::g_net2->sslHandshakerThreadsStarted > 0) ||
			    N2::g_net2->sslPoolHandshakesInProgress < N2::g_net2->sslHandshakerThreadsStarted) {
				static SimpleCounter<int64_t>* countClientTLSHandshakesOnSideThreads =
				    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ClientTLSHandshakesOnSideThreads");
				countClientTLSHandshakesOnSideThreads->increment(1);
				holder = Hold(&N2::g_net2->sslPoolHandshakesInProgress);
				auto handshake =
				    new SSLHandshakerThread::Handshake(self->ssl_sock, boost::asio::ssl::stream_base::client);
				handshake->setPeerAddr(self->getPeerAddress());
				onHandshook = handshake->done.getFuture();
				N2::g_net2->sslHandshakerPool->post(handshake);
			} else {
				// Otherwise use flow network thread
				static SimpleCounter<int64_t>* countClientTLSHandshakesOnMainThread =
				    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ClientTLSHandshakesOnMainThread");
				countClientTLSHandshakesOnMainThread->increment(1);
				BindPromise p("N2_ConnectHandshakeError"_audit, self->id);
				p.setPeerAddr(self->getPeerAddress());
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
		wait(g_network->networkInfo.handshakeLock->take(
		    getTaskPriorityFromInt(FLOW_KNOBS->TLS_HANDSHAKE_FLOWLOCK_PRIORITY)));
		state FlowLock::Releaser releaser(*g_network->networkInfo.handshakeLock);
		static SimpleCounter<int64_t>* countClientTLSHandshakeLocked =
		    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ClientTLSHandshakeLocked");
		countClientTLSHandshakeLocked->increment(1);

		Promise<Void> connected;
		doConnectHandshake(self, connected);
		try {
			choose {
				when(wait(connected.getFuture())) {
					static SimpleCounter<int64_t>* countClientTLSHandshakesSucceed =
					    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ClientTLSHandshakesSucceed");
					countClientTLSHandshakesSucceed->increment(1);
					return Void();
				}
				when(wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT))) {
					static SimpleCounter<int64_t>* countClientTLSHandshakesTimedout =
					    SimpleCounter<int64_t>::makeCounter("/Net2/TLS/ClientTLSHandshakesTimedout");
					countClientTLSHandshakesTimedout->increment(1);
					throw connection_failed();
				}
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

	bool hasTrustedPeer() const override { return has_trusted_peer; }

	UID getDebugID() const override { return id; }

	tcp::socket& getSocket() override { return socket; }

	ssl_socket& getSSLSocket() { return ssl_sock; }

private:
	UID id;
	tcp::socket socket;
	ssl_socket ssl_sock;
	NetworkAddress peer_address;
	Reference<ReferencedObject<boost::asio::ssl::context>> sslContext;
	bool has_trusted_peer;
	std::string sni_hostname; // For Server Name Indication

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
		    .detail("PeerAddr", peer_address)
		    .detail("PeerAddress", peer_address)
		    .detail("ErrorCode", error.value())
		    .detail("Message", error.message());
		closeSocket();
	}
	void onWriteError(const boost::system::error_code& error) {
		TraceEvent(SevWarn, "N2_WriteError", id)
		    .suppressFor(1.0)
		    .detail("PeerAddr", peer_address)
		    .detail("PeerAddress", peer_address)
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
		// when port 0 is passed in, a random port will be opened
		// set listenAddress as the address with the actual port opened instead of port 0
		if (listenAddress.port == 0) {
			this->listenAddress = NetworkAddress::parse(acceptor.local_endpoint()
			                                                .address()
			                                                .to_string()
			                                                .append(":")
			                                                .append(std::to_string(acceptor.local_endpoint().port()))
			                                                .append(listenAddress.isTLS() ? ":tls" : ""));
		}
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

// 5MB for loading files into memory

Net2::Net2(const TLSConfig& tlsConfig, bool useThreadPool, bool useMetrics)
  : globals(enumGlobal::COUNT), useThreadPool(useThreadPool), reactor(this),
    sslContextVar({ ReferencedObject<boost::asio::ssl::context>::from(
        boost::asio::ssl::context(boost::asio::ssl::context::tls)) }),
    sslHandshakerThreadsStarted(0), sslPoolHandshakesInProgress(0), tlsConfig(tlsConfig),
    tlsInitializedState(ETLSInitState::NONE), network(this), tscBegin(0), tscEnd(0), taskBegin(0),
    currentTaskID(TaskPriority::DefaultYield), stopped(false), started(false), numYields(0),
    lastPriorityStats(nullptr) {
	// Until run() is called, yield() will always yield
	TraceEvent("Net2Starting").log();

	// Set the global members
	if (useMetrics) {
		setGlobal(INetwork::enTDMetrics, (flowGlobalType)&tdmetrics);
	}
	if (FLOW_KNOBS->ENABLE_CHAOS_FEATURES) {
		setGlobal(INetwork::enChaosMetrics, (flowGlobalType)&chaosMetrics);
	}
	setGlobal(INetwork::enMetrics, (flowGlobalType)&metrics);
	setGlobal(INetwork::enNetworkConnections, (flowGlobalType)network);
	setGlobal(INetwork::enASIOService, (flowGlobalType)&reactor.ios);
	setGlobal(INetwork::enBlobCredentialFiles, &blobCredentialFiles);
	setGlobal(INetwork::enProxy, &proxy);

#ifdef __linux__
	setGlobal(INetwork::enEventFD, (flowGlobalType)N2::ASIOReactor::newEventFD(reactor));
#endif

	updateNow();
}

ACTOR static Future<Void> reloadCertificatesOnChange(
    TLSConfig config,
    std::function<void()> onPolicyFailure,
    AsyncVar<Reference<ReferencedObject<boost::asio::ssl::context>>>* contextVar,
    Reference<TLSPolicy>* policy) {
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
	const int& intervalSeconds = FLOW_KNOBS->TLS_CERT_REFRESH_DELAY_SECONDS;
	lifetimes.push_back(watchFileForChanges(
	    config.getCertificatePathSync(), &fileChanged, &intervalSeconds, "TLSCertificateRefreshStatError"));
	lifetimes.push_back(
	    watchFileForChanges(config.getKeyPathSync(), &fileChanged, &intervalSeconds, "TLSKeyRefreshStatError"));
	lifetimes.push_back(
	    watchFileForChanges(config.getCAPathSync(), &fileChanged, &intervalSeconds, "TLSCARefreshStatError"));
	loop {
		wait(fileChanged.onTrigger());
		TraceEvent("TLSCertificateRefreshBegin").log();

		try {
			LoadedTLSConfig loaded = wait(config.loadAsync());
			boost::asio::ssl::context context(boost::asio::ssl::context::tls);
			ConfigureSSLContext(loaded, context);
			*policy = makeReference<TLSPolicy>(loaded, onPolicyFailure);
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

void Net2::initTLS(ETLSInitState targetState) {
	if (tlsInitializedState >= targetState) {
		return;
	}
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
			    .detail("VerifyPeers", boost::algorithm::join(loaded.getVerifyPeers(), "|"))
			    .detail("DisablePlainTextConnection", tlsConfig.getDisablePlainTextConnection());
			auto loadedTlsConfig = tlsConfig.loadSync();
			ConfigureSSLContext(loadedTlsConfig, newContext);
			activeTlsPolicy = makeReference<TLSPolicy>(loadedTlsConfig, onPolicyFailure);
			sslContextVar.set(ReferencedObject<boost::asio::ssl::context>::from(std::move(newContext)));
		} catch (Error& e) {
			TraceEvent("Net2TLSInitError").error(e);
		}
		backgroundCertRefresh =
		    reloadCertificatesOnChange(tlsConfig, onPolicyFailure, &sslContextVar, &activeTlsPolicy);
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
	bytesReceived.init("Net2.BytesReceived"_sr);
	countWriteProbes.init("Net2.CountWriteProbes"_sr);
	countReadProbes.init("Net2.CountReadProbes"_sr);
	countReads.init("Net2.CountReads"_sr);
	countWouldBlock.init("Net2.CountWouldBlock"_sr);
	countWrites.init("Net2.CountWrites"_sr);
	countRunLoop.init("Net2.CountRunLoop"_sr);
	countTasks.init("Net2.CountTasks"_sr);
	countYields.init("Net2.CountYields"_sr);
	countYieldBigStack.init("Net2.CountYieldBigStack"_sr);
	countYieldCalls.init("Net2.CountYieldCalls"_sr);
	countASIOEvents.init("Net2.CountASIOEvents"_sr);
	countYieldCallsTrue.init("Net2.CountYieldCallsTrue"_sr);
	countRunLoopProfilingSignals.init("Net2.CountRunLoopProfilingSignals"_sr);
	countTLSPolicyFailures.init("Net2.CountTLSPolicyFailures"_sr);
	priorityMetric.init("Net2.Priority"_sr);
	awakeMetric.init("Net2.Awake"_sr);
	slowTaskMetric.init("Net2.SlowTask"_sr);
	countLaunchTime.init("Net2.CountLaunchTime"_sr);
	countReactTime.init("Net2.CountReactTime"_sr);
	taskQueue.initMetrics();
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

	unsigned int tasksSinceReact = 0;

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
		if (taskQueue.canSleep()) {
			sleepTime = 1e99;
			double sleepStart = timer_monotonic();
			sleepTime = taskQueue.getSleepTime(sleepStart);
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
		tasksSinceReact = 0;

		updateNow();
		double now = this->currentTime;
		trackAtPriority(TaskPriority::RunLoop, now);

		countReactTime += now - taskBegin;
		checkForSlowTask(tscBegin, timestampCounter(), now - taskBegin, TaskPriority::ASIOReactor);

		if ((now - nnow) > FLOW_KNOBS->SLOW_LOOP_CUTOFF &&
		    nondeterministicRandom()->random01() < (now - nnow) * FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopTop").detail("Elapsed", now - nnow);

		taskQueue.processReadyTimers(now);

		taskQueue.processThreadReady();

		tscBegin = timestampCounter();
		tscEnd = tscBegin + FLOW_KNOBS->TSC_YIELD_TIME;
		taskBegin = timer_monotonic();
		numYields = 0;
		TaskPriority minTaskID = TaskPriority::Max;
		[[maybe_unused]] int queueSize = taskQueue.getNumReadyTasks();

		FDB_TRACE_PROBE(run_loop_tasks_start, queueSize);
		while (taskQueue.hasReadyTask()) {
			++countTasks;
			currentTaskID = taskQueue.getReadyTaskID();
			priorityMetric = static_cast<int64_t>(currentTaskID);
			PromiseTask* task = taskQueue.getReadyTask();
			taskQueue.popReadyTask();

			try {
				++tasksSinceReact;
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
			if (tasksSinceReact >= FLOW_KNOBS->TASKS_PER_REACTOR_CHECK) {
				if (runFunc) {
					runFunc();
				}
				reactor.react();
				tasksSinceReact = 0;
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

		queueSize = taskQueue.getNumReadyTasks();
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

	taskQueue.processThreadReady();

	if (taskID == TaskPriority::DefaultYield)
		taskID = currentTaskID;
	if (taskQueue.hasReadyTask() && taskQueue.getReadyTaskPriority() > int64_t(taskID) << 32) {
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

// TODO: can we wrap our swift task and insert it in here?
Future<Void> Net2::delay(double seconds, TaskPriority taskId) {
	if (seconds >= 4e12) // Intervals that overflow an int64_t in microseconds (more than 100,000 years) are treated
	                     // as infinite
		return Never();

	PromiseTask* t = new PromiseTask;
	if (seconds <= 0.) {
		taskQueue.addReady(taskId, t);
	} else {
		double at = now() + seconds;
		taskQueue.addTimer(at, taskId, t);
	}
	return t->promise.getFuture();
}

Future<Void> Net2::orderedDelay(double seconds, TaskPriority taskId) {
	// The regular delay already provides the required ordering property
	return delay(seconds, taskId);
}

void Net2::_swiftEnqueue(void* _job) {
#ifdef WITH_SWIFT
	swift::Job* job = (swift::Job*)_job;
	TaskPriority priority = swift_priority_to_net2(job->getPriority());
	PromiseTask* t = new PromiseTask(job);
	taskQueue.addReady(priority, t);
#endif
}

void Net2::onMainThread(Promise<Void>&& signal, TaskPriority taskID) {
	if (stopped)
		return;
	PromiseTask* p = new PromiseTask(std::move(signal));
	if (taskQueue.addReadyThreadSafe(isOnMainThread(), taskID, p)) {
		reactor.wake();
	}
}

THREAD_HANDLE Net2::startThread(THREAD_FUNC_RETURN (*func)(void*), void* arg, int stackSize, const char* name) {
	return ::startThread(func, arg, stackSize, name);
}

Future<Reference<IConnection>> Net2::connect(NetworkAddress toAddr, tcp::socket* existingSocket) {
	if (toAddr.isTLS()) {
		initTLS(ETLSInitState::CONNECT);
		return SSLConnection::connect(&this->reactor.ios, this->sslContextVar.get(), toAddr, existingSocket);
	}

	if (tlsConfig.getDisablePlainTextConnection()) {
		TraceEvent(SevError, "PlainTextConnectionDisabled").detail("toAddr", toAddr);
		throw connection_failed();
	}

	return Connection::connect(&this->reactor.ios, toAddr);
}

Future<Reference<IConnection>> Net2::connectExternal(NetworkAddress toAddr) {
	return connect(toAddr);
}

Future<Reference<IConnection>> Net2::connectExternalWithHostname(NetworkAddress toAddr, const std::string& hostname) {
	if (toAddr.isTLS()) {
		initTLS(ETLSInitState::CONNECT);
		return SSLConnection::connectWithHostname(&this->reactor.ios, this->sslContextVar.get(), toAddr, hostname);
	}
	return connect(toAddr);
}

Future<Reference<IUDPSocket>> Net2::createUDPSocket(NetworkAddress toAddr) {
	return UDPSocket::connect(&reactor.ios, toAddr, toAddr.ip.isV6());
}

Future<Reference<IUDPSocket>> Net2::createUDPSocket(bool isV6) {
	return UDPSocket::connect(&reactor.ios, Optional<NetworkAddress>(), isV6);
}

ACTOR static Future<std::vector<NetworkAddress>> resolveTCPEndpoint_impl(Net2* self,
                                                                         std::string host,
                                                                         std::string service) {
	state tcp::resolver tcpResolver(self->reactor.ios);
	Promise<std::vector<NetworkAddress>> promise;
	state Future<std::vector<NetworkAddress>> result = promise.getFuture();

	tcpResolver.async_resolve(
	    host, service, [promise](const boost::system::error_code& ec, tcp::resolver::iterator iter) {
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
				    // IPV6 loopback might not be supported, only return IPV6 address
				    if (!addr.is_loopback()) {
					    addrs.emplace_back(IPAddress(addr.to_v6().to_bytes()), endpoint.port());
				    }
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

	try {
		wait(ready(result));
	} catch (Error& e) {
		if (e.code() == error_code_lookup_failed) {
			self->dnsCache.remove(host, service);
		}
		throw e;
	}
	tcpResolver.cancel();
	std::vector<NetworkAddress> ret = result.get();
	self->dnsCache.add(host, service, ret);

	return ret;
}

Future<std::vector<NetworkAddress>> Net2::resolveTCPEndpoint(const std::string& host, const std::string& service) {
	return resolveTCPEndpoint_impl(this, host, service);
}

Future<std::vector<NetworkAddress>> Net2::resolveTCPEndpointWithDNSCache(const std::string& host,
                                                                         const std::string& service) {
	if (FLOW_KNOBS->ENABLE_COORDINATOR_DNS_CACHE) {
		Optional<std::vector<NetworkAddress>> cache = dnsCache.find(host, service);
		if (cache.present()) {
			return cache.get();
		}
	}
	return resolveTCPEndpoint_impl(this, host, service);
}

std::vector<NetworkAddress> Net2::resolveTCPEndpointBlocking(const std::string& host, const std::string& service) {
	tcp::resolver tcpResolver(reactor.ios);
	try {
		auto iter = tcpResolver.resolve(host, service);
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
		if (addrs.empty()) {
			throw lookup_failed();
		}
		return addrs;
	} catch (...) {
		dnsCache.remove(host, service);
		throw lookup_failed();
	}
}

std::vector<NetworkAddress> Net2::resolveTCPEndpointBlockingWithDNSCache(const std::string& host,
                                                                         const std::string& service) {
	if (FLOW_KNOBS->ENABLE_COORDINATOR_DNS_CACHE) {
		Optional<std::vector<NetworkAddress>> cache = dnsCache.find(host, service);
		if (cache.present()) {
			return cache.get();
		}
	}
	return resolveTCPEndpointBlocking(host, service);
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
		if (localAddr.isTLS()) {
			initTLS(ETLSInitState::LISTEN);
			return Reference<IListener>(new SSLListener(reactor.ios, &this->sslContextVar, localAddr));
		}
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
THREAD_HANDLE startThreadF(F&& func) {
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
	return g_network->startThread(Thing::start, t);
}

TEST_CASE("flow/Net2/ThreadSafeQueue/Interface") {
	ThreadSafeQueue<int> tq;
	ASSERT(!tq.pop().present());
	ASSERT(tq.canSleep());

	ASSERT(tq.push(1) == true);
	ASSERT(!tq.canSleep());
	ASSERT(!tq.canSleep());
	ASSERT(tq.push(2) == false);
	ASSERT(tq.push(3) == false);

	ASSERT(tq.pop().get() == 1);
	ASSERT(tq.pop().get() == 2);
	ASSERT(tq.push(4) == false);
	ASSERT(tq.pop().get() == 3);
	ASSERT(tq.pop().get() == 4);
	ASSERT(!tq.pop().present());
	ASSERT(tq.canSleep());
	return Void();
}

// A helper struct used by queueing tests which use multiple threads.
struct QueueTestThreadState {
	QueueTestThreadState(int threadId, int toProduce) : threadId(threadId), toProduce(toProduce) {}
	int threadId;
	THREAD_HANDLE handle;
	int toProduce;
	int produced = 0;
	Promise<Void> doneProducing;
	int consumed = 0;

	static int valueToThreadId(int value) { return value >> 20; }
	int elementValue(int index) { return index + (threadId << 20); }
	int nextProduced() { return elementValue(produced++); }
	int nextConsumed() { return elementValue(consumed++); }
	void checkDone() {
		ASSERT_EQ(produced, toProduce);
		ASSERT_EQ(consumed, produced);
	}
};

TEST_CASE("flow/Net2/ThreadSafeQueue/Threaded") {
	// Uses ThreadSafeQueue from multiple threads. Verifies that all pushed elements are popped, maintaining the
	// ordering within a thread.
	noUnseed = true; // multi-threading inherently non-deterministic

	ThreadSafeQueue<int> queue;
	state std::vector<QueueTestThreadState> perThread = { QueueTestThreadState(0, 1000000),
		                                                  QueueTestThreadState(1, 100000),
		                                                  QueueTestThreadState(2, 1000000) };
	state std::vector<Future<Void>> doneProducing;

	int total = 0;
	for (int t = 0; t < perThread.size(); ++t) {
		auto& s = perThread[t];
		doneProducing.push_back(s.doneProducing.getFuture());
		total += s.toProduce;
		s.handle = startThreadF([&queue, &s]() {
			printf("Thread%d\n", s.threadId);
			int nextYield = 0;
			while (s.produced < s.toProduce) {
				queue.push(s.nextProduced());
				if (nextYield-- == 0) {
					std::this_thread::yield();
					nextYield = nondeterministicRandom()->randomInt(0, 100);
				}
			}
			printf("T%dDone\n", s.threadId);
			s.doneProducing.send(Void());
		});
	}
	int consumed = 0;
	while (consumed < total) {
		Optional<int> element = queue.pop();
		if (element.present()) {
			int v = element.get();
			auto& s = perThread[QueueTestThreadState::valueToThreadId(v)];
			++consumed;
			ASSERT(v == s.nextConsumed());
		} else {
			std::this_thread::yield();
		}
		if ((consumed & 3) == 0)
			queue.canSleep();
	}

	wait(waitForAll(doneProducing));

	// Make sure we continue on the main thread.
	Promise<Void> signal;
	state Future<Void> doneConsuming = signal.getFuture();
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
	wait(doneConsuming);

	for (int t = 0; t < perThread.size(); ++t) {
		waitThread(perThread[t].handle);
		perThread[t].checkDone();
	}
	return Void();
}

TEST_CASE("noSim/flow/Net2/onMainThreadFIFO") {
	// Verifies that signals processed by onMainThread() are executed in order.
	noUnseed = true; // multi-threading inherently non-deterministic

	state std::vector<QueueTestThreadState> perThread = { QueueTestThreadState(0, 1000000),
		                                                  QueueTestThreadState(1, 100000),
		                                                  QueueTestThreadState(2, 1000000) };
	state std::vector<Future<Void>> doneProducing;
	for (int t = 0; t < perThread.size(); ++t) {
		auto& s = perThread[t];
		doneProducing.push_back(s.doneProducing.getFuture());
		s.handle = startThreadF([&s]() {
			int nextYield = 0;
			while (s.produced < s.toProduce) {
				if (nextYield-- == 0) {
					std::this_thread::yield();
					nextYield = nondeterministicRandom()->randomInt(0, 100);
				}
				int v = s.nextProduced();
				onMainThreadVoid([&s, v]() { ASSERT_EQ(v, s.nextConsumed()); });
			}
			s.doneProducing.send(Void());
		});
	}
	wait(waitForAll(doneProducing));

	// Wait for one more onMainThread to wait for all scheduled signals to be executed.
	Promise<Void> signal;
	state Future<Void> doneConsuming = signal.getFuture();
	g_network->onMainThread(std::move(signal), TaskPriority::DefaultOnMainThread);
	wait(doneConsuming);

	for (int t = 0; t < perThread.size(); ++t) {
		waitThread(perThread[t].handle);
		perThread[t].checkDone();
	}
	return Void();
}

void net2_test() {
	/*
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
	        req.key = "Foobar"_sr;

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
