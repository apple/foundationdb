/*
 * Net2.actor.cpp
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
#include "boost/bind.hpp"
#include "boost/date_time/posix_time/posix_time_types.hpp"
#include "flow/network.h"
#include "flow/IThreadPool.h"
#include "boost/range.hpp"

#include "flow/ActorCollection.h"
#include "flow/ThreadSafeQueue.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/TDMetric.actor.h"
#include "flow/AsioReactor.h"
#include "flow/Profiler.h"
#include "flow/ProtocolVersion.h"
#include "flow/TLSPolicy.h"

#ifdef WIN32
#include <mmsystem.h>
#endif
#include "flow/actorcompiler.h"  // This must be the last #include.

// Defined to track the stack limit
extern "C" intptr_t g_stackYieldLimit;
intptr_t g_stackYieldLimit = 0;

using namespace boost::asio::ip;

#if defined(__linux__)
#include <execinfo.h>

std::atomic<int64_t> net2liveness(0);

volatile size_t net2backtraces_max = 10000;
volatile void** volatile net2backtraces = NULL;
volatile size_t net2backtraces_offset = 0;
volatile bool net2backtraces_overflow = false;
volatile int net2backtraces_count = 0;

volatile void **other_backtraces = NULL;
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
	int64_t clocks; //clocks
	int64_t duration; // ns
	int64_t priority; // priority level
	int64_t numYields; // count
};

namespace N2 {  // No indent, it's the whole file

class Net2;
class Peer;
class Connection;

Net2 *g_net2 = 0;

class Task {
public:
	virtual void operator()() = 0;
};

struct OrderedTask {
	int64_t priority;
	TaskPriority taskID;
	Task *task;
	OrderedTask(int64_t priority, TaskPriority taskID, Task* task) : priority(priority), taskID(taskID), task(task) {}
	bool operator < (OrderedTask const& rhs) const { return priority < rhs.priority; }
};

thread_local INetwork* thread_network = 0;

class Net2 sealed : public INetwork, public INetworkConnections {

public:
	Net2(bool useThreadPool, bool useMetrics, Reference<TLSPolicy> tlsPolicy, const TLSParams& tlsParams);
	void initTLS();
	void run();
	void initMetrics();

	// INetworkConnections interface
	virtual Future<Reference<IConnection>> connect( NetworkAddress toAddr, std::string host );
	virtual Future<std::vector<NetworkAddress>> resolveTCPEndpoint( std::string host, std::string service);
	virtual Reference<IListener> listen( NetworkAddress localAddr );

	// INetwork interface
	virtual double now() { return currentTime; };
	virtual double timer() { return ::timer(); };
	virtual Future<Void> delay( double seconds, TaskPriority taskId );
	virtual Future<class Void> yield( TaskPriority taskID );
	virtual bool check_yield(TaskPriority taskId);
	virtual TaskPriority getCurrentTask() { return currentTaskID; }
	virtual void setCurrentTask(TaskPriority taskID ) { currentTaskID = taskID; priorityMetric = (int64_t)taskID; }
	virtual void onMainThread( Promise<Void>&& signal, TaskPriority taskID );
	bool isOnMainThread() const override {
		return thread_network == this;
	}
	virtual void stop() {
		if ( thread_network == this )
			stopImmediately();
		else
			// SOMEDAY: NULL for deferred error, no analysis of correctness (itp)
			onMainThreadVoid( [this] { this->stopImmediately(); }, NULL );
	}

	virtual bool isSimulated() const { return false; }
	virtual THREAD_HANDLE startThread( THREAD_FUNC_RETURN (*func) (void*), void *arg);

	virtual void getDiskBytes( std::string const& directory, int64_t& free, int64_t& total );
	virtual bool isAddressOnThisHost( NetworkAddress const& addr );
	void updateNow(){ currentTime = timer_monotonic(); }

	virtual flowGlobalType global(int id) { return (globals.size() > id) ? globals[id] : NULL; }
	virtual void setGlobal(size_t id, flowGlobalType v) { globals.resize(std::max(globals.size(),id+1)); globals[id] = v; }
	std::vector<flowGlobalType>		globals;

	bool useThreadPool;
//private:

	ASIOReactor reactor;
#ifndef TLS_DISABLED
	boost::asio::ssl::context sslContext;
#endif
	Reference<TLSPolicy> tlsPolicy;
	TLSParams tlsParams;
	bool tlsInitialized;

	std::string get_password() const {
		return tlsParams.tlsPassword;
	}

	INetworkConnections *network;  // initially this, but can be changed

	int64_t tsc_begin, tsc_end;
	double taskBegin;
	TaskPriority currentTaskID;
	uint64_t tasksIssued;
	TDMetricCollection tdmetrics;
	double currentTime;
	bool stopped;
	std::map<IPAddress, bool> addressOnHostCache;

	uint64_t numYields;

	TaskPriority lastMinTaskID;

	std::priority_queue<OrderedTask, std::vector<OrderedTask>> ready;
	ThreadSafeQueue<OrderedTask> threadReady;

	struct DelayedTask : OrderedTask {
		double at;
		DelayedTask(double at, int64_t priority, TaskPriority taskID, Task* task) : at(at), OrderedTask(priority, taskID, task) {}
		bool operator < (DelayedTask const& rhs) const { return at > rhs.at; } // Ordering is reversed for priority_queue
	};
	std::priority_queue<DelayedTask, std::vector<DelayedTask>> timers;

	void checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, TaskPriority priority);
	bool check_yield(TaskPriority taskId, bool isRunLoop);
	void processThreadReady();
	void trackMinPriority( TaskPriority minTaskID, double now );
	void stopImmediately() {
		stopped=true; decltype(ready) _1; ready.swap(_1); decltype(timers) _2; timers.swap(_2);
	}

	Future<Void> timeOffsetLogger;
	Future<Void> logTimeOffset();

	Int64MetricHandle bytesReceived;
	Int64MetricHandle countWriteProbes;
	Int64MetricHandle countReadProbes;
	Int64MetricHandle countReads;
	Int64MetricHandle countWouldBlock;
	Int64MetricHandle countWrites;
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
	Int64MetricHandle countSlowTaskSignals;
	Int64MetricHandle priorityMetric;
	DoubleMetricHandle countLaunchTime;
	DoubleMetricHandle countReactTime;
	BoolMetricHandle awakeMetric;

	EventMetricHandle<SlowTask> slowTaskMetric;

	std::vector<std::string> blobCredentialFiles;
};

static boost::asio::ip::address tcpAddress(IPAddress const& n) {
	if (n.isV6()) {
		return boost::asio::ip::address_v6(n.toV6());
	} else {
		return boost::asio::ip::address_v4(n.toV4());
	}
}

static tcp::endpoint tcpEndpoint( NetworkAddress const& n ) {
	return tcp::endpoint(tcpAddress(n.ip), n.port);
}

class BindPromise {
	Promise<Void> p;
	const char* errContext;
	UID errID;
public:
	BindPromise( const char* errContext, UID errID ) : errContext(errContext), errID(errID) {}
	BindPromise( BindPromise const& r ) : p(r.p), errContext(r.errContext), errID(r.errID) {}
	BindPromise(BindPromise&& r) BOOST_NOEXCEPT : p(std::move(r.p)), errContext(r.errContext), errID(r.errID) {}

	Future<Void> getFuture() { return p.getFuture(); }

	void operator()( const boost::system::error_code& error, size_t bytesWritten=0 ) {
		try {
			if (error) {
				// Log the error...
				TraceEvent(SevWarn, errContext, errID).suppressFor(1.0).detail("ErrorCode", error.value()).detail("Message", error.message())
#ifndef TLS_DISABLED
				.detail("WhichMeans", TLSPolicy::ErrorString(error))
#endif
				;
				p.sendError( connection_failed() );
			} else
				p.send( Void() );
		} catch (Error& e) {
			p.sendError(e);
		} catch (...) {
			p.sendError(unknown_error());
		}
	}
};

class Connection : public IConnection, ReferenceCounted<Connection> {
public:
	virtual void addref() { ReferenceCounted<Connection>::addref(); }
	virtual void delref() { ReferenceCounted<Connection>::delref(); }

	virtual void close() {
		closeSocket();
	}

	explicit Connection( boost::asio::io_service& io_service )
		: id(nondeterministicRandom()->randomUniqueID()), socket(io_service)
	{
	}

	// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
	ACTOR static Future<Reference<IConnection>> connect( boost::asio::io_service* ios, NetworkAddress addr ) {
		state Reference<Connection> self( new Connection(*ios) );

		self->peer_address = addr;
		try {
			auto to = tcpEndpoint(addr);
			BindPromise p("N2_ConnectError", self->id);
			Future<Void> onConnected = p.getFuture();
			self->socket.async_connect( to, std::move(p) );

			wait( onConnected );
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

	virtual Future<Void> acceptHandshake() { return Void(); }

	virtual Future<Void> connectHandshake() { return Void(); }

	// returns when write() can write at least one byte
	virtual Future<Void> onWritable() {
		++g_net2->countWriteProbes;
		BindPromise p("N2_WriteProbeError", id);
		auto f = p.getFuture();
		socket.async_write_some( boost::asio::null_buffers(), std::move(p) );
		return f;
	}

	// returns when read() can read at least one byte
	virtual Future<Void> onReadable() {
		++g_net2->countReadProbes;
		BindPromise p("N2_ReadProbeError", id);
		auto f = p.getFuture();
		socket.async_read_some( boost::asio::null_buffers(), std::move(p) );
		return f;
	}

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might be 0)
	virtual int read( uint8_t* begin, uint8_t* end ) {
		boost::system::error_code err;
		++g_net2->countReads;
		size_t toRead = end-begin;
		size_t size = socket.read_some( boost::asio::mutable_buffers_1(begin, toRead), err );
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
		ASSERT( size );  // If the socket is closed, we expect an 'eof' error, not a zero return value

		return size;
	}

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of bytes written (might be 0)
	virtual int write( SendBuffer const* data, int limit ) {
		boost::system::error_code err;
		++g_net2->countWrites;

		size_t sent = socket.write_some( boost::iterator_range<SendBufferIterator>(SendBufferIterator(data, limit), SendBufferIterator()), err );

		if (err) {
			// Since there was an error, sent's value can't be used to infer that the buffer has data and the limit is positive so check explicitly.
			ASSERT(limit > 0);
			bool notEmpty = false;
			for(auto p = data; p; p = p->next)
				if(p->bytes_written - p->bytes_sent > 0) {
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

		ASSERT( sent );  // Make sure data was sent, and also this check will fail if the buffer chain was empty or the limit was not > 0.
		return sent;
	}

	virtual NetworkAddress getPeerAddress() { return peer_address; }

	virtual UID getDebugID() { return id; }

	tcp::socket& getSocket() { return socket; }
private:
	UID id;
	tcp::socket socket;
	NetworkAddress peer_address;

	struct SendBufferIterator {
		typedef boost::asio::const_buffer value_type;
		typedef std::forward_iterator_tag iterator_category;
		typedef size_t difference_type;
		typedef boost::asio::const_buffer* pointer;
		typedef boost::asio::const_buffer& reference;

		SendBuffer const* p;
		int limit;

		SendBufferIterator(SendBuffer const* p=0, int limit = std::numeric_limits<int>::max()) : p(p), limit(limit) {
			ASSERT(limit > 0);
		}

		bool operator == (SendBufferIterator const& r) const { return p == r.p; }
		bool operator != (SendBufferIterator const& r) const { return p != r.p; }
		void operator++() {
			limit -= p->bytes_written - p->bytes_sent;
			if(limit > 0)
				p = p->next;
			else
				p = NULL;
		}

		boost::asio::const_buffer operator*() const {
			return boost::asio::const_buffer( p->data + p->bytes_sent, std::min(limit, p->bytes_written - p->bytes_sent) );
		}
	};

	void init() {
		// Socket settings that have to be set after connect or accept succeeds
		socket.non_blocking(true);
		socket.set_option(boost::asio::ip::tcp::no_delay(true));
		platform::setCloseOnExec(socket.native_handle());
	}

	void closeSocket() {
		boost::system::error_code error;
		socket.close(error);
		if (error)
			TraceEvent(SevWarn, "N2_CloseError", id).suppressFor(1.0).detail("ErrorCode", error.value()).detail("Message", error.message());
	}

	void onReadError( const boost::system::error_code& error ) {
		TraceEvent(SevWarn, "N2_ReadError", id).suppressFor(1.0).detail("ErrorCode", error.value()).detail("Message", error.message());
		closeSocket();
	}
	void onWriteError( const boost::system::error_code& error ) {
		TraceEvent(SevWarn, "N2_WriteError", id).suppressFor(1.0).detail("ErrorCode", error.value()).detail("Message", error.message());
		closeSocket();
	}
};

class Listener : public IListener, ReferenceCounted<Listener> {
	NetworkAddress listenAddress;
	tcp::acceptor acceptor;

public:
	Listener( boost::asio::io_service& io_service, NetworkAddress listenAddress )
		: listenAddress(listenAddress), acceptor( io_service, tcpEndpoint( listenAddress ) )
	{
		platform::setCloseOnExec(acceptor.native_handle());
	}

	virtual void addref() { ReferenceCounted<Listener>::addref(); }
	virtual void delref() { ReferenceCounted<Listener>::delref(); }

	// Returns one incoming connection when it is available
	virtual Future<Reference<IConnection>> accept() {
		return doAccept( this );
	}

	virtual NetworkAddress getListenAddress() { return listenAddress; }

private:
	ACTOR static Future<Reference<IConnection>> doAccept( Listener* self ) {
		state Reference<Connection> conn( new Connection( self->acceptor.get_io_service() ) );
		state tcp::acceptor::endpoint_type peer_endpoint;
		try {
			BindPromise p("N2_AcceptError", UID());
			auto f = p.getFuture();
			self->acceptor.async_accept( conn->getSocket(), peer_endpoint, std::move(p) );
			wait( f );
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

class SSLConnection : public IConnection, ReferenceCounted<SSLConnection> {
public:
	virtual void addref() { ReferenceCounted<SSLConnection>::addref(); }
	virtual void delref() { ReferenceCounted<SSLConnection>::delref(); }

	virtual void close() {
		closeSocket();
	}

	explicit SSLConnection( boost::asio::io_service& io_service, boost::asio::ssl::context& context )
		: id(nondeterministicRandom()->randomUniqueID()), socket(io_service), ssl_sock(socket, context)
	{
	}

	// This is not part of the IConnection interface, because it is wrapped by INetwork::connect()
	ACTOR static Future<Reference<IConnection>> connect( boost::asio::io_service* ios, boost::asio::ssl::context* context, NetworkAddress addr ) {
		std::pair<IPAddress,uint16_t> peerIP = std::make_pair(addr.ip, addr.port);
		auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
		if(iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
			if (now() < iter->second.second) {
				if(iter->second.first >= FLOW_KNOBS->TLS_CLIENT_CONNECTION_THROTTLE_ATTEMPTS) {
					TraceEvent("TLSOutgoingConnectionThrottlingWarning").suppressFor(1.0).detail("PeerIP", addr);
					wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT));
					throw connection_failed();
				}
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler.erase(peerIP);
			}
		}
		
		state Reference<SSLConnection> self( new SSLConnection(*ios, *context) );
		self->peer_address = addr;
		
		try {
			auto to = tcpEndpoint(self->peer_address);
			BindPromise p("N2_ConnectError", self->id);
			Future<Void> onConnected = p.getFuture();
			self->socket.async_connect( to, std::move(p) );

			wait( onConnected );
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

	ACTOR static void doAcceptHandshake( Reference<SSLConnection> self, Promise<Void> connected) {
		try {
			state std::pair<IPAddress,uint16_t> peerIP = std::make_pair(self->getPeerAddress().ip, static_cast<uint16_t>(0));
			auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
			if(iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
				if (now() < iter->second.second) {
					if(iter->second.first >= FLOW_KNOBS->TLS_SERVER_CONNECTION_THROTTLE_ATTEMPTS) {
						TraceEvent("TLSIncomingConnectionThrottlingWarning").suppressFor(1.0).detail("PeerIP", peerIP.first.toString());
						wait(delay(FLOW_KNOBS->CONNECTION_MONITOR_TIMEOUT));
						self->closeSocket();
						connected.sendError(connection_failed());
						return;
					}
				} else {
					g_network->networkInfo.serverTLSConnectionThrottler.erase(peerIP);
				}
			}

			int64_t permitNumber = wait(g_network->networkInfo.handshakeLock->take());
			state BoundedFlowLock::Releaser releaser(g_network->networkInfo.handshakeLock, permitNumber);

			BindPromise p("N2_AcceptHandshakeError", UID());
			auto onHandshook = p.getFuture();
			self->getSSLSocket().async_handshake( boost::asio::ssl::stream_base::server, std::move(p) );
			wait( onHandshook );
			wait(delay(0, TaskPriority::Handshake));
			connected.send(Void());
		} catch (...) {
			auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
			if(iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
				iter->second.first++;
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler[peerIP] = std::make_pair(0,now() + FLOW_KNOBS->TLS_SERVER_CONNECTION_THROTTLE_TIMEOUT);
			}
			self->closeSocket();
			connected.sendError(connection_failed());
		}
	}

	ACTOR static Future<Void> acceptHandshakeWrapper( Reference<SSLConnection> self ) {
		Promise<Void> connected;
		doAcceptHandshake(self, connected);
		try {
			wait(connected.getFuture());
			return Void();
		} catch (Error& e) {
			// Either the connection failed, or was cancelled by the caller
			self->closeSocket();
			throw;
		}
	}

	virtual Future<Void> acceptHandshake() { 
		return acceptHandshakeWrapper( Reference<SSLConnection>::addRef(this) );
	}

	ACTOR static void doConnectHandshake( Reference<SSLConnection> self, Promise<Void> connected) {
		try {
			int64_t permitNumber = wait(g_network->networkInfo.handshakeLock->take());
			state BoundedFlowLock::Releaser releaser(g_network->networkInfo.handshakeLock, permitNumber);

			BindPromise p("N2_ConnectHandshakeError", self->id);
			Future<Void> onHandshook = p.getFuture();
			self->ssl_sock.async_handshake( boost::asio::ssl::stream_base::client, std::move(p) );
			wait( onHandshook );
			wait(delay(0, TaskPriority::Handshake));
			connected.send(Void());
		} catch (...) {
			std::pair<IPAddress,uint16_t> peerIP = std::make_pair(self->peer_address.ip, self->peer_address.port);
			auto iter(g_network->networkInfo.serverTLSConnectionThrottler.find(peerIP));
			if(iter != g_network->networkInfo.serverTLSConnectionThrottler.end()) {
				iter->second.first++;
			} else {
				g_network->networkInfo.serverTLSConnectionThrottler[peerIP] = std::make_pair(0,now() + FLOW_KNOBS->TLS_CLIENT_CONNECTION_THROTTLE_TIMEOUT);
			}
			self->closeSocket();
			connected.sendError(connection_failed());
		}
	}

	ACTOR static Future<Void> connectHandshakeWrapper( Reference<SSLConnection> self ) {
		Promise<Void> connected;
		doConnectHandshake(self, connected);
		try {
			wait(connected.getFuture());
			return Void();
		} catch (Error& e) {
			// Either the connection failed, or was cancelled by the caller
			self->closeSocket();
			throw;
		}
	}

	virtual Future<Void> connectHandshake() { 
		return connectHandshakeWrapper( Reference<SSLConnection>::addRef(this) );
	}

	// returns when write() can write at least one byte
	virtual Future<Void> onWritable() {
		++g_net2->countWriteProbes;
		BindPromise p("N2_WriteProbeError", id);
		auto f = p.getFuture();
		socket.async_write_some( boost::asio::null_buffers(), std::move(p) );
		return f;
	}

	// returns when read() can read at least one byte
	virtual Future<Void> onReadable() {
		++g_net2->countReadProbes;
		BindPromise p("N2_ReadProbeError", id);
		auto f = p.getFuture();
		socket.async_read_some( boost::asio::null_buffers(), std::move(p) );
		return f;
	}

	// Reads as many bytes as possible from the read buffer into [begin,end) and returns the number of bytes read (might be 0)
	virtual int read( uint8_t* begin, uint8_t* end ) {
		boost::system::error_code err;
		++g_net2->countReads;
		size_t toRead = end-begin;
		size_t size = ssl_sock.read_some( boost::asio::mutable_buffers_1(begin, toRead), err );
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
		ASSERT( size );  // If the socket is closed, we expect an 'eof' error, not a zero return value

		return size;
	}

	// Writes as many bytes as possible from the given SendBuffer chain into the write buffer and returns the number of bytes written (might be 0)
	virtual int write( SendBuffer const* data, int limit ) {
		boost::system::error_code err;
		++g_net2->countWrites;

		size_t sent = ssl_sock.write_some( boost::iterator_range<SendBufferIterator>(SendBufferIterator(data, limit), SendBufferIterator()), err );

		if (err) {
			// Since there was an error, sent's value can't be used to infer that the buffer has data and the limit is positive so check explicitly.
			ASSERT(limit > 0);
			bool notEmpty = false;
			for(auto p = data; p; p = p->next)
				if(p->bytes_written - p->bytes_sent > 0) {
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

		ASSERT( sent );  // Make sure data was sent, and also this check will fail if the buffer chain was empty or the limit was not > 0.
		return sent;
	}

	virtual NetworkAddress getPeerAddress() { return peer_address; }

	virtual UID getDebugID() { return id; }

	tcp::socket& getSocket() { return socket; }

	ssl_socket& getSSLSocket() { return ssl_sock; }
private:
	UID id;
	tcp::socket socket;
	ssl_socket ssl_sock;
	NetworkAddress peer_address;

	struct SendBufferIterator {
		typedef boost::asio::const_buffer value_type;
		typedef std::forward_iterator_tag iterator_category;
		typedef size_t difference_type;
		typedef boost::asio::const_buffer* pointer;
		typedef boost::asio::const_buffer& reference;

		SendBuffer const* p;
		int limit;

		SendBufferIterator(SendBuffer const* p=0, int limit = std::numeric_limits<int>::max()) : p(p), limit(limit) {
			ASSERT(limit > 0);
		}

		bool operator == (SendBufferIterator const& r) const { return p == r.p; }
		bool operator != (SendBufferIterator const& r) const { return p != r.p; }
		void operator++() {
			limit -= p->bytes_written - p->bytes_sent;
			if(limit > 0)
				p = p->next;
			else
				p = NULL;
		}

		boost::asio::const_buffer operator*() const {
			return boost::asio::const_buffer( p->data + p->bytes_sent, std::min(limit, p->bytes_written - p->bytes_sent) );
		}
	};

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

	void onReadError( const boost::system::error_code& error ) {
		TraceEvent(SevWarn, "N2_ReadError", id).suppressFor(1.0).detail("Message", error.value());
		closeSocket();
	}
	void onWriteError( const boost::system::error_code& error ) {
		TraceEvent(SevWarn, "N2_WriteError", id).suppressFor(1.0).detail("Message", error.value());
		closeSocket();
	}
};

class SSLListener : public IListener, ReferenceCounted<SSLListener> {
	NetworkAddress listenAddress;
	tcp::acceptor acceptor;
	boost::asio::ssl::context* context;

public:
	SSLListener( boost::asio::io_service& io_service, boost::asio::ssl::context* context, NetworkAddress listenAddress )
		: listenAddress(listenAddress), acceptor( io_service, tcpEndpoint( listenAddress ) ), context(context)
	{
		platform::setCloseOnExec(acceptor.native_handle());
	}

	virtual void addref() { ReferenceCounted<SSLListener>::addref(); }
	virtual void delref() { ReferenceCounted<SSLListener>::delref(); }

	// Returns one incoming connection when it is available
	virtual Future<Reference<IConnection>> accept() {
		return doAccept( this );
	}

	virtual NetworkAddress getListenAddress() { return listenAddress; }

private:
	ACTOR static Future<Reference<IConnection>> doAccept( SSLListener* self ) {
		state Reference<SSLConnection> conn( new SSLConnection( self->acceptor.get_io_service(), *self->context) );
		state tcp::acceptor::endpoint_type peer_endpoint;
		try {
			BindPromise p("N2_AcceptError", UID());
			auto f = p.getFuture();
			self->acceptor.async_accept( conn->getSocket(), peer_endpoint, std::move(p) );
			wait( f );
			auto peer_address = peer_endpoint.address().is_v6() ? IPAddress(peer_endpoint.address().to_v6().to_bytes()) : IPAddress(peer_endpoint.address().to_v4().to_ulong());
			
			conn->accept(NetworkAddress(peer_address, peer_endpoint.port(), false, true));

			return conn;
		} catch (...) {
			conn->close();
			throw;
		}
	}
};
#endif

struct PromiseTask : public Task, public FastAllocated<PromiseTask> {
	Promise<Void> promise;
	PromiseTask() {}
	explicit PromiseTask( Promise<Void>&& promise ) BOOST_NOEXCEPT : promise(std::move(promise)) {}

	virtual void operator()() {
		promise.send(Void());
		delete this;
	}
};

// 5MB for loading files into memory

#ifndef TLS_DISABLED
bool insecurely_always_accept(bool _1, boost::asio::ssl::verify_context& _2) {
	return true;
}
#endif

Net2::Net2(bool useThreadPool, bool useMetrics, Reference<TLSPolicy> tlsPolicy, const TLSParams& tlsParams)
	: useThreadPool(useThreadPool),
	  network(this),
	  reactor(this),
	  stopped(false),
	  tasksIssued(0),
	  // Until run() is called, yield() will always yield
	  tsc_begin(0), tsc_end(0), taskBegin(0), currentTaskID(TaskPriority::DefaultYield),
	  lastMinTaskID(TaskPriority::Zero),
	  numYields(0),
	  tlsInitialized(false),
	  tlsPolicy(tlsPolicy),
	  tlsParams(tlsParams)
#ifndef TLS_DISABLED
	  ,sslContext(boost::asio::ssl::context(boost::asio::ssl::context::tlsv12))
#endif

{
	TraceEvent("Net2Starting");

	// Set the global members
	if(useMetrics) {
		setGlobal(INetwork::enTDMetrics, (flowGlobalType) &tdmetrics);
	}
	setGlobal(INetwork::enNetworkConnections, (flowGlobalType) network);
	setGlobal(INetwork::enASIOService, (flowGlobalType) &reactor.ios);
	setGlobal(INetwork::enBlobCredentialFiles, &blobCredentialFiles);

#ifdef __linux__
	setGlobal(INetwork::enEventFD, (flowGlobalType) N2::ASIOReactor::newEventFD(reactor));
#endif


	int priBins[] = { 1, 2050, 3050, 4050, 4950, 5050, 7050, 8050, 10050 };
	static_assert( sizeof(priBins) == sizeof(int)*NetworkMetrics::PRIORITY_BINS, "Fix priority bins");
	for(int i=0; i<NetworkMetrics::PRIORITY_BINS; i++)
		networkInfo.metrics.priorityBins[i] = static_cast<TaskPriority>(priBins[i]);
	updateNow();

}

void Net2::initTLS() {
	if(tlsInitialized) {
		return;
	}
#ifndef TLS_DISABLED
	try {
		const char *defaultCertFileName = "fdb.pem";

		if( tlsPolicy && !tlsPolicy->rules.size() ) {
			std::string verify_peers;
			if (platform::getEnvironmentVar("FDB_TLS_VERIFY_PEERS", verify_peers)) {
				tlsPolicy->set_verify_peers({ verify_peers });
			} else {
				tlsPolicy->set_verify_peers({ std::string("Check.Valid=1")});
			}
		}

		sslContext.set_options(boost::asio::ssl::context::default_workarounds);
		sslContext.set_verify_mode(boost::asio::ssl::context::verify_peer | boost::asio::ssl::verify_fail_if_no_peer_cert);
		if (tlsPolicy) {
			Reference<TLSPolicy> policy = tlsPolicy;
			sslContext.set_verify_callback([policy](bool preverified, boost::asio::ssl::verify_context& ctx) {
				return policy->verify_peer(preverified, ctx.native_handle());
			});
		} else {
			sslContext.set_verify_callback(boost::bind(&insecurely_always_accept, _1, _2));
		}

		if ( !tlsParams.tlsPassword.size() ) {
			platform::getEnvironmentVar( "FDB_TLS_PASSWORD", tlsParams.tlsPassword );
		}
		sslContext.set_password_callback(std::bind(&Net2::get_password, this));

		if ( tlsParams.tlsCertBytes.size() ) {
			sslContext.use_certificate_chain(boost::asio::buffer(tlsParams.tlsCertBytes.data(), tlsParams.tlsCertBytes.size()));
		}
		else {
			if ( !tlsParams.tlsCertPath.size() ) {
				if ( !platform::getEnvironmentVar( "FDB_TLS_CERTIFICATE_FILE", tlsParams.tlsCertPath ) ) {
					if( fileExists(defaultCertFileName) ) {
						tlsParams.tlsCertPath = defaultCertFileName;
					} else if( fileExists( joinPath(platform::getDefaultConfigPath(), defaultCertFileName) ) ) {
						tlsParams.tlsCertPath = joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
					}
				}
			}
			if ( tlsParams.tlsCertPath.size() ) {
				sslContext.use_certificate_chain_file(tlsParams.tlsCertPath);
			}
		}

		if ( tlsParams.tlsCABytes.size() ) {
			sslContext.add_certificate_authority(boost::asio::buffer(tlsParams.tlsCABytes.data(), tlsParams.tlsCABytes.size()));
		}
		else {
			if ( !tlsParams.tlsCAPath.size() ) {
				platform::getEnvironmentVar("FDB_TLS_CA_FILE", tlsParams.tlsCAPath);
			}
			if ( tlsParams.tlsCAPath.size() ) {
				try {
					std::string cert = readFileBytes(tlsParams.tlsCAPath, FLOW_KNOBS->CERT_FILE_MAX_SIZE);
					sslContext.add_certificate_authority(boost::asio::buffer(cert.data(), cert.size()));
				}
				catch (Error& e) {
					fprintf(stderr, "Error reading CA file %s: %s\n", tlsParams.tlsCAPath.c_str(), e.what());
					TraceEvent("Net2TLSReadCAError").error(e);
					throw tls_error();
				}
			}
		}

		if (tlsParams.tlsKeyBytes.size()) {
			sslContext.use_private_key(boost::asio::buffer(tlsParams.tlsKeyBytes.data(), tlsParams.tlsKeyBytes.size()), boost::asio::ssl::context::pem);
		} else {
			if (!tlsParams.tlsKeyPath.size()) {
				if(!platform::getEnvironmentVar( "FDB_TLS_KEY_FILE", tlsParams.tlsKeyPath)) {
					if( fileExists(defaultCertFileName) ) {
						tlsParams.tlsKeyPath = defaultCertFileName;
					} else if( fileExists( joinPath(platform::getDefaultConfigPath(), defaultCertFileName) ) ) {
						tlsParams.tlsKeyPath = joinPath(platform::getDefaultConfigPath(), defaultCertFileName);
					}
				}
			}
			if (tlsParams.tlsKeyPath.size()) {
				sslContext.use_private_key_file(tlsParams.tlsKeyPath, boost::asio::ssl::context::pem);
			}
		}
	} catch(boost::system::system_error e) {
		fprintf(stderr, "Error initializing TLS: %s\n", e.what());
		TraceEvent("Net2TLSInitError").detail("Message", e.what());
		throw tls_error();
	}
#endif
	tlsInitialized = true;
}

ACTOR Future<Void> Net2::logTimeOffset() {
	loop {
		double processTime = timer_monotonic();
		double systemTime = timer();
		TraceEvent("ProcessTimeOffset").detailf("ProcessTime", "%lf", processTime).detailf("SystemTime", "%lf", systemTime).detailf("OffsetFromSystemTime", "%lf", processTime - systemTime);
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
	countSlowTaskSignals.init(LiteralStringRef("Net2.CountSlowTaskSignals"));
	priorityMetric.init(LiteralStringRef("Net2.Priority"));
	awakeMetric.init(LiteralStringRef("Net2.Awake"));
	slowTaskMetric.init(LiteralStringRef("Net2.SlowTask"));
	countLaunchTime.init(LiteralStringRef("Net2.CountLaunchTime"));
	countReactTime.init(LiteralStringRef("Net2.CountReactTime"));
}

void Net2::run() {
	TraceEvent::setNetworkThread();
	TraceEvent("Net2Running");

	thread_network = this;

#ifdef WIN32
	if (timeBeginPeriod(1) != TIMERR_NOERROR)
		TraceEvent(SevError, "TimeBeginPeriodError");
#endif

	timeOffsetLogger = logTimeOffset();
	const char *flow_profiler_enabled = getenv("FLOW_PROFILER_ENABLED");
	if (flow_profiler_enabled != nullptr && *flow_profiler_enabled != '\0') {
		// The empty string check is to allow running `FLOW_PROFILER_ENABLED= ./fdbserver` to force disabling flow profiling at startup.
		startProfiling(this);
	}

	// Get the address to the launch function
	typedef void (*runCycleFuncPtr)();
	runCycleFuncPtr runFunc = reinterpret_cast<runCycleFuncPtr>(reinterpret_cast<flowGlobalType>(g_network->global(INetwork::enRunCycleFunc)));

	double nnow = timer_monotonic();

	while(!stopped) {
		FDB_TRACE_PROBE(run_loop_begin);
		++countRunLoop;

		if (runFunc) {
			tsc_begin = __rdtsc();
			taskBegin = nnow;
			trackMinPriority(TaskPriority::RunCycleFunction, taskBegin);
			runFunc();
			double taskEnd = timer_monotonic();
			countLaunchTime += taskEnd - taskBegin;
			checkForSlowTask(tsc_begin, __rdtsc(), taskEnd - taskBegin, TaskPriority::RunCycleFunction);
		}

		double sleepTime = 0;
		bool b = ready.empty();
		if (b) {
			b = threadReady.canSleep();
			if (!b) ++countCantSleep;
		} else
			++countWontSleep;
		if (b) {
			sleepTime = 1e99;
			double sleepStart = timer_monotonic();
			if (!timers.empty()) {
				sleepTime = timers.top().at - sleepStart;  // + 500e-6?
			}
			if (sleepTime > 0) {
				trackMinPriority(TaskPriority::Zero, sleepStart);
				awakeMetric = false;
				priorityMetric = 0;
				reactor.sleep(sleepTime);
				awakeMetric = true;
			}
		}

		tsc_begin = __rdtsc();
		taskBegin = timer_monotonic();
		trackMinPriority(TaskPriority::ASIOReactor, taskBegin);
		reactor.react();
		
		updateNow();
		double now = this->currentTime;

		countReactTime += now - taskBegin;
		checkForSlowTask(tsc_begin, __rdtsc(), now - taskBegin, TaskPriority::ASIOReactor);

		if ((now-nnow) > FLOW_KNOBS->SLOW_LOOP_CUTOFF && nondeterministicRandom()->random01() < (now-nnow)*FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopTop").detail("Elapsed", now - nnow);

		int numTimers = 0;
		while (!timers.empty() && timers.top().at < now) {
			++numTimers;
			++countTimers;
			ready.push( timers.top() );
			timers.pop();
		}
		countTimers += numTimers;
		FDB_TRACE_PROBE(run_loop_ready_timers, numTimers);

		processThreadReady();

		tsc_begin = __rdtsc();
		tsc_end = tsc_begin + FLOW_KNOBS->TSC_YIELD_TIME;
		taskBegin = timer_monotonic();
		numYields = 0;
		TaskPriority minTaskID = TaskPriority::Max;
		int queueSize = ready.size();

		FDB_TRACE_PROBE(run_loop_tasks_start, queueSize);
		while (!ready.empty()) {
			++countTasks;
			currentTaskID = ready.top().taskID;
			priorityMetric = static_cast<int64_t>(currentTaskID);
			minTaskID = std::min(minTaskID, currentTaskID);
			Task* task = ready.top().task;
			ready.pop();

			try {
				(*task)();
			} catch (Error& e) {
				TraceEvent(SevError, "TaskError").error(e);
			} catch (...) {
				TraceEvent(SevError, "TaskError").error(unknown_error());
			}

			if (check_yield(TaskPriority::Max, true)) {
				FDB_TRACE_PROBE(run_loop_yield);
				++countYields;
                break;
			}
		}
		queueSize = ready.size();
		FDB_TRACE_PROBE(run_loop_done, queueSize);

		trackMinPriority(minTaskID, now);

#if defined(__linux__)
		if(FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL > 0) {
			sigset_t orig_set;
			pthread_sigmask(SIG_BLOCK, &sigprof_set, &orig_set);

			size_t other_offset = net2backtraces_offset;
			bool was_overflow = net2backtraces_overflow;
			int signal_count = net2backtraces_count;

			countSlowTaskSignals += signal_count;

			if (other_offset) {
				volatile void** _traces = net2backtraces;
				net2backtraces = other_backtraces;
				other_backtraces = _traces;

				net2backtraces_offset = 0;
			}

			net2backtraces_overflow = false;
			net2backtraces_count = 0;

			pthread_sigmask(SIG_SETMASK, &orig_set, NULL);

			if (was_overflow) {
				TraceEvent("Net2SlowTaskOverflow")
					.detail("SignalsReceived", signal_count)
					.detail("BackTraceHarvested", other_offset != 0);
			}
			if (other_offset) {
				size_t iter_offset = 0;
				while (iter_offset < other_offset) {
					ProfilingSample *ps = (ProfilingSample *)(other_backtraces + iter_offset);
					TraceEvent(SevWarn, "Net2SlowTaskTrace").detailf("TraceTime", "%.6f", ps->timestamp).detail("Trace", platform::format_backtrace(ps->frames, ps->length));
					iter_offset += ps->length + 2;
				}
			}

			// to keep the thread liveness check happy
			net2liveness.fetch_add(1);
		}
#endif
		nnow = timer_monotonic();

		if ((nnow-now) > FLOW_KNOBS->SLOW_LOOP_CUTOFF && nondeterministicRandom()->random01() < (nnow-now)*FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopBottom").detail("Elapsed", nnow - now); // This includes the time spent running tasks
	}

	#ifdef WIN32
	timeEndPeriod(1);
	#endif
}

void Net2::trackMinPriority( TaskPriority minTaskID, double now ) {
	if (minTaskID != lastMinTaskID) {
		for(int c=0; c<NetworkMetrics::PRIORITY_BINS; c++) {
			TaskPriority pri = networkInfo.metrics.priorityBins[c];
			if (pri > minTaskID && pri <= lastMinTaskID) {  // busy -> idle
				networkInfo.metrics.priorityBlocked[c] = false;
				networkInfo.metrics.priorityBlockedDuration[c] += now - networkInfo.metrics.windowedPriorityTimer[c];
				networkInfo.metrics.priorityMaxBlockedDuration[c] = std::max(networkInfo.metrics.priorityMaxBlockedDuration[c], now - networkInfo.metrics.priorityTimer[c]);
			}
			if (pri <= minTaskID && pri > lastMinTaskID) {  // idle -> busy
				networkInfo.metrics.priorityBlocked[c] = true;
				networkInfo.metrics.priorityTimer[c] = now;
				networkInfo.metrics.windowedPriorityTimer[c] = now;
			}
		}
	}

	lastMinTaskID = minTaskID;
}

void Net2::processThreadReady() {
	int numReady = 0;
	while (true) {
		Optional<OrderedTask> t = threadReady.pop();
		if (!t.present()) break;
		t.get().priority -= ++tasksIssued;
		ASSERT( t.get().task != 0 );
		ready.push( t.get() );
		++numReady;
	}
	FDB_TRACE_PROBE(run_loop_thread_ready, numReady);
}

void Net2::checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, TaskPriority priority) {
	int64_t elapsed = tscEnd-tscBegin;
	if (elapsed > FLOW_KNOBS->TSC_YIELD_TIME && tscBegin > 0) {
		int i = std::min<double>(NetworkMetrics::SLOW_EVENT_BINS-1, log( elapsed/1e6 ) / log(2.));
		++networkInfo.metrics.countSlowEvents[i];
		int64_t warnThreshold = g_network->isSimulated() ? 10e9 : 500e6;

		//printf("SlowTask: %d, %d yields\n", (int)(elapsed/1e6), numYields);

		slowTaskMetric->clocks = elapsed;
		slowTaskMetric->duration = (int64_t)(duration*1e9);
		slowTaskMetric->priority = static_cast<int64_t>(priority);
		slowTaskMetric->numYields = numYields;
		slowTaskMetric->log();

		double sampleRate = std::min(1.0, (elapsed > warnThreshold) ? 1.0 : elapsed / 10e9);
		if(FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL > 0 && duration > FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL) {
			sampleRate = 1; // Always include slow task events that could show up in our slow task profiling.
		}

		if ( !DEBUG_DETERMINISM && (nondeterministicRandom()->random01() < sampleRate ))
			TraceEvent(elapsed > warnThreshold ? SevWarnAlways : SevInfo, "SlowTask").detail("TaskID", priority).detail("MClocks", elapsed/1e6).detail("Duration", duration).detail("SampleRate", sampleRate).detail("NumYields", numYields);
	}
}

bool Net2::check_yield( TaskPriority taskID, bool isRunLoop ) {
	if(!isRunLoop && numYields > 0) {
		++numYields;
		return true;
	}

	if ((g_stackYieldLimit) && ( (intptr_t)&taskID < g_stackYieldLimit )) {
		++countYieldBigStack;
		return true;
	}

	processThreadReady();

	if (taskID == TaskPriority::DefaultYield) taskID = currentTaskID;
	if (!ready.empty() && ready.top().priority > int64_t(taskID)<<32)  {
		return true;
	}

	// SOMEDAY: Yield if there are lots of higher priority tasks queued?
	int64_t tsc_now = __rdtsc();
	double newTaskBegin = timer_monotonic();
	if (tsc_now < tsc_begin) {
		return true;
	}

	if(isRunLoop) {
		checkForSlowTask(tsc_begin, tsc_now, newTaskBegin-taskBegin, currentTaskID);
	}

	if (tsc_now > tsc_end) {
		++numYields;
		return true;
	}

	taskBegin = newTaskBegin;
	tsc_begin = tsc_now;
	return false;
}

bool Net2::check_yield( TaskPriority taskID ) {
	return check_yield(taskID, false);
}

Future<class Void> Net2::yield( TaskPriority taskID ) {
	++countYieldCalls;
	if (taskID == TaskPriority::DefaultYield) taskID = currentTaskID;
	if (check_yield(taskID, false)) {
		++countYieldCallsTrue;
		return delay(0, taskID);
	}
	g_network->setCurrentTask(taskID);
	return Void();
}

Future<Void> Net2::delay( double seconds, TaskPriority taskId ) {
	if (seconds <= 0.) {
		PromiseTask* t = new PromiseTask;
		this->ready.push( OrderedTask( (int64_t(taskId)<<32)-(++tasksIssued), taskId, t) );
		return t->promise.getFuture();
	}
	if (seconds >= 4e12)  // Intervals that overflow an int64_t in microseconds (more than 100,000 years) are treated as infinite
		return Never();

	double at = now() + seconds;
	PromiseTask* t = new PromiseTask;
	this->timers.push( DelayedTask( at, (int64_t(taskId)<<32)-(++tasksIssued), taskId, t ) );
	return t->promise.getFuture();
}

void Net2::onMainThread(Promise<Void>&& signal, TaskPriority taskID) {
	if (stopped) return;
	PromiseTask* p = new PromiseTask( std::move(signal) );
	int64_t priority = int64_t(taskID)<<32;

	if ( thread_network == this )
	{
		processThreadReady();
		this->ready.push( OrderedTask( priority-(++tasksIssued), taskID, p ) );
	} else {
		if (threadReady.push( OrderedTask( priority, taskID, p ) ))
			reactor.wake();
	}
}

THREAD_HANDLE Net2::startThread( THREAD_FUNC_RETURN (*func) (void*), void *arg ) {
	return ::startThread(func, arg);
}

Future< Reference<IConnection> > Net2::connect( NetworkAddress toAddr, std::string host ) {
#ifndef TLS_DISABLED
	initTLS();
	if ( toAddr.isTLS() ) {
		return SSLConnection::connect(&this->reactor.ios, &this->sslContext, toAddr);
	}
#endif

	return Connection::connect(&this->reactor.ios, toAddr);
}

ACTOR static Future<std::vector<NetworkAddress>> resolveTCPEndpoint_impl( Net2 *self, std::string host, std::string service) {
	state tcp::resolver tcpResolver(self->reactor.ios);
	Promise<std::vector<NetworkAddress>> promise;
	state Future<std::vector<NetworkAddress>> result = promise.getFuture();

	tcpResolver.async_resolve(tcp::resolver::query(host, service), [=](const boost::system::error_code &ec, tcp::resolver::iterator iter) {
		if(ec) {
			promise.sendError(lookup_failed());
			return;
		}

		std::vector<NetworkAddress> addrs;

		tcp::resolver::iterator end;
		while(iter != end) {
			auto endpoint = iter->endpoint();
			auto addr = endpoint.address();
			if (addr.is_v6()) {
				addrs.push_back(NetworkAddress(IPAddress(addr.to_v6().to_bytes()), endpoint.port()));
			} else {
				addrs.push_back(NetworkAddress(addr.to_v4().to_ulong(), endpoint.port()));
			}
			++iter;
		}

		if(addrs.empty()) {
			promise.sendError(lookup_failed());
		}
		else {
			promise.send(addrs);
		}
	});

	wait(ready(result));
	tcpResolver.cancel();

	return result.get();
}

Future<std::vector<NetworkAddress>> Net2::resolveTCPEndpoint( std::string host, std::string service) {
	return resolveTCPEndpoint_impl(this, host, service);
}

bool Net2::isAddressOnThisHost( NetworkAddress const& addr ) {
	auto it = addressOnHostCache.find( addr.ip );
	if (it != addressOnHostCache.end())
		return it->second;

	if (addressOnHostCache.size() > 50000) addressOnHostCache.clear();  // Bound cache memory; should not really happen

	try {
		boost::asio::io_service ioService;
		boost::asio::ip::udp::socket socket(ioService);
		boost::asio::ip::udp::endpoint endpoint(tcpAddress(addr.ip), 1);
		socket.connect(endpoint);
		bool local = addr.ip.isV6() ? socket.local_endpoint().address().to_v6().to_bytes() == addr.ip.toV6()
		                            : socket.local_endpoint().address().to_v4().to_ulong() == addr.ip.toV4();
		socket.close();
		if (local) TraceEvent(SevInfo, "AddressIsOnHost").detail("Address", addr);
		return addressOnHostCache[ addr.ip ] = local;
	}
	catch(boost::system::system_error e)
	{
		TraceEvent(SevWarnAlways, "IsAddressOnHostError").detail("Address", addr).detail("ErrDesc", e.what()).detail("ErrCode", e.code().value());
		return addressOnHostCache[ addr.ip ] = false;
	}
}

Reference<IListener> Net2::listen( NetworkAddress localAddr ) {
	try {
#ifndef TLS_DISABLED
		initTLS();
		if ( localAddr.isTLS() ) {
			return Reference<IListener>(new SSLListener( reactor.ios, &this->sslContext, localAddr ));
		}
#endif
		return Reference<IListener>( new Listener( reactor.ios, localAddr ) );
	} catch (boost::system::system_error const& e) {
		Error x;
		if(e.code().value() == EADDRINUSE)
			x = address_in_use();
		else if(e.code().value() == EADDRNOTAVAIL)
			x = invalid_local_address();
		else
			x = bind_failed();
		TraceEvent("Net2ListenError").error(x).detail("Message", e.what());
		throw x;
	} catch (std::exception const& e) {
		Error x = unknown_error();
		TraceEvent("Net2ListenError").error(x).detail("Message", e.what());
		throw x;
	} catch (Error &e ) {
		TraceEvent("Net2ListenError").error(e);
		throw e;
	} catch (...) {
		Error x = unknown_error();
		TraceEvent("Net2ListenError").error(x);
		throw x;
	}
}

void Net2::getDiskBytes( std::string const& directory, int64_t& free, int64_t& total ) {
	return ::getDiskBytes(directory, free, total);
}

#ifdef __linux__
#include <sys/prctl.h>
#include <pthread.h>
#include <sched.h>
#endif

ASIOReactor::ASIOReactor(Net2* net)
	: network(net), firstTimer(ios), do_not_stop(ios)
{
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
		if (ret != 0) printf("Error setting priority (%d %d)\n", ret, errno);
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
			nanosleep(&tv, NULL);
#endif
		}
		else
		{
			sleepTime -= FLOW_KNOBS->BUSY_WAIT_THRESHOLD;
			if (sleepTime < 4e12) {
				this->firstTimer.expires_from_now(boost::posix_time::microseconds(int64_t(sleepTime*1e6)));
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
	while (ios.poll_one()) ++network->countASIOEvents;  // Make this a task?
}

void ASIOReactor::wake() {
	ios.post( nullCompletionHandler );
}

} // namespace net2

INetwork* newNet2(bool useThreadPool, bool useMetrics, Reference<TLSPolicy> policy, const TLSParams& tlsParams) {
	try {
		N2::g_net2 = new N2::Net2(useThreadPool, useMetrics, policy, tlsParams);
	}
	catch(boost::system::system_error e) {
		TraceEvent("Net2InitError").detail("Message", e.what());
		throw unknown_error();
	}
	catch(std::exception const& e) {
		TraceEvent("Net2InitError").detail("Message", e.what());
		throw unknown_error();
	}

	return N2::g_net2;
}

struct TestGVR {
	Standalone<StringRef> key;
	int64_t version;
	Optional<std::pair<UID,UID>> debugID;
	Promise< Optional<Standalone<StringRef>> > reply;

	TestGVR(){}

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, key, version, debugID, reply);
	}
};

template <class F>
void startThreadF( F && func ) {
	struct Thing {
		F f;
		Thing( F && f ) : f(std::move(f)) {}
		THREAD_FUNC start(void* p) { Thing* self = (Thing*)p; self->f(); delete self; THREAD_RETURN; }
	};
	Thing* t = new Thing(std::move(func));
	startThread(Thing::start, t);
}

void net2_test() {
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
	printf("%d %d %x %x %s\n", c, p, mx[0], mx[1], mx[0]==thread1Iterations && mx[1]==(1<<20)+thread2Iterations ? "OK" : "FAIL");

	finished.block();
	finished2.block();


	g_network = newNet2();  // for promise serialization below

	Endpoint destination;

	printf("  Used: %lld\n", FastAllocator<4096>::getTotalMemory());

	char junk[100];

	double before = timer();

	vector<TestGVR> reqs;
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

			PacketWriter wr(pb,rp,AssumeVersion(currentProtocolVersion));
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
