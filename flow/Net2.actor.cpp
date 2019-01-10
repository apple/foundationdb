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

#ifdef WIN32
#include <mmsystem.h>
#endif
#include "flow/actorcompiler.h"  // This must be the last #include.

// Defined to track the stack limit
extern "C" intptr_t g_stackYieldLimit;
intptr_t g_stackYieldLimit = 0;

using namespace boost::asio::ip;

// These impact both communications and the deserialization of certain database and IKeyValueStore keys.
//
// The convention is that 'x' and 'y' should match the major and minor version of the software, and 'z' should be 0.
// To make a change without a corresponding increase to the x.y version, increment the 'dev' digit.
//
//                                                       xyzdev
//                                                       vvvv
const uint64_t currentProtocolVersion        = 0x0FDB00B061020001LL;
const uint64_t compatibleProtocolVersionMask = 0xffffffffffff0000LL;
const uint64_t minValidProtocolVersion       = 0x0FDB00A200060001LL;

// This assert is intended to help prevent incrementing the leftmost digits accidentally. It will probably need to change when we reach version 10.
static_assert(currentProtocolVersion < 0x0FDB00B100000000LL, "Unexpected protocol version");

#if defined(__linux__)
#include <execinfo.h>

volatile double net2liveness = 0;

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
	int taskID;
	Task *task;
	OrderedTask(int64_t priority, int taskID, Task* task) : priority(priority), taskID(taskID), task(task) {}
	bool operator < (OrderedTask const& rhs) const { return priority < rhs.priority; }
};

thread_local INetwork* thread_network = 0;

class Net2 sealed : public INetwork, public INetworkConnections {

public:
	Net2(NetworkAddress localAddress, bool useThreadPool, bool useMetrics);
	void run();
	void initMetrics();

	// INetworkConnections interface
	virtual Future<Reference<IConnection>> connect( NetworkAddress toAddr, std::string host );
	virtual Future<std::vector<NetworkAddress>> resolveTCPEndpoint( std::string host, std::string service);
	virtual Reference<IListener> listen( NetworkAddress localAddr );

	// INetwork interface
	virtual double now() { return currentTime; };
	virtual Future<Void> delay( double seconds, int taskId );
	virtual Future<class Void> yield( int taskID );
	virtual bool check_yield(int taskId);
	virtual int getCurrentTask() { return currentTaskID; }
	virtual void setCurrentTask(int taskID ) { priorityMetric = currentTaskID = taskID; }
	virtual void onMainThread( Promise<Void>&& signal, int taskID );
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
	INetworkConnections *network;  // initially this, but can be changed

	int64_t tsc_begin, tsc_end;
	double taskBegin;
	int currentTaskID;
	uint64_t tasksIssued;
	TDMetricCollection tdmetrics;
	double currentTime;
	bool stopped;
	std::map< uint32_t, bool > addressOnHostCache;

	uint64_t numYields;

	double lastPriorityTrackTime;
	int lastMinTaskID;
	double priorityTimer[NetworkMetrics::PRIORITY_BINS];

	std::priority_queue<OrderedTask, std::vector<OrderedTask>> ready;
	ThreadSafeQueue<OrderedTask> threadReady;

	struct DelayedTask : OrderedTask {
		double at;
		DelayedTask(double at, int64_t priority, int taskID, Task* task) : at(at), OrderedTask(priority, taskID, task) {}
		bool operator < (DelayedTask const& rhs) const { return at > rhs.at; } // Ordering is reversed for priority_queue
	};
	std::priority_queue<DelayedTask, std::vector<DelayedTask>> timers;

	void checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, int64_t priority);
	bool check_yield(int taskId, bool isRunLoop);
	void processThreadReady();
	void trackMinPriority( int minTaskID, double now );
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
	BoolMetricHandle awakeMetric;

	EventMetricHandle<SlowTask> slowTaskMetric;

	std::vector<std::string> blobCredentialFiles;
};

static tcp::endpoint tcpEndpoint( NetworkAddress const& n ) {
	return tcp::endpoint( boost::asio::ip::address_v4( n.ip ), n.port );
}

class BindPromise {
	Promise<Void> p;
	const char* errContext;
	UID errID;
public:
	BindPromise( const char* errContext, UID errID ) : errContext(errContext), errID(errID) {}
	BindPromise( BindPromise const& r ) : p(r.p), errContext(r.errContext), errID(r.errID) {}
	BindPromise(BindPromise&& r) noexcept(true) : p(std::move(r.p)), errContext(r.errContext), errID(r.errID) {}

	Future<Void> getFuture() { return p.getFuture(); }

	void operator()( const boost::system::error_code& error, size_t bytesWritten=0 ) {
		try {
			if (error) {
				// Log the error...
				TraceEvent(SevWarn, errContext, errID).suppressFor(1.0).detail("Message", error.value());
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
		: id(g_nondeterministic_random->randomUniqueID()), socket(io_service)
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
	}

	void closeSocket() {
		boost::system::error_code error;
		socket.close(error);
		if (error)
			TraceEvent(SevWarn, "N2_CloseError", id).suppressFor(1.0).detail("Message", error.value());
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

class Listener : public IListener, ReferenceCounted<Listener> {
	NetworkAddress listenAddress;
	tcp::acceptor acceptor;

public:
	Listener( boost::asio::io_service& io_service, NetworkAddress listenAddress )
		: listenAddress(listenAddress), acceptor( io_service, tcpEndpoint( listenAddress ) )
	{
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
			conn->accept( NetworkAddress(peer_endpoint.address().to_v4().to_ulong(), peer_endpoint.port()) );

			return conn;
		} catch (...) {
			conn->close();
			throw;
		}
	}
};

struct PromiseTask : public Task, public FastAllocated<PromiseTask> {
	Promise<Void> promise;
	PromiseTask() {}
	explicit PromiseTask( Promise<Void>&& promise ) noexcept(true) : promise(std::move(promise)) {}

	virtual void operator()() {
		promise.send(Void());
		delete this;
	}
};

Net2::Net2(NetworkAddress localAddress, bool useThreadPool, bool useMetrics)
	: useThreadPool(useThreadPool),
	  network(this),
	  reactor(this),
	  stopped(false),
	  tasksIssued(0),
	  // Until run() is called, yield() will always yield
	  tsc_begin(0), tsc_end(0), taskBegin(0), currentTaskID(TaskDefaultYield),
	  lastMinTaskID(0),
	  numYields(0)
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
		networkMetrics.priorityBins[i] = priBins[i];
	updateNow();

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
		++countRunLoop;

		if (runFunc) {
			tsc_begin = __rdtsc();
			taskBegin = timer_monotonic();
			runFunc();
			checkForSlowTask(tsc_begin, __rdtsc(), timer_monotonic() - taskBegin, TaskRunCycleFunction);
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
			if (!timers.empty())
				sleepTime = timers.top().at - timer_monotonic();  // + 500e-6?
		}

		awakeMetric = false;
		if( sleepTime > 0 )
			priorityMetric = 0;
		reactor.sleepAndReact(sleepTime);
		awakeMetric = true;

		updateNow();
		double now = this->currentTime;

		if ((now-nnow) > FLOW_KNOBS->SLOW_LOOP_CUTOFF && g_nondeterministic_random->random01() < (now-nnow)*FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopTop").detail("Elapsed", now - nnow);

		if (sleepTime) trackMinPriority( 0, now );
		while (!timers.empty() && timers.top().at < now) {
			++countTimers;
			ready.push( timers.top() );
			timers.pop();
		}

		processThreadReady();

		tsc_begin = __rdtsc();
		tsc_end = tsc_begin + FLOW_KNOBS->TSC_YIELD_TIME;
		taskBegin = timer_monotonic();
		numYields = 0;
		int minTaskID = TaskMaxPriority;

		while (!ready.empty()) {
			++countTasks;
			currentTaskID = ready.top().taskID;
			priorityMetric = currentTaskID;
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

			if (check_yield(TaskMaxPriority, true)) { ++countYields; break; }
		}

		nnow = timer_monotonic();

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
			net2liveness = g_nondeterministic_random->random01();
		}
#endif

		if ((nnow-now) > FLOW_KNOBS->SLOW_LOOP_CUTOFF && g_nondeterministic_random->random01() < (nnow-now)*FLOW_KNOBS->SLOW_LOOP_SAMPLING_RATE)
			TraceEvent("SomewhatSlowRunLoopBottom").detail("Elapsed", nnow - now); // This includes the time spent running tasks

		trackMinPriority( minTaskID, nnow );
	}

	#ifdef WIN32
	timeEndPeriod(1);
	#endif
}

void Net2::trackMinPriority( int minTaskID, double now ) {
	if (minTaskID != lastMinTaskID)
		for(int c=0; c<NetworkMetrics::PRIORITY_BINS; c++) {
			int64_t pri = networkMetrics.priorityBins[c];
			if (pri >= minTaskID && pri < lastMinTaskID) {  // busy -> idle
				double busyFor = lastPriorityTrackTime - priorityTimer[c];
				networkMetrics.secSquaredPriorityBlocked[c] += busyFor*busyFor;
			}
			if (pri < minTaskID && pri >= lastMinTaskID) {  // idle -> busy
				priorityTimer[c] = now;
			}
		}
	lastMinTaskID = minTaskID;
	lastPriorityTrackTime = now;
}

void Net2::processThreadReady() {
	while (true) {
		Optional<OrderedTask> t = threadReady.pop();
		if (!t.present()) break;
		t.get().priority -= ++tasksIssued;
		ASSERT( t.get().task != 0 );
		ready.push( t.get() );
	}
}

void Net2::checkForSlowTask(int64_t tscBegin, int64_t tscEnd, double duration, int64_t priority) {
	int64_t elapsed = tscEnd-tscBegin;
	if (elapsed > FLOW_KNOBS->TSC_YIELD_TIME && tscBegin > 0) {
		int i = std::min<double>(NetworkMetrics::SLOW_EVENT_BINS-1, log( elapsed/1e6 ) / log(2.));
		int s = ++networkMetrics.countSlowEvents[i];
		uint64_t warnThreshold = g_network->isSimulated() ? 10e9 : 500e6;

		//printf("SlowTask: %d, %d yields\n", (int)(elapsed/1e6), numYields);

		slowTaskMetric->clocks = elapsed;
		slowTaskMetric->duration = (int64_t)(duration*1e9);
		slowTaskMetric->priority = priority;
		slowTaskMetric->numYields = numYields;
		slowTaskMetric->log();

		double sampleRate = std::min(1.0, (elapsed > warnThreshold) ? 1.0 : elapsed / 10e9);
		if(FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL > 0 && duration > FLOW_KNOBS->SLOWTASK_PROFILING_INTERVAL) {
			sampleRate = 1; // Always include slow task events that could show up in our slow task profiling.
		}

		if ( !DEBUG_DETERMINISM && (g_nondeterministic_random->random01() < sampleRate ))
			TraceEvent(elapsed > warnThreshold ? SevWarnAlways : SevInfo, "SlowTask").detail("TaskID", priority).detail("MClocks", elapsed/1e6).detail("Duration", duration).detail("SampleRate", sampleRate).detail("NumYields", numYields);
	}
}

bool Net2::check_yield( int taskID, bool isRunLoop ) {
	if(!isRunLoop && numYields > 0) {
		++numYields;
		return true;
	}

	if ((g_stackYieldLimit) && ( (intptr_t)&taskID < g_stackYieldLimit )) {
		++countYieldBigStack;
		return true;
	}

	processThreadReady();

	if (taskID == TaskDefaultYield) taskID = currentTaskID;
	if (!ready.empty() && ready.top().priority > (int64_t(taskID)<<32))  {
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

bool Net2::check_yield( int taskID ) {
	return check_yield(taskID, false);
}

Future<class Void> Net2::yield( int taskID ) {
	++countYieldCalls;
	if (taskID == TaskDefaultYield) taskID = currentTaskID;
	if (check_yield(taskID, false)) {
		++countYieldCallsTrue;
		return delay(0, taskID);
	}
	g_network->setCurrentTask(taskID);
	return Void();
}

Future<Void> Net2::delay( double seconds, int taskId ) {
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

void Net2::onMainThread(Promise<Void>&& signal, int taskID) {
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
			// Currently only ipv4 is supported by NetworkAddress
			auto addr = endpoint.address();
			if(addr.is_v4()) {
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
		boost::asio::ip::udp::endpoint endpoint(boost::asio::ip::address_v4(addr.ip), 1);
		socket.connect(endpoint);
		bool local = socket.local_endpoint().address().to_v4().to_ulong() == addr.ip;
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

void ASIOReactor::sleepAndReact(double sleepTime) {
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
	while (ios.poll_one()) ++network->countASIOEvents;  // Make this a task?
}

void ASIOReactor::wake() {
	ios.post( nullCompletionHandler );
}

} // namespace net2

INetwork* newNet2(NetworkAddress localAddress, bool useThreadPool, bool useMetrics) {
	try {
		N2::g_net2 = new N2::Net2(localAddress, useThreadPool, useMetrics);
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


	g_network = newNet2(NetworkAddress::parse("127.0.0.1:12345"));  // for promise serialization below

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
