/*
 * Trace.cpp
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


#include "Trace.h"
#include "flow.h"
#include "DeterministicRandom.h"
#include <stdlib.h>
#include <stdarg.h>
#ifdef WIN32
#include <windows.h>
#undef max
#undef min
#endif
#include <time.h>

#include "IThreadPool.h"
#include "ThreadHelper.actor.h"
#include "FastRef.h"
#include "EventTypes.actor.h"
#include "TDMetric.actor.h"
#include "MetricSample.h"

#include <fcntl.h>
#if defined(__unixish__)
#define __open ::open
#define __write ::write
#define __close ::close
#define __fsync ::fsync
#define TRACEFILE_FLAGS O_WRONLY | O_CREAT | O_EXCL
#define TRACEFILE_MODE 0664
#elif defined(_WIN32)
#include <io.h>
#include <stdio.h>
#include <sys/stat.h>
#define __open _open
#define __write _write
#define __close _close
#define __fsync _commit
#define TRACEFILE_FLAGS _O_WRONLY | _O_CREAT | _O_EXCL
#define TRACEFILE_MODE _S_IWRITE
#endif

class DummyThreadPool : public IThreadPool, ReferenceCounted<DummyThreadPool> {
public:
	~DummyThreadPool() {}
	DummyThreadPool() : thread(NULL) {}
	Future<Void> getError() {
		return errors.getFuture();
	}
	void addThread( IThreadPoolReceiver* userData ) {
		ASSERT( !thread );
		thread = userData;
	}
	void post( PThreadAction action ) {
		try {
			(*action)( thread );
		} catch (Error& e) {
			errors.sendError( e );
		} catch (...) {
			errors.sendError( unknown_error() );
		}
	}
	Future<Void> stop() {
		return Void();
	}
	void addref() {
		ReferenceCounted<DummyThreadPool>::addref();
	}
	void delref() {
		ReferenceCounted<DummyThreadPool>::delref();
	}

private:
	IThreadPoolReceiver* thread;
	Promise<Void> errors;
};

struct SuppressionMap {
	struct SuppressionInfo {
		double endTime;
		int64_t suppressedEventCount;

		SuppressionInfo() : endTime(0), suppressedEventCount(0) {}
	};

	std::map<std::string, SuppressionInfo> suppressionMap;

	// Returns -1 if this event is suppressed
	int64_t checkAndInsertSuppression(std::string type, double duration) {
		if(suppressionMap.size() >= FLOW_KNOBS->MAX_TRACE_SUPPRESSIONS) {
			TraceEvent(SevWarnAlways, "ClearingTraceSuppressionMap");
			suppressionMap.clear();
		}

		auto insertion = suppressionMap.insert(std::make_pair(type, SuppressionInfo()));
		if(insertion.second || insertion.first->second.endTime <= now()) {
			int64_t suppressedEventCount = insertion.first->second.suppressedEventCount;
			insertion.first->second.endTime = now() + duration;
			insertion.first->second.suppressedEventCount = 0;
			return suppressedEventCount;
		}
		else {
			++insertion.first->second.suppressedEventCount;
			return -1;
		}
	}
};

TraceBatch g_traceBatch;
trace_clock_t g_trace_clock = TRACE_CLOCK_NOW;
std::set<StringRef> suppress;
IRandom* trace_random = NULL;

LatestEventCache latestEventCache;
SuppressionMap suppressedEvents;

static TransientThresholdMetricSample<Standalone<StringRef>> *traceEventThrottlerCache;
static const char *TRACE_EVENT_THROTTLE_STARTING_TYPE = "TraceEventThrottle_";


struct TraceLog {
	Standalone< VectorRef<StringRef> > buffer;
	int file_length;
	int buffer_length;
	bool opened;
	std::string basename;
	std::string logGroup;

	std::string directory;
	std::string processName;
	NetworkAddress localAddress;

	Reference<IThreadPool> writer;
	uint64_t rollsize;
	Mutex mutex;

	bool logTraceEventMetrics;
	EventMetricHandle<TraceEventNameID> SevErrorNames;
	EventMetricHandle<TraceEventNameID> SevWarnAlwaysNames;
	EventMetricHandle<TraceEventNameID> SevWarnNames;
	EventMetricHandle<TraceEventNameID> SevInfoNames;
	EventMetricHandle<TraceEventNameID> SevDebugNames;

	void initMetrics()
	{
		SevErrorNames.init(LiteralStringRef("TraceEvents.SevError"));
		SevWarnAlwaysNames.init(LiteralStringRef("TraceEvents.SevWarnAlways"));
		SevWarnNames.init(LiteralStringRef("TraceEvents.SevWarn"));
		SevInfoNames.init(LiteralStringRef("TraceEvents.SevInfo"));
		SevDebugNames.init(LiteralStringRef("TraceEvents.SevDebug"));
		logTraceEventMetrics = true;
	}

	struct BarrierList : ThreadSafeReferenceCounted<BarrierList> {
		BarrierList() : ntriggered(0) {}
		void push( ThreadFuture<Void> f ) {
			MutexHolder h(mutex);
			barriers.push_back(f);
		}
		void pop() {
			MutexHolder h(mutex);
			unsafeTrigger(0);
			barriers.pop_front();
			if (ntriggered) ntriggered--;
		}
		void triggerAll() {
			MutexHolder h(mutex);
			for(int i=ntriggered; i<barriers.size(); i++)
				unsafeTrigger(i);
			ntriggered = barriers.size();
		}
	private:
		Mutex mutex;
		Deque< ThreadFuture<Void> > barriers;
		int ntriggered;
		void unsafeTrigger(int i) {
			auto b = ((ThreadSingleAssignmentVar<Void>*)barriers[i].getPtr());
			if (!b->isReady())
				b->send(Void());
		}
	};

	Reference<BarrierList> barriers;

	struct WriterThread : IThreadPoolReceiver {
		WriterThread( std::string directory, std::string processName, uint32_t maxLogsSize, std::string basename, Reference<BarrierList> barriers ) : directory(directory), processName(processName), maxLogsSize(maxLogsSize), basename(basename), traceFileFD(0), index(0), barriers(barriers) {}

		virtual void init() {}

		Reference<BarrierList> barriers;
		int traceFileFD;
		std::string directory;
		std::string processName;
		int maxLogsSize;
		std::string basename;
		int index;

		void lastError(int err) {
			// Whenever we get a serious error writing a trace log, all flush barriers posted between the operation encountering
			// the error and the occurrence of the error are unblocked, even though we haven't actually succeeded in flushing.
			// Otherwise a permanent write error would make the program block forever.
			if (err != 0 && err != EINTR) {
				barriers->triggerAll();
			}
		}

		void writeReliable(const uint8_t* buf, int count) {
			auto ptr = buf;
			int remaining = count;

			while ( remaining ) {
				int ret = __write( traceFileFD, ptr, remaining );
				if ( ret > 0 ) {
					lastError(0);
					remaining -= ret;
					ptr += ret;
				} else {
					lastError(errno);
					threadSleep(0.1);
				}
			}
		}
		void writeReliable(const char* msg) {
			writeReliable( (const uint8_t*)msg, strlen(msg) );
		}

		void handleTraceFileOpenError(const std::string& filename)
		{
		}

		struct Open : TypedAction<WriterThread,Open> {
			virtual double getTimeEstimate() { return 0; }
		};
		void action( Open& o ) {
			if (traceFileFD) {
				writeReliable("</Trace>");
				while ( __close(traceFileFD) ) threadSleep(0.1);
			}

			cleanupTraceFiles();

			auto finalname = format("%s.%d.xml", basename.c_str(), ++index);
			while ( (traceFileFD = __open( finalname.c_str(), TRACEFILE_FLAGS, TRACEFILE_MODE )) == -1 ) {
				lastError(errno);
				if (errno == EEXIST)
					finalname = format("%s.%d.xml", basename.c_str(), ++index);
				else {
					fprintf(stderr, "ERROR: could not create trace log file `%s' (%d: %s)\n", finalname.c_str(), errno, strerror(errno));

					int errorNum = errno;
					onMainThreadVoid([finalname, errorNum]{
						TraceEvent(SevWarnAlways, "TraceFileOpenError")
							.detail("Filename", finalname)
							.detail("ErrorCode", errorNum)
							.detail("Error", strerror(errorNum))
							.trackLatest("TraceFileOpenError"); }, NULL);
					threadSleep(FLOW_KNOBS->TRACE_RETRY_OPEN_INTERVAL);
				}
			}
			onMainThreadVoid([]{ latestEventCache.clear("TraceFileOpenError"); }, NULL);
			lastError(0);

			writeReliable( "<?xml version=\"1.0\"?>\r\n<Trace>\r\n" );
		}

		struct Barrier : TypedAction<WriterThread, Barrier> {
			virtual double getTimeEstimate() { return 0; }
		};
		void action( Barrier& a ) {
			barriers->pop();
		}

		struct WriteBuffer : TypedAction<WriterThread, WriteBuffer> {
			Standalone< VectorRef<StringRef> > buffer;

			WriteBuffer( Standalone< VectorRef<StringRef> > buffer ) : buffer(buffer) {}
			virtual double getTimeEstimate() { return .001; }
		};
		void action( WriteBuffer& a ) {
			if ( traceFileFD ) {
				for ( auto i = a.buffer.begin(); i != a.buffer.end(); ++i )
					writeReliable( i->begin(), i->size() );

				if(FLOW_KNOBS->TRACE_FSYNC_ENABLED) {
					__fsync( traceFileFD );
				}

				a.buffer = Standalone< VectorRef<StringRef> >();
			}
		}

		void cleanupTraceFiles() {
			// Setting maxLogsSize=0 disables trace file cleanup based on dir size
			if(!g_network->isSimulated() && maxLogsSize > 0) {
				try {
					std::vector<std::string> existingFiles = platform::listFiles(directory, ".xml");
					std::vector<std::string> existingTraceFiles;

					for(auto f = existingFiles.begin(); f != existingFiles.end(); ++f)
						if(f->substr(0, processName.length()) == processName)
							existingTraceFiles.push_back(*f);

					// reverse sort, so we preserve the most recent files and delete the oldest
					std::sort(existingTraceFiles.begin(), existingTraceFiles.end(), TraceLog::reverseCompareTraceFileName);

					int64_t runningTotal = 0;
					std::vector<std::string>::iterator fileListIterator = existingTraceFiles.begin();

					while(runningTotal < maxLogsSize && fileListIterator != existingTraceFiles.end()) {
						runningTotal += (fileSize(joinPath(directory, *fileListIterator)) + FLOW_KNOBS->ZERO_LENGTH_FILE_PAD);
						++fileListIterator;
					}

					while(fileListIterator != existingTraceFiles.end()) {
						deleteFile(joinPath(directory, *fileListIterator));
						++fileListIterator;
					}
				} catch( Error & ) {}
			}
		}
	};

	TraceLog() : buffer_length(0), file_length(0), opened(false), barriers(new BarrierList), logTraceEventMetrics(false) {}

	bool isOpen() const { return opened; }

	void open( std::string const& directory, std::string const& processName, std::string const& timestamp, uint64_t rs, uint64_t maxLogsSize, NetworkAddress na ) {
		ASSERT( !writer && !opened );

		this->directory = directory;
		this->processName = processName;
		this->localAddress = na;

		basename = format("%s/%s.%s.%s", directory.c_str(), processName.c_str(), timestamp.c_str(), g_random->randomAlphaNumeric(6).c_str());

		if ( g_network->isSimulated() )
			writer = Reference<IThreadPool>(new DummyThreadPool());
		else
			writer = createGenericThreadPool();
		writer->addThread( new WriterThread(directory, processName, maxLogsSize, basename, barriers) );

		rollsize = rs;

		auto a = new WriterThread::Open;
		writer->post(a);

		opened = true;
	}

	static void extractTraceFileNameInfo(std::string const& filename, std::string &root, int &index) {
		int split = filename.find_last_of('.', filename.size() - 5);
		root = filename.substr(0, split);
		if(sscanf(filename.substr(split + 1, filename.size() - split - 4).c_str(), "%d", &index) == EOF)
			index = -1;
	}

	static bool compareTraceFileName (std::string const& f1, std::string const& f2) {
		std::string root1;
		std::string root2;

		int index1;
		int index2;

		extractTraceFileNameInfo(f1, root1, index1);
		extractTraceFileNameInfo(f2, root2, index2);

		if(root1 != root2)
			return root1 < root2;
		if(index1 != index2)
			return index1 < index2;

		return f1 < f2;
	}

	static bool reverseCompareTraceFileName(std::string f1, std::string f2) {
		return compareTraceFileName(f2, f1);
	}

	void write( const void* data, int length ) {
		MutexHolder hold(mutex);
		// FIXME: What if we are using way too much memory for buffer?
		buffer.push_back_deep( buffer.arena(), StringRef((const uint8_t*)data,length) );
		buffer_length += length;
	}

	void log(int severity, const char *name, UID id, uint64_t event_ts)
	{
		if(!logTraceEventMetrics)
			return;

		EventMetricHandle<TraceEventNameID> *m = NULL;
		switch(severity)
		{
			case SevError:       m = &SevErrorNames;      break;
			case SevWarnAlways:  m = &SevWarnAlwaysNames; break;
			case SevWarn:        m = &SevWarnNames;       break;
			case SevInfo:        m = &SevInfoNames;       break;
			case SevDebug:       m = &SevDebugNames;      break;
			default:
			break;
		}
		if(m != NULL)
		{
			(*m)->name = StringRef((uint8_t*)name, strlen(name));
			(*m)->id = id.toString();
			(*m)->log(event_ts);
		}
	}

	ThreadFuture<Void> flush() {
		traceEventThrottlerCache->poll();

		MutexHolder hold(mutex);
		bool roll = false;
		if (!buffer.size()) return Void(); // SOMEDAY: maybe we still roll the tracefile here?

		if (rollsize && buffer_length + file_length > rollsize) // SOMEDAY: more conditions to roll
			roll = true;

		auto a = new WriterThread::WriteBuffer( std::move(buffer) );
		file_length += buffer_length;
		buffer = Standalone<VectorRef<StringRef>>();
		buffer_length = 0;
		writer->post( a );

		if (roll) {
			std::vector<std::string> events = latestEventCache.getAllUnsafe();
			for (int idx = 0; idx < events.size(); idx++) {
				if(events[idx].size() > 0) {
					int timeIndex = events[idx].find(" Time=");
					ASSERT(timeIndex != events[idx].npos);

					double time = g_trace_clock == TRACE_CLOCK_NOW ? now() : timer();
					std::string rolledEvent = format("%s Time=\"%.6f\" Original%s", events[idx].substr(0, timeIndex).c_str(), time, events[idx].substr(timeIndex+1).c_str());

					buffer.push_back_deep( buffer.arena(), StringRef(rolledEvent) );
				}
			}

			auto o = new WriterThread::Open;
			file_length = 0;
			writer->post( o );
		}

		ThreadFuture<Void> f(new ThreadSingleAssignmentVar<Void>);
		barriers->push(f);
		writer->post( new WriterThread::Barrier );

		return f;
	}

	void close() {
		if (opened) {
			auto s = LiteralStringRef( "</Trace>\r\n" );
			write( s.begin(), s.size() );

			auto f = flush();

			// If we are using a writer thread, try to wait for it to finish its work queue before killing it
			// If it's encountering errors, we'll get past here without it actually finishing
			if (writer) f.getBlocking();

			opened = false;
		}
	}

	~TraceLog() {
		close();
		if (writer) writer->addref(); // FIXME: We are not shutting down the writer thread at all, because the ThreadPool shutdown mechanism is blocking (necessarily waits for current work items to finish) and we might not be able to finish everything.  Also we (already) weren't closing the file on shutdown anyway.
	}
};

NetworkAddress getAddressIndex() {
// ahm
//	if( g_network->isSimulated() )
//		return g_simulator.getCurrentProcess()->address;
//	else
	return g_network->getLocalAddress();
}

// This does not check for simulation, and as such is not safe for external callers
void clearPrefix_internal( std::map<std::string, std::string>& data, std::string prefix ) {
	auto first = data.lower_bound( prefix );
	auto last = data.lower_bound( strinc( prefix ).toString() );
	data.erase( first, last );
}

void LatestEventCache::clear( std::string prefix ) {
	clearPrefix_internal( latest[getAddressIndex()], prefix );
}

void LatestEventCache::clear() {
	latest[getAddressIndex()].clear();
}

void LatestEventCache::set( std::string tag, std::string contents ) {
	latest[getAddressIndex()][tag] = contents;
}

std::string LatestEventCache::get( std::string tag ) {
	return latest[getAddressIndex()][tag];
}

std::vector<std::string> allEvents( std::map<std::string, std::string> const& data ) {
	std::vector<std::string> all;
	for(auto it = data.begin(); it != data.end(); it++) {
		all.push_back( it->second );
	}
	return all;
}

std::vector<std::string> LatestEventCache::getAll() {
	return allEvents( latest[getAddressIndex()] );
}

// if in simulation, all events from all machines will be returned
std::vector<std::string> LatestEventCache::getAllUnsafe() {
	std::vector<std::string> all;
	for(auto it = latest.begin(); it != latest.end(); ++it) {
		auto m = allEvents( it->second );
		all.insert( all.end(), m.begin(), m.end() );
	}
	return all;
}

void LatestEventCache::setLatestError( std::string contents ) {
	if(TraceEvent::isNetworkThread()) { // The latest event cache doesn't track errors that happen on other threads
		latestErrors[getAddressIndex()] = contents;
	}
}

std::string LatestEventCache::getLatestError() {
	return latestErrors[getAddressIndex()];
}

static TraceLog g_traceLog;

ThreadFuture<Void> flushTraceFile() {
	if (!g_traceLog.isOpen())
		return Void();
	return g_traceLog.flush();
}

void flushTraceFileVoid() {
	if ( g_network && g_network->isSimulated() )
		flushTraceFile();
	else {
		flushTraceFile().getBlocking();
	}
}

void openTraceFile(const NetworkAddress& na, uint64_t rollsize, uint64_t maxLogsSize, std::string directory, std::string baseOfBase, std::string logGroup) {
	if(g_traceLog.isOpen())
		return;

	if(directory.empty())
		directory = ".";

	if (baseOfBase.empty())
		baseOfBase = "trace";

	std::string baseName = format("%s.%03d.%03d.%03d.%03d.%d", baseOfBase.c_str(), (na.ip>>24)&0xff, (na.ip>>16)&0xff, (na.ip>>8)&0xff, na.ip&0xff, na.port);
	g_traceLog.logGroup = logGroup;
	g_traceLog.open( directory, baseName, format("%lld", time(NULL)), rollsize, maxLogsSize, na );

	// FIXME
	suppress.insert( LiteralStringRef( "TLogCommitDurable" ) );
	suppress.insert( LiteralStringRef( "StorageServerUpdate" ) );
	suppress.insert( LiteralStringRef( "TLogCommit" ) );
	suppress.insert( LiteralStringRef( "StorageServerDurable" ) );
	suppress.insert( LiteralStringRef( "ForgotVersionsBefore" ) );
	suppress.insert( LiteralStringRef( "FDData" ) );
	suppress.insert( LiteralStringRef( "FailureDetectionPoll" ) );
	suppress.insert( LiteralStringRef( "MasterProxyRate" ) );

	uncancellable(recurring(&flushTraceFile, FLOW_KNOBS->TRACE_FLUSH_INTERVAL, TaskFlushTrace));
	g_traceBatch.dump();
}

void initTraceEventMetrics() {
	g_traceLog.initMetrics();
}

void closeTraceFile() {
	g_traceLog.close();
}

bool traceFileIsOpen() {
	return g_traceLog.isOpen();
}

bool TraceEvent::isEnabled( const char* type, Severity severity ) {
	//if (!g_traceLog.isOpen()) return false;
	if( !g_network ) return false;
	if( severity < FLOW_KNOBS->MIN_TRACE_SEVERITY) return false;
	StringRef s( (const uint8_t*)type, strlen(type) );
	return !suppress.count(s);
}

TraceEvent::TraceEvent( const char* type, UID id ) : type(type), id(id) {
	init(SevInfo, type);
	detail("ID", id);
}

TraceEvent::TraceEvent( Severity severity, const char* type, UID id ) : type(type), id(id) {
	init(severity, type);
	detail("ID", id);
}

TraceEvent::TraceEvent(const char* type, const StringRef& zoneId) : type(type) {
	id = UID(hashlittle(zoneId.begin(), zoneId.size(), 0), 0);
	init(SevInfo, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent(Severity severity, const char* type, const StringRef& zoneId) : type(type) {
	id = UID(hashlittle(zoneId.begin(), zoneId.size(), 0), 0);
	init(severity, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent(const char* type, const Optional<Standalone<StringRef>>& zoneId) : type(type) {
	id = zoneId.present() ? UID(hashlittle(zoneId.get().begin(), zoneId.get().size(), 0), 0) : UID(-1LL,0);
	init(SevInfo, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent(Severity severity, const char* type, const Optional<Standalone<StringRef>>& zoneId) : type(type) {
	id = zoneId.present() ? UID(hashlittle(zoneId.get().begin(), zoneId.get().size(), 0), 0) : UID(-1LL, 0);
	init(severity, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent( TraceInterval& interval, UID id ) : type(""), id(id) {
	init(interval.severity, interval);
	detail("ID", id);
}

TraceEvent::TraceEvent( Severity severity, TraceInterval& interval, UID id ) : type(""), id(id) {
	init(severity, interval);
	detail("ID", id);
}

bool TraceEvent::init( Severity severity, TraceInterval& interval ) {
	bool result = init( severity, interval.type );
	switch (interval.count++) {
		case 0: { detail("BeginPair", interval.pairID); break; }
		case 1: { detail("EndPair", interval.pairID); break; }
		default: ASSERT(false);
	}
	return result;
}

bool TraceEvent::init( Severity severity, const char* type ) {
	this->severity = severity;
	tmpEventMetric = new DynamicEventMetric(MetricNameRef());
	tmpEventMetric->setField("Severity", (int64_t)severity);

	length = 0;
	if (isEnabled(type, severity)) {
		enabled = true;
		buffer[sizeof(buffer)-1]=0;
		NetworkAddress local = g_network->isSimulated() ? g_network->getLocalAddress() : g_traceLog.localAddress;
		double time = g_trace_clock == TRACE_CLOCK_NOW ? now() : timer();
		writef( "<Event Severity=\"%d\" Time=\"%.6f\" Type=\"%s\" Machine=\"%d.%d.%d.%d:%d\"",
			(int)severity, time, type,
			(local.ip>>24)&0xff,(local.ip>>16)&0xff,(local.ip>>8)&0xff,local.ip&0xff,local.port );
	} else
		enabled = false;

	return enabled;
}

TraceEvent& TraceEvent::error(class Error const& error, bool includeCancelled) {
	if (enabled) {
		if (error.code() == error_code_actor_cancelled && !includeCancelled) {
			// Suppress the entire message
			enabled = false;
		} else {
			if (error.isInjectedFault()) {
				detail("ErrorIsInjectedFault", true);
				if (severity == SevError) severity = SevWarnAlways;
			}
			detail("Error", error.name());
			detail("ErrorDescription", error.what());
			detail("ErrorCode", error.code());
		}
	}
	return *this;
}

TraceEvent& TraceEvent::detail( const char* key, const char* value ) {
	if (enabled) {
		if( strlen( value ) > 495 ) {
			char replacement[500];
			strncpy( replacement, value, 495 );
			strcpy( &replacement[495], "\\..." );
			value = replacement;
		}
		writef( " %s=\"", key );
		writeEscaped( value );
		writef( "\"" );
		tmpEventMetric->setField(key, Standalone<StringRef>(StringRef((const uint8_t *)value, strlen(value))));
	}
	return *this;
}
TraceEvent& TraceEvent::detail( const char* key, const std::string& value ) {
	return detail( key, value.c_str() );
}
TraceEvent& TraceEvent::detail( const char* key, double value ) {
	if(enabled)
		tmpEventMetric->setField(key, value);
	return _detailf( key, "%g", value );
}
TraceEvent& TraceEvent::detail( const char* key, int value ) {
	if(enabled)
		tmpEventMetric->setField(key, (int64_t)value);
	return _detailf( key, "%d", value );
}
TraceEvent& TraceEvent::detail( const char* key, unsigned value ) {
	if(enabled)
		tmpEventMetric->setField(key, (int64_t)value);
	return _detailf( key, "%u", value );
}
TraceEvent& TraceEvent::detail( const char* key, long int value ) {
	if(enabled)
		tmpEventMetric->setField(key, (int64_t)value);
	return _detailf( key, "%ld", value );
}
TraceEvent& TraceEvent::detail( const char* key, long unsigned int value ) {
	if(enabled)
		tmpEventMetric->setField(key, (int64_t)value);
	return _detailf( key, "%lu", value );
}
TraceEvent& TraceEvent::detail( const char* key, long long int value ) {
	if(enabled)
		tmpEventMetric->setField(key, (int64_t)value);
	return _detailf( key, "%lld", value );
}
TraceEvent& TraceEvent::detail( const char* key, long long unsigned int value ) {
	if(enabled)
		tmpEventMetric->setField(key, (int64_t)value);
	return _detailf( key, "%llu", value );
}
TraceEvent& TraceEvent::detail( const char* key, NetworkAddress const& value ) {
	return detail( key, value.toString() );
}
TraceEvent& TraceEvent::detail( const char* key, UID const& value ) {
	return detailf( key, "%016llx", value.first() );  // SOMEDAY: Log entire value?  We also do this explicitly in some "lists" in various individual TraceEvent calls
}
TraceEvent& TraceEvent::detailext(const char* key, StringRef const& value) {
	return detail(key, value.printable());
}
TraceEvent& TraceEvent::detailext(const char* key, Optional<Standalone<StringRef>> const& value) {
	return detail(key, (value.present()) ? value.get().printable() : "[not set]");
}
TraceEvent& TraceEvent::detailf( const char* key, const char* valueFormat, ... ) {
	if (enabled) {
		va_list args;
		va_start(args, valueFormat);
		detailfv( key, valueFormat, args, true);  // Write this detail to eventMetric
		va_end(args);
	}
	return *this;
}
TraceEvent& TraceEvent::_detailf( const char* key, const char* valueFormat, ... ) {
	if (enabled) {
		va_list args;
		va_start(args, valueFormat);
		detailfv( key, valueFormat, args, false); // Do NOT write this detail to the event metric, caller of _detailf should do that itself with the appropriate value type
		va_end(args);
	}
	return *this;
}
TraceEvent& TraceEvent::detailfv( const char* key, const char* valueFormat, va_list args, bool writeEventMetricField ) {
	if (enabled) {
		writef( " %s=\"", key );
		va_list args2;
		va_copy(args2, args);
		writeEscapedfv( valueFormat, args );
		writef( "\"" );
		if(writeEventMetricField)
		{
			// TODO: It would be better to make use of the formatted string created in writeEscapedfv above
			char temp[ 1024 ];
			size_t n = vsnprintf(temp, sizeof(temp)-1, valueFormat, args2);
			if(n > sizeof(temp) - 1)
				n = sizeof(temp) - 1;
			temp[n] = 0;
			tmpEventMetric->setField(key, Standalone<StringRef>(StringRef((uint8_t *)temp, n)));
		}
	}
	return *this;
}

TraceEvent& TraceEvent::trackLatest( const char *trackingKey ){
	this->trackingKey = trackingKey;
	ASSERT( this->trackingKey.size() != 0 && this->trackingKey[0] != '/' && this->trackingKey[0] != '\\');
	return *this;
}

TraceEvent& TraceEvent::sample( double sampleRate, bool logSampleRate ) {
	if(!g_random) {
		sampleRate = 1.0;
	}
	else {
		enabled = enabled && g_random->random01() < sampleRate;
	}

	if(enabled && logSampleRate) {
		detail("SampleRate", sampleRate);
	}

	return *this;
}

TraceEvent& TraceEvent::suppressFor( double duration, bool logSuppressedEventCount ) {
	if(g_network) {
		if(isNetworkThread()) {
			int64_t suppressedEventCount = suppressedEvents.checkAndInsertSuppression(type, duration);
			enabled = enabled && suppressedEventCount >= 0;
			if(enabled && logSuppressedEventCount) {
				detail("SuppressedEventCount", suppressedEventCount);
			}
		}
		else {
			TraceEvent(SevError, "SuppressionFromNonNetworkThread");
			detail("__InvalidSuppression__", ""); // Choosing a detail name that is unlikely to collide with other names
		}
	}

	return *this;
}

TraceEvent& TraceEvent::GetLastError() {
#ifdef _WIN32
	return detailf("WinErrorCode", "%x", ::GetLastError());
#elif defined(__unixish__)
	return detailf("UnixErrorCode", "%x", errno).detail("UnixError", strerror(errno));
#endif
}

// We're cheating in counting, as in practice, we only use {10,20,30,40}.
static_assert(SevMaxUsed / 10 + 1 == 5, "Please bump eventCounts[5] to SevMaxUsed/10+1");
unsigned long TraceEvent::eventCounts[5] = {0,0,0,0,0};

unsigned long TraceEvent::CountEventsLoggedAt(Severity sev) {
  return TraceEvent::eventCounts[sev/10];
}

TraceEvent& TraceEvent::backtrace(std::string prefix) {
	if (this->severity == SevError) return *this; // We'll backtrace this later in ~TraceEvent
	return detail((prefix + "Backtrace").c_str(), platform::get_backtrace());
}

TraceEvent::~TraceEvent() {
	try {
		if (enabled) {
			// TRACE_EVENT_THROTTLER
			if (severity > SevDebug && isNetworkThread()) {
				if (traceEventThrottlerCache->isAboveThreshold(StringRef((uint8_t *)type, strlen(type)))) {
					TraceEvent(SevWarnAlways, std::string(TRACE_EVENT_THROTTLE_STARTING_TYPE).append(type).c_str()).suppressFor(5);
					// Throttle Msg
					delete tmpEventMetric;
					return;
				}
				else {
					traceEventThrottlerCache->addAndExpire(StringRef((uint8_t *)type, strlen(type)), 1, now() + FLOW_KNOBS->TRACE_EVENT_THROTLLER_SAMPLE_EXPIRY);
				}
			} // End of Throttler

			_detailf("logGroup", "%.*s", g_traceLog.logGroup.size(), g_traceLog.logGroup.data());
			if (!trackingKey.empty()) {
				if(!isNetworkThread()) {
					TraceEvent(SevError, "TrackLatestFromNonNetworkThread");
					detail("__InvalidTrackLatest__", ""); // Choosing a detail name that is unlikely to collide with other names
				}
				else {
					latestEventCache.set( trackingKey, std::string(buffer, length) + " TrackLatestType=\"Rolled\"/>\r\n" );
				}

				detail("TrackLatestType", "Original");
			}

			if (this->severity == SevError) {
				severity = SevInfo;
				backtrace();
				severity = SevError;
			}
			if (g_traceLog.isOpen()) {
				writef("/>\r\n");
				g_traceLog.write( buffer, length );
				TraceEvent::eventCounts[severity/10]++;

				// Log Metrics
				if(g_traceLog.logTraceEventMetrics && *type != '\0' && isNetworkThread()) {
					// Get the persistent Event Metric representing this trace event and push the fields (details) accumulated in *this to it and then log() it.
					// Note that if the event metric is disabled it won't actually be logged BUT any new fields added to it will be registered.
					// If the event IS logged, a timestamp will be returned, if not then 0.  Either way, pass it through to be used if possible
					// in the Sev* event metrics.

					uint64_t event_ts = DynamicEventMetric::getOrCreateInstance(format("TraceEvent.%s", type), StringRef(), true)->setFieldsAndLogFrom(tmpEventMetric);
					g_traceLog.log(severity, type, id, event_ts);
				}
			}
			if (severity > SevWarnAlways) {
				latestEventCache.setLatestError( std::string(buffer, length) + " latestError=\"1\"/>\r\n" );
			}
		}
	} catch( Error &e ) {
		TraceEvent(SevError, "TraceEventDestructorError").error(e,true);
		delete tmpEventMetric;
		throw;
	}
	delete tmpEventMetric;
}

void TraceEvent::write( int length, const void* bytes ) {
	if (!(this->length + length <= sizeof(buffer))) {
		buffer[300] = 0;
		TraceEvent(SevError, "TraceEventOverflow").detail("TraceFirstBytes", buffer);
		enabled = false;
	} else {
		memcpy( buffer + this->length, bytes, length );
		this->length += length;
	}
}

void TraceEvent::writef( const char* format, ... ) {
	va_list args;
	va_start( args, format );
	int size = sizeof(buffer)-1-length;
	int i = size<0 ? -1 : vsnprintf( buffer + length, size, format, args );
	if( i < 0 || i >= size ) {  // first block catches truncations on windows, second on linux
		buffer[300] = 0;
		TraceEvent(SevError, "TraceEventOverflow").detail("TraceFirstBytes", buffer);
		enabled = false;
	} else {
		length += i;
	}
	va_end(args);
}

void TraceEvent::writeEscaped( const char* data ) {
	while (*data) {
		if (*data == '&') {
			write( 5, "&amp;" );
			data++;
		} else if (*data == '"') {
			write( 6, "&quot;" );
			data++;
		} else if (*data == '<') {
			write( 4, "&lt;" );
			data++;
		} else if (*data == '>') {
			write( 4, "&gt;" );
			data++;
		} else {
			const char* e = data;
			while (*e && *e != '"' && *e != '&' && *e != '<' && *e != '>') e++;
			write( e-data, data );
			data = e;
		}
	}
}

void TraceEvent::writeEscapedfv( const char* format, va_list args ) {
	char temp[ 1024 ];
	vsnprintf(temp, sizeof(temp)-1, format, args);
	temp[sizeof(temp)-1] = 0;
	writeEscaped( temp );
}

thread_local bool TraceEvent::networkThread = false;

void TraceEvent::setNetworkThread() {
	traceEventThrottlerCache = new TransientThresholdMetricSample<Standalone<StringRef>>(FLOW_KNOBS->TRACE_EVENT_METRIC_UNITS_PER_SAMPLE, FLOW_KNOBS->TRACE_EVENT_THROTTLER_MSG_LIMIT);
	networkThread = true;
}

bool TraceEvent::isNetworkThread() {
	return networkThread;
}

TraceInterval& TraceInterval::begin() {
	pairID = trace_random->randomUniqueID();
	count = 0;
	return *this;
}

void TraceBatch::addEvent( const char *name, uint64_t id, const char *location ) {
	eventBatch.push_back( EventInfo(g_trace_clock == TRACE_CLOCK_NOW ? now() : timer(), name, id, location));
	if( g_network->isSimulated() || FLOW_KNOBS->AUTOMATIC_TRACE_DUMP )
		dump();
}

void TraceBatch::addAttach( const char *name, uint64_t id, uint64_t to ) {
	attachBatch.push_back( AttachInfo(g_trace_clock == TRACE_CLOCK_NOW ? now() : timer(), name, id, to));
	if( g_network->isSimulated() || FLOW_KNOBS->AUTOMATIC_TRACE_DUMP )
		dump();
}

void TraceBatch::addBuggify( int activated, int line, std::string file ) {
	if( g_network ) {
		buggifyBatch.push_back( BuggifyInfo(g_trace_clock == TRACE_CLOCK_NOW ? now() : timer(), activated, line, file));
		if( g_network->isSimulated() || FLOW_KNOBS->AUTOMATIC_TRACE_DUMP )
			dump();
	} else {
		buggifyBatch.push_back( BuggifyInfo(0, activated, line, file));
	}
}

void TraceBatch::dump() {
	if (!g_traceLog.isOpen())
		return;
	NetworkAddress local = g_network->getLocalAddress();

	for(int i = 0; i < attachBatch.size(); i++) {
		char buffer[256];
		int length = sprintf(buffer, "<Event Severity=\"%d\" Time=\"%.6f\" Type=\"%s\" Machine=\"%d.%d.%d.%d:%d\" logGroup=\"%.*s\" ID=\"%016" PRIx64 "\" To=\"%016" PRIx64 "\"/>\r\n",
			(int)SevInfo, attachBatch[i].time, attachBatch[i].name, (local.ip>>24)&0xff,(local.ip>>16)&0xff,(local.ip>>8)&0xff,local.ip&0xff,local.port, g_traceLog.logGroup.size(), g_traceLog.logGroup.data(),
			attachBatch[i].id, attachBatch[i].to);
		g_traceLog.write( buffer, length );
	}

	for(int i = 0; i < eventBatch.size(); i++) {
		char buffer[256];
		int length = sprintf(buffer, "<Event Severity=\"%d\" Time=\"%.6f\" Type=\"%s\" Machine=\"%d.%d.%d.%d:%d\" logGroup=\"%.*s\" ID=\"%016" PRIx64 "\" Location=\"%s\"/>\r\n",
			(int)SevInfo, eventBatch[i].time, eventBatch[i].name, (local.ip>>24)&0xff,(local.ip>>16)&0xff,(local.ip>>8)&0xff,local.ip&0xff,local.port, g_traceLog.logGroup.size(), g_traceLog.logGroup.data(),
			eventBatch[i].id, eventBatch[i].location );
		g_traceLog.write( buffer, length );
	}

	for(int i = 0; i < buggifyBatch.size(); i++) {
		char buffer[256];
		int length = sprintf( buffer, "<Event Severity=\"%d\" Time=\"%.6f\" Type=\"BuggifySection\" Machine=\"%d.%d.%d.%d:%d\" logGroup=\"%.*s\" Activated=\"%d\" File=\"%s\" Line=\"%d\"/>\r\n",
			(int)SevInfo, buggifyBatch[i].time, (local.ip>>24)&0xff,(local.ip>>16)&0xff,(local.ip>>8)&0xff,local.ip&0xff,local.port, g_traceLog.logGroup.size(), g_traceLog.logGroup.data(),
			buggifyBatch[i].activated, buggifyBatch[i].file.c_str(), buggifyBatch[i].line );
		g_traceLog.write( buffer, length );
	}

	g_traceLog.flush();
	eventBatch.clear();
	attachBatch.clear();
	buggifyBatch.clear();
}
