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
#include "fdbclient/FDBTypes.h"
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
		ASSERT(g_network);
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

struct XmlTraceLogFormatter : public TraceLogFormatter {
	const char* getExtension() {
		return "xml";
	}

	const char* getHeader() {
		return "<?xml version=\"1.0\"?>\r\n<Trace>\r\n";
	}

	const char* getFooter() {
		return "</Trace>\r\n";
	}

	std::string escape(std::string source) {
		std::string result;
		int offset = 0;
		loop {
			int index = source.find_first_of(std::string({'&', '"', '<', '>', '\r', '\n', '\0'}), offset);
			if(index == source.npos) {
				break;
			}

			result += source.substr(offset, index-offset);
			if(source[index] == '&') {
				result += "&amp;";
			}
			else if(source[index] == '"') {
				result += "&quot;";
			}
			else if(source[index] == '<') {
				result += "&lt;";
			}
			else if(source[index] == '>') {
				result += "&gt;";
			}
			else if(source[index] == '\n' || source[index] == '\r') {
				result += " ";
			}
			else if(source[index] == '\0') {
				result += " ";
				TraceEvent(SevWarnAlways, "StrippedIllegalCharacterFromTraceEvent").detail("Source", printable(source)).detail("Character", printable(source.substr(index, 1)));
			}
			else {
				ASSERT(false);
			}

			offset = index+1;
		}

		if(offset == 0) {
			return source;
		}
		else {
			result += source.substr(offset);
			return result;
		}
	}

	std::string formatEvent(TraceEventFields fields) {
		std::string event = "<Event ";

		for(auto itr : fields) {
			event += escape(itr.first) + "=\"" + escape(itr.second) + "\" ";
		}

		event += "/>\r\n";
		return event;
	}
};

struct TraceLog {

private:
	TraceLogFormatter *formatter = new XmlTraceLogFormatter(); // TODO: Memory handling
	Standalone< VectorRef<StringRef> > buffer;
	int file_length;
	int buffer_length;
	bool opened;
	int64_t preopenOverflowCount;
	std::string basename;
	std::string logGroup;

	std::string directory;
	std::string processName;
	Optional<NetworkAddress> localAddress;

	Reference<IThreadPool> writer;
	uint64_t rollsize;
	Mutex mutex;

	EventMetricHandle<TraceEventNameID> SevErrorNames;
	EventMetricHandle<TraceEventNameID> SevWarnAlwaysNames;
	EventMetricHandle<TraceEventNameID> SevWarnNames;
	EventMetricHandle<TraceEventNameID> SevInfoNames;
	EventMetricHandle<TraceEventNameID> SevDebugNames;

public:
	bool logTraceEventMetrics;

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
		WriterThread( std::string directory, std::string processName, uint32_t maxLogsSize, std::string basename, std::string logGroup, Optional<NetworkAddress> localAddress, Reference<BarrierList> barriers, TraceLogFormatter *formatter ) 
			: directory(directory), processName(processName), maxLogsSize(maxLogsSize), basename(basename), logGroup(logGroup), localAddress(localAddress), traceFileFD(0), index(0), barriers(barriers), formatter(formatter) {}

		virtual void init() {}

		TraceLogFormatter *formatter;
		Reference<BarrierList> barriers;
		int traceFileFD;
		std::string directory;
		std::string processName;
		int maxLogsSize;
		std::string basename;
		std::string logGroup;
		Optional<NetworkAddress> localAddress;
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
			close();
			cleanupTraceFiles();

			auto finalname = format("%s.%d.%s", basename.c_str(), ++index, formatter->getExtension());
			while ( (traceFileFD = __open( finalname.c_str(), TRACEFILE_FLAGS, TRACEFILE_MODE )) == -1 ) {
				lastError(errno);
				if (errno == EEXIST)
					finalname = format("%s.%d.%s", basename.c_str(), ++index, formatter->getExtension());
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

			writeReliable(formatter->getHeader());
		}

		struct Close : TypedAction<WriterThread,Close> {
			virtual double getTimeEstimate() { return 0; }
		};
		void action( Close& c ) {
			close();
		}

		void close() {
			if (traceFileFD) {
				writeReliable(formatter->getFooter());
				while ( __close(traceFileFD) ) threadSleep(0.1);
			}
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
				for ( auto i = a.buffer.begin(); i != a.buffer.end(); ++i ) {
					writeReliable(i->begin(), i->size());
				}

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

	TraceLog() : buffer_length(0), file_length(0), opened(false), preopenOverflowCount(0), barriers(new BarrierList), logTraceEventMetrics(false) {}

	bool isOpen() const { return opened; }

	void open( std::string const& directory, std::string const& processName, std::string logGroup, std::string const& timestamp, uint64_t rs, uint64_t maxLogsSize, Optional<NetworkAddress> na ) {
		ASSERT( !writer && !opened );

		this->directory = directory;
		this->processName = processName;
		this->localAddress = na;

		basename = format("%s/%s.%s.%s", directory.c_str(), processName.c_str(), timestamp.c_str(), g_random->randomAlphaNumeric(6).c_str());

		if ( g_network->isSimulated() )
			writer = Reference<IThreadPool>(new DummyThreadPool());
		else
			writer = createGenericThreadPool();
		writer->addThread( new WriterThread(directory, processName, maxLogsSize, basename, logGroup, localAddress, barriers, formatter) );

		rollsize = rs;

		auto a = new WriterThread::Open;
		writer->post(a);

		MutexHolder holder(mutex);
		if(g_network->isSimulated()) {
			// We don't support early trace logs in simulation.
			// This is because we don't know if we're being simulated prior to the network being created, which causes two ambiguities:
			//
			// 1. We need to employ two different methods to determine the time of an event prior to the network starting for real-world and simulated runs.
			// 2. Simulated runs manually insert the Machine field at TraceEvent creation time. Real-world runs add this field at write time.
			//
			// Without the ability to resolve the ambiguity, we've chosen to always favor the real-world approach and not support such events in simulation.
			buffer = Standalone<VectorRef<StringRef>>();
		}

		opened = true;
		if(preopenOverflowCount > 0) {
			TraceEvent(SevWarn, "TraceLogPreopenOverflow").detail("OverflowEventCount", preopenOverflowCount);
			preopenOverflowCount = 0;
		}
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

	void writeEvent( TraceEventFields fields, std::string trackLatestKey, bool trackError ) {
		fields.addField("logGroup", logGroup);
		if(localAddress.present()) {
			fields.addField("Machine", format("%d.%d.%d.%d:%d", (localAddress.get().ip>>24)&0xff, (localAddress.get().ip>>16)&0xff, (localAddress.get().ip>>8)&0xff, localAddress.get().ip&0xff, localAddress.get().port));
		}

		if(!trackLatestKey.empty()) {
			fields.addField("TrackLatestType", "Original");
		}

		std::string eventStr = formatter->formatEvent(fields);

		MutexHolder hold(mutex);
		if(!isOpen() && (preopenOverflowCount > 0 || buffer_length + eventStr.size() > FLOW_KNOBS->TRACE_LOG_MAX_PREOPEN_BUFFER)) {
			++preopenOverflowCount;
			return;
		}

		// FIXME: What if we are using way too much memory for buffer?
		buffer.push_back_deep( buffer.arena(), StringRef((const uint8_t*)eventStr.c_str(), eventStr.size()) );
		buffer_length += eventStr.size();

		if(trackError) {
			latestEventCache.setLatestError(fields);
		}
		if(!trackLatestKey.empty()) {
			latestEventCache.set(trackLatestKey, fields);
		}
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
			std::vector<TraceEventFields> events = latestEventCache.getAllUnsafe();
			for (int idx = 0; idx < events.size(); idx++) {
				if(events[idx].size() > 0) {
					TraceEventFields rolledFields = events[idx];

					// TODO: do this better
					std::string originalTime;
					for(int i = 0; i < rolledFields.size(); ++i) {
						if(rolledFields[i].first == "Time") {
							rolledFields.addField("OriginalTime", rolledFields[i].second);
							rolledFields.replaceValue(i, format("%.6f", (g_trace_clock == TRACE_CLOCK_NOW) ? now() : timer()));
						}
						else if(rolledFields[i].first == "TrackLatestType") {
							rolledFields.replaceValue(i, "Rolled");
						}
					}

					buffer.push_back_deep( buffer.arena(), StringRef(formatter->formatEvent(rolledFields)) );
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
			MutexHolder hold(mutex);

			// Write remaining contents
			auto a = new WriterThread::WriteBuffer( std::move(buffer) );
			file_length += buffer_length;
			buffer = Standalone<VectorRef<StringRef>>();
			buffer_length = 0;
			writer->post( a );

			auto c = new WriterThread::Close();
			writer->post( c );

			ThreadFuture<Void> f(new ThreadSingleAssignmentVar<Void>);
			barriers->push(f);
			writer->post( new WriterThread::Barrier );

			f.getBlocking();

			opened = false;
		}
	}

	~TraceLog() {
		close();
		if (writer) writer->addref(); // FIXME: We are not shutting down the writer thread at all, because the ThreadPool shutdown mechanism is blocking (necessarily waits for current work items to finish) and we might not be able to finish everything.
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
void clearPrefix_internal( std::map<std::string, TraceEventFields>& data, std::string prefix ) {
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

void LatestEventCache::set( std::string tag, TraceEventFields contents ) {
	latest[getAddressIndex()][tag] = contents;
}

TraceEventFields LatestEventCache::get( std::string tag ) {
	return latest[getAddressIndex()][tag];
}

std::vector<TraceEventFields> allEvents( std::map<std::string, TraceEventFields> const& data ) {
	std::vector<TraceEventFields> all;
	for(auto it = data.begin(); it != data.end(); it++) {
		all.push_back( it->second );
	}
	return all;
}

std::vector<TraceEventFields> LatestEventCache::getAll() {
	return allEvents( latest[getAddressIndex()] );
}

// if in simulation, all events from all machines will be returned
std::vector<TraceEventFields> LatestEventCache::getAllUnsafe() {
	std::vector<TraceEventFields> all;
	for(auto it = latest.begin(); it != latest.end(); ++it) {
		auto m = allEvents( it->second );
		all.insert( all.end(), m.begin(), m.end() );
	}
	return all;
}

void LatestEventCache::setLatestError( TraceEventFields contents ) {
	if(TraceEvent::isNetworkThread()) { // The latest event cache doesn't track errors that happen on other threads
		latestErrors[getAddressIndex()] = contents;
	}
}

TraceEventFields LatestEventCache::getLatestError() {
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
	g_traceLog.open( directory, baseName, logGroup, format("%lld", time(NULL)), rollsize, maxLogsSize, !g_network->isSimulated() ? na : Optional<NetworkAddress>());

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
	if( severity < FLOW_KNOBS->MIN_TRACE_SEVERITY) return false;
	StringRef s( (const uint8_t*)type, strlen(type) );
	return !suppress.count(s);
}

TraceEvent::TraceEvent( const char* type, UID id ) : id(id) {
	init(SevInfo, type);
	detail("ID", id);
}

TraceEvent::TraceEvent( Severity severity, const char* type, UID id ) : id(id) {
	init(severity, type);
	detail("ID", id);
}

TraceEvent::TraceEvent(const char* type, const StringRef& zoneId) {
	id = UID(hashlittle(zoneId.begin(), zoneId.size(), 0), 0);
	init(SevInfo, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent(Severity severity, const char* type, const StringRef& zoneId) {
	id = UID(hashlittle(zoneId.begin(), zoneId.size(), 0), 0);
	init(severity, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent(const char* type, const Optional<Standalone<StringRef>>& zoneId) {
	id = zoneId.present() ? UID(hashlittle(zoneId.get().begin(), zoneId.get().size(), 0), 0) : UID(-1LL,0);
	init(SevInfo, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent(Severity severity, const char* type, const Optional<Standalone<StringRef>>& zoneId) {
	id = zoneId.present() ? UID(hashlittle(zoneId.get().begin(), zoneId.get().size(), 0), 0) : UID(-1LL, 0);
	init(severity, type);
	detailext("ID", zoneId);
}

TraceEvent::TraceEvent( TraceInterval& interval, UID id ) : id(id) {
	init(interval.severity, interval);
	detail("ID", id);
}

TraceEvent::TraceEvent( Severity severity, TraceInterval& interval, UID id ) : id(id) {
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
	ASSERT(*type != '\0');

	this->type = type;
	this->severity = severity;
	tmpEventMetric = new DynamicEventMetric(MetricNameRef());

	length = 0;
	if (isEnabled(type, severity)) {
		enabled = true;

		double time;
		if(g_trace_clock == TRACE_CLOCK_NOW) {
			if(!g_network) {
				static double preNetworkTime = timer_monotonic();
				time = preNetworkTime;
			}
			else {
				time = now();
			}
		}
		else {
			time = timer();
		}

		detail("Severity", severity);
		detailf("Time", "%.6f", time);
		detail("Type", type);
		if(g_network && g_network->isSimulated()) {
			NetworkAddress local = g_network->getLocalAddress();
			detailf("Machine", "%d.%d.%d.%d:%d", (local.ip>>24)&0xff, (local.ip>>16)&0xff, (local.ip>>8)&0xff, local.ip&0xff, local.port);
		}
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

TraceEvent& TraceEvent::detailImpl( std::string key, std::string value, bool writeEventMetricField) {
	if (enabled) {
		if( value.size() > 495 ) {
			value = value.substr(0, 495) + "...";
		}

		fields.addField(key, value);
		length += key.size() + value.size();

		if(length > FLOW_KNOBS->TRACE_EVENT_MAX_SIZE) {
			TraceEvent(SevError, "TraceEventOverflow").detail("TraceFirstBytes", fields.toString().substr(300));	
			enabled = false;
		}
		else if(writeEventMetricField) {
			tmpEventMetric->setField(key.c_str(), Standalone<StringRef>(StringRef(value)));
		}
	}
	return *this;
}

TraceEvent& TraceEvent::detail( std::string key, std::string value ) {
	return detailImpl(key, value);
}
TraceEvent& TraceEvent::detail( std::string key, double value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), value);
	return detailfNoMetric( key, "%g", value );
}
TraceEvent& TraceEvent::detail( std::string key, int value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), (int64_t)value);
	return detailfNoMetric( key, "%d", value );
}
TraceEvent& TraceEvent::detail( std::string key, unsigned value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), (int64_t)value);
	return detailfNoMetric( key, "%u", value );
}
TraceEvent& TraceEvent::detail( std::string key, long int value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), (int64_t)value);
	return detailfNoMetric( key, "%ld", value );
}
TraceEvent& TraceEvent::detail( std::string key, long unsigned int value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), (int64_t)value);
	return detailfNoMetric( key, "%lu", value );
}
TraceEvent& TraceEvent::detail( std::string key, long long int value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), (int64_t)value);
	return detailfNoMetric( key, "%lld", value );
}
TraceEvent& TraceEvent::detail( std::string key, long long unsigned int value ) {
	if(enabled)
		tmpEventMetric->setField(key.c_str(), (int64_t)value);
	return detailfNoMetric( key, "%llu", value );
}
TraceEvent& TraceEvent::detail( std::string key, NetworkAddress const& value ) {
	return detailImpl( key, value.toString() );
}
TraceEvent& TraceEvent::detail( std::string key, UID const& value ) {
	return detailf( key, "%016llx", value.first() );  // SOMEDAY: Log entire value?  We also do this explicitly in some "lists" in various individual TraceEvent calls
}
TraceEvent& TraceEvent::detailext( std::string key, StringRef const& value ) {
	return detailImpl(key, value.printable());
}
TraceEvent& TraceEvent::detailext( std::string key, Optional<Standalone<StringRef>> const& value ) {
	return detailImpl(key, (value.present()) ? value.get().printable() : "[not set]");
}
TraceEvent& TraceEvent::detailf( std::string key, const char* valueFormat, ... ) {
	if (enabled) {
		va_list args;
		va_start(args, valueFormat);
		std::string value;
		int result = vsformat(value, valueFormat, args);
		va_end(args);

		ASSERT(result >= 0);
		detailImpl(key, value);
	}
	return *this;
}
TraceEvent& TraceEvent::detailfNoMetric( std::string key, const char* valueFormat, ... ) {
	if (enabled) {
		va_list args;
		va_start(args, valueFormat);
		std::string value;
		int result = vsformat(value, valueFormat, args);
		va_end(args);

		ASSERT(result >= 0);
		detailImpl(key, value, false); // Do NOT write this detail to the event metric, caller of detailfNoMetric should do that itself with the appropriate value type
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
			if (g_network && severity > SevDebug && isNetworkThread()) {
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

			if (this->severity == SevError) {
				severity = SevInfo;
				backtrace();
				severity = SevError;
			}

			TraceEvent::eventCounts[severity/10]++;
			g_traceLog.writeEvent( fields, trackingKey, severity > SevWarnAlways );

			if (g_traceLog.isOpen()) {
				// Log Metrics
				if(g_traceLog.logTraceEventMetrics && isNetworkThread()) {
					// Get the persistent Event Metric representing this trace event and push the fields (details) accumulated in *this to it and then log() it.
					// Note that if the event metric is disabled it won't actually be logged BUT any new fields added to it will be registered.
					// If the event IS logged, a timestamp will be returned, if not then 0.  Either way, pass it through to be used if possible
					// in the Sev* event metrics.

					uint64_t event_ts = DynamicEventMetric::getOrCreateInstance(format("TraceEvent.%s", type), StringRef(), true)->setFieldsAndLogFrom(tmpEventMetric);
					g_traceLog.log(severity, type, id, event_ts);
				}
			}
		}
	} catch( Error &e ) {
		TraceEvent(SevError, "TraceEventDestructorError").error(e,true);
		delete tmpEventMetric;
		throw;
	}
	delete tmpEventMetric;
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
	std::string machine;
	if(g_network->isSimulated()) {
		NetworkAddress local = g_network->getLocalAddress();
		machine = format("%d.%d.%d.%d:%d", (local.ip>>24)&0xff,(local.ip>>16)&0xff,(local.ip>>8)&0xff,local.ip&0xff,local.port);
	}

	for(int i = 0; i < attachBatch.size(); i++) {
		if(g_network->isSimulated()) {
			attachBatch[i].fields.addField("Machine", machine);
		}
		g_traceLog.writeEvent(attachBatch[i].fields, "", false);
	}

	for(int i = 0; i < eventBatch.size(); i++) {
		if(g_network->isSimulated()) {
			eventBatch[i].fields.addField("Machine", machine);
		}
		g_traceLog.writeEvent(eventBatch[i].fields, "", false);
	}

	for(int i = 0; i < buggifyBatch.size(); i++) {
		if(g_network->isSimulated()) {
			buggifyBatch[i].fields.addField("Machine", machine);
		}
		g_traceLog.writeEvent(buggifyBatch[i].fields, "", false);
	}

	g_traceLog.flush();
	eventBatch.clear();
	attachBatch.clear();
	buggifyBatch.clear();
}

TraceBatch::EventInfo::EventInfo(double time, const char *name, uint64_t id, const char *location) {
	fields.addField("Severity", format("%d", (int)SevInfo));
	fields.addField("Time", format("%.6f", time));
	fields.addField("Type", name);
	fields.addField("ID", format("%016" PRIx64, id));
	fields.addField("Location", location);
}

TraceBatch::AttachInfo::AttachInfo(double time, const char *name, uint64_t id, uint64_t to) {
	fields.addField("Severity", format("%d", (int)SevInfo));
	fields.addField("Time", format("%.6f", time));
	fields.addField("Type", name);
	fields.addField("ID", format("%016" PRIx64, id));
	fields.addField("To", format("%016" PRIx64, to));
}

TraceBatch::BuggifyInfo::BuggifyInfo(double time, int activated, int line, std::string file) {
	fields.addField("Severity", format("%d", (int)SevInfo));
	fields.addField("Time", format("%.6f", time));
	fields.addField("Type", "BuggifySection");
	fields.addField("Activated", format("%d", activated));
	fields.addField("File", file);
	fields.addField("Line", format("%d", line));
}

void TraceEventFields::addField(std::string key, std::string value) {
	fields.push_back(std::make_pair(key, value));
}

size_t TraceEventFields::size() const {
	return fields.size();
}

TraceEventFields::FieldIterator TraceEventFields::begin() const {
	return fields.cbegin();
}

TraceEventFields::FieldIterator TraceEventFields::end() const {
	return fields.cend();
}

const TraceEventFields::Field &TraceEventFields::operator[] (int index) const {
	ASSERT(index >= 0 && index < size());
	return fields.at(index);
}

void TraceEventFields::replaceValue(int index, std::string value) {
	ASSERT(index >= 0 && index < size());
	fields[index].second = value;
}

bool TraceEventFields::tryGetValue(std::string key, std::string &outValue) const {
	for(auto itr = begin(); itr != end(); ++itr) {
		if(itr->first == key) {
			outValue = itr->second;
			return true;
		}
	}

	return false;
}

std::string TraceEventFields::getValue(std::string key) const {
	std::string value;
	if(tryGetValue(key, value)) {
		return value;
	}
	else {
		throw attribute_not_found();
	}
}

std::string TraceEventFields::toString() const {
	std::string str;
	bool first = true;
	for(auto itr = begin(); itr != end(); ++itr) {
		if(!first) {
			str += ", ";
		}
		first = false;

		str += format("\"%s\"=\"%s\"", itr->first.c_str(), itr->second.c_str());
	}

	return str;
}
