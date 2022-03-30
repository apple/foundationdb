/*
 * Trace.cpp
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

#include "flow/Trace.h"
#include "flow/FileTraceLogWriter.h"
#include "flow/Knobs.h"
#include "flow/XmlTraceLogFormatter.h"
#include "flow/JsonTraceLogFormatter.h"
#include "flow/flow.h"
#include "flow/DeterministicRandom.h"
#include <stdlib.h>
#include <stdarg.h>
#include <cctype>
#include <time.h>
#include <set>
#include <iomanip>
#include "flow/IThreadPool.h"
#include "flow/ThreadHelper.actor.h"
#include "flow/FastRef.h"
#include "flow/EventTypes.actor.h"
#include "flow/TDMetric.actor.h"
#include "flow/MetricSample.h"

#ifdef _WIN32
#include <windows.h>
#undef max
#undef min
#endif

// Allocations can only be logged when this value is 0.
// Anybody that needs to disable tracing should increment this by 1 for the duration
// that they need the disabling to be in effect.
//
// This is done for multiple reasons:
// 1. To avoid recursion in the allocation tracing when each trace event does an allocation
// 2. To avoid a historically documented but unknown crash that occurs when logging allocations
//    during an open trace event
thread_local int g_allocation_tracing_disabled = 1;

ITraceLogIssuesReporter::~ITraceLogIssuesReporter() {}

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
		if (suppressionMap.size() >= FLOW_KNOBS->MAX_TRACE_SUPPRESSIONS) {
			TraceEvent(SevWarnAlways, "ClearingTraceSuppressionMap").log();
			suppressionMap.clear();
		}

		auto insertion = suppressionMap.insert(std::make_pair(type, SuppressionInfo()));
		if (insertion.second || insertion.first->second.endTime <= now()) {
			int64_t suppressedEventCount = insertion.first->second.suppressedEventCount;
			insertion.first->second.endTime = now() + duration;
			insertion.first->second.suppressedEventCount = 0;
			return suppressedEventCount;
		} else {
			++insertion.first->second.suppressedEventCount;
			return -1;
		}
	}
};

#define TRACE_BATCH_IMPLICIT_SEVERITY SevInfo
TraceBatch g_traceBatch;
std::atomic<trace_clock_t> g_trace_clock{ TRACE_CLOCK_NOW };

LatestEventCache latestEventCache;
SuppressionMap suppressedEvents;

static TransientThresholdMetricSample<Standalone<StringRef>>* traceEventThrottlerCache;
static const char* TRACE_EVENT_THROTTLE_STARTING_TYPE = "TraceEventThrottle_";
static const char* TRACE_EVENT_INVALID_SUPPRESSION = "InvalidSuppression_";
static int TRACE_LOG_MAX_PREOPEN_BUFFER = 1000000;

struct TraceLog {
	Reference<ITraceLogFormatter> formatter;

private:
	Reference<ITraceLogWriter> logWriter;
	std::vector<TraceEventFields> eventBuffer;
	int loggedLength;
	int bufferLength;
	std::atomic<bool> opened;
	int64_t preopenOverflowCount;
	std::string basename;
	std::string logGroup;
	std::map<std::string, std::string> universalFields;

	std::string directory;
	std::string processName;
	Optional<NetworkAddress> localAddress;
	std::string tracePartialFileSuffix;

	Reference<IThreadPool> writer;
	uint64_t rollsize;
	Mutex mutex;

	EventMetricHandle<TraceEventNameID> SevErrorNames;
	EventMetricHandle<TraceEventNameID> SevWarnAlwaysNames;
	EventMetricHandle<TraceEventNameID> SevWarnNames;
	EventMetricHandle<TraceEventNameID> SevInfoNames;
	EventMetricHandle<TraceEventNameID> SevDebugNames;

	struct RoleInfo {
		std::map<std::string, int> roles;
		std::string rolesString;

		void refreshRolesString() {
			rolesString = "";
			for (auto itr : roles) {
				if (!rolesString.empty()) {
					rolesString += ",";
				}
				rolesString += itr.first;
			}
		}
	};

	RoleInfo roleInfo;
	std::map<NetworkAddress, RoleInfo> roleInfoMap;

	RoleInfo& mutateRoleInfo() {
		ASSERT(g_network);

		if (g_network->isSimulated()) {
			return roleInfoMap[g_network->getLocalAddress()];
		}

		return roleInfo;
	}

public:
	bool logTraceEventMetrics;

	void initMetrics() {
		SevErrorNames.init(LiteralStringRef("TraceEvents.SevError"));
		SevWarnAlwaysNames.init(LiteralStringRef("TraceEvents.SevWarnAlways"));
		SevWarnNames.init(LiteralStringRef("TraceEvents.SevWarn"));
		SevInfoNames.init(LiteralStringRef("TraceEvents.SevInfo"));
		SevDebugNames.init(LiteralStringRef("TraceEvents.SevDebug"));
		logTraceEventMetrics = true;
	}

	struct BarrierList : ThreadSafeReferenceCounted<BarrierList> {
		BarrierList() : ntriggered(0) {}
		void push(ThreadFuture<Void> f) {
			MutexHolder h(mutex);
			barriers.push_back(f);
		}
		void pop() {
			MutexHolder h(mutex);
			unsafeTrigger(0);
			barriers.pop_front();
			if (ntriggered)
				ntriggered--;
		}
		void triggerAll() {
			MutexHolder h(mutex);
			for (uint32_t i = ntriggered; i < barriers.size(); i++)
				unsafeTrigger(i);
			ntriggered = barriers.size();
		}

	private:
		Mutex mutex;
		Deque<ThreadFuture<Void>> barriers;
		int ntriggered;
		void unsafeTrigger(int i) {
			auto b = ((ThreadSingleAssignmentVar<Void>*)barriers[i].getPtr());
			if (!b->isReady())
				b->send(Void());
		}
	};

	Reference<IssuesList> issues;

	Reference<BarrierList> barriers;

	struct WriterThread final : IThreadPoolReceiver {
		WriterThread(Reference<BarrierList> barriers,
		             Reference<ITraceLogWriter> logWriter,
		             Reference<ITraceLogFormatter> formatter)
		  : logWriter(logWriter), formatter(formatter), barriers(barriers) {}

		void init() override {}

		Reference<ITraceLogWriter> logWriter;
		Reference<ITraceLogFormatter> formatter;
		Reference<BarrierList> barriers;

		struct Open final : TypedAction<WriterThread, Open> {
			double getTimeEstimate() const override { return 0; }
		};
		void action(Open& o) {
			logWriter->open();
			logWriter->write(formatter->getHeader());
		}

		struct Close final : TypedAction<WriterThread, Close> {
			double getTimeEstimate() const override { return 0; }
		};
		void action(Close& c) {
			logWriter->write(formatter->getFooter());
			logWriter->close();
		}

		struct Roll final : TypedAction<WriterThread, Roll> {
			double getTimeEstimate() const override { return 0; }
		};
		void action(Roll& c) {
			logWriter->write(formatter->getFooter());
			logWriter->roll();
			logWriter->write(formatter->getHeader());
		}

		struct Barrier final : TypedAction<WriterThread, Barrier> {
			double getTimeEstimate() const override { return 0; }
		};
		void action(Barrier& a) { barriers->pop(); }

		struct WriteBuffer final : TypedAction<WriterThread, WriteBuffer> {
			std::vector<TraceEventFields> events;

			WriteBuffer(std::vector<TraceEventFields> events) : events(events) {}
			double getTimeEstimate() const override { return .001; }
		};
		void action(WriteBuffer& a) {
			for (auto event : a.events) {
				event.validateFormat();
				logWriter->write(formatter->formatEvent(event));
			}

			if (FLOW_KNOBS->TRACE_SYNC_ENABLED) {
				logWriter->sync();
			}
		}

		struct Ping final : TypedAction<WriterThread, Ping> {
			ThreadReturnPromise<Void> ack;

			explicit Ping(){};
			double getTimeEstimate() const override { return 0; }
		};
		void action(Ping& ping) {
			try {
				ping.ack.send(Void());
			} catch (Error& e) {
				TraceEvent(SevError, "CrashDebugPingActionFailed").error(e);
				throw;
			}
		}
	};

	TraceLog()
	  : formatter(new XmlTraceLogFormatter()), loggedLength(0), bufferLength(0), opened(false), preopenOverflowCount(0),
	    logTraceEventMetrics(false), issues(new IssuesList), barriers(new BarrierList) {}

	bool isOpen() const { return opened; }

	void open(std::string const& directory,
	          std::string const& processName,
	          std::string logGroup,
	          std::string const& timestamp,
	          uint64_t rs,
	          uint64_t maxLogsSize,
	          Optional<NetworkAddress> na,
	          std::string const& tracePartialFileSuffix) {
		ASSERT(!writer && !opened);

		this->directory = directory;
		this->processName = processName;
		this->logGroup = logGroup;
		this->localAddress = na;
		this->tracePartialFileSuffix = tracePartialFileSuffix;

		basename = format("%s/%s.%s.%s",
		                  directory.c_str(),
		                  processName.c_str(),
		                  timestamp.c_str(),
		                  deterministicRandom()->randomAlphaNumeric(6).c_str());
		logWriter = Reference<ITraceLogWriter>(new FileTraceLogWriter(
		    directory,
		    processName,
		    basename,
		    formatter->getExtension(),
		    tracePartialFileSuffix,
		    maxLogsSize,
		    [this]() { barriers->triggerAll(); },
		    issues));

		if (g_network->isSimulated())
			writer = Reference<IThreadPool>(new DummyThreadPool());
		else
			writer = createGenericThreadPool();
		writer->addThread(new WriterThread(barriers, logWriter, formatter), "fdb-trace-log");

		rollsize = rs;

		auto a = new WriterThread::Open;
		writer->post(a);

		MutexHolder holder(mutex);
		if (g_network->isSimulated()) {
			// We don't support early trace logs in simulation.
			// This is because we don't know if we're being simulated prior to the network being created, which causes
			// two ambiguities:
			//
			// 1. We need to employ two different methods to determine the time of an event prior to the network
			// starting for real-world and simulated runs.
			// 2. Simulated runs manually insert the Machine field at TraceEvent creation time. Real-world runs add this
			// field at write time.
			//
			// Without the ability to resolve the ambiguity, we've chosen to always favor the real-world approach and
			// not support such events in simulation.
			eventBuffer.clear();
		}

		opened = true;
		for (TraceEventFields& fields : eventBuffer) {
			annotateEvent(fields);
		}

		if (preopenOverflowCount > 0) {
			TraceEvent(SevWarn, "TraceLogPreopenOverflow").detail("OverflowEventCount", preopenOverflowCount);
			preopenOverflowCount = 0;
		}
	}

	void annotateEvent(TraceEventFields& fields) {
		MutexHolder holder(mutex);
		if (!opened || fields.isAnnotated())
			return;
		if (localAddress.present()) {
			fields.addField("Machine", formatIpPort(localAddress.get().ip, localAddress.get().port));
		}
		fields.addField("LogGroup", logGroup);

		RoleInfo const& r = mutateRoleInfo();
		if (r.rolesString.size() > 0) {
			fields.addField("Roles", r.rolesString);
		}

		for (auto const& field : universalFields) {
			fields.addField(field.first, field.second);
		}

		fields.setAnnotated();
	}

	void writeEvent(TraceEventFields fields, std::string trackLatestKey, bool trackError) {
		MutexHolder hold(mutex);

		annotateEvent(fields);

		if (!trackLatestKey.empty()) {
			fields.addField("TrackLatestType", "Original");
		}

		if (!isOpen() &&
		    (preopenOverflowCount > 0 || bufferLength + fields.sizeBytes() > TRACE_LOG_MAX_PREOPEN_BUFFER)) {
			++preopenOverflowCount;
			return;
		}

		// FIXME: What if we are using way too much memory for buffer?
		ASSERT(!isOpen() || fields.isAnnotated());
		eventBuffer.push_back(fields);
		bufferLength += fields.sizeBytes();

		// If we have queued up a large number of events in simulation, then throw an error. This makes it easier to
		// diagnose cases where we get stuck in a loop logging trace events that eventually runs out of memory.
		// Without this we would never see any trace events from that loop, and it would be more difficult to identify
		// where the process is actually stuck.
		if (g_network && g_network->isSimulated() && bufferLength > 1e8) {
			fprintf(stderr, "Trace log buffer overflow\n");
			fprintf(stderr, "Last event: %s\n", fields.toString().c_str());
			// Setting this to 0 avoids a recurse from the assertion trace event and also prevents a situation where
			// we roll the trace log only to log the single assertion event when using --crash.
			bufferLength = 0;
			ASSERT(false);
		}

		if (trackError) {
			latestEventCache.setLatestError(fields);
		}
		if (!trackLatestKey.empty()) {
			latestEventCache.set(trackLatestKey, fields);
		}
	}

	void log(int severity, const char* name, UID id, uint64_t event_ts) {
		if (!logTraceEventMetrics)
			return;

		EventMetricHandle<TraceEventNameID>* m = nullptr;
		switch (severity) {
		case SevError:
			m = &SevErrorNames;
			break;
		case SevWarnAlways:
			m = &SevWarnAlwaysNames;
			break;
		case SevWarn:
			m = &SevWarnNames;
			break;
		case SevInfo:
			m = &SevInfoNames;
			break;
		case SevDebug:
			m = &SevDebugNames;
			break;
		default:
			break;
		}
		if (m != nullptr) {
			(*m)->name = StringRef((uint8_t*)name, strlen(name));
			(*m)->id = id.toString();
			(*m)->log(event_ts);
		}
	}

	ThreadFuture<Void> flush() {
		traceEventThrottlerCache->poll();

		MutexHolder hold(mutex);
		bool roll = false;
		if (!eventBuffer.size())
			return Void(); // SOMEDAY: maybe we still roll the tracefile here?

		if (rollsize && bufferLength + loggedLength > rollsize) // SOMEDAY: more conditions to roll
			roll = true;

		auto a = new WriterThread::WriteBuffer(std::move(eventBuffer));
		loggedLength += bufferLength;
		eventBuffer = std::vector<TraceEventFields>();
		bufferLength = 0;
		writer->post(a);

		if (roll) {
			auto o = new WriterThread::Roll;
			double time = 0;
			writer->post(o);

			std::vector<TraceEventFields> events = latestEventCache.getAllUnsafe();
			for (int idx = 0; idx < events.size(); idx++) {
				if (events[idx].size() > 0) {
					TraceEventFields rolledFields;
					for (auto itr = events[idx].begin(); itr != events[idx].end(); ++itr) {
						if (itr->first == "Time") {
							time = TraceEvent::getCurrentTime();
							rolledFields.addField("Time", format("%.6f", time));
							rolledFields.addField("OriginalTime", itr->second);
						} else if (itr->first == "DateTime") {
							UNSTOPPABLE_ASSERT(time > 0); // "Time" field should always come first
							rolledFields.addField("DateTime", TraceEvent::printRealTime(time));
							rolledFields.addField("OriginalDateTime", itr->second);
						} else if (itr->first == "TrackLatestType") {
							rolledFields.addField("TrackLatestType", "Rolled");
						} else {
							rolledFields.addField(itr->first, itr->second);
						}
					}

					eventBuffer.push_back(rolledFields);
				}
			}

			loggedLength = 0;
		}

		ThreadFuture<Void> f(new ThreadSingleAssignmentVar<Void>);
		barriers->push(f);
		writer->post(new WriterThread::Barrier);

		return f;
	}

	void close() {
		if (opened) {
			MutexHolder hold(mutex);

			// Write remaining contents
			auto a = new WriterThread::WriteBuffer(std::move(eventBuffer));
			loggedLength += bufferLength;
			eventBuffer = std::vector<TraceEventFields>();
			bufferLength = 0;
			writer->post(a);

			auto c = new WriterThread::Close();
			writer->post(c);

			ThreadFuture<Void> f(new ThreadSingleAssignmentVar<Void>);
			barriers->push(f);
			writer->post(new WriterThread::Barrier);

			f.getBlocking();

			opened = false;
		}
	}

	void addRole(std::string const& role) {
		MutexHolder holder(mutex);

		RoleInfo& r = mutateRoleInfo();
		++r.roles[role];
		r.refreshRolesString();
	}

	void removeRole(std::string const& role) {
		MutexHolder holder(mutex);

		RoleInfo& r = mutateRoleInfo();

		auto itr = r.roles.find(role);
		ASSERT(itr != r.roles.end() || (g_network->isSimulated() && g_network->getLocalAddress() == NetworkAddress()));

		if (itr != r.roles.end() && --(*itr).second == 0) {
			r.roles.erase(itr);
			r.refreshRolesString();
		}
	}

	void setLogGroup(const std::string& logGroup) {
		MutexHolder holder(mutex);
		this->logGroup = logGroup;
	}

	void addUniversalTraceField(const std::string& name, const std::string& value) {
		MutexHolder holder(mutex);
		ASSERT(universalFields.count(name) == 0);
		universalFields[name] = value;
	}

	Future<Void> pingWriterThread() {
		auto ping = new WriterThread::Ping;
		auto f = ping->ack.getFuture();
		writer->post(ping);
		return f;
	}

	void retrieveTraceLogIssues(std::set<std::string>& out) { return issues->retrieveIssues(out); }

	~TraceLog() {
		close();
		if (writer)
			writer->addref(); // FIXME: We are not shutting down the writer thread at all, because the ThreadPool
			                  // shutdown mechanism is blocking (necessarily waits for current work items to finish) and
			                  // we might not be able to finish everything.
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
void clearPrefix_internal(std::map<std::string, TraceEventFields>& data, std::string const& prefix) {
	auto first = data.lower_bound(prefix);
	auto last = data.lower_bound(strinc(prefix).toString());
	data.erase(first, last);
}

void LatestEventCache::clear(std::string const& prefix) {
	clearPrefix_internal(latest[getAddressIndex()], prefix);
}

void LatestEventCache::clear() {
	latest[getAddressIndex()].clear();
}

void LatestEventCache::set(std::string tag, const TraceEventFields& contents) {
	latest[getAddressIndex()][tag] = contents;
}

TraceEventFields LatestEventCache::get(std::string const& tag) {
	return latest[getAddressIndex()][tag];
}

std::vector<TraceEventFields> allEvents(std::map<std::string, TraceEventFields> const& data) {
	std::vector<TraceEventFields> all;
	for (auto it = data.begin(); it != data.end(); it++) {
		all.push_back(it->second);
	}
	return all;
}

std::vector<TraceEventFields> LatestEventCache::getAll() {
	return allEvents(latest[getAddressIndex()]);
}

// if in simulation, all events from all machines will be returned
std::vector<TraceEventFields> LatestEventCache::getAllUnsafe() {
	std::vector<TraceEventFields> all;
	for (auto it = latest.begin(); it != latest.end(); ++it) {
		auto m = allEvents(it->second);
		all.insert(all.end(), m.begin(), m.end());
	}
	return all;
}

void LatestEventCache::setLatestError(const TraceEventFields& contents) {
	if (TraceEvent::isNetworkThread()) { // The latest event cache doesn't track errors that happen on other threads
		latestErrors[getAddressIndex()] = contents;
	}
}

TraceEventFields LatestEventCache::getLatestError() {
	return latestErrors[getAddressIndex()];
}

static TraceLog g_traceLog;

namespace {
template <bool validate>
bool traceFormatImpl(std::string& format) {
	std::transform(format.begin(), format.end(), format.begin(), ::tolower);
	if (format == "xml") {
		if (!validate) {
			g_traceLog.formatter = Reference<ITraceLogFormatter>(new XmlTraceLogFormatter());
		}
		return true;
	} else if (format == "json") {
		if (!validate) {
			g_traceLog.formatter = Reference<ITraceLogFormatter>(new JsonTraceLogFormatter());
		}
		return true;
	} else {
		if (!validate) {
			g_traceLog.formatter = Reference<ITraceLogFormatter>(new XmlTraceLogFormatter());
		}
		return false;
	}
}

template <bool validate>
bool traceClockSource(std::string& source) {
	std::transform(source.begin(), source.end(), source.begin(), ::tolower);
	if (source == "now") {
		if (!validate) {
			g_trace_clock.store(TRACE_CLOCK_NOW);
		}
		return true;
	} else if (source == "realtime") {
		if (!validate) {
			g_trace_clock.store(TRACE_CLOCK_REALTIME);
		}
		return true;
	} else {
		return false;
	}
}

std::string toString(ErrorKind errorKind) {
	switch (errorKind) {
	case ErrorKind::Unset:
		return "Unset";
	case ErrorKind::DiskIssue:
		return "DiskIssue";
	case ErrorKind::BugDetected:
		return "BugDetected";
	default:
		UNSTOPPABLE_ASSERT(false);
		return "";
	}
}

} // namespace

bool selectTraceFormatter(std::string format) {
	ASSERT(!g_traceLog.isOpen());
	bool recognized = traceFormatImpl</*validate*/ false>(format);
	if (!recognized) {
		TraceEvent(SevWarnAlways, "UnrecognizedTraceFormat").detail("format", format);
	}
	return recognized;
}

bool validateTraceFormat(std::string format) {
	return traceFormatImpl</*validate*/ true>(format);
}

bool selectTraceClockSource(std::string source) {
	ASSERT(!g_traceLog.isOpen());
	bool recognized = traceClockSource</*validate*/ false>(source);
	if (!recognized) {
		TraceEvent(SevWarnAlways, "UnrecognizedTraceClockSource").detail("source", source);
	}
	return recognized;
}

bool validateTraceClockSource(std::string source) {
	return traceClockSource</*validate*/ true>(source);
}

ThreadFuture<Void> flushTraceFile() {
	if (!g_traceLog.isOpen())
		return Void();
	return g_traceLog.flush();
}

void flushTraceFileVoid() {
	if (g_network && g_network->isSimulated())
		flushTraceFile();
	else {
		flushTraceFile().getBlocking();
	}
}

void openTraceFile(const NetworkAddress& na,
                   uint64_t rollsize,
                   uint64_t maxLogsSize,
                   std::string directory,
                   std::string baseOfBase,
                   std::string logGroup,
                   std::string identifier,
                   std::string tracePartialFileSuffix) {
	if (g_traceLog.isOpen())
		return;

	if (directory.empty())
		directory = ".";

	if (baseOfBase.empty())
		baseOfBase = "trace";

	std::string ip = na.ip.toString();
	std::replace(ip.begin(), ip.end(), ':', '_'); // For IPv6, Windows doesn't accept ':' in filenames.
	std::string baseName;
	if (identifier.size() > 0) {
		baseName = format("%s.%s.%s", baseOfBase.c_str(), ip.c_str(), identifier.c_str());
	} else {
		baseName = format("%s.%s.%d", baseOfBase.c_str(), ip.c_str(), na.port);
	}
	g_traceLog.open(directory,
	                baseName,
	                logGroup,
	                format("%lld", time(nullptr)),
	                rollsize,
	                maxLogsSize,
	                !g_network->isSimulated() ? na : Optional<NetworkAddress>(),
	                tracePartialFileSuffix);

	uncancellable(recurring(&flushTraceFile, FLOW_KNOBS->TRACE_FLUSH_INTERVAL, TaskPriority::FlushTrace));
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

void addTraceRole(std::string const& role) {
	g_traceLog.addRole(role);
}

void removeTraceRole(std::string const& role) {
	g_traceLog.removeRole(role);
}

void setTraceLogGroup(const std::string& logGroup) {
	g_traceLog.setLogGroup(logGroup);
}

void addUniversalTraceField(const std::string& name, const std::string& value) {
	g_traceLog.addUniversalTraceField(name, value);
}

BaseTraceEvent::BaseTraceEvent() : initialized(true), enabled(false), logged(true) {}
BaseTraceEvent::BaseTraceEvent(Severity severity, const char* type, UID id)
  : initialized(false), enabled(g_network == nullptr || FLOW_KNOBS->MIN_TRACE_SEVERITY <= severity), logged(false),
    severity(severity), type(type), id(id) {}

BaseTraceEvent::BaseTraceEvent(BaseTraceEvent&& ev) {
	enabled = ev.enabled;
	err = ev.err;
	fields = std::move(ev.fields);
	id = ev.id;
	initialized = ev.initialized;
	logged = ev.logged;
	maxEventLength = ev.maxEventLength;
	maxFieldLength = ev.maxFieldLength;
	severity = ev.severity;
	tmpEventMetric = std::move(ev.tmpEventMetric);
	trackingKey = ev.trackingKey;
	type = ev.type;
	timeIndex = ev.timeIndex;

	for (int i = 0; i < 5; i++) {
		eventCounts[i] = ev.eventCounts[i];
	}

	networkThread = ev.networkThread;

	ev.initialized = true;
	ev.enabled = false;
	ev.logged = true;
}

BaseTraceEvent& BaseTraceEvent::operator=(BaseTraceEvent&& ev) {
	// Note: still broken if ev and this are the same memory address.
	enabled = ev.enabled;
	err = ev.err;
	fields = std::move(ev.fields);
	id = ev.id;
	initialized = ev.initialized;
	logged = ev.logged;
	maxEventLength = ev.maxEventLength;
	maxFieldLength = ev.maxFieldLength;
	severity = ev.severity;
	tmpEventMetric = std::move(ev.tmpEventMetric);
	trackingKey = ev.trackingKey;
	type = ev.type;
	timeIndex = ev.timeIndex;

	for (int i = 0; i < 5; i++) {
		eventCounts[i] = ev.eventCounts[i];
	}

	networkThread = ev.networkThread;

	ev.initialized = true;
	ev.enabled = false;
	ev.logged = true;

	return *this;
}

void retrieveTraceLogIssues(std::set<std::string>& out) {
	return g_traceLog.retrieveTraceLogIssues(out);
}

Future<Void> pingTraceLogWriterThread() {
	return g_traceLog.pingWriterThread();
}

TraceEvent::TraceEvent(const char* type, UID id) : BaseTraceEvent(SevInfo, type, id) {
	setMaxFieldLength(0);
	setMaxEventLength(0);
}
TraceEvent::TraceEvent(Severity severity, const char* type, UID id) : BaseTraceEvent(severity, type, id) {
	setMaxFieldLength(0);
	setMaxEventLength(0);
}
TraceEvent::TraceEvent(TraceInterval& interval, UID id) : BaseTraceEvent(interval.severity, interval.type, id) {
	setMaxFieldLength(0);
	setMaxEventLength(0);

	init(interval);
}
TraceEvent::TraceEvent(Severity severity, TraceInterval& interval, UID id)
  : BaseTraceEvent(severity, interval.type, id) {
	setMaxFieldLength(0);
	setMaxEventLength(0);

	init(interval);
}

bool BaseTraceEvent::init(TraceInterval& interval) {
	bool result = init();
	switch (interval.count++) {
	case 0: {
		detail("BeginPair", interval.pairID);
		break;
	}
	case 1: {
		detail("EndPair", interval.pairID);
		break;
	}
	default:
		ASSERT(false);
	}
	return result;
}

bool BaseTraceEvent::init() {
	ASSERT(!logged);
	if (initialized) {
		return enabled;
	}

	initialized = true;
	ASSERT(*type != '\0');

	++g_allocation_tracing_disabled;

	enabled = enabled && (!g_network || severity >= FLOW_KNOBS->MIN_TRACE_SEVERITY);

	// Backstop to throttle very spammy trace events
	if (enabled && g_network && !g_network->isSimulated() && severity > SevDebug && isNetworkThread()) {
		if (traceEventThrottlerCache->isAboveThreshold(StringRef((uint8_t*)type, strlen(type)))) {
			enabled = false;
			TraceEvent(SevWarnAlways, std::string(TRACE_EVENT_THROTTLE_STARTING_TYPE).append(type).c_str())
			    .suppressFor(5);
		} else {
			traceEventThrottlerCache->addAndExpire(
			    StringRef((uint8_t*)type, strlen(type)), 1, now() + FLOW_KNOBS->TRACE_EVENT_THROTTLER_SAMPLE_EXPIRY);
		}
	}

	if (enabled) {
		tmpEventMetric = std::make_unique<DynamicEventMetric>(MetricNameRef());

		if (err.isValid() && err.isInjectedFault() && severity == SevError) {
			severity = SevWarnAlways;
		}

		detail("Severity", int(severity));
		if (severity >= SevError) {
			detail("ErrorKind", errorKind);
			errorKindIndex = fields.size() - 1;
		}
		detail("Time", "0.000000");
		timeIndex = fields.size() - 1;
		if (FLOW_KNOBS && FLOW_KNOBS->TRACE_DATETIME_ENABLED) {
			detail("DateTime", "");
		}

		detail("Type", type);
		if (g_network && g_network->isSimulated()) {
			NetworkAddress local = g_network->getLocalAddress();
			detail("Machine", formatIpPort(local.ip, local.port));
		}
		detail("ID", id);
		if (err.isValid()) {
			if (err.isInjectedFault()) {
				detail("ErrorIsInjectedFault", true);
			}
			detail("Error", err.name());
			detail("ErrorDescription", err.what());
			detail("ErrorCode", err.code());
		}
	} else {
		tmpEventMetric = nullptr;
	}

	--g_allocation_tracing_disabled;
	return enabled;
}

TraceEvent& TraceEvent::errorImpl(class Error const& error, bool includeCancelled) {
	ASSERT(!logged);
	if (error.code() != error_code_actor_cancelled || includeCancelled) {
		err = error;
		if (initialized) {
			if (error.isInjectedFault()) {
				detail("ErrorIsInjectedFault", true);
				if (severity == SevError)
					severity = SevWarnAlways;
			}
			detail("Error", error.name());
			detail("ErrorDescription", error.what());
			detail("ErrorCode", error.code());
		}
		if (err.isDiskError()) {
			setErrorKind(ErrorKind::DiskIssue);
		}
	} else {
		if (initialized) {
			TraceEvent(g_network && g_network->isSimulated() ? SevError : SevWarnAlways,
			           std::string(TRACE_EVENT_INVALID_SUPPRESSION).append(type).c_str())
			    .suppressFor(5);
		} else {
			enabled = false;
		}
	}
	return *this;
}

BaseTraceEvent& BaseTraceEvent::detailImpl(std::string&& key, std::string&& value, bool writeEventMetricField) {
	init();
	if (enabled) {
		++g_allocation_tracing_disabled;
		if (maxFieldLength >= 0 && value.size() > maxFieldLength) {
			value = value.substr(0, maxFieldLength) + "...";
		}

		if (writeEventMetricField) {
			tmpEventMetric->setField(key.c_str(), Standalone<StringRef>(StringRef(value)));
		}

		fields.addField(std::move(key), std::move(value));

		if (maxEventLength >= 0 && fields.sizeBytes() > maxEventLength) {
			TraceEvent(g_network && g_network->isSimulated() ? SevError : SevWarnAlways, "TraceEventOverflow")
			    .setMaxEventLength(1000)
			    .detail("TraceFirstBytes", fields.toString().substr(0, 300));
			enabled = false;
		}
		--g_allocation_tracing_disabled;
	}
	return *this;
}

void BaseTraceEvent::setField(const char* key, int64_t value) {
	++g_allocation_tracing_disabled;
	tmpEventMetric->setField(key, value);
	--g_allocation_tracing_disabled;
}

void BaseTraceEvent::setField(const char* key, double value) {
	++g_allocation_tracing_disabled;
	tmpEventMetric->setField(key, value);
	--g_allocation_tracing_disabled;
}

void BaseTraceEvent::setField(const char* key, const std::string& value) {
	++g_allocation_tracing_disabled;
	tmpEventMetric->setField(key, Standalone<StringRef>(value));
	--g_allocation_tracing_disabled;
}

BaseTraceEvent& BaseTraceEvent::detailf(std::string key, const char* valueFormat, ...) {
	if (enabled) {
		va_list args;
		va_start(args, valueFormat);
		std::string value;
		int result = vsformat(value, valueFormat, args);
		va_end(args);

		ASSERT(result >= 0);
		detailImpl(std::move(key), std::move(value));
	}
	return *this;
}
BaseTraceEvent& BaseTraceEvent::detailfNoMetric(std::string&& key, const char* valueFormat, ...) {
	if (enabled) {
		va_list args;
		va_start(args, valueFormat);
		std::string value;
		int result = vsformat(value, valueFormat, args);
		va_end(args);

		ASSERT(result >= 0);
		detailImpl(std::move(key),
		           std::move(value),
		           false); // Do NOT write this detail to the event metric, caller of detailfNoMetric should do that
		                   // itself with the appropriate value type
	}
	return *this;
}

BaseTraceEvent& BaseTraceEvent::trackLatest(const std::string& trackingKey) {
	ASSERT(!logged);
	this->trackingKey = trackingKey;
	ASSERT(this->trackingKey.size() != 0 && this->trackingKey[0] != '/' && this->trackingKey[0] != '\\');
	return *this;
}

BaseTraceEvent& TraceEvent::sample(double sampleRate, bool logSampleRate) {
	if (enabled) {
		if (initialized) {
			TraceEvent(g_network && g_network->isSimulated() ? SevError : SevWarnAlways,
			           std::string(TRACE_EVENT_INVALID_SUPPRESSION).append(type).c_str())
			    .suppressFor(5);
			return *this;
		}

		enabled = enabled && deterministicRandom()->random01() < sampleRate;

		if (enabled && logSampleRate) {
			detail("SampleRate", sampleRate);
		}
	}

	return *this;
}

BaseTraceEvent& TraceEvent::suppressFor(double duration, bool logSuppressedEventCount) {
	ASSERT(!logged);
	if (enabled) {
		if (initialized) {
			TraceEvent(g_network && g_network->isSimulated() ? SevError : SevWarnAlways,
			           std::string(TRACE_EVENT_INVALID_SUPPRESSION).append(type).c_str())
			    .suppressFor(5);
			return *this;
		}

		if (g_network) {
			if (isNetworkThread()) {
				int64_t suppressedEventCount = suppressedEvents.checkAndInsertSuppression(type, duration);
				enabled = enabled && suppressedEventCount >= 0;
				if (enabled && logSuppressedEventCount) {
					detail("SuppressedEventCount", suppressedEventCount);
				}
			} else {
				TraceEvent(SevWarnAlways, "SuppressionFromNonNetworkThread").detail("Event", type);
				// Choosing a detail name that is unlikely to collide with other names
				detail("__InvalidSuppression__", "");
			}
		}

		// we do not want any future calls on this trace event to disable it, because we have already counted it
		// towards our suppression budget
		init();
	}

	return *this;
}

BaseTraceEvent& BaseTraceEvent::setErrorKind(ErrorKind errorKind) {
	this->errorKind = errorKind;
	return *this;
}

BaseTraceEvent& BaseTraceEvent::setMaxFieldLength(int maxFieldLength) {
	ASSERT(!logged);
	if (maxFieldLength == 0) {
		this->maxFieldLength = FLOW_KNOBS ? FLOW_KNOBS->MAX_TRACE_FIELD_LENGTH : 495;
	} else {
		this->maxFieldLength = maxFieldLength;
	}

	return *this;
}

// A unique, per-thread ID.  This is particularly important for multithreaded
// or multiversion client setups and for multithreaded storage engines.
thread_local uint64_t threadId = 0;

uint64_t getTraceThreadId() {
	while (threadId == 0) {
		threadId = deterministicRandom()->randomUInt64();
	}

	return threadId;
}

void BaseTraceEvent::setThreadId() {
	this->detail("ThreadID", getTraceThreadId());
}

int BaseTraceEvent::getMaxFieldLength() const {
	return maxFieldLength;
}

BaseTraceEvent& BaseTraceEvent::setMaxEventLength(int maxEventLength) {
	ASSERT(!logged);
	if (maxEventLength == 0) {
		this->maxEventLength = FLOW_KNOBS ? FLOW_KNOBS->MAX_TRACE_EVENT_LENGTH : 4000;
	} else {
		this->maxEventLength = maxEventLength;
	}

	return *this;
}

int BaseTraceEvent::getMaxEventLength() const {
	return maxEventLength;
}

BaseTraceEvent& BaseTraceEvent::GetLastError() {
#ifdef _WIN32
	return detailf("WinErrorCode", "%x", ::GetLastError());
#elif defined(__unixish__)
	return detailf("UnixErrorCode", "%x", errno).detail("UnixError", strerror(errno));
#endif
}

unsigned long BaseTraceEvent::eventCounts[NUM_MAJOR_LEVELS_OF_EVENTS] = { 0, 0, 0, 0, 0 };

unsigned long BaseTraceEvent::CountEventsLoggedAt(Severity sev) {
	ASSERT(sev <= SevMaxUsed);
	return TraceEvent::eventCounts[sev / 10];
}

BaseTraceEvent& BaseTraceEvent::backtrace(const std::string& prefix) {
	ASSERT(!logged);
	if (this->severity == SevError || !enabled)
		return *this; // We'll backtrace this later in ~TraceEvent
	return detail(prefix + "Backtrace", platform::get_backtrace());
}

void BaseTraceEvent::log() {
	if (!logged) {
		init();
		++g_allocation_tracing_disabled;
		try {
			if (enabled) {
				double time = TraceEvent::getCurrentTime();
				fields.mutate(timeIndex).second = format("%.6f", time);
				if (FLOW_KNOBS && FLOW_KNOBS->TRACE_DATETIME_ENABLED) {
					fields.mutate(timeIndex + 1).second = TraceEvent::printRealTime(time);
				}

				setThreadId();

				if (this->severity == SevError) {
					severity = SevInfo;
					backtrace();
					severity = SevError;
					if (errorKindIndex != -1) {
						fields.mutate(errorKindIndex).second = toString(errorKind);
					}
				}

				if (isNetworkThread()) {
					TraceEvent::eventCounts[severity / 10]++;
				}

				g_traceLog.writeEvent(fields, trackingKey, severity > SevWarnAlways);

				if (g_traceLog.isOpen()) {
					// Log Metrics
					if (g_traceLog.logTraceEventMetrics && isNetworkThread()) {
						// Get the persistent Event Metric representing this trace event and push the fields (details)
						// accumulated in *this to it and then log() it. Note that if the event metric is disabled it
						// won't actually be logged BUT any new fields added to it will be registered. If the event IS
						// logged, a timestamp will be returned, if not then 0.  Either way, pass it through to be used
						// if possible in the Sev* event metrics.

						uint64_t event_ts =
						    DynamicEventMetric::getOrCreateInstance(format("TraceEvent.%s", type), StringRef(), true)
						        ->setFieldsAndLogFrom(tmpEventMetric.get());
						g_traceLog.log(severity, type, id, event_ts);
					}
				}
			}
		} catch (Error& e) {
			TraceEvent(SevError, "TraceEventLoggingError").errorUnsuppressed(e);
		}
		tmpEventMetric.reset();
		logged = true;
		--g_allocation_tracing_disabled;
	}
}

BaseTraceEvent::~BaseTraceEvent() {
	log();
}

thread_local bool BaseTraceEvent::networkThread = false;

void BaseTraceEvent::setNetworkThread() {
	if (!networkThread) {
		if (FLOW_KNOBS->ALLOCATION_TRACING_ENABLED) {
			--g_allocation_tracing_disabled;
		}

		traceEventThrottlerCache = new TransientThresholdMetricSample<Standalone<StringRef>>(
		    FLOW_KNOBS->TRACE_EVENT_METRIC_UNITS_PER_SAMPLE, FLOW_KNOBS->TRACE_EVENT_THROTTLER_MSG_LIMIT);
		networkThread = true;
	}
}

bool BaseTraceEvent::isNetworkThread() {
	return networkThread;
}

double BaseTraceEvent::getCurrentTime() {
	if (g_trace_clock.load() == TRACE_CLOCK_NOW) {
		if (!isNetworkThread() || !g_network) {
			return timer_monotonic();
		} else {
			return now();
		}
	} else {
		return timer();
	}
}

// converts the given flow time into a string
// with format: %Y-%m-%dT%H:%M:%S
// This only has second-resolution for the simple reason
// that std::put_time does not support higher resolution.
// This is fine since we always log the flow time as well.
std::string BaseTraceEvent::printRealTime(double time) {
	using Clock = std::chrono::system_clock;
	time_t ts = Clock::to_time_t(Clock::time_point(
	    std::chrono::duration_cast<Clock::duration>(std::chrono::duration<double, std::ratio<1>>(time))));
	if (g_network && g_network->isSimulated()) {
		// The clock is simulated, so return the real time
		ts = Clock::to_time_t(Clock::now());
	}
	std::stringstream ss;
#ifdef _WIN32
	// MSVC gmtime is threadsafe
	ss << std::put_time(::gmtime(&ts), "%Y-%m-%dT%H:%M:%SZ");
#else
	// use threadsafe gmt
	struct tm result;
	ss << std::put_time(::gmtime_r(&ts, &result), "%Y-%m-%dT%H:%M:%SZ");
#endif
	return ss.str();
}

TraceInterval& TraceInterval::begin() {
	pairID = nondeterministicRandom()->randomUniqueID();
	count = 0;
	return *this;
}

bool TraceBatch::dumpImmediately() {
	return (g_network->isSimulated() || FLOW_KNOBS->AUTOMATIC_TRACE_DUMP);
}

void TraceBatch::addEvent(const char* name, uint64_t id, const char* location) {
	if (FLOW_KNOBS->MIN_TRACE_SEVERITY > TRACE_BATCH_IMPLICIT_SEVERITY) {
		return;
	}
	auto& eventInfo = eventBatch.emplace_back(EventInfo(TraceEvent::getCurrentTime(), name, id, location));
	if (dumpImmediately())
		dump();
	else
		g_traceLog.annotateEvent(eventInfo.fields);
}

void TraceBatch::addAttach(const char* name, uint64_t id, uint64_t to) {
	if (FLOW_KNOBS->MIN_TRACE_SEVERITY > TRACE_BATCH_IMPLICIT_SEVERITY) {
		return;
	}
	auto& attachInfo = attachBatch.emplace_back(AttachInfo(TraceEvent::getCurrentTime(), name, id, to));
	if (dumpImmediately())
		dump();
	else
		g_traceLog.annotateEvent(attachInfo.fields);
}

void TraceBatch::addBuggify(int activated, int line, std::string file) {
	if (FLOW_KNOBS->MIN_TRACE_SEVERITY > TRACE_BATCH_IMPLICIT_SEVERITY) {
		return;
	}
	if (g_network) {
		auto& buggifyInfo = buggifyBatch.emplace_back(BuggifyInfo(TraceEvent::getCurrentTime(), activated, line, file));
		if (dumpImmediately())
			dump();
		else
			g_traceLog.annotateEvent(buggifyInfo.fields);
	} else {
		buggifyBatch.push_back(BuggifyInfo(0, activated, line, file));
	}
}

void TraceBatch::dump() {
	if (!g_traceLog.isOpen() || FLOW_KNOBS->MIN_TRACE_SEVERITY > TRACE_BATCH_IMPLICIT_SEVERITY)
		return;
	std::string machine;
	if (g_network->isSimulated()) {
		NetworkAddress local = g_network->getLocalAddress();
		machine = formatIpPort(local.ip, local.port);
	}

	for (int i = 0; i < attachBatch.size(); i++) {
		if (g_network->isSimulated()) {
			attachBatch[i].fields.addField("Machine", machine);
		}
		g_traceLog.writeEvent(attachBatch[i].fields, "", false);
	}

	for (int i = 0; i < eventBatch.size(); i++) {
		if (g_network->isSimulated()) {
			eventBatch[i].fields.addField("Machine", machine);
		}
		g_traceLog.writeEvent(eventBatch[i].fields, "", false);
	}

	for (int i = 0; i < buggifyBatch.size(); i++) {
		if (g_network->isSimulated()) {
			buggifyBatch[i].fields.addField("Machine", machine);
		}
		g_traceLog.writeEvent(buggifyBatch[i].fields, "", false);
	}

	onMainThreadVoid([]() { g_traceLog.flush(); }, nullptr);
	eventBatch.clear();
	attachBatch.clear();
	buggifyBatch.clear();
}

TraceBatch::EventInfo::EventInfo(double time, const char* name, uint64_t id, const char* location) {
	fields.addField("Severity", format("%d", (int)TRACE_BATCH_IMPLICIT_SEVERITY));
	fields.addField("Time", format("%.6f", time));
	if (FLOW_KNOBS && FLOW_KNOBS->TRACE_DATETIME_ENABLED) {
		fields.addField("DateTime", TraceEvent::printRealTime(time));
	}
	fields.addField("Type", name);
	fields.addField("ID", format("%016" PRIx64, id));
	fields.addField("Location", location);
}

TraceBatch::AttachInfo::AttachInfo(double time, const char* name, uint64_t id, uint64_t to) {
	fields.addField("Severity", format("%d", (int)TRACE_BATCH_IMPLICIT_SEVERITY));
	fields.addField("Time", format("%.6f", time));
	if (FLOW_KNOBS && FLOW_KNOBS->TRACE_DATETIME_ENABLED) {
		fields.addField("DateTime", TraceEvent::printRealTime(time));
	}
	fields.addField("Type", name);
	fields.addField("ID", format("%016" PRIx64, id));
	fields.addField("To", format("%016" PRIx64, to));
}

TraceBatch::BuggifyInfo::BuggifyInfo(double time, int activated, int line, std::string file) {
	fields.addField("Severity", format("%d", (int)TRACE_BATCH_IMPLICIT_SEVERITY));
	fields.addField("Time", format("%.6f", time));
	if (FLOW_KNOBS && FLOW_KNOBS->TRACE_DATETIME_ENABLED) {
		fields.addField("DateTime", TraceEvent::printRealTime(time));
	}
	fields.addField("Type", "BuggifySection");
	fields.addField("Activated", format("%d", activated));
	fields.addField("File", std::move(file));
	fields.addField("Line", format("%d", line));
}

TraceEventFields::TraceEventFields() : bytes(0), annotated(false) {}

void TraceEventFields::addField(const std::string& key, const std::string& value) {
	bytes += key.size() + value.size();
	fields.emplace_back(key, value);
}

void TraceEventFields::addField(std::string&& key, std::string&& value) {
	bytes += key.size() + value.size();
	fields.emplace_back(std::move(key), std::move(value));
}

size_t TraceEventFields::size() const {
	return fields.size();
}

size_t TraceEventFields::sizeBytes() const {
	return bytes;
}

TraceEventFields::FieldIterator TraceEventFields::begin() const {
	return fields.cbegin();
}

TraceEventFields::FieldIterator TraceEventFields::end() const {
	return fields.cend();
}

bool TraceEventFields::isAnnotated() const {
	return annotated;
}

void TraceEventFields::setAnnotated() {
	annotated = true;
}

const TraceEventFields::Field& TraceEventFields::operator[](int index) const {
	ASSERT(index >= 0 && index < size());
	return fields.at(index);
}

bool TraceEventFields::tryGetValue(std::string key, std::string& outValue) const {
	for (auto itr = begin(); itr != end(); ++itr) {
		if (itr->first == key) {
			outValue = itr->second;
			return true;
		}
	}

	return false;
}

std::string TraceEventFields::getValue(std::string key) const {
	std::string value;
	if (tryGetValue(key, value)) {
		return value;
	} else {
		TraceEvent ev(SevWarn, "TraceEventFieldNotFound");
		ev.suppressFor(1.0);
		if (tryGetValue("Type", value)) {
			ev.detail("Event", value);
		}
		ev.detail("FieldName", key);

		throw attribute_not_found();
	}
}

TraceEventFields::Field& TraceEventFields::mutate(int index) {
	return fields.at(index);
}

namespace {
void parseNumericValue(std::string const& s, double& outValue, bool permissive = false) {
	double d = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%lf%n", &d, &consumed);
	if (r == 1 && (consumed == s.size() || permissive)) {
		outValue = d;
		return;
	}

	throw attribute_not_found();
}

void parseNumericValue(std::string const& s, int& outValue, bool permissive = false) {
	long long int iLong = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%lld%n", &iLong, &consumed);
	if (r == 1 && (consumed == s.size() || permissive)) {
		if (std::numeric_limits<int>::min() <= iLong && iLong <= std::numeric_limits<int>::max()) {
			outValue = (int)iLong; // Downcast definitely safe
			return;
		} else {
			throw attribute_too_large();
		}
	}

	throw attribute_not_found();
}

void parseNumericValue(std::string const& s, int64_t& outValue, bool permissive = false) {
	long long int i = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%lld%n", &i, &consumed);
	if (r == 1 && (consumed == s.size() || permissive)) {
		outValue = i;
		return;
	}

	throw attribute_not_found();
}

void parseNumericValue(std::string const& s, uint64_t& outValue, bool permissive = false) {
	unsigned long long int i = 0;
	int consumed = 0;
	int r = sscanf(s.c_str(), "%llu%n", &i, &consumed);
	if (r == 1 && (consumed == s.size() || permissive)) {
		outValue = i;
		return;
	}

	throw attribute_not_found();
}

template <class T>
T getNumericValue(TraceEventFields const& fields, std::string key, bool permissive) {
	std::string field = fields.getValue(key);

	try {
		T value;
		parseNumericValue(field, value, permissive);
		return value;
	} catch (Error& e) {
		std::string type;

		TraceEvent ev(SevWarn, "ErrorParsingNumericTraceEventField");
		ev.error(e);
		if (fields.tryGetValue("Type", type)) {
			ev.detail("Event", type);
		}
		ev.detail("FieldName", key);
		ev.detail("FieldValue", field);

		throw;
	}
}
} // namespace

int TraceEventFields::getInt(std::string key, bool permissive) const {
	return getNumericValue<int>(*this, key, permissive);
}

int64_t TraceEventFields::getInt64(std::string key, bool permissive) const {
	return getNumericValue<int64_t>(*this, key, permissive);
}

uint64_t TraceEventFields::getUint64(std::string key, bool permissive) const {
	return getNumericValue<uint64_t>(*this, key, permissive);
}

double TraceEventFields::getDouble(std::string key, bool permissive) const {
	return getNumericValue<double>(*this, key, permissive);
}

std::string TraceEventFields::toString() const {
	std::string str;
	bool first = true;
	for (auto itr = begin(); itr != end(); ++itr) {
		if (!first) {
			str += ", ";
		}
		first = false;

		str += format("\"%s\"=\"%s\"", itr->first.c_str(), itr->second.c_str());
	}

	return str;
}

bool validateField(const char* key, bool allowUnderscores) {
	if ((key[0] < 'A' || key[0] > 'Z') && key[0] != '_') {
		return false;
	}

	const char* underscore = strchr(key, '_');
	while (underscore) {
		if (!allowUnderscores || ((underscore[1] < 'A' || underscore[1] > 'Z') && key[0] != '_' && key[0] != '\0')) {
			return false;
		}

		underscore = strchr(&underscore[1], '_');
	}

	return true;
}

void TraceEventFields::validateFormat() const {
	if (g_network && g_network->isSimulated()) {
		for (Field field : fields) {
			if (!validateField(field.first.c_str(), false)) {
				fprintf(stderr,
				        "Trace event detail name `%s' is invalid in:\n\t%s\n",
				        field.first.c_str(),
				        toString().c_str());
			}
			if (field.first == "Type" && !validateField(field.second.c_str(), true)) {
				fprintf(stderr, "Trace event detail Type `%s' is invalid\n", field.second.c_str());
			}
		}
	}
}

std::string traceableStringToString(const char* value, size_t S) {
	if (g_network) {
		ASSERT_WE_THINK(S > 0 && value[S - 1] == '\0');
	}

	return std::string(value, S - 1); // Exclude trailing \0 byte
}
