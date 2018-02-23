/*
 * Trace.h
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

#ifndef FLOW_TRACE_H
#define FLOW_TRACE_H
#pragma once

#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <map>
#include "IRandom.h"
#include "Error.h"

#define TRACE_DEFAULT_ROLL_SIZE (10 << 20)
#define TRACE_DEFAULT_MAX_LOGS_SIZE (10 * TRACE_DEFAULT_ROLL_SIZE)

inline int fastrand() {
	static int g_seed = 0;
	g_seed = 214013*g_seed + 2531011;
	return (g_seed>>16)&0x7fff;
}

//inline static bool TRACE_SAMPLE() { return fastrand()<16; }
inline static bool TRACE_SAMPLE() { return false; }

enum Severity {
	SevSample=1,
	SevDebug=5,
	SevInfo=10,
	SevWarn=20,
	SevWarnAlways=30,
	SevError=40,
	SevMaxUsed=SevError,
	SevMax=1000000
};

class TraceBatch {
public:
	void addEvent( const char *name, uint64_t id, const char *location );
	void addAttach( const char *name, uint64_t id, uint64_t to );
	void addBuggify( int activated, int line, std::string file );
	void dump();

private:
	struct EventInfo {
		double time;
		const char *name;
		uint64_t id;
		const char *location;

		EventInfo(double time, const char *name, uint64_t id, const char *location) : time(time), name(name), id(id), location(location) {}
	};

	struct AttachInfo {
		double time;
		const char *name;
		uint64_t id;
		uint64_t to;

		AttachInfo(double time, const char *name, uint64_t id, uint64_t to) : time(time), name(name), id(id), to(to) {}
	};

	struct BuggifyInfo {
		double time;
		int activated;
		int line;
		std::string file;

		BuggifyInfo(double time, int activated, int line, std::string file) : time(time), activated(activated), line(line), file(file) {}
	};

	std::vector<EventInfo> eventBatch;
	std::vector<AttachInfo> attachBatch;
	std::vector<BuggifyInfo> buggifyBatch;
};

struct DynamicEventMetric;
class StringRef;
template <class T> class Standalone;
template <class T> class Optional;

#if 1
struct TraceEvent {
	TraceEvent( const char* type, UID id = UID() );   // Assumes SevInfo severity
	TraceEvent( Severity, const char* type, UID id = UID() );
	TraceEvent( struct TraceInterval&, UID id = UID() );
	TraceEvent( Severity severity, struct TraceInterval& interval, UID id = UID() );
	TraceEvent(const char* type, const StringRef& id);   // Assumes SevInfo severity
	TraceEvent(Severity, const char* type, const StringRef& id);
	TraceEvent(const char* type, const Optional<Standalone<StringRef>>& id);   // Assumes SevInfo severity
	TraceEvent(Severity, const char* type, const Optional<Standalone<StringRef>>& id);

	static bool isEnabled( const char* type, Severity = SevMax );
	static void setNetworkThread();
	static bool isNetworkThread();

	TraceEvent& error(class Error const& e, bool includeCancelled=false);

	TraceEvent& detail( const char* key, const char* value );
	TraceEvent& detail( const char* key, const std::string& value );
	TraceEvent& detail( const char* key, double value );
	TraceEvent& detail( const char* key, long int value );
	TraceEvent& detail( const char* key, long unsigned int value );
	TraceEvent& detail( const char* key, long long int value );
	TraceEvent& detail( const char* key, long long unsigned int value );
	TraceEvent& detail( const char* key, int value );
	TraceEvent& detail( const char* key, unsigned value );
	TraceEvent& detail( const char* key, struct NetworkAddress const& value );
	TraceEvent& detailf( const char* key, const char* valueFormat, ... );
	TraceEvent& detailext(const char* key, StringRef const& value);
	TraceEvent& detailext(const char* key, Optional<Standalone<StringRef>> const& value);
private:
	// Private version of _detailf that does NOT write to the eventMetric.  This is to be used by other detail methods
	// which can write field metrics of a more appropriate type than string but use detailf() to add to the TraceEvent.
	TraceEvent& _detailf( const char* key, const char* valueFormat, ... );
public:
	TraceEvent& detailfv( const char* key, const char* valueFormat, va_list args, bool writeEventMetricField);
	TraceEvent& detail( const char* key, UID const& value );
	TraceEvent& backtrace(std::string prefix = "");
	TraceEvent& trackLatest( const char* trackingKey );
	TraceEvent& sample( double sampleRate, bool logSampleRate=true );
	TraceEvent& suppressFor( double duration, bool logSuppressedEventCount=true );

	TraceEvent& GetLastError();

	~TraceEvent();  // Actually logs the event

	// Return the number of invocations of TraceEvent() at the specified logging level.
	static unsigned long CountEventsLoggedAt(Severity);

	DynamicEventMetric *tmpEventMetric;  // This just just a place to store fields

private:
	bool enabled;
	std::string trackingKey;
	char buffer[4000];
	int length;
	Severity severity;
	const char *type;
	UID id;

	static unsigned long eventCounts[5];
	static thread_local bool networkThread;

	bool init( Severity, const char* type );
	bool init( Severity, struct TraceInterval& );

	void write( int length, const void* data );
	void writef( const char* format, ... );
	void writeEscaped( const char* data );
	void writeEscapedfv( const char* format, va_list args );
};
#else
struct TraceEvent {
	TraceEvent(const char* type, UID id = UID()) {}
	TraceEvent(Severity, const char* type, UID id = UID()) {}
	TraceEvent(struct TraceInterval&, UID id = UID()) {}
	TraceEvent(const char* type, StringRef& const id); {}   // Assumes SevInfo severity
	TraceEvent(Severity, const char* type, StringRef& const id); {}

	static bool isEnabled(const char* type) { return false; }

	TraceEvent& error(class Error const& e, bool includeCancelled = false) { return *this; }

	TraceEvent& detail(const char* key, const char* value) { return *this; }
	TraceEvent& detail(const char* key, const std::string& value) { return *this; }
	TraceEvent& detail(const char* key, double value) { return *this; }
	TraceEvent& detail(const char* key, long int value) { return *this; }
	TraceEvent& detail(const char* key, long unsigned int value) { return *this; }
	TraceEvent& detail(const char* key, long long int value) { return *this; }
	TraceEvent& detail(const char* key, long long unsigned int value) { return *this; }
	TraceEvent& detail(const char* key, int value) { return *this; }
	TraceEvent& detail(const char* key, unsigned value) { return *this; }
	TraceEvent& detail(const char* key, struct NetworkAddress const& value) { return *this; }
	TraceEvent& detailf(const char* key, const char* valueFormat, ...) { return *this; }
	TraceEvent& detailfv(const char* key, const char* valueFormat, va_list args) { return *this; }
	TraceEvent& detail(const char* key, UID const& value) { return *this; }
	TraceEvent& detailext(const char* key, StringRef const& value) { return *this; }
	TraceEvent& detailext(const char* key, Optional<Standalone<StringRef>> const& value); { return *this; }
	TraceEvent& backtrace(std::string prefix = "") { return *this; }
	TraceEvent& trackLatest(const char* trackingKey) { return *this; }

	TraceEvent& GetLastError() { return *this; }
};
#endif

struct TraceInterval {
	TraceInterval( const char* type ) : count(-1), type(type), severity(SevInfo) {}

	TraceInterval& begin();
	TraceInterval& end() { return *this; }

	const char* type;
	UID pairID;
	int count;
	Severity severity;
};

struct LatestEventCache {
public:
	void set( std::string tag, std::string contents );
	std::string get( std::string tag );
	std::vector<std::string> getAll();
	std::vector<std::string> getAllUnsafe();

	void clear( std::string prefix );
	void clear();

	// Latest error tracking only tracks errors when called from the main thread. Other errors are silently ignored.
	void setLatestError( std::string contents );
	std::string getLatestError();
private:
	std::map<NetworkAddress, std::map<std::string, std::string>> latest;
	std::map<NetworkAddress, std::string> latestErrors;
};

extern LatestEventCache latestEventCache;

// Evil but potentially useful for verbose messages:
#if CENABLED(0, NOT_IN_CLEAN)
#define TRACE( t, m ) if (TraceEvent::isEnabled(t)) TraceEvent(t,m)
#endif

struct NetworkAddress;
void openTraceFile(const NetworkAddress& na, uint64_t rollsize, uint64_t maxLogsSize, std::string directory = ".", std::string baseOfBase = "trace", std::string logGroup = "default");
void initTraceEventMetrics();
void closeTraceFile();
bool traceFileIsOpen();

enum trace_clock_t { TRACE_CLOCK_NOW, TRACE_CLOCK_REALTIME };
extern trace_clock_t g_trace_clock;
extern TraceBatch g_traceBatch;

#endif
