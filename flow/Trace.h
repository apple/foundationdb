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
#include "flow/IRandom.h"
#include "flow/Error.h"

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

class TraceEventFields {
public:
	typedef std::pair<std::string, std::string> Field;
	typedef std::vector<Field> FieldContainer;
	typedef FieldContainer::const_iterator FieldIterator;

	TraceEventFields();

	size_t size() const;
	size_t sizeBytes() const;
	FieldIterator begin() const;
	FieldIterator end() const;

	void addField(const std::string& key, const std::string& value);
	void addField(std::string&& key, std::string&& value);

	const Field &operator[] (int index) const;
	bool tryGetValue(std::string key, std::string &outValue) const;
	std::string getValue(std::string key) const;

	std::string toString() const;
	void validateFormat() const;

private:
	FieldContainer fields;
	size_t bytes;
};

template <class Archive>
inline void load( Archive& ar, TraceEventFields& value ) {
	uint32_t count;
	ar >> count;

	std::string k;
	std::string v;
	for(uint32_t i = 0; i < count; ++i) {
		ar >> k >> v;
		value.addField(k, v);
	}
}
template <class Archive>
inline void save( Archive& ar, const TraceEventFields& value ) {
	ar << (uint32_t)value.size();

	for(auto itr : value) {
		ar << itr.first << itr.second;
	}
}

class TraceBatch {
public:
	void addEvent( const char *name, uint64_t id, const char *location );
	void addAttach( const char *name, uint64_t id, uint64_t to );
	void addBuggify( int activated, int line, std::string file );
	void dump();

private:
	struct EventInfo {
		TraceEventFields fields;
		EventInfo(double time, const char *name, uint64_t id, const char *location); 
	};

	struct AttachInfo {
		TraceEventFields fields;
		AttachInfo(double time, const char *name, uint64_t id, uint64_t to);
	};

	struct BuggifyInfo {
		TraceEventFields fields;
		BuggifyInfo(double time, int activated, int line, std::string file);
	};

	std::vector<EventInfo> eventBatch;
	std::vector<AttachInfo> attachBatch;
	std::vector<BuggifyInfo> buggifyBatch;
};

struct DynamicEventMetric;
class StringRef;
template <class T> class Standalone;
template <class T> class Optional;

struct TraceEvent {
	TraceEvent( const char* type, UID id = UID() );   // Assumes SevInfo severity
	TraceEvent( Severity, const char* type, UID id = UID() );
	TraceEvent( struct TraceInterval&, UID id = UID() );
	TraceEvent( Severity severity, struct TraceInterval& interval, UID id = UID() );

	static void setNetworkThread();
	static bool isNetworkThread();

	//Must be called directly after constructing the trace event
	TraceEvent& error(const class Error& e, bool includeCancelled=false);

	TraceEvent& detail( std::string key, std::string value );
	TraceEvent& detail( std::string key, double value );
	TraceEvent& detail( std::string key, long int value );
	TraceEvent& detail( std::string key, long unsigned int value );
	TraceEvent& detail( std::string key, long long int value );
	TraceEvent& detail( std::string key, long long unsigned int value );
	TraceEvent& detail( std::string key, int value );
	TraceEvent& detail( std::string key, unsigned value );
	TraceEvent& detail( std::string key, const struct NetworkAddress& value );
	TraceEvent& detailf( std::string key, const char* valueFormat, ... );
	TraceEvent& detailext( std::string key, const StringRef& value );
	TraceEvent& detailext( std::string key, const Optional<Standalone<StringRef>>& value );
private:
	// Private version of detailf that does NOT write to the eventMetric.  This is to be used by other detail methods
	// which can write field metrics of a more appropriate type than string but use detailf() to add to the TraceEvent.
	TraceEvent& detailfNoMetric( std::string&& key, const char* valueFormat, ... );
	TraceEvent& detailImpl( std::string&& key, std::string&& value, bool writeEventMetricField=true );
public:
	TraceEvent& detail( std::string key, const UID& value );
	TraceEvent& backtrace(const std::string& prefix = "");
	TraceEvent& trackLatest( const char* trackingKey );
	TraceEvent& sample( double sampleRate, bool logSampleRate=true );

	//Cannot call other functions which could disable the trace event afterwords
	TraceEvent& suppressFor( double duration, bool logSuppressedEventCount=true );

	TraceEvent& GetLastError();

	~TraceEvent();  // Actually logs the event

	// Return the number of invocations of TraceEvent() at the specified logging level.
	static unsigned long CountEventsLoggedAt(Severity);

	DynamicEventMetric *tmpEventMetric;  // This just just a place to store fields

private:
	bool initialized;
	bool enabled;
	std::string trackingKey;
	TraceEventFields fields;
	Severity severity;
	const char *type;
	UID id;
	Error err;

	static unsigned long eventCounts[5];
	static thread_local bool networkThread;

	bool init();
	bool init( struct TraceInterval& );
};

struct ITraceLogWriter {
	virtual void open() = 0;
	virtual void roll() = 0;
	virtual void close() = 0;
	virtual void write(const std::string&) = 0;
	virtual void sync() = 0;

	virtual void addref() = 0;
	virtual void delref() = 0;
};

struct ITraceLogFormatter {
	virtual const char* getExtension() = 0;
	virtual const char* getHeader() = 0; // Called when starting a new file
	virtual const char* getFooter() = 0; // Called when ending a file
	virtual std::string formatEvent(const TraceEventFields&) = 0; // Called for each event

	virtual void addref() = 0;
	virtual void delref() = 0;
};

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
	void set( std::string tag, const TraceEventFields& fields );
	TraceEventFields get( std::string tag );
	std::vector<TraceEventFields> getAll();
	std::vector<TraceEventFields> getAllUnsafe();

	void clear( std::string prefix );
	void clear();

	// Latest error tracking only tracks errors when called from the main thread. Other errors are silently ignored.
	void setLatestError( const TraceEventFields& contents );
	TraceEventFields getLatestError();
private:
	std::map<NetworkAddress, std::map<std::string, TraceEventFields>> latest;
	std::map<NetworkAddress, TraceEventFields> latestErrors;
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

// Changes the format of trace files. Returns false if the format is unrecognized. No longer safe to call after a call
// to openTraceFile.
bool selectTraceFormatter(std::string format);

void addTraceRole(std::string role);
void removeTraceRole(std::string role);

enum trace_clock_t { TRACE_CLOCK_NOW, TRACE_CLOCK_REALTIME };
extern trace_clock_t g_trace_clock;
extern TraceBatch g_traceBatch;

#endif
