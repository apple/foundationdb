/*
 * Trace.h
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

#ifndef FLOW_TRACE_H
#define FLOW_TRACE_H
#pragma once

#include <algorithm>
#include <atomic>
#include <stdarg.h>
#include <stdint.h>
#include <string>
#include <string_view>
#include <map>
#include <set>
#include <type_traits>
#include "flow/BooleanParam.h"
#include "flow/IRandom.h"
#include "flow/Error.h"
#include "flow/ITrace.h"
#include "flow/Traceable.h"

#define TRACE_DEFAULT_ROLL_SIZE (10 << 20)
#define TRACE_DEFAULT_MAX_LOGS_SIZE (10 * TRACE_DEFAULT_ROLL_SIZE)

FDB_BOOLEAN_PARAM(InitializeTraceMetrics);

inline int fastrand() {
	static int g_seed = 0;
	g_seed = 214013 * g_seed + 2531011;
	return (g_seed >> 16) & 0x7fff;
}

// inline static bool TRACE_SAMPLE() { return fastrand()<16; }
inline static bool TRACE_SAMPLE() {
	return false;
}

extern thread_local int g_allocation_tracing_disabled;
extern bool g_traceProcessEvents;

// Each major level of severity has 10 levels of minor levels, which are not all
// used. when the numbers of severity events in each level are counted, they are
// grouped by the major level.
enum Severity {
	SevVerbose = 0,
	SevSample = 1,
	SevDebug = 5,
	SevInfo = 10,
	SevWarn = 20,
	SevWarnAlways = 30,
	SevError = 40,
	SevMaxUsed = SevError,
	SevMax = 1000000
};

inline Severity intToSeverity(int sevnum) {
	switch (sevnum) {
	case 0:
		return SevVerbose;
	case 1:
		return SevSample;
	case 5:
		return SevDebug;
	case 10:
		return SevInfo;
	case 20:
		return SevWarn;
	case 30:
		return SevWarnAlways;
	case 40:
		return SevError;
	case 1000000:
		return SevMax;
	default:
		return SevInfo;
	}
}

enum class ErrorKind : uint8_t {
	Unset,
	DiskIssue,
	BugDetected,
};

const int NUM_MAJOR_LEVELS_OF_EVENTS = SevMaxUsed / 10 + 1;

class TraceEventFields {
public:
	constexpr static FileIdentifier file_identifier = 11262274;
	typedef std::pair<std::string, std::string> Field;
	typedef std::vector<Field> FieldContainer;
	typedef FieldContainer::const_iterator FieldIterator;

	TraceEventFields();

	size_t size() const;
	size_t sizeBytes() const;
	FieldIterator begin() const;
	FieldIterator end() const;
	bool isAnnotated() const;
	void setAnnotated();

	void addField(const std::string& key, const std::string& value);
	void addField(std::string&& key, std::string&& value);

	const Field& operator[](int index) const;
	bool tryGetValue(std::string key, std::string& outValue) const;
	std::string getValue(std::string key) const;
	bool tryGetInt(std::string key, int& outVal, bool permissive = false) const;
	int getInt(std::string key, bool permissive = false) const;
	bool tryGetInt64(std::string key, int64_t& outVal, bool permissive = false) const;
	int64_t getInt64(std::string key, bool permissive = false) const;
	bool tryGetUint64(std::string key, uint64_t& outVal, bool permissive = false) const;
	uint64_t getUint64(std::string key, bool permissive = false) const;
	bool tryGetDouble(std::string key, double& outVal, bool permissive = false) const;
	double getDouble(std::string key, bool permissive = false) const;

	Field& mutate(int index);

	std::string toString() const;
	void validateFormat() const;
	template <class Archiver>
	void serialize(Archiver& ar) {
		static_assert(is_fb_function<Archiver>, "Streaming serializer has to use load/save");
		serializer(ar, fields);
	}

private:
	FieldContainer fields;
	size_t bytes;
	bool annotated;
};

template <class Archive>
inline void load(Archive& ar, TraceEventFields& value) {
	uint32_t count;
	ar >> count;

	std::string k;
	std::string v;
	for (uint32_t i = 0; i < count; ++i) {
		ar >> k >> v;
		value.addField(k, v);
	}
}
template <class Archive>
inline void save(Archive& ar, const TraceEventFields& value) {
	ar << (uint32_t)value.size();

	for (auto itr : value) {
		ar << itr.first << itr.second;
	}
}

class TraceBatch {
public:
	void addEvent(const char* name, uint64_t id, const char* location);
	void addAttach(const char* name, uint64_t id, uint64_t to);
	void addBuggify(int activated, int line, std::string file);
	void dump();

private:
	struct EventInfo {
		TraceEventFields fields;
		EventInfo(double time, double monotonicTime, const char* name, uint64_t id, const char* location);
	};

	struct AttachInfo {
		TraceEventFields fields;
		AttachInfo(double time, const char* name, uint64_t id, uint64_t to);
	};

	struct BuggifyInfo {
		TraceEventFields fields;
		BuggifyInfo(double time, int activated, int line, std::string file);
	};

	std::vector<EventInfo> eventBatch;
	std::vector<AttachInfo> attachBatch;
	std::vector<BuggifyInfo> buggifyBatch;
	static bool dumpImmediately();
};

struct DynamicEventMetric;

template <class T>
struct SpecialTraceMetricType
  : std::conditional<std::is_integral<T>::value || std::is_enum<T>::value, std::true_type, std::false_type>::type {
	static int64_t getValue(T v) { return v; }
};

#define TRACE_METRIC_TYPE(from, to)                                                                                    \
	template <>                                                                                                        \
	struct SpecialTraceMetricType<from> : std::true_type {                                                             \
		static to getValue(from v) {                                                                                   \
			return v;                                                                                                  \
		}                                                                                                              \
	}

TRACE_METRIC_TYPE(double, double);

class AuditedEvent;

inline constexpr AuditedEvent operator""_audit(const char*, size_t) noexcept;

class AuditedEvent {
	// special TraceEvents that may bypass throttling or suppression
	static constexpr std::string_view auditTopics[]{
		"AttemptedRPCToPrivatePrevented",
		"AuditTokenUsed",
		"AuthzPublicKeySetApply",
		"AuthzPublicKeySetRefreshError",
		"IncomingConnection",
		"InvalidToken",
		"N2_ConnectHandshakeError",
		"N2_ConnectHandshakeUnknownError",
		"N2_AcceptHandshakeError",
		"N2_AcceptHandshakeUnknownError",
		"UnauthorizedAccessPrevented",
	};
	const char* eventType;
	int len;
	bool valid;
	explicit constexpr AuditedEvent(const char* type, int len) noexcept
	  : eventType(type), len(len),
	    valid(std::find(std::begin(auditTopics), std::end(auditTopics), std::string_view(type, len)) !=
	          std::end(auditTopics)) // whitelist looked up during compile time
	{}

	friend constexpr AuditedEvent operator""_audit(const char*, size_t) noexcept;

public:
	constexpr const char* type() const noexcept { return eventType; }

	constexpr std::string_view typeSv() const noexcept { return std::string_view(eventType, len); }

	explicit constexpr operator bool() const noexcept { return valid; }
};

// This, along with private AuditedEvent constructor, guarantees that AuditedEvent is always created with a string
// literal
inline constexpr AuditedEvent operator""_audit(const char* eventType, size_t len) noexcept {
	return AuditedEvent(eventType, len);
}

// The BaseTraceEvent class is the parent class of TraceEvent and provides all functionality on the TraceEvent except
// for the functionality that can be used to suppress the trace event.
//
// This class is not intended to be used directly. Instead, this type is returned from most calls on trace events
// (e.g. detail). This is done to disallow calling suppression functions anywhere but first in a chained sequence of
// trace event function calls.
struct SWIFT_CXX_IMPORT_OWNED BaseTraceEvent {
	BaseTraceEvent(BaseTraceEvent&& ev);
	BaseTraceEvent& operator=(BaseTraceEvent&& ev);

	static void setNetworkThread();
	static bool isNetworkThread();

	static double getCurrentTime();
	static std::string printRealTime(double time);

	template <class T>
	typename std::enable_if<Traceable<T>::value && !std::is_enum_v<T>, BaseTraceEvent&>::type detail(std::string&& key,
	                                                                                                 const T& value) {
		if (enabled && init()) {
			auto s = Traceable<T>::toString(value);
			addMetric(key.c_str(), value, s);
			return detailImpl(std::move(key), std::move(s), false);
		}
		return *this;
	}

	template <class T>
	typename std::enable_if<Traceable<T>::value && !std::is_enum_v<T>, BaseTraceEvent&>::type detail(const char* key,
	                                                                                                 const T& value) {
		if (enabled && init()) {
			auto s = Traceable<T>::toString(value);
			addMetric(key, value, s);
			return detailImpl(std::string(key), std::move(s), false);
		}
		return *this;
	}
	template <class T>
	typename std::enable_if<std::is_enum<T>::value, BaseTraceEvent&>::type detail(const char* key, T value) {
		if (enabled && init()) {
			setField(key, int64_t(value));
			return detailImpl(std::string(key), format("%d", value), false);
		}
		return *this;
	}
	BaseTraceEvent& detailf(std::string key, const char* valueFormat, ...);

protected:
	class State {
		enum class Type {
			DISABLED = 0,
			ENABLED,
			FORCED,
		};
		Type value;

	public:
		constexpr State() noexcept : value(Type::DISABLED) {}
		State(Severity severity) noexcept;
		State(Severity severity, AuditedEvent) noexcept : State(severity) {
			if (*this)
				value = Type::FORCED;
		}

		State(const State& other) noexcept = default;
		State(State&& other) noexcept : value(other.value) { other.value = Type::DISABLED; }
		State& operator=(const State& other) noexcept = default;
		State& operator=(State&& other) noexcept {
			if (this != &other) {
				value = other.value;
				other.value = Type::DISABLED;
			}
			return *this;
		}
		bool operator==(const State& other) const noexcept = default;
		bool operator!=(const State& other) const noexcept = default;

		explicit operator bool() const noexcept { return value == Type::ENABLED || value == Type::FORCED; }

		void suppress() noexcept {
			if (value == Type::ENABLED)
				value = Type::DISABLED;
		}

		bool isSuppressible() const noexcept { return value == Type::ENABLED; }

		void promoteToForcedIfEnabled() noexcept {
			if (value == Type::ENABLED)
				value = Type::FORCED;
		}

		static constexpr State disabled() noexcept { return State(); }
	};

	BaseTraceEvent();
	BaseTraceEvent(Severity, const char* type, UID id = UID());

	template <class T>
	typename std::enable_if<SpecialTraceMetricType<T>::value, void>::type addMetric(const char* key,
	                                                                                const T& value,
	                                                                                const std::string&) {
		setField(key, SpecialTraceMetricType<T>::getValue(value));
	}

	template <class T>
	typename std::enable_if<!SpecialTraceMetricType<T>::value, void>::type addMetric(const char* key,
	                                                                                 const T&,
	                                                                                 const std::string& value) {
		setField(key, value);
	}

	void setField(const char* key, int64_t value);
	void setField(const char* key, double value);
	void setField(const char* key, const std::string& value);
	void setThreadId();

	// Private version of detailf that does NOT write to the eventMetric.  This is to be used by other detail methods
	// which can write field metrics of a more appropriate type than string but use detailf() to add to the TraceEvent.
	BaseTraceEvent& detailfNoMetric(std::string&& key, const char* valueFormat, ...);
	BaseTraceEvent& detailImpl(std::string&& key, std::string&& value, bool writeEventMetricField = true);

public:
	BaseTraceEvent& backtrace(const std::string& prefix = "");
	BaseTraceEvent& trackLatest(const std::string& trackingKey);
	// Sets the maximum length a field can be before it gets truncated. A value of 0 uses the default, a negative value
	// disables truncation. This should be called before the field whose length you want to change, and it can be
	// changed multiple times in a single event.
	BaseTraceEvent& setMaxFieldLength(int maxFieldLength);

	int getMaxFieldLength() const;

	// Sets the maximum event length before the event gets suppressed and a warning is logged. A value of 0 uses the
	// default, a negative value disables length suppression. This should be called before adding details.
	BaseTraceEvent& setMaxEventLength(int maxEventLength);

	int getMaxEventLength() const;

	BaseTraceEvent& GetLastError();

	bool isEnabled() const { return static_cast<bool>(enabled); }

	BaseTraceEvent& errorUnsuppressed(const class Error& e);
	BaseTraceEvent& setErrorKind(ErrorKind errorKind);

	explicit operator bool() const { return static_cast<bool>(enabled); }

	void log();

	void disable() { enabled.suppress(); } // Disables the trace event so it doesn't get logged

	virtual ~BaseTraceEvent(); // Actually logs the event

	// Return the number of invocations of TraceEvent() at the specified logging level.
	static unsigned long CountEventsLoggedAt(Severity);

	std::unique_ptr<DynamicEventMetric> tmpEventMetric; // This just just a place to store fields

	const TraceEventFields& getFields() const { return fields; }
	Severity getSeverity() const { return severity; }

	template <class Object>
	void moveTo(Object& obj) {
		obj.debugTrace(std::move(*this));
	}

	template <class Object>
	void moveTo(Reference<Object> obj) {
		obj->debugTrace(std::move(*this));
	}

	template <class Object>
	void moveTo(Object* obj) {
		obj->debugTrace(std::move(*this));
	}

protected:
	State enabled;
	bool initialized;
	bool logged;
	std::string trackingKey;
	TraceEventFields fields;
	Severity severity;
	ErrorKind errorKind{ ErrorKind::Unset };
	const char* type;
	UID id;
	Error err;

	int maxFieldLength;
	int maxEventLength;
	int timeIndex;
	int errorKindIndex{ -1 };

	void setSizeLimits();

	static unsigned long eventCounts[NUM_MAJOR_LEVELS_OF_EVENTS];
	static thread_local bool networkThread;

	State init();
	void init(struct TraceInterval&);
};

// The TraceEvent class provides the implementation for BaseTraceEvent. The only functions that should be implemented
// here are those that must be called first in a trace event call sequence, such as the suppression functions.
struct SWIFT_CXX_IMPORT_OWNED TraceEvent : public BaseTraceEvent {
	TraceEvent() {}
	TraceEvent(const char* type, UID id = UID()); // Assumes SevInfo severity
	TraceEvent(Severity, const char* type, UID id = UID());
	TraceEvent(struct TraceInterval&, UID id = UID());
	TraceEvent(Severity severity, struct TraceInterval& interval, UID id = UID());
	TraceEvent(AuditedEvent, UID id = UID());
	TraceEvent(Severity, AuditedEvent, UID id = UID());

	BaseTraceEvent& error(const class Error& e);
	TraceEvent& errorUnsuppressed(const class Error& e) {
		BaseTraceEvent::errorUnsuppressed(e);
		return *this;
	}

	BaseTraceEvent& sample(double sampleRate, bool logSampleRate = true);
	BaseTraceEvent& suppressFor(double duration, bool logSuppressedEventCount = true);

	// Exposed for Swift which cannot use std::enable_if
	template <class T>
	void addDetail(std::string key, const T& value) {
		if (enabled && init()) {
			auto s = Traceable<T>::toString(value);
			addMetric(key.c_str(), value, s);
			detailImpl(std::move(key), std::move(s), false);
		}
	}
};

class StringRef;

struct TraceInterval {
	TraceInterval(const char* type, UID id = UID()) : type(type), pairID(id), count(-1), severity(SevInfo) {}

	TraceInterval& begin();
	TraceInterval& end() { return *this; }

	const char* type;
	UID pairID;
	int count;
	Severity severity;
};

struct LatestEventCache {
public:
	void set(std::string tag, const TraceEventFields& fields);
	TraceEventFields get(std::string const& tag);
	std::vector<TraceEventFields> getAll();
	std::vector<TraceEventFields> getAllUnsafe();

	void clear(std::string const& prefix);
	void clear();

	// Latest error tracking only tracks errors when called from the main thread. Other errors are silently ignored.
	void setLatestError(const TraceEventFields& contents);
	TraceEventFields getLatestError();

private:
	std::map<struct NetworkAddress, std::map<std::string, TraceEventFields>> latest;
	std::map<struct NetworkAddress, TraceEventFields> latestErrors;
};

extern LatestEventCache latestEventCache;

struct EventCacheHolder : public ReferenceCounted<EventCacheHolder> {
	std::string trackingKey;

	EventCacheHolder(const std::string& trackingKey) : trackingKey(trackingKey) {}

	~EventCacheHolder() { latestEventCache.clear(trackingKey); }
};

// Evil but potentially useful for verbose messages:
#if CENABLED(0, NOT_IN_CLEAN)
#define TRACE(t, m)                                                                                                    \
	if (TraceEvent::isEnabled(t))                                                                                      \
	TraceEvent(t, m)
#endif

struct NetworkAddress;
template <class T>
class Optional;

using OptionalStdString = Optional<std::string>;
using OptionalInt64 = Optional<int64_t>;

void openTraceFile(const Optional<NetworkAddress>& na,
                   uint64_t rollsize,
                   uint64_t maxLogsSize,
                   std::string directory = ".",
                   std::string baseOfBase = "trace",
                   std::string logGroup = "default",
                   std::string identifier = "",
                   std::string tracePartialFileSuffix = "",
                   InitializeTraceMetrics initializeTraceMetrics = InitializeTraceMetrics::False);
void closeTraceFile();
bool traceFileIsOpen();
void flushTraceFileVoid();

// Changes the format of trace files. Returns false if the format is unrecognized. No longer safe to call after a call
// to openTraceFile.
bool selectTraceFormatter(std::string format);
// Returns true iff format is recognized.
bool validateTraceFormat(std::string format);

// Select the clock source for trace files. Returns false if the format is unrecognized. No longer safe to call after a
// call to openTraceFile.
bool selectTraceClockSource(std::string source);
// Returns true iff source is recognized.
bool validateTraceClockSource(std::string source);

void addTraceRole(std::string const& role);
void removeTraceRole(std::string const& role);
void retrieveTraceLogIssues(std::set<std::string>& out);
void setTraceLogGroup(const std::string& role);
void addUniversalTraceField(std::string const& name, std::string const& value);
bool isTraceLocalAddressSet();
void setTraceLocalAddress(const NetworkAddress& addr);
void disposeTraceFileWriter();
std::string getTraceFormatExtension();
uint64_t getTraceThreadId();

template <class T>
class Future;
class Void;
Future<Void> pingTraceLogWriterThread();

enum trace_clock_t { TRACE_CLOCK_NOW, TRACE_CLOCK_REALTIME };
extern std::atomic<trace_clock_t> g_trace_clock;
extern TraceBatch g_traceBatch;

#define DUMPTOKEN(name)                                                                                                \
	TraceEvent("DumpToken", recruited.id()).detail("Name", #name).detail("Token", name.getEndpoint().token)

#define DisabledTraceEvent(...) false && TraceEvent()
#endif
