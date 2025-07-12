/*
 * TDMetric.actor.h
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

#pragma once

// When actually compiled (NO_INTELLISENSE), include the generated version of this file.  In intellisense use the source
// version.
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include <cstddef>
#if defined(NO_INTELLISENSE) && !defined(FLOW_TDMETRIC_ACTOR_G_H)
#define FLOW_TDMETRIC_ACTOR_G_H
#include "flow/TDMetric.actor.g.h"
#elif !defined(FLOW_TDMETRIC_ACTOR_H)
#define FLOW_TDMETRIC_ACTOR_H
#include <string>
#include <unordered_map>
#include "flow/flow.h"
#include "flow/network.h"
#include "flow/Knobs.h"
#include "flow/genericactors.actor.h"
#include "flow/CompressedInt.h"
#include "flow/OTELMetrics.h"
#include <algorithm>
#include <functional>
#include <cmath>
#include "flow/actorcompiler.h" // This must be the last #include.

enum MetricsDataModel { STATSD = 0, OTLP, NONE };
MetricsDataModel knobToMetricModel(const std::string& knob);

struct MetricNameRef {
	MetricNameRef() {}
	MetricNameRef(const StringRef& type, const StringRef& name, const StringRef& id) : type(type), name(name), id(id) {}
	MetricNameRef(Arena& a, const MetricNameRef& copyFrom)
	  : type(a, copyFrom.type), name(a, copyFrom.name), id(a, copyFrom.id) {}

	StringRef type, name, id;

	std::string toString() const {
		return format("(%s,%s,%s,%s)", type.toString().c_str(), name.toString().c_str(), id.toString().c_str());
	}

	int expectedSize() const { return type.expectedSize() + name.expectedSize(); }

	inline int compare(MetricNameRef const& r) const {
		int cmp;
		if ((cmp = type.compare(r.type))) {
			return cmp;
		}
		if ((cmp = name.compare(r.name))) {
			return cmp;
		}
		return id.compare(r.id);
	}
};

extern std::string reduceFilename(std::string const& filename);

inline bool operator<(const MetricNameRef& l, const MetricNameRef& r) {
	int cmp = l.type.compare(r.type);
	if (cmp == 0) {
		cmp = l.name.compare(r.name);
		if (cmp == 0)
			cmp = l.id.compare(r.id);
	}
	return cmp < 0;
}

inline bool operator==(const MetricNameRef& l, const MetricNameRef& r) {
	return l.type == r.type && l.name == r.name && l.id == r.id;
}

inline bool operator!=(const MetricNameRef& l, const MetricNameRef& r) {
	return !(l == r);
}

struct KeyWithWriter {
	Standalone<StringRef> key;
	BinaryWriter writer;
	int writerOffset;

	KeyWithWriter(Standalone<StringRef> const& key, BinaryWriter& writer, int writerOffset = 0)
	  : key(key), writer(std::move(writer)), writerOffset(writerOffset) {}
	KeyWithWriter(KeyWithWriter&& r)
	  : key(std::move(r.key)), writer(std::move(r.writer)), writerOffset(r.writerOffset) {}
	void operator=(KeyWithWriter&& r) {
		key = std::move(r.key);
		writer = std::move(r.writer);
		writerOffset = r.writerOffset;
	}

	StringRef value() const { return StringRef(writer.toValue().substr(writerOffset)); }
};

// This is a very minimal interface for getting metric data from the DB which is needed
// to support continuing existing metric data series.
// It's lack of generality is intentional.
class IMetricDB {
public:
	virtual ~IMetricDB() {}

	// key should be the result of calling metricKey or metricFieldKey with time = 0
	virtual Future<Optional<Standalone<StringRef>>> getLastBlock(Standalone<StringRef> key) = 0;
};

// Key generator for metric keys for various things.
struct MetricKeyRef {
	MetricKeyRef() : level(-1) {}
	MetricKeyRef(Arena& a, const MetricKeyRef& copyFrom)
	  : prefix(a, copyFrom.prefix), name(a, copyFrom.name), address(a, copyFrom.address),
	    fieldName(a, copyFrom.fieldName), fieldType(a, copyFrom.fieldType), level(copyFrom.level) {}

	StringRef prefix;
	MetricNameRef name;
	StringRef address;
	StringRef fieldName;
	StringRef fieldType;
	uint64_t level;

	int expectedSize() const {
		return prefix.expectedSize() + name.expectedSize() + address.expectedSize() + fieldName.expectedSize() +
		       fieldType.expectedSize();
	}

	template <typename T>
	inline MetricKeyRef withField(const T& field) const {
		MetricKeyRef mk(*this);
		mk.fieldName = field.name();
		mk.fieldType = field.typeName();
		return mk;
	}

	const Standalone<StringRef> packLatestKey() const;
	const Standalone<StringRef> packDataKey(int64_t time = -1) const;
	const Standalone<StringRef> packFieldRegKey() const;

	bool isField() const { return fieldName.size() > 0 && fieldType.size() > 0; }
	void writeField(BinaryWriter& wr) const;
	void writeMetricName(BinaryWriter& wr) const;
};

struct FDBScope {
	std::vector<KeyWithWriter> inserts;
	std::vector<KeyWithWriter> appends;
	std::vector<std::pair<Standalone<StringRef>, Standalone<StringRef>>> updates;
	std::vector<std::function<Future<Void>(IMetricDB*, FDBScope*)>> callbacks;

	void clear() {
		inserts.clear();
		appends.clear();
		updates.clear();
		callbacks.clear();
	}
};

struct MetricBatch {
	FDBScope scope;

	MetricBatch() {}

	MetricBatch(FDBScope* in) {
		assert(in != nullptr);
		scope.inserts = std::move(in->inserts);
		scope.appends = std::move(in->appends);
		scope.updates = std::move(in->updates);
		scope.callbacks = std::move(in->callbacks);
	}

	void clear() { scope.clear(); }
};

template <typename T>
inline StringRef metricTypeName() {
	// If this function does not compile then T is not a supported metric type
	return T::metric_field_type();
}
#define MAKE_TYPENAME(T, S)                                                                                            \
	template <>                                                                                                        \
	inline StringRef metricTypeName<T>() {                                                                             \
		return S;                                                                                                      \
	}
MAKE_TYPENAME(bool, "Bool"_sr)
MAKE_TYPENAME(int64_t, "Int64"_sr)
MAKE_TYPENAME(double, "Double"_sr)
MAKE_TYPENAME(Standalone<StringRef>, "String"_sr)
#undef MAKE_TYPENAME

struct BaseMetric;
class IMetric;

// The collection of metrics that exist for a single process, at a single address.
class TDMetricCollection {
public:
	TDMetricCollection() : currentTimeBytes(0) {}

	// Metric Name to reference to its instance
	Map<Standalone<MetricNameRef>,
	    Reference<BaseMetric>,
	    MapPair<Standalone<MetricNameRef>, Reference<BaseMetric>>,
	    int>
	    metricMap;

	AsyncTrigger metricAdded;
	AsyncTrigger metricEnabled;
	AsyncTrigger metricRegistrationChanged;

	// Initialize the collection.  Once this returns true, metric data can be written to a database.  Note that metric
	// data can be logged before that time, just not written to a database.
	bool init() {
		// Get and store the local address in the metric collection, but only if it is not 0.0.0.0:0
		if (address.size() == 0) {
			NetworkAddress addr = g_network->getLocalAddress();
			if (addr.ip.isValid() && addr.port != 0)
				address = StringRef(addr.toString());
		}
		return address.size() != 0;
	}

	// Returns the TDMetrics that the calling process should use
	static TDMetricCollection* getTDMetrics() {
		if (g_network == nullptr)
			return nullptr;
		return static_cast<TDMetricCollection*>((void*)g_network->global(INetwork::enTDMetrics));
	}

	Deque<uint64_t> rollTimes;
	int64_t currentTimeBytes;
	Standalone<StringRef> address;

	void checkRoll(uint64_t t, int64_t usedBytes);
	bool canLog(int level) const;
};

class MetricCollection {
public:
	std::unordered_map<UID, IMetric*> map;
	std::unordered_map<UID, OTEL::OTELSum> sumMap;
	std::unordered_map<UID, OTEL::OTELHistogram> histMap;
	std::unordered_map<UID, OTEL::OTELGauge> gaugeMap;
	std::vector<std::string> statsd_message;

	MetricCollection() {}

	static MetricCollection* getMetricCollection() {
		if (g_network == nullptr || knobToMetricModel(FLOW_KNOBS->METRICS_DATA_MODEL) == MetricsDataModel::NONE)
			return nullptr;
		return static_cast<MetricCollection*>((void*)g_network->global(INetwork::enMetrics));
	}
};

struct MetricData {
	uint64_t start;
	uint64_t rollTime;
	uint64_t appendStart;
	BinaryWriter writer;

	explicit MetricData(uint64_t appendStart = 0)
	  : start(0), rollTime(std::numeric_limits<uint64_t>::max()), appendStart(appendStart),
	    writer(AssumeVersion(g_network->protocolVersion())) {}

	MetricData(MetricData&& r) noexcept
	  : start(r.start), rollTime(r.rollTime), appendStart(r.appendStart), writer(std::move(r.writer)) {}

	void operator=(MetricData&& r) noexcept {
		start = r.start;
		rollTime = r.rollTime;
		appendStart = r.appendStart;
		writer = std::move(r.writer);
	}

	std::string toString() const;
};

// Some common methods to reduce code redundancy across different metric definitions
template <typename T, typename _ValueType = Void>
struct MetricUtil {
	typedef _ValueType ValueType;
	typedef T MetricType;

	// Looks up a metric by name and id and returns a reference to it if it exists.
	// Empty names will not be looked up.
	// If create is true then a metric will be created with the given initial value if one could not be found to return.
	// If a metric is created and name is not empty then the metric will be placed in the collection.
	static Reference<T> getOrCreateInstance(StringRef const& name,
	                                        StringRef const& id = StringRef(),
	                                        bool create = false,
	                                        ValueType initial = ValueType()) {
		Reference<T> m;
		TDMetricCollection* collection = TDMetricCollection::getTDMetrics();

		// If there is a metric collect and this metric has a name then look it up in the collection
		bool useMap = collection != nullptr && name.size() > 0;
		MetricNameRef mname;

		if (useMap) {
			mname = MetricNameRef(T::metricType, name, id);
			auto mi = collection->metricMap.find(mname);
			if (mi != collection->metricMap.end()) {
				m = mi->value.castTo<T>();
			}
		}

		// If we don't have a valid metric reference yet and the create flag was set then create one and possibly put it
		// in the map
		if (!m && create) {
			// Metric not found in collection but create is set then create it in the map
			m = makeReference<T>(mname, initial);
			if (useMap) {
				collection->metricMap[mname] = m.template castTo<BaseMetric>();
				collection->metricAdded.trigger();
			}
		}

		return m;
	}

	static ValueType getValueOrDefault(StringRef const& name,
	                                   StringRef const& id = StringRef(),
	                                   ValueType defaultValue = ValueType()) {
		Reference<T> r = getOrCreateInstance(name, id);
		if (r) {
			return r->getValue();
		}
		return defaultValue;
	}

	// Lookup the T metric by name and return its value (or nullptr if it doesn't exist)
	static T* lookupMetric(MetricNameRef const& name) {
		auto it = T::metricMap().find(name);
		if (it != T::metricMap().end())
			return it->value;
		return nullptr;
	}
};

// index_sequence implementation since VS2013 doesn't have it yet
template <size_t... Ints>
class index_sequence {
public:
	static size_t size() { return sizeof...(Ints); }
};

template <size_t Start, typename Indices, size_t End>
struct make_index_sequence_impl;

template <size_t Start, size_t... Indices, size_t End>
struct make_index_sequence_impl<Start, index_sequence<Indices...>, End> {
	typedef typename make_index_sequence_impl<Start + 1, index_sequence<Indices..., Start>, End>::type type;
};

template <size_t End, size_t... Indices>
struct make_index_sequence_impl<End, index_sequence<Indices...>, End> {
	typedef index_sequence<Indices...> type;
};

// The code that actually implements tuple_map
template <size_t I, typename F, typename... Tuples>
auto tuple_zip_invoke(F f, const Tuples&... ts) -> decltype(f(std::get<I>(ts)...)) {
	return f(std::get<I>(ts)...);
}

template <typename F, size_t... Is, typename... Tuples>
auto tuple_map_impl(F f, index_sequence<Is...>, const Tuples&... ts)
    -> decltype(std::make_tuple(tuple_zip_invoke<Is>(f, ts...)...)) {
	return std::make_tuple(tuple_zip_invoke<Is>(f, ts...)...);
}

// tuple_map( f(a,b), (a1,a2,a3), (b1,b2,b3) ) = (f(a1,b1), f(a2,b2), f(a3,b3))
template <typename F, typename Tuple, typename... Tuples>
auto tuple_map(F f, const Tuple& t, const Tuples&... ts) -> decltype(tuple_map_impl(
    f,
    typename make_index_sequence_impl<0, index_sequence<>, std::tuple_size<Tuple>::value>::type(),
    t,
    ts...)) {
	return tuple_map_impl(
	    f, typename make_index_sequence_impl<0, index_sequence<>, std::tuple_size<Tuple>::value>::type(), t, ts...);
}

template <class T>
struct Descriptor {
#ifndef NO_INTELLISENSE
	using fields = std::tuple<>;
	typedef make_index_sequence_impl<0, index_sequence<>, std::tuple_size<fields>::value>::type field_indexes;

	static StringRef typeName() { return ""_sr; }
#endif
};

// FieldHeader is a serializable (FIXED SIZE!) and updatable Header type for Metric field levels.
// Update is via += with either a T or another FieldHeader
// Default implementation is sufficient for ints and doubles
template <typename T>
struct FieldHeader {
	FieldHeader() : version(1), count(0), sum(0) {}
	uint8_t version;
	int64_t count;
	// sum is a T if T is arithmetic, otherwise it's an int64_t
	typename std::conditional<std::is_floating_point<T>::value, double, int64_t>::type sum;

	void update(FieldHeader const& h) {
		count += h.count;
		sum += h.sum;
	}
	void update(T const& v) {
		++count;
		sum += v;
	}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
		ASSERT(version == 1);
		serializer(ar, count, sum);
	}
};

template <>
inline void FieldHeader<Standalone<StringRef>>::update(Standalone<StringRef> const& v) {
	++count;
	sum += v.size();
}

// FieldValueBlockEncoding is a class for reading and writing encoded field values to and from field
// value data blocks.  Note that an implementation can be stateful.
// Proper usage requires that a single Encoding instance is used to either write all field values to a metric
// data block or to read all field values from a metric value block.  This usage pattern enables enables
// encoding and decoding values as deltas from previous values.
//
// The default implementation works for ints and writes delta from the previous value.
template <typename T>
struct FieldValueBlockEncoding {
	FieldValueBlockEncoding() : prev(0) {}
	inline void write(BinaryWriter& w, T v) {
		w << CompressedInt<T>(v - prev);
		prev = v;
	}
	T read(BinaryReader& r) {
		CompressedInt<T> v;
		r >> v;
		prev += v.value;
		return prev;
	}
	T prev;
};

template <>
struct FieldValueBlockEncoding<double> {
	inline void write(BinaryWriter& w, double v) { w << v; }
	double read(BinaryReader& r) {
		double v;
		r >> v;
		return v;
	}
};

template <>
struct FieldValueBlockEncoding<bool> {
	inline void write(BinaryWriter& w, bool v) { w.serializeBytes(v ? "\x01"_sr : "\x00"_sr); }
	bool read(BinaryReader& r) {
		uint8_t* v = (uint8_t*)r.readBytes(sizeof(uint8_t));
		return *v != 0;
	}
};

// Encoder for strings, writes deltas
template <>
struct FieldValueBlockEncoding<Standalone<StringRef>> {
	inline void write(BinaryWriter& w, Standalone<StringRef> const& v) {
		int reuse = 0;
		int stop = std::min(v.size(), prev.size());
		while (reuse < stop && v[reuse] == prev[reuse])
			++reuse;
		w << CompressedInt<int>(reuse) << CompressedInt<int>(v.size() - reuse);
		if (v.size() > reuse)
			w.serializeBytes(v.substr(reuse));
		prev = v;
	}
	Standalone<StringRef> read(BinaryReader& r) {
		CompressedInt<int> reuse;
		CompressedInt<int> extra;
		r >> reuse >> extra;
		ASSERT(reuse.value >= 0 && extra.value >= 0 && reuse.value <= prev.size());
		Standalone<StringRef> v = makeString(reuse.value + extra.value);
		memcpy(mutateString(v), prev.begin(), reuse.value);
		memcpy(mutateString(v) + reuse.value, r.readBytes(extra.value), extra.value);
		prev = v;
		return v;
	}
	// Using a Standalone<StringRef> for prev is efficient for writing but not great for reading.
	Standalone<StringRef> prev;
};

// Field level for value type of T using header type of Header.  Default header type is the default FieldHeader
// implementation for type T.
template <class T, class Header = FieldHeader<T>, class Encoder = FieldValueBlockEncoding<T>>
class FieldLevel {
	int64_t appendUsed;
	Deque<MetricData> metrics;
	Header header;

public:
	// The previous header and the last timestamp at which an out going MetricData block requires header patching
	Optional<Header> previousHeader;
	uint64_t lastTimeRequiringHeaderPatch;

	Encoder enc;

	explicit FieldLevel() : appendUsed(0) {
		metrics.emplace_back();
		metrics.back().writer << header;
	}

	// update Header, use Encoder to write T v
	void log(T v, uint64_t t, bool& overflow, int64_t& bytes) {
		int lastLength = metrics.back().writer.getLength();
		if (metrics.back().start == 0)
			metrics.back().start = t;

		header.update(v);
		enc.write(metrics.back().writer, v);

		bytes += metrics.back().writer.getLength() - lastLength;
		if (lastLength + appendUsed > FLOW_KNOBS->MAX_METRIC_SIZE)
			overflow = true;
	}

	void nextKey(uint64_t t) {
		// If nothing has actually been written to the current block, don't add a new block,
		// just modify this one if needed so that the next log call will set the ts for this block.
		auto& m = metrics.back();
		if (m.start == 0 && m.appendStart == 0)
			return;

		// This block would have appended but had no data so just reset it to a non-append block instead of adding a new
		// one
		if (m.appendStart != 0 && m.writer.getLength() == 0) {
			m.appendStart = 0;
			m.writer << header;
			enc = Encoder();
			return;
		}

		metrics.back().rollTime = t;
		metrics.emplace_back();
		metrics.back().writer << header;
		enc = Encoder();
		appendUsed = 0;
	}

	void rollMetric(uint64_t t) {
		ASSERT(metrics.size());

		if (metrics.back().start) {
			metrics.back().rollTime = t;
			appendUsed += metrics.back().writer.getLength();
			if (metrics.back().appendStart)
				metrics.emplace_back(metrics.back().appendStart);
			else
				metrics.emplace_back(metrics.back().start);
		}
	}

	// Calculate header as of the end of a value block
	static Header calculateHeader(StringRef block) {
		BinaryReader r(block, AssumeVersion(g_network->protocolVersion()));
		Header h;
		r >> h;
		Encoder dec;
		while (!r.empty()) {
			T v = dec.read(r);
			h.update(v);
		}
		return h;
	}

	// Read header at position, update it with previousHeader, overwrite old header with new header.
	static void updateSerializedHeader(StringRef buf, const Header& patch) {
		BinaryReader r(buf, AssumeVersion(g_network->protocolVersion()));
		Header h;
		r >> h;
		h.update(patch);
		OverWriter w(mutateString(buf), buf.size(), AssumeVersion(g_network->protocolVersion()));
		w << h;
	}

	// Flushes data blocks in metrics to batch, optionally patching headers if a header is given
	void flushUpdates(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) {
		while (metrics.size()) {
			auto& data = metrics.front();

			if (data.start != 0 && data.rollTime <= rollTime) {
				// If this data is to be appended, write it to the batch now.
				if (data.appendStart) {
					batch.scope.appends.push_back(KeyWithWriter(mk.packDataKey(data.appendStart), data.writer));
				} else {
					// Otherwise, insert but first, patch the header if this block is old enough
					if (data.rollTime <= lastTimeRequiringHeaderPatch) {
						ASSERT(previousHeader.present());
						FieldLevel<T>::updateSerializedHeader(data.writer.toValue(), previousHeader.get());
					}

					batch.scope.inserts.push_back(KeyWithWriter(mk.packDataKey(data.start), data.writer));
				}

				if (metrics.size() == 1) {
					rollMetric(data.rollTime);
					metrics.pop_front();
					break;
				}

				metrics.pop_front();
			} else
				break;
		}
	}

	ACTOR static Future<Void> updatePreviousHeader(FieldLevel* self,
	                                               IMetricDB* db,
	                                               Standalone<MetricKeyRef> mk,
	                                               uint64_t rollTime,
	                                               FDBScope* scope) {

		Optional<Standalone<StringRef>> block = wait(db->getLastBlock(mk.packDataKey(-1)));

		// If the block is present, use it
		if (block.present()) {
			// Calculate the previous data's final header value
			Header oldHeader = calculateHeader(block.get());

			// Set the previous header in self to this header for us in patching outgoing blocks
			self->previousHeader = oldHeader;

			// Update the header in self so the next new block created will be current
			self->header.update(oldHeader);

			// Any blocks already in the metrics queue will need to be patched at the time that they are
			// flushed to the DB (which isn't necessarity part of the current flush) so set the last time
			// that requires a patch to the time of the last MetricData in the queue
			self->lastTimeRequiringHeaderPatch = self->metrics.back().rollTime;
		} else {
			// Otherwise, there is no previous header so no headers need to be updated at all ever.
			// Set the previous header to an empty header so that flush() sees that this process
			// has already finished, and set lastTimeRequiringHeaderPatch to 0 since no blocks ever need to be patched.
			self->previousHeader = Header();
			self->lastTimeRequiringHeaderPatch = 0;
		}

		// Now flush the level data up to the rollTime argument and patch anything older than
		// lastTimeRequiringHeaderPatch
		MetricBatch batch{ scope };
		self->flushUpdates(mk, rollTime, batch);

		return Void();
	}

	// Flush this level's data to the output batch.
	// This function must NOT be called again until any callbacks added to batch have been completed.
	void flush(const MetricKeyRef& mk, uint64_t rollTime, MetricBatch& batch) {
		// Don't do anything if there is no data in the queue to flush.
		if (metrics.empty() || metrics.front().start == 0)
			return;

		// If the previous header is present then just call flushUpdates now.
		if (previousHeader.present())
			return flushUpdates(mk, rollTime, batch);

		Standalone<MetricKeyRef> mkCopy = mk;

		// Previous header is not present so queue a callback which will update it
		batch.scope.callbacks.push_back([=](IMetricDB* db, FDBScope* s) mutable -> Future<Void> {
			return updatePreviousHeader(this, db, mkCopy, rollTime, s);
		});
	}
};

// A field Description to be used for continuous metrics, whose field name and type should never be accessed
struct NullDescriptor {
	static StringRef name() { return StringRef(); }
};

// Descriptor must have the methods name() and typeName().  They can be either static or member functions (such as for
// runtime configurability). Descriptor is inherited so that syntactically Descriptor::fn() works in either case and so
// that an empty Descriptor with static methods will take up 0 space.  EventField() accepts an optional Descriptor
// instance.
template <class T, class Descriptor = NullDescriptor, class FieldLevelType = FieldLevel<T>>
struct EventField : public Descriptor {
	std::vector<FieldLevelType> levels;

	EventField(EventField&& r) noexcept : Descriptor(r), levels(std::move(r.levels)) {}

	void operator=(EventField&& r) noexcept { levels = std::move(r.levels); }

	EventField(Descriptor d = Descriptor()) : Descriptor(d) {}

	static StringRef typeName() { return metricTypeName<T>(); }

	void init() {
		if (levels.size() != FLOW_KNOBS->MAX_METRIC_LEVEL) {
			levels.clear();
			levels.resize(FLOW_KNOBS->MAX_METRIC_LEVEL);
		}
	}

	void log(T v, uint64_t t, int64_t l, bool& overflow, int64_t& bytes) {
		return levels[l].log(v, t, overflow, bytes);
	}

	void nextKey(uint64_t t, int level) { levels[level].nextKey(t); }

	void nextKeyAllLevels(uint64_t t) {
		for (int64_t i = 0; i < FLOW_KNOBS->MAX_METRIC_LEVEL; i++)
			nextKey(t, i);
	}

	void rollMetric(uint64_t t) {
		for (int i = 0; i < levels.size(); i++) {
			levels[i].rollMetric(t);
		}
	}

	void flushField(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) {
		MetricKeyRef fk = mk.withField(*this);
		for (int j = 0; j < levels.size(); ++j) {
			fk.level = j;
			levels[j].flush(fk, rollTime, batch);
		}
	}

	// Writes and Event metric field registration key
	void registerField(const MetricKeyRef& mk, std::vector<Standalone<StringRef>>& fieldKeys) {
		fieldKeys.push_back(mk.withField(*this).packFieldRegKey());
	}
};

struct MakeEventField {
	template <class Descriptor>
	EventField<typename Descriptor::type, Descriptor> operator()(Descriptor) {
		return EventField<typename Descriptor::type, Descriptor>();
	}
};

struct TimeDescriptor {
	static StringRef name() { return "Time"_sr; }
};

struct BaseMetric {
	BaseMetric(MetricNameRef const& name) : metricName(name), enabled(false), pCollection(nullptr), registered(false) {
		setConfig(false);
	}
	virtual ~BaseMetric() {}

	virtual void addref() = 0;
	virtual void delref() = 0;

	virtual void rollMetric(uint64_t t) = 0;

	virtual void flushData(const MetricKeyRef& mk, uint64_t rollTime, MetricBatch& batch) = 0;
	virtual void registerFields(const MetricKeyRef& mk, std::vector<Standalone<StringRef>>& fieldKeys) {};

	// Set the metric's config.  An assert will fail if the metric is enabled before the metrics collection is
	// available.
	void setConfig(bool enable, int minLogLevel = 0) {
		bool wasEnabled = enabled;
		enabled = enable;
		minLevel = minLogLevel;

		if (enable && pCollection == nullptr) {
			pCollection = TDMetricCollection::getTDMetrics();
			ASSERT(pCollection != nullptr);
		}

		if (wasEnabled != enable) {
			if (enabled) {
				onEnable();
				pCollection->metricEnabled.trigger();
			} else
				onDisable();
		}
	}

	// Callbacks for when metric is Enabled or Disabled.
	// Metrics should verify their underlying storage on Enable because they could have been initially created
	// at a time when the knobs were not initialized.
	virtual void onEnable() = 0;
	virtual void onDisable() {};

	// Combines checking this metric's configured minimum level and any collection-wide throttling
	// This should only be called after it is determined that a metric is enabled.
	bool canLog(int level) const { return level >= minLevel && pCollection->canLog(level); }

	Standalone<MetricNameRef> metricName;

	bool enabled; // The metric is currently logging data
	int minLevel; // The minimum level that will be logged.

	// All metrics need a pointer to their collection for performance reasons - every time a data point is logged
	// canLog must be called which uses the collection's canLog to decide based on the metric write queue.
	TDMetricCollection* pCollection;

	// The metric has been registered in its current form (some metrics can change and require re-reg)
	bool registered;
};

struct BaseEventMetric : BaseMetric {

	BaseEventMetric(MetricNameRef const& name) : BaseMetric(name) {}

	// Needed for MetricUtil
	alignas(8) static const StringRef metricType;
	Void getValue() const { return Void(); }
	~BaseEventMetric() override {}

	// Every metric should have a set method for its underlying type in order for MetricUtil::getOrCreateInstance
	// to initialize it.  In the case of event metrics there is no underlying type so the underlying type
	// is Void and set does nothing.
	void set(Void const& val) {}

	virtual StringRef getTypeName() const = 0;
};

template <class E>
struct EventMetric final : E, ReferenceCounted<EventMetric<E>>, MetricUtil<EventMetric<E>>, BaseEventMetric {
	EventField<int64_t, TimeDescriptor> time;
	bool latestRecorded;
	decltype(tuple_map(MakeEventField(), typename Descriptor<E>::fields())) values;

	void addref() override { ReferenceCounted<EventMetric<E>>::addref(); }
	void delref() override { ReferenceCounted<EventMetric<E>>::delref(); }

	EventMetric(MetricNameRef const& name, Void) : BaseEventMetric(name), latestRecorded(false) {}

	StringRef getTypeName() const override { return Descriptor<E>::typeName(); }

	void onEnable() override {
		// Must initialize fields, previously knobs may not have been set.
		time.init();
		initFields(typename Descriptor<E>::field_indexes());
	}

	// Log the event.
	// Returns the time that was logged for the event so that it can be passed to other events that need to be
	// time-sync'd. NOTE:  Do NOT use the same time for two consecutive loggings of the SAME event.  This *could* cause
	// there to be metric data blocks such that the last timestamp of one block is equal to the first timestamp of the
	// next, which means if a search is done for the exact timestamp then the first event will not be found.
	uint64_t log(uint64_t explicitTime = 0) {
		if (!enabled)
			return 0;

		uint64_t t = explicitTime ? explicitTime : timer_int();
		double x = deterministicRandom()->random01();

		int64_t l = 0;
		if (x == 0.0)
			l = FLOW_KNOBS->MAX_METRIC_LEVEL - 1;
		else
			l = std::min(FLOW_KNOBS->MAX_METRIC_LEVEL - 1,
			             (int64_t)(::log(1.0 / x) / FLOW_KNOBS->METRIC_LEVEL_DIVISOR));

		if (!canLog(l))
			return 0;

		bool overflow = false;
		int64_t bytes = 0;
		time.log(t, t, l, overflow, bytes);
		logFields(typename Descriptor<E>::field_indexes(), t, l, overflow, bytes);
		if (overflow) {
			time.nextKey(t, l);
			nextKeys(typename Descriptor<E>::field_indexes(), t, l);
		}
		latestRecorded = false;
		return t;
	}

	template <size_t... Is>
	void logFields(index_sequence<Is...>, uint64_t t, int64_t l, bool& overflow, int64_t& bytes) {
#ifdef NO_INTELLISENSE
		auto _ = { (std::get<Is>(values).log(
			            std::tuple_element<Is, typename Descriptor<E>::fields>::type::get(static_cast<E&>(*this)),
			            t,
			            l,
			            overflow,
			            bytes),
			        Void())... };
		(void)_;
#endif
	}

	template <size_t... Is>
	void initFields(index_sequence<Is...>) {
#ifdef NO_INTELLISENSE
		auto _ = { (std::get<Is>(values).init(), Void())... };
		(void)_;
#endif
	}

	template <size_t... Is>
	void nextKeys(index_sequence<Is...>, uint64_t t, int64_t l) {
#ifdef NO_INTELLISENSE
		auto _ = { (std::get<Is>(values).nextKey(t, l), Void())... };
		(void)_;
#endif
	}

	void flushData(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) override {
		time.flushField(mk, rollTime, batch);
		flushFields(typename Descriptor<E>::field_indexes(), mk, rollTime, batch);
		if (!latestRecorded) {
			batch.scope.updates.emplace_back(mk.packLatestKey(), StringRef());
			latestRecorded = true;
		}
	}

	template <size_t... Is>
	void flushFields(index_sequence<Is...>, MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) {
#ifdef NO_INTELLISENSE
		auto _ = { (std::get<Is>(values).flushField(mk, rollTime, batch), Void())... };
		(void)_;
#endif
	}

	void rollMetric(uint64_t t) override {
		time.rollMetric(t);
		rollFields(typename Descriptor<E>::field_indexes(), t);
	}

	template <size_t... Is>
	void rollFields(index_sequence<Is...>, uint64_t t) {
#ifdef NO_INTELLISENSE
		auto _ = { (std::get<Is>(values).rollMetric(t), Void())... };
		(void)_;
#endif
	}

	void registerFields(MetricKeyRef const& mk, std::vector<Standalone<StringRef>>& fieldKeys) override {
		time.registerField(mk, fieldKeys);
		registerFields(typename Descriptor<E>::field_indexes(), mk, fieldKeys);
	}

	template <size_t... Is>
	void registerFields(index_sequence<Is...>, const MetricKeyRef& mk, std::vector<Standalone<StringRef>>& fieldKeys) {
#ifdef NO_INTELLISENSE
		auto _ = { (std::get<Is>(values).registerField(mk, fieldKeys), Void())... };
		(void)_;
#endif
	}

private:
	bool it;
};

// A field Descriptor compatible with EventField but with name set at runtime
struct DynamicDescriptor {
	DynamicDescriptor(const char* name) : _name(StringRef((uint8_t*)name, strlen(name))) {}
	StringRef name() const { return _name; }

private:
	const Standalone<StringRef> _name;
};

template <typename T>
struct DynamicField;

struct DynamicFieldBase {
	virtual ~DynamicFieldBase() {}

	virtual StringRef fieldName() const = 0;
	virtual StringRef getDerivedTypeName() const = 0;
	virtual void init() = 0;
	virtual void clear() = 0;
	virtual void log(uint64_t t, int64_t l, bool& overflow, int64_t& bytes) = 0;
	virtual void nextKey(uint64_t t, int level) = 0;
	virtual void nextKeyAllLevels(uint64_t t) = 0;
	virtual void rollMetric(uint64_t t) = 0;
	virtual void flushField(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) = 0;
	virtual void registerField(MetricKeyRef const& mk, std::vector<Standalone<StringRef>>& fieldKeys) = 0;

	// Set the current value of this field from the value of another
	virtual void setValueFrom(DynamicFieldBase* src, StringRef eventType) = 0;

	// Create a new field of the same type and with the same current value as this one and with the given name
	virtual std::unique_ptr<DynamicFieldBase> createNewWithValue(const char* name) = 0;

	// This does a fairly cheap and "safe" downcast without using dynamic_cast / RTTI by checking that the pointer value
	// of the const char * type string is the same as getDerivedTypeName for this object.
	template <typename T>
	DynamicField<T>* safe_downcast(StringRef eventType) {
		if (getDerivedTypeName() == metricTypeName<T>())
			return (DynamicField<T>*)this;

		TraceEvent(g_network->isSimulated() ? SevError : SevWarnAlways, "ScopeEventFieldTypeMismatch")
		    .detail("EventType", eventType.toString())
		    .detail("FieldName", fieldName().toString())
		    .detail("OldType", getDerivedTypeName().toString())
		    .detail("NewType", metricTypeName<T>().toString());
		return nullptr;
	}
};

template <typename T>
struct DynamicField final : public DynamicFieldBase, EventField<T, DynamicDescriptor> {
	typedef EventField<T, DynamicDescriptor> EventFieldType;
	DynamicField(const char* name) : DynamicFieldBase(), EventFieldType(DynamicDescriptor(name)), value(T()) {}

	StringRef fieldName() const override { return EventFieldType::name(); }

	// Get the field's datatype, this is used as a form of RTTI by DynamicFieldBase::safe_downcast()
	StringRef getDerivedTypeName() const override { return metricTypeName<T>(); }

	// Pure virtual implementations
	void clear() override { value = T(); }

	void log(uint64_t t, int64_t l, bool& overflow, int64_t& bytes) override {
		return EventFieldType::log(value, t, l, overflow, bytes);
	}

	// Redirects to EventFieldType methods
	void nextKey(uint64_t t, int level) override { return EventFieldType::nextKey(t, level); }
	void nextKeyAllLevels(uint64_t t) override { return EventFieldType::nextKeyAllLevels(t); }
	void rollMetric(uint64_t t) override { return EventFieldType::rollMetric(t); }
	void flushField(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) override {
		return EventFieldType::flushField(mk, rollTime, batch);
	}
	void registerField(MetricKeyRef const& mk, std::vector<Standalone<StringRef>>& fieldKeys) override {
		return EventFieldType::registerField(mk, fieldKeys);
	}
	void init() override { return EventFieldType::init(); }

	// Set this field's value to the value of another field of exactly the same type.
	void setValueFrom(DynamicFieldBase* src, StringRef eventType) override {
		DynamicField<T>* s = src->safe_downcast<T>(eventType);
		if (s != nullptr)
			set(s->value);
		else
			clear(); // Not really necessary with proper use but just in case it is better to clear than use an old
			         // value.
	}

	std::unique_ptr<DynamicFieldBase> createNewWithValue(const char* name) override {
		auto n = std::make_unique<DynamicField<T>>(name);
		n->set(value);
		return n;
	}

	// Non virtuals
	void set(T val) { value = val; }

private:
	T value;
};

// A DynamicEventMetric is an EventMetric whose field set can be modified at runtime.
struct DynamicEventMetric final : ReferenceCounted<DynamicEventMetric>,
                                  MetricUtil<DynamicEventMetric>,
                                  BaseEventMetric {
private:
	EventField<int64_t, TimeDescriptor> time;
	bool latestRecorded;

	// TODO:  A Standalone key type isn't ideal because on lookups a ref will be made Standalone just for the search
	// All fields that are set with setField will be in fields.
	std::map<Standalone<StringRef>, std::unique_ptr<DynamicFieldBase>> fields;

	// Set of fields not yet registered
	std::set<Standalone<StringRef>> fieldsToRegister;

	// Whether or not new fields have been added since the last logging.  fieldsToRegister can't
	// be used for this because registration is independent of actually logging data.
	bool newFields;

	void newFieldAdded(Standalone<StringRef> const& fname) {
		fieldsToRegister.insert(
		    fname); // So that this field will be registered when asked by the metrics logger actor later
		newFields = true; // So that log() will know that there is a new field

		// Registration has now changed so set registered to false and trigger a reg change event if possible
		registered = false;
		if (pCollection != nullptr)
			pCollection->metricRegistrationChanged.trigger();
	}

public:
	DynamicEventMetric(MetricNameRef const& name, Void = Void());
	~DynamicEventMetric() override = default;

	void addref() override { ReferenceCounted<DynamicEventMetric>::addref(); }
	void delref() override { ReferenceCounted<DynamicEventMetric>::delref(); }

	void onEnable() override {
		// Must initialize fields, previously knobs may not have been set.
		// Note that future fields will be okay because the field constructor will init and the knobs will be set.
		time.init();
		for (auto& [name, field] : fields)
			field->init();
	}

	// Set (or create) a new field in the event
	template <typename ValueType>
	void setField(const char* fieldName, const ValueType& value) {
		StringRef fname((uint8_t*)fieldName, strlen(fieldName));
		auto& p = fields[fname];
		// DynamicFieldBase *&p = fields[fname];
		if (p.get() != nullptr) {
			// FIXME:  This will break for DynamicEventMetric instances that are reused, such as use cases outside
			// of TraceEvents.  Currently there are none in the code, and there may never any be but if you're here
			// because you reused a DynamicEventMetric and got the error below then this issue must be fixed.  One
			// possible solution is to have a flag in DynamicEventMetric which enables this check so that
			// TraceEvent can preserve this behavior.
			TraceEvent(SevError, "DuplicateTraceProperty").detail("Property", fieldName).backtrace();
			if (g_network->isSimulated())
				ASSERT(false);
		}
		p = std::make_unique<DynamicField<ValueType>>(fieldName);
		if (pCollection != nullptr)
			p->init();
		newFieldAdded(fname);

		// This will return nullptr if the datatype is wrong.
		DynamicField<ValueType>* f = p->safe_downcast<ValueType>(getTypeName());
		// Only set the field value if the type is correct.
		// Another option here is to redefine the field to the new type and flush (roll) the existing field but that
		// would create many keys with small values in the db if two frequent events keep tug-of-war'ing the types back
		// and forth.
		if (f != nullptr)
			f->set(value);
		else
			p->clear(); // Not really necessary with proper use but just in case it is better to clear than use an old
			            // value.
	}

	// This provides a way to first set fields in a temporary DynamicEventMetric and then push all of those field values
	// into another DynamicEventMetric (which is actually logging somewhere) and log the event.
	uint64_t setFieldsAndLogFrom(DynamicEventMetric* source, uint64_t explicitTime = 0) {
		for (auto& f : source->fields) {
			std::unique_ptr<DynamicFieldBase>& p = fields[f.first];
			if (p.get() == nullptr) {
				p = f.second->createNewWithValue(f.first.toString().c_str());
				if (pCollection != nullptr)
					p->init();
				newFieldAdded(f.first);
			} else
				p->setValueFrom(f.second.get(), getTypeName());
		}
		return log(explicitTime);
	}

	StringRef getTypeName() const override { return metricName.name; }

	// Set all of the fields to their default values.
	void clearFields() {
		for (auto& [name, field] : fields)
			field->clear();
	}

	uint64_t log(uint64_t explicitTime = 0);

	// Virtual function implementations
	void flushData(MetricKeyRef const& mk, uint64_t rollTime, MetricBatch& batch) override;
	void rollMetric(uint64_t t) override;
	void registerFields(MetricKeyRef const& mk, std::vector<Standalone<StringRef>>& fieldKeys) override;
};

// Continuous metrics are a single-field metric using an EventField<TimeAndValue<T>>
template <typename T>
struct TimeAndValue {
	TimeAndValue() : time(0), value() {}
	int64_t time;
	T value;

	// The metric field type for TimeAndValue is just the Value type.
	static inline StringRef metric_field_type() { return metricTypeName<T>(); }
};

// FieldHeader for continuous metrics, works for T = int, double, bool
template <typename T>
struct FieldHeader<TimeAndValue<T>> {
	FieldHeader() : version(1), count(0), area(0), previous_time(0) {}
	uint8_t version;
	int64_t count;
	// If T is a floating point type then area is a double, otherwise it's an int64_t
	typename std::conditional<std::is_floating_point<T>::value, double, int64_t>::type area;
	int64_t previous_time;

	void update(FieldHeader const& h) {
		count += h.count;
		area += h.area;
	}
	void update(TimeAndValue<T> const& v) {
		++count;
		if (previous_time > 0)
			area += v.value * (v.time - previous_time);
		previous_time = v.time;
	}
	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, version);
		ASSERT(version == 1);
		serializer(ar, count, area);
	}
};

template <>
inline void FieldHeader<TimeAndValue<Standalone<StringRef>>>::update(TimeAndValue<Standalone<StringRef>> const& v) {
	++count;
	area += v.value.size();
}

// ValueBlock encoder/decoder for continuous metrics which have a type of TimeAndValue<T>
// Uses encodings for int64_t and T and encodes (time, value, [time, value]...)
template <typename T>
struct FieldValueBlockEncoding<TimeAndValue<T>> {
	FieldValueBlockEncoding() : time_encoding(), value_encoding() {}
	inline void write(BinaryWriter& w, TimeAndValue<T> const& v) {
		time_encoding.write(w, v.time);
		value_encoding.write(w, v.value);
	}
	TimeAndValue<T> read(BinaryReader& r) {
		TimeAndValue<T> result;
		result.time = time_encoding.read(r);
		result.value = value_encoding.read(r);
		return result;
	}
	FieldValueBlockEncoding<int64_t> time_encoding;
	FieldValueBlockEncoding<T> value_encoding;
};

// ValueBlock encoder/decoder specialization for continuous bool metrics because they are encoded
// more efficiently than encoding the time and bool types separately.
// Instead, time and value are combined to a single value (time delta << 1) + (value ? 1 : 0) and then
// that value is encoded as a delta.
template <>
struct FieldValueBlockEncoding<TimeAndValue<bool>> {
	FieldValueBlockEncoding() : prev(), prev_combined(0) {}
	inline void write(BinaryWriter& w, TimeAndValue<bool> const& v) {
		int64_t combined = (v.time << 1) | (v.value ? 1 : 0);
		w << CompressedInt<int64_t>(combined - prev_combined);
		prev = v;
		prev_combined = combined;
	}
	TimeAndValue<bool> read(BinaryReader& r) {
		CompressedInt<int64_t> d;
		r >> d;
		prev_combined += d.value;
		prev.value = prev_combined & 1;
		prev.time = prev_combined << 1;
		return prev;
	}
	TimeAndValue<bool> prev;
	int64_t prev_combined;
};

template <typename T>
struct ContinuousMetric final : NonCopyable,
                                ReferenceCounted<ContinuousMetric<T>>,
                                MetricUtil<ContinuousMetric<T>, T>,
                                BaseMetric {
	// Needed for MetricUtil
	alignas(8) static const StringRef metricType;

private:
	EventField<TimeAndValue<T>> field;
	TimeAndValue<T> tv;
	bool recorded;

public:
	ContinuousMetric(MetricNameRef const& name, T const& initial) : BaseMetric(name), recorded(false) {
		tv.value = initial;
	}

	void addref() override { ReferenceCounted<ContinuousMetric<T>>::addref(); }
	void delref() override { ReferenceCounted<ContinuousMetric<T>>::delref(); }

	T getValue() const { return tv.value; }

	void flushData(const MetricKeyRef& mk, uint64_t rollTime, MetricBatch& batch) override {
		if (!recorded) {
			batch.scope.updates.emplace_back(mk.packLatestKey(), getLatestAsValue());
			recorded = true;
		}

		field.flushField(mk, rollTime, batch);
	}

	void rollMetric(uint64_t t) override { field.rollMetric(t); }

	Standalone<StringRef> getLatestAsValue() const {
		FieldValueBlockEncoding<TimeAndValue<T>> enc;
		BinaryWriter wr(AssumeVersion(g_network->protocolVersion()));
		// Write a header so the client can treat this value like a normal data value block.
		// TODO: If it is useful, this could be the current header value of the most recently logged level.
		wr << FieldHeader<TimeAndValue<T>>();
		enc.write(wr, tv);
		return wr.toValue();
	}

	void onEnable() override {
		field.init();
		change();
	}

	void onDisable() override { change(); }

	void set(const T& v) {
		if (v != tv.value) {
			if (enabled)
				change();
			tv.value = v;
		}
	}

	// requires += on T
	void add(const T& delta) {
		if (delta != T()) {
			if (enabled)
				change();
			tv.value += delta;
		}
	}

	// requires ! on T
	void toggle() {
		if (enabled)
			change();
		tv.value = !tv.value;
	}

	void change() {
		uint64_t toggleTime = timer_int();
		int64_t bytes = 0;

		if (tv.time != 0) {
			double x = deterministicRandom()->random01();

			int64_t l = 0;
			if (x == 0.0)
				l = FLOW_KNOBS->MAX_METRIC_LEVEL - 1;
			else if (toggleTime != tv.time)
				l = std::min(FLOW_KNOBS->MAX_METRIC_LEVEL - 1,
				             (int64_t)(log((toggleTime - tv.time) / x) / FLOW_KNOBS->METRIC_LEVEL_DIVISOR));

			if (!canLog(l))
				return;

			bool overflow = false;
			field.log(tv, tv.time, l, overflow, bytes);
			if (overflow)
				field.nextKey(toggleTime, l);
		}
		tv.time = toggleTime;
		recorded = false;
		TDMetricCollection::getTDMetrics()->checkRoll(tv.time, bytes);
	}
};

typedef ContinuousMetric<int64_t> Int64Metric;
typedef ContinuousMetric<double> DoubleMetric;
typedef Int64Metric VersionMetric;
typedef ContinuousMetric<bool> BoolMetric;
typedef ContinuousMetric<Standalone<StringRef>> StringMetric;

// MetricHandle / EventMetricHandle are wrappers for a Reference<MetricType> which provides
// the following interface conveniences
//
//   * The underlying metric reference is always initialized to a valid object.  That valid object
//     may not actually be in a metric collection and therefore may not actually be able to write
//     data to a database, but it will work in other ways (i.e. int metrics will act like integers).
//
//   * Operator =, ++, --, +=, and -= can be used as though the handle is an object of the MetricType::ValueType of
//     the metric type for which it is a handle.
//
//   * Operator -> is defined such that the MetricHandle acts like a pointer to the underlying MetricType
//
//   * Cast operator to MetricType::ValueType is defined so that the handle will act like a MetricType::ValueType
//
//   * The last three features allow, for example, a MetricHandle<Int64Metric> to be a drop-in replacement for an
//   int64_t.
//
template <typename T>
struct MetricHandle {
	using ValueType = typename T::ValueType;

	MetricHandle(StringRef const& name = StringRef(),
	             StringRef const& id = StringRef(),
	             ValueType const& initial = ValueType())
	  : ref(T::getOrCreateInstance(name, id, true, initial)) {}

	// Initialize this handle to point to a new or existing metric with (name, id).  If a new metric is created then the
	// handle's current metric's current value will be the new metric's initial value.  This allows Metric handle users
	// to treat their Metric variables as normal variables and then bind them to actual logging metrics later while
	// continuing with the current value.
	void init(StringRef const& name, StringRef const& id = StringRef()) {
		ref = T::getOrCreateInstance(name, id, true, ref->getValue());
	}

	void init(StringRef const& name, StringRef const& id, typename T::ValueType const& initial) {
		ref = T::getOrCreateInstance(name, id, true, initial);
	}

	void operator=(typename T::ValueType const& v) { ref->set(v); }
	void operator++() { ref->add(1); }
	void operator++(int) { ref->add(1); }
	void operator--() { ref->add(-1); }
	void operator--(int) { ref->add(-1); }
	void operator+=(typename T::ValueType const& v) { ref->add(v); }
	void operator-=(typename T::ValueType const& v) { ref->add(-v); }

	T* operator->() { return ref.getPtr(); }

	operator typename T::ValueType() const { return ref->getValue(); }
	typename T::ValueType getValue() const { return ref->getValue(); }

	Reference<T> ref;
};

template <class T>
struct Traceable<MetricHandle<T>> : Traceable<typename T::ValueType> {
	static std::string toString(const MetricHandle<T>& value) {
		return Traceable<typename T::ValueType>::toString(value.getValue());
	}
};

template <class T>
struct SpecialTraceMetricType<MetricHandle<T>> : SpecialTraceMetricType<typename T::ValueType> {
	using parent = SpecialTraceMetricType<typename T::ValueType>;
	static auto getValue(const MetricHandle<T>& value) { return parent::getValue(value.getValue()); }
};

typedef MetricHandle<Int64Metric> Int64MetricHandle;
typedef MetricHandle<VersionMetric> VersionMetricHandle;
typedef MetricHandle<BoolMetric> BoolMetricHandle;
typedef MetricHandle<StringMetric> StringMetricHandle;
typedef MetricHandle<DoubleMetric> DoubleMetricHandle;

template <typename E>
using EventMetricHandle = MetricHandle<EventMetric<E>>;

enum StatsDMetric { GAUGE = 0, COUNTER };

class IMetric {
public:
	const UID id;
	const MetricsDataModel model;
	IMetric(MetricsDataModel m) : id{ deterministicRandom()->randomUniqueID() }, model{ m } {
		MetricCollection* metrics = MetricCollection::getMetricCollection();
		if (metrics != nullptr) {
			if (metrics->map.count(id) > 0) {
				TraceEvent(SevError, "MetricCollection_NameCollision").detail("NameConflict", id.toString().c_str());
				ASSERT(metrics->map.count(id) > 0);
			}
			metrics->map[id] = this;
		}
	}
	~IMetric() {
		MetricCollection* metrics = MetricCollection::getMetricCollection();
		if (metrics != nullptr) {
			metrics->map.erase(id);
		}
	}
};

std::string createStatsdMessage(const std::string& name,
                                StatsDMetric type,
                                const std::string& val,
                                const std::vector<std::pair<std::string, std::string>>& tags);

std::string createStatsdMessage(const std::string& name, StatsDMetric type, const std::string& val);

std::vector<std::string> splitString(const std::string& str, const std::string& delimit);

bool verifyStatsdMessage(const std::string& msg);

void createOtelGauge(UID id, const std::string& name, double value);

void createOtelGauge(UID id, const std::string& name, double value, const std::vector<OTEL::Attribute>&);

#include "flow/unactorcompiler.h"

#endif
