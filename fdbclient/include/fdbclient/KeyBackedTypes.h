/*
 * KeyBackedTypes.h
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

#pragma once

#include <utility>
#include <vector>

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/Subspace.h"
#include "flow/ObjectSerializer.h"
#include "flow/Platform.h"
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"
#include "flow/ThreadHelper.actor.h"

// TupleCodec is a utility struct to convert a type to and from a value using Tuple encoding.
// It is used by the template classes below like KeyBackedProperty and KeyBackedMap to convert
// key parts and values from various types to Value strings and back.
// New types can be supported either by writing a new specialization or adding these
// methods to the type so that the default specialization can be used:
//   static T T::unpack(Standalone<StringRef> const& val)
//   Standalone<StringRef> T::pack(T const& val) const
// Since TupleCodec is a struct, partial specialization can be used, such as the std::pair
// partial specialization below allowing any std::pair<T1,T2> where T1 and T2 are already
// supported by TupleCodec.
template <typename T>
struct TupleCodec {
	static inline Standalone<StringRef> pack(T const& val) { return val.pack().pack(); }
	static inline T unpack(Standalone<StringRef> const& val) { return T::unpack(Tuple::unpack(val)); }
};

// If T is Tuple then conversion is simple.
template <>
inline Standalone<StringRef> TupleCodec<Tuple>::pack(Tuple const& val) {
	return val.pack();
}
template <>
inline Tuple TupleCodec<Tuple>::unpack(Standalone<StringRef> const& val) {
	return Tuple::unpack(val);
}

template <>
inline Standalone<StringRef> TupleCodec<int64_t>::pack(int64_t const& val) {
	return Tuple::makeTuple(val).pack();
}
template <>
inline int64_t TupleCodec<int64_t>::unpack(Standalone<StringRef> const& val) {
	return Tuple::unpack(val).getInt(0);
}

template <>
inline Standalone<StringRef> TupleCodec<bool>::pack(bool const& val) {
	return Tuple::makeTuple(val ? 1 : 0).pack();
}
template <>
inline bool TupleCodec<bool>::unpack(Standalone<StringRef> const& val) {
	return Tuple::unpack(val).getInt(0) == 1;
}

template <>
inline Standalone<StringRef> TupleCodec<Standalone<StringRef>>::pack(Standalone<StringRef> const& val) {
	return Tuple::makeTuple(val).pack();
}
template <>
inline Standalone<StringRef> TupleCodec<Standalone<StringRef>>::unpack(Standalone<StringRef> const& val) {
	return Tuple::unpack(val).getString(0);
}

template <>
inline Standalone<StringRef> TupleCodec<UID>::pack(UID const& val) {
	return TupleCodec<Standalone<StringRef>>::pack(BinaryWriter::toValue<UID>(val, Unversioned()));
}
template <>
inline UID TupleCodec<UID>::unpack(Standalone<StringRef> const& val) {
	return BinaryReader::fromStringRef<UID>(TupleCodec<Standalone<StringRef>>::unpack(val), Unversioned());
}

// This is backward compatible with TupleCodec<Standalone<StringRef>>
template <>
inline Standalone<StringRef> TupleCodec<std::string>::pack(std::string const& val) {
	return Tuple::makeTuple(val).pack();
}
template <>
inline std::string TupleCodec<std::string>::unpack(Standalone<StringRef> const& val) {
	return Tuple::unpack(val).getString(0).toString();
}

// Partial specialization to cover all std::pairs as long as the component types are TupleCodec compatible
template <typename First, typename Second>
struct TupleCodec<std::pair<First, Second>> {
	static Standalone<StringRef> pack(typename std::pair<First, Second> const& val) {
		// Packing a concatenated tuple is the same as concatenating two packed tuples
		return TupleCodec<First>::pack(val.first).withSuffix(TupleCodec<Second>::pack(val.second));
	}
	static std::pair<First, Second> unpack(Standalone<StringRef> const& val) {
		Tuple t = Tuple::unpack(val);
		ASSERT(t.size() == 2);
		return { TupleCodec<First>::unpack(t.subTupleRawString(0)),
			     TupleCodec<Second>::unpack(t.subTupleRawString(1)) };
	}
};

template <typename T>
struct TupleCodec<std::vector<T>> {
	static Standalone<StringRef> pack(typename std::vector<T> const& val) {
		Tuple t;
		for (T item : val) {
			// fdbclient doesn't support nested tuples yet. For now, flatten the tuple into StringRef
			t.append(TupleCodec<T>::pack(item));
		}
		return t.pack();
	}

	static std::vector<T> unpack(Standalone<StringRef> const& val) {
		Tuple t = Tuple::unpack(val);
		std::vector<T> v;

		for (int i = 0; i < t.size(); i++) {
			v.push_back(TupleCodec<T>::unpack(t.getString(i)));
		}

		return v;
	}
};

template <>
inline Standalone<StringRef> TupleCodec<KeyRange>::pack(KeyRange const& val) {
	return Tuple::makeTuple(val.begin, val.end).pack();
}
template <>
inline KeyRange TupleCodec<KeyRange>::unpack(Standalone<StringRef> const& val) {
	Tuple t = Tuple::unpack(val);
	return KeyRangeRef(t.getString(0), t.getString(1));
}

struct NullCodec {
	static Standalone<StringRef> pack(Standalone<StringRef> val) { return val; }
	static Standalone<StringRef> unpack(Standalone<StringRef> val) { return val; }
};

template <class T>
struct BinaryCodec {
	static Standalone<StringRef> pack(T val) { return BinaryWriter::toValue<T>(val, Unversioned()); }
	static T unpack(Standalone<StringRef> val) { return BinaryReader::fromStringRef<T>(val, Unversioned()); }
};

// Codec for using Flatbuffer compatible types via ObjectWriter/ObjectReader
template <typename T, typename VersionOptions>
struct ObjectCodec {
	ObjectCodec(VersionOptions vo) : vo(vo) {}
	VersionOptions vo;

	inline Standalone<StringRef> pack(T const& val) const { return ObjectWriter::toValue<T>(val, vo); }
	inline T unpack(Standalone<StringRef> const& val) const { return ObjectReader::fromStringRef<T>(val, vo); }
};

template <typename ResultType>
struct KeyBackedRangeResult {
	std::vector<ResultType> results;
	bool more;

	bool operator==(KeyBackedRangeResult const& other) const { return results == other.results && more == other.more; }
	bool operator!=(KeyBackedRangeResult const& other) const { return !(*this == other); }
};

class WatchableTrigger {
private:
	Key key;

public:
	WatchableTrigger(Key k) : key(k) {}

	template <class Transaction>
	void update(Transaction tr, AddConflictRange conflict = AddConflictRange::False) {
		std::array<uint8_t, 14> value;
		value.fill(0);
		tr->atomicOp(key, StringRef(value.begin(), value.size()), MutationRef::SetVersionstampedValue);
		if (conflict) {
			tr->addReadConflictRange(singleKeyRange(key));
		}
	}

	template <class Transaction>
	Future<Versionstamp> get(Transaction tr, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

		return holdWhile(getFuture,
		                 map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> Versionstamp {
			                 Versionstamp v;
			                 if (val.present()) {
				                 return v = BinaryReader::fromStringRef<Versionstamp>(val.get(), Unversioned());
			                 }
			                 return v;
		                 }));
	}

	template <class Transaction>
	Future<Void> onChange(Transaction tr) const {
		return safeThreadFutureToFuture(tr->watch(key));
	}
};

// Convenient read/write access to a single value of type T stored at key
// Even though 'this' is not actually mutated, methods that change the db key are not const.
template <typename T, typename Codec = TupleCodec<T>>
class KeyBackedProperty {
public:
	KeyBackedProperty(KeyRef key, Optional<WatchableTrigger> trigger = {}, Codec codec = {})
	  : key(key), trigger(trigger), codec(codec) {}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, Future<Optional<T>>>::type get(
	    Transaction tr,
	    Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

		return holdWhile(
		    getFuture,
		    map(safeThreadFutureToFuture(getFuture), [codec = codec](Optional<Value> const& val) -> Optional<T> {
			    if (val.present())
				    return codec.unpack(val.get());
			    return {};
		    }));
	}

	// Get property's value or defaultValue if it doesn't exist
	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, Future<T>>::type
	getD(Transaction tr, Snapshot snapshot = Snapshot::False, T defaultValue = T()) const {
		return map(get(tr, snapshot), [=](Optional<T> val) -> T { return val.present() ? val.get() : defaultValue; });
	}

	// Get property's value or throw error if it doesn't exist
	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, Future<T>>::type
	getOrThrow(Transaction tr, Snapshot snapshot = Snapshot::False, Error err = key_not_found()) const {
		return map(get(tr, snapshot), [=](Optional<T> val) -> T {
			if (!val.present()) {
				throw err;
			}

			return val.get();
		});
	}

	template <class DB>
	typename std::enable_if<is_transaction_creator<DB>, Future<Optional<T>>>::type get(
	    Reference<DB> db,
	    Snapshot snapshot = Snapshot::False) const {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.get(tr, snapshot);
		});
	}

	template <class DB>
	typename std::enable_if<is_transaction_creator<DB>, Future<T>>::type getD(Reference<DB> db,
	                                                                          Snapshot snapshot = Snapshot::False,
	                                                                          T defaultValue = T()) const {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getD(tr, snapshot, defaultValue);
		});
	}

	template <class DB>
	typename std::enable_if<is_transaction_creator<DB>, Future<T>>::type getOrThrow(Reference<DB> db,
	                                                                                Snapshot snapshot = Snapshot::False,
	                                                                                Error err = key_not_found()) const {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getOrThrow(tr, snapshot, err);
		});
	}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, void>::type set(Transaction tr, T const& val) {
		tr->set(key, packValue(val));
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class DB>
	typename std::enable_if<is_transaction_creator<DB>, Future<Void>>::type set(Reference<DB> db, T const& val) {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			self->set(tr, val);
			return Future<Void>(Void());
		});
	}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, void>::type clear(Transaction tr) {
		tr->clear(key);
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	Future<Void> watch(Transaction tr) {
		return tr->watch(key);
	}

	Value packValue(T const& value) const { return codec.pack(value); }
	T unpackValue(ValueRef const& value) const { return codec.unpack(value); }

	Key key;
	Optional<WatchableTrigger> trigger;
	Codec codec;
};

// KeyBackedObjectProperty is a convenience wrapper of KeyBackedProperty which uses ObjectCodec<T, VersionOptions> as
// the codec
template <typename T, typename VersionOptions>
class KeyBackedObjectProperty : public KeyBackedProperty<T, ObjectCodec<T, VersionOptions>> {
	typedef ObjectCodec<T, VersionOptions> TCodec;
	typedef KeyBackedProperty<T, TCodec> Base;

public:
	KeyBackedObjectProperty(KeyRef key, VersionOptions vo, Optional<WatchableTrigger> trigger = {})
	  : Base(key, trigger, TCodec(vo)) {}
};

// KeyBackedBinaryValue is a convenience wrapper of KeyBackedProperty but using BinaryCodec<T> as the codec and adds
// atomic ops and version stamp operations.
template <typename T>
class KeyBackedBinaryValue : public KeyBackedProperty<T, BinaryCodec<T>> {
	typedef KeyBackedProperty<T, BinaryCodec<T>> Base;

public:
	KeyBackedBinaryValue(KeyRef key, Optional<WatchableTrigger> trigger = {}) : Base(key, trigger) {}

	template <class Transaction>
	void atomicOp(Transaction tr, T const& val, MutationRef::Type type) {
		tr->atomicOp(this->key, BinaryWriter::toValue<T>(val, Unversioned()), type);
		if (this->trigger.present()) {
			this->trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void setVersionstamp(Transaction tr, T const& val, int offset) {
		tr->atomicOp(
		    this->key,
		    BinaryWriter::toValue<T>(val, Unversioned()).withSuffix(StringRef(reinterpret_cast<uint8_t*>(&offset), 4)),
		    MutationRef::SetVersionstampedValue);
		if (this->trigger.present()) {
			this->trigger.get().update(tr);
		}
	}
};

template <typename KeyType, typename ValueType>
struct TypedKVPair {
	KeyType key;
	ValueType value;
};

// Convenient read/write access to a sorted map of KeyType to ValueType under prefix
// Even though 'this' is not actually mutated, methods that change db keys are not const.
template <typename _KeyType,
          typename _ValueType,
          typename KeyCodec = TupleCodec<_KeyType>,
          typename ValueCodec = TupleCodec<_ValueType>>
class KeyBackedMap {
public:
	KeyBackedMap(KeyRef prefix, Optional<WatchableTrigger> trigger = {}, ValueCodec valueCodec = {})
	  : subspace(prefixRange(prefix)), trigger(trigger), valueCodec(valueCodec) {}

	typedef _KeyType KeyType;
	typedef _ValueType ValueType;
	typedef std::pair<KeyType, ValueType> PairType;
	typedef TypedKVPair<KeyType, ValueType> KVType;
	typedef KeyBackedRangeResult<PairType> RangeResultType;
	typedef KeyBackedProperty<ValueType, ValueCodec> SingleRecordProperty;

	// If end is not present one key past the end of the map is used.
	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 Optional<KeyType> const& begin,
	                                 Optional<KeyType> const& end,
	                                 int limit,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {
		Key prefix = subspace.begin; // 'this' could be invalid inside lambda

		Key beginKey = begin.present() ? prefix.withSuffix(KeyCodec::pack(begin.get())) : subspace.begin;
		Key endKey = end.present() ? prefix.withSuffix(KeyCodec::pack(end.get())) : subspace.end;

		typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(KeyRangeRef(beginKey, endKey), GetRangeLimits(limit), snapshot, reverse);

		return holdWhile(getRangeFuture,
		                 map(safeThreadFutureToFuture(getRangeFuture),
		                     [prefix, valueCodec = valueCodec](RangeResult const& kvs) -> RangeResultType {
			                     RangeResultType rangeResult;
			                     for (int i = 0; i < kvs.size(); ++i) {
				                     KeyType key = KeyCodec::unpack(kvs[i].key.removePrefix(prefix));
				                     ValueType val = valueCodec.unpack(kvs[i].value);
				                     rangeResult.results.push_back(PairType(key, val));
			                     }
			                     rangeResult.more = kvs.more;
			                     return rangeResult;
		                     }));
	}

	template <class Transaction>
	Future<Optional<ValueType>> get(Transaction tr, KeyType const& key, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture =
		    tr->get(packKey(key), snapshot);

		return holdWhile(getFuture,
		                 map(safeThreadFutureToFuture(getFuture),
		                     [valueCodec = valueCodec](Optional<Value> const& val) -> Optional<ValueType> {
			                     if (val.present())
				                     return valueCodec.unpack(val.get());
			                     return {};
		                     }));
	}

	// Get key's value or defaultValue if it doesn't exist
	template <class Transaction>
	Future<ValueType> getD(Transaction tr,
	                       KeyType const& key,
	                       Snapshot snapshot = Snapshot::False,
	                       ValueType defaultValue = ValueType()) const {
		return map(get(tr, key, snapshot),
		           [=](Optional<ValueType> val) -> ValueType { return val.orDefault(defaultValue); });
	}

	// Returns a Property that can be get/set that represents key's entry in this this.
	SingleRecordProperty getProperty(KeyType const& key) const { return { packKey(key), trigger, valueCodec }; }

	// Returns the expectedSize of the set key
	template <class Transaction>
	int set(Transaction tr, KeyType const& key, ValueType const& val) {
		Key k = packKey(key);
		Value v = packValue(val);
		tr->set(k, v);
		if (trigger.present()) {
			trigger.get().update(tr);
		}
		return k.expectedSize() + v.expectedSize();
	}

	template <class Transaction>
	void atomicOp(Transaction tr, KeyType const& key, ValueType const& val, MutationRef::Type type) {
		Key k = packKey(key);
		Value v = packValue(val);
		tr->atomicOp(k, v, type);
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& key) {
		tr->clear(packKey(key));
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& begin, KeyType const& end) {
		tr->clear(KeyRangeRef(packKey(begin), packKey(end)));
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void addReadConflictKey(Transaction tr, KeyType const& key) {
		Key k = packKey(key);
		tr->addReadConflictRange(singleKeyRange(k));
	}

	template <class Transaction>
	void addReadConflictRange(Transaction tr, KeyType const& begin, KeyType const& end) {
		tr->addReadConflictRange(packKey(begin), packKey(end));
	}

	template <class Transaction>
	void addWriteConflictKey(Transaction tr, KeyType const& key) {
		Key k = packKey(key);
		tr->addWriteConflictRange(singleKeyRange(k));
	}

	template <class Transaction>
	void addWriteConflictRange(Transaction tr, KeyType const& begin, KeyType const& end) {
		tr->addWriteConflictRange(packKey(begin), packKey(end));
	}

	KeyRange subspace;
	Optional<WatchableTrigger> trigger;
	ValueCodec valueCodec;

	Key packKey(KeyType const& key) const { return subspace.begin.withSuffix(KeyCodec::pack(key)); }
	KeyType unpackKey(KeyRef const& key) const { return KeyCodec::unpack(key.removePrefix(subspace.begin)); }

	Value packValue(ValueType const& value) const { return valueCodec.pack(value); }
	ValueType unpackValue(ValueRef const& value) const { return valueCodec.unpack(value); }

	KVType unpackKV(KeyValueRef const& kv) const { return { unpackKey(kv.key), unpackValue(kv.value) }; }
};

// KeyBackedObjectMap is a convenience wrapper of KeyBackedMap which uses ObjectCodec<_ValueType, VersionOptions> as
// the value codec
template <typename _KeyType, typename _ValueType, typename VersionOptions, typename KeyCodec = TupleCodec<_KeyType>>
class KeyBackedObjectMap
  : public KeyBackedMap<_KeyType, _ValueType, KeyCodec, ObjectCodec<_ValueType, VersionOptions>> {
	typedef KeyBackedMap<_KeyType, _ValueType, KeyCodec, ObjectCodec<_ValueType, VersionOptions>> Base;
	typedef ObjectCodec<_ValueType, VersionOptions> ObjectCodec;

public:
	KeyBackedObjectMap(KeyRef key, VersionOptions vo, Optional<WatchableTrigger> trigger = {})
	  : Base(key, trigger, ObjectCodec(vo)) {}
};

template <typename _ValueType, typename Codec = TupleCodec<_ValueType>>
class KeyBackedSet {
public:
	KeyBackedSet(KeyRef key, Optional<WatchableTrigger> trigger = {}) : subspace(prefixRange(key)), trigger(trigger) {}

	typedef _ValueType ValueType;
	typedef KeyBackedRangeResult<ValueType> RangeResultType;

	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 Optional<ValueType> const& begin,
	                                 Optional<ValueType> const& end,
	                                 int limit,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {
		Key prefix = subspace.begin; // 'this' could be invalid inside lambda
		Key beginKey = begin.present() ? prefix.withSuffix(Codec::pack(begin.get())) : subspace.begin;
		Key endKey = end.present() ? prefix.withSuffix(Codec::pack(end.get())) : subspace.end;

		typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(KeyRangeRef(beginKey, endKey), GetRangeLimits(limit), snapshot, reverse);

		return holdWhile(
		    getRangeFuture,
		    map(safeThreadFutureToFuture(getRangeFuture), [prefix](RangeResult const& kvs) -> RangeResultType {
			    RangeResultType rangeResult;
			    for (int i = 0; i < kvs.size(); ++i) {
				    rangeResult.results.push_back(Codec::unpack(kvs[i].key.removePrefix(prefix)));
			    }
			    rangeResult.more = kvs.more;
			    return rangeResult;
		    }));
	}

	template <class Transaction>
	Future<bool> exists(Transaction tr, ValueType const& val, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture =
		    tr->get(packKey(val), snapshot);

		return holdWhile(getFuture, map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> bool {
			                 return val.present();
		                 }));
	}

	// Returns the expectedSize of the set key
	template <class Transaction>
	int insert(Transaction tr, ValueType const& val) {
		Key k = packKey(val);
		tr->set(k, StringRef());
		if (trigger.present()) {
			trigger.get().update(tr);
		}
		return k.expectedSize();
	}

	template <class Transaction>
	void erase(Transaction tr, ValueType const& val) {
		tr->clear(packKey(val));
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void erase(Transaction tr, ValueType const& begin, ValueType const& end) {
		tr->clear(KeyRangeRef(packKey(begin), packKey(end)));
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
		if (trigger.present()) {
			trigger.get().update(tr);
		}
	}

	KeyRange subspace;
	Optional<WatchableTrigger> trigger;

	Key packKey(ValueType const& value) const { return subspace.begin.withSuffix(Codec::pack(value)); }
};

// KeyBackedKeyRangeMap is similar to KeyRangeMap but without a Metric
// It is assumed that any range not covered by the map is set to a default ValueType()
// The ValueType must have
//     // Return a copy of *this updated with properties in value
//     ValueType update(ValueType const& value) const;
//
//     // Return true if adjacent ranges with values *this and value could be merged to a single
//     // range represented by *this and retain identical meaning
//     bool canMerge(ValueType const& value) const;
template <typename KeyType,
          typename ValueType,
          typename KeyCodec = TupleCodec<KeyType>,
          typename ValueCodec = TupleCodec<ValueType>>
class KeyBackedRangeMap {
public:
	typedef KeyBackedMap<KeyType, ValueType, KeyCodec, ValueCodec> Map;
	typedef KeyBackedRangeResult<typename Map::KVType> RangeMapResult;

	KeyBackedRangeMap(KeyRef prefix, Optional<WatchableTrigger> trigger = {}, ValueCodec valueCodec = {})
	  : kvMap(prefix, trigger, valueCodec) {}

	// Get ranges to fully describe the given range.
	// Result will be that returned by a range read from lastLessOrEqual(begin) to lastLessOrEqual(end)
	template <class Transaction>
	Future<RangeMapResult> getRanges(Transaction* tr, KeyType const& begin, KeyType const& end) const {
		KeySelector rangeBegin = lastLessOrEqual(kvMap.packKey(begin));
		KeySelector rangeEnd = lastLessOrEqual(kvMap.packKey(end));

		typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(rangeBegin, rangeEnd, std::numeric_limits<int>::max());

		return holdWhile(
		    getRangeFuture,
		    map(safeThreadFutureToFuture(getRangeFuture), [=, kvMap = kvMap](RangeResult const& rawkvs) mutable {
				RangeMapResult result;
				result.results.reserve(rawkvs.size());
			    for (auto const& kv : rawkvs) {
					result.results.emplace_back(kvMap.unpackKV(kv));
			    }
				return result;
			}));
	}

	// Get the Value for the range that key is in
	template <class Transaction>
	Future<ValueType> getRangeForKey(Transaction* tr, KeyType const& key) {
		KeySelector rangeBegin = lastLessOrEqual(kvMap.packKey(key));
		KeySelector rangeEnd = firstGreaterOrEqual(kvMap.packKey(key));

		typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(rangeBegin, rangeEnd, std::numeric_limits<int>::max());

		return holdWhile(
		    getRangeFuture,
		    map(safeThreadFutureToFuture(getRangeFuture), [=, kvMap = kvMap](RangeResult const& rawkvs) mutable {
				ASSERT(rawkvs.size() <= 1);
				if(rawkvs.empty()) {
					return ValueType();
				}
				return kvMap.unpackValue(rawkvs.back().value);
			}));
	}

	// Update the range from begin to end by applying valueUpdate to it
	// Since the transaction type may not be RYW, this method must take care to not rely on reading its own updates.
	template <class Transaction>
	Future<Void> updateRange(Transaction* tr, KeyType const& begin, KeyType const& end, ValueType const& valueUpdate) {
		return map(getRanges(tr, begin, end), [=, kvMap = kvMap](RangeMapResult const& range) mutable {
				// TODO:  Handle partial ranges.
				ASSERT(!range.more);

				// A deque here makes the following logic easier, though it's not very efficient.
				std::deque<typename Map::KVType> kvs(range.results.begin(), range.results.end());

			    // First, handle modification at the begin boundary.
			    // If there are no records, or no records <= begin, then set begin to valueUpdate
			    if (kvs.empty() || kvs.front().key > begin) {
				    kvMap.set(tr, begin, valueUpdate);
			    } else {
				    // Otherwise, the first record represents the range that begin is currently in.
				    // This record may be begin or before it, either way the action is to set begin
				    // to the first record's value updated with valueUpdate
				    kvMap.set(tr, begin, kvs.front().value.update(valueUpdate));
				    // The first record has consumed
				    kvs.pop_front();
			    }

			    // Next, handle modification at the end boundary
			    // If there are no records, then set end to the default value to clear the effects of begin as of the
			    // end boundary.
			    if (kvs.empty()) {
				    kvMap.set(tr, end, ValueType());
			    } else if (kvs.back().key < end) {
				    // If the last key is less than end, then end is not yet a boundary in the range map so we must
				    // create it.  It will be set to the value of the range it resides in, which begins with the last
				    // item in kvs. Note that it is very important to do this *before* modifying all the middle
				    // boundaries of the range so that the changes we are making here terminate at end.
				    kvMap.set(tr, end, kvs.back().value);
			    } else {
				    // Otherwise end is in kvs, which effectively ends the range we are modifying so just pop it from
				    // kvs so that it is not modified in the loop below.
				    ASSERT(kvs.back().key == end);
				    kvs.pop_back();
			    }

			    // Last, update all of the range boundaries in the middle which are left in the kvs queue
			    for (auto const& kv : kvs) {
				    kvMap.set(tr, kv.key, kv.value.update(valueUpdate));
			    }

			    return Void();
		    });
	}

private:
	Map kvMap;
};

// KeyBackedClass is a convenient base class for a set of related KeyBacked types that exist
// under a single key prefix and other help functions relevant to the concepts that the class
// represent.
//
// A WatchableTrigger called trigger is provided, which a default key which is under the struct's
// root space.  Alternatively, a custom WatchableTrigger can be provided to the constructor
// to use any other database key instead.

class KeyBackedClass {
public:
	KeyBackedClass(StringRef prefix, Optional<Key> triggerOverride = {})
	  : subSpace(prefix), trigger(triggerOverride.orDefault(subSpace.pack("_changeTrigger"_sr))) {}

protected:
	Subspace subSpace;
	WatchableTrigger trigger;
};
