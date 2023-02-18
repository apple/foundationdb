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

template <typename ResultType>
struct KeyBackedRangeResult {
	std::vector<ResultType> results;
	bool more;

	bool operator==(KeyBackedRangeResult const& other) const { return results == other.results && more == other.more; }
	bool operator!=(KeyBackedRangeResult const& other) const { return !(*this == other); }
};

// Convenient read/write access to a single value of type T stored at key
// Even though 'this' is not actually mutated, methods that change the db key are not const.
template <typename T, typename Codec = TupleCodec<T>>
class KeyBackedProperty {
public:
	KeyBackedProperty(KeyRef key) : key(key) {}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, Future<Optional<T>>>::type get(
	    Transaction tr,
	    Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

		return holdWhile(getFuture,
		                 map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> Optional<T> {
			                 if (val.present())
				                 return Codec::unpack(val.get());
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
		tr->set(key, Codec::pack(val));
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
	}

	Key key;
};

// This is just like KeyBackedProperty but instead of using a Codec for conversion to/from values it
// uses BinaryReader and BinaryWriter.  This enables allows atomic ops with integer types, and also
// allows reading and writing of existing keys which use BinaryReader/Writer.
template <typename T>
class KeyBackedBinaryValue {
public:
	KeyBackedBinaryValue(KeyRef key) : key(key) {}

	template <class Transaction>
	Future<Optional<T>> get(Transaction tr, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

		return holdWhile(getFuture,
		                 map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> Optional<T> {
			                 if (val.present())
				                 return BinaryReader::fromStringRef<T>(val.get(), Unversioned());
			                 return {};
		                 }));
	}
	// Get property's value or defaultValue if it doesn't exist
	template <class Transaction>
	Future<T> getD(Transaction tr, Snapshot snapshot = Snapshot::False, T defaultValue = T()) const {
		return map(get(tr, Snapshot::False),
		           [=](Optional<T> val) -> T { return val.present() ? val.get() : defaultValue; });
	}

	template <class Transaction>
	void set(Transaction tr, T const& val) {
		tr->set(key, BinaryWriter::toValue<T>(val, Unversioned()));
	}

	template <class Transaction>
	void atomicOp(Transaction tr, T const& val, MutationRef::Type type) {
		tr->atomicOp(key, BinaryWriter::toValue<T>(val, Unversioned()), type);
	}

	template <class Transaction>
	void setVersionstamp(Transaction tr, T const& val, int offset) {
		tr->atomicOp(
		    key,
		    BinaryWriter::toValue<T>(val, Unversioned()).withSuffix(StringRef(reinterpret_cast<uint8_t*>(&offset), 4)),
		    MutationRef::SetVersionstampedValue);
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(key);
	}

	template <class Transaction>
	Future<Void> watch(Transaction tr) {
		return tr->watch(key);
	}

	Key key;
};

// Convenient read/write access to a sorted map of KeyType to ValueType under prefix
// Even though 'this' is not actually mutated, methods that change db keys are not const.
template <typename _KeyType,
          typename _ValueType,
          typename KeyCodec = TupleCodec<_KeyType>,
          typename ValueCodec = TupleCodec<_ValueType>>
class KeyBackedMap {
public:
	KeyBackedMap(KeyRef prefix) : subspace(prefixRange(prefix)) {}

	typedef _KeyType KeyType;
	typedef _ValueType ValueType;
	typedef std::pair<KeyType, ValueType> PairType;
	typedef KeyBackedRangeResult<PairType> RangeResultType;

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

		return holdWhile(
		    getRangeFuture,
		    map(safeThreadFutureToFuture(getRangeFuture), [prefix](RangeResult const& kvs) -> RangeResultType {
			    RangeResultType rangeResult;
			    for (int i = 0; i < kvs.size(); ++i) {
				    KeyType key = KeyCodec::unpack(kvs[i].key.removePrefix(prefix));
				    ValueType val = ValueCodec::unpack(kvs[i].value);
				    rangeResult.results.push_back(PairType(key, val));
			    }
			    rangeResult.more = kvs.more;
			    return rangeResult;
		    }));
	}

	template <class Transaction>
	Future<Optional<ValueType>> get(Transaction tr, KeyType const& key, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture =
		    tr->get(subspace.begin.withSuffix(KeyCodec::pack(key)), snapshot);

		return holdWhile(
		    getFuture, map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> Optional<ValueType> {
			    if (val.present())
				    return ValueCodec::unpack(val.get());
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
	KeyBackedProperty<ValueType> getProperty(KeyType const& key) const {
		return subspace.begin.withSuffix(KeyCodec::pack(key));
	}

	// Returns the expectedSize of the set key
	template <class Transaction>
	int set(Transaction tr, KeyType const& key, ValueType const& val) {
		Key k = subspace.begin.withSuffix(KeyCodec::pack(key));
		Value v = ValueCodec::pack(val);
		tr->set(k, v);
		return k.expectedSize() + v.expectedSize();
	}

	template <class Transaction>
	void atomicOp(Transaction tr, KeyType const& key, ValueType const& val, MutationRef::Type type) {
		Key k = subspace.begin.withSuffix(KeyCodec::pack(key));
		Value v = ValueCodec::pack(val);
		tr->atomicOp(k, v, type);
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& key) {
		tr->clear(subspace.begin.withSuffix(KeyCodec::pack(key)));
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& begin, KeyType const& end) {
		tr->clear(KeyRangeRef(subspace.begin.withSuffix(KeyCodec::pack(begin)),
		                      subspace.begin.withSuffix(KeyCodec::pack(end))));
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
	}

	KeyRange subspace;
};

// Convenient read/write access to a single value of type T stored at key
// Even though 'this' is not actually mutated, methods that change the db key are not const.
template <typename T, typename VersionOptions>
class KeyBackedObjectProperty {
public:
	KeyBackedObjectProperty(KeyRef key, VersionOptions versionOptions) : key(key), versionOptions(versionOptions) {}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, Future<Optional<T>>>::type get(
	    Transaction tr,
	    Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

		return holdWhile(
		    getFuture,
		    map(safeThreadFutureToFuture(getFuture), [vo = versionOptions](Optional<Value> const& val) -> Optional<T> {
			    if (val.present())
				    return ObjectReader::fromStringRef<T>(val.get(), vo);
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
		tr->set(key, ObjectWriter::toValue(val, versionOptions));
	}

	template <class DB>
	typename std::enable_if<is_transaction_creator<DB>, Future<Void>>::type set(Reference<DB> db, T const& val) {
		return runTransaction(db, [=, self = *this](Reference<typename DB::TransactionT> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			self.set(tr, val);
			return Future<Void>(Void());
		});
	}

	template <class Transaction>
	typename std::enable_if<!is_transaction_creator<Transaction>, void>::type clear(Transaction tr) {
		tr->clear(key);
	}

	Key key;
	VersionOptions versionOptions;
};

// Convenient read/write access to a sorted map of KeyType to ValueType under key prefix
// ValueType is encoded / decoded with ObjectWriter/ObjectReader
// Even though 'this' is not actually mutated, methods that change db keys are not const.
template <typename _KeyType, typename _ValueType, typename VersionOptions, typename KeyCodec = TupleCodec<_KeyType>>
class KeyBackedObjectMap {
public:
	KeyBackedObjectMap(KeyRef prefix, VersionOptions versionOptions)
	  : subspace(prefixRange(prefix)), versionOptions(versionOptions) {}

	typedef _KeyType KeyType;
	typedef _ValueType ValueType;
	typedef std::pair<KeyType, ValueType> PairType;
	typedef KeyBackedRangeResult<PairType> RangeResultType;

	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 Optional<KeyType> const& begin,
	                                 Optional<KeyType> const& end,
	                                 int limit,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {
		Key beginKey = begin.present() ? subspace.begin.withSuffix(KeyCodec::pack(begin.get())) : subspace.begin;
		Key endKey = end.present() ? subspace.begin.withSuffix(KeyCodec::pack(end.get())) : subspace.end;

		typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(KeyRangeRef(beginKey, endKey), GetRangeLimits(limit), snapshot, reverse);

		return holdWhile(
		    getRangeFuture,
		    map(safeThreadFutureToFuture(getRangeFuture), [self = *this](RangeResult const& kvs) -> RangeResultType {
			    RangeResultType rangeResult;
			    for (int i = 0; i < kvs.size(); ++i) {
				    KeyType key = KeyCodec::unpack(kvs[i].key.removePrefix(self.subspace.begin));
				    ValueType val = ObjectReader::fromStringRef<ValueType>(kvs[i].value, self.versionOptions);
				    rangeResult.results.push_back(PairType(key, val));
			    }
			    rangeResult.more = kvs.more;
			    return rangeResult;
		    }));
	}

	template <class Transaction>
	Future<Optional<ValueType>> get(Transaction tr, KeyType const& key, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture =
		    tr->get(subspace.begin.withSuffix(KeyCodec::pack(key)), snapshot);

		return holdWhile(getFuture,
		                 map(safeThreadFutureToFuture(getFuture),
		                     [vo = versionOptions](Optional<Value> const& val) -> Optional<ValueType> {
			                     if (val.present())
				                     return ObjectReader::fromStringRef<ValueType>(val.get(), vo);
			                     return {};
		                     }));
	}

	// Returns a Property that can be get/set that represents key's entry in this this.
	KeyBackedObjectProperty<ValueType, VersionOptions> getProperty(KeyType const& key) const {
		return KeyBackedObjectProperty<ValueType, VersionOptions>(subspace.begin.withSuffix(KeyCodec::pack(key)),
		                                                          versionOptions);
	}

	// Returns the expectedSize of the set key
	template <class Transaction>
	int set(Transaction tr, KeyType const& key, ValueType const& val) {
		Key k = subspace.begin.withSuffix(KeyCodec::pack(key));
		Value v = ObjectWriter::toValue(val, versionOptions);
		tr->set(k, v);
		return k.expectedSize() + v.expectedSize();
	}

	Key serializeKey(KeyType const& key) { return subspace.begin.withSuffix(KeyCodec::pack(key)); }

	Value serializeValue(ValueType const& val) { return ObjectWriter::toValue(val, versionOptions); }

	template <class Transaction>
	void erase(Transaction tr, KeyType const& key) {
		tr->clear(subspace.begin.withSuffix(KeyCodec::pack(key)));
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& begin, KeyType const& end) {
		tr->clear(KeyRangeRef(subspace.begin.withSuffix(KeyCodec::pack(begin)),
		                      subspace.begin.withSuffix(KeyCodec::pack(end))));
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
	}

	KeyRange subspace;
	VersionOptions versionOptions;
};

template <typename _ValueType, typename Codec = TupleCodec<_ValueType>>
class KeyBackedSet {
public:
	KeyBackedSet(KeyRef key) : subspace(prefixRange(key)) {}

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
		    tr->get(subspace.begin.withSuffix(Codec::pack(val)), snapshot);

		return holdWhile(getFuture, map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> bool {
			                 return val.present();
		                 }));
	}

	// Returns the expectedSize of the set key
	template <class Transaction>
	int insert(Transaction tr, ValueType const& val) {
		Key k = subspace.begin.withSuffix(Codec::pack(val));
		tr->set(k, StringRef());
		return k.expectedSize();
	}

	template <class Transaction>
	void erase(Transaction tr, ValueType const& val) {
		tr->clear(subspace.begin.withSuffix(Codec::pack(val)));
	}

	template <class Transaction>
	void erase(Transaction tr, ValueType const& begin, ValueType const& end) {
		tr->clear(
		    KeyRangeRef(subspace.begin.withSuffix(Codec::pack(begin)), subspace.begin.withSuffix(Codec::pack(end))));
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
	}

	KeyRange subspace;
};

// all fields are under prefix, the schema is like prefix/"packed key"/"packed key2"
class KeyBackedStruct {
public:
	KeyBackedStruct(StringRef prefix) : prefix(prefix), rootSpace(prefix) {}

protected:
	Key prefix;
	Subspace rootSpace;
};