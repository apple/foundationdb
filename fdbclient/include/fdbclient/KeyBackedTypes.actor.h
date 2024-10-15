/*
 * KeyBackedTypes.actor.h
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

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_KEYBACKEDTYPES_ACTOR_G_H)
#define FDBCLIENT_KEYBACKEDTYPES_ACTOR_G_H
#include "fdbclient/KeyBackedTypes.actor.g.h"
#elif !defined(FDBCLIENT_KEYBACKEDTYPES_ACTOR_H)
#define FDBCLIENT_KEYBACKEDTYPES_ACTOR_H

#include <utility>
#include <vector>
#include <ranges>

#include "fdbclient/ClientBooleanParams.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/RunTransaction.actor.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/GenericTransactionHelper.h"
#include "fdbclient/Subspace.h"
#include "fdbclient/TupleVersionstamp.h"
#include "flow/ObjectSerializer.h"
#include "flow/Platform.h"
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"
#include "flow/ThreadHelper.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

#define KEYBACKEDTYPES_DEBUG 0
#if KEYBACKEDTYPES_DEBUG || !defined(NO_INTELLISENSE)
#define kbt_debug fmt::print
#else
#define kbt_debug(...)
#endif

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
template <typename T, typename Enabled = void>
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
inline Standalone<StringRef> TupleCodec<int>::pack(int const& val) {
	return Tuple::makeTuple(val).pack();
}
template <>
inline int TupleCodec<int>::unpack(Standalone<StringRef> const& val) {
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

template <>
inline Standalone<StringRef> TupleCodec<TupleVersionstamp>::pack(TupleVersionstamp const& val) {
	return Tuple::makeTuple(val).pack();
}
template <>
inline TupleVersionstamp TupleCodec<TupleVersionstamp>::unpack(Standalone<StringRef> const& val) {
	return Tuple::unpack(val).getVersionstamp(0);
}

template <>
inline Standalone<StringRef> TupleCodec<Versionstamp>::pack(Versionstamp const& val) {
	return TupleCodec<TupleVersionstamp>::pack(TupleVersionstamp(val.version, val.batchNumber));
}
template <>
inline Versionstamp TupleCodec<Versionstamp>::unpack(Standalone<StringRef> const& val) {
	TupleVersionstamp vs = TupleCodec<TupleVersionstamp>::unpack(val);
	ASSERT(vs.getUserVersion() == 0);
	return Versionstamp(vs.getVersion(), vs.getBatchNumber());
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

template <class Enum>
struct TupleCodec<Enum, std::enable_if_t<std::is_enum_v<Enum>>> {
	static inline Standalone<StringRef> pack(Enum const& val) {
		return Tuple::makeTuple(static_cast<int64_t>(val)).pack();
	}
	static inline Enum unpack(Standalone<StringRef> const& val) {
		return static_cast<Enum>(Tuple::unpack(val).getInt(0));
	}
};

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
		tr->atomicOp(key, StringRef(value.data(), value.size()), MutationRef::SetVersionstampedValue);
		if (conflict) {
			tr->addReadConflictRange(singleKeyRange(key));
		}
	}

	template <class Transaction>
	Future<Optional<Versionstamp>> get(Transaction tr, Snapshot snapshot = Snapshot::False) const {
		typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

		return holdWhile(
		    getFuture,
		    map(safeThreadFutureToFuture(getFuture), [](Optional<Value> const& val) -> Optional<Versionstamp> {
			    if (val.present()) {
				    return BinaryReader::fromStringRef<Versionstamp>(*val, Unversioned());
			    }
			    return {};
		    }));
	}

	template <class Transaction>
	Future<Void> watch(Transaction tr) const {
		typename transaction_future_type<Transaction, Void>::type watchFuture = tr->watch(key);
		return holdWhile(watchFuture, safeThreadFutureToFuture(watchFuture));
	}

// Forward declaration of this static template actor does not work correctly, so this actor is forward declared
// in a way that works for both compiling and in IDEs.
#if defined(NO_INTELLISENSE)
	template <class DB>
	static Future<Version> onChangeActor(WatchableTrigger const& self,
	                                     Reference<DB> const& db,
	                                     Optional<Version> const& initialVersion,
	                                     Promise<Version> const& watching);
#else
	ACTOR template <class DB>
	static Future<Version> onChangeActor(WatchableTrigger self,
	                                     Reference<DB> db,
	                                     Optional<Version> initialVersion,
	                                     Promise<Version> watching);
#endif

	// Watch the trigger until it changes.  The result will be ready when it is observed that the trigger value's
	// version is greater than initialVersion, and the observed trigger value version will be returned.
	//
	// If initialVersion is not present it will be initialized internally to the first read version successfully
	// obtained from the db.
	//
	// initialVersion can be thought of as a "last known version" but it could also be used to indicate some future
	// version after which you want to be notified if the trigger's value changes.
	//
	// If watching can be set, the initialized value of initialVersion will be sent to it once known.
	template <class DB>
	Future<Version> onChange(Reference<DB> db,
	                         Optional<Version> initialVersion = {},
	                         Promise<Version> watching = {}) const {
		return onChangeActor(*this, db, initialVersion, watching);
	}
};

ACTOR template <class DB>
Future<Version> WatchableTrigger::onChangeActor(WatchableTrigger self,
                                                Reference<DB> db,
                                                Optional<Version> initialVersion,
                                                Promise<Version> watching) {
	state Reference<typename DB::TransactionT> tr = db->createTransaction();

	loop {
		if constexpr (can_set_transaction_options<DB>) {
			db->setOptions(tr);
		}
		try {
			// If the initialVersion is not set yet, then initialize it with the read version
			if (!initialVersion.present()) {
				wait(store(initialVersion, safeThreadFutureToFuture(tr->getReadVersion())));
			}
			if (watching.canBeSet()) {
				watching.send(*initialVersion);
			}

			// Get the trigger's latest value.
			Optional<Versionstamp> currentVal = wait(self.get(tr));

			// If the trigger has a value and its version is > initialVersion then the trigger has fired so break
			if (currentVal.present() && currentVal->version > *initialVersion) {
				return currentVal->version;
			}

			// Otherwise, watch the key and repeat the loop once the watch fires
			state Future<Void> watch = self.watch(tr);
			wait(safeThreadFutureToFuture(tr->commit()));
			wait(watch);
			tr->reset();
		} catch (Error& e) {
			wait(safeThreadFutureToFuture(tr->onError(e)));
		}
	}
}

// Convenient read/write access to a single value of type T stored at key
// Even though 'this' is not actually mutated, methods that change the db key are not const.
template <typename T, typename Codec = TupleCodec<T>, bool SystemAccess = true>
class KeyBackedProperty {
public:
	KeyBackedProperty(KeyRef key = invalidKey, Optional<WatchableTrigger> trigger = {}, Codec codec = {})
	  : key(key), trigger(trigger), codec(codec) {}

	template <class Transaction>
	Future<Optional<T>> get(Transaction tr, Snapshot snapshot = Snapshot::False) const {
		if constexpr (is_transaction_creator<Transaction>) {
			return runTransaction(tr, [=, self = *this](decltype(tr->createTransaction()) tr) {
				if constexpr (SystemAccess) {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				}
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				return self.get(tr, snapshot);
			});
		} else {
			typename transaction_future_type<Transaction, Optional<Value>>::type getFuture = tr->get(key, snapshot);

			return holdWhile(
			    getFuture,
			    map(safeThreadFutureToFuture(getFuture), [codec = codec](Optional<Value> const& val) -> Optional<T> {
				    if (val.present())
					    return codec.unpack(val.get());
				    return {};
			    }));
		}
	}

	// Get property's value or defaultValue if it doesn't exist
	template <class Transaction>
	Future<T> getD(Transaction tr, Snapshot snapshot = Snapshot::False, T defaultValue = T()) const {
		if constexpr (is_transaction_creator<Transaction>) {
			return runTransaction(tr, [=, self = *this](decltype(tr->createTransaction()) tr) {
				if constexpr (SystemAccess) {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				}
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				return self.getD(tr, snapshot, defaultValue);
			});
		} else {
			return map(get(tr, snapshot),
			           [=](Optional<T> val) -> T { return val.present() ? val.get() : defaultValue; });
		}
	}

	// Get property's value or throw error if it doesn't exist
	template <class Transaction>
	Future<T> getOrThrow(Transaction tr, Snapshot snapshot = Snapshot::False, Error err = key_not_found()) const {
		if constexpr (is_transaction_creator<Transaction>) {
			return runTransaction(tr, [=, self = *this](decltype(tr->createTransaction()) tr) {
				if constexpr (SystemAccess) {
					tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				}
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				return self.getOrThrow(tr, snapshot, err);
			});
		} else {
			return map(get(tr, snapshot), [=](Optional<T> val) -> T {
				if (!val.present()) {
					throw err;
				}

				return val.get();
			});
		}
	}

	template <class TransactionContext>
	std::enable_if_t<is_transaction_creator<TransactionContext>, Future<Void>> set(TransactionContext tcx,
	                                                                               T const& val) {
		return runTransaction(tcx, [this, val](decltype(tcx->createTransaction()) tr) {
			if constexpr (SystemAccess) {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			}
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			set(tr, val);
			return Future<Void>(Void());
		});
	}

	template <class Transaction>
	std::enable_if_t<!is_transaction_creator<Transaction>, void> set(Transaction tr, T const& val) {
		tr->set(key, packValue(val));
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class TransactionContext>
	std::enable_if_t<is_transaction_creator<TransactionContext>, Future<Void>> clear(TransactionContext tcx) {
		return runTransaction(tcx, [this](decltype(tcx->createTransaction()) tr) {
			if constexpr (SystemAccess) {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			}
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			clear(tr);
			return Future<Void>(Void());
		});
	}

	template <class Transaction>
	std::enable_if_t<!is_transaction_creator<Transaction>, void> clear(Transaction tr) {
		tr->clear(key);
		if (trigger.present()) {
			trigger->update(tr);
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
	KeyBackedBinaryValue(KeyRef key = invalidKey, Optional<WatchableTrigger> trigger = {}) : Base(key, trigger) {}

	template <class Transaction>
	void atomicOp(Transaction tr, T const& val, MutationRef::Type type) {
		tr->atomicOp(this->key, BinaryWriter::toValue<T>(val, Unversioned()), type);
		if (this->trigger.present()) {
			this->trigger->update(tr);
		}
	}

	template <class Transaction>
	void setVersionstamp(Transaction tr, T const& val, int offset) {
		tr->atomicOp(
		    this->key,
		    BinaryWriter::toValue<T>(val, Unversioned()).withSuffix(StringRef(reinterpret_cast<uint8_t*>(&offset), 4)),
		    MutationRef::SetVersionstampedValue);
		if (this->trigger.present()) {
			this->trigger->update(tr);
		}
	}
};

template <typename KeyType, typename ValueType>
struct TypedKVPair {
	KeyType key;
	ValueType value;
};

template <typename KeyType>
struct TypedRange {
	KeyType begin;
	KeyType end;
};

template <typename KeyType, typename KeyCodec>
struct TypedKeySelector {
	KeyType key;
	bool orEqual;
	int offset;

	TypedKeySelector operator+(int delta) { return { key, orEqual, offset + delta }; }

	TypedKeySelector operator-(int delta) { return { key, orEqual, offset - delta }; }

	KeySelector pack(const KeyRef& prefix) const {
		Key packed = KeyCodec::pack(key).withPrefix(prefix);
		return KeySelector(KeySelectorRef(packed, orEqual, offset), packed.arena());
	}

	static TypedKeySelector lastLessThan(const KeyType& k) { return { k, false, 0 }; }

	static TypedKeySelector lastLessOrEqual(const KeyType& k) { return { k, true, 0 }; }

	static TypedKeySelector firstGreaterThan(const KeyType& k) { return { k, true, +1 }; }

	static TypedKeySelector firstGreaterOrEqual(const KeyType& k) { return { k, false, +1 }; }
};

// Convenient read/write access to a sorted map of KeyType to ValueType under prefix
// Even though 'this' is not actually mutated, methods that change db keys are not const.
template <typename _KeyType,
          typename _ValueType,
          typename KeyCodec = TupleCodec<_KeyType>,
          typename ValueCodec = TupleCodec<_ValueType>>
class KeyBackedMap {
public:
	KeyBackedMap(KeyRef prefix = invalidKey, Optional<WatchableTrigger> trigger = {}, ValueCodec valueCodec = {})
	  : subspace(prefixRange(prefix)), trigger(trigger), valueCodec(valueCodec) {}

	typedef _KeyType KeyType;
	typedef _ValueType ValueType;
	typedef std::pair<KeyType, ValueType> PairType;
	typedef KeyBackedRangeResult<PairType> RangeResultType;
	typedef TypedKVPair<KeyType, ValueType> KVType;
	typedef KeyBackedProperty<ValueType, ValueCodec> SingleRecordProperty;
	typedef TypedKeySelector<KeyType, KeyCodec> KeySelector;

	// If end is not present one key past the end of the map is used.
	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 Optional<KeyType> const& begin,
	                                 Optional<KeyType> const& end,
	                                 int limit,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {

		if constexpr (is_transaction_creator<Transaction>) {
			return runTransaction(tr, [=, self = *this](decltype(tr->createTransaction()) tr) {
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr->setOption(FDBTransactionOptions::LOCK_AWARE);
				return self.getRange(tr, begin, end, limit, snapshot, reverse);
			});
		} else {
			Key beginKey = begin.present() ? packKey(begin.get()) : subspace.begin;
			Key endKey = end.present() ? packKey(end.get()) : subspace.end;

			typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
			    tr->getRange(KeyRangeRef(beginKey, endKey), GetRangeLimits(limit), snapshot, reverse);

			return holdWhile(
			    getRangeFuture,
			    map(safeThreadFutureToFuture(getRangeFuture),
			        [prefix = subspace.begin, valueCodec = valueCodec](RangeResult const& kvs) -> RangeResultType {
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
	}

	ACTOR template <class Transaction>
	static Future<RangeResultType> getRangeActor(KeyBackedMap self,
	                                             Transaction tr,
	                                             KeySelector begin,
	                                             KeySelector end,
	                                             GetRangeLimits limits,
	                                             Snapshot snapshot,
	                                             Reverse reverse) {
		kbt_debug("MAP GETRANGE KeySelectors {} - {}\n",
		          begin.pack(self.subspace.begin).toString(),
		          end.pack(self.subspace.begin).toString());

		state ::KeySelector ksBegin = begin.pack(self.subspace.begin);
		state ::KeySelector ksEnd = end.pack(self.subspace.begin);
		state typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(ksBegin, ksEnd, limits, snapshot, reverse);

		// Since the getRange result must be filtered to keys within the map subspace, it is possible that the given
		// TypedKeySelectors and GetRangeLimits yields a result in which no KV pairs are within the map subspace.  If
		// this happens, we can't return any map KV pairs for the caller to base the next request on, so we will loop
		// and continue with the next request here.
		state RangeResultType rangeResult;
		loop {
			RangeResult kvs = wait(safeThreadFutureToFuture(getRangeFuture));
			kbt_debug("MAP GETRANGE KeySelectors {} - {} results={} more={}\n",
			          begin.pack(self.subspace.begin).toString(),
			          end.pack(self.subspace.begin).toString(),
			          kvs.size(),
			          kvs.more);

			for (auto const& kv : kvs) {
				kbt_debug("   {} -> {}\n", kv.key.printable(), kv.value.printable());

				// KeySelectors could resolve to keys outside the map subspace so we must filter
				if (self.subspace.contains(kv.key)) {
					KeyType key = self.unpackKey(kv.key);
					ValueType val = self.unpackValue(kv.value);
					rangeResult.results.push_back(PairType(key, val));
				}
			}

			// Stop if there are no more raw results
			if (!kvs.more) {
				break;
			}

			// There may be more raw results in the range, so now determine if we need to read any of them or if we can
			// return what we have found so far. If the filtered result set is not empty then we can return it
			if (!rangeResult.results.empty()) {
				// kvs.more is known to be true and kvs is not empty since the filtered result set is not empty.  Set
				// the output rangeResult.more to true if the last raw result was within the map range, else false.
				rangeResult.more = self.subspace.contains(kvs.back().key);
				break;
			}

			// At this point, the filtered rangeResult is empty but there may be more raw results in the db.  We cannot
			// return this rangeResult to the caller because the caller won't know which key to start reading at for the
			// next chunk, so we will use the raw keys to read the next chunk here and repeat the loop.
			if (reverse) {
				// The last key is the end of the new getRange, which includes the back key with orEqual because
				// getRange end is exclusive
				ksEnd = ::firstGreaterOrEqual(kvs.back().key);
			} else {
				// The last key is the exclusive begin of the new getRange
				ksBegin = ::firstGreaterThan(kvs.back().key);
			}
			getRangeFuture = tr->getRange(ksBegin, ksEnd, limits, snapshot, reverse);
		}

		return rangeResult;
	}

	// GetRange with typed KeySelectors
	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 KeySelector begin,
	                                 KeySelector end,
	                                 GetRangeLimits limits,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {
		return getRangeActor(*this, tr, begin, end, limits, snapshot, reverse);
	}

	// Find the closest key which is <, <=, >, or >= query
	// These operation can be accomplished using KeySelectors however they run the risk of touching keys outside of
	// map subspace, which can cause problems if this touches an offline range or a key which is unreadable by range
	// read operations due to having been modified with a version stamp operation in the current transaction.
	ACTOR template <class Transaction>
	static Future<Optional<KVType>> seek(KeyBackedMap self,
	                                     Transaction tr,
	                                     KeyType query,
	                                     bool lessThan,
	                                     bool orEqual,
	                                     Snapshot snapshot) {
		// Operations map to the following getRange operations
		// <  query  getRange begin               query   1                reverse
		// <= query  getRange begin               keyAfter(query)          reverse
		// >= query  getRange key                 end   1                  forward
		// >  query  getRange keyAfter(query)     end   1                  forward
		Key begin;
		Key end;

		if (lessThan) {
			begin = self.subspace.begin;
			end = self.packKey(query);
			if (orEqual) {
				end = keyAfter(end);
			}
		} else {
			begin = self.packKey(query);
			if (!orEqual) {
				begin = keyAfter(begin);
			}
			end = self.subspace.end;
		}

		state typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(KeyRangeRef(begin, end), 1, snapshot, Reverse{ lessThan });

		RangeResult kvs = wait(safeThreadFutureToFuture(getRangeFuture));
		if (kvs.empty()) {
			return Optional<KVType>();
		}

		return self.unpackKV(kvs.front());
	}

	template <class Transaction>
	Future<Optional<KVType>> seekLessThan(Transaction tr, KeyType query, Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, true, false, snapshot);
	}

	template <class Transaction>
	Future<Optional<KVType>> seekLessOrEqual(Transaction tr, KeyType query, Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, true, true, snapshot);
	}

	template <class Transaction>
	Future<Optional<KVType>> seekGreaterThan(Transaction tr, KeyType query, Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, false, false, snapshot);
	}

	template <class Transaction>
	Future<Optional<KVType>> seekGreaterOrEqual(Transaction tr,
	                                            KeyType query,
	                                            Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, false, true, snapshot);
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
		kbt_debug("MAP SET {} -> {}\n", k.printable(), v.printable());
		tr->set(k, v);
		if (trigger.present()) {
			trigger->update(tr);
		}
		return k.expectedSize() + v.expectedSize();
	}

	template <class Transaction>
	void atomicOp(Transaction tr, KeyType const& key, ValueType const& val, MutationRef::Type type) {
		Key k = packKey(key);
		Value v = packValue(val);
		tr->atomicOp(k, v, type);
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class Transaction>
	void setVersionstamp(Transaction tr, KeyType const& key, ValueType const& val, int offset = 0) {
		Key k = packKey(key);
		Value v = packValue(val).withSuffix(StringRef(reinterpret_cast<uint8_t*>(&offset), 4));
		tr->atomicOp(k, v, MutationRef::SetVersionstampedValue);
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& key) {
		kbt_debug("MAP ERASE {}\n", packKey(key).printable());
		tr->clear(packKey(key));
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class Transaction>
	void erase(Transaction tr, KeyType const& begin, KeyType const& end) {
		tr->clear(KeyRangeRef(packKey(begin), packKey(end)));
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
		if (trigger.present()) {
			trigger->update(tr);
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
	typedef ObjectCodec<_ValueType, VersionOptions> ValueCodec;
	typedef KeyBackedMap<_KeyType, _ValueType, KeyCodec, ValueCodec> Base;

public:
	KeyBackedObjectMap(KeyRef key, VersionOptions vo, Optional<WatchableTrigger> trigger = {})
	  : Base(key, trigger, ValueCodec(vo)) {}
};

template <typename _ValueType, typename Codec = TupleCodec<_ValueType>>
class KeyBackedSet {
public:
	KeyBackedSet(KeyRef key = invalidKey, Optional<WatchableTrigger> trigger = {})
	  : subspace(prefixRange(key)), trigger(trigger) {}

	typedef _ValueType ValueType;
	typedef KeyBackedRangeResult<ValueType> RangeResultType;
	typedef TypedKeySelector<ValueType, Codec> KeySelector;

	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 Optional<ValueType> const& begin,
	                                 Optional<ValueType> const& end,
	                                 int limit,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {
		Key beginKey = begin.present() ? packKey(begin.get()) : subspace.begin;
		Key endKey = end.present() ? packKey(end.get()) : subspace.end;

		typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(KeyRangeRef(beginKey, endKey), GetRangeLimits(limit), snapshot, reverse);

		return holdWhile(getRangeFuture,
		                 map(safeThreadFutureToFuture(getRangeFuture),
		                     [prefix = subspace.begin](RangeResult const& kvs) -> RangeResultType {
			                     RangeResultType rangeResult;
			                     for (auto const& kv : kvs) {
				                     rangeResult.results.push_back(Codec::unpack(kv.key.removePrefix(prefix)));
			                     }
			                     rangeResult.more = kvs.more;
			                     return rangeResult;
		                     }));
	}

	ACTOR template <class Transaction>
	static Future<RangeResultType> getRangeActor(KeyBackedSet self,
	                                             Transaction tr,
	                                             KeySelector begin,
	                                             KeySelector end,
	                                             GetRangeLimits limits,
	                                             Snapshot snapshot,
	                                             Reverse reverse) {
		kbt_debug("MAP GETRANGE KeySelectors {} - {}\n",
		          begin.pack(self.subspace.begin).toString(),
		          end.pack(self.subspace.begin).toString());

		state ::KeySelector ksBegin = begin.pack(self.subspace.begin);
		state ::KeySelector ksEnd = end.pack(self.subspace.begin);
		state typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(ksBegin, ksEnd, limits, snapshot, reverse);

		// Since the getRange result must be filtered to keys within the map subspace, it is possible that the given
		// TypedKeySelectors and GetRangeLimits yields a result in which no KV pairs are within the map subspace.  If
		// this happens, we can't return any map KV pairs for the caller to base the next request on, so we will loop
		// and continue with the next request here.
		state RangeResultType rangeResult;
		loop {
			RangeResult kvs = wait(safeThreadFutureToFuture(getRangeFuture));
			kbt_debug("MAP GETRANGE KeySelectors {} - {} results={} more={}\n",
			          begin.pack(self.subspace.begin).toString(),
			          end.pack(self.subspace.begin).toString(),
			          kvs.size(),
			          kvs.more);

			for (auto const& kv : kvs) {
				kbt_debug("   {} -> {}\n", kv.key.printable(), kv.value.printable());

				// KeySelectors could resolve to keys outside the map subspace so we must filter
				if (self.subspace.contains(kv.key)) {
					rangeResult.results.push_back(self.unpackKey(kv.key));
				}
			}

			// Stop if there are no more raw results
			if (!kvs.more) {
				break;
			}

			// There may be more raw results in the range, so now determine if we need to read any of them or if we can
			// return what we have found so far. If the filtered result set is not empty then we can return it
			if (!rangeResult.results.empty()) {
				// kvs.more is known to be true and kvs is not empty since the filtered result set is not empty.  Set
				// the output rangeResult.more to true if the last raw result was within the map range, else false.
				rangeResult.more = self.subspace.contains(kvs.back().key);
				break;
			}

			// At this point, the filtered rangeResult is empty but there may be more raw results in the db.  We cannot
			// return this rangeResult to the caller because the caller won't know which key to start reading at for the
			// next chunk, so we will use the raw keys to read the next chunk here and repeat the loop.
			if (reverse) {
				// The last key is the end of the new getRange, which includes the back key with orEqual because
				// getRange end is exclusive
				ksEnd = ::firstGreaterOrEqual(kvs.back().key);
			} else {
				// The last key is the exclusive begin of the new getRange
				ksBegin = ::firstGreaterThan(kvs.back().key);
			}
			getRangeFuture = tr->getRange(ksBegin, ksEnd, limits, snapshot, reverse);
		}

		return rangeResult;
	}

	// GetRange with typed KeySelectors
	template <class Transaction>
	Future<RangeResultType> getRange(Transaction tr,
	                                 KeySelector begin,
	                                 KeySelector end,
	                                 GetRangeLimits limits,
	                                 Snapshot snapshot = Snapshot::False,
	                                 Reverse reverse = Reverse::False) const {
		return getRangeActor(*this, tr, begin, end, limits, snapshot, reverse);
	}

	// Find the closest key which is <, <=, >, or >= query
	// These operation can be accomplished using KeySelectors however they run the risk of touching keys outside of
	// map subspace, which can cause problems if this touches an offline range or a key which is unreadable by range
	// read operations due to having been modified with a version stamp operation in the current transaction.
	ACTOR template <class Transaction>
	static Future<Optional<ValueType>> seek(KeyBackedSet self,
	                                        Transaction tr,
	                                        ValueType query,
	                                        bool lessThan,
	                                        bool orEqual,
	                                        Snapshot snapshot) {
		// Operations map to the following getRange operations
		// <  query  getRange begin               query   1                reverse
		// <= query  getRange begin               keyAfter(query)          reverse
		// >= query  getRange key                 end   1                  forward
		// >  query  getRange keyAfter(query)     end   1                  forward
		Key begin;
		Key end;

		if (lessThan) {
			begin = self.subspace.begin;
			end = self.packKey(query);
			if (orEqual) {
				end = keyAfter(end);
			}
		} else {
			begin = self.packKey(query);
			if (!orEqual) {
				begin = keyAfter(begin);
			}
			end = self.subspace.end;
		}

		state typename transaction_future_type<Transaction, RangeResult>::type getRangeFuture =
		    tr->getRange(KeyRangeRef(begin, end), 1, snapshot, Reverse{ lessThan });

		RangeResult kvs = wait(safeThreadFutureToFuture(getRangeFuture));
		if (kvs.empty()) {
			return Optional<ValueType>();
		}

		return self.unpackKey(kvs.front());
	}

	template <class Transaction>
	Future<Optional<ValueType>> seekLessThan(Transaction tr,
	                                         ValueType query,
	                                         Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, true, false, snapshot);
	}

	template <class Transaction>
	Future<Optional<ValueType>> seekLessOrEqual(Transaction tr,
	                                            ValueType query,
	                                            Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, true, true, snapshot);
	}

	template <class Transaction>
	Future<Optional<ValueType>> seekGreaterThan(Transaction tr,
	                                            ValueType query,
	                                            Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, false, false, snapshot);
	}

	template <class Transaction>
	Future<Optional<ValueType>> seekGreaterOrEqual(Transaction tr,
	                                               ValueType query,
	                                               Snapshot snapshot = Snapshot::False) const {
		return seek(*this, tr, query, false, true, snapshot);
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
			trigger->update(tr);
		}
		return k.expectedSize();
	}

	template <class Transaction>
	void erase(Transaction tr, ValueType const& val) {
		tr->clear(packKey(val));
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class Transaction>
	void erase(Transaction tr, ValueType const& begin, ValueType const& end) {
		tr->clear(KeyRangeRef(packKey(begin), packKey(end)));
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	template <class Transaction>
	void clear(Transaction tr) {
		tr->clear(subspace);
		if (trigger.present()) {
			trigger->update(tr);
		}
	}

	KeyRange subspace;
	Optional<WatchableTrigger> trigger;

	Key packKey(ValueType const& value) const { return subspace.begin.withSuffix(Codec::pack(value)); }
	ValueType unpackKey(KeyRef const& key) const { return Codec::unpack(key.removePrefix(subspace.begin)); }
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
	  : subspace(prefix), trigger(triggerOverride.orDefault(subspace.pack("_changeTrigger"_sr))) {}

	Subspace subspace;
	WatchableTrigger trigger;
};

#include "flow/unactorcompiler.h"

#endif
