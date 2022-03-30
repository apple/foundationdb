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

#include "fdbclient/IClientApi.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Subspace.h"
#include "flow/ObjectSerializer.h"
#include "flow/genericactors.actor.h"
#include "flow/serialize.h"

// Codec is a utility struct to convert a type to and from a Tuple.  It is used by the template
// classes below like KeyBackedProperty and KeyBackedMap to convert key parts and values
// from various types to Value strings and back.
// New types can be supported either by writing a new specialization or adding these
// methods to the type so that the default specialization can be used:
//   static T T::unpack(Tuple const &t)
//   Tuple T::pack() const
// Since Codec is a struct, partial specialization can be used, such as the std::pair
// partial specialization below allowing any std::pair<T1,T2> where T1 and T2 are already
// supported by Codec.
template <typename T>
struct Codec {
	static inline Tuple pack(T const& val) { return val.pack(); }
	static inline T unpack(Tuple const& t) { return T::unpack(t); }
};

// If T is Tuple then conversion is simple.
template <>
inline Tuple Codec<Tuple>::pack(Tuple const& val) {
	return val;
}
template <>
inline Tuple Codec<Tuple>::unpack(Tuple const& val) {
	return val;
}

template <>
inline Tuple Codec<int64_t>::pack(int64_t const& val) {
	return Tuple().append(val);
}
template <>
inline int64_t Codec<int64_t>::unpack(Tuple const& val) {
	return val.getInt(0);
}

template <>
inline Tuple Codec<bool>::pack(bool const& val) {
	return Tuple().append(val ? 1 : 0);
}
template <>
inline bool Codec<bool>::unpack(Tuple const& val) {
	return val.getInt(0) == 1;
}

template <>
inline Tuple Codec<Standalone<StringRef>>::pack(Standalone<StringRef> const& val) {
	return Tuple().append(val);
}
template <>
inline Standalone<StringRef> Codec<Standalone<StringRef>>::unpack(Tuple const& val) {
	return val.getString(0);
}

template <>
inline Tuple Codec<UID>::pack(UID const& val) {
	return Codec<Standalone<StringRef>>::pack(BinaryWriter::toValue<UID>(val, Unversioned()));
}
template <>
inline UID Codec<UID>::unpack(Tuple const& val) {
	return BinaryReader::fromStringRef<UID>(Codec<Standalone<StringRef>>::unpack(val), Unversioned());
}

// This is backward compatible with Codec<Standalone<StringRef>>
template <>
inline Tuple Codec<std::string>::pack(std::string const& val) {
	return Tuple().append(StringRef(val));
}
template <>
inline std::string Codec<std::string>::unpack(Tuple const& val) {
	return val.getString(0).toString();
}

// Partial specialization to cover all std::pairs as long as the component types are Codec compatible
template <typename First, typename Second>
struct Codec<std::pair<First, Second>> {
	static Tuple pack(typename std::pair<First, Second> const& val) {
		return Tuple().append(Codec<First>::pack(val.first)).append(Codec<Second>::pack(val.second));
	}
	static std::pair<First, Second> unpack(Tuple const& t) {
		ASSERT(t.size() == 2);
		return { Codec<First>::unpack(t.subTuple(0, 1)), Codec<Second>::unpack(t.subTuple(1, 2)) };
	}
};

template <typename T>
struct Codec<std::vector<T>> {
	static Tuple pack(typename std::vector<T> const& val) {
		Tuple t;
		for (T item : val) {
			Tuple itemTuple = Codec<T>::pack(item);
			// fdbclient doesn't support nested tuples yet. For now, flatten the tuple into StringRef
			t.append(itemTuple.pack());
		}
		return t;
	}

	static std::vector<T> unpack(Tuple const& t) {
		std::vector<T> v;

		for (int i = 0; i < t.size(); i++) {
			Tuple itemTuple = Tuple::unpack(t.getString(i));
			v.push_back(Codec<T>::unpack(itemTuple));
		}

		return v;
	}
};

template <>
inline Tuple Codec<KeyRange>::pack(KeyRange const& val) {
	return Tuple().append(val.begin).append(val.end);
}
template <>
inline KeyRange Codec<KeyRange>::unpack(Tuple const& val) {
	return KeyRangeRef(val.getString(0), val.getString(1));
}

// Convenient read/write access to a single value of type T stored at key
// Even though 'this' is not actually mutated, methods that change the db key are not const.
template <typename T>
class KeyBackedProperty {
public:
	KeyBackedProperty(KeyRef key) : key(key) {}
	Future<Optional<T>> get(Reference<ReadYourWritesTransaction> tr, Snapshot snapshot = Snapshot::False) const {
		return map(tr->get(key, snapshot), [](Optional<Value> const& val) -> Optional<T> {
			if (val.present())
				return Codec<T>::unpack(Tuple::unpack(val.get()));
			return {};
		});
	}
	// Get property's value or defaultValue if it doesn't exist
	Future<T> getD(Reference<ReadYourWritesTransaction> tr,
	               Snapshot snapshot = Snapshot::False,
	               T defaultValue = T()) const {
		return map(get(tr, snapshot), [=](Optional<T> val) -> T { return val.present() ? val.get() : defaultValue; });
	}
	// Get property's value or throw error if it doesn't exist
	Future<T> getOrThrow(Reference<ReadYourWritesTransaction> tr,
	                     Snapshot snapshot = Snapshot::False,
	                     Error err = key_not_found()) const {
		return map(get(tr, snapshot), [=](Optional<T> val) -> T {
			if (!val.present()) {
				throw err;
			}

			return val.get();
		});
	}

	Future<Optional<T>> get(Database cx, Snapshot snapshot = Snapshot::False) const {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.get(tr, snapshot);
		});
	}

	Future<T> getD(Database cx, Snapshot snapshot = Snapshot::False, T defaultValue = T()) const {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getD(tr, snapshot, defaultValue);
		});
	}

	Future<T> getOrThrow(Database cx, Snapshot snapshot = Snapshot::False, Error err = key_not_found()) const {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getOrThrow(tr, snapshot, err);
		});
	}

	void set(Reference<ReadYourWritesTransaction> tr, T const& val) { return tr->set(key, Codec<T>::pack(val).pack()); }

	Future<Void> set(Database cx, T const& val) {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			self->set(tr, val);
			return Future<Void>(Void());
		});
	}

	void clear(Reference<ReadYourWritesTransaction> tr) { return tr->clear(key); }
	Key key;
};

// This is just like KeyBackedProperty but instead of using Codec for conversion to/from values it
// uses BinaryReader and BinaryWriter.  This enables allows atomic ops with integer types, and also
// allows reading and writing of existing keys which use BinaryReader/Writer.
template <typename T>
class KeyBackedBinaryValue {
public:
	KeyBackedBinaryValue(KeyRef key) : key(key) {}
	Future<Optional<T>> get(Reference<ReadYourWritesTransaction> tr, Snapshot snapshot = Snapshot::False) const {
		return map(tr->get(key, snapshot), [](Optional<Value> const& val) -> Optional<T> {
			if (val.present())
				return BinaryReader::fromStringRef<T>(val.get(), Unversioned());
			return {};
		});
	}
	// Get property's value or defaultValue if it doesn't exist
	Future<T> getD(Reference<ReadYourWritesTransaction> tr,
	               Snapshot snapshot = Snapshot::False,
	               T defaultValue = T()) const {
		return map(get(tr, Snapshot::False),
		           [=](Optional<T> val) -> T { return val.present() ? val.get() : defaultValue; });
	}
	void set(Reference<ReadYourWritesTransaction> tr, T const& val) {
		return tr->set(key, BinaryWriter::toValue<T>(val, Unversioned()));
	}
	void atomicOp(Reference<ReadYourWritesTransaction> tr, T const& val, MutationRef::Type type) {
		return tr->atomicOp(key, BinaryWriter::toValue<T>(val, Unversioned()), type);
	}
	void clear(Reference<ReadYourWritesTransaction> tr) { return tr->clear(key); }
	Key key;
};

// Convenient read/write access to a sorted map of KeyType to ValueType under prefix
// Even though 'this' is not actually mutated, methods that change db keys are not const.
template <typename _KeyType, typename _ValueType>
class KeyBackedMap {
public:
	KeyBackedMap(KeyRef prefix) : space(prefix) {}

	typedef _KeyType KeyType;
	typedef _ValueType ValueType;
	typedef std::pair<KeyType, ValueType> PairType;
	typedef std::vector<PairType> PairsType;

	// If end is not present one key past the end of the map is used.
	Future<PairsType> getRange(Reference<ReadYourWritesTransaction> tr,
	                           KeyType const& begin,
	                           Optional<KeyType> const& end,
	                           int limit,
	                           Snapshot snapshot = Snapshot::False,
	                           Reverse reverse = Reverse::False) const {
		Subspace s = space; // 'this' could be invalid inside lambda
		Key endKey = end.present() ? s.pack(Codec<KeyType>::pack(end.get())) : space.range().end;
		return map(
		    tr->getRange(
		        KeyRangeRef(s.pack(Codec<KeyType>::pack(begin)), endKey), GetRangeLimits(limit), snapshot, reverse),
		    [s](RangeResult const& kvs) -> PairsType {
			    PairsType results;
			    for (int i = 0; i < kvs.size(); ++i) {
				    KeyType key = Codec<KeyType>::unpack(s.unpack(kvs[i].key));
				    ValueType val = Codec<ValueType>::unpack(Tuple::unpack(kvs[i].value));
				    results.push_back(PairType(key, val));
			    }
			    return results;
		    });
	}

	Future<Optional<ValueType>> get(Reference<ReadYourWritesTransaction> tr,
	                                KeyType const& key,
	                                Snapshot snapshot = Snapshot::False) const {
		return map(tr->get(space.pack(Codec<KeyType>::pack(key)), snapshot),
		           [](Optional<Value> const& val) -> Optional<ValueType> {
			           if (val.present())
				           return Codec<ValueType>::unpack(Tuple::unpack(val.get()));
			           return {};
		           });
	}

	// Returns a Property that can be get/set that represents key's entry in this this.
	KeyBackedProperty<ValueType> getProperty(KeyType const& key) const { return space.pack(Codec<KeyType>::pack(key)); }

	// Returns the expectedSize of the set key
	int set(Reference<ReadYourWritesTransaction> tr, KeyType const& key, ValueType const& val) {
		Key k = space.pack(Codec<KeyType>::pack(key));
		Value v = Codec<ValueType>::pack(val).pack();
		tr->set(k, v);
		return k.expectedSize() + v.expectedSize();
	}

	void erase(Reference<ReadYourWritesTransaction> tr, KeyType const& key) {
		return tr->clear(space.pack(Codec<KeyType>::pack(key)));
	}

	void erase(Reference<ITransaction> tr, KeyType const& key) {
		return tr->clear(space.pack(Codec<KeyType>::pack(key)));
	}

	void erase(Reference<ReadYourWritesTransaction> tr, KeyType const& begin, KeyType const& end) {
		return tr->clear(KeyRangeRef(space.pack(Codec<KeyType>::pack(begin)), space.pack(Codec<KeyType>::pack(end))));
	}

	void clear(Reference<ReadYourWritesTransaction> tr) { return tr->clear(space.range()); }

	Subspace space;
};

// Convenient read/write access to a single value of type T stored at key
// Even though 'this' is not actually mutated, methods that change the db key are not const.
template <typename T, typename VersionOptions>
class KeyBackedObjectProperty {
public:
	KeyBackedObjectProperty(KeyRef key, VersionOptions versionOptions) : key(key), versionOptions(versionOptions) {}
	Future<Optional<T>> get(Reference<ReadYourWritesTransaction> tr, Snapshot snapshot = Snapshot::False) const {

		return map(tr->get(key, snapshot), [vo = versionOptions](Optional<Value> const& val) -> Optional<T> {
			if (val.present())
				return ObjectReader::fromStringRef<T>(val.get(), vo);
			return {};
		});
	}

	// Get property's value or defaultValue if it doesn't exist
	Future<T> getD(Reference<ReadYourWritesTransaction> tr,
	               Snapshot snapshot = Snapshot::False,
	               T defaultValue = T()) const {
		return map(get(tr, snapshot), [=](Optional<T> val) -> T { return val.present() ? val.get() : defaultValue; });
	}
	// Get property's value or throw error if it doesn't exist
	Future<T> getOrThrow(Reference<ReadYourWritesTransaction> tr,
	                     Snapshot snapshot = Snapshot::False,
	                     Error err = key_not_found()) const {
		return map(get(tr, snapshot), [=](Optional<T> val) -> T {
			if (!val.present()) {
				throw err;
			}

			return val.get();
		});
	}

	Future<Optional<T>> get(Database cx, Snapshot snapshot = Snapshot::False) const {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.get(tr, snapshot);
		});
	}

	Future<T> getD(Database cx, Snapshot snapshot = Snapshot::False, T defaultValue = T()) const {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getD(tr, snapshot, defaultValue);
		});
	}

	Future<T> getOrThrow(Database cx, Snapshot snapshot = Snapshot::False, Error err = key_not_found()) const {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);

			return self.getOrThrow(tr, snapshot, err);
		});
	}

	void set(Reference<ReadYourWritesTransaction> tr, T const& val) {
		return tr->set(key, ObjectWriter::toValue(val, versionOptions));
	}

	Future<Void> set(Database cx, T const& val) {
		return runRYWTransaction(cx, [=, self = *this](Reference<ReadYourWritesTransaction> tr) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			self.set(tr, val);
			return Future<Void>(Void());
		});
	}

	void clear(Reference<ReadYourWritesTransaction> tr) { return tr->clear(key); }

	Key key;
	VersionOptions versionOptions;
};

// Convenient read/write access to a sorted map of KeyType to ValueType under key prefix
// ValueType is encoded / decoded with ObjectWriter/ObjectReader
// Even though 'this' is not actually mutated, methods that change db keys are not const.
template <typename _KeyType, typename _ValueType, typename VersionOptions>
class KeyBackedObjectMap {
public:
	KeyBackedObjectMap(KeyRef prefix, VersionOptions versionOptions) : space(prefix), versionOptions(versionOptions) {}

	typedef _KeyType KeyType;
	typedef _ValueType ValueType;
	typedef std::pair<KeyType, ValueType> PairType;
	typedef std::vector<PairType> PairsType;

	// If end is not present one key past the end of the map is used.
	Future<PairsType> getRange(Reference<ReadYourWritesTransaction> tr,
	                           KeyType const& begin,
	                           Optional<KeyType> const& end,
	                           int limit,
	                           Snapshot snapshot = Snapshot::False,
	                           Reverse reverse = Reverse::False) const {
		Key endKey = end.present() ? space.pack(Codec<KeyType>::pack(end.get())) : space.range().end;
		return map(
		    tr->getRange(
		        KeyRangeRef(space.pack(Codec<KeyType>::pack(begin)), endKey), GetRangeLimits(limit), snapshot, reverse),
		    [self = *this](RangeResult const& kvs) -> PairsType {
			    PairsType results;
			    for (int i = 0; i < kvs.size(); ++i) {
				    KeyType key = Codec<KeyType>::unpack(self.space.unpack(kvs[i].key));
				    ValueType val = ObjectReader::fromStringRef<ValueType>(kvs[i].value, self.versionOptions);
				    results.push_back(PairType(key, val));
			    }
			    return results;
		    });
	}

	Future<Optional<ValueType>> get(Reference<ReadYourWritesTransaction> tr,
	                                KeyType const& key,
	                                Snapshot snapshot = Snapshot::False) const {
		return map(tr->get(space.pack(Codec<KeyType>::pack(key)), snapshot),
		           [vo = versionOptions](Optional<Value> const& val) -> Optional<ValueType> {
			           if (val.present())
				           return ObjectReader::fromStringRef<ValueType>(val.get(), vo);
			           return {};
		           });
	}

	// Returns a Property that can be get/set that represents key's entry in this this.
	KeyBackedObjectProperty<ValueType, VersionOptions> getProperty(KeyType const& key) const {
		return KeyBackedObjectProperty<ValueType, VersionOptions>(space.pack(Codec<KeyType>::pack(key)),
		                                                          versionOptions);
	}

	// Returns the expectedSize of the set key
	int set(Reference<ReadYourWritesTransaction> tr, KeyType const& key, ValueType const& val) {
		Key k = space.pack(Codec<KeyType>::pack(key));
		Value v = ObjectWriter::toValue(val, versionOptions);
		tr->set(k, v);
		return k.expectedSize() + v.expectedSize();
	}

	Key serializeKey(KeyType const& key) { return space.pack(Codec<KeyType>::pack(key)); }

	Value serializeValue(ValueType const& val) { return ObjectWriter::toValue(val, versionOptions); }

	void erase(Reference<ReadYourWritesTransaction> tr, KeyType const& key) {
		return tr->clear(space.pack(Codec<KeyType>::pack(key)));
	}

	void erase(Reference<ITransaction> tr, KeyType const& key) {
		return tr->clear(space.pack(Codec<KeyType>::pack(key)));
	}

	void erase(Reference<ReadYourWritesTransaction> tr, KeyType const& begin, KeyType const& end) {
		return tr->clear(KeyRangeRef(space.pack(Codec<KeyType>::pack(begin)), space.pack(Codec<KeyType>::pack(end))));
	}

	void clear(Reference<ReadYourWritesTransaction> tr) { return tr->clear(space.range()); }

	Subspace space;
	VersionOptions versionOptions;
};

template <typename _ValueType>
class KeyBackedSet {
public:
	KeyBackedSet(KeyRef key) : space(key) {}

	typedef _ValueType ValueType;
	typedef std::vector<ValueType> Values;

	// If end is not present one key past the end of the map is used.
	Future<Values> getRange(Reference<ReadYourWritesTransaction> tr,
	                        ValueType const& begin,
	                        Optional<ValueType> const& end,
	                        int limit,
	                        Snapshot snapshot = Snapshot::False) const {
		Subspace s = space; // 'this' could be invalid inside lambda
		Key endKey = end.present() ? s.pack(Codec<ValueType>::pack(end.get())) : space.range().end;
		return map(
		    tr->getRange(KeyRangeRef(s.pack(Codec<ValueType>::pack(begin)), endKey), GetRangeLimits(limit), snapshot),
		    [s](RangeResult const& kvs) -> Values {
			    Values results;
			    for (int i = 0; i < kvs.size(); ++i) {
				    results.push_back(Codec<ValueType>::unpack(s.unpack(kvs[i].key)));
			    }
			    return results;
		    });
	}

	Future<bool> exists(Reference<ReadYourWritesTransaction> tr,
	                    ValueType const& val,
	                    Snapshot snapshot = Snapshot::False) const {
		return map(tr->get(space.pack(Codec<ValueType>::pack(val)), snapshot),
		           [](Optional<Value> const& val) -> bool { return val.present(); });
	}

	// Returns the expectedSize of the set key
	int insert(Reference<ReadYourWritesTransaction> tr, ValueType const& val) {
		Key k = space.pack(Codec<ValueType>::pack(val));
		tr->set(k, StringRef());
		return k.expectedSize();
	}

	void erase(Reference<ReadYourWritesTransaction> tr, ValueType const& val) {
		return tr->clear(space.pack(Codec<ValueType>::pack(val)));
	}

	void erase(Reference<ReadYourWritesTransaction> tr, ValueType const& begin, ValueType const& end) {
		return tr->clear(
		    KeyRangeRef(space.pack(Codec<ValueType>::pack(begin)), space.pack(Codec<ValueType>::pack(end))));
	}

	void clear(Reference<ReadYourWritesTransaction> tr) { return tr->clear(space.range()); }

	Subspace space;
};
