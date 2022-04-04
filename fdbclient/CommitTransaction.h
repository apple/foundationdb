/*
 * CommitTransaction.h
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

#ifndef FLOW_FDBCLIENT_COMMITTRANSACTION_H
#define FLOW_FDBCLIENT_COMMITTRANSACTION_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/Knobs.h"

// The versioned message has wire format : -1, version, messages
static const int32_t VERSION_HEADER = -1;

static const char* typeString[] = { "SetValue",
	                                "ClearRange",
	                                "AddValue",
	                                "DebugKeyRange",
	                                "DebugKey",
	                                "NoOp",
	                                "And",
	                                "Or",
	                                "Xor",
	                                "AppendIfFits",
	                                "AvailableForReuse",
	                                "Reserved_For_LogProtocolMessage",
	                                "Max",
	                                "Min",
	                                "SetVersionstampedKey",
	                                "SetVersionstampedValue",
	                                "ByteMin",
	                                "ByteMax",
	                                "MinV2",
	                                "AndV2",
	                                "CompareAndClear",
	                                "Reserved_For_SpanContextMessage",
	                                "MAX_ATOMIC_OP" };

struct MutationRef {
	static const int OVERHEAD_BYTES = 12; // 12 is the size of Header in MutationList entries
	enum Type : uint8_t {
		SetValue = 0,
		ClearRange,
		AddValue,
		DebugKeyRange,
		DebugKey,
		NoOp,
		And,
		Or,
		Xor,
		AppendIfFits,
		AvailableForReuse,
		Reserved_For_LogProtocolMessage /* See fdbserver/LogProtocolMessage.h */,
		Max,
		Min,
		SetVersionstampedKey,
		SetVersionstampedValue,
		ByteMin,
		ByteMax,
		MinV2,
		AndV2,
		CompareAndClear,
		Reserved_For_SpanContextMessage /* See fdbserver/SpanContextMessage.h */,
		MAX_ATOMIC_OP
	};
	// This is stored this way for serialization purposes.
	uint8_t type;
	StringRef param1, param2;

	MutationRef() {}
	MutationRef(Type t, StringRef a, StringRef b) : type(t), param1(a), param2(b) {}
	MutationRef(Arena& to, Type t, StringRef a, StringRef b) : type(t), param1(to, a), param2(to, b) {}
	MutationRef(Arena& to, const MutationRef& from)
	  : type(from.type), param1(to, from.param1), param2(to, from.param2) {}
	int totalSize() const { return OVERHEAD_BYTES + param1.size() + param2.size(); }
	int expectedSize() const { return param1.size() + param2.size(); }
	int weightedTotalSize() const {
		// AtomicOp can cause more workload to FDB cluster than the same-size set mutation;
		// Amplify atomicOp size to consider such extra workload.
		// A good value for FASTRESTORE_ATOMICOP_WEIGHT needs experimental evaluations.
		if (isAtomicOp()) {
			return totalSize() * CLIENT_KNOBS->FASTRESTORE_ATOMICOP_WEIGHT;
		} else {
			return totalSize();
		}
	}

	std::string toString() const {
		return format("code: %s param1: %s param2: %s",
		              type < MutationRef::MAX_ATOMIC_OP ? typeString[(int)type] : "Unset",
		              printable(param1).c_str(),
		              printable(param2).c_str());
	}

	bool isAtomicOp() const { return (ATOMIC_MASK & (1 << type)) != 0; }

	template <class Ar>
	void serialize(Ar& ar) {
		if (ar.isSerializing && type == ClearRange && equalsKeyAfter(param1, param2)) {
			StringRef empty;
			serializer(ar, type, param2, empty);
		} else {
			serializer(ar, type, param1, param2);
		}
		if (ar.isDeserializing && type == ClearRange && param2 == StringRef() && param1 != StringRef()) {
			ASSERT(param1[param1.size() - 1] == '\x00');
			param2 = param1;
			param1 = param2.substr(0, param2.size() - 1);
		}
	}

	// These masks define which mutation types have particular properties (they are used to implement
	// isSingleKeyMutation() etc)
	enum {
		ATOMIC_MASK = (1 << AddValue) | (1 << And) | (1 << Or) | (1 << Xor) | (1 << AppendIfFits) | (1 << Max) |
		              (1 << Min) | (1 << SetVersionstampedKey) | (1 << SetVersionstampedValue) | (1 << ByteMin) |
		              (1 << ByteMax) | (1 << MinV2) | (1 << AndV2) | (1 << CompareAndClear),
		SINGLE_KEY_MASK = ATOMIC_MASK | (1 << SetValue),
		NON_ASSOCIATIVE_MASK = (1 << AddValue) | (1 << Or) | (1 << Xor) | (1 << Max) | (1 << Min) |
		                       (1 << SetVersionstampedKey) | (1 << SetVersionstampedValue) | (1 << MinV2) |
		                       (1 << CompareAndClear)
	};
};

template <>
struct Traceable<MutationRef> : std::true_type {
	static std::string toString(MutationRef const& value) { return value.toString(); }
};

static inline std::string getTypeString(MutationRef::Type type) {
	return type < MutationRef::MAX_ATOMIC_OP ? typeString[(int)type] : "Unset";
}

static inline std::string getTypeString(uint8_t type) {
	return type < MutationRef::MAX_ATOMIC_OP ? typeString[type] : "Unset";
}

// A 'single key mutation' is one which affects exactly the value of the key specified by its param1
static inline bool isSingleKeyMutation(MutationRef::Type type) {
	return (MutationRef::SINGLE_KEY_MASK & (1 << type)) != 0;
}

// Returns true if the given type can be safely cast to MutationRef::Type and used as a parameter to
// isSingleKeyMutation, isAtomicOp, etc.  It does NOT mean that the type is a valid type of a MutationRef in any
// particular context.
static inline bool isValidMutationType(uint32_t type) {
	return (type < MutationRef::MAX_ATOMIC_OP);
}

// An 'atomic operation' is a single key mutation which sets the key specified by its param1 to a
//   nontrivial function of the previous value of the key and param2, and thus requires a
//   read/modify/write to implement.  (Basically a single key mutation other than a set)
static inline bool isAtomicOp(MutationRef::Type mutationType) {
	return (MutationRef::ATOMIC_MASK & (1 << mutationType)) != 0;
}

// Returns true for operations which do not obey the associative law (i.e. a*(b*c) == (a*b)*c) in all cases
// unless a, b, and c have equal lengths, in which case even these operations are associative.
static inline bool isNonAssociativeOp(MutationRef::Type mutationType) {
	return (MutationRef::NON_ASSOCIATIVE_MASK & (1 << mutationType)) != 0;
}

struct CommitTransactionRef {
	CommitTransactionRef() = default;
	CommitTransactionRef(Arena& a, const CommitTransactionRef& from)
	  : read_conflict_ranges(a, from.read_conflict_ranges), write_conflict_ranges(a, from.write_conflict_ranges),
	    mutations(a, from.mutations), read_snapshot(from.read_snapshot),
	    report_conflicting_keys(from.report_conflicting_keys), lock_aware(from.lock_aware),
	    spanContext(from.spanContext) {}

	VectorRef<KeyRangeRef> read_conflict_ranges;
	VectorRef<KeyRangeRef> write_conflict_ranges;
	VectorRef<MutationRef> mutations; // metadata mutations
	Version read_snapshot = 0;
	bool report_conflicting_keys = false;
	bool lock_aware = false; // set when metadata mutations are present
	Optional<SpanID> spanContext;

	template <class Ar>
	force_inline void serialize(Ar& ar) {
		if constexpr (is_fb_function<Ar>) {
			serializer(ar,
			           read_conflict_ranges,
			           write_conflict_ranges,
			           mutations,
			           read_snapshot,
			           report_conflicting_keys,
			           lock_aware,
			           spanContext);
		} else {
			serializer(ar, read_conflict_ranges, write_conflict_ranges, mutations, read_snapshot);
			if (ar.protocolVersion().hasReportConflictingKeys()) {
				serializer(ar, report_conflicting_keys);
			}
			if (ar.protocolVersion().hasSpanContext()) {
				serializer(ar, lock_aware, spanContext);
			}
		}
	}

	// Convenience for internal code required to manipulate these without the Native API
	void set(Arena& arena, KeyRef const& key, ValueRef const& value) {
		mutations.push_back_deep(arena, MutationRef(MutationRef::SetValue, key, value));
		write_conflict_ranges.push_back(arena, singleKeyRange(key, arena));
	}

	void clear(Arena& arena, KeyRangeRef const& keys) {
		mutations.push_back_deep(arena, MutationRef(MutationRef::ClearRange, keys.begin, keys.end));
		write_conflict_ranges.push_back_deep(arena, keys);
	}

	size_t expectedSize() const {
		return read_conflict_ranges.expectedSize() + write_conflict_ranges.expectedSize() + mutations.expectedSize();
	}
};

struct MutationsAndVersionRef {
	VectorRef<MutationRef> mutations;
	Version version = invalidVersion;
	Version knownCommittedVersion = invalidVersion;

	MutationsAndVersionRef() {}
	explicit MutationsAndVersionRef(Version version, Version knownCommittedVersion)
	  : version(version), knownCommittedVersion(knownCommittedVersion) {}
	MutationsAndVersionRef(VectorRef<MutationRef> mutations, Version version, Version knownCommittedVersion)
	  : mutations(mutations), version(version), knownCommittedVersion(knownCommittedVersion) {}
	MutationsAndVersionRef(Arena& to, VectorRef<MutationRef> mutations, Version version, Version knownCommittedVersion)
	  : mutations(to, mutations), version(version), knownCommittedVersion(knownCommittedVersion) {}
	MutationsAndVersionRef(Arena& to, const MutationsAndVersionRef& from)
	  : mutations(to, from.mutations), version(from.version), knownCommittedVersion(from.knownCommittedVersion) {}
	int expectedSize() const { return mutations.expectedSize(); }

	struct OrderByVersion {
		bool operator()(MutationsAndVersionRef const& a, MutationsAndVersionRef const& b) const {
			return a.version < b.version;
		}
	};

	template <class Ar>
	void serialize(Ar& ar) {
		serializer(ar, mutations, version, knownCommittedVersion);
	}
};

#endif
