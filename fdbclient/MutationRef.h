/*
 * MutationRef.h
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

#ifndef FDBCLIENT_MUTATIONREF_H
#define FDBCLIENT_MUTATIONREF_H
#pragma once

#include "flow/Arena.h"
#include "fdbserver/Knobs.h"
#include <cstdint>

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
	                                "CompareAndClear"};

struct MutationRef {
	static const int OVERHEAD_BYTES = 12; //12 is the size of Header in MutationList entries
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
		MAX_ATOMIC_OP
	};
	// This is stored this way for serialization purposes.
	uint8_t type;
	StringRef param1, param2;

	MutationRef() {}
	MutationRef( Type t, StringRef a, StringRef b ) : type(t), param1(a), param2(b) {}
	MutationRef( Arena& to, const MutationRef& from ) : type(from.type), param1( to, from.param1 ), param2( to, from.param2 ) {}
	int totalSize() const { return OVERHEAD_BYTES + param1.size() + param2.size(); }
	int expectedSize() const { return param1.size() + param2.size(); }
	int weightedTotalSize() const {
		// AtomicOp can cause more workload to FDB cluster than the same-size set mutation;
		// Amplify atomicOp size to consider such extra workload.
		// A good value for FASTRESTORE_ATOMICOP_WEIGHT needs experimental evaluations.
		if (isAtomicOp()) {
			return totalSize() * SERVER_KNOBS->FASTRESTORE_ATOMICOP_WEIGHT;
		} else {
			return totalSize();
		}
	}

	std::string toString() const {
		if (type < MutationRef::MAX_ATOMIC_OP) {
			return format("code: %s param1: %s param2: %s", typeString[type], printable(param1).c_str(), printable(param2).c_str());
		}
		else {
			return format("code: Invalid param1: %s param2: %s", printable(param1).c_str(), printable(param2).c_str());
		}
	}

	bool isAtomicOp() const { return (ATOMIC_MASK & (1 << type)) != 0; }

	template <class Ar>
	void serialize( Ar& ar ) {
		serializer(ar, type, param1, param2);
	}

	// These masks define which mutation types have particular properties (they are used to implement isSingleKeyMutation() etc)
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

#endif
