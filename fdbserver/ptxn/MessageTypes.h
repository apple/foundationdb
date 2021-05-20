/*
 * MessageTypes.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_PTXN_MESSAGETYPES_H
#define FDBSERVER_PTXN_MESSAGETYPES_H

#pragma once

#include <ostream>
#include <typeindex>
#include <variant>

#include "fdbclient/CommitTransaction.h"
#include "fdbserver/SpanContextMessage.h"
#include "flow/ObjectSerializerTraits.h"
#include "flow/serialize.h"

namespace ptxn {

struct VersionSubsequence {
	Version version = 0;
	Subsequence subsequence = 0;

	explicit VersionSubsequence(const Version& version_, const Subsequence& subsequence_)
	  : version(version_), subsequence(subsequence_) {}

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> version >> subsequence;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, version, subsequence);
	}
};

// Stores the mutations and their subsequences, or the relative order of each mutations.
// The order is used in recovery and restoring from backups.
struct SubsequenceMutationItem {
	Subsequence subsequence;
	// The item can be a mutation in MutationRef or a serialized StringRef.
	// However, the item can also be a SpanContextMessage.
	// When deserialized, we always use the MutationRef format for mutations.
	std::variant<MutationRef, StringRef, SpanContextMessage> item_;

	// Returns mutation in MutationRef format after deserialization.
	const MutationRef& mutation() const {
		ASSERT(isMutation());
		return std::get<MutationRef>(item_);
	}

	// Returns SpanContextMessage after deserialization.
	const SpanContextMessage& span() const {
		ASSERT(item_.index() == 2);
		return std::get<SpanContextMessage>(item_);
	}

	// Returns if the item is a mutation (MutationRef or StringRef)
	bool isMutation() const { return item_.index() <= 1; }

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> subsequence;
		if (SpanContextMessage::isNextIn(reader)) {
			SpanContextMessage span;
			reader >> span;
			item_ = span;
		} else {
			MutationRef m;
			reader >> m;
			item_ = m;
		}
	}

	// Ideally, this should be the default one. However, we want to deserialize
	// to either MutationRef or SpanContextMessage. So it's handled by specialized
	// serialize() templates.
	template <typename Ar>
	void serializeImpl(Ar& ar) {
		if (item_.index() == 0) {
			serializer(ar, subsequence, std::get<MutationRef>(item_));
		} else if (item_.index() == 1) {
			serializer(ar, subsequence);
			auto& bytes = std::get<StringRef>(item_);
			ar.serializeBytes(bytes);
		} else if (item_.index() == 2) {
			serializer(ar, subsequence, std::get<SpanContextMessage>(item_));
		} else {
			UNREACHABLE();
		}
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		if (ar.isDeserializing) {
			serializer(ar, subsequence);
			if (SpanContextMessage::isNextIn(ar)) {
				SpanContextMessage span;
				serializer(ar, span);
				item_ = span;
			} else {
				MutationRef m;
				serializer(ar, m);
				item_ = m;
			}
		} else {
			serializeImpl(ar);
		}
	}

	// Specialize for ArenaReader.
	void serialize(ArenaReader& ar) { loadFromArena(ar); }

	// Specialize for BinaryWriter.
	void serialize(BinaryWriter& ar) { serializeImpl(ar); }
};

// Stores the
//    Version - Subsequence - Mutation
// tuple
struct VersionSubsequenceMutation {
	Version version;
	Subsequence subsequence;
	MutationRef mutation;

	VersionSubsequenceMutation();
	VersionSubsequenceMutation(const Version&, const Subsequence&, const MutationRef&);

	bool operator==(const VersionSubsequenceMutation& another) const;
	bool operator!=(const VersionSubsequenceMutation& another) const;

	std::string toString() const;
};

std::ostream& operator<<(std::ostream&, const VersionSubsequenceMutation&);

} // namespace ptxn

#endif // FDBSERVER_PTXN_MESSAGETYPES_H
