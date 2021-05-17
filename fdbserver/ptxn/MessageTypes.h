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

#include "fdbclient/CommitTransaction.h"

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

// Stores the mutations and their subsequences, or the relative order of each mutations. The order
// is used in recovery.
struct SubsequenceMutationItem {
	Subsequence subsequence;
	MutationRef mutation;

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> subsequence >> mutation;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, subsequence, mutation);
	}
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