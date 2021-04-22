/*
 * TLogStorageServerPeekMessageSerializer.h
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

#ifndef FDBSERVER_PTXN_TLOGSTORAGESERVERPEEKMESSAGESERIALIZER_H
#define FDBSERVER_PTXN_TLOGSTORAGESERVERPEEKMESSAGESERIALIZER_H

#pragma once

#include <cstdint>
#include <unordered_map>
#include <utility>

#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/Serializer.h"
#include "flow/Error.h"

namespace ptxn {

const SerializationProtocolVersion TLogStorageServerPeekMessageProtocolVersion = 1;

// When passing a series of mutations, from TLog to StorageServer, the TLogStorageServerMessageHeader
// is prefixed to the mutations.
struct TLogStorageServerMessageHeader : MultipleItemHeaderBase {
	static constexpr FileIdentifier file_identifier = 617401;

	// TeamID
	TeamID teamID;

	// The first version that being serialized
	Version firstVersion = invalidVersion;

	// The last version that being serialized
	Version lastVersion = invalidVersion;

	// The last subsequence that being serialized
	Subsequence lastSubsequence = 0;

	TLogStorageServerMessageHeader() : MultipleItemHeaderBase(TLogStorageServerPeekMessageProtocolVersion) {}

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		MultipleItemHeaderBase::loadFromArena(reader);
		reader >> teamID >> firstVersion >> lastVersion >> lastSubsequence;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		MultipleItemHeaderBase::serialize(ar);
		serializer(ar, teamID, firstVersion, lastVersion, lastSubsequence);
	}
};

struct SubsequenceMutationItemsHeader : MultipleItemHeaderBase {
	static constexpr FileIdentifier file_identifier = 340226;

	// The version of the following mutations
	Version version;

	SubsequenceMutationItemsHeader() : MultipleItemHeaderBase(TLogStorageServerPeekMessageProtocolVersion) {}

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		MultipleItemHeaderBase::loadFromArena(reader);
		reader >> version;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		MultipleItemHeaderBase::serialize(ar);
		serializer(ar, version);
	}
};

// Encodes the subsequence/mutations pair, grouped by the version. The format of
// serialized data would look like
//
//    | Header | V(1)Data | V(2)Data | ...
//
// where the Header is defined in TLogStorageServerMessageHeader. The V(n)Data,
// or version section, is serialized data in the format of:
//
//    | VersionHeader | Subsequence/MutationRef | Subsequence/MutationRef | ...
//
// where the VersionHeader is defined in SubsequenceMutationItemsHeader.
//
// In the serialized data, the versions are strictly increasing ordered. And the
// subsequence with for a given version is also strictly increasing ordered.
class TLogStorageServerMessageSerializer {

private:
	// The serializer that generates the final output
	TwoLevelHeaderedItemsSerializer<TLogStorageServerMessageHeader,
	                                SubsequenceMutationItemsHeader,
	                                SubsequenceMutationItem>
	    serializer;

	// The header of the whole message
	TLogStorageServerMessageHeader header;

public:
	TLogStorageServerMessageSerializer(const TeamID&);

	// Start to write a new version of mutations.
	void startVersionWriting(const Version& version);

	// Write a subsequence/MutationRef pair
	void writeSubsequenceMutationRef(const SubsequenceMutationItem& item);

	// Write a subseqeunce/MutationRef pair
	void writeSubsequenceMutationRef(const Subsequence& subsequence, const MutationRef& mutationRef);

	// Get the current version being written
	const Version& getCurrentVersion() const;

	// Get the current subsequence
	const Subsequence& getCurrentSubsequence() const;

	// Complete writing a set of mutations of the current version
	void completeVersionWriting();

	// Update the header and mark the serialization complete
	void completeMessageWriting();

	// Total bytes of serialized data
	size_t getTotalBytes() const;

	// Get the serialized data for a given TeamID
	Standalone<StringRef> getSerialized();
};

template <typename T>
using ConstInputIteratorBase = std::iterator<std::input_iterator_tag, T, size_t, const T* const, const T&>;

class TLogStorageServerMessageDeserializer {
private:
	Arena serializedArena;
	StringRef serialized;

	using DeserializerImpl = TwoLevelHeaderedItemsDeserializer<TLogStorageServerMessageHeader,
	                                                           SubsequenceMutationItemsHeader,
	                                                           SubsequenceMutationItem>;

	// Header of the deserialized data
	TLogStorageServerMessageHeader header;

public:
	class iterator : public ConstInputIteratorBase<VersionSubsequenceMutation> {
	private:
		friend class TLogStorageServerMessageDeserializer;

		DeserializerImpl deserializer;

		// We keep a pointer to the original serialized data, so when we compare two iterators, we knows they are not
		// the same if they are pointing to two different seralized.
		StringRef rawSerializedData;

		// THe header of the deserialized data, it is small so we hold a local copy
		TLogStorageServerMessageHeader header;

		// The header of current version section
		SubsequenceMutationItemsHeader versionHeader;

		// The index of current section
		int32_t sectionIndex;
		// The index of the *NEXT* item in the current version section
		int32_t itemIndex;

		// Store the deserialized data
		VersionSubsequenceMutation currentItem;

		// serializedArena_ is the arena that used to store the serialized data
		// serialized_ refers to the serialized data
		// If isEndIterator, then the iterator indicates the end of the serialized data. The behavior of dereferencing
		// the iterator is undefined.
		iterator(const Arena& serializedArena_,
		         StringRef serialized_,
		         bool isEndIterator = false);

	public:
		bool operator==(const iterator& another) const;
		bool operator!=(const iterator& another) const;

		reference operator*() const;
		pointer operator->() const;

		// Prefix operator++
		iterator& operator++();

		// Postfix operator++, this is more expensive and should be avoided.
		iterator operator++(int);
	};

	using const_iterator = iterator;

	TLogStorageServerMessageDeserializer(const Standalone<StringRef>& serialized_);

	// serializedArena_ is the arena that used to store the serialized data
	// serialized_ refers to the serialized data
	TLogStorageServerMessageDeserializer(const Arena& serializedArena_, const StringRef serialized_);

	// Reset the deserializer with new arena and StringRef, see comments in constructor
	// NOTE: All iterators will be invalidated after reset.
	void reset(const Arena&, const StringRef);

	// Get the team ID
	const TeamID& getTeamID() const;

	// Get the number of different versions in this part
	size_t getNumVersions() const;

	// Get the first version in this serialized message
	const Version& getFirstVersion() const;

	// Get the last version in this serialized message
	const Version& getLastVersion() const;

	iterator begin() const;
	iterator end() const;
	const_iterator cbegin() const;
	const_iterator cend() const;
};

} // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGSTORAGESERVERPEEKMESSAGESERIALIZER_H
