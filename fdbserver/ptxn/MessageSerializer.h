/*
 * MessageSerializer.h
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
#include <unordered_set>
#include <utility>

#include "fdbserver/ptxn/MessageTypes.h"
#include "fdbserver/ptxn/Serializer.h"
#include "flow/Error.h"

namespace ptxn {

const SerializationProtocolVersion MessageSerializationProtocolVersion = 1;

namespace details {
// When passing a series of mutations, from TLog to StorageServer, the MessageHeader
// is prefixed to the mutations.
struct MessageHeader : MultipleItemHeaderBase {
	static constexpr FileIdentifier file_identifier = 617401;

	// TeamID
	StorageTeamID storageTeamID;

	// The first version that being serialized
	Version firstVersion = invalidVersion;

	// The last version that being serialized
	Version lastVersion = invalidVersion;

	// The last subsequence that being serialized
	Subsequence lastSubsequence = 0;

	MessageHeader() : MultipleItemHeaderBase(MessageSerializationProtocolVersion) {}

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		MultipleItemHeaderBase::loadFromArena(reader);
		reader >> storageTeamID >> firstVersion >> lastVersion >> lastSubsequence;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		MultipleItemHeaderBase::serialize(ar);
		serializer(ar, storageTeamID, firstVersion, lastVersion, lastSubsequence);
	}
};

struct SubsequencedItemsHeader : MultipleItemHeaderBase {
	static constexpr FileIdentifier file_identifier = 340226;

	// The version of the following mutations
	Version version;

	SubsequencedItemsHeader() : MultipleItemHeaderBase(MessageSerializationProtocolVersion) {}

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

} // namespace details

// Encodes the subsequence/mutations pair, grouped by the version. The format of serialized data would look like
//
//    | Header | V(1)Data | V(2)Data | ...
//
// where the Header is defined in MessageHeader. The V(n)Data, or version section, is serialized data in the format of:
//
//    | VersionHeader | Subsequence/Item | Subsequence/Item | ...
//
// where the VersionHeader is defined in SubsequencedItemsHeader. Each item can be an
//
//    * MutationRef
//    * SpanContext
//    * LogProtocolMessage
//
// In the serialized data, the versions are strictly increasing ordered. And the subsequence with for a given version is
// also strictly increasing ordered.
class SubsequencedMessageSerializer {
private:
	// The serializer that generates the final output
	TwoLevelHeaderedItemsSerializer<details::MessageHeader, details::SubsequencedItemsHeader> serializer;

	// The header of the whole message
	details::MessageHeader header;

public:
	SubsequencedMessageSerializer(const StorageTeamID&);

	// Starts to write a new version of mutations.
	void startVersionWriting(const Version& version);

	// Writes a mutation to the serializer
	void write(const SubsequenceMutationItem&);

	// Writes a mutation to the serializer
	void write(const Subsequence&, const MutationRef&);

	// Writes a SpanContext to the serializer
	void write(const SubsequenceSpanContextItem&);

	// Writes a SpanContext to the serializer
	void write(const Subsequence&, const SpanContextMessage&);

	// Writes a LogProtocolMessage to the serializer
	void write(const SubsequenceLogProtocolMessageItem&);

	// Writes a LogProtocolMessage to the serializer
	void write(const Subsequence&, const LogProtocolMessage&);

	// Writes a Message to the serializer
	void write(const Subsequence&, const Message&);

	// Writes a serialized message to the serializer
	void write(const SubsequenceSerializedMessageItem&);

	// Writes a serialized message to the serializer
	void write(const Subsequence&, StringRef);

	// Gets the current version being written
	const Version& getCurrentVersion() const;

	// Gets the current subsequence
	const Subsequence& getCurrentSubsequence() const;

	// Completes writing a set of mutations of the current version
	void completeVersionWriting();

	// Updates the header and mark the serialization complete
	void completeMessageWriting();

	// Total bytes of serialized data
	size_t getTotalBytes() const;

	// Gets the serialized data for a given TeamID
	Standalone<StringRef> getSerialized();
};

// Wrapper of multiple StorageTeamID <-> SubsequencedMessageSerializer
// Each serializer servers a single storage team ID. The serializer will only seriaize one version
// (SubsequencedMessageSerializer supports serializing multiple versions).
// This is useful for CommitProxies to serialize one commit into multiple storage team IDs.
class ProxySubsequencedMessageSerializer {
private:
	// Mapper between StorageTeamID and SubsequencedMessageSerializer
	std::unordered_map<StorageTeamID, SubsequencedMessageSerializer> serializers;

	// Subsequence of the mutation
	// NOTE: The subsequence is designed to start at 1. This allows a cursor,  which initialized at subsequence 0, not
	// positioned at a mutation. This simplifies the implementation of iteration. e.g. for a given TeamID and a given
	// version
	//
	//  Subversion      1    3    6    7   ...
	//              ^ cursor starts here, thus we can write
	//  while(pCursor->hasMessage()) pCursor->getMessage();
	//
	// If the currentSubsequence starts at 0, we have to verify if the initial cursor is located at a mutation, or
	// located at end-of-subsequences, bringing extra complexity.
	//
	// This is the sequence by using unsigned integer as subsequence in the old code.
	Subsequence subsequence = 1;

	// SpanContextMessage to be broadcasted
	Optional<SpanContextMessage> spanContextMessage;

	// Records all storage teams that has injected the current span context message.
	std::unordered_set<StorageTeamID> storageTeamInjectedSpanContext;

	// Tries to inject the span context to the serializer with given storage team ID
	void tryInjectSpanContextMessage(const StorageTeamID& storageTeamID);

	// Stores the version of the commit
	const Version version;

	// Prepares writing a message:
	//     * Create a new StorageTeam serializer if necessary.
	//     * Write SpanContextMessage if exists/not preposed.
	void prepareWriteMessage(const StorageTeamID& storageTeamID);

public:
	explicit ProxySubsequencedMessageSerializer(const Version&);

	// Gets the version the serializer is currently using
	const Version& getVersion() const;

	// Broadcasts the span context to all storage teams. After this function is called, for any storage team, the first
	// write will always prepose this SpanContextMessage before the mutation.
	void broadcastSpanContext(const SpanContextMessage&);

	// Writes a mutation to a given stoarge team.
	void write(const MutationRef&, const StorageTeamID&);

	// Writes an already serialized message to a given storage team.
	void write(const StringRef&, const StorageTeamID&);

	// Writes a mutation to multiple storage teams.
	// storageTeams is a container that supports input iterator, and contains all storage team IDs that should receive
	// the mutation.
	template <typename Container_t>
	void write(const MutationRef& mutation, const Container_t& storageTeamIDs) {
		for (const auto& storageTeamID : storageTeamIDs) {
			write(mutation, storageTeamID);
		}
	}

	// Get serialized data for a given storage team ID
	StringRef getSerialized(const StorageTeamID& storageTeamID);

	// Get all serialized data
	std::unordered_map<StorageTeamID, Standalone<StringRef>> getAllSerialized();
};

template <typename T>
using ConstInputIteratorBase = std::iterator<std::input_iterator_tag, T, size_t, const T* const, const T&>;

class SubsequencedMessageDeserializer {
private:
	Arena serializedArena;
	StringRef serialized;

	using DeserializerImpl =
	    TwoLevelHeaderedItemsDeserializer<details::MessageHeader, details::SubsequencedItemsHeader>;

	// Header of the deserialized data
	details::MessageHeader header;

public:
	class iterator : public ConstInputIteratorBase<VersionSubsequenceMessage> {
	private:
		friend class SubsequencedMessageDeserializer;

		DeserializerImpl deserializer;

		// We keep a pointer to the original serialized data, so when we compare two iterators, we knows they are not
		// the same if they are pointing to two different seralized.
		StringRef rawSerializedData;

		// The header of the deserialized data, it is small so we hold a local copy
		details::MessageHeader header;

		// The header of current version section
		details::SubsequencedItemsHeader versionHeader;

		// The index of current section
		int32_t sectionIndex;

		// The index of the *NEXT* item in the current version section
		int32_t itemIndex;

		// Store the deserialized data
		VersionSubsequenceMessage currentItem;

		// serializedArena_ is the arena that used to store the serialized data
		// serialized_ refers to the serialized data
		// If isEndIterator, then the iterator indicates the end of the serialized data. The behavior of dereferencing
		// the iterator is undefined.
		iterator(const Arena& serializedArena_, StringRef serialized_, bool isEndIterator = false);

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

private:
	iterator endIterator;

public:
	using const_iterator = iterator;

	SubsequencedMessageDeserializer(const Standalone<StringRef>& serialized_);

	// serializedArena_ is the arena that used to store the serialized data
	// serialized_ refers to the serialized data
	SubsequencedMessageDeserializer(const Arena& serializedArena_, const StringRef serialized_);

	// Resets the deserializer with new arena and StringRef, see comments in constructor
	// NOTE: All iterators will be invalidated after reset.
	void reset(const Arena&, const StringRef);

	// Gets the team ID
	const StorageTeamID& getStorageTeamID() const;

	// Gets the number of different versions in this part
	size_t getNumVersions() const;

	// Gets the first version in this serialized message
	const Version& getFirstVersion() const;

	// Gets the last version in this serialized message
	const Version& getLastVersion() const;

	iterator begin() const;
	// end() is called multiple times in typical for loop:
	//    for(auto iter = deserializer.begin(); iter != deserializer.end(); ++iter)
	// since creating an iterator is *NOT* trivial, the end iterator is cached.
	const iterator& end() const;

	const_iterator cbegin() const;
	const const_iterator& cend() const;
};

} // namespace ptxn

#endif // FDBSERVER_PTXN_TLOGSTORAGESERVERPEEKMESSAGESERIALIZER_H
