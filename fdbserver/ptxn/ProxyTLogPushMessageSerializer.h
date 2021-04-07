/*
 * ProxyTLogPushMessageSerializer.h
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

#ifndef FDBSERVER_PTXN_PROXYTLOGPUSHMESSAGESERIALIZER_H
#define FDBSERVER_PTXN_PROXYTLOGPUSHMESSAGESERIALIZER_H

#pragma once

#include <cstdint>
#include <unordered_map>

#include "fdbclient/CommitTransaction.h"
#include "fdbserver/ptxn/Serializer.h"
#include "flow/Error.h"

namespace ptxn {

const uint8_t ProxyTLogMessageProtocolVersion = 1;

/**
 * When passing a series of mutations, or a commit, from Proxy to TLog, the ProxyTLogMessageHeader
 * is prefixed to the mutations.
 */
struct ProxyTLogMessageHeader {
	static constexpr FileIdentifier file_identifier = 356918;

	// The version of the protocol
	uint8_t protocolVersion = ProxyTLogMessageProtocolVersion;

	// Number of items
	size_t numItems;

	// The raw length, i.e. the number of bytes, in this message, excluding the header
	size_t length;

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> protocolVersion >> numItems >> length;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, protocolVersion, numItems, length);
	}
};

/**
 * Stores the mutations and their subsequences, or the relative order of each mutations. The order
 * is used in recovery.
 */
struct SubsequenceMutationItem {
	uint32_t subsequence;
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

// Encodes the mutations that commit proxy have received, for TLog's consumption
class ProxyTLogPushMessageSerializer {
	// Maps the TeamID to the list of BinaryWriters
	std::unordered_map<TeamID, HeaderedItemsSerializerBase<ProxyTLogMessageHeader, SubsequenceMutationItem>> writers;

	// Subsequence of the mutation
	// NOTE: The subsequence is designed to sart at 1, as in the TagPartitionLogSystem
	// the cursors requires this (see comments in LogSystem.h:LogPushData), we
	// follow the custom.
	uint32_t currentSubsequence = 1;

public:
	// For a given TeamID, serialize a new mutation
	void writeMessage(const MutationRef& mutation, const TeamID& teamID);

	// For a given TeamID, mark the serializer not accepting more mutations, and write the header.
	void completeMessageWriting(const TeamID& teamID);

	// Get the serialized data for a given TeamID
	Standalone<StringRef> getSerialized(const TeamID& teamID);
};

// Deserialize the ProxyTLogPushMessage
bool proxyTLogPushMessageDeserializer(const Arena& arena,
                                      StringRef serialized,
                                      ProxyTLogMessageHeader& header,
                                      std::vector<SubsequenceMutationItem>& messages);

} // namespace ptxn

#endif // FDBSERVER_PTXN_PROXYTLOGPUSHMESSAGESERIALIZER_H
