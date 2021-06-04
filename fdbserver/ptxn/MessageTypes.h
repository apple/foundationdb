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
#include <variant>

#include "fdbclient/CommitTransaction.h"
#include "fdbserver/LogProtocolMessage.h"
#include "fdbserver/SpanContextMessage.h"

namespace ptxn {

// Stores the mutation and their subsequences, or the relative order of each item. The order is used in recovery and
// restoring from backups. This, and the following Subsequence***Item are used for serialization
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

// Stores the span context and their subsequences.
// See the comments for SubsequenceMutationItem
struct SubsequenceSpanContextItem {
	Subsequence subsequence;
	SpanContextMessage spanContext;

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> subsequence >> spanContext;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, subsequence, spanContext);
	}
};

// Stores the log protocol message and their subsequences.
// See the comments for SubsequenceMutationItem
struct SubsequenceLogProtocolMessageItem {
	Subsequence subsequence;
	LogProtocolMessage logProtocolMessage;

	template <typename Reader>
	void loadFromArena(Reader& reader) {
		reader >> subsequence >> logProtocolMessage;
	}

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, subsequence, logProtocolMessage);
	}
};

// Stores one of the following:
//    * MutationRef
//    * SpanContextMessage
//    * LogProtocolMessage
struct Message : public std::variant<MutationRef, SpanContextMessage, LogProtocolMessage> {
	using variant_t = std::variant<MutationRef, SpanContextMessage, LogProtocolMessage>;

	enum class Type {
		MUTATION_REF,
		SPAN_CONTEXT_MESSAGE,
		LOG_PROTOCOL_MESSAGE
	};

	using variant_t::variant_t;
	using variant_t::operator=;

	// Alias to std::variant::index()
	Type getType() const;

	std::string toString() const;

	bool operator==(const Message&) const;
	bool operator!=(const Message&) const;
};

std::ostream& operator<<(std::ostream&, const Message&);

// Stores the
//    Version - Subsequence - Message
// tuple. The message is having the format Message and can store different type of objects. See the comment of Message.
struct VersionSubsequenceMessage {
	Version version;
	Subsequence subsequence;
	Message message;

	VersionSubsequenceMessage() = default;
	VersionSubsequenceMessage(const Version&, const Subsequence&);
	VersionSubsequenceMessage(const Version&, const Subsequence&, const Message&);

	std::string toString() const;

	bool operator==(const VersionSubsequenceMessage&) const;
	bool operator!=(const VersionSubsequenceMessage&) const;
};

std::ostream& operator<<(std::ostream&, const VersionSubsequenceMessage&);

} // namespace ptxn

#endif // FDBSERVER_PTXN_MESSAGETYPES_H
