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

// Stores the mutation and its subsequence, or the relative order of each item. The order is used in recovery and
// restoring from backups. This, and the following Subsequence***Item are used for serialization
struct SubsequenceMutationItem {
	Subsequence subsequence;
	MutationRef mutation;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, subsequence, mutation);
	}
};

// Stores the span context and its subsequence.
// See the comments for SubsequenceMutationItem
struct SubsequenceSpanContextItem {
	Subsequence subsequence;
	SpanContextMessage spanContext;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, subsequence, spanContext);
	}
};

// Stores the log protocol message and its subsequence.
// See the comments for SubsequenceMutationItem
struct SubsequenceLogProtocolMessageItem {
	Subsequence subsequence;
	LogProtocolMessage logProtocolMessage;

	template <typename Ar>
	void serialize(Ar& ar) {
		serializer(ar, subsequence, logProtocolMessage);
	}
};

// Stores a pre-serialized message and its subsequence.
// See the comments for SubsequenceMutationItem
struct SubsequenceSerializedMessageItem {
	Subsequence subsequence;
	StringRef serializedMessage;

	template <typename Ar>
	void serialize(Ar& ar) {
		static_assert(!Ar::isDeserializing, "Only support serialization");
		serializer(ar, subsequence);
		// For StringRef, standard serialization will add a length information. The deserializer will *NOT* be able to
		// recognize such information. In this case, serializeBytes is used instead of the default StringRef serializer.
		ar.serializeBytes(serializedMessage);
	}
};

// Placeholder message, used to notify an version w/o any messages inside it
struct EmptyMessage {
	// All empty messages are the same, the version difference is checked at VersionSubsequenceMessage
	bool operator==(const EmptyMessage&) const { return true; }

	// All empty messages are the same, the version difference is checked at VersionSubsequenceMessage
	bool operator!=(const EmptyMessage&) const { return false; }

	std::string toString() const { return std::string("<EmptyMessage>"); }
};

// Stores one of the following:
//    * MutationRef
//    * SpanContextMessage
//    * LogProtocolMessage
// .  * EmptyMessage
struct Message : public std::variant<MutationRef, SpanContextMessage, LogProtocolMessage, EmptyMessage> {
	using variant_t = std::variant<MutationRef, SpanContextMessage, LogProtocolMessage, EmptyMessage>;

	enum class Type : std::size_t {
		MUTATION_REF,
		SPAN_CONTEXT_MESSAGE,
		LOG_PROTOCOL_MESSAGE,
		EMPTY_VERSION_MESSAGE,
		UNDEFINED = std::variant_npos
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
	Version version = invalidVersion;
	Subsequence subsequence = invalidSubsequence;
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
