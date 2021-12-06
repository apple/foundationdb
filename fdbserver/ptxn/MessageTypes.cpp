/*
 * MessageTypes.cpp
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

#include "fdbserver/ptxn/MessageTypes.h"

namespace ptxn {

Message::Type Message::getType() const {
	return static_cast<Type>(this->index());
}

std::string Message::toString() const {
	std::string result;

	switch (getType()) {
	case Type::MUTATION_REF:
		result = concatToString(result, "Mutation     ", std::get<MutationRef>(*this));
		break;
	case Type::SPAN_CONTEXT_MESSAGE:
		result = concatToString(result, "Span Context ", std::get<SpanContextMessage>(*this));
		break;
	case Type::LOG_PROTOCOL_MESSAGE:
		result = concatToString(result, "Log Protocol ", std::get<LogProtocolMessage>(*this));
		break;
	case Type::EMPTY_VERSION_MESSAGE:
		result = concatToString(result, "Empty version ", std::get<EmptyMessage>(*this));
		break;
	case Type::UNDEFINED:
		result = concatToString(result, "Undefined value");
	default:
		UNREACHABLE();
	}

	return result;
}

bool Message::operator==(const Message& another) const {
	if (getType() != another.getType()) {
		return false;
	}
	switch (getType()) {
	case Type::MUTATION_REF:
		return std::get<MutationRef>(*this) == std::get<MutationRef>(another);
	case Type::SPAN_CONTEXT_MESSAGE:
		return std::get<SpanContextMessage>(*this) == std::get<SpanContextMessage>(another);
	case Type::LOG_PROTOCOL_MESSAGE:
		return std::get<LogProtocolMessage>(*this) == std::get<LogProtocolMessage>(another);
	case Type::EMPTY_VERSION_MESSAGE:
		return std::get<EmptyMessage>(*this) == std::get<EmptyMessage>(another);
	case Type::UNDEFINED:
		return true;
	default:
		UNREACHABLE();
	}
}

bool Message::operator!=(const Message& another) const {
	return !(*this == another);
}

VersionSubsequenceMessage::VersionSubsequenceMessage(const Version& version_, const Subsequence& subsequence_)
  : version(version_), subsequence(subsequence_) {}

VersionSubsequenceMessage::VersionSubsequenceMessage(const Version& version_,
                                                     const Subsequence& subsequence_,
                                                     const Message& message_)
  : version(version_), subsequence(subsequence_), message(message_) {}

std::string VersionSubsequenceMessage::toString() const {
	std::string result = concatToString("Version ", version, "\tSubsequence ", subsequence, "\t", message);
	return result;
}

bool VersionSubsequenceMessage::operator==(const VersionSubsequenceMessage& another) const {
	return operatorSpaceship(another) == 0;
}

bool VersionSubsequenceMessage::operator!=(const VersionSubsequenceMessage& another) const {
	return operatorSpaceship(another) != 0;
}

bool VersionSubsequenceMessage::operator<(const VersionSubsequenceMessage& another) const {
	return operatorSpaceship(another) == -1;
}

bool VersionSubsequenceMessage::operator>(const VersionSubsequenceMessage& another) const {
	return operatorSpaceship(another) == 1;
}

bool VersionSubsequenceMessage::operator<=(const VersionSubsequenceMessage& another) const {
	return operatorSpaceship(another) <= 0;
}

bool VersionSubsequenceMessage::operator>=(const VersionSubsequenceMessage& another) const {
	return operatorSpaceship(another) >= 0;
}

int VersionSubsequenceMessage::operatorSpaceship(const VersionSubsequenceMessage& another) const {
	if (version < another.version) {
		return -1;
	} else if (version > another.version) {
		return 1;
	}

	if (subsequence < another.subsequence) {
		return -1;
	} else if (subsequence > another.subsequence) {
		return 1;
	}

	if (message.getType() == another.message.getType() && message == another.message) {
		return 0;
	}

	// Two different messages cannot share one single (version, subsequnece) pair.
	ASSERT(false);

	// This return will not be reachable, but to mute the non-void function does not return a value warning.
	return 0;
}

std::ostream& operator<<(std::ostream& stream, const Message& message) {
	stream << concatToString(message);
	return stream;
}

std::ostream& operator<<(std::ostream& stream, const VersionSubsequenceMessage& vsm) {
	stream << concatToString(vsm);
	return stream;
}

} // namespace ptxn