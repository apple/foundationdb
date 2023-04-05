/*
 * LogProtocolMessage.h
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

#ifndef FDBSERVER_LOGPROTOCOLMESSAGE_H
#define FDBSERVER_LOGPROTOCOLMESSAGE_H
#pragma once

#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"

template <class Ar, class VersionOptions>
typename Ar::READER& applyVersionStartingHere(Ar& ar, VersionOptions vo) {
	vo.read(ar);
	return ar;
}

template <class Ar, class VersionOptions>
typename Ar::WRITER& applyVersionStartingHere(Ar& ar, VersionOptions vo) {
	vo.write(ar);
	return ar;
}

struct LogProtocolMessage {
	// This message is pushed into the log system tag for each storage server to inform it what protocol version
	// should be used to deserialize subsequent MutationRefs.

	// It's legitimate to add extra information here (using the protocol version for backward compatibility) but
	// currently there is none.

	// This mechanism passes various ASSERTs in simulation, but has never been used in anger (because MutationRef's
	// serialization format has never changed) so think about testing when it does change.

	// Storage servers merging these messages with MutationRefs need to distinguish the two, so the first byte of
	// the serialization of this message is a type code which is reserved in the MutationRef::Type enum and will thus
	// never be the first byte of a MutationRef message.

	LogProtocolMessage() {}

	std::string toString() const { return format("code: %d", MutationRef::Reserved_For_LogProtocolMessage); }

	template <class Ar>
	void serialize(Ar& ar) {
		uint8_t poly = MutationRef::Reserved_For_LogProtocolMessage;
		serializer(ar, poly);
		applyVersionStartingHere(ar, IncludeVersion());
	}

	static bool startsLogProtocolMessage(uint8_t byte) { return byte == MutationRef::Reserved_For_LogProtocolMessage; }
	template <class Ar>
	static bool isNextIn(Ar& ar) {
		return startsLogProtocolMessage(*(const uint8_t*)ar.peekBytes(1));
	}
};

#endif
