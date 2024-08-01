/*
 * OTELSpanContextMessage.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_OTELSPANCONTEXTMESSAGE_H
#define FDBSERVER_OTELSPANCONTEXTMESSAGE_H
#pragma once

#include "fdbclient/Tracing.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/CommitTransaction.h"

struct OTELSpanContextMessage {
	// This message is pushed into the the transaction logs' memory to inform
	// it what transaction subsequent mutations were a part of. This allows
	// transaction logs and storage servers to associate mutations with a
	// transaction identifier, called a span context.
	//
	// This message is similar to LogProtocolMessage. Storage servers read the
	// first byte of this message to uniquely identify it, meaning it will
	// never be mistaken for another message. See LogProtocolMessage.h for more
	// information.

	SpanContext spanContext;

	OTELSpanContextMessage() {}
	OTELSpanContextMessage(SpanContext const& spanContext) : spanContext(spanContext) {}

	std::string toString() const {
		return format("code: %d, span context: %s",
		              MutationRef::Reserved_For_OTELSpanContextMessage,
		              spanContext.toString().c_str());
	}

	template <class Ar>
	void serialize(Ar& ar) {
		uint8_t poly = MutationRef::Reserved_For_OTELSpanContextMessage;
		serializer(ar, poly, spanContext);
	}

	static bool startsOTELSpanContextMessage(uint8_t byte) {
		return byte == MutationRef::Reserved_For_OTELSpanContextMessage;
	}
	template <class Ar>
	static bool isNextIn(Ar& ar) {
		return startsOTELSpanContextMessage(*(const uint8_t*)ar.peekBytes(1));
	}
};

#endif
