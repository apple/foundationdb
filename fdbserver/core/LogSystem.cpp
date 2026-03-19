/*
 * LogSystem.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/core/LogSystem.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/core/OTELSpanContextMessage.h"
#include "fdbserver/core/SpanContextMessage.h"
#include "flow/serialize.h"

LogPushData::LogPushData(Reference<ILogSystem> logSystem, int tlogCount) : logSystem(logSystem), subsequence(1) {
	ASSERT(tlogCount > 0);
	messagesWriter.reserve(tlogCount);
	for (int i = 0; i < tlogCount; i++) {
		messagesWriter.emplace_back(AssumeVersion(g_network->protocolVersion()));
	}
	messagesWritten = std::vector<bool>(tlogCount, false);
}

void LogPushData::addTxsTag() {
	next_message_tags.push_back(logSystem->getRandomTxsTag());
}

void LogPushData::addTransactionInfo(SpanContext const& context) {
	CODE_PROBE(!spanContext.isValid(), "addTransactionInfo with invalid SpanContext");
	spanContext = context;
	writtenLocations.clear();
}

void LogPushData::writeMessage(StringRef rawMessageWithoutLength, bool usePreviousLocations) {
	if (!usePreviousLocations) {
		prev_tags.clear();
		if (logSystem->hasRemoteLogs()) {
			prev_tags.push_back(chooseRouterTag());
		}
		for (auto& tag : next_message_tags) {
			prev_tags.push_back(tag);
		}
		msg_locations.clear();
		logSystem->getPushLocations(prev_tags, msg_locations);
		written_tags.insert(next_message_tags.begin(), next_message_tags.end());
		next_message_tags.clear();
	}
	uint32_t subseq = this->subsequence++;
	uint32_t msgsize =
	    rawMessageWithoutLength.size() + sizeof(subseq) + sizeof(uint16_t) + sizeof(Tag) * prev_tags.size();
	for (int loc : msg_locations) {
		BinaryWriter& wr = messagesWriter[loc];
		wr << msgsize << subseq << uint16_t(prev_tags.size());
		for (auto& tag : prev_tags)
			wr << tag;
		wr.serializeBytes(rawMessageWithoutLength);
	}
}

std::vector<Standalone<StringRef>> LogPushData::getAllMessages() const {
	std::vector<Standalone<StringRef>> results;
	results.reserve(messagesWriter.size());
	for (int loc = 0; loc < messagesWriter.size(); loc++) {
		results.push_back(getMessages(loc));
	}
	return results;
}

void LogPushData::recordEmptyMessage(int loc, const Standalone<StringRef>& value) {
	if (!messagesWritten[loc]) {
		BinaryWriter w(AssumeVersion(g_network->protocolVersion()));
		Standalone<StringRef> v = w.toValue();
		if (value.size() > v.size()) {
			messagesWritten[loc] = true;
		}
	}
}

float LogPushData::getEmptyMessageRatio() const {
	auto count = std::count(messagesWritten.begin(), messagesWritten.end(), false);
	ASSERT_WE_THINK(!messagesWritten.empty());
	return 1.0 * count / messagesWritten.size();
}

bool LogPushData::writeTransactionInfo(int location, uint32_t subseq) {
	if (!FLOW_KNOBS->WRITE_TRACING_ENABLED || logSystem->getTLogVersion() < TLogVersion::V6 ||
	    writtenLocations.contains(location)) {
		return false;
	}

	CODE_PROBE(true, "Wrote SpanContextMessage to a transaction log");
	writtenLocations.insert(location);

	BinaryWriter& wr = messagesWriter[location];
	int offset = wr.getLength();
	wr << uint32_t(0) << subseq << uint16_t(prev_tags.size());
	for (auto& tag : prev_tags)
		wr << tag;
	if (logSystem->getTLogVersion() >= TLogVersion::V7) {
		OTELSpanContextMessage contextMessage(spanContext);
		wr << contextMessage;
	} else {
		// When we're on a TLog version below 7, but the front end of the system (i.e. proxy, sequencer, resolver)
		// is using OpenTelemetry tracing (i.e on or above 7.2), we need to convert the OpenTelemetry Span data model
		// i.e. 16 bytes for traceId, 8 bytes for spanId, to the OpenTracing spec, which is 8 bytes for traceId
		// and 8 bytes for spanId. That means we need to drop some data.
		//
		// As a workaround for this special case we've decided to drop is the 8 bytes
		// for spanId. Therefore we're passing along the full 16 byte traceId to the storage server with 0 for spanID.
		// This will result in a follows from relationship for the storage span within the trace rather than a
		// parent->child.
		SpanContextMessage contextMessage;
		if (spanContext.isSampled()) {
			CODE_PROBE(true, "Converting OTELSpanContextMessage to traced SpanContextMessage");
			contextMessage = SpanContextMessage(UID(spanContext.traceID.first(), spanContext.traceID.second()));
		} else {
			CODE_PROBE(true, "Converting OTELSpanContextMessage to untraced SpanContextMessage");
			contextMessage = SpanContextMessage(UID(0, 0));
		}
		wr << contextMessage;
	}
	int length = wr.getLength() - offset;
	*(uint32_t*)((uint8_t*)wr.getData() + offset) = length - sizeof(uint32_t);
	return true;
}

void LogPushData::setMutations(uint32_t totalMutations, VectorRef<StringRef> mutations) {
	ASSERT_EQ(subsequence, 1);
	subsequence = totalMutations + 1; // set to next mutation number

	ASSERT_EQ(messagesWriter.size(), mutations.size());
	BinaryWriter w(AssumeVersion(g_network->protocolVersion()));
	Standalone<StringRef> v = w.toValue();
	const int header = v.size();
	for (int i = 0; i < mutations.size(); i++) {
		BinaryWriter& wr = messagesWriter[i];
		wr.serializeBytes(mutations[i].substr(header));
	}
}
