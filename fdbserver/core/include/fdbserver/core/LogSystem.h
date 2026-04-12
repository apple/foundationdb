/*
 * LogSystem.h
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

#ifndef FDBSERVER_LOGSYSTEM_H
#define FDBSERVER_LOGSYSTEM_H

#include <cstdint>
#include <set>
#include <vector>

#include "fdbclient/DatabaseConfiguration.h"
#include "fdbrpc/Locality.h"
#include "fdbrpc/Replication.h"
#include "fdbrpc/ReplicationPolicy.h"
#include "fdbserver/core/LogSystemConfig.h"
#include "fdbserver/core/MutationTracking.h"
#include "fdbserver/core/OTELSpanContextMessage.h"
#include "fdbserver/core/SpanContextMessage.h"
#include "fdbserver/core/TLogInterface.h"
#include "fdbserver/core/WorkerInterface.actor.h"
#include "flow/Arena.h"
#include "flow/Error.h"
#include "flow/Histogram.h"
#include "flow/IndexedSet.h"
#include "flow/Knobs.h"

struct DBCoreState;
struct LogPushData;
struct LocalityData;
struct LogSystem;

struct ConnectionResetInfo : public ReferenceCounted<ConnectionResetInfo> {
	double lastReset;
	Future<Void> resetCheck;
	int slowReplies;
	int fastReplies;

	ConnectionResetInfo() : lastReset(now()), resetCheck(Void()), slowReplies(0), fastReplies(0) {}
};

struct IPeekCursor {
	// clones the peek cursor, however you cannot call getMore() on the cloned cursor.
	virtual Reference<IPeekCursor> cloneNoMore() = 0;

	virtual void setProtocolVersion(ProtocolVersion version) = 0;

	virtual bool hasMessage() const = 0;
	virtual VectorRef<Tag> getTags() const = 0;
	virtual Arena& arena() = 0;
	virtual ArenaReader* reader() = 0;
	virtual StringRef getMessage() = 0;
	virtual StringRef getMessageWithTags() = 0;
	virtual void nextMessage() = 0;
	virtual void advanceTo(LogMessageVersion n) = 0;
	virtual Future<Void> getMore(TaskPriority taskID = TaskPriority::TLogPeekReply) = 0;
	virtual Future<Void> onFailed() const = 0;
	virtual bool isActive() const = 0;
	virtual bool isExhausted() const = 0;
	virtual const LogMessageVersion& version() const = 0;
	virtual Version popped() const = 0;
	virtual Version getMaxKnownVersion() const { return 0; }
	virtual Version getMinKnownCommittedVersion() const = 0;
	virtual Optional<UID> getPrimaryPeekLocation() const = 0;
	virtual Optional<UID> getCurrentPeekLocation() const = 0;
	virtual void addref() = 0;
	virtual void delref() = 0;
};

struct LogPushVersionSet {
	Version prevVersion;
	Version version;
	Version knownCommittedVersion;
	Version minKnownCommittedVersion;
};

struct EpochTagsVersionsInfo {
	int32_t logRouterTags;
	Version epochBegin, epochEnd;

	explicit EpochTagsVersionsInfo(int32_t n, Version begin, Version end)
	  : logRouterTags(n), epochBegin(begin), epochEnd(end) {}
};

bool logSystemHasRemoteLogs(LogSystem const& logSystem);
void logSystemGetPushLocations(
    LogSystem const& logSystem,
    VectorRef<Tag> tags,
    std::vector<int>& locations,
    bool allLocations = false,
    Optional<std::vector<Reference<LocalitySet>>> fromLocations = Optional<std::vector<Reference<LocalitySet>>>());
std::vector<Reference<LocalitySet>> logSystemGetPushLocationsForTags(LogSystem const& logSystem,
                                                                     std::vector<int>& fromLocations);
Tag logSystemGetRandomRouterTag(LogSystem const& logSystem);
int logSystemGetLogRouterTags(LogSystem const& logSystem);
Tag logSystemGetRandomTxsTag(LogSystem const& logSystem);
TLogVersion logSystemGetTLogVersion(LogSystem const& logSystem);

struct LengthPrefixedStringRef {
	// Represents a pointer to a string which is prefixed by a 4-byte length
	// A LengthPrefixedStringRef is only pointer-sized (8 bytes vs 12 bytes for StringRef), but the corresponding string
	// is 4 bytes bigger, and substring operations aren't efficient as they are with StringRef.  It's a good choice when
	// there might be lots of references to the same exact string.

	uint32_t* length;

	StringRef toStringRef() const {
		ASSERT(length);
		return StringRef((uint8_t*)(length + 1), *length);
	}
	int expectedSize() const {
		ASSERT(length);
		return *length;
	}
	uint32_t* getLengthPtr() const { return length; }

	LengthPrefixedStringRef() : length(nullptr) {}
	LengthPrefixedStringRef(uint32_t* length) : length(length) {}
};

// Structure to store serialized mutations sent from the proxy to the
// transaction logs. The serialization repeats with the following format:
//
// +----------------------+ +----------------------+ +----------+ +----------------+         +----------------------+
// |     Message size     | |      Subsequence     | | # of tags| |      Tag       | . . . . |       Mutation       |
// +----------------------+ +----------------------+ +----------+ +----------------+         +----------------------+
// <------- 32 bits ------> <------- 32 bits ------> <- 16 bits-> <---- 24 bits --->         <---- variable bits --->
//
// `Mutation` can be a serialized MutationRef or a special metadata message
// such as LogProtocolMessage or SpanContextMessage. The type of `Mutation` is
// uniquely identified by its first byte -- a value from MutationRef::Type.
//
struct LogPushData : NonCopyable {
	// Log subsequences have to start at 1 (the MergedPeekCursor relies on this to make sure we never have !hasMessage()
	// in the middle of data for a version

	explicit LogPushData(Reference<LogSystem> logSystem, int tlogCount);

	void addTxsTag();

	// addTag() adds a tag for the *next* message to be added
	void addTag(Tag tag) { next_message_tags.push_back(tag); }

	template <class T>
	void addTags(T tags) {
		next_message_tags.insert(next_message_tags.end(), tags.begin(), tags.end());
	}

	// Add transaction info to be written before the first mutation in the transaction.
	void addTransactionInfo(SpanContext const& context);

	// copy written_tags, after filtering, into given set
	void saveTags(std::set<Tag>& filteredTags) const {
		for (const auto& tag : written_tags) {
			filteredTags.insert(tag);
		}
	}

	void addWrittenTags(const std::set<Tag>& tags) { written_tags.insert(tags.begin(), tags.end()); }

	void getLocations(const std::set<Tag>& tags, std::set<uint16_t>& writtenTLogs) {
		std::vector<Tag> vtags(tags.begin(), tags.end());
		std::vector<int> msg_locations;
		logSystemGetPushLocations(*logSystem, VectorRef<Tag>((Tag*)vtags.data(), vtags.size()), msg_locations);
		writtenTLogs.insert(msg_locations.begin(), msg_locations.end());
	}

	// store tlogs as represented by index
	void saveLocations(std::set<uint16_t>& writtenTLogs) {
		writtenTLogs.insert(msg_locations.begin(), msg_locations.end());
	}

	void setLogsChanged() { logsChanged = true; }
	bool haveLogsChanged() const { return logsChanged; }

	void writeMessage(StringRef rawMessageWithoutLength, bool usePreviousLocations);

	void setPushLocationsForTags(std::vector<int> fromLocationsVec) {
		fromLocations = logSystemGetPushLocationsForTags(*logSystem, fromLocationsVec);
	}

	template <class T>
	void writeTypedMessage(T const& item, bool metadataMessage = false, bool allLocations = false);

	Standalone<StringRef> getMessages(int loc) const { return messagesWriter[loc].toValue(); }

	// Returns all locations' messages, including empty ones.
	std::vector<Standalone<StringRef>> getAllMessages() const;

	// Records if a tlog (specified by "loc") will receive an empty version batch message.
	// "value" is the message returned by getMessages() call.
	void recordEmptyMessage(int loc, const Standalone<StringRef>& value);

	// Returns the ratio of empty messages in this version batch.
	// MUST be called after getMessages() and recordEmptyMessage().
	float getEmptyMessageRatio() const;

	// Returns the total number of mutations. Subsequence is initialized to 1, so subtract 1 to get count.
	uint32_t getMutationCount() const { return subsequence - 1; }

	// Sets mutations for all internal writers. "mutations" is the output from
	// getAllMessages() and is used before writing any other mutations.
	void setMutations(uint32_t totalMutations, VectorRef<StringRef> mutations);

	Optional<Tag> savedRandomRouterTag;
	void storeRandomRouterTag() { savedRandomRouterTag = logSystemGetRandomRouterTag(*logSystem); }
	int getLogRouterTags() { return logSystemGetLogRouterTags(*logSystem); }

private:
	Reference<LogSystem> logSystem;
	std::vector<Tag> next_message_tags;
	std::vector<Tag> prev_tags;
	std::set<Tag> written_tags;
	std::vector<BinaryWriter> messagesWriter;
	std::vector<bool> messagesWritten; // if messagesWriter has written anything
	std::vector<int> msg_locations;
	Optional<std::vector<Reference<LocalitySet>>> fromLocations;
	// Stores message locations that have had span information written to them
	// for the current transaction. Adding transaction info will reset this
	// field.
	std::unordered_set<int> writtenLocations;
	uint32_t subsequence;
	SpanContext spanContext;
	bool logsChanged = false; // if keyServers has any changes, i.e., shard boundary modifications.

	// Writes transaction info to the message stream at the given location if
	// it has not already been written (for the current transaction). Returns
	// true on a successful write, and false if the location has already been
	// written.
	bool writeTransactionInfo(int location, uint32_t subseq);

	Tag chooseRouterTag() {
		return savedRandomRouterTag.present() ? savedRandomRouterTag.get() : logSystemGetRandomRouterTag(*logSystem);
	}
};

template <class T>
void LogPushData::writeTypedMessage(T const& item, bool metadataMessage, bool allLocations) {
	prev_tags.clear();
	if (logSystemHasRemoteLogs(*logSystem)) {
		prev_tags.push_back(chooseRouterTag());
	}
	for (auto& tag : next_message_tags) {
		prev_tags.push_back(tag);
	}
	msg_locations.clear();
	logSystemGetPushLocations(*logSystem,
	                          VectorRef<Tag>((Tag*)prev_tags.data(), prev_tags.size()),
	                          msg_locations,
	                          allLocations,
	                          fromLocations);

	BinaryWriter bw(AssumeVersion(g_network->protocolVersion()));

	// Metadata messages (currently LogProtocolMessage is the only metadata
	// message) should be written before span information. If this isn't a
	// metadata message, make sure all locations have had transaction info
	// written to them.  Mutations may have different sets of tags, so it
	// is necessary to check all tag locations each time a mutation is
	// written.
	if (!metadataMessage) {
		uint32_t subseq = this->subsequence++;
		bool updatedLocation = false;
		for (int loc : msg_locations) {
			updatedLocation = writeTransactionInfo(loc, subseq) || updatedLocation;
		}
		// If this message doesn't write to any new locations, the
		// subsequence wasn't actually used and can be decremented.
		if (!updatedLocation) {
			this->subsequence--;
			CODE_PROBE(true, "No new SpanContextMessage written to transaction logs");
			ASSERT(this->subsequence > 0);
		}
	} else {
		// When writing a metadata message, make sure transaction state has
		// been reset. If you are running into this assertion, make sure
		// you are calling addTransactionInfo before each transaction.
		ASSERT(writtenLocations.size() == 0);
	}

	uint32_t subseq = this->subsequence++;
	bool first = true;
	int firstOffset = -1, firstLength = -1;
	for (int loc : msg_locations) {
		BinaryWriter& wr = messagesWriter[loc];

		if (first) {
			firstOffset = wr.getLength();
			wr << uint32_t(0) << subseq << uint16_t(prev_tags.size());
			for (auto& tag : prev_tags)
				wr << tag;
			wr << item;
			firstLength = wr.getLength() - firstOffset;
			*(uint32_t*)((uint8_t*)wr.getData() + firstOffset) = firstLength - sizeof(uint32_t);
			DEBUG_TAGS_AND_MESSAGE(
			    "ProxyPushLocations", invalidVersion, StringRef(((uint8_t*)wr.getData() + firstOffset), firstLength))
			    .detail("PushLocations", msg_locations);
			first = false;
		} else {
			BinaryWriter& from = messagesWriter[msg_locations[0]];
			wr.serializeBytes((uint8_t*)from.getData() + firstOffset, firstLength);
		}
	}
	written_tags.insert(next_message_tags.begin(), next_message_tags.end());
	next_message_tags.clear();
}

#endif // FDBSERVER_LOGSYSTEM_H
