/*
 * NativeCdcMetadata.cpp
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

#include <algorithm>
#include <limits>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/SystemData.h"
#include "NativeCdcInternal.h"
#include "fdbserver/core/NativeCdcMetadata.h"
#include "flow/CodeProbe.h"
#include "flow/Error.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

namespace {

using CDCTagId = uint16_t;

constexpr uint32_t maxNativeCdcTagCount = static_cast<uint32_t>(std::numeric_limits<CDCTagId>::max()) + 1;

bool validNativeCdcTagCount(int tagCount) {
	return tagCount > 0 && static_cast<uint32_t>(tagCount) <= maxNativeCdcTagCount;
}

class NativeCdcIdentifierAllocator {
	bool sawStream = false;
	CDCStreamId maxStreamId = 0;
	std::unordered_map<CDCTagId, uint32_t> tagStreamCounts;

public:
	void observeStreamId(CDCStreamId streamId) {
		sawStream = true;
		maxStreamId = std::max(maxStreamId, streamId);
	}

	void observeTag(Tag tag) {
		ASSERT_WE_THINK(tag.locality == tagLocalityCDC);
		++tagStreamCounts[tag.id];
	}

	bool hasStreams(Tag tag) const { return tagStreamCounts.contains(tag.id); }

	std::pair<CDCStreamId, Tag> allocate(int tagCount) const {
		if (sawStream && maxStreamId == std::numeric_limits<CDCStreamId>::max()) {
			throw operation_failed();
		}

		const CDCStreamId streamId = sawStream ? maxStreamId + 1 : 1;
		if (!validNativeCdcTagCount(tagCount)) {
			throw invalid_option_value();
		}
		uint32_t leastStreams = std::numeric_limits<uint32_t>::max();
		CDCTagId selectedTagId = 0;
		for (uint32_t tagId = 0; tagId < static_cast<uint32_t>(tagCount); ++tagId) {
			auto count = tagStreamCounts.find(static_cast<CDCTagId>(tagId));
			const uint32_t streamCount = count == tagStreamCounts.end() ? 0 : count->second;
			if (streamCount < leastStreams) {
				leastStreams = streamCount;
				selectedTagId = static_cast<CDCTagId>(tagId);
			}
		}
		return { streamId, Tag(tagLocalityCDC, selectedTagId) };
	}
};

Future<Optional<UID>> getNativeCdcProxyAssignment(Transaction* tr, CDCStreamId streamId) {
	RangeResult assignments = co_await tr->getRange(cdcProxyRangeFor(streamId), 2);
	ASSERT_LE(assignments.size(), 1);
	if (assignments.empty()) {
		co_return Optional<UID>();
	}
	const auto [assignedStreamId, proxyId] = decodeCDCProxyKey(assignments[0].key);
	ASSERT_WE_THINK(assignedStreamId == streamId);
	co_return proxyId;
}

Future<Tag> getNativeCdcCurrentTag(Transaction* tr, CDCStreamId streamId) {
	// Tag-history keys sort by their big-endian assignment version, so the final
	// key in this stream's prefix range contains its current tag.
	RangeResult history = co_await tr->getRange(cdcTagHistoryRangeFor(streamId), 1, Snapshot::False, Reverse::True);
	if (history.empty()) {
		throw client_invalid_operation();
	}
	co_return decodeCDCTagHistoryKey(history.front().key).tag;
}

Future<Void> readNativeCdcCurrentTags(Transaction* tr,
                                      std::unordered_map<CDCStreamId, Tag>* currentTags,
                                      NativeCdcIdentifierAllocator* allocator = nullptr) {
	std::set<CDCStreamId> activeStreamIds;
	Key begin = cdcStreamKeys.begin;
	while (begin < cdcStreamKeys.end) {
		RangeResult streams = co_await tr->getRange(KeyRangeRef(begin, cdcStreamKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& kv : streams) {
			const CDCStreamId streamId = decodeCDCStreamKey(kv.key);
			activeStreamIds.insert(streamId);
			if (allocator) {
				allocator->observeStreamId(streamId);
			}
		}
		if (!streams.more) {
			break;
		}
		begin = keyAfter(streams.back().key);
	}

	begin = cdcTagHistoryKeys.begin;
	while (begin < cdcTagHistoryKeys.end) {
		RangeResult histories =
		    co_await tr->getRange(KeyRangeRef(begin, cdcTagHistoryKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& kv : histories) {
			const CDCTagHistoryEntry history = decodeCDCTagHistoryKey(kv.key);
			if (allocator) {
				allocator->observeStreamId(history.streamId);
			}
			if (activeStreamIds.contains(history.streamId)) {
				(*currentTags)[history.streamId] = history.tag;
			}
		}
		if (!histories.more) {
			break;
		}
		begin = keyAfter(histories.back().key);
	}
}

Future<Optional<UID>> getNativeCdcProxyAssignmentForTag(Transaction* tr, Tag targetTag) {
	const Key ownerKey = cdcTagOwnerKeyFor(targetTag);
	Optional<Value> indexedStream = co_await tr->get(ownerKey);
	if (indexedStream.present()) {
		const CDCStreamId streamId = decodeCDCTagOwnerValue(indexedStream.get());
		Future<Optional<Value>> activeStream = tr->get(cdcStreamKeyFor(streamId));
		RangeResult history = co_await tr->getRange(cdcTagHistoryRangeFor(streamId), 1, Snapshot::False, Reverse::True);
		// Keep the await separate so GCC 13 does not evaluate history.front() before the short-circuit guards.
		const Optional<Value> activeStreamValue = co_await activeStream;
		// The index is derived: removal or retagging can invalidate its representative, and the per-stream
		// assignment remains authoritative across proxy replacement, including by older metadata writers.
		if (activeStreamValue.present() && !history.empty() &&
		    decodeCDCTagHistoryKey(history.front().key).tag == targetTag) {
			Optional<UID> proxyId = co_await getNativeCdcProxyAssignment(tr, streamId);
			if (proxyId.present()) {
				CODE_PROBE(true, "Native CDC resolves a shared tag owner from its persisted index");
				co_return proxyId;
			}
		}
		CODE_PROBE(true, "Native CDC rebuilds a stale tag owner index");
		tr->clear(ownerKey);
	}

	std::unordered_map<CDCStreamId, Tag> currentTags;
	co_await readNativeCdcCurrentTags(tr, &currentTags);
	for (const auto& [streamId, tag] : currentTags) {
		if (tag == targetTag) {
			Optional<UID> proxyId = co_await getNativeCdcProxyAssignment(tr, streamId);
			if (proxyId.present()) {
				tr->set(ownerKey, cdcTagOwnerValue(streamId));
				CODE_PROBE(true, "Native CDC reconstructs a missing tag owner index from active streams");
				co_return proxyId;
			}
		}
	}
	co_return Optional<UID>();
}

void retireNativeCdcTag(Transaction* tr, Tag tag) {
	// Dropping a history row must retain its final-pop obligation, including
	// when another stream still protects the same tag or recovery intervenes.
	tr->set(cdcRetiredTagPopKeyFor(tag), Value());
	tr->atomicOp(
	    cdcRetiredTagPopVersionKeyFor(tag), cdcVersionstampedMinVersionValue(), MutationRef::SetVersionstampedValue);
}

void signalNativeCdcProxyAssignmentChange(Transaction* tr) {
	// Assignment updates are low-rate control-plane operations. A single
	// coalescing signal lets the cluster controller rescan all durable owners.
	tr->set(cdcProxyAssignmentChangeKey,
	        BinaryWriter::toValue(deterministicRandom()->randomUniqueID(),
	                              IncludeVersion(ProtocolVersion::withNativeCdc())));
}

Future<Void> observeNativeCdcMetadata(Transaction* tr, NativeCdcIdentifierAllocator* allocator) {
	Optional<Value> maxStreamId = co_await tr->get(cdcMaxStreamIdKey);
	if (maxStreamId.present()) {
		allocator->observeStreamId(decodeCDCMaxStreamIdValue(maxStreamId.get()));
	}

	std::unordered_map<CDCStreamId, Tag> currentTags;
	co_await readNativeCdcCurrentTags(tr, &currentTags, allocator);
	for (const auto& tagAssignment : currentTags) {
		allocator->observeTag(tagAssignment.second);
	}
}

Future<Optional<NativeCdcTagState>> readNativeCdcTagStateImpl(Transaction* tr, CDCStreamId streamId) {
	Future<Optional<Value>> keysFuture = tr->get(cdcStreamKeyFor(streamId));
	Future<Optional<Value>> minimumFuture = tr->get(cdcMinVersionKeyFor(streamId));
	Future<Optional<UID>> ownerFuture = getNativeCdcProxyAssignment(tr, streamId);
	Future<RangeResult> historyFuture =
	    tr->getRange(cdcTagHistoryRangeFor(streamId), 3, Snapshot::False, Reverse::True);
	const Optional<Value> keys = co_await keysFuture;
	const Optional<Value> minimum = co_await minimumFuture;
	const Optional<UID> owner = co_await ownerFuture;
	const RangeResult history = co_await historyFuture;
	if (!keys.present() || !minimum.present() || !owner.present() || history.empty() || history.more ||
	    history.size() > 2) {
		co_return Optional<NativeCdcTagState>();
	}
	NativeCdcTagState state;
	state.streamId = streamId;
	state.ranges = decodeCDCStreamKeysValue(keys.get());
	state.historyKey = history.front().key;
	state.assignment = decodeCDCTagHistoryEntry(history.front().key, history.front().value);
	state.proxyId = owner.get();
	state.minVersion = decodeCDCMinVersionValue(minimum.get());
	state.pending = history.size() > 1 || !history.front().value.empty();
	co_return state;
}

bool sameNativeCdcTagState(NativeCdcTagState const& current, NativeCdcTagState const& expected) {
	return current.streamId == expected.streamId && current.ranges == expected.ranges &&
	       current.historyKey == expected.historyKey && current.proxyId == expected.proxyId &&
	       current.assignment.version == expected.assignment.version &&
	       current.assignment.tag == expected.assignment.tag;
}

} // namespace

Future<Optional<NativeCdcTagState>> readNativeCdcTagState(Transaction* tr, CDCStreamId streamId) {
	return readNativeCdcTagStateImpl(tr, streamId);
}

Future<Optional<std::vector<NativeCdcTagState>>> readNativeCdcTagStates(Transaction* tr, int maxStreams) {
	if (maxStreams <= 0 || maxStreams == std::numeric_limits<int>::max()) {
		throw invalid_option_value();
	}
	const RangeResult streams = co_await tr->getRange(cdcStreamKeys, maxStreams + 1);
	if (streams.more || streams.size() > maxStreams) {
		co_return Optional<std::vector<NativeCdcTagState>>();
	}
	std::vector<Future<Optional<NativeCdcTagState>>> reads;
	reads.reserve(streams.size());
	for (const auto& stream : streams) {
		reads.push_back(readNativeCdcTagState(tr, decodeCDCStreamKey(stream.key)));
	}
	const std::vector<Optional<NativeCdcTagState>> states = co_await getAll(reads);
	std::vector<NativeCdcTagState> result;
	result.reserve(states.size());
	for (const auto& state : states) {
		if (!state.present()) {
			co_return Optional<std::vector<NativeCdcTagState>>();
		}
		result.push_back(state.get());
	}
	co_return Optional<std::vector<NativeCdcTagState>>(std::move(result));
}

Future<bool> retagNativeCdcStream(Transaction* tr, NativeCdcTagState expected, Tag destination) {
	Optional<NativeCdcTagState> current = co_await readNativeCdcTagState(tr, expected.streamId);
	if (!current.present() || !sameNativeCdcTagState(current.get(), expected) || current.get().pending ||
	    destination.locality != tagLocalityCDC || destination == current.get().assignment.tag) {
		co_return false;
	}
	const Optional<UID> destinationOwner = co_await getNativeCdcProxyAssignmentForTag(tr, destination);
	if (destinationOwner.present() && destinationOwner.get() != current.get().proxyId) {
		co_return false;
	}
	const Version readVersion = co_await tr->getReadVersion();
	const auto& clientInfo = tr->getDatabase()->clientInfo->get();
	if (!clientInfo.nativeCdcEnabled || !validNativeCdcTagCount(clientInfo.nativeCdcTagCount) ||
	    destination.id >= clientInfo.nativeCdcTagCount) {
		co_return false;
	}
	const Key historyKey = cdcTagHistoryKeyFor(expected.streamId, readVersion, destination);
	if (historyKey <= current.get().historyKey) {
		co_return false;
	}
	// The key orders assignments, while the value supplies the exact routing
	// cutover. A read-version boundary could skip writes before this commit.
	tr->atomicOp(historyKey, cdcVersionstampedMinVersionValue(), MutationRef::SetVersionstampedValue);
	signalNativeCdcProxyAssignmentChange(tr);
	co_return true;
}

Future<bool> finishNativeCdcRetag(Transaction* tr, NativeCdcTagState expected) {
	const Optional<NativeCdcTagState> current = co_await readNativeCdcTagState(tr, expected.streamId);
	if (!current.present() || !sameNativeCdcTagState(current.get(), expected) || !current.get().pending ||
	    current.get().minVersion < current.get().assignment.version) {
		co_return false;
	}
	const RangeResult history = co_await tr->getRange(cdcTagHistoryRangeFor(expected.streamId), 3);
	if (history.more || history.empty() || history.size() > 2 || history.back().key != current.get().historyKey) {
		co_return false;
	}
	std::set<Tag> retiredTags;
	for (const auto& row : history) {
		const Tag tag = decodeCDCTagHistoryEntry(row.key, row.value).tag;
		if (tag != current.get().assignment.tag) {
			retiredTags.insert(tag);
		}
	}
	tr->clear(cdcTagHistoryRangeFor(expected.streamId));
	tr->set(cdcTagHistoryKeyFor(expected.streamId, current.get().assignment.version, current.get().assignment.tag),
	        Value());
	for (const Tag tag : retiredTags) {
		retireNativeCdcTag(tr, tag);
	}
	signalNativeCdcProxyAssignmentChange(tr);
	co_return true;
}

Future<NativeCdcRegistrationResult> prepareNativeCdcStreamRegistration(Transaction* tr,
                                                                       Key name,
                                                                       std::vector<KeyRange> ranges,
                                                                       UID proxyId) {
	normalizeNativeCdcStreamRanges(name, ranges);

	const Key nameKey = cdcStreamNameKeyFor(name);
	Optional<Value> currentId = co_await tr->get(nameKey);
	if (currentId.present()) {
		const CDCStreamId streamId = decodeCDCStreamNameValue(currentId.get());
		Optional<Value> currentKeys = co_await tr->get(cdcStreamKeyFor(streamId));
		if (!currentKeys.present() || decodeCDCStreamKeysValue(currentKeys.get()) != ranges) {
			throw client_invalid_operation();
		}
		if (!(co_await getNativeCdcProxyAssignment(tr, streamId)).present()) {
			CODE_PROBE(true, "Native CDC registration restores missing stream owner", probe::decoration::rare);
			const Tag tag = co_await getNativeCdcCurrentTag(tr, streamId);
			Optional<UID> sharedTagProxy = co_await getNativeCdcProxyAssignmentForTag(tr, tag);
			CODE_PROBE(
			    sharedTagProxy.present(), "Native CDC shared-tag streams use one owner", probe::decoration::rare);
			const UID selectedProxy = sharedTagProxy.present() ? sharedTagProxy.get() : proxyId;
			tr->set(cdcProxyKeyFor(streamId, selectedProxy), Value());
			if (!sharedTagProxy.present()) {
				tr->set(cdcTagOwnerKeyFor(tag), cdcTagOwnerValue(streamId));
			}
			signalNativeCdcProxyAssignmentChange(tr);
			co_return NativeCdcRegistrationResult{ streamId, true };
		}
		co_return NativeCdcRegistrationResult{ streamId, false };
	}

	// Disabling CDC stops new admission, but existing registrations and
	// owner repair must remain available so durable streams can drain.
	const bool nativeCdcEnabled = tr->getDatabase()->clientInfo->get().nativeCdcEnabled;
	const int nativeCdcTagCount = tr->getDatabase()->clientInfo->get().nativeCdcTagCount;
	validateNativeCdcEnabled(nativeCdcEnabled);
	NativeCdcIdentifierAllocator allocator;
	co_await observeNativeCdcMetadata(tr, &allocator);
	const auto [streamId, tag] = allocator.allocate(nativeCdcTagCount);
	// The read version is a conservative lower bound for tag routing.
	// The versionstamped minimum below is the commit version, and stream
	// initialization takes their maximum before exposing mutations.
	const Version registrationVersion = co_await tr->getReadVersion();

	tr->set(nameKey, cdcStreamNameValue(streamId));
	tr->set(cdcMaxStreamIdKey, cdcMaxStreamIdValue(streamId));
	tr->set(cdcStreamKeyFor(streamId), cdcStreamKeysValue(ranges));
	tr->set(cdcTagHistoryKeyFor(streamId, registrationVersion, tag), Value());
	tr->atomicOp(
	    cdcMinVersionKeyFor(streamId), cdcVersionstampedMinVersionValue(), MutationRef::SetVersionstampedValue);
	Optional<UID> sharedTagProxy;
	if (allocator.hasStreams(tag)) {
		sharedTagProxy = co_await getNativeCdcProxyAssignmentForTag(tr, tag);
	}
	const UID selectedProxy = sharedTagProxy.present() ? sharedTagProxy.get() : proxyId;
	tr->set(cdcProxyKeyFor(streamId, selectedProxy), Value());
	if (!sharedTagProxy.present()) {
		tr->set(cdcTagOwnerKeyFor(tag), cdcTagOwnerValue(streamId));
	}
	signalNativeCdcProxyAssignmentChange(tr);
	co_return NativeCdcRegistrationResult{ streamId, true };
}

Future<CDCStreamId> registerNativeCdcStream(Database cx, Key name, std::vector<KeyRange> ranges, UID proxyId) {
	normalizeNativeCdcStreamRanges(name, ranges);

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			const NativeCdcRegistrationResult result =
			    co_await prepareNativeCdcStreamRegistration(&tr, name, ranges, proxyId);
			if (result.requiresCommit) {
				co_await tr.commit();
			}
			co_return result.streamId;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<bool> removeNativeCdcStream(Database cx, Key name, CDCStreamId streamId, UID proxyId) {
	if (name.empty() || streamId == 0) {
		throw client_invalid_operation();
	}

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			const Key nameKey = cdcStreamNameKeyFor(name);
			Optional<Value> currentId = co_await tr.get(nameKey);
			if (!nativeCdcNameMatchesStream(currentId, streamId)) {
				CODE_PROBE(currentId.present(), "Native CDC preserves a replacement stream during removal retry");
				if (currentId.present()) {
					TraceEvent("NativeCdcRemovalPreservesReplacement")
					    .detail("RemovedStreamId", streamId)
					    .detail("ReplacementStreamId", decodeCDCStreamNameValue(currentId.get()));
				}
				co_return false;
			}

			Optional<UID> assignedProxy = co_await getNativeCdcProxyAssignment(&tr, streamId);
			if (!assignedProxy.present() || assignedProxy.get() != proxyId) {
				CODE_PROBE(true, "Native CDC rejects removal through a stale owner");
				throw wrong_shard_server();
			}

			std::set<Tag> removedTags;
			const KeyRange historyRange = cdcTagHistoryRangeFor(streamId);
			Key begin = historyRange.begin;
			while (begin < historyRange.end) {
				RangeResult history =
				    co_await tr.getRange(KeyRangeRef(begin, historyRange.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& entry : history) {
					removedTags.insert(decodeCDCTagHistoryKey(entry.key).tag);
				}
				if (!history.more) {
					break;
				}
				begin = keyAfter(history.back().key);
			}

			tr.clear(nameKey);
			tr.clear(cdcStreamKeyFor(streamId));
			for (const Tag& tag : removedTags) {
				const Key ownerKey = cdcTagOwnerKeyFor(tag);
				Optional<Value> indexedStream = co_await tr.get(ownerKey);
				if (indexedStream.present() && decodeCDCTagOwnerValue(indexedStream.get()) == streamId) {
					tr.clear(ownerKey);
				}
				retireNativeCdcTag(&tr, tag);
			}
			tr.clear(cdcTagHistoryRangeFor(streamId));
			tr.clear(cdcMinVersionKeyFor(streamId));
			tr.clear(cdcProxyRangeFor(streamId));
			if (assignedProxy.present()) {
				signalNativeCdcProxyAssignmentChange(&tr);
			}
			co_await tr.commit();
			CODE_PROBE(!removedTags.empty(), "Native CDC removal records final tagged pop work");
			TraceEvent("NativeCdcStreamRemoved")
			    .detail("StreamId", streamId)
			    .detail("ProxyID", proxyId)
			    .detail("CommitVersion", tr.getCommittedVersion())
			    .detail("RetiredTagCount", removedTags.size());
			co_return true;
		} catch (Error& e) {
			if (e.code() == error_code_wrong_shard_server) {
				throw;
			}
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Void> reassignNativeCdcStreams(Database cx, UID oldProxyId, UID newProxyId) {
	if (oldProxyId == newProxyId) {
		co_return;
	}

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			bool changed = false;
			Key begin = cdcProxyKeys.begin;
			while (begin < cdcProxyKeys.end) {
				RangeResult assignments =
				    co_await tr.getRange(KeyRangeRef(begin, cdcProxyKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& assignment : assignments) {
					const auto [streamId, proxyId] = decodeCDCProxyKey(assignment.key);
					if (proxyId == oldProxyId) {
						tr.clear(assignment.key);
						tr.set(cdcProxyKeyFor(streamId, newProxyId), Value());
						changed = true;
					}
				}
				if (!assignments.more) {
					break;
				}
				begin = keyAfter(assignments.back().key);
			}

			if (changed) {
				CODE_PROBE(true, "Native CDC reassigns streams after proxy replacement");
				signalNativeCdcProxyAssignmentChange(&tr);
				co_await tr.commit();
			}
			co_return;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

void forceLinkNativeCdcMetadataTests() {}

TEST_CASE("/NativeCDC/LifecycleAllocation") {
	ASSERT(!validNativeCdcTagCount(-1));
	ASSERT(!validNativeCdcTagCount(0));
	ASSERT(validNativeCdcTagCount(1));
	ASSERT(validNativeCdcTagCount(std::numeric_limits<uint16_t>::max() + 1u));
	ASSERT(!validNativeCdcTagCount(std::numeric_limits<uint16_t>::max() + 2u));

	NativeCdcIdentifierAllocator allocator;
	auto [initialId, initialTag] = allocator.allocate(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT);
	ASSERT_EQ(initialId, 1);
	ASSERT_EQ(initialTag, Tag(tagLocalityCDC, 0));

	allocator.observeStreamId(9);
	allocator.observeTag(initialTag);
	allocator.observeTag(Tag(tagLocalityCDC, 2));
	auto [nextId, nextTag] = allocator.allocate(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT);
	ASSERT_EQ(nextId, 10);
	ASSERT_EQ(nextTag, Tag(tagLocalityCDC, 1));

	NativeCdcIdentifierAllocator publishedPoolAllocator;
	publishedPoolAllocator.observeTag(Tag(tagLocalityCDC, 0));
	// The cluster-controller-published pool is authoritative even when it differs from this process's knob.
	auto [publishedPoolId, publishedPoolTag] = publishedPoolAllocator.allocate(1);
	ASSERT_EQ(publishedPoolId, 1);
	ASSERT_EQ(publishedPoolTag, Tag(tagLocalityCDC, 0));

	NativeCdcIdentifierAllocator fullPoolAllocator;
	for (uint32_t tagId = 0; tagId < static_cast<uint32_t>(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT); ++tagId) {
		fullPoolAllocator.observeTag(Tag(tagLocalityCDC, static_cast<uint16_t>(tagId)));
	}
	auto [sharedId, sharedTag] = fullPoolAllocator.allocate(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT);
	ASSERT_EQ(sharedId, 1);
	ASSERT_EQ(sharedTag, Tag(tagLocalityCDC, 0));

	return Void();
}
