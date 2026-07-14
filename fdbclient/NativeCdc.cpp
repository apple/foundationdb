/*
 * NativeCdc.cpp
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
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "NativeCdcInternal.h"
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

void validateNativeCdcEnabled(bool enabled) {
	if (!enabled) {
		CODE_PROBE(true, "Native CDC registration rejected while feature disabled");
		throw client_invalid_operation();
	}
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
		// TODO: Use data-distributor-observed per-tag write throughput to rebalance CDC tags, including
		// migrating active streams with versioned tag-history assignments.
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

void validateNativeCdcStream(KeyRef const& name, KeyRangeRef const& keys) {
	if (name.empty() || keys.empty() || !normalKeys.contains(keys)) {
		throw client_invalid_operation();
	}
}

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
	Optional<Tag> currentTag;
	const KeyRange historyRange = cdcTagHistoryRangeFor(streamId);
	Key begin = historyRange.begin;
	while (begin < historyRange.end) {
		RangeResult history = co_await tr->getRange(KeyRangeRef(begin, historyRange.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& assignment : history) {
			currentTag = decodeCDCTagHistoryKey(assignment.key).tag;
		}
		if (!history.more) {
			break;
		}
		begin = keyAfter(history.back().key);
	}
	if (!currentTag.present()) {
		throw client_invalid_operation();
	}
	co_return currentTag.get();
}

// TODO: Persist current per-tag ownership so registration does not reconstruct it by scanning all active streams.
Future<Optional<UID>> getNativeCdcProxyAssignmentForTag(Transaction* tr, Tag targetTag) {
	std::set<CDCStreamId> activeStreamIds;
	Key begin = cdcStreamKeys.begin;
	while (begin < cdcStreamKeys.end) {
		RangeResult streams = co_await tr->getRange(KeyRangeRef(begin, cdcStreamKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& stream : streams) {
			activeStreamIds.insert(decodeCDCStreamKey(stream.key));
		}
		if (!streams.more) {
			break;
		}
		begin = keyAfter(streams.back().key);
	}

	std::unordered_map<CDCStreamId, Tag> currentTags;
	begin = cdcTagHistoryKeys.begin;
	while (begin < cdcTagHistoryKeys.end) {
		RangeResult histories =
		    co_await tr->getRange(KeyRangeRef(begin, cdcTagHistoryKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& history : histories) {
			const CDCTagHistoryEntry decoded = decodeCDCTagHistoryKey(history.key);
			if (activeStreamIds.contains(decoded.streamId)) {
				currentTags[decoded.streamId] = decoded.tag;
			}
		}
		if (!histories.more) {
			break;
		}
		begin = keyAfter(histories.back().key);
	}

	for (const auto& [streamId, tag] : currentTags) {
		if (tag == targetTag) {
			Optional<UID> proxyId = co_await getNativeCdcProxyAssignment(tr, streamId);
			if (proxyId.present()) {
				co_return proxyId;
			}
		}
	}
	co_return Optional<UID>();
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

	std::set<CDCStreamId> activeStreamIds;
	Key begin = cdcStreamKeys.begin;
	while (begin < cdcStreamKeys.end) {
		RangeResult streams = co_await tr->getRange(KeyRangeRef(begin, cdcStreamKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& kv : streams) {
			const CDCStreamId streamId = decodeCDCStreamKey(kv.key);
			activeStreamIds.insert(streamId);
			allocator->observeStreamId(streamId);
		}
		if (!streams.more) {
			break;
		}
		begin = keyAfter(streams.back().key);
	}

	std::unordered_map<CDCStreamId, Tag> currentTags;
	begin = cdcTagHistoryKeys.begin;
	while (begin < cdcTagHistoryKeys.end) {
		RangeResult histories =
		    co_await tr->getRange(KeyRangeRef(begin, cdcTagHistoryKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& kv : histories) {
			const CDCTagHistoryEntry history = decodeCDCTagHistoryKey(kv.key);
			allocator->observeStreamId(history.streamId);
			if (activeStreamIds.contains(history.streamId)) {
				currentTags[history.streamId] = history.tag;
			}
		}
		if (!histories.more) {
			break;
		}
		begin = keyAfter(histories.back().key);
	}
	for (const auto& tagAssignment : currentTags) {
		allocator->observeTag(tagAssignment.second);
	}
}

bool retryNativeCdcProxyRequest(Error const& error) {
	return error.code() == error_code_wrong_shard_server || error.code() == error_code_broken_promise ||
	       error.code() == error_code_connection_failed || error.code() == error_code_request_maybe_delivered;
}

bool rewindUnacknowledgedCursorAfterProxyReplacement(CDCCursor* currentPosition,
                                                     Version lastAcknowledgedVersion,
                                                     Optional<UID>* deliveryProxyId,
                                                     UID currentProxyId) {
	const bool proxyReplaced = deliveryProxyId->present() && deliveryProxyId->get() != currentProxyId;
	*deliveryProxyId = currentProxyId;
	if (!proxyReplaced || currentPosition->lastConsumedVersion <= lastAcknowledgedVersion) {
		return false;
	}
	currentPosition->lastConsumedVersion = lastAcknowledgedVersion;
	return true;
}

// TODO: Have the cluster controller rebalance stream ownership using aggregate CDC proxy throughput and
// update cdcProxyKeys and ClientDBInfo assignments; registration currently chooses any available proxy.
Optional<CDCProxyInterface> selectAvailableNativeCdcProxy(ClientDBInfo const& clientInfo, Optional<UID> previousProxy) {
	for (const auto& proxy : clientInfo.cdcProxies) {
		if (!previousProxy.present() || proxy.id() != previousProxy.get()) {
			return proxy;
		}
	}
	if (!clientInfo.cdcProxies.empty()) {
		return clientInfo.cdcProxies.front();
	}
	return Optional<CDCProxyInterface>();
}

bool containsNativeCdcProxy(ClientDBInfo const& clientInfo, UID proxyId) {
	return std::any_of(clientInfo.cdcProxies.begin(),
	                   clientInfo.cdcProxies.end(),
	                   [proxyId](CDCProxyInterface const& proxy) { return proxy.id() == proxyId; });
}

Future<bool> nativeCdcStreamStillExists(Database cx, CDCStreamId streamId) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			co_return (co_await tr.get(cdcStreamKeyFor(streamId))).present();
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Optional<CDCStreamId>> findNativeCdcStreamId(Database cx, Key name) {
	if (name.empty()) {
		throw client_invalid_operation();
	}

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> streamId = co_await tr.get(cdcStreamNameKeyFor(name));
			if (!streamId.present()) {
				co_return Optional<CDCStreamId>();
			}
			co_return decodeCDCStreamNameValue(streamId.get());
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<CDCStreamId> getNativeCdcStreamId(Database cx, Key name) {
	Optional<CDCStreamId> streamId = co_await findNativeCdcStreamId(cx, name);
	if (!streamId.present()) {
		throw client_invalid_operation();
	}
	co_return streamId.get();
}

Future<CDCProxyInterface> getNativeCdcStreamProxy(Database cx, CDCStreamId streamId) {
	if (streamId == 0) {
		throw client_invalid_operation();
	}

	while (true) {
		const ClientDBInfo& clientInfo = cx->clientInfo->get();
		auto assigned = clientInfo.streamToCDCProxyId.find(streamId);
		if (assigned != clientInfo.streamToCDCProxyId.end()) {
			for (const auto& proxy : clientInfo.cdcProxies) {
				if (proxy.id() == assigned->second) {
					co_return proxy;
				}
			}
		}
		if (!(co_await nativeCdcStreamStillExists(cx, streamId))) {
			CODE_PROBE(true, "Native CDC client rejected operation after stream removal");
			throw client_invalid_operation();
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

bool nativeCdcNameMatchesStream(Optional<Value> const& currentId, CDCStreamId streamId) {
	return currentId.present() && decodeCDCStreamNameValue(currentId.get()) == streamId;
}

Future<bool> namedNativeCdcStreamStillExists(Database cx, Key name, CDCStreamId streamId) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> currentId = co_await tr.get(cdcStreamNameKeyFor(name));
			co_return nativeCdcNameMatchesStream(currentId, streamId);
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Optional<CDCProxyInterface>> getNativeCdcStreamProxyForRemoval(Database cx, Key name, CDCStreamId streamId) {
	while (true) {
		const ClientDBInfo& clientInfo = cx->clientInfo->get();
		auto assigned = clientInfo.streamToCDCProxyId.find(streamId);
		if (assigned != clientInfo.streamToCDCProxyId.end()) {
			for (const auto& proxy : clientInfo.cdcProxies) {
				if (proxy.id() == assigned->second) {
					co_return proxy;
				}
			}
		}
		if (!(co_await namedNativeCdcStreamStillExists(cx, name, streamId))) {
			co_return Optional<CDCProxyInterface>();
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

} // namespace

Future<CDCStreamId> registerNativeCdcStream(Database cx, Key name, KeyRange keys, UID proxyId) {
	validateNativeCdcStream(name, keys);

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			const Key nameKey = cdcStreamNameKeyFor(name);
			Optional<Value> currentId = co_await tr.get(nameKey);
			if (currentId.present()) {
				const CDCStreamId streamId = decodeCDCStreamNameValue(currentId.get());
				Optional<Value> currentKeys = co_await tr.get(cdcStreamKeyFor(streamId));
				if (!currentKeys.present() || decodeCDCStreamKeysValue(currentKeys.get()) != keys) {
					throw client_invalid_operation();
				}
				if (!(co_await getNativeCdcProxyAssignment(&tr, streamId)).present()) {
					CODE_PROBE(true, "Native CDC registration restores missing stream owner");
					const Tag tag = co_await getNativeCdcCurrentTag(&tr, streamId);
					Optional<UID> sharedTagProxy = co_await getNativeCdcProxyAssignmentForTag(&tr, tag);
					CODE_PROBE(sharedTagProxy.present(), "Native CDC shared-tag streams use one owner");
					const UID selectedProxy = sharedTagProxy.present() ? sharedTagProxy.get() : proxyId;
					tr.set(cdcProxyKeyFor(streamId, selectedProxy), Value());
					signalNativeCdcProxyAssignmentChange(&tr);
					co_await tr.commit();
				}
				co_return streamId;
			}

			// Disabling CDC stops new admission, but existing registrations and
			// owner repair must remain available so durable streams can drain.
			validateNativeCdcEnabled(cx->clientInfo->get().nativeCdcEnabled);
			NativeCdcIdentifierAllocator allocator;
			co_await observeNativeCdcMetadata(&tr, &allocator);
			const auto [streamId, tag] = allocator.allocate(cx->clientInfo->get().nativeCdcTagCount);
			// The read version is a conservative lower bound for tag routing.
			// The versionstamped minimum below is the commit version, and stream
			// initialization takes their maximum before exposing mutations.
			const Version registrationVersion = co_await tr.getReadVersion();

			tr.set(nameKey, cdcStreamNameValue(streamId));
			tr.set(cdcMaxStreamIdKey, cdcMaxStreamIdValue(streamId));
			tr.set(cdcStreamKeyFor(streamId), cdcStreamKeysValue(keys));
			tr.set(cdcTagHistoryKeyFor(streamId, registrationVersion, tag), Value());
			tr.atomicOp(
			    cdcMinVersionKeyFor(streamId), cdcVersionstampedMinVersionValue(), MutationRef::SetVersionstampedValue);
			Optional<UID> sharedTagProxy = co_await getNativeCdcProxyAssignmentForTag(&tr, tag);
			const UID selectedProxy = sharedTagProxy.present() ? sharedTagProxy.get() : proxyId;
			tr.set(cdcProxyKeyFor(streamId, selectedProxy), Value());
			signalNativeCdcProxyAssignmentChange(&tr);
			co_await tr.commit();
			co_return streamId;
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
				tr.set(cdcRetiredTagPopKeyFor(tag), Value());
				tr.atomicOp(cdcRetiredTagPopVersionKeyFor(tag),
				            cdcVersionstampedMinVersionValue(),
				            MutationRef::SetVersionstampedValue);
			}
			tr.clear(cdcTagHistoryRangeFor(streamId));
			tr.clear(cdcMinVersionKeyFor(streamId));
			tr.clear(cdcProxyRangeFor(streamId));
			if (assignedProxy.present()) {
				signalNativeCdcProxyAssignmentChange(&tr);
			}
			co_await tr.commit();
			CODE_PROBE(!removedTags.empty(), "Native CDC removal records final tagged pop work");
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

Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreams(Database cx) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			std::vector<std::pair<Key, CDCStreamId>> names;
			Key begin = cdcStreamNameKeys.begin;
			while (begin < cdcStreamNameKeys.end) {
				RangeResult page =
				    co_await tr.getRange(KeyRangeRef(begin, cdcStreamNameKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : page) {
					names.emplace_back(decodeCDCStreamNameKey(kv.key), decodeCDCStreamNameValue(kv.value));
				}
				if (!page.more) {
					break;
				}
				begin = keyAfter(page.back().key);
			}

			std::unordered_map<CDCStreamId, KeyRange> streamKeys;
			begin = cdcStreamKeys.begin;
			while (begin < cdcStreamKeys.end) {
				RangeResult page = co_await tr.getRange(KeyRangeRef(begin, cdcStreamKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : page) {
					streamKeys.emplace(decodeCDCStreamKey(kv.key), decodeCDCStreamKeysValue(kv.value));
				}
				if (!page.more) {
					break;
				}
				begin = keyAfter(page.back().key);
			}

			std::unordered_map<CDCStreamId, Version> minVersions;
			begin = cdcMinVersionKeys.begin;
			while (begin < cdcMinVersionKeys.end) {
				RangeResult page =
				    co_await tr.getRange(KeyRangeRef(begin, cdcMinVersionKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : page) {
					minVersions.emplace(decodeCDCMinVersionKey(kv.key), decodeCDCMinVersionValue(kv.value));
				}
				if (!page.more) {
					break;
				}
				begin = keyAfter(page.back().key);
			}

			std::vector<NativeCdcStreamInfo> result;
			result.reserve(names.size());
			for (auto& [name, streamId] : names) {
				auto keys = streamKeys.find(streamId);
				auto minVersion = minVersions.find(streamId);
				if (keys != streamKeys.end() && minVersion != minVersions.end()) {
					result.push_back(
					    NativeCdcStreamInfo{ std::move(name), streamId, keys->second, minVersion->second });
				}
			}
			co_return result;
		} catch (Error& e) {
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

Future<Version> acknowledgeNativeCdcStream(Database cx,
                                           CDCStreamId streamId,
                                           Version consumedThrough,
                                           Version knownAvailableThrough) {
	if (streamId == 0 || consumedThrough < 0 || consumedThrough >= std::numeric_limits<Version>::max() - 1) {
		throw client_invalid_operation();
	}
	const Version minUnpoppedVersion = consumedThrough + 1;

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Optional<Value> minVersionValue = co_await tr.get(cdcMinVersionKeyFor(streamId));
			if (!minVersionValue.present()) {
				throw client_invalid_operation();
			}

			const Version minVersion = decodeCDCMinVersionValue(minVersionValue.get());
			if (minUnpoppedVersion <= minVersion) {
				CODE_PROBE(true, "Native CDC preserves a durable duplicate acknowledgement");
				co_return minVersion;
			}

			const Version readVersion = co_await tr.getReadVersion();
			if (consumedThrough > readVersion && consumedThrough > knownAvailableThrough) {
				CODE_PROBE(true, "Native CDC rejects unproven acknowledgement progress");
				throw client_invalid_operation();
			}

			tr.set(cdcMinVersionKeyFor(streamId), cdcMinVersionValue(minUnpoppedVersion));
			co_await tr.commit();
			co_return minUnpoppedVersion;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<CDCStreamId> registerNativeCdcStreamClient(Database cx, Key name, KeyRange keys) {
	validateNativeCdcStream(name, keys);
	Optional<UID> previousProxy;
	while (true) {
		Future<Void> proxyChanged = cx->clientInfo->onChange();
		Optional<CDCProxyInterface> selectedProxy;
		bool registrationEnabled;
		UID clientInfoId;
		{
			const ClientDBInfo& clientInfo = cx->clientInfo->get();
			selectedProxy = selectAvailableNativeCdcProxy(clientInfo, previousProxy);
			registrationEnabled = clientInfo.nativeCdcEnabled;
			clientInfoId = clientInfo.id;
		}
		if (!registrationEnabled) {
			Optional<CDCStreamId> existingStream = co_await findNativeCdcStreamId(cx, name);
			if (cx->clientInfo->get().id != clientInfoId) {
				CODE_PROBE(true,
				           "Native CDC registration retries after client info changes during existence check",
				           probe::decoration::rare);
				continue;
			}
			if (!existingStream.present()) {
				validateNativeCdcEnabled(registrationEnabled);
			}
		}
		if (!selectedProxy.present()) {
			co_await proxyChanged;
			continue;
		}
		CDCProxyInterface proxy = selectedProxy.get();
		try {
			Future<ErrorOr<CDCRegisterStreamReply>> request =
			    proxy.registerStream.tryGetReply(CDCRegisterStreamRequest(name, keys));
			// Assignment publications for other streams also change ClientDBInfo. Keep this request alive while its
			// proxy remains published; abandoning it can let a server-side retry recreate the stream after removal.
			while (true) {
				auto result = co_await race(throwErrorOr(request), proxyChanged);
				if (result.index() == 0) {
					co_return std::get<0>(result).streamId;
				}

				proxyChanged = cx->clientInfo->onChange();
				if (!containsNativeCdcProxy(cx->clientInfo->get(), proxy.id())) {
					break;
				}
				CODE_PROBE(true, "Native CDC registration preserves request across unrelated client info change");
			}
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
		}
		previousProxy = proxy.id();
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreamsClient(Database cx) {
	co_return co_await listNativeCdcStreams(cx);
}

Future<Void> removeNativeCdcStreamClient(Database cx, Key name) {
	if (name.empty()) {
		throw client_invalid_operation();
	}

	Optional<CDCStreamId> streamId = co_await findNativeCdcStreamId(cx, name);
	if (!streamId.present()) {
		co_return;
	}

	while (true) {
		Optional<CDCProxyInterface> proxy = co_await getNativeCdcStreamProxyForRemoval(cx, name, streamId.get());
		if (!proxy.present()) {
			co_return;
		}
		try {
			Future<Void> proxyChanged = cx->clientInfo->onChange();
			auto result = co_await race(
			    throwErrorOr(proxy.get().removeStream.tryGetReply(CDCRemoveStreamRequest(name, streamId.get()))),
			    proxyChanged);
			if (result.index() == 0) {
				co_return;
			}
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<Reference<NativeCdcConsumer>> createNativeCdcConsumer(Database cx, Key name) {
	const CDCStreamId streamId = co_await getNativeCdcStreamId(cx, name);
	co_return makeReference<NativeCdcConsumer>(cx, CDCCursor(streamId, invalidVersion), invalidVersion);
}

Reference<NativeCdcConsumer> resumeNativeCdcConsumer(Database cx, CDCCursor position) {
	return makeReference<NativeCdcConsumer>(cx, position);
}

Future<CDCConsumeReply> NativeCdcConsumer::consumeImpl(Reference<NativeCdcConsumer> self) {
	try {
		while (true) {
			CDCProxyInterface proxy = co_await getNativeCdcStreamProxy(self->cx, self->currentPosition.streamId);
			if (rewindUnacknowledgedCursorAfterProxyReplacement(
			        &self->currentPosition, self->lastAcknowledgedVersion, &self->deliveryProxyId, proxy.id())) {
				self->knownAvailableThrough = self->lastAcknowledgedVersion;
				CODE_PROBE(true,
				           "Native CDC consumer rewinds unacknowledged cursor after proxy replacement",
				           probe::decoration::rare);
			}
			try {
				CDCConsumeReply reply =
				    co_await throwErrorOr(proxy.consume.tryGetReply(CDCConsumeRequest(self->currentPosition)));
				if (reply.lastConsumedVersion == self->currentPosition.lastConsumedVersion && reply.mutations.empty()) {
					// The server lease bounds abandoned long polls. Renew it transparently so the public consume
					// operation remains a long poll without accumulating server actors after client cancellation.
					CODE_PROBE(true, "Native CDC consume renews an idle server lease", probe::decoration::rare);
					continue;
				}
				self->knownAvailableThrough = reply.lastConsumedVersion;
				self->currentPosition.lastConsumedVersion = reply.lastConsumedVersion;
				self->operationOutstanding = false;
				co_return reply;
			} catch (Error& error) {
				if (!retryNativeCdcProxyRequest(error)) {
					throw;
				}
				CODE_PROBE(true, "Native CDC consume retries after proxy request failure");
			}
			co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, self->cx->taskID);
		}
	} catch (Error&) {
		self->operationOutstanding = false;
		throw;
	}
}

Future<CDCConsumeReply> NativeCdcConsumer::consume() {
	if (operationOutstanding) {
		CODE_PROBE(true, "Native CDC consumer rejects overlapping operations", probe::decoration::rare);
		return Future<CDCConsumeReply>(client_invalid_operation());
	}
	operationOutstanding = true;
	return consumeImpl(Reference<NativeCdcConsumer>::addRef(this));
}

Future<Void> NativeCdcConsumer::acknowledgeImpl(Reference<NativeCdcConsumer> self) {
	try {
		if (self->currentPosition.streamId == 0 || self->currentPosition.lastConsumedVersion < 0 ||
		    self->currentPosition.lastConsumedVersion == std::numeric_limits<Version>::max()) {
			throw client_invalid_operation();
		}
		const Version acknowledgedVersion = self->currentPosition.lastConsumedVersion;
		const Version durableMinVersion = co_await acknowledgeNativeCdcStream(
		    self->cx, self->currentPosition.streamId, acknowledgedVersion, self->knownAvailableThrough);
		self->lastAcknowledgedVersion = std::max(self->lastAcknowledgedVersion, durableMinVersion - 1);
		// The durable transaction completes before the proxy RPC. FoundationDB's
		// transaction ordering guarantees the proxy's subsequent metadata read
		// observes this acknowledgement.

		while (true) {
			CDCProxyInterface proxy = co_await getNativeCdcStreamProxy(self->cx, self->currentPosition.streamId);
			try {
				Future<Void> proxyChanged = self->cx->clientInfo->onChange();
				auto result = co_await race(throwErrorOr(proxy.ack.tryGetReply(
				                                CDCAckRequest(self->currentPosition.streamId, acknowledgedVersion))),
				                            proxyChanged);
				if (result.index() == 0) {
					self->operationOutstanding = false;
					co_return;
				}
			} catch (Error& error) {
				if (!retryNativeCdcProxyRequest(error)) {
					throw;
				}
			}
			co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, self->cx->taskID);
		}
	} catch (Error&) {
		self->operationOutstanding = false;
		throw;
	}
}

Future<Void> NativeCdcConsumer::acknowledge() {
	if (operationOutstanding) {
		CODE_PROBE(true, "Native CDC consumer rejects overlapping operations", probe::decoration::rare);
		return Future<Void>(client_invalid_operation());
	}
	operationOutstanding = true;
	return acknowledgeImpl(Reference<NativeCdcConsumer>::addRef(this));
}

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

TEST_CASE("/NativeCDC/ConsumerRewindsUnacknowledgedCursorOnProxyReplacement") {
	CDCCursor cursor(1, invalidVersion);
	Optional<UID> deliveryProxyId;
	const UID firstProxy(1, 2);
	const UID secondProxy(3, 4);

	ASSERT(!rewindUnacknowledgedCursorAfterProxyReplacement(&cursor, invalidVersion, &deliveryProxyId, firstProxy));
	cursor.lastConsumedVersion = 100;
	ASSERT(!rewindUnacknowledgedCursorAfterProxyReplacement(&cursor, invalidVersion, &deliveryProxyId, firstProxy));
	ASSERT(rewindUnacknowledgedCursorAfterProxyReplacement(&cursor, invalidVersion, &deliveryProxyId, secondProxy));
	ASSERT_EQ(cursor.lastConsumedVersion, invalidVersion);

	cursor.lastConsumedVersion = 100;
	ASSERT(rewindUnacknowledgedCursorAfterProxyReplacement(&cursor, 80, &deliveryProxyId, firstProxy));
	ASSERT_EQ(cursor.lastConsumedVersion, 80);
	ASSERT(!rewindUnacknowledgedCursorAfterProxyReplacement(&cursor, 80, &deliveryProxyId, secondProxy));
	ASSERT_EQ(cursor.lastConsumedVersion, 80);

	return Void();
}

TEST_CASE("/NativeCDC/RemovalMatchesOriginalStream") {
	const CDCStreamId originalStreamId = 1;
	ASSERT(nativeCdcNameMatchesStream(Optional<Value>(cdcStreamNameValue(originalStreamId)), originalStreamId));
	ASSERT(!nativeCdcNameMatchesStream(Optional<Value>(cdcStreamNameValue(originalStreamId + 1)), originalStreamId));
	ASSERT(!nativeCdcNameMatchesStream(Optional<Value>(), originalStreamId));

	return Void();
}
