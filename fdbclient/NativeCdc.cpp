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
#include <map>
#include <set>
#include <unordered_map>
#include <utility>
#include <vector>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"

namespace {

void validateNativeCdcEnabled() {
	if (!CLIENT_KNOBS->ENABLE_NATIVE_CDC) {
		throw client_invalid_operation();
	}
}

struct NativeCdcIdentifierAllocator {
	bool sawStream = false;
	CDCStreamId maxStreamId = 0;
	std::unordered_map<uint16_t, uint32_t> tagStreamCounts;

	void observeStreamId(CDCStreamId streamId) {
		sawStream = true;
		maxStreamId = std::max(maxStreamId, streamId);
	}

	void observeTag(Tag tag) {
		ASSERT_WE_THINK(tag.locality == tagLocalityCDC);
		++tagStreamCounts[tag.id];
	}

	std::pair<CDCStreamId, Tag> allocate() const {
		if (sawStream && maxStreamId == std::numeric_limits<CDCStreamId>::max()) {
			throw operation_failed();
		}

		const CDCStreamId streamId = sawStream ? maxStreamId + 1 : 1;
		ASSERT_WE_THINK(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT > 0);
		ASSERT_WE_THINK(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT <= std::numeric_limits<uint16_t>::max() + 1u);
		uint32_t leastStreams = std::numeric_limits<uint32_t>::max();
		uint16_t selectedTagId = 0;
		// TODO: Use data-distributor-observed per-tag write throughput to rebalance CDC tags, including
		// migrating active streams with versioned tag-history assignments.
		for (uint32_t tagId = 0; tagId < static_cast<uint32_t>(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT); ++tagId) {
			auto count = tagStreamCounts.find(static_cast<uint16_t>(tagId));
			const uint32_t streamCount = count == tagStreamCounts.end() ? 0 : count->second;
			if (streamCount < leastStreams) {
				leastStreams = streamCount;
				selectedTagId = static_cast<uint16_t>(tagId);
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
	ASSERT(assignments.size() <= 1);
	if (assignments.empty()) {
		co_return Optional<UID>();
	}
	const auto [assignedStreamId, proxyId] = decodeCDCProxyKey(assignments[0].key);
	ASSERT_WE_THINK(assignedStreamId == streamId);
	co_return proxyId;
}

void signalNativeCdcProxyAssignmentChange(Transaction* tr) {
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

	std::map<CDCStreamId, Tag> currentTags;
	begin = cdcTagHistoryKeys.begin;
	while (begin < cdcTagHistoryKeys.end) {
		RangeResult histories =
		    co_await tr->getRange(KeyRangeRef(begin, cdcTagHistoryKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& kv : histories) {
			const auto history = decodeCDCTagHistoryKey(kv.key);
			allocator->observeStreamId(std::get<0>(history));
			if (activeStreamIds.contains(std::get<0>(history))) {
				currentTags[std::get<0>(history)] = std::get<2>(history);
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

// TODO: Have the cluster controller rebalance stream ownership using aggregate CDC proxy throughput and
// update cdcProxyKeys and ClientDBInfo assignments; registration currently chooses any available proxy.
Future<CDCProxyInterface> getAvailableNativeCdcProxy(Database cx, Optional<UID> previousProxy = Optional<UID>()) {
	while (true) {
		for (const auto& proxy : cx->clientInfo->get().cdcProxies) {
			if (!previousProxy.present() || proxy.id() != previousProxy.get()) {
				co_return proxy;
			}
		}
		if (!cx->clientInfo->get().cdcProxies.empty()) {
			co_return cx->clientInfo->get().cdcProxies.front();
		}
		co_await cx->clientInfo->onChange();
	}
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

Future<CDCStreamId> getNativeCdcStreamId(Database cx, Key name) {
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
				throw client_invalid_operation();
			}
			co_return decodeCDCStreamNameValue(streamId.get());
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
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
			throw client_invalid_operation();
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<bool> namedNativeCdcStreamStillExists(Database cx, Key name, CDCStreamId streamId) {
	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			Optional<Value> currentId = co_await tr.get(cdcStreamNameKeyFor(name));
			co_return currentId.present() && decodeCDCStreamNameValue(currentId.get()) == streamId;
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

Future<CDCStreamId> registerNativeCdcStream(Database cx, Key name, KeyRange keys, Optional<UID> proxyId) {
	validateNativeCdcEnabled();
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
				if (proxyId.present() && !(co_await getNativeCdcProxyAssignment(&tr, streamId)).present()) {
					tr.set(cdcProxyKeyFor(streamId, proxyId.get()), Value());
					signalNativeCdcProxyAssignmentChange(&tr);
					co_await tr.commit();
				}
				co_return streamId;
			}

			NativeCdcIdentifierAllocator allocator;
			co_await observeNativeCdcMetadata(&tr, &allocator);
			const auto [streamId, tag] = allocator.allocate();
			const Version registrationVersion = co_await tr.getReadVersion();

			tr.set(nameKey, cdcStreamNameValue(streamId));
			tr.set(cdcMaxStreamIdKey, cdcMaxStreamIdValue(streamId));
			tr.set(cdcStreamKeyFor(streamId), cdcStreamKeysValue(keys));
			tr.set(cdcTagHistoryKeyFor(streamId, registrationVersion, tag), Value());
			tr.atomicOp(
			    cdcMinVersionKeyFor(streamId), cdcVersionstampedMinVersionValue(), MutationRef::SetVersionstampedValue);
			if (proxyId.present()) {
				tr.set(cdcProxyKeyFor(streamId, proxyId.get()), Value());
				signalNativeCdcProxyAssignmentChange(&tr);
			}
			co_await tr.commit();
			co_return streamId;
		} catch (Error& e) {
			err = e;
		}
		co_await tr.onError(err);
	}
}

Future<Optional<NativeCdcRemovedStreamInfo>> removeNativeCdcStream(Database cx, Key name, Optional<UID> proxyId) {
	if (name.empty()) {
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
			if (!currentId.present()) {
				co_return Optional<NativeCdcRemovedStreamInfo>();
			}

			const CDCStreamId streamId = decodeCDCStreamNameValue(currentId.get());
			Optional<UID> assignedProxy = co_await getNativeCdcProxyAssignment(&tr, streamId);
			if (proxyId.present() && (!assignedProxy.present() || assignedProxy.get() != proxyId.get())) {
				throw wrong_shard_server();
			}

			std::set<Tag> removedTags;
			const KeyRange historyRange = cdcTagHistoryRangeFor(streamId);
			Key begin = historyRange.begin;
			while (begin < historyRange.end) {
				RangeResult history =
				    co_await tr.getRange(KeyRangeRef(begin, historyRange.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& entry : history) {
					removedTags.insert(std::get<2>(decodeCDCTagHistoryKey(entry.key)));
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
			NativeCdcRemovedStreamInfo removed;
			removed.removalVersion = tr.getCommittedVersion();
			removed.tags.assign(removedTags.begin(), removedTags.end());
			co_return Optional<NativeCdcRemovedStreamInfo>(removed);
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
	std::vector<NativeCdcStreamInfo> result;
	Key begin = cdcStreamNameKeys.begin;
	Transaction tr(cx);

	while (begin < cdcStreamNameKeys.end) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			RangeResult names = co_await tr.getRange(KeyRangeRef(begin, cdcStreamNameKeys.end), CLIENT_KNOBS->TOO_MANY);
			for (const auto& kv : names) {
				const CDCStreamId streamId = decodeCDCStreamNameValue(kv.value);
				Optional<Value> keys = co_await tr.get(cdcStreamKeyFor(streamId));
				Optional<Value> minVersion = co_await tr.get(cdcMinVersionKeyFor(streamId));
				if (keys.present() && minVersion.present()) {
					result.push_back(NativeCdcStreamInfo{ decodeCDCStreamNameKey(kv.key),
					                                      streamId,
					                                      decodeCDCStreamKeysValue(keys.get()),
					                                      decodeCDCMinVersionValue(minVersion.get()) });
				}
			}
			if (!names.more) {
				break;
			}
			begin = keyAfter(names.back().key);
			continue;
		} catch (Error& e) {
			err = e;
		}
		result.clear();
		begin = cdcStreamNameKeys.begin;
		co_await tr.onError(err);
	}
	co_return result;
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

Future<Version> acknowledgeNativeCdcStream(Database cx, CDCStreamId streamId, Version consumedThrough) {
	if (streamId == 0 || consumedThrough < 0 || consumedThrough == std::numeric_limits<Version>::max()) {
		throw client_invalid_operation();
	}
	const Version minUnpoppedVersion = consumedThrough + 1;

	Transaction tr(cx);
	while (true) {
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			Optional<Value> minVersionValue = co_await tr.get(cdcMinVersionKeyFor(streamId));
			if (!minVersionValue.present()) {
				throw client_invalid_operation();
			}

			const Version readVersion = co_await tr.getReadVersion();
			if (consumedThrough > readVersion) {
				throw client_invalid_operation();
			}

			const Version minVersion = decodeCDCMinVersionValue(minVersionValue.get());
			if (minUnpoppedVersion <= minVersion) {
				co_return minVersion;
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
	validateNativeCdcEnabled();
	validateNativeCdcStream(name, keys);
	Optional<UID> previousProxy;

	while (true) {
		CDCProxyInterface proxy = co_await getAvailableNativeCdcProxy(cx, previousProxy);
		try {
			CDCRegisterStreamReply reply = co_await proxy.registerStream.getReply(CDCRegisterStreamRequest(name, keys));
			co_return reply.streamId;
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
			previousProxy = proxy.id();
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreamsClient(Database cx) {
	validateNativeCdcEnabled();
	Optional<UID> previousProxy;

	while (true) {
		CDCProxyInterface proxy = co_await getAvailableNativeCdcProxy(cx, previousProxy);
		try {
			CDCListStreamsReply reply = co_await proxy.listStreams.getReply(CDCListStreamsRequest());
			std::vector<NativeCdcStreamInfo> streams;
			streams.reserve(reply.streams.size());
			for (const auto& stream : reply.streams) {
				streams.push_back(
				    NativeCdcStreamInfo{ Key(stream.name), stream.streamId, KeyRange(stream.keys), stream.minVersion });
			}
			co_return streams;
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
			previousProxy = proxy.id();
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<Void> removeNativeCdcStreamClient(Database cx, Key name) {
	validateNativeCdcEnabled();
	if (name.empty()) {
		throw client_invalid_operation();
	}

	while (true) {
		std::vector<NativeCdcStreamInfo> streams = co_await listNativeCdcStreamsClient(cx);
		auto stream = std::find_if(
		    streams.begin(), streams.end(), [&](NativeCdcStreamInfo const& info) { return info.name == name; });
		if (stream == streams.end()) {
			co_return;
		}

		Optional<CDCProxyInterface> proxy = co_await getNativeCdcStreamProxyForRemoval(cx, name, stream->streamId);
		if (!proxy.present()) {
			co_return;
		}
		try {
			co_await proxy.get().removeStream.getReply(CDCRemoveStreamRequest(name));
			co_return;
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<CDCCursor> createNativeCdcCursor(Database cx, Key name) {
	validateNativeCdcEnabled();
	const CDCStreamId streamId = co_await getNativeCdcStreamId(cx, name);
	co_return CDCCursor(streamId, invalidVersion);
}

Future<CDCConsumeReply> consumeNativeCdcStream(Database cx, CDCCursor cursor) {
	validateNativeCdcEnabled();
	while (true) {
		CDCProxyInterface proxy = co_await getNativeCdcStreamProxy(cx, cursor.streamId);
		try {
			co_return co_await proxy.consume.getReply(CDCConsumeRequest(cursor));
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<Void> acknowledgeNativeCdcStreamClient(Database cx, CDCCursor cursor) {
	validateNativeCdcEnabled();
	if (cursor.streamId == 0 || cursor.lastConsumedVersion < 0 ||
	    cursor.lastConsumedVersion == std::numeric_limits<Version>::max()) {
		throw client_invalid_operation();
	}

	while (true) {
		CDCProxyInterface proxy = co_await getNativeCdcStreamProxy(cx, cursor.streamId);
		try {
			co_await proxy.ack.getReply(CDCAckRequest(cursor.streamId, cursor.lastConsumedVersion));
			co_return;
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

TEST_CASE("/NativeCDC/LifecycleAllocation") {
	NativeCdcIdentifierAllocator allocator;
	auto [initialId, initialTag] = allocator.allocate();
	ASSERT(initialId == 1);
	ASSERT(initialTag == Tag(tagLocalityCDC, 0));

	allocator.observeStreamId(9);
	allocator.observeTag(initialTag);
	allocator.observeTag(Tag(tagLocalityCDC, 2));
	auto [nextId, nextTag] = allocator.allocate();
	ASSERT(nextId == 10);
	ASSERT(nextTag == Tag(tagLocalityCDC, 1));

	NativeCdcIdentifierAllocator fullPoolAllocator;
	for (uint32_t tagId = 0; tagId < static_cast<uint32_t>(CLIENT_KNOBS->NATIVE_CDC_TAG_COUNT); ++tagId) {
		fullPoolAllocator.observeTag(Tag(tagLocalityCDC, static_cast<uint16_t>(tagId)));
	}
	auto [sharedId, sharedTag] = fullPoolAllocator.allocate();
	ASSERT(sharedId == 1);
	ASSERT(sharedTag == Tag(tagLocalityCDC, 0));

	return Void();
}
