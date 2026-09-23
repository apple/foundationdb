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
#include "NativeCdcInternal.h"
#include "NativeCdcOrderedMetadata.h"
#include "NativeCdcOrderedMerge.h"
#include "NativeCdcOrderedLifecycle.h"
#include "flow/CodeProbe.h"
#include "flow/Error.h"
#include "flow/ScopeExit.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"

void validateNativeCdcEnabled(bool enabled) {
	if (!enabled) {
		CODE_PROBE(true, "Native CDC registration rejected while feature disabled");
		throw client_invalid_operation();
	}
}

void normalizeNativeCdcStreamRanges(KeyRef const& name, std::vector<KeyRange>& ranges) {
	if (name.empty() || ranges.empty() || ranges.size() > NATIVE_CDC_MAX_RANGES) {
		throw client_invalid_operation();
	}
	for (const auto& range : ranges) {
		if (range.begin >= range.end || !normalKeys.contains(range)) {
			throw client_invalid_operation();
		}
	}
	std::sort(
	    ranges.begin(), ranges.end(), [](const KeyRange& lhs, const KeyRange& rhs) { return lhs.begin < rhs.begin; });
	size_t count = 0;
	for (const auto& range : ranges) {
		if (count > 0 && range.begin <= ranges[count - 1].end) {
			if (range.end > ranges[count - 1].end) {
				ranges[count - 1] = KeyRange(KeyRangeRef(ranges[count - 1].begin, range.end));
			}
		} else {
			ranges[count++] = range;
		}
	}
	ranges.resize(count);

	int64_t keyBytes = 0;
	for (const auto& range : ranges) {
		// Single-key ranges serialize only the end key, so count the encoded payload before allocating metadata.
		keyBytes += static_cast<int64_t>(range.end.size()) + (range.singleKeyRange() ? 0 : range.begin.size());
	}
	if (keyBytes > CLIENT_KNOBS->VALUE_SIZE_LIMIT ||
	    cdcStreamKeysValue(ranges).size() > CLIENT_KNOBS->VALUE_SIZE_LIMIT) {
		throw client_invalid_operation();
	}
}

bool nativeCdcNameMatchesStream(Optional<Value> const& currentId, CDCStreamId streamId) {
	return currentId.present() && decodeCDCStreamNameValue(currentId.get()) == streamId;
}

namespace {

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

// TODO: Use measured aggregate CDC proxy throughput instead of stream counts when balancing ownership;
// registration currently chooses any available proxy before the controller's opt-in balancing pass.
Optional<CDCProxyInterface> selectAvailableNativeCdcProxy(ClientDBInfo const& clientInfo,
                                                          Optional<UID> previousProxy,
                                                          bool requireOrdered = false) {
	Optional<CDCProxyInterface> fallback;
	for (const auto& proxy : clientInfo.cdcProxies) {
		if (requireOrdered && !proxy.supportsOrderedStreams) {
			continue;
		}
		if (!fallback.present()) {
			fallback = proxy;
		}
		if (!previousProxy.present() || proxy.id() != previousProxy.get()) {
			return proxy;
		}
	}
	return fallback;
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
		if ((co_await readNativeCdcOrderedSnapshot(cx, streamId)).present()) {
			const ClientDBInfo& currentInfo = cx->clientInfo->get();
			const auto proxy = selectAvailableNativeCdcProxy(currentInfo, {}, true);
			if (proxy.present()) {
				co_return proxy;
			}
			if (!currentInfo.cdcProxies.empty()) {
				throw unsupported_operation();
			}
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, cx->taskID);
	}
}

Future<Void> removeNativeCdcStreamById(Database cx, Key name, CDCStreamId streamId) {
	while (true) {
		Optional<CDCProxyInterface> proxy = co_await getNativeCdcStreamProxyForRemoval(cx, name, streamId);
		if (!proxy.present()) {
			co_return;
		}
		try {
			Future<Void> proxyChanged = cx->clientInfo->onChange();
			auto result = co_await race(
			    throwErrorOr(proxy.get().removeStream.tryGetReply(CDCRemoveStreamRequest(name, streamId))),
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

Future<Standalone<VectorRef<KeyValueRef>>> readNativeCdcStatusRange(Transaction* tr, KeyRange keys) {
	Standalone<VectorRef<KeyValueRef>> result;
	Key begin = keys.begin;
	while (begin < keys.end) {
		RangeResult page = co_await tr->getRange(KeyRangeRef(begin, keys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& entry : page) {
			result.push_back_deep(result.arena(), entry);
		}
		if (!page.more) {
			break;
		}
		begin = keyAfter(page.back().key);
	}
	co_return result;
}

Future<NativeCdcProxyStatus> sampleNativeCdcProxy(CDCProxyInterface proxy, std::vector<CDCStreamId> streamIds) {
	NativeCdcProxyStatus result;
	result.id = proxy.id();
	result.address = proxy.address();
	try {
		std::vector<Future<CDCProxyStatusReply>> samples;
		for (size_t begin = 0; begin < streamIds.size() || samples.empty();
		     begin += GetCDCProxyStatusRequest::MAX_STREAMS) {
			const size_t end = std::min(streamIds.size(), begin + GetCDCProxyStatusRequest::MAX_STREAMS);
			GetCDCProxyStatusRequest request;
			request.streamIds.assign(streamIds.begin() + begin, streamIds.begin() + end);
			samples.push_back(timeoutError(throwErrorOr(proxy.getStatus.tryGetReply(request)),
			                               CLIENT_KNOBS->NATIVE_CDC_STATUS_TIMEOUT));
		}
		co_await waitForAll(samples);
		CDCProxyStatusReply combined = samples.front().get();
		for (size_t i = 1; i < samples.size(); ++i) {
			const auto& streams = samples[i].get().streams;
			combined.streams.insert(combined.streams.end(), streams.begin(), streams.end());
		}
		result.sample = std::move(combined);
	} catch (Error& error) {
		if (error.code() == error_code_actor_cancelled) {
			throw;
		}
		result.error = error;
	}
	co_return result;
}

} // namespace

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

			std::unordered_map<CDCStreamId, std::vector<KeyRange>> streamRanges;
			begin = cdcStreamKeys.begin;
			while (begin < cdcStreamKeys.end) {
				RangeResult page = co_await tr.getRange(KeyRangeRef(begin, cdcStreamKeys.end), CLIENT_KNOBS->TOO_MANY);
				for (const auto& kv : page) {
					streamRanges.emplace(decodeCDCStreamKey(kv.key), decodeCDCStreamKeysValue(kv.value));
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
				const auto ordered = co_await readNativeCdcOrderedSnapshot(&tr, streamId);
				if (ordered.present()) {
					result.push_back(NativeCdcStreamInfo{
					    std::move(name), streamId, ordered.get().metadata.ranges(), ordered.get().minVersion });
					continue;
				}
				auto ranges = streamRanges.find(streamId);
				auto minVersion = minVersions.find(streamId);
				if (ranges != streamRanges.end() && minVersion != minVersions.end()) {
					result.push_back(NativeCdcStreamInfo{
					    std::move(name), streamId, std::move(ranges->second), minVersion->second });
				}
			}
			co_return result;
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
			if ((co_await tr.get(cdcOrderedParentKeyFor(streamId))).present()) {
				// Only an aggregate acknowledgement may advance a partition. The duplicate
				// case above remains valid for its subsequent proxy notification.
				throw client_invalid_operation();
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

namespace {

Future<CDCStreamId> registerNativeCdcStreamClientImpl(Database cx,
                                                      Key name,
                                                      std::vector<KeyRange> ranges,
                                                      Optional<std::vector<Key>> splitPoints) {
	normalizeNativeCdcStreamRanges(name, ranges);
	if (splitPoints.present()) {
		nativeCdcOrderedPartitionRanges(ranges, splitPoints.get());
	}
	Optional<UID> previousProxy;
	while (true) {
		Future<Void> proxyChanged = cx->clientInfo->onChange();
		Optional<CDCProxyInterface> selectedProxy;
		bool registrationEnabled;
		UID clientInfoId;
		{
			const ClientDBInfo& clientInfo = cx->clientInfo->get();
			selectedProxy = selectAvailableNativeCdcProxy(clientInfo, previousProxy, splitPoints.present());
			if (splitPoints.present() &&
			    std::any_of(clientInfo.cdcProxies.begin(),
			                clientInfo.cdcProxies.end(),
			                [](const CDCProxyInterface& proxy) { return !proxy.supportsOrderedStreams; })) {
				throw unsupported_operation();
			}
			registrationEnabled = clientInfo.nativeCdcEnabled;
			clientInfoId = clientInfo.id;
		}
		if (!registrationEnabled) {
			Optional<CDCStreamId> existingStream = co_await findNativeCdcStreamId(cx, name);
			if (cx->clientInfo->get().id != clientInfoId) {
				CODE_PROBE(true, "Native CDC registration retries after client info changes during existence check");
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
			    splitPoints.present() ? proxy.registerOrderedStream.tryGetReply(
			                                CDCRegisterOrderedStreamRequest(name, ranges, splitPoints.get()))
			                          : proxy.registerStream.tryGetReply(CDCRegisterStreamRequest(name, ranges));
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

} // namespace

Future<CDCStreamId> registerNativeCdcStreamClient(Database cx, Key name, std::vector<KeyRange> ranges) {
	return registerNativeCdcStreamClientImpl(cx, name, std::move(ranges), {});
}

Future<CDCStreamId> registerNativeCdcOrderedStreamClient(Database cx,
                                                         Key name,
                                                         std::vector<KeyRange> ranges,
                                                         std::vector<Key> splitPoints) {
	return registerNativeCdcStreamClientImpl(cx, name, std::move(ranges), std::move(splitPoints));
}

Future<std::vector<NativeCdcStreamInfo>> listNativeCdcStreamsClient(Database cx) {
	co_return co_await listNativeCdcStreams(cx);
}

Future<NativeCdcStatus> getNativeCdcStatus(Database cx) {
	NativeCdcStatus result;
	ClientDBInfo clientInfo;
	Transaction tr(cx);
	while (true) {
		Error error;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			result = NativeCdcStatus();
			result.readVersion = co_await tr.getReadVersion();
			std::vector<Future<Standalone<VectorRef<KeyValueRef>>>> metadata;
			for (KeyRangeRef keys : { cdcStreamNameKeys,
			                          cdcStreamKeys,
			                          cdcMinVersionKeys,
			                          cdcProxyKeys,
			                          cdcTagHistoryKeys,
			                          cdcRetiredTagPopKeys,
			                          cdcRetiredTagPopVersionKeys,
			                          cdcOrderedStreamKeys,
			                          cdcOrderedParentKeys }) {
				metadata.push_back(readNativeCdcStatusRange(&tr, keys));
			}
			co_await waitForAll(metadata);

			std::map<CDCStreamId, NativeCdcStreamStatus> streams;
			for (const auto& entry : metadata[0].get()) {
				auto& stream = streams[decodeCDCStreamNameValue(entry.value)];
				if (!stream.info.name.empty()) {
					result.metadataComplete = false;
				}
				stream.info.name = decodeCDCStreamNameKey(entry.key);
			}
			for (const auto& entry : metadata[1].get()) {
				streams[decodeCDCStreamKey(entry.key)].info.ranges = decodeCDCStreamKeysValue(entry.value);
			}
			for (const auto& entry : metadata[2].get()) {
				streams[decodeCDCMinVersionKey(entry.key)].info.minVersion = decodeCDCMinVersionValue(entry.value);
			}
			for (const auto& entry : metadata[3].get()) {
				const auto [streamId, proxyId] = decodeCDCProxyKey(entry.key);
				auto& stream = streams[streamId];
				if (stream.owner.present()) {
					result.metadataComplete = false;
				}
				stream.owner = proxyId;
			}
			for (const auto& entry : metadata[4].get()) {
				const auto history = decodeCDCTagHistoryKey(entry.key);
				streams[history.streamId].tags.push_back(history.tag);
			}
			for (const auto& entry : metadata[8].get()) {
				streams[decodeCDCOrderedParentKey(entry.key)].orderedParent = decodeCDCOrderedParentValue(entry.value);
			}
			for (const auto& entry : metadata[7].get()) {
				const CDCStreamId logicalId = decodeCDCOrderedStreamKey(entry.key);
				const auto group = decodeCDCOrderedStreamValue(entry.value);
				const auto partitionRanges = nativeCdcOrderedPartitionRanges(group.ranges(), group.splitPoints());
				auto& parent = streams[logicalId];
				if (!parent.info.ranges.empty() || parent.owner.present() || !parent.tags.empty() ||
				    parent.info.minVersion != invalidVersion || parent.orderedParent.present()) {
					result.metadataComplete = false;
				}
				parent.info.ranges = group.ranges();
				parent.partitions = group.partitions();
				bool commonMinimum = true;
				for (size_t i = 0; i < group.partitions().size(); ++i) {
					const CDCStreamId childId = group.partitions()[i];
					if (childId == logicalId) {
						result.metadataComplete = false;
						commonMinimum = false;
						continue;
					}
					auto& child = streams[childId];
					if (child.orderedParent != Optional<CDCStreamId>(logicalId) || !child.info.name.empty() ||
					    child.info.ranges != partitionRanges[i]) {
						result.metadataComplete = false;
					}
					child.info.name = parent.info.name;
					if (i == 0) {
						parent.info.minVersion = child.info.minVersion;
					} else if (child.info.minVersion != parent.info.minVersion) {
						commonMinimum = false;
					}
					parent.tags.insert(parent.tags.end(), child.tags.begin(), child.tags.end());
				}
				if (!commonMinimum) {
					parent.info.minVersion = invalidVersion;
					result.metadataComplete = false;
				}
			}
			for (const auto& [streamId, stream] : streams) {
				if (stream.orderedParent.present()) {
					const auto parent = streams.find(stream.orderedParent.get());
					if (parent == streams.end() ||
					    std::find(parent->second.partitions.begin(), parent->second.partitions.end(), streamId) ==
					        parent->second.partitions.end()) {
						result.metadataComplete = false;
					}
				}
			}

			std::map<Tag, NativeCdcTagStatus> tags;
			std::set<Tag> incompleteTags;
			for (auto& [streamId, stream] : streams) {
				stream.info.streamId = streamId;
				std::sort(stream.tags.begin(), stream.tags.end());
				stream.tags.erase(std::unique(stream.tags.begin(), stream.tags.end()), stream.tags.end());
				if (stream.info.name.empty() || stream.info.ranges.empty() ||
				    stream.info.minVersion == invalidVersion || stream.tags.empty()) {
					result.metadataComplete = false;
				}
				for (const Tag& tag : stream.tags) {
					// Logical parents summarize their children; only physical rows retain log history.
					if (!stream.partitions.empty()) {
						continue;
					}
					auto& status = tags[tag];
					status.tag = tag;
					if (stream.info.minVersion == invalidVersion) {
						incompleteTags.insert(tag);
					} else if (status.safePopVersion == invalidVersion ||
					           stream.info.minVersion < status.safePopVersion) {
						status.safePopVersion = stream.info.minVersion;
						status.blockingStreams = { streamId };
					} else if (stream.info.minVersion == status.safePopVersion) {
						status.blockingStreams.push_back(streamId);
					}
				}
				result.streams.push_back(std::move(stream));
			}
			for (const auto& entry : metadata[5].get()) {
				const Tag tag = decodeCDCRetiredTagPopKey(entry.key);
				tags[tag].tag = tag;
				tags[tag].pendingRetiredPop = true;
			}
			for (const auto& entry : metadata[6].get()) {
				const Tag tag = decodeCDCRetiredTagPopVersionKey(entry.key);
				auto& status = tags[tag];
				status.tag = tag;
				if (!status.pendingRetiredPop) {
					result.metadataComplete = false;
				}
				status.pendingRetiredPop = true;
				status.retiredPopVersion = decodeCDCMinVersionValue(entry.value);
			}
			for (auto& [tag, status] : tags) {
				if (status.pendingRetiredPop && status.retiredPopVersion == invalidVersion) {
					result.metadataComplete = false;
				}
				if (status.safePopVersion == invalidVersion) {
					status.safePopVersion = status.retiredPopVersion;
				}
				if (incompleteTags.contains(tag)) {
					status.safePopVersion = invalidVersion;
				}
				result.tags.push_back(std::move(status));
			}
			clientInfo = cx->clientInfo->get();
			break;
		} catch (Error& e) {
			error = e;
		}
		co_await tr.onError(error);
	}

	result.admissionEnabled = clientInfo.nativeCdcEnabled;
	result.tagCount = clientInfo.nativeCdcTagCount;
	std::map<UID, std::vector<CDCStreamId>> requests;
	for (const auto& proxy : clientInfo.cdcProxies) {
		requests[proxy.id()];
	}
	for (auto& stream : result.streams) {
		if (stream.owner.present()) {
			auto request = requests.find(stream.owner.get());
			const auto published = clientInfo.streamToCDCProxyId.find(stream.info.streamId);
			stream.ownerPublished = request != requests.end() && published != clientInfo.streamToCDCProxyId.end() &&
			                        published->second == stream.owner.get();
			if (request != requests.end()) {
				request->second.push_back(stream.info.streamId);
			}
		}
	}
	std::map<CDCStreamId, bool> publishedOwners;
	for (const auto& stream : result.streams) {
		publishedOwners.emplace(stream.info.streamId, stream.ownerPublished);
	}
	for (auto& stream : result.streams) {
		if (!stream.partitions.empty()) {
			stream.ownerPublished =
			    std::all_of(stream.partitions.begin(), stream.partitions.end(), [&](CDCStreamId child) {
				    const auto owner = publishedOwners.find(child);
				    return owner != publishedOwners.end() && owner->second;
			    });
		}
	}
	std::vector<Future<NativeCdcProxyStatus>> samples;
	samples.reserve(clientInfo.cdcProxies.size());
	for (const auto& proxy : clientInfo.cdcProxies) {
		samples.push_back(sampleNativeCdcProxy(proxy, std::move(requests[proxy.id()])));
	}
	co_await waitForAll(samples);
	for (const auto& sample : samples) {
		result.proxies.push_back(sample.get());
	}
	co_return result;
}

Future<Void> removeNativeCdcStreamClient(Database cx, Key name) {
	if (name.empty()) {
		throw client_invalid_operation();
	}

	Optional<CDCStreamId> streamId = co_await findNativeCdcStreamId(cx, name);
	if (!streamId.present()) {
		co_return;
	}

	co_await removeNativeCdcStreamById(cx, name, streamId.get());
}

Future<NativeCdcRemoveResult> removeNativeCdcStreamGuarded(Database cx, Key name, CDCStreamId expectedStreamId) {
	if (name.empty() || expectedStreamId == 0) {
		throw client_invalid_operation();
	}
	Optional<CDCStreamId> current = co_await findNativeCdcStreamId(cx, name);
	if (!current.present()) {
		co_return NativeCdcRemoveResult::AlreadyAbsent;
	}
	if (current.get() != expectedStreamId) {
		co_return NativeCdcRemoveResult::StreamReplaced;
	}
	co_await removeNativeCdcStreamById(cx, name, expectedStreamId);
	current = co_await findNativeCdcStreamId(cx, name);
	if (current.present()) {
		if (current.get() != expectedStreamId) {
			co_return NativeCdcRemoveResult::StreamReplaced;
		}
		throw operation_failed();
	}
	co_return NativeCdcRemoveResult::Removed;
}

class NativeCdcConsumer::OrderedState : public ReferenceCounted<NativeCdcConsumer::OrderedState> {
	friend class NativeCdcConsumer;
	NativeCdcOrderedMetadata metadata;
	NativeCdcOrderedMerge merge;
	std::vector<Reference<NativeCdcConsumer>> children;
	std::vector<Optional<UID>> owners;
	std::vector<size_t> readIndexes;
	std::vector<Future<CDCConsumeReply>> reads;
	int64_t childReplyByteLimit;
	int64_t aggregateReplyByteLimit;
	bool resetPending = false;

	void cancelReads() {
		for (auto& read : reads) {
			read.cancel();
		}
		reads.clear();
		readIndexes.clear();
	}

	void resetChildren(Database cx, Version position, Version acknowledged) {
		cancelReads();
		std::vector<UID> identities;
		identities.reserve(children.size());
		for (const auto& child : children) {
			identities.push_back(child->consumerId);
		}
		children.clear();
		owners.assign(metadata.partitions().size(), Optional<UID>());
		merge.reset(position);
		for (size_t i = 0; i < metadata.partitions().size(); ++i) {
			auto child =
			    makeReference<NativeCdcConsumer>(cx, CDCCursor(metadata.partitions()[i], position), acknowledged);
			child->internalPartition = true;
			child->initialized = true;
			child->knownAvailableThrough = acknowledged;
			child->replyByteLimit = childReplyByteLimit;
			if (i < identities.size()) {
				// Replacing the local object isolates canceled coroutines; retaining the
				// RPC identity supersedes any abandoned long poll still on the proxy.
				child->consumerId = identities[i];
			}
			children.push_back(std::move(child));
		}
		resetPending = false;
	}

	bool ownerChanged(Database cx) const {
		const auto& info = cx->clientInfo->get();
		for (size_t i = 0; i < children.size(); ++i) {
			if (!owners[i].present()) {
				continue;
			}
			const auto assigned = info.streamToCDCProxyId.find(metadata.partitions()[i]);
			if (assigned == info.streamToCDCProxyId.end() || assigned->second != owners[i].get() ||
			    !containsNativeCdcProxy(info, owners[i].get()) ||
			    (children[i]->deliveryProxyId.present() && children[i]->deliveryProxyId.get() != owners[i].get())) {
				return true;
			}
		}
		return false;
	}

public:
	OrderedState(NativeCdcOrderedMetadata metadata, Version position, int64_t bufferBytes, int64_t replyBytes)
	  : metadata(std::move(metadata)), merge(this->metadata.partitions().size(), position, bufferBytes),
	    childReplyByteLimit(bufferBytes / (2 * this->metadata.partitions().size())),
	    aggregateReplyByteLimit(replyBytes) {
		if (childReplyByteLimit <= 0 || aggregateReplyByteLimit <= 0) {
			throw invalid_option_value();
		}
	}
	~OrderedState() { cancelReads(); }
};

NativeCdcConsumer::NativeCdcConsumer(Database cx, CDCCursor position)
  : cx(cx), currentPosition(position), lastAcknowledgedVersion(position.lastConsumedVersion) {}

NativeCdcConsumer::NativeCdcConsumer(Database cx, CDCCursor position, Version lastAcknowledgedVersion)
  : cx(cx), currentPosition(position), lastAcknowledgedVersion(lastAcknowledgedVersion) {}

NativeCdcConsumer::~NativeCdcConsumer() = default;

Future<Void> NativeCdcConsumer::initialize(Reference<NativeCdcConsumer> self) {
	if (self->initialized && (!self->ordered.isValid() || !self->ordered->resetPending)) {
		co_return;
	}
	if (self->currentPosition.streamId == 0 || self->currentPosition.lastConsumedVersion < invalidVersion ||
	    self->currentPosition.lastConsumedVersion == std::numeric_limits<Version>::max()) {
		throw client_invalid_operation();
	}
	Transaction tr(self->cx);
	Optional<NativeCdcOrderedSnapshot> snapshot;
	while (true) {
		Error error;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			// Retention pressure must not throttle the metadata reads needed to resume draining CDC.
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			if (!self->internalPartition &&
			    (co_await tr.get(cdcOrderedParentKeyFor(self->currentPosition.streamId))).present()) {
				throw client_invalid_operation();
			}
			snapshot = co_await readNativeCdcOrderedSnapshot(&tr, self->currentPosition.streamId);
			break;
		} catch (Error& caught) {
			error = caught;
		}
		co_await tr.onError(error);
	}
	if (!snapshot.present()) {
		if (self->ordered.isValid()) {
			throw client_invalid_operation();
		}
		self->initialized = true;
		co_return;
	}
	const Version acknowledged = snapshot.get().minVersion - 1;
	const bool resetting = self->ordered.isValid();
	Version position = self->currentPosition.lastConsumedVersion;
	if (resetting || position == invalidVersion) {
		position = acknowledged;
	} else if (position < acknowledged) {
		// A stale caller checkpoint is permanent; retrying the metadata transaction cannot repair it.
		throw transaction_too_old();
	}
	if (resetting) {
		if (self->ordered->metadata != snapshot.get().metadata) {
			throw client_invalid_operation();
		}
	} else {
		self->ordered = makeReference<OrderedState>(snapshot.get().metadata,
		                                            position,
		                                            CLIENT_KNOBS->NATIVE_CDC_ORDERED_BUFFER_BYTES,
		                                            CLIENT_KNOBS->NATIVE_CDC_ORDERED_REPLY_BYTES);
	}
	self->ordered->resetChildren(self->cx, position, acknowledged);
	self->currentPosition.lastConsumedVersion = position;
	self->lastAcknowledgedVersion = acknowledged;
	// A supplied checkpoint is not proof of delivery. Each physical owner
	// validates it on consume; acknowledgement retains its read-version guard.
	self->knownAvailableThrough = acknowledged;
	self->initialized = true;
	co_return;
}

Future<CDCConsumeReply> NativeCdcConsumer::consumeOrdered(Reference<NativeCdcConsumer> self) {
	Reference<OrderedState> ordered = self->ordered;
	// A bounded suffix may already be ready locally. Validate the logical stream
	// before returning it so a consume started after removal cannot bypass the
	// metadata check that every physical consume performs.
	Transaction tr(self->cx);
	while (true) {
		Error error;
		try {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			const Optional<Value> value = co_await tr.get(cdcOrderedStreamKeyFor(self->currentPosition.streamId));
			if (!value.present() || ordered->metadata != decodeCDCOrderedStreamValue(value.get())) {
				throw client_invalid_operation();
			}
			break;
		} catch (Error& caught) {
			error = caught;
		}
		co_await tr.onError(error);
	}
	while (true) {
		if (ordered->resetPending) {
			co_await initialize(self);
		}
		if (ordered->ownerChanged(self->cx)) {
			ordered->resetPending = true;
			continue;
		}
		CDCConsumeReply ready = ordered->merge.next(ordered->aggregateReplyByteLimit);
		if (ready.lastConsumedVersion > self->currentPosition.lastConsumedVersion) {
			self->currentPosition.lastConsumedVersion = ready.lastConsumedVersion;
			self->knownAvailableThrough = ready.lastConsumedVersion;
			co_return ready;
		}
		ASSERT(ordered->reads.empty());
		for (size_t i = 0; i < ordered->children.size(); ++i) {
			if (!ordered->merge.needsRead(i)) {
				continue;
			}
			const CDCProxyInterface proxy =
			    co_await getNativeCdcStreamProxy(self->cx, ordered->metadata.partitions()[i]);
			if (ordered->owners[i].present() && ordered->owners[i].get() != proxy.id()) {
				ordered->resetPending = true;
				break;
			}
			ordered->owners[i] = proxy.id();
			ordered->readIndexes.push_back(i);
			ordered->reads.push_back(ordered->children[i]->consume());
		}
		if (ordered->resetPending || ordered->ownerChanged(self->cx)) {
			ordered->cancelReads();
			ordered->resetPending = true;
			continue;
		}
		ASSERT(!ordered->reads.empty());
		Future<std::vector<CDCConsumeReply>> all = getAll(ordered->reads);
		while (true) {
			Future<Void> changed = self->cx->clientInfo->onChange();
			if (ordered->ownerChanged(self->cx)) {
				ordered->resetPending = true;
				break;
			}
			auto result = co_await race(all, changed);
			if (result.index() == 0) {
				break;
			}
		}
		if (ordered->resetPending || ordered->ownerChanged(self->cx)) {
			all.cancel();
			ordered->cancelReads();
			ordered->resetPending = true;
			continue;
		}
		std::vector<CDCConsumeReply> replies = co_await all;
		for (size_t i = 0; i < replies.size(); ++i) {
			const size_t partition = ordered->readIndexes[i];
			ASSERT(ordered->children[partition]->deliveryProxyId.present());
			ordered->merge.accept(partition, std::move(replies[i]));
		}
		ordered->reads.clear();
		ordered->readIndexes.clear();
	}
}

namespace {

Future<Void> notifyNativeCdcAcknowledgement(Database cx, CDCStreamId streamId, Version acknowledged) {
	while (true) {
		CDCProxyInterface proxy = co_await getNativeCdcStreamProxy(cx, streamId);
		try {
			Future<Void> changed = cx->clientInfo->onChange();
			auto result =
			    co_await race(throwErrorOr(proxy.ack.tryGetReply(CDCAckRequest(streamId, acknowledged))), changed);
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

} // namespace

Future<Void> NativeCdcConsumer::acknowledgeOrdered(Reference<NativeCdcConsumer> self) {
	Reference<OrderedState> ordered = self->ordered;
	const Version acknowledged = self->currentPosition.lastConsumedVersion;
	const Version minimum = co_await acknowledgeNativeCdcOrderedStream(
	    self->cx, self->currentPosition.streamId, ordered->metadata, acknowledged, self->knownAvailableThrough);
	self->lastAcknowledgedVersion = minimum - 1;
	for (const auto& child : ordered->children) {
		child->lastAcknowledgedVersion = self->lastAcknowledgedVersion;
	}
	if (self->lastAcknowledgedVersion > self->currentPosition.lastConsumedVersion) {
		ordered->resetPending = true;
	}
	std::vector<Future<Void>> notifications;
	ScopeExit cancelNotifications([&notifications]() {
		for (auto& notification : notifications) {
			notification.cancel();
		}
	});
	for (const CDCStreamId child : ordered->metadata.partitions()) {
		notifications.push_back(notifyNativeCdcAcknowledgement(self->cx, child, acknowledged));
	}
	co_await waitForAll(notifications);
}

Future<Reference<NativeCdcConsumer>> createNativeCdcConsumer(Database cx, Key name) {
	const CDCStreamId streamId = co_await getNativeCdcStreamId(cx, name);
	co_return makeReference<NativeCdcConsumer>(cx, CDCCursor(streamId, invalidVersion), invalidVersion);
}

Reference<NativeCdcConsumer> resumeNativeCdcConsumer(Database cx, CDCCursor position) {
	return makeReference<NativeCdcConsumer>(cx, position);
}

Future<CDCConsumeReply> NativeCdcConsumer::consumeImpl(Reference<NativeCdcConsumer> self) {
	bool completed = false;
	ScopeExit finishOperation([&self, &completed]() {
		self->operationOutstanding = false;
		if (!completed && self->ordered.isValid()) {
			self->ordered->cancelReads();
			self->ordered->resetPending = true;
		}
	});
	co_await initialize(self);
	if (self->ordered.isValid()) {
		CDCConsumeReply reply = co_await consumeOrdered(self);
		completed = true;
		co_return reply;
	}
	while (true) {
		CDCProxyInterface proxy = co_await getNativeCdcStreamProxy(self->cx, self->currentPosition.streamId);
		if (rewindUnacknowledgedCursorAfterProxyReplacement(
		        &self->currentPosition, self->lastAcknowledgedVersion, &self->deliveryProxyId, proxy.id())) {
			self->knownAvailableThrough = self->lastAcknowledgedVersion;
			CODE_PROBE(true, "Native CDC consumer rewinds unacknowledged cursor after proxy replacement");
		}
		try {
			CDCConsumeReply reply = co_await throwErrorOr(proxy.consume.tryGetReply(
			    CDCConsumeRequest(self->currentPosition, self->consumerId, self->replyByteLimit)));
			if (reply.lastConsumedVersion == self->currentPosition.lastConsumedVersion && reply.mutations.empty()) {
				// The server lease bounds abandoned long polls. Renew it transparently so the public consume
				// operation remains a long poll without accumulating server actors after client cancellation.
				CODE_PROBE(true, "Native CDC consume renews an idle server lease");
				continue;
			}
			self->knownAvailableThrough = reply.lastConsumedVersion;
			self->currentPosition.lastConsumedVersion = reply.lastConsumedVersion;
			completed = true;
			co_return reply;
		} catch (Error& error) {
			if (!retryNativeCdcProxyRequest(error)) {
				throw;
			}
			CODE_PROBE(true, "Native CDC consume retries after proxy request failure");
		}
		co_await delay(CLIENT_KNOBS->WRONG_SHARD_SERVER_DELAY, self->cx->taskID);
	}
}

Future<CDCConsumeReply> NativeCdcConsumer::consume() {
	if (operationOutstanding) {
		CODE_PROBE(true, "Native CDC consumer rejects overlapping operations");
		return Future<CDCConsumeReply>(client_invalid_operation());
	}
	operationOutstanding = true;
	return consumeImpl(Reference<NativeCdcConsumer>::addRef(this));
}

Future<Void> NativeCdcConsumer::acknowledgeImpl(Reference<NativeCdcConsumer> self) {
	bool completed = false;
	ScopeExit finishOperation([&self, &completed]() {
		self->operationOutstanding = false;
		if (!completed && self->ordered.isValid()) {
			self->ordered->cancelReads();
			self->ordered->resetPending = true;
		}
	});
	co_await initialize(self);
	if (self->currentPosition.streamId == 0 || self->currentPosition.lastConsumedVersion < 0 ||
	    self->currentPosition.lastConsumedVersion == std::numeric_limits<Version>::max()) {
		throw client_invalid_operation();
	}
	if (self->ordered.isValid()) {
		co_await acknowledgeOrdered(self);
		completed = true;
		co_return;
	}
	const Version acknowledgedVersion = self->currentPosition.lastConsumedVersion;
	const Version durableMinVersion = co_await acknowledgeNativeCdcStream(
	    self->cx, self->currentPosition.streamId, acknowledgedVersion, self->knownAvailableThrough);
	self->lastAcknowledgedVersion = std::max(self->lastAcknowledgedVersion, durableMinVersion - 1);
	// The durable transaction completes before the proxy RPC. FoundationDB's
	// transaction ordering guarantees the proxy's subsequent metadata read
	// observes this acknowledgement.

	co_await notifyNativeCdcAcknowledgement(self->cx, self->currentPosition.streamId, acknowledgedVersion);
	completed = true;
}

Future<Void> NativeCdcConsumer::acknowledge() {
	if (operationOutstanding) {
		CODE_PROBE(true, "Native CDC consumer rejects overlapping operations");
		return Future<Void>(client_invalid_operation());
	}
	operationOutstanding = true;
	return acknowledgeImpl(Reference<NativeCdcConsumer>::addRef(this));
}

TEST_CASE("/NativeCDC/RangeNormalization") {
	std::vector<KeyRange> ranges{
		KeyRangeRef("x"_sr, "z"_sr), KeyRangeRef("b"_sr, "d"_sr), KeyRangeRef("a"_sr, "b"_sr),
		KeyRangeRef("a"_sr, "c"_sr), KeyRangeRef("b"_sr, "c"_sr), KeyRangeRef("x"_sr, "z"_sr)
	};
	const std::vector<KeyRange> expected{ KeyRangeRef("a"_sr, "d"_sr), KeyRangeRef("x"_sr, "z"_sr) };
	normalizeNativeCdcStreamRanges("orders"_sr, ranges);
	ASSERT(ranges == expected);
	normalizeNativeCdcStreamRanges("orders"_sr, ranges);
	ASSERT(ranges == expected);

	std::vector<KeyRange> entireKeyspace{ normalKeys };
	normalizeNativeCdcStreamRanges("all"_sr, entireKeyspace);
	ASSERT(entireKeyspace == std::vector<KeyRange>{ normalKeys });

	constexpr int singletonCount = 10;
	const int keyLength = CLIENT_KNOBS->VALUE_SIZE_LIMIT / (2LL * singletonCount) + 1;
	ASSERT_LT(keyLength, CLIENT_KNOBS->KEY_SIZE_LIMIT);
	std::vector<KeyRange> singletonRanges;
	int64_t endpointBytes = 0;
	for (int i = 0; i < singletonCount; ++i) {
		const std::string prefix = format("%04d/", i);
		ASSERT_GT(keyLength, prefix.size());
		const Key key(StringRef(prefix + std::string(keyLength - prefix.size(), 'x')));
		singletonRanges.push_back(singleKeyRange(key));
		endpointBytes += static_cast<int64_t>(key.size()) + key.size() + 1;
	}
	ASSERT_GT(endpointBytes, CLIENT_KNOBS->VALUE_SIZE_LIMIT);
	ASSERT_LE(cdcStreamKeysValue(singletonRanges).size(), CLIENT_KNOBS->VALUE_SIZE_LIMIT);
	const auto expectedSingletons = singletonRanges;
	normalizeNativeCdcStreamRanges("singletons"_sr, singletonRanges);
	ASSERT(singletonRanges == expectedSingletons);
	return Void();
}

TEST_CASE("/NativeCDC/InvalidRanges") {
	auto expectInvalid = [](KeyRef name, std::vector<KeyRange> ranges) {
		try {
			normalizeNativeCdcStreamRanges(name, ranges);
		} catch (Error& error) {
			ASSERT_EQ(error.code(), error_code_client_invalid_operation);
			return;
		}
		ASSERT(false);
	};
	expectInvalid(KeyRef(), { normalKeys });
	expectInvalid("orders"_sr, {});
	expectInvalid("orders"_sr, { KeyRangeRef("a"_sr, "a"_sr) });
	expectInvalid("orders"_sr, { normalKeys, systemKeys });
	expectInvalid("orders"_sr, std::vector<KeyRange>(NATIVE_CDC_MAX_RANGES + 1, normalKeys));

	std::vector<KeyRange> maximumCount;
	std::vector<KeyRange> oversizedMetadata;
	const int endpointLength = CLIENT_KNOBS->VALUE_SIZE_LIMIT / (int64_t{ 2 } * NATIVE_CDC_MAX_RANGES);
	for (int i = 0; i < NATIVE_CDC_MAX_RANGES; ++i) {
		const std::string prefix = format("%04d/", i);
		maximumCount.emplace_back(KeyRangeRef(prefix + "a", prefix + "z"));
		ASSERT_GT(endpointLength, prefix.size());
		oversizedMetadata.emplace_back(KeyRangeRef(prefix + std::string(endpointLength - prefix.size(), 'a'),
		                                           prefix + std::string(endpointLength - prefix.size(), 'z')));
	}
	normalizeNativeCdcStreamRanges("orders"_sr, maximumCount);
	ASSERT_EQ(maximumCount.size(), NATIVE_CDC_MAX_RANGES);
	ASSERT_LE(2 * NATIVE_CDC_MAX_RANGES * endpointLength, CLIENT_KNOBS->VALUE_SIZE_LIMIT);
	ASSERT_GT(cdcStreamKeysValue(oversizedMetadata).size(), CLIENT_KNOBS->VALUE_SIZE_LIMIT);
	expectInvalid("orders"_sr, oversizedMetadata);
	const std::string oversizedBegin(CLIENT_KNOBS->VALUE_SIZE_LIMIT, 'a');
	const std::string oversizedEnd(CLIENT_KNOBS->VALUE_SIZE_LIMIT, 'b');
	expectInvalid("orders"_sr, { KeyRangeRef(oversizedBegin, oversizedEnd) });
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

TEST_CASE("/NativeCDC/OrderedProxySelectionRequiresCapability") {
	const NetworkAddress address(IPAddress(0x01020304), 4500);
	CDCProxyInterface legacy;
	legacy.consume = PublicRequestStream<CDCConsumeRequest>(Endpoint({ address }, UID(1, 2)));
	CDCProxyInterface ordered;
	ordered.consume = PublicRequestStream<CDCConsumeRequest>(Endpoint({ address }, UID(3, 4)));
	ordered.supportsOrderedStreams = true;
	ClientDBInfo clientInfo;
	clientInfo.cdcProxies = { legacy, ordered };

	ASSERT_EQ(selectAvailableNativeCdcProxy(clientInfo, {}).get().id(), legacy.id());
	ASSERT_EQ(selectAvailableNativeCdcProxy(clientInfo, {}, true).get().id(), ordered.id());
	ASSERT_EQ(selectAvailableNativeCdcProxy(clientInfo, ordered.id(), true).get().id(), ordered.id());
	clientInfo.cdcProxies = { legacy };
	ASSERT(!selectAvailableNativeCdcProxy(clientInfo, {}, true).present());
	return Void();
}
