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
#include <utility>
#include <vector>

#include "fdbclient/Knobs.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "flow/Error.h"
#include "flow/UnitTest.h"

namespace {

struct NativeCdcIdentifierAllocator {
	bool sawStream = false;
	CDCStreamId maxStreamId = 0;
	std::set<uint16_t> usedTagIds;

	void observeStreamId(CDCStreamId streamId) {
		sawStream = true;
		maxStreamId = std::max(maxStreamId, streamId);
	}

	void observeTag(Tag tag) {
		ASSERT_WE_THINK(tag.locality == tagLocalityCDC);
		usedTagIds.insert(tag.id);
	}

	std::pair<CDCStreamId, Tag> allocate() const {
		if (sawStream && maxStreamId == std::numeric_limits<CDCStreamId>::max()) {
			throw operation_failed();
		}

		const CDCStreamId streamId = sawStream ? maxStreamId + 1 : 1;
		for (uint32_t tagId = 0; tagId <= std::numeric_limits<uint16_t>::max(); ++tagId) {
			if (!usedTagIds.contains(static_cast<uint16_t>(tagId))) {
				return { streamId, Tag(tagLocalityCDC, static_cast<uint16_t>(tagId)) };
			}
		}
		throw operation_failed();
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
	Key begin = cdcStreamKeys.begin;
	while (begin < cdcStreamKeys.end) {
		RangeResult streams = co_await tr->getRange(KeyRangeRef(begin, cdcStreamKeys.end), CLIENT_KNOBS->TOO_MANY);
		for (const auto& kv : streams) {
			allocator->observeStreamId(decodeCDCStreamKey(kv.key));
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
			const auto history = decodeCDCTagHistoryKey(kv.key);
			allocator->observeStreamId(std::get<0>(history));
			allocator->observeTag(std::get<2>(history));
		}
		if (!histories.more) {
			break;
		}
		begin = keyAfter(histories.back().key);
	}
}

} // namespace

Future<CDCStreamId> registerNativeCdcStream(Database cx, Key name, KeyRange keys, Optional<UID> proxyId) {
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
			tr.set(cdcStreamKeyFor(streamId), cdcStreamKeysValue(keys));
			tr.set(cdcTagHistoryKeyFor(streamId, registrationVersion, tag), Value());
			tr.set(cdcMinVersionKeyFor(streamId), cdcMinVersionValue(registrationVersion));
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

Future<Void> removeNativeCdcStream(Database cx, Key name, Optional<UID> proxyId) {
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
				co_return;
			}

			const CDCStreamId streamId = decodeCDCStreamNameValue(currentId.get());
			Optional<UID> assignedProxy = co_await getNativeCdcProxyAssignment(&tr, streamId);
			if (proxyId.present() && (!assignedProxy.present() || assignedProxy.get() != proxyId.get())) {
				throw wrong_shard_server();
			}
			tr.clear(nameKey);
			tr.clear(cdcStreamKeyFor(streamId));
			tr.clear(cdcProxyRangeFor(streamId));
			if (assignedProxy.present()) {
				signalNativeCdcProxyAssignmentChange(&tr);
			}
			// Retain tag history and minVersion until the pop/cleanup phase can
			// safely release all durable mutations for this retired stream.
			co_await tr.commit();
			co_return;
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

TEST_CASE("noSim/NativeCDC/LifecycleAllocation") {
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

	return Void();
}
