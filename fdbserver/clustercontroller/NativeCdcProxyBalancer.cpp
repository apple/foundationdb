/*
 * NativeCdcProxyBalancer.cpp
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
#include <map>
#include <set>
#include <vector>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/clustercontroller/NativeCdcProxyBalancer.h"
#include "flow/CodeProbe.h"
#include "flow/DeterministicRandom.h"
#include "flow/Trace.h"

namespace {

bool containsNativeCdcProxy(ClientDBInfo const& clientInfo, UID proxyId) {
	return std::any_of(clientInfo.cdcProxies.begin(),
	                   clientInfo.cdcProxies.end(),
	                   [proxyId](CDCProxyInterface const& proxy) { return proxy.id() == proxyId; });
}

void signalNativeCdcProxyAssignmentChange(Transaction* tr) {
	tr->set(cdcProxyAssignmentChangeKey,
	        BinaryWriter::toValue(deterministicRandom()->randomUniqueID(),
	                              IncludeVersion(ProtocolVersion::withNativeCdc())));
}

} // namespace

Future<bool> rebalanceNativeCdcProxyAssignments(Database cx,
                                                std::vector<UID> availableProxies,
                                                std::function<bool()> stillEligible) {
	// The metadata is read and the whole tag group is moved in one transaction. Do not split a shared tag
	// across owners or make a partial move when the scan approaches the transaction size/lifetime limits.
	constexpr int maxStreams = 512;
	constexpr int maxHistoryRows = 2048;
	constexpr int maxStreamsPerMove = 64;
	constexpr int maxRangeBytes = 1 << 20;
	const int64_t timeoutMs = 5000;
	const std::set<UID> available(availableProxies.begin(), availableProxies.end());
	if (available.size() < 2) {
		co_return false;
	}

	Transaction tr(cx);
	int attempts = 0;
	while (true) {
		if (++attempts > 3) {
			co_return false;
		}
		Error err;
		try {
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::TIMEOUT,
			             StringRef(reinterpret_cast<const uint8_t*>(&timeoutMs), sizeof(timeoutMs)));
			const UID publishedInfoId = cx->clientInfo->get().id;
			if (!cx->clientInfo->get().nativeCdcEnabled || !stillEligible()) {
				co_return false;
			}

			RangeResult active = co_await tr.getRange(cdcStreamKeys, GetRangeLimits(maxStreams + 1, maxRangeBytes));
			RangeResult assignments = co_await tr.getRange(cdcProxyKeys, GetRangeLimits(maxStreams + 1, maxRangeBytes));
			RangeResult histories =
			    co_await tr.getRange(cdcTagHistoryKeys, GetRangeLimits(maxHistoryRows + 1, maxRangeBytes));
			if (active.more || assignments.more || histories.more || active.size() > maxStreams ||
			    assignments.size() > maxStreams || histories.size() > maxHistoryRows) {
				CODE_PROBE(true, "Native CDC proxy rebalancing skips oversized metadata");
				co_return false;
			}

			std::set<CDCStreamId> activeIds;
			for (const auto& stream : active) {
				activeIds.insert(decodeCDCStreamKey(stream.key));
			}
			std::map<CDCStreamId, UID> ownerByStream;
			for (const auto& assignment : assignments) {
				const auto [streamId, owner] = decodeCDCProxyKey(assignment.key);
				if (activeIds.contains(streamId) && !ownerByStream.emplace(streamId, owner).second) {
					co_return false;
				}
			}
			std::map<CDCStreamId, Tag> currentTagByStream;
			std::set<CDCStreamId> pendingHistories;
			for (const auto& history : histories) {
				const CDCTagHistoryEntry entry = decodeCDCTagHistoryKey(history.key);
				if (activeIds.contains(entry.streamId)) {
					currentTagByStream[entry.streamId] = entry.tag;
					if (!history.value.empty()) {
						pendingHistories.insert(entry.streamId);
					}
				}
			}

			std::map<UID, int> ownerLoads;
			for (const UID& proxyId : available) {
				ownerLoads.emplace(proxyId, 0);
			}
			std::map<Tag, std::vector<CDCStreamId>> membersByTag;
			for (const CDCStreamId streamId : activeIds) {
				auto owner = ownerByStream.find(streamId);
				auto tag = currentTagByStream.find(streamId);
				if (owner == ownerByStream.end() || tag == currentTagByStream.end() ||
				    !ownerLoads.contains(owner->second)) {
					// Let the cluster controller repair missing or stale ownership before balancing.
					co_return false;
				}
				const auto published = cx->clientInfo->get().streamToCDCProxyId.find(streamId);
				if (published == cx->clientInfo->get().streamToCDCProxyId.end() || published->second != owner->second) {
					co_return false;
				}
				++ownerLoads[owner->second];
				membersByTag[tag->second].push_back(streamId);
			}

			Optional<Tag> selectedTag;
			Optional<UID> selectedSource;
			Optional<UID> selectedTarget;
			int bestImprovement = 0;
			size_t bestGroupSize = 0;
			for (const auto& [tag, members] : membersByTag) {
				const UID source = ownerByStream.at(members.front());
				for (const CDCStreamId streamId : members) {
					if (ownerByStream.at(streamId) != source) {
						// Registration relies on every stream sharing a current tag having one owner.
						co_return false;
					}
				}
				if (members.size() > maxStreamsPerMove ||
				    std::any_of(members.begin(), members.end(), [&](CDCStreamId id) {
					    return pendingHistories.contains(id);
				    })) {
					continue;
				}
				for (const UID& target : available) {
					if (source == target) {
						continue;
					}
					const int difference = ownerLoads.at(source) - ownerLoads.at(target);
					const int moved = 2 * static_cast<int>(members.size());
					const int after = difference >= moved ? difference - moved : moved - difference;
					const int improvement = difference - after;
					if (improvement > bestImprovement ||
					    (improvement == bestImprovement && improvement > 0 && members.size() > bestGroupSize)) {
						bestImprovement = improvement;
						bestGroupSize = members.size();
						selectedTag = tag;
						selectedSource = source;
						selectedTarget = target;
					}
				}
			}
			if (!selectedTag.present()) {
				co_return false;
			}
			if (!stillEligible() || !cx->clientInfo->get().nativeCdcEnabled ||
			    cx->clientInfo->get().id != publishedInfoId ||
			    !containsNativeCdcProxy(cx->clientInfo->get(), selectedSource.get()) ||
			    !containsNativeCdcProxy(cx->clientInfo->get(), selectedTarget.get())) {
				co_return false;
			}

			for (const CDCStreamId streamId : membersByTag.at(selectedTag.get())) {
				tr.clear(cdcProxyKeyFor(streamId, selectedSource.get()));
				tr.set(cdcProxyKeyFor(streamId, selectedTarget.get()), Value());
			}
			signalNativeCdcProxyAssignmentChange(&tr);
			co_await tr.commit();
			CODE_PROBE(true, "Native CDC rebalances an entire shared tag across live proxies");
			TraceEvent("CDCProxyTagRebalanced")
			    .detail("Tag", selectedTag.get().toString())
			    .detail("OldCDCProxyID", selectedSource.get())
			    .detail("NewCDCProxyID", selectedTarget.get())
			    .detail("StreamCount", bestGroupSize);
			co_return true;
		} catch (Error& e) {
			// An ambiguous commit may already have moved a group. Reconcile on the next controller pass.
			if (e.code() == error_code_commit_unknown_result) {
				throw;
			}
			err = e;
		}
		co_await tr.onError(err);
	}
}
