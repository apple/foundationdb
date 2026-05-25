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

#include <utility>
#include <vector>

#include "fdbclient/CDCProxyInterface.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"

struct NativeCdcWorkload : TestWorkload {
	static constexpr auto NAME = "NativeCdc";

	explicit NativeCdcWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return run(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<std::pair<Tag, Version>> getPersistedRoute(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> keys = co_await tr.get(cdcStreamKeyFor(streamId));
				Optional<Value> minVersion = co_await tr.get(cdcMinVersionKeyFor(streamId));
				RangeResult history = co_await tr.getRange(cdcTagHistoryRangeFor(streamId), 2);
				ASSERT(keys.present());
				ASSERT(minVersion.present());
				ASSERT(history.size() == 1);
				const auto [historyStreamId, historyVersion, tag] = decodeCDCTagHistoryKey(history[0].key);
				ASSERT(historyStreamId == streamId);
				ASSERT(historyVersion == decodeCDCMinVersionValue(minVersion.get()));
				co_return std::make_pair(tag, historyVersion);
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Version> getPersistedMinVersion(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> minVersion = co_await tr.get(cdcMinVersionKeyFor(streamId));
				ASSERT(minVersion.present());
				co_return decodeCDCMinVersionValue(minVersion.get());
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<CDCProxyInterface> getCDCProxy() {
		while (dbInfo->get().client.cdcProxies.empty()) {
			co_await dbInfo->onChange();
		}
		co_return dbInfo->get().client.cdcProxies.front();
	}

	Future<CDCProxyInterface> getCDCProxy(CDCStreamId streamId) {
		while (true) {
			const ClientDBInfo& client = dbInfo->get().client;
			auto assigned = client.streamToCDCProxyId.find(streamId);
			if (assigned != client.streamToCDCProxyId.end()) {
				for (const auto& proxy : client.cdcProxies) {
					if (proxy.id() == assigned->second) {
						co_return proxy;
					}
				}
			}
			co_await dbInfo->onChange();
		}
	}

	Future<CDCProxyInterface> getReplacementCDCProxy(CDCStreamId streamId, UID failedProxyId) {
		while (true) {
			const ClientDBInfo& client = dbInfo->get().client;
			auto assigned = client.streamToCDCProxyId.find(streamId);
			if (assigned != client.streamToCDCProxyId.end() && assigned->second != failedProxyId) {
				for (const auto& proxy : client.cdcProxies) {
					if (proxy.id() == assigned->second) {
						co_return proxy;
					}
				}
			}
			co_await dbInfo->onChange();
		}
	}

	Future<Void> waitForCDCProxyAssignmentRemoval(CDCStreamId streamId) {
		while (dbInfo->get().client.streamToCDCProxyId.contains(streamId)) {
			co_await dbInfo->onChange();
		}
		co_return;
	}

	Future<Void> run(Database cx) {
		const Key firstName = "native-cdc-first"_sr;
		const Key secondName = "native-cdc-second"_sr;
		const KeyRange firstRange(KeyRangeRef("a"_sr, "m"_sr));
		const KeyRange conflictingRange(KeyRangeRef("a"_sr, "z"_sr));
		const KeyRange secondRange(KeyRangeRef("g"_sr, "z"_sr));

		const CDCStreamId firstId = co_await registerNativeCdcStream(cx, firstName, firstRange);
		ASSERT(co_await registerNativeCdcStream(cx, firstName, firstRange) == firstId);

		bool conflictingDuplicateRejected = false;
		try {
			co_await registerNativeCdcStream(cx, firstName, conflictingRange);
		} catch (Error& e) {
			if (e.code() == error_code_client_invalid_operation) {
				conflictingDuplicateRejected = true;
			} else {
				throw;
			}
		}
		ASSERT(conflictingDuplicateRejected);

		const auto firstRoute = co_await getPersistedRoute(cx, firstId);
		ASSERT(firstRoute.first.locality == tagLocalityCDC);

		std::vector<NativeCdcStreamInfo> streams = co_await listNativeCdcStreams(cx);
		ASSERT(streams.size() == 1);
		ASSERT(streams[0].name == firstName);
		ASSERT(streams[0].streamId == firstId);
		ASSERT(streams[0].keys == firstRange);
		ASSERT(streams[0].minVersion == firstRoute.second);

		const Version firstConsumedThrough = firstRoute.second + 5;
		const Version firstAckMinVersion = firstConsumedThrough + 1;
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, firstConsumedThrough) == firstAckMinVersion);
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, firstRoute.second) == firstAckMinVersion);
		streams = co_await listNativeCdcStreams(cx);
		ASSERT(streams.size() == 1);
		ASSERT(streams[0].minVersion == firstAckMinVersion);

		co_await removeNativeCdcStream(cx, firstName);
		ASSERT((co_await listNativeCdcStreams(cx)).empty());
		const Version retiredConsumedThrough = firstConsumedThrough + 5;
		const Version retiredAckMinVersion = retiredConsumedThrough + 1;
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, retiredConsumedThrough) == retiredAckMinVersion);
		ASSERT(co_await getPersistedMinVersion(cx, firstId) == retiredAckMinVersion);

		const CDCStreamId secondId = co_await registerNativeCdcStream(cx, secondName, secondRange);
		const auto secondRoute = co_await getPersistedRoute(cx, secondId);
		ASSERT(secondId > firstId);
		ASSERT(secondRoute.first != firstRoute.first);

		co_await removeNativeCdcStream(cx, secondName);

		CDCProxyInterface proxy = co_await getCDCProxy();
		const Key liveName = "native-cdc-live"_sr;
		const KeyRange liveRange(KeyRangeRef("live/"_sr, "live0"_sr));
		CDCRegisterStreamReply liveRegistration =
		    co_await proxy.registerStream.getReply(CDCRegisterStreamRequest(liveName, liveRange));
		CDCProxyInterface owner = co_await getCDCProxy(liveRegistration.streamId);
		ASSERT(owner.id() == proxy.id());

		CDCListStreamsReply listed = co_await proxy.listStreams.getReply(CDCListStreamsRequest());
		ASSERT(listed.streams.size() == 1);
		ASSERT(listed.streams[0].name == liveName);
		ASSERT(listed.streams[0].streamId == liveRegistration.streamId);
		ASSERT(listed.streams[0].keys == liveRange);

		Transaction write(cx);
		write.set("live/in"_sr, "captured"_sr);
		write.set("other/out"_sr, "ignored"_sr);
		co_await write.commit();
		const Version writeVersion = write.getCommittedVersion();

		for (const auto& nonOwner : dbInfo->get().client.cdcProxies) {
			if (nonOwner.id() == owner.id()) {
				continue;
			}
			bool wrongOwnerRejected = false;
			try {
				co_await nonOwner.consume.getReply(
				    CDCConsumeRequest(CDCCursor(liveRegistration.streamId, invalidVersion)));
			} catch (Error& e) {
				wrongOwnerRejected = e.code() == error_code_wrong_shard_server;
			}
			ASSERT(wrongOwnerRejected);
			bool wrongOwnerRemoveRejected = false;
			try {
				co_await nonOwner.removeStream.getReply(CDCRemoveStreamRequest(liveName));
			} catch (Error& e) {
				wrongOwnerRemoveRejected = e.code() == error_code_wrong_shard_server;
			}
			ASSERT(wrongOwnerRemoveRejected);
			break;
		}

		CDCConsumeReply consumed = co_await timeoutError(
		    owner.consume.getReply(CDCConsumeRequest(CDCCursor(liveRegistration.streamId, invalidVersion))), 30.0);
		ASSERT(consumed.lastConsumedVersion >= writeVersion);
		bool foundInRangeWrite = false;
		bool foundOutOfRangeWrite = false;
		for (const auto& versioned : consumed.mutations) {
			for (const auto& mutation : versioned.mutations) {
				if (mutation.param1 == "live/in"_sr) {
					foundInRangeWrite = true;
				}
				if (mutation.param1 == "other/out"_sr) {
					foundOutOfRangeWrite = true;
				}
			}
		}
		ASSERT(foundInRangeWrite);
		ASSERT(!foundOutOfRangeWrite);

		const uint64_t recoveryCount = dbInfo->get().recoveryCount;
		co_await owner.haltForTesting.getReply(HaltCDCProxyRequest());
		CDCProxyInterface replacement =
		    co_await timeoutError(getReplacementCDCProxy(liveRegistration.streamId, owner.id()), 30.0);
		ASSERT(replacement.id() != owner.id());
		ASSERT(dbInfo->get().recoveryCount == recoveryCount);

		Transaction afterFailureWrite(cx);
		afterFailureWrite.set("live/after-failure"_sr, "captured-after-failure"_sr);
		co_await afterFailureWrite.commit();
		const Version afterFailureVersion = afterFailureWrite.getCommittedVersion();
		CDCConsumeReply afterFailure =
		    co_await timeoutError(replacement.consume.getReply(CDCConsumeRequest(
		                              CDCCursor(liveRegistration.streamId, consumed.lastConsumedVersion))),
		                          30.0);
		ASSERT(afterFailure.lastConsumedVersion >= afterFailureVersion);
		bool foundAfterFailureWrite = false;
		for (const auto& versioned : afterFailure.mutations) {
			for (const auto& mutation : versioned.mutations) {
				if (mutation.param1 == "live/after-failure"_sr) {
					foundAfterFailureWrite = true;
				}
			}
		}
		ASSERT(foundAfterFailureWrite);

		co_await replacement.ack.getReply(CDCAckRequest(liveRegistration.streamId, afterFailureVersion));
		ASSERT(co_await getPersistedMinVersion(cx, liveRegistration.streamId) == afterFailureVersion + 1);
		co_await replacement.removeStream.getReply(CDCRemoveStreamRequest(liveName));
		co_await waitForCDCProxyAssignmentRemoval(liveRegistration.streamId);
	}
};

WorkloadFactory<NativeCdcWorkload> NativeCdcWorkloadFactory;
