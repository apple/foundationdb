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

#include <limits>
#include <set>
#include <utility>
#include <vector>

#include "fdbclient/CDCProxyInterface.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeCdc.h"
#include "fdbclient/SystemData.h"
#include "fdbserver/core/RecoveryState.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/tester/workloads.h"

struct NativeCdcWorkload : TestWorkload {
	static constexpr auto NAME = "NativeCdc";
	bool sharedTagSafety;

	explicit NativeCdcWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), sharedTagSafety(getOption(options, "sharedTagSafety"_sr, false)) {}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			return Void();
		}
		return sharedTagSafety ? runSharedTagSafety(cx) : run(cx);
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
				const Version initialMinVersion = decodeCDCMinVersionValue(minVersion.get());
				ASSERT(historyVersion <= initialMinVersion);
				co_return std::make_pair(tag, initialMinVersion);
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

	Future<bool> hasPersistedRetention(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> minVersion = co_await tr.get(cdcMinVersionKeyFor(streamId));
				RangeResult history = co_await tr.getRange(cdcTagHistoryRangeFor(streamId), 1);
				co_return minVersion.present() || !history.empty();
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Version> getRetiredTagPopVersion(Database cx, Tag tag) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> marker = co_await tr.get(cdcRetiredTagPopKeyFor(tag));
				Optional<Value> version = co_await tr.get(cdcRetiredTagPopVersionKeyFor(tag));
				ASSERT(marker.present());
				ASSERT(version.present());
				co_return decodeCDCMinVersionValue(version.get());
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<bool> hasRetiredTagPopState(Database cx, Tag tag) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> marker = co_await tr.get(cdcRetiredTagPopKeyFor(tag));
				Optional<Value> version = co_await tr.get(cdcRetiredTagPopVersionKeyFor(tag));
				ASSERT(marker.present() == version.present());
				co_return marker.present();
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Void> appendPersistedTag(Database cx, CDCStreamId streamId, Tag tag) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				const Version assignmentVersion = co_await tr.getReadVersion();
				tr.set(cdcTagHistoryKeyFor(streamId, assignmentVersion, tag), Value());
				co_await tr.commit();
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Version> writeValues(Database cx, std::vector<std::pair<KeyRef, ValueRef>> values) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				for (const auto& [key, value] : values) {
					tr.set(key, value);
				}
				co_await tr.commit();
				co_return tr.getCommittedVersion();
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	Future<Tag> getLatestPersistedTag(Database cx, CDCStreamId streamId) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				RangeResult history = co_await tr.getRange(cdcTagHistoryRangeFor(streamId), CLIENT_KNOBS->TOO_MANY);
				ASSERT(!history.empty());
				const auto historyEntry = decodeCDCTagHistoryKey(history.back().key);
				ASSERT(std::get<0>(historyEntry) == streamId);
				co_return std::get<2>(historyEntry);
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
	}

	Future<Void> waitForNoCDCProxies() {
		while (!dbInfo->get().client.cdcProxies.empty()) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> changeResolverCount(Database cx, int32_t count) {
		Standalone<StringRef> config(format("resolvers=%d", count));
		while (true) {
			Optional<ConfigureAutoResult> conf;
			ConfigurationResult result =
			    co_await ManagementAPI::changeConfig(cx.getReference(), { config }, conf, true);
			if (result == ConfigurationResult::SUCCESS) {
				co_return;
			}
			co_await delay(1.0);
		}
	}

	Future<Void> waitForRecoveryAfter(uint64_t previousRecoveryCount, RecoveryState requiredState) {
		while (dbInfo->get().recoveryCount <= previousRecoveryCount || dbInfo->get().recoveryState < requiredState) {
			co_await dbInfo->onChange();
		}
	}

	Future<Void> runSharedTagSafety(Database cx) {
		const Key firstName = "native-cdc-shared-first"_sr;
		const Key secondName = "native-cdc-shared-second"_sr;
		const KeyRange keys(KeyRangeRef("shared/"_sr, "shared0"_sr));
		const CDCStreamId firstId = co_await registerNativeCdcStream(cx, firstName, keys);
		const CDCStreamId secondId = co_await registerNativeCdcStream(cx, secondName, keys);
		const auto firstRoute = co_await getPersistedRoute(cx, firstId);
		co_await appendPersistedTag(cx, secondId, firstRoute.first);
		ASSERT((co_await getLatestPersistedTag(cx, secondId)) == firstRoute.first);

		ASSERT(co_await registerNativeCdcStreamClient(cx, firstName, keys) == firstId);
		ASSERT(co_await registerNativeCdcStreamClient(cx, secondName, keys) == secondId);
		ASSERT((co_await getCDCProxy(firstId)).id() == (co_await getCDCProxy(secondId)).id());
		const Version writeVersion = co_await writeValues(cx, { { "shared/unread"_sr, "protected-by-minimum"_sr } });
		CDCCursor firstCursor = co_await createNativeCdcCursor(cx, firstName);
		ASSERT(firstCursor.streamId == firstId);
		const double firstConsumeDeadline = now() + 30.0;
		while (firstCursor.lastConsumedVersion < writeVersion) {
			CDCConsumeReply consumed = co_await timeoutError(consumeNativeCdcStream(cx, firstCursor), 30.0);
			if (consumed.lastConsumedVersion == firstCursor.lastConsumedVersion) {
				ASSERT(now() < firstConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(consumed.lastConsumedVersion > firstCursor.lastConsumedVersion);
			firstCursor.lastConsumedVersion = consumed.lastConsumedVersion;
		}
		co_await acknowledgeNativeCdcStreamClient(cx, firstCursor);
		co_await removeNativeCdcStreamClient(cx, firstName);
		co_await waitForCDCProxyAssignmentRemoval(firstId);

		CDCCursor unreadCursor = co_await createNativeCdcCursor(cx, secondName);
		ASSERT(unreadCursor.streamId == secondId);
		bool foundUnread = false;
		while (unreadCursor.lastConsumedVersion < writeVersion) {
			CDCConsumeReply unread = co_await timeoutError(consumeNativeCdcStream(cx, unreadCursor), 30.0);
			ASSERT(unread.lastConsumedVersion > unreadCursor.lastConsumedVersion);
			for (const auto& versioned : unread.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (mutation.param1 == "shared/unread"_sr) {
						foundUnread = true;
					}
				}
			}
			unreadCursor.lastConsumedVersion = unread.lastConsumedVersion;
		}
		ASSERT(foundUnread);
		co_await acknowledgeNativeCdcStreamClient(cx, unreadCursor);

		co_await removeNativeCdcStreamClient(cx, secondName);
		co_await waitForCDCProxyAssignmentRemoval(secondId);
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

		const Version firstConsumedThrough =
		    co_await writeValues(cx, { { "first/acknowledged"_sr, "acknowledged"_sr } });
		const Version firstAckMinVersion = firstConsumedThrough + 1;
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, firstConsumedThrough) == firstAckMinVersion);
		ASSERT(co_await acknowledgeNativeCdcStream(cx, firstId, firstRoute.second) == firstAckMinVersion);
		streams = co_await listNativeCdcStreams(cx);
		ASSERT(streams.size() == 1);
		ASSERT(streams[0].minVersion == firstAckMinVersion);

		Optional<NativeCdcRemovedStreamInfo> removedFirst = co_await removeNativeCdcStream(cx, firstName);
		ASSERT(removedFirst.present());
		ASSERT((co_await getRetiredTagPopVersion(cx, firstRoute.first)) == removedFirst.get().removalVersion);
		ASSERT((co_await listNativeCdcStreams(cx)).empty());
		ASSERT(!(co_await hasPersistedRetention(cx, firstId)));

		bool retiredAcknowledgeRejected = false;
		try {
			co_await acknowledgeNativeCdcStream(cx, firstId, firstConsumedThrough + 5);
		} catch (Error& e) {
			retiredAcknowledgeRejected = e.code() == error_code_client_invalid_operation;
		}
		ASSERT(retiredAcknowledgeRejected);

		const CDCStreamId secondId = co_await registerNativeCdcStream(cx, secondName, secondRange);
		const auto secondRoute = co_await getPersistedRoute(cx, secondId);
		ASSERT(secondId > firstId);
		ASSERT(secondRoute.first == firstRoute.first);

		Optional<NativeCdcRemovedStreamInfo> removedSecond = co_await removeNativeCdcStream(cx, secondName);
		ASSERT(removedSecond.present());
		ASSERT((co_await getRetiredTagPopVersion(cx, secondRoute.first)) == removedSecond.get().removalVersion);

		const Key liveName = "native-cdc-live"_sr;
		const KeyRange liveRange(KeyRangeRef("live/"_sr, "live0"_sr));
		const CDCStreamId liveStreamId = co_await registerNativeCdcStreamClient(cx, liveName, liveRange);
		const Tag liveTag = co_await getLatestPersistedTag(cx, liveStreamId);
		CDCCursor liveCursor = co_await createNativeCdcCursor(cx, liveName);
		ASSERT(liveCursor.streamId == liveStreamId);
		CDCProxyInterface owner = co_await getCDCProxy(liveStreamId);

		bool futureAcknowledgeRejected = false;
		try {
			co_await acknowledgeNativeCdcStreamClient(cx,
			                                          CDCCursor(liveStreamId, std::numeric_limits<Version>::max() - 1));
		} catch (Error& e) {
			futureAcknowledgeRejected = e.code() == error_code_client_invalid_operation;
		}
		ASSERT(futureAcknowledgeRejected);

		std::vector<NativeCdcStreamInfo> listed = co_await listNativeCdcStreamsClient(cx);
		ASSERT(listed.size() == 1);
		ASSERT(listed[0].name == liveName);
		ASSERT(listed[0].streamId == liveStreamId);
		ASSERT(listed[0].keys == liveRange);

		const Version writeVersion =
		    co_await writeValues(cx, { { "live/in"_sr, "captured"_sr }, { "other/out"_sr, "ignored"_sr } });

		for (const auto& nonOwner : dbInfo->get().client.cdcProxies) {
			if (nonOwner.id() == owner.id()) {
				continue;
			}
			bool wrongOwnerRejected = false;
			try {
				co_await nonOwner.consume.getReply(CDCConsumeRequest(CDCCursor(liveStreamId, invalidVersion)));
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

		bool foundInRangeWrite = false;
		bool foundOutOfRangeWrite = false;
		const double initialConsumeDeadline = now() + 30.0;
		while (liveCursor.lastConsumedVersion < writeVersion) {
			CDCConsumeReply consumed = co_await timeoutError(consumeNativeCdcStream(cx, liveCursor), 30.0);
			if (consumed.lastConsumedVersion == liveCursor.lastConsumedVersion) {
				ASSERT(now() < initialConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(consumed.lastConsumedVersion > liveCursor.lastConsumedVersion);
			liveCursor.lastConsumedVersion = consumed.lastConsumedVersion;
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
		}
		ASSERT(foundInRangeWrite);
		ASSERT(!foundOutOfRangeWrite);

		const uint64_t recoveryCount = dbInfo->get().recoveryCount;
		co_await owner.haltForTesting.getReply(HaltCDCProxyRequest());
		CDCProxyInterface replacement = co_await timeoutError(getReplacementCDCProxy(liveStreamId, owner.id()), 30.0);
		ASSERT(replacement.id() != owner.id());
		ASSERT(dbInfo->get().recoveryCount == recoveryCount);

		const Version afterFailureVersion =
		    co_await writeValues(cx, { { "live/after-failure"_sr, "captured-after-failure"_sr } });
		bool foundAfterFailureWrite = false;
		const double afterFailureConsumeDeadline = now() + 30.0;
		while (liveCursor.lastConsumedVersion < afterFailureVersion) {
			CDCConsumeReply afterFailure = co_await timeoutError(consumeNativeCdcStream(cx, liveCursor), 30.0);
			if (afterFailure.lastConsumedVersion == liveCursor.lastConsumedVersion) {
				ASSERT(now() < afterFailureConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(afterFailure.lastConsumedVersion > liveCursor.lastConsumedVersion);
			liveCursor.lastConsumedVersion = afterFailure.lastConsumedVersion;
			for (const auto& versioned : afterFailure.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (mutation.param1 == "live/after-failure"_sr) {
						foundAfterFailureWrite = true;
					}
				}
			}
		}
		ASSERT(foundAfterFailureWrite);

		const Version cursorBeforeRecovery = liveCursor.lastConsumedVersion;
		co_await acknowledgeNativeCdcStreamClient(cx, liveCursor);
		ASSERT(co_await getPersistedMinVersion(cx, liveStreamId) == cursorBeforeRecovery + 1);

		const int32_t recoveredResolverCount = (co_await getDatabaseConfiguration(cx)).getDesiredResolvers() + 1;
		const UID ownerBeforeRecovery = replacement.id();
		const uint64_t recoveryBeforeChange = dbInfo->get().recoveryCount;
		co_await changeResolverCount(cx, recoveredResolverCount);
		co_await timeoutError(waitForRecoveryAfter(recoveryBeforeChange, RecoveryState::ACCEPTING_COMMITS), 60.0);
		CDCProxyInterface recoveredOwner = co_await getCDCProxy(liveStreamId);
		ASSERT(recoveredOwner.id() == ownerBeforeRecovery);

		const Version afterRecoveryVersion =
		    co_await writeValues(cx, { { "live/after-recovery"_sr, "captured-after-recovery"_sr } });
		bool foundAfterRecoveryWrite = false;
		const double afterRecoveryConsumeDeadline = now() + 30.0;
		while (liveCursor.lastConsumedVersion < afterRecoveryVersion) {
			CDCConsumeReply afterRecovery = co_await timeoutError(consumeNativeCdcStream(cx, liveCursor), 30.0);
			if (afterRecovery.lastConsumedVersion == liveCursor.lastConsumedVersion) {
				ASSERT(now() < afterRecoveryConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(afterRecovery.lastConsumedVersion > liveCursor.lastConsumedVersion);
			liveCursor.lastConsumedVersion = afterRecovery.lastConsumedVersion;
			for (const auto& versioned : afterRecovery.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (mutation.param1 == "live/after-recovery"_sr) {
						foundAfterRecoveryWrite = true;
					}
				}
			}
		}
		ASSERT(foundAfterRecoveryWrite);

		co_await acknowledgeNativeCdcStreamClient(cx, liveCursor);
		ASSERT(co_await getPersistedMinVersion(cx, liveStreamId) == liveCursor.lastConsumedVersion + 1);

		Future<CDCConsumeReply> pendingConsume = recoveredOwner.consume.getReply(
		    CDCConsumeRequest(CDCCursor(liveStreamId, liveCursor.lastConsumedVersion + 1000000)));
		co_await delay(0.1);
		co_await removeNativeCdcStreamClient(cx, liveName);
		co_await waitForCDCProxyAssignmentRemoval(liveStreamId);

		bool pendingConsumeRejected = false;
		try {
			co_await timeoutError(pendingConsume, 30.0);
		} catch (Error& e) {
			pendingConsumeRejected =
			    e.code() == error_code_wrong_shard_server || e.code() == error_code_client_invalid_operation;
		}
		ASSERT(pendingConsumeRejected);

		bool retiredConsumeRejected = false;
		try {
			co_await timeoutError(consumeNativeCdcStream(cx, liveCursor), 30.0);
		} catch (Error& e) {
			retiredConsumeRejected = e.code() == error_code_client_invalid_operation;
		}
		ASSERT(retiredConsumeRejected);

		bool retiredClientAcknowledgeRejected = false;
		try {
			co_await timeoutError(acknowledgeNativeCdcStreamClient(cx, liveCursor), 30.0);
		} catch (Error& e) {
			retiredClientAcknowledgeRejected = e.code() == error_code_client_invalid_operation;
		}
		ASSERT(retiredClientAcknowledgeRejected);
		ASSERT(!(co_await hasRetiredTagPopState(cx, liveTag)));

		if (g_network->isSimulated()) {
			(const_cast<ClientKnobs*>(CLIENT_KNOBS))->ENABLE_NATIVE_CDC = false;
			const int32_t disabledResolverCount = (co_await getDatabaseConfiguration(cx)).getDesiredResolvers() + 1;
			const uint64_t recoveryBeforeDisable = dbInfo->get().recoveryCount;
			co_await changeResolverCount(cx, disabledResolverCount);
			co_await timeoutError(waitForRecoveryAfter(recoveryBeforeDisable, RecoveryState::ACCEPTING_COMMITS), 60.0);
			co_await timeoutError(waitForNoCDCProxies(), 30.0);
		}
	}
};

WorkloadFactory<NativeCdcWorkload> NativeCdcWorkloadFactory;
