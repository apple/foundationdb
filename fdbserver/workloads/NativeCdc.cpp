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

#include <set>
#include <utility>
#include <vector>

#include "fdbclient/CDCProxyInterface.h"
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
		co_return;
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
		co_return;
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
		const Version writeVersion = co_await writeValues(cx, { { "shared/unread"_sr, "protected-by-minimum"_sr } });
		CDCCursor firstCursor(firstId, invalidVersion);
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
		co_await acknowledgeNativeCdcStreamClient(cx, firstId, firstCursor.lastConsumedVersion);

		ASSERT(co_await registerNativeCdcStreamClient(cx, secondName, keys) == secondId);
		CDCCursor unreadCursor(secondId, invalidVersion);
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
		co_await acknowledgeNativeCdcStreamClient(cx, secondId, unreadCursor.lastConsumedVersion);

		co_await removeNativeCdcStreamClient(cx, firstName);
		co_await removeNativeCdcStreamClient(cx, secondName);
		co_await waitForCDCProxyAssignmentRemoval(firstId);
		co_await waitForCDCProxyAssignmentRemoval(secondId);
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

		co_await removeNativeCdcStream(cx, secondName);

		const Key liveName = "native-cdc-live"_sr;
		const KeyRange liveRange(KeyRangeRef("live/"_sr, "live0"_sr));
		const CDCStreamId liveStreamId = co_await registerNativeCdcStreamClient(cx, liveName, liveRange);
		CDCProxyInterface owner = co_await getCDCProxy(liveStreamId);

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

		Version consumedThrough = invalidVersion;
		bool foundInRangeWrite = false;
		bool foundOutOfRangeWrite = false;
		const double initialConsumeDeadline = now() + 30.0;
		while (consumedThrough < writeVersion) {
			CDCConsumeReply consumed =
			    co_await timeoutError(consumeNativeCdcStream(cx, CDCCursor(liveStreamId, consumedThrough)), 30.0);
			if (consumed.lastConsumedVersion == consumedThrough) {
				ASSERT(now() < initialConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(consumed.lastConsumedVersion > consumedThrough);
			consumedThrough = consumed.lastConsumedVersion;
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
		Version afterFailureCursor = consumedThrough;
		bool foundAfterFailureWrite = false;
		const double afterFailureConsumeDeadline = now() + 30.0;
		while (afterFailureCursor < afterFailureVersion) {
			CDCConsumeReply afterFailure =
			    co_await timeoutError(consumeNativeCdcStream(cx, CDCCursor(liveStreamId, afterFailureCursor)), 30.0);
			if (afterFailure.lastConsumedVersion == afterFailureCursor) {
				ASSERT(now() < afterFailureConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(afterFailure.lastConsumedVersion > afterFailureCursor);
			afterFailureCursor = afterFailure.lastConsumedVersion;
			for (const auto& versioned : afterFailure.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (mutation.param1 == "live/after-failure"_sr) {
						foundAfterFailureWrite = true;
					}
				}
			}
		}
		ASSERT(foundAfterFailureWrite);

		const Version cursorBeforeRecovery = afterFailureCursor;
		co_await acknowledgeNativeCdcStreamClient(cx, liveStreamId, cursorBeforeRecovery);
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
		Version afterRecoveryCursor = cursorBeforeRecovery;
		bool foundAfterRecoveryWrite = false;
		const double afterRecoveryConsumeDeadline = now() + 30.0;
		while (afterRecoveryCursor < afterRecoveryVersion) {
			CDCConsumeReply afterRecovery =
			    co_await timeoutError(consumeNativeCdcStream(cx, CDCCursor(liveStreamId, afterRecoveryCursor)), 30.0);
			if (afterRecovery.lastConsumedVersion == afterRecoveryCursor) {
				ASSERT(now() < afterRecoveryConsumeDeadline);
				co_await delay(0.1);
				continue;
			}
			ASSERT(afterRecovery.lastConsumedVersion > afterRecoveryCursor);
			afterRecoveryCursor = afterRecovery.lastConsumedVersion;
			for (const auto& versioned : afterRecovery.mutations) {
				for (const auto& mutation : versioned.mutations) {
					if (mutation.param1 == "live/after-recovery"_sr) {
						foundAfterRecoveryWrite = true;
					}
				}
			}
		}
		ASSERT(foundAfterRecoveryWrite);

		co_await acknowledgeNativeCdcStreamClient(cx, liveStreamId, afterRecoveryCursor);
		ASSERT(co_await getPersistedMinVersion(cx, liveStreamId) == afterRecoveryCursor + 1);
		co_await removeNativeCdcStreamClient(cx, liveName);
		co_await waitForCDCProxyAssignmentRemoval(liveStreamId);
	}
};

WorkloadFactory<NativeCdcWorkload> NativeCdcWorkloadFactory;
