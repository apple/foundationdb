/*
 * DDTxnProcessor.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/DDTxnProcessor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class DDTxnProcessorImpl {
	friend class DDTxnProcessor;

	// return {sourceServers, completeSources}
	ACTOR static Future<IDDTxnProcessor::SourceServers> getSourceServersForRange(Database cx, KeyRangeRef keys) {
		state std::set<UID> servers;
		state std::vector<UID> completeSources;
		state Transaction tr(cx);

		loop {
			servers.clear();
			completeSources.clear();

			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			try {
				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				RangeResult keyServersEntries = wait(tr.getRange(lastLessOrEqual(keyServersKey(keys.begin)),
				                                                 firstGreaterOrEqual(keyServersKey(keys.end)),
				                                                 SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS));

				if (keyServersEntries.size() < SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS) {
					for (int shard = 0; shard < keyServersEntries.size(); shard++) {
						std::vector<UID> src, dest;
						decodeKeyServersValue(UIDtoTagMap, keyServersEntries[shard].value, src, dest);
						ASSERT(src.size());
						for (int i = 0; i < src.size(); i++) {
							servers.insert(src[i]);
						}
						if (shard == 0) {
							completeSources = src;
						} else {
							for (int i = 0; i < completeSources.size(); i++) {
								if (std::find(src.begin(), src.end(), completeSources[i]) == src.end()) {
									swapAndPop(&completeSources, i--);
								}
							}
						}
					}

					ASSERT(servers.size() > 0);
				}

				// If the size of keyServerEntries is large, then just assume we are using all storage servers
				// Why the size can be large?
				// When a shard is inflight and DD crashes, some destination servers may have already got the data.
				// The new DD will treat the destination servers as source servers. So the size can be large.
				else {
					RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

					for (auto s = serverList.begin(); s != serverList.end(); ++s)
						servers.insert(decodeServerListValue(s->value).id());

					ASSERT(servers.size() > 0);
				}

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		return IDDTxnProcessor::SourceServers{ std::vector<UID>(servers.begin(), servers.end()), completeSources };
	}

	// set the system key space
	ACTOR static Future<Void> updateReplicaKeys(Database cx,
	                                            std::vector<Optional<Key>> primaryDcId,
	                                            std::vector<Optional<Key>> remoteDcIds,
	                                            DatabaseConfiguration configuration) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				RangeResult replicaKeys = wait(tr.getRange(datacenterReplicasKeys, CLIENT_KNOBS->TOO_MANY));

				for (auto& kv : replicaKeys) {
					auto dcId = decodeDatacenterReplicasKey(kv.key);
					auto replicas = decodeDatacenterReplicasValue(kv.value);
					if ((primaryDcId.size() && primaryDcId.at(0) == dcId) ||
					    (remoteDcIds.size() && remoteDcIds.at(0) == dcId && configuration.usableRegions > 1)) {
						if (replicas > configuration.storageTeamSize) {
							tr.set(kv.key, datacenterReplicasValue(configuration.storageTeamSize));
						}
					} else {
						tr.clear(kv.key);
					}
				}

				wait(tr.commit());
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> waitForDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
		state Transaction tr(cx);
		loop {
			wait(delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskPriority::DataDistribution));

			try {
				Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
				if (!mode.present() && ddEnabledState->isDDEnabled()) {
					TraceEvent("WaitForDDEnabledSucceeded").log();
					return Void();
				}
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					int m;
					rd >> m;
					TraceEvent(SevDebug, "WaitForDDEnabled")
					    .detail("Mode", m)
					    .detail("IsDDEnabled", ddEnabledState->isDDEnabled());
					if (m && ddEnabledState->isDDEnabled()) {
						TraceEvent("WaitForDDEnabledSucceeded").log();
						return Void();
					}
				}

				tr.reset();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}
};

Future<IDDTxnProcessor::SourceServers> DDTxnProcessor::getSourceServersForRange(const KeyRangeRef range) {
	return DDTxnProcessorImpl::getSourceServersForRange(cx, range);
}

Future<std::vector<std::pair<StorageServerInterface, ProcessClass>>> DDTxnProcessor::getServerListAndProcessClasses() {
	Transaction tr(cx);
	return NativeAPI::getServerListAndProcessClasses(&tr);
}

Future<MoveKeysLock> DDTxnProcessor::takeMoveKeysLock(UID ddId) const {
	return ::takeMoveKeysLock(cx, ddId);
}

Future<DatabaseConfiguration> DDTxnProcessor::getDatabaseConfiguration() const {
	return ::getDatabaseConfiguration(cx);
}

Future<Void> DDTxnProcessor::updateReplicaKeys(const std::vector<Optional<Key>>& primaryIds,
                                               const std::vector<Optional<Key>>& remoteIds,
                                               const DatabaseConfiguration& configuration) const {
	return DDTxnProcessorImpl::updateReplicaKeys(cx, primaryIds, remoteIds, configuration);
}

Future<Void> DDTxnProcessor::waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const {
	return DDTxnProcessorImpl::waitForDataDistributionEnabled(cx, ddEnabledState);
}