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
#include "fdbserver/DataDistribution.actor.h"
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

			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
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

	// Read keyservers, return unique set of teams
	ACTOR static Future<Reference<InitialDataDistribution>> getInitialDataDistribution(
	    Database cx,
	    UID distributorId,
	    MoveKeysLock moveKeysLock,
	    std::vector<Optional<Key>> remoteDcIds,
	    const DDEnabledState* ddEnabledState) {
		state Reference<InitialDataDistribution> result = makeReference<InitialDataDistribution>();
		state Key beginKey = allKeys.begin;

		state bool succeeded;

		state Transaction tr(cx);

		state std::map<UID, Optional<Key>> server_dc;
		state std::map<std::vector<UID>, std::pair<std::vector<UID>, std::vector<UID>>> team_cache;
		state std::vector<std::pair<StorageServerInterface, ProcessClass>> tss_servers;
		state int numDataMoves = 0;

		// Get the server list in its own try/catch block since it modifies result.  We don't want a subsequent failure
		// causing entries to be duplicated
		loop {
			numDataMoves = 0;
			server_dc.clear();
			result->allServers.clear();
			tss_servers.clear();
			team_cache.clear();
			succeeded = false;
			try {
				// Read healthyZone value which is later used to determine on/off of failure triggered DD
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Optional<Value> val = wait(tr.get(healthyZoneKey));
				if (val.present()) {
					auto p = decodeHealthyZoneValue(val.get());
					if (p.second > tr.getReadVersion().get() || p.first == ignoreSSFailuresZoneString) {
						result->initHealthyZoneValue = Optional<Key>(p.first);
					} else {
						result->initHealthyZoneValue = Optional<Key>();
					}
				} else {
					result->initHealthyZoneValue = Optional<Key>();
				}

				result->mode = 1;
				Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					rd >> result->mode;
				}
				if (!result->mode || !ddEnabledState->isDDEnabled()) {
					// DD can be disabled persistently (result->mode = 0) or transiently (isDDEnabled() = 0)
					TraceEvent(SevDebug, "GetInitialDataDistribution_DisabledDD").log();
					return result;
				}

				state Future<std::vector<ProcessData>> workers = getWorkers(&tr);
				state Future<RangeResult> serverList = tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
				wait(success(workers) && success(serverList));
				ASSERT(!serverList.get().more && serverList.get().size() < CLIENT_KNOBS->TOO_MANY);

				std::map<Optional<Standalone<StringRef>>, ProcessData> id_data;
				for (int i = 0; i < workers.get().size(); i++)
					id_data[workers.get()[i].locality.processId()] = workers.get()[i];

				for (int i = 0; i < serverList.get().size(); i++) {
					auto ssi = decodeServerListValue(serverList.get()[i].value);
					if (!ssi.isTss()) {
						result->allServers.emplace_back(ssi, id_data[ssi.locality.processId()].processClass);
						server_dc[ssi.id()] = ssi.locality.dcId();
					} else {
						tss_servers.emplace_back(ssi, id_data[ssi.locality.processId()].processClass);
					}
				}

				RangeResult dms = wait(tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!dms.more && dms.size() < CLIENT_KNOBS->TOO_MANY);
				for (int i = 0; i < dms.size(); ++i) {
					auto dataMove = std::make_shared<DataMove>(decodeDataMoveValue(dms[i].value), true);
					const DataMoveMetaData& meta = dataMove->meta;
					for (const UID& id : meta.src) {
						auto& dc = server_dc[id];
						if (std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
							dataMove->remoteSrc.push_back(id);
						} else {
							dataMove->primarySrc.push_back(id);
						}
					}
					for (const UID& id : meta.dest) {
						auto& dc = server_dc[id];
						if (std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
							dataMove->remoteDest.push_back(id);
						} else {
							dataMove->primaryDest.push_back(id);
						}
					}
					std::sort(dataMove->primarySrc.begin(), dataMove->primarySrc.end());
					std::sort(dataMove->remoteSrc.begin(), dataMove->remoteSrc.end());
					std::sort(dataMove->primaryDest.begin(), dataMove->primaryDest.end());
					std::sort(dataMove->remoteDest.begin(), dataMove->remoteDest.end());

					auto ranges = result->dataMoveMap.intersectingRanges(meta.range);
					for (auto& r : ranges) {
						ASSERT(!r.value()->valid);
					}
					result->dataMoveMap.insert(meta.range, std::move(dataMove));
					++numDataMoves;
				}

				succeeded = true;

				break;
			} catch (Error& e) {
				wait(tr.onError(e));

				ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in this loop
				TraceEvent("GetInitialTeamsRetry", distributorId).log();
			}
		}

		// If keyServers is too large to read in a single transaction, then we will have to break this process up into
		// multiple transactions. In that case, each iteration should begin where the previous left off
		while (beginKey < allKeys.end) {
			CODE_PROBE(beginKey > allKeys.begin, "Multi-transactional getInitialDataDistribution");
			loop {
				succeeded = false;
				try {
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					wait(checkMoveKeysLockReadOnly(&tr, moveKeysLock, ddEnabledState));
					state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					RangeResult keyServers = wait(krmGetRanges(&tr,
					                                           keyServersPrefix,
					                                           KeyRangeRef(beginKey, allKeys.end),
					                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
					                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
					succeeded = true;

					std::vector<UID> src, dest, last;
					UID srcId, destId;

					// for each range
					for (int i = 0; i < keyServers.size() - 1; i++) {
						decodeKeyServersValue(UIDtoTagMap, keyServers[i].value, src, dest, srcId, destId);
						DDShardInfo info(keyServers[i].key, srcId, destId);
						if (remoteDcIds.size()) {
							auto srcIter = team_cache.find(src);
							if (srcIter == team_cache.end()) {
								for (auto& id : src) {
									auto& dc = server_dc[id];
									if (std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) != remoteDcIds.end()) {
										info.remoteSrc.push_back(id);
									} else {
										info.primarySrc.push_back(id);
									}
								}
								result->primaryTeams.insert(info.primarySrc);
								result->remoteTeams.insert(info.remoteSrc);
								team_cache[src] = std::make_pair(info.primarySrc, info.remoteSrc);
							} else {
								info.primarySrc = srcIter->second.first;
								info.remoteSrc = srcIter->second.second;
							}
							if (dest.size()) {
								info.hasDest = true;
								auto destIter = team_cache.find(dest);
								if (destIter == team_cache.end()) {
									for (auto& id : dest) {
										auto& dc = server_dc[id];
										if (std::find(remoteDcIds.begin(), remoteDcIds.end(), dc) !=
										    remoteDcIds.end()) {
											info.remoteDest.push_back(id);
										} else {
											info.primaryDest.push_back(id);
										}
									}
									result->primaryTeams.insert(info.primaryDest);
									result->remoteTeams.insert(info.remoteDest);
									team_cache[dest] = std::make_pair(info.primaryDest, info.remoteDest);
								} else {
									info.primaryDest = destIter->second.first;
									info.remoteDest = destIter->second.second;
								}
							}
						} else {
							info.primarySrc = src;
							auto srcIter = team_cache.find(src);
							if (srcIter == team_cache.end()) {
								result->primaryTeams.insert(src);
								team_cache[src] = std::pair<std::vector<UID>, std::vector<UID>>();
							}
							if (dest.size()) {
								info.hasDest = true;
								info.primaryDest = dest;
								auto destIter = team_cache.find(dest);
								if (destIter == team_cache.end()) {
									result->primaryTeams.insert(dest);
									team_cache[dest] = std::pair<std::vector<UID>, std::vector<UID>>();
								}
							}
						}
						result->shards.push_back(info);
					}

					ASSERT_GT(keyServers.size(), 0);
					beginKey = keyServers.end()[-1].key;
					break;
				} catch (Error& e) {
					TraceEvent("GetInitialTeamsKeyServersRetry", distributorId).error(e);

					wait(tr.onError(e));
					ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in this
					                    // loop
				}
			}

			tr.reset();
		}

		// a dummy shard at the end with no keys or servers makes life easier for trackInitialShards()
		result->shards.push_back(DDShardInfo(allKeys.end));

		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA && numDataMoves > 0) {
			for (int shard = 0; shard < result->shards.size() - 1; ++shard) {
				const DDShardInfo& iShard = result->shards[shard];
				KeyRangeRef keys = KeyRangeRef(iShard.key, result->shards[shard + 1].key);
				result->dataMoveMap[keys.begin]->validateShard(iShard, keys);
			}
		}

		// add tss to server list AFTER teams are built
		for (auto& it : tss_servers) {
			result->allServers.push_back(it);
		}

		return result;
	}

	ACTOR static Future<Void> waitForDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
		state Transaction tr(cx);
		loop {
			wait(delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskPriority::DataDistribution));

			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

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

Future<Reference<InitialDataDistribution>> DDTxnProcessor::getInitialDataDistribution(
    const UID& distributorId,
    const MoveKeysLock& moveKeysLock,
    const std::vector<Optional<Key>>& remoteDcIds,
    const DDEnabledState* ddEnabledState) {
	return DDTxnProcessorImpl::getInitialDataDistribution(cx, distributorId, moveKeysLock, remoteDcIds, ddEnabledState);
}

Future<Void> DDTxnProcessor::waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const {
	return DDTxnProcessorImpl::waitForDataDistributionEnabled(cx, ddEnabledState);
}
