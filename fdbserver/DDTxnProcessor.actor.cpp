/*
 * DDTxnProcessor.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
#include "fdbclient/DatabaseContext.h"
#include "flow/genericactors.actor.h"
#include "flow/actorcompiler.h" // This must be the last #include.

static void updateServersAndCompleteSources(std::set<UID>& servers,
                                            std::vector<UID>& completeSources,
                                            int shard,
                                            const std::vector<UID>& src) {
	servers.insert(src.begin(), src.end());
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

class DDTxnProcessorImpl {
	friend class DDTxnProcessor;

	ACTOR static Future<ServerWorkerInfos> getServerListAndProcessClasses(Database cx) {
		state Transaction tr(cx);
		state ServerWorkerInfos res;
		loop {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			try {
				wait(store(res.servers, NativeAPI::getServerListAndProcessClasses(&tr)));
				res.readVersion = tr.getReadVersion().get();
				return res;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

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
						updateServersAndCompleteSources(servers, completeSources, shard, src);
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

	ACTOR static Future<std::vector<IDDTxnProcessor::DDRangeLocations>> getSourceServerInterfacesForRange(
	    Database cx,
	    KeyRangeRef range) {
		state std::vector<IDDTxnProcessor::DDRangeLocations> res;
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

		loop {
			res.clear();
			try {
				state RangeResult shards = wait(krmGetRanges(&tr,
				                                             keyServersPrefix,
				                                             range,
				                                             SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
				                                             SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT));
				ASSERT(!shards.empty());

				state RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				state int i = 0;
				for (i = 0; i < shards.size() - 1; ++i) {
					state std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId, destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);

					std::vector<Future<Optional<Value>>> serverListEntries;
					for (int j = 0; j < src.size(); ++j) {
						serverListEntries.push_back(tr.get(serverListKeyFor(src[j])));
					}
					std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));
					IDDTxnProcessor::DDRangeLocations current(KeyRangeRef(shards[i].key, shards[i + 1].key));
					for (int j = 0; j < serverListValues.size(); ++j) {
						if (!serverListValues[j].present()) {
							TraceEvent(SevWarnAlways, "GetSourceServerInterfacesMissing")
							    .detail("StorageServer", src[j])
							    .detail("Range", KeyRangeRef(shards[i].key, shards[i + 1].key));
							continue;
						}
						StorageServerInterface ssi = decodeServerListValue(serverListValues[j].get());
						current.servers[ssi.locality.describeDcId()].push_back(ssi);
					}
					res.push_back(current);
				}
				break;
			} catch (Error& e) {
				TraceEvent(SevWarnAlways, "GetSourceServerInterfacesError").errorUnsuppressed(e).detail("Range", range);
				wait(tr.onError(e));
			}
		}

		return res;
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

	ACTOR static Future<int> tryUpdateReplicasKeyForDc(Database cx, Optional<Key> dcId, int storageTeamSize) {
		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			try {
				Optional<Value> val = wait(tr.get(datacenterReplicasKeyFor(dcId)));
				state int oldReplicas = val.present() ? decodeDatacenterReplicasValue(val.get()) : 0;
				if (oldReplicas == storageTeamSize) {
					return oldReplicas;
				}
				if (oldReplicas < storageTeamSize) {
					tr.set(rebootWhenDurableKey, StringRef());
				}
				tr.set(datacenterReplicasKeyFor(dcId), datacenterReplicasValue(storageTeamSize));
				wait(tr.commit());

				return oldReplicas;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Read keyservers, return unique set of teams
	ACTOR static Future<Reference<InitialDataDistribution>> getInitialDataDistribution(
	    Database cx,
	    UID distributorId,
	    MoveKeysLock moveKeysLock,
	    std::vector<Optional<Key>> remoteDcIds,
	    const DDEnabledState* ddEnabledState,
	    SkipDDModeCheck skipDDModeCheck) {
		state Reference<InitialDataDistribution> result = makeReference<InitialDataDistribution>();
		state Key beginKey = allKeys.begin;

		state bool succeeded;

		state Transaction tr(cx);

		if (ddLargeTeamEnabled()) {
			wait(store(result->userRangeConfig,
			           DDConfiguration().userRangeConfig().getSnapshot(
			               SystemDBWriteLockedNow(cx.getReference()), allKeys.begin, allKeys.end)));
		}
		state std::map<UID, Optional<Key>> server_dc;
		state std::map<std::vector<UID>, std::pair<std::vector<UID>, std::vector<UID>>> team_cache;
		state std::vector<std::pair<StorageServerInterface, ProcessClass>> tss_servers;
		state int numDataMoves = 0;

		CODE_PROBE((bool)skipDDModeCheck, "DD Mode won't prevent read initial data distribution.");
		// Get the server list in its own try/catch block since it modifies result.  We don't want a subsequent failure
		// causing entries to be duplicated
		loop {
			numDataMoves = 0;
			server_dc.clear();
			result->allServers.clear();
			result->dataMoveMap = KeyRangeMap<std::shared_ptr<DataMove>>(std::make_shared<DataMove>());
			result->auditStates.clear();
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
				if ((!skipDDModeCheck && !result->mode) || !ddEnabledState->isEnabled()) {
					// DD can be disabled persistently (result->mode = 0) or transiently (isEnabled() = 0)
					TraceEvent(SevDebug, "GetInitialDataDistribution_DisabledDD").log();
					return result;
				}

				result->bulkLoadMode = 0;
				Optional<Value> bulkLoadMode = wait(tr.get(bulkLoadModeKey));
				if (bulkLoadMode.present()) {
					BinaryReader rd(bulkLoadMode.get(), Unversioned());
					rd >> result->bulkLoadMode;
				}
				TraceEvent(SevInfo, "DDBulkLoadInitMode").detail("Mode", result->bulkLoadMode);

				result->bulkDumpMode = 0;
				Optional<Value> bulkDumpMode = wait(tr.get(bulkDumpModeKey));
				if (bulkDumpMode.present()) {
					BinaryReader rd(bulkDumpMode.get(), Unversioned());
					rd >> result->bulkDumpMode;
				}
				TraceEvent(SevInfo, "DDBulkDumpInitMode").detail("Mode", result->bulkDumpMode);

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
				// For each data move, find out the src or dst servers are in primary or remote DC.
				for (int i = 0; i < dms.size(); ++i) {
					auto dataMove = std::make_shared<DataMove>(decodeDataMoveValue(dms[i].value), true);
					const DataMoveMetaData& meta = dataMove->meta;
					if (meta.ranges.empty()) {
						// Any persisted datamove with an empty range must be an tombstone persisted by
						// a background cleanup (with retry_clean_up_datamove_tombstone_added),
						// and this datamove must be in DataMoveMetaData::Deleting state
						// A datamove without processed by a background cleanup must have a non-empty range
						// For this case, we simply clear the range when dd init
						ASSERT(meta.getPhase() == DataMoveMetaData::Deleting);
						result->toCleanDataMoveTombstone.push_back(meta.id);
						continue;
					}
					ASSERT(!meta.ranges.empty());
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

					auto ranges = result->dataMoveMap.intersectingRanges(meta.ranges.front());
					for (auto& r : ranges) {
						ASSERT(!r.value()->valid);
					}
					result->dataMoveMap.insert(meta.ranges.front(), std::move(dataMove));
					++numDataMoves;
				}

				succeeded = true;

				break;
			} catch (Error& e) {
				TraceEvent("GetInitialTeamsRetry", distributorId).error(e);
				wait(tr.onError(e));

				ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in this loop
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
				if (!mode.present() && ddEnabledState->isEnabled()) {
					TraceEvent("WaitForDDEnabledSucceeded").log();
					return Void();
				}
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					int m;
					rd >> m;
					TraceEvent(SevDebug, "WaitForDDEnabled")
					    .detail("Mode", m)
					    .detail("IsDDEnabled", ddEnabledState->isEnabled());
					if (m && ddEnabledState->isEnabled()) {
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

	ACTOR static Future<bool> isDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
		state Transaction tr(cx);
		loop {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			try {
				Optional<Value> mode = wait(tr.get(dataDistributionModeKey));
				if (!mode.present() && ddEnabledState->isEnabled())
					return true;
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					int m;
					rd >> m;
					if (m && ddEnabledState->isEnabled()) {
						TraceEvent(SevDebug, "IsDDEnabledSucceeded")
						    .detail("Mode", m)
						    .detail("IsDDEnabled", ddEnabledState->isEnabled());
						return true;
					}
				}
				// SOMEDAY: Write a wrapper in MoveKeys.actor.h
				Optional<Value> readVal = wait(tr.get(moveKeysLockOwnerKey));
				UID currentOwner =
				    readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
				if (ddEnabledState->isEnabled() && (currentOwner != dataDistributionModeLock)) {
					TraceEvent(SevDebug, "IsDDEnabledSucceeded")
					    .detail("CurrentOwner", currentOwner)
					    .detail("DDModeLock", dataDistributionModeLock)
					    .detail("IsDDEnabled", ddEnabledState->isEnabled());
					return true;
				}
				TraceEvent(SevDebug, "IsDDEnabledFailed")
				    .detail("CurrentOwner", currentOwner)
				    .detail("DDModeLock", dataDistributionModeLock)
				    .detail("IsDDEnabled", ddEnabledState->isEnabled());
				return false;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> pollMoveKeysLock(Database cx, MoveKeysLock lock, const DDEnabledState* ddEnabledState) {
		loop {
			wait(delay(SERVER_KNOBS->MOVEKEYS_LOCK_POLLING_DELAY));
			state Transaction tr(cx);
			loop {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				try {
					wait(checkMoveKeysLockReadOnly(&tr, lock, ddEnabledState));
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}
	}

	ACTOR static Future<Optional<Value>> readRebalanceDDIgnoreKey(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				Optional<Value> res = wait(tr.get(rebalanceDDIgnoreKey));
				return res;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> waitDDTeamInfoPrintSignal(Database cx) {
		state ReadYourWritesTransaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				state Future<Void> watchFuture = tr.watch(triggerDDTeamInfoPrintKey);
				wait(tr.commit());
				wait(watchFuture);
				return Void();
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> waitForAllDataRemoved(
	    Database cx,
	    UID serverID,
	    Version addedVersion,
	    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) {
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		loop {
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Version ver = wait(tr->getReadVersion());

				// we cannot remove a server immediately after adding it, because a perfectly timed cluster recovery
				// could cause us to not store the mutations sent to the short lived storage server.
				if (ver > addedVersion + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
					bool canRemove = wait(canRemoveStorageServer(tr, serverID));
					auto shards = shardsAffectedByTeamFailure->getNumberOfShards(serverID);
					TraceEvent(SevVerbose, "WaitForAllDataRemoved")
					    .detail("Server", serverID)
					    .detail("CanRemove", canRemove)
					    .detail("Shards", shards);
					ASSERT_GE(shards, 0);
					if (canRemove && shards == 0) {
						return Void();
					}
				}

				// Wait for any change to the serverKeys for this server
				wait(delay(SERVER_KNOBS->ALL_DATA_REMOVED_DELAY, TaskPriority::DataDistribution));
				tr->reset();
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}
};

Future<Void> DDTxnProcessor::waitForAllDataRemoved(
    const UID& serverID,
    const Version& addedVersion,
    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) const {
	return DDTxnProcessorImpl::waitForAllDataRemoved(cx, serverID, addedVersion, shardsAffectedByTeamFailure);
}

Future<IDDTxnProcessor::SourceServers> DDTxnProcessor::getSourceServersForRange(const KeyRangeRef range) {
	return DDTxnProcessorImpl::getSourceServersForRange(cx, range);
}

Future<std::vector<IDDTxnProcessor::DDRangeLocations>> DDTxnProcessor::getSourceServerInterfacesForRange(
    const KeyRangeRef range) {
	return DDTxnProcessorImpl::getSourceServerInterfacesForRange(cx, range);
}

Future<ServerWorkerInfos> DDTxnProcessor::getServerListAndProcessClasses() {
	return DDTxnProcessorImpl::getServerListAndProcessClasses(cx);
}

Future<MoveKeysLock> DDTxnProcessor::takeMoveKeysLock(const UID& ddId) const {
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
    const DDEnabledState* ddEnabledState,
    SkipDDModeCheck skipDDModeCheck) {
	return DDTxnProcessorImpl::getInitialDataDistribution(
	    cx, distributorId, moveKeysLock, remoteDcIds, ddEnabledState, skipDDModeCheck);
}

Future<Void> DDTxnProcessor::waitForDataDistributionEnabled(const DDEnabledState* ddEnabledState) const {
	return DDTxnProcessorImpl::waitForDataDistributionEnabled(cx, ddEnabledState);
}

Future<bool> DDTxnProcessor::isDataDistributionEnabled(const DDEnabledState* ddEnabledState) const {
	return DDTxnProcessorImpl::isDataDistributionEnabled(cx, ddEnabledState);
}

Future<Void> DDTxnProcessor::pollMoveKeysLock(const MoveKeysLock& lock, const DDEnabledState* ddEnabledState) const {
	return DDTxnProcessorImpl::pollMoveKeysLock(cx, lock, ddEnabledState);
}

Future<std::pair<Optional<StorageMetrics>, int>> DDTxnProcessor::waitStorageMetrics(
    const KeyRange& keys,
    const StorageMetrics& min,
    const StorageMetrics& max,
    const StorageMetrics& permittedError,
    int shardLimit,
    int expectedShardCount) const {
	return cx->waitStorageMetrics(keys, min, max, permittedError, shardLimit, expectedShardCount);
}

Future<Standalone<VectorRef<KeyRef>>> DDTxnProcessor::splitStorageMetrics(const KeyRange& keys,
                                                                          const StorageMetrics& limit,
                                                                          const StorageMetrics& estimated,
                                                                          const Optional<int>& minSplitBytes) const {
	return cx->splitStorageMetrics(keys, limit, estimated, minSplitBytes);
}

Future<Standalone<VectorRef<ReadHotRangeWithMetrics>>> DDTxnProcessor::getReadHotRanges(const KeyRange& keys) const {
	return cx->getReadHotRanges(keys);
}

Future<HealthMetrics> DDTxnProcessor::getHealthMetrics(bool detailed) const {
	return cx->getHealthMetrics(detailed);
}

Future<Optional<Value>> DDTxnProcessor::readRebalanceDDIgnoreKey() const {
	return DDTxnProcessorImpl::readRebalanceDDIgnoreKey(cx);
}

Future<int> DDTxnProcessor::tryUpdateReplicasKeyForDc(const Optional<Key>& dcId, const int& storageTeamSize) const {
	return DDTxnProcessorImpl::tryUpdateReplicasKeyForDc(cx, dcId, storageTeamSize);
}

Future<Void> DDTxnProcessor::waitDDTeamInfoPrintSignal() const {
	return DDTxnProcessorImpl::waitDDTeamInfoPrintSignal(cx);
}

Future<std::vector<ProcessData>> DDTxnProcessor::getWorkers() const {
	return ::getWorkers(cx);
}

Future<Void> DDTxnProcessor::rawStartMovement(const MoveKeysParams& params,
                                              std::map<UID, StorageServerInterface>& tssMapping) {
	return ::rawStartMovement(cx, params, tssMapping);
}

Future<Void> DDTxnProcessor::rawFinishMovement(const MoveKeysParams& params,
                                               const std::map<UID, StorageServerInterface>& tssMapping) {
	return ::rawFinishMovement(cx, params, tssMapping);
}

struct DDMockTxnProcessorImpl {
	// return when all status become FETCHED
	ACTOR static Future<Void> checkFetchingState(DDMockTxnProcessor* self, std::vector<UID> ids, KeyRangeRef range) {
		state TraceInterval interval("MockCheckFetchingState");
		TraceEvent(SevDebug, interval.begin()).detail("Range", range);
		loop {
			wait(delayJittered(1.0, TaskPriority::FetchKeys));
			bool done = true;
			for (auto& id : ids) {
				auto& server = self->mgs->allServers.at(id);
				if (!server->allShardStatusIn(range, { MockShardStatus::FETCHED, MockShardStatus::COMPLETED })) {
					done = false;
					break;
				}
			}
			if (done) {
				break;
			}
		}
		TraceEvent(SevDebug, interval.end()).log();
		return Void();
	}

	static Future<Void> rawCheckFetchingState(DDMockTxnProcessor* self, const MoveKeysParams& params) {
		if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
			ASSERT(params.ranges.present());
			// TODO: make startMoveShards work with multiple ranges.
			ASSERT(params.ranges.get().size() == 1);
			return checkFetchingState(self, params.destinationTeam, params.ranges.get().at(0));
		}
		ASSERT(params.keys.present());
		return checkFetchingState(self, params.destinationTeam, params.keys.get());
	}

	ACTOR static Future<Void> moveKeys(DDMockTxnProcessor* self, MoveKeysParams params) {
		state std::map<UID, StorageServerInterface> tssMapping;
		// Because SFBTF::Team requires the ID is ordered
		std::sort(params.destinationTeam.begin(), params.destinationTeam.end());
		std::sort(params.healthyDestinations.begin(), params.healthyDestinations.end());

		wait(self->rawStartMovement(params, tssMapping));
		ASSERT(tssMapping.empty());

		wait(rawCheckFetchingState(self, params));

		wait(self->rawFinishMovement(params, tssMapping));
		if (!params.dataMovementComplete.isSet())
			params.dataMovementComplete.send(Void());
		return Void();
	}
};

Future<ServerWorkerInfos> DDMockTxnProcessor::getServerListAndProcessClasses() {
	ServerWorkerInfos res;
	for (auto& [_, mss] : mgs->allServers) {
		res.servers.emplace_back(mss->ssi, ProcessClass(ProcessClass::StorageClass, ProcessClass::DBSource));
	}
	// FIXME(xwang): possible generate version from time?
	res.readVersion = 0;
	return res;
}

std::pair<std::set<std::vector<UID>>, std::set<std::vector<UID>>> getAllTeamsInRegion(
    const std::vector<DDShardInfo>& shards) {
	std::set<std::vector<UID>> primary, remote;
	for (auto& info : shards) {
		if (!info.primarySrc.empty())
			primary.emplace(info.primarySrc);
		if (!info.primaryDest.empty())
			primary.emplace(info.primaryDest);
		if (!info.remoteSrc.empty())
			remote.emplace(info.remoteSrc);
		if (!info.remoteDest.empty())
			remote.emplace(info.remoteDest);
	}
	return { primary, remote };
}

inline void transformTeamsToServerIds(std::vector<ShardsAffectedByTeamFailure::Team>& teams,
                                      std::vector<UID>& primaryIds,
                                      std::vector<UID>& remoteIds) {
	std::set<UID> primary, remote;
	for (auto& team : teams) {
		team.primary ? primary.insert(team.servers.begin(), team.servers.end())
		             : remote.insert(team.servers.begin(), team.servers.end());
	}
	primaryIds = std::vector<UID>(primary.begin(), primary.end());
	remoteIds = std::vector<UID>(remote.begin(), remote.end());
}

// reconstruct DDShardInfos from shardMapping
std::vector<DDShardInfo> DDMockTxnProcessor::getDDShardInfos() const {
	std::vector<DDShardInfo> res;
	res.reserve(mgs->shardMapping->getNumberOfShards());
	auto allRange = mgs->shardMapping->getAllRanges();
	ASSERT(allRange.end().begin() == allKeys.end);
	for (auto it = allRange.begin(); it != allRange.end(); ++it) {
		// FIXME: now just use anonymousShardId
		KeyRangeRef curRange = it->range();
		DDShardInfo info(curRange.begin);

		auto teams = mgs->shardMapping->getTeamsForFirstShard(curRange);
		if (!teams.first.empty() && !teams.second.empty()) {
			CODE_PROBE(true, "Mock InitialDataDistribution In-Flight shard");
			info.hasDest = true;
			info.destId = anonymousShardId;
			info.srcId = anonymousShardId;
			transformTeamsToServerIds(teams.second, info.primarySrc, info.remoteSrc);
			transformTeamsToServerIds(teams.first, info.primaryDest, info.remoteDest);
		} else if (!teams.first.empty()) {
			CODE_PROBE(true, "Mock InitialDataDistribution Static shard");
			info.srcId = anonymousShardId;
			transformTeamsToServerIds(teams.first, info.primarySrc, info.remoteSrc);
		} else {
			ASSERT(false);
		}

		res.push_back(std::move(info));
	}
	res.emplace_back(allKeys.end);

	return res;
}

Future<Reference<InitialDataDistribution>> DDMockTxnProcessor::getInitialDataDistribution(
    const UID& distributorId,
    const MoveKeysLock& moveKeysLock,
    const std::vector<Optional<Key>>& remoteDcIds,
    const DDEnabledState* ddEnabledState,
    SkipDDModeCheck skipDDModeCheck) {

	// FIXME: now we just ignore ddEnabledState and moveKeysLock, will fix it in the future
	Reference<InitialDataDistribution> res = makeReference<InitialDataDistribution>();
	res->mode = 1;
	res->allServers = getServerListAndProcessClasses().get().servers;
	res->shards = getDDShardInfos();
	std::tie(res->primaryTeams, res->remoteTeams) = getAllTeamsInRegion(res->shards);
	return res;
}

Future<Void> DDMockTxnProcessor::removeKeysFromFailedServer(const UID& serverID,
                                                            const std::vector<UID>& teamForDroppedRange,
                                                            const MoveKeysLock& lock,
                                                            const DDEnabledState* ddEnabledState) const {

	// This function only takes effect when user exclude failed IP:PORT in the fdbcli. In the first version , the mock
	// class won’t support this.
	UNREACHABLE();
}

Future<Void> DDMockTxnProcessor::removeStorageServer(const UID& serverID,
                                                     const Optional<UID>& tssPairID,
                                                     const MoveKeysLock& lock,
                                                     const DDEnabledState* ddEnabledState) const {
	ASSERT(mgs->allShardsRemovedFromServer(serverID));
	mgs->allServers.erase(serverID);
	return Void();
}

void DDMockTxnProcessor::setupMockGlobalState(Reference<InitialDataDistribution> initData) {
	for (auto& [ssi, pInfo] : initData->allServers) {
		mgs->addStorageServer(ssi);
	}
	mgs->shardMapping->setCheckMode(ShardsAffectedByTeamFailure::CheckMode::ForceNoCheck);

	for (int i = 0; i < initData->shards.size() - 1; ++i) {
		// insert to keyServers
		auto& shardInfo = initData->shards[i];
		ASSERT(shardInfo.remoteSrc.empty() && shardInfo.remoteDest.empty());

		uint64_t shardBytes =
		    deterministicRandom()->randomInt(SERVER_KNOBS->MIN_SHARD_BYTES, SERVER_KNOBS->MAX_SHARD_BYTES);
		KeyRangeRef keys(shardInfo.key, initData->shards[i + 1].key);
		mgs->shardMapping->assignRangeToTeams(keys, { { shardInfo.primarySrc, true } });
		if (shardInfo.hasDest) {
			mgs->shardMapping->moveShard(keys, { { shardInfo.primaryDest, true } });
		}
		// insert to serverKeys
		for (auto& id : shardInfo.primarySrc) {
			mgs->allServers.at(id)->serverKeys.insert(keys, { MockShardStatus::COMPLETED, shardBytes });
		}
		for (auto& id : shardInfo.primaryDest) {
			mgs->allServers.at(id)->serverKeys.insert(keys, { MockShardStatus::INFLIGHT, shardBytes });
		}
	}

	mgs->shardMapping->setCheckMode(ShardsAffectedByTeamFailure::CheckMode::Normal);
}

Future<Void> DDMockTxnProcessor::moveKeys(const MoveKeysParams& params) {
	// Not support location metadata yet
	ASSERT(!SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
	return DDMockTxnProcessorImpl::moveKeys(this, params);
}

// FIXME: finish implementation
Future<HealthMetrics> DDMockTxnProcessor::getHealthMetrics(bool detailed) const {
	return Future<HealthMetrics>();
}

Future<Standalone<VectorRef<KeyRef>>> DDMockTxnProcessor::splitStorageMetrics(
    const KeyRange& keys,
    const StorageMetrics& limit,
    const StorageMetrics& estimated,
    const Optional<int>& minSplitBytes) const {
	return mgs->splitStorageMetrics(keys, limit, estimated, minSplitBytes);
}

Future<std::pair<Optional<StorageMetrics>, int>> DDMockTxnProcessor::waitStorageMetrics(
    const KeyRange& keys,
    const StorageMetrics& min,
    const StorageMetrics& max,
    const StorageMetrics& permittedError,
    int shardLimit,
    int expectedShardCount) const {
	return mgs->waitStorageMetrics(keys, min, max, permittedError, shardLimit, expectedShardCount);
}

// FIXME: finish implementation
Future<std::vector<ProcessData>> DDMockTxnProcessor::getWorkers() const {
	return Future<std::vector<ProcessData>>();
}

ACTOR Future<Void> rawStartMovement(std::shared_ptr<MockGlobalState> mgs,
                                    MoveKeysParams params,
                                    std::map<UID, StorageServerInterface> tssMapping) {
	state TraceInterval interval("RelocateShard_MockStartMoveKeys");
	state KeyRange keys;
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		ASSERT(params.ranges.present());
		// TODO: make startMoveShards work with multiple ranges.
		ASSERT(params.ranges.get().size() == 1);
		keys = params.ranges.get().at(0);
	} else {
		ASSERT(params.keys.present());
		keys = params.keys.get();
	}
	TraceEvent(SevDebug, interval.begin()).detail("Keys", keys);
	wait(params.startMoveKeysParallelismLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser(*params.startMoveKeysParallelismLock);

	std::vector<ShardsAffectedByTeamFailure::Team> destTeams;
	destTeams.emplace_back(params.destinationTeam, true);
	// invariant: the splitting and merge operation won't happen at the same moveKeys action. For example, if [a,c) [c,
	// e) exists, the params.keys won't be [b, d).
	auto intersectRanges = mgs->shardMapping->intersectingRanges(keys);
	// 1. splitting or just move a range. The new boundary need to be defined in startMovement
	if (intersectRanges.begin().range().contains(keys)) {
		mgs->shardMapping->defineShard(keys);
	}
	// 2. merge ops will coalesce the boundary in finishMovement;
	intersectRanges = mgs->shardMapping->intersectingRanges(keys);
	fmt::print("Keys: {}; intersect: {} {}\n",
	           keys.toString(),
	           intersectRanges.begin().begin(),
	           intersectRanges.end().begin());
	// NOTE: What if there is a split follow up by a merge?
	// ASSERT(keys.begin == intersectRanges.begin().begin());
	// ASSERT(keys.end == intersectRanges.end().begin());

	int totalRangeSize = 0;
	for (auto it = intersectRanges.begin(); it != intersectRanges.end(); ++it) {
		auto teamPair = mgs->shardMapping->getTeamsFor(it->begin());
		auto& srcTeams = teamPair.second.empty() ? teamPair.first : teamPair.second;
		totalRangeSize += mgs->getRangeSize(it->range());
		mgs->shardMapping->rawMoveShard(it->range(), srcTeams, destTeams);
	}

	for (auto& id : params.destinationTeam) {
		auto& server = mgs->allServers.at(id);
		server->setShardStatus(keys, MockShardStatus::INFLIGHT);
		server->signalFetchKeys(keys, totalRangeSize);
	}
	TraceEvent(SevDebug, interval.end());
	return Void();
}

Future<Void> DDMockTxnProcessor::rawStartMovement(const MoveKeysParams& params,
                                                  std::map<UID, StorageServerInterface>& tssMapping) {
	return ::rawStartMovement(mgs, params, tssMapping);
}

ACTOR Future<Void> rawFinishMovement(std::shared_ptr<MockGlobalState> mgs,
                                     MoveKeysParams params,
                                     std::map<UID, StorageServerInterface> tssMapping) {
	state TraceInterval interval("RelocateShard_MockFinishMoveKeys");
	state KeyRange keys;
	if (SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
		ASSERT(params.ranges.present());
		// TODO: make startMoveShards work with multiple ranges.
		ASSERT(params.ranges.get().size() == 1);
		keys = params.ranges.get().at(0);
	} else {
		ASSERT(params.keys.present());
		keys = params.keys.get();
	}

	TraceEvent(SevDebug, interval.begin()).detail("Keys", keys);

	wait(params.finishMoveKeysParallelismLock->take(TaskPriority::DataDistributionLaunch));
	state FlowLock::Releaser releaser(*params.finishMoveKeysParallelismLock);

	// get source and dest teams
	auto [destTeams, srcTeams] = mgs->shardMapping->getTeamsForFirstShard(keys);

	ASSERT_EQ(destTeams.size(), 1); // Will the multi-region or dynamic replica make destTeam.size() > 1?
	if (destTeams.front() != ShardsAffectedByTeamFailure::Team{ params.destinationTeam, true }) {
		TraceEvent(SevError, "MockRawFinishMovementError")
		    .detail("Reason", "InconsistentDestinations")
		    .detail("ShardMappingDest", describe(destTeams.front().servers))
		    .detail("ParamDest", describe(params.destinationTeam));
		ASSERT(false); // This shouldn't happen because the overlapped key range movement won't be executed in parallel
	}

	for (auto& id : params.destinationTeam) {
		auto server = mgs->allServers.at(id);
		server->setShardStatus(keys, MockShardStatus::COMPLETED);
		server->coalesceCompletedRange(keys);
	}

	// remove destination servers from source servers
	ASSERT_EQ(srcTeams.size(), 1);
	for (auto& id : srcTeams.front().servers) {
		// the only caller moveKeys will always make sure the UID are sorted
		if (!std::binary_search(params.destinationTeam.begin(), params.destinationTeam.end(), id)) {
			mgs->allServers.at(id)->removeShard(keys);
		}
	}
	mgs->shardMapping->finishMove(keys);
	mgs->shardMapping->defineShard(keys); // coalesce for merge
	TraceEvent(SevDebug, interval.end());
	return Void();
}

Future<Void> DDMockTxnProcessor::rawFinishMovement(const MoveKeysParams& params,
                                                   const std::map<UID, StorageServerInterface>& tssMapping) {
	return ::rawFinishMovement(mgs, params, tssMapping);
}

Future<Optional<HealthMetrics::StorageStats>> DDTxnProcessor::getStorageStats(const UID& id,
                                                                              double maxStaleness) const {
	return cx->getStorageStats(id, maxStaleness);
}

Future<Optional<HealthMetrics::StorageStats>> DDMockTxnProcessor::getStorageStats(const UID& id,
                                                                                  double maxStaleness) const {
	auto it = mgs->allServers.find(id);
	if (it == mgs->allServers.end()) {
		return Optional<HealthMetrics::StorageStats>();
	}
	return Optional<HealthMetrics::StorageStats>(it->second->getStorageStats());
}

Future<DatabaseConfiguration> DDMockTxnProcessor::getDatabaseConfiguration() const {
	return mgs->configuration;
}

Future<IDDTxnProcessor::SourceServers> DDMockTxnProcessor::getSourceServersForRange(const KeyRangeRef keys) {
	std::set<UID> servers;
	std::vector<UID> completeSources;
	auto ranges = mgs->shardMapping->intersectingRanges(keys);
	int count = 0;
	for (auto it = ranges.begin(); it != ranges.end(); ++it, ++count) {
		auto sources = mgs->shardMapping->getSourceServerIdsFor(it->begin());
		ASSERT(!sources.empty());
		updateServersAndCompleteSources(servers, completeSources, count, sources);
	}
	ASSERT(!servers.empty());
	return IDDTxnProcessor::SourceServers{ std::vector<UID>(servers.begin(), servers.end()), completeSources };
}

Future<Void> DDMockTxnProcessor::waitForAllDataRemoved(
    const UID& serverID,
    const Version& addedVersion,
    Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) const {
	return checkUntil(
	    SERVER_KNOBS->ALL_DATA_REMOVED_DELAY,
	    [&, this]() -> bool {
		    return mgs->allShardsRemovedFromServer(serverID) &&
		           shardsAffectedByTeamFailure->getNumberOfShards(serverID) == 0;
	    },
	    TaskPriority::DataDistribution);
}