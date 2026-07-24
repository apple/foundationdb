/*
 * DDTxnProcessor.cpp
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

#include "DDTxnProcessor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ManagementAPI.h"
#include "DataDistribution.h"
#include "fdbclient/DatabaseContext.h"
#include "flow/TxnCounters.h"
#include "flow/CoroUtils.h"
#include "flow/genericactors.actor.h"

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

	static Future<ServerWorkerInfos> getServerListAndProcessClasses(Database cx) {
		Transaction tr(cx);
		ServerWorkerInfos res;
		while (true) {
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Error err;
			try {
				res.servers = co_await NativeAPI::getServerListAndProcessClasses(&tr);
				res.readVersion = tr.getReadVersion().get();
				co_return res;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	// return {sourceServers, completeSources}
	static Future<IDDTxnProcessor::SourceServers> getSourceServersForRange(Database cx, KeyRangeRef keys) {
		std::set<UID> servers;
		std::vector<UID> completeSources;
		Transaction tr(cx);

		while (true) {
			servers.clear();
			completeSources.clear();

			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			Error err;
			try {
				RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
				RangeResult keyServersEntries = co_await tr.getRange(lastLessOrEqual(keyServersKey(keys.begin)),
				                                                     firstGreaterOrEqual(keyServersKey(keys.end)),
				                                                     SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS);

				if (keyServersEntries.size() < SERVER_KNOBS->DD_QUEUE_MAX_KEY_SERVERS) {
					for (int shard = 0; shard < keyServersEntries.size(); shard++) {
						std::vector<UID> src, dest;
						decodeKeyServersValue(UIDtoTagMap, keyServersEntries[shard].value, src, dest);
						ASSERT(!src.empty());
						updateServersAndCompleteSources(servers, completeSources, shard, src);
					}

					ASSERT(!servers.empty());
				}

				// If the size of keyServerEntries is large, then just assume we are using all storage servers
				// Why the size can be large?
				// When a shard is inflight and DD crashes, some destination servers may have already got the data.
				// The new DD will treat the destination servers as source servers. So the size can be large.
				else {
					RangeResult serverList = co_await tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
					ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

					for (auto s = serverList.begin(); s != serverList.end(); ++s)
						servers.insert(decodeServerListValue(s->value).id());

					ASSERT(!servers.empty());
				}

				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}

		co_return IDDTxnProcessor::SourceServers{ std::vector<UID>(servers.begin(), servers.end()), completeSources };
	}

	static Future<std::vector<IDDTxnProcessor::DDRangeLocations>> getSourceServerInterfacesForRange(Database cx,
	                                                                                                KeyRangeRef range) {
		std::vector<IDDTxnProcessor::DDRangeLocations> res;
		Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

		while (true) {
			res.clear();
			Error err;
			try {
				RangeResult shards = co_await krmGetRanges(&tr,
				                                           keyServersPrefix,
				                                           range,
				                                           SERVER_KNOBS->MOVE_SHARD_KRM_ROW_LIMIT,
				                                           SERVER_KNOBS->MOVE_SHARD_KRM_BYTE_LIMIT);
				ASSERT(!shards.empty());

				RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
				ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

				int i = 0;
				for (i = 0; i < shards.size() - 1; ++i) {
					std::vector<UID> src;
					std::vector<UID> dest;
					UID srcId, destId;
					decodeKeyServersValue(UIDtoTagMap, shards[i].value, src, dest, srcId, destId);

					std::vector<Future<Optional<Value>>> serverListEntries;
					for (int j = 0; j < src.size(); ++j) {
						serverListEntries.push_back(tr.get(serverListKeyFor(src[j])));
					}
					std::vector<Optional<Value>> serverListValues = co_await getAll(serverListEntries);
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
				err = e;
				TraceEvent(SevWarnAlways, "GetSourceServerInterfacesError")
				    .errorUnsuppressed(err)
				    .detail("Range", range);
			}
			co_await tr.onError(err);
		}

		co_return res;
	}

	// set the system key space
	static Future<Void> updateReplicaKeys(Database cx,
	                                      std::vector<Optional<Key>> primaryDcId,
	                                      std::vector<Optional<Key>> remoteDcIds,
	                                      DatabaseConfiguration configuration) {
		static auto* counters = makeCounters("/dd/updateReplicaKeys");
		Transaction tr(cx);
		while (true) {
			counters->started->increment(1);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				RangeResult replicaKeys = co_await tr.getRange(datacenterReplicasKeys, CLIENT_KNOBS->TOO_MANY);

				for (auto& kv : replicaKeys) {
					auto dcId = decodeDatacenterReplicasKey(kv.key);
					auto replicas = decodeDatacenterReplicasValue(kv.value);
					if ((!primaryDcId.empty() && primaryDcId.at(0) == dcId) ||
					    (!remoteDcIds.empty() && remoteDcIds.at(0) == dcId && configuration.usableRegions > 1)) {
						if (replicas > configuration.storageTeamSize) {
							tr.set(kv.key, datacenterReplicasValue(configuration.storageTeamSize));
						}
					} else {
						tr.clear(kv.key);
					}
				}

				co_await tr.commit();
				counters->committed->increment(1);
				break;
			} catch (Error& e) {
				counters->aborted->increment(1);
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<int> tryUpdateReplicasKeyForDc(Database cx, Optional<Key> dcId, int storageTeamSize) {
		static auto* counters = makeCounters("/dd/tryUpdateReplicasKeyForDc");
		Transaction tr(cx);
		while (true) {
			counters->started->increment(1);
			tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Error err;
			try {
				Optional<Value> val = co_await tr.get(datacenterReplicasKeyFor(dcId));
				int oldReplicas = val.present() ? decodeDatacenterReplicasValue(val.get()) : 0;
				if (oldReplicas == storageTeamSize) {
					co_return oldReplicas;
				}
				if (oldReplicas < storageTeamSize) {
					tr.set(rebootWhenDurableKey, StringRef());
				}
				tr.set(datacenterReplicasKeyFor(dcId), datacenterReplicasValue(storageTeamSize));
				co_await tr.commit();
				counters->committed->increment(1);

				co_return oldReplicas;
			} catch (Error& e) {
				counters->aborted->increment(1);
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Optional<Key>> getHealthyZone(Database cx, UID distributorId) {
		// Read healthyZone value which is later used to determine on/off of failure triggered DD.
		// Occasionally this can be slow to read.  This is due to hotspots on the \xff\x02 shard
		// due to over-aggressive use of this feature:
		// https://apple.github.io/foundationdb/transaction-profiler-analyzer.html
		// All in all, it's better to succeed in starting up with a risk to a
		// non-default feature (maintenance mode) than to block DD startup indefinitely.
		Transaction tr(cx);
		int maxRetries = SERVER_KNOBS->DD_HEALTHY_ZONE_READ_RETRY_COUNT;
		bool healthyZoneRead = false;
		Optional<Value> healthyZoneVal;
		int i = 0;
		for (; i < maxRetries; i++) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				Optional<Value> _hz = co_await tr.get(healthyZoneKey);
				healthyZoneVal = _hz;
				healthyZoneRead = true;
				break;
			} catch (Error& e) {
				err = e;
				TraceEvent("ReadHealthyZone", distributorId).error(err);
			}
			co_await tr.onError(err);
		}
		if (healthyZoneRead) {
			if (healthyZoneVal.present()) {
				auto p = decodeHealthyZoneValue(healthyZoneVal.get());
				if (p.second > tr.getReadVersion().get() || p.first == ignoreSSFailuresZoneString) {
					co_return Optional<Key>(p.first);
				}
			}
		} else {
			TraceEvent(g_network->isSimulated() ? SevWarnAlways : SevError, "ReadHealthyZone", distributorId)
			    .detail("MaxRetries", maxRetries)
			    .detail("AdditionalInfo",
			            "Maintenance mode settings (if any) will be ignored until the next"
			            " cluster restart or change to maintenance mode settings."
			            " Warning: Data distribution may happen for storage servers in maintenance zones that"
			            " appear to be down. Data distributor/cluster restart can be used to reattempt to read"
			            " the maintenance mode settings.");
		}
		co_return Optional<Key>();
	}

	// When SHARD_ENCODE_LOCATION_METADATA is false on DD init, do a bounded
	// rewrite to start the rollback. Two phases, both bounded:
	//
	// Phase 1: Clear all DataMoveMetaData (single transaction; the dataMoves
	// keyspace is small, this is always one commit).
	//
	// Phase 2: Rewrite up to 1000 keyServers entries at the head of the
	// prefix from new (UID-based) to old (tag-based) format. Returns true
	// if either phase committed; the caller restarts the outer init loop
	// and calls back in. The Phase-2 cap means clusters with more than
	// 1000 shard-encoded keyServers entries are NOT fully rewritten by
	// this function — by design. Bulk rewrite happens through normal
	// shard movement / storage wiggle once the knob is false (see the
	// "Migration for downgrade" section of
	// design/shard-encode-location-metadata.md); this function just
	// clears dataMoves and rewrites the small remnant at the head so DD
	// init has a tidy starting point.
	//
	// Calling this when nothing needs rewriting (knob has been false the
	// whole time, or rollback already complete) is safe and
	// write-cost-free: the reads find no shard-encoded entries, no
	// commits happen, returns false. Cost is three system-key reads on
	// every DD init when knob is false.
	//
	// serverKeys entries are left in place — they drain naturally as DD
	// moves shards using the old path.
	static Future<bool> rewriteShardEncodedMetadata(Transaction& tr, UID distributorId) {
		TraceEvent(SevInfo, "DDInitShardEncodeOff", distributorId)
		    .detail("KnobValue", SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);

		// Phase 1: Clear all DataMoveMetaData
		RangeResult dmsCheck = co_await tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY);
		ASSERT(!dmsCheck.more && dmsCheck.size() < CLIENT_KNOBS->TOO_MANY);
		if (!dmsCheck.empty()) {
			TraceEvent(SevWarnAlways, "DDInitCancellingShardEncodedMoves", distributorId)
			    .detail("Count", dmsCheck.size());
			tr.clear(dataMoveKeys);
			co_await tr.commit();
			tr.reset();
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			co_return true;
		}

		// Phase 2: Rewrite shard-encoded keyServers entries to old format.
		// Reads 1000 entries per iteration. Caller loops (co_return true triggers
		// re-entry) until no shard-encoded entries remain. Previously-rewritten
		// entries won't match hasShardEncodeLocationMetaData() on re-read.
		RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
		ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

		bool rewroteAny = false;
		RangeResult ksEntries = co_await tr.getRange(KeyRangeRef(keyServersPrefix, keyServersEnd), 1000);
		for (const auto& kv : ksEntries) {
			if (kv.value.empty())
				continue;
			BinaryReader rd(kv.value, IncludeVersion());
			if (rd.protocolVersion().hasShardEncodeLocationMetaData()) {
				std::vector<UID> src, dest;
				UID srcId, destId;
				decodeKeyServersValue(UIDtoTagMap, kv.value, src, dest, srcId, destId);
				Value oldValue = keyServersValue(UIDtoTagMap, src, dest);
				tr.set(kv.key, oldValue);
				rewroteAny = true;
			}
		}

		if (rewroteAny) {
			TraceEvent(SevInfo, "DDInitRewritingShardEncodedMetadata", distributorId)
			    .detail("KeyServersEntries", ksEntries.size());
			co_await tr.commit();
			tr.reset();
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			co_return true;
		}

		co_return false;
	}

	// Read keyservers, return unique set of teams
	static Future<Reference<InitialDataDistribution>> getInitialDataDistribution(Database cx,
	                                                                             UID distributorId,
	                                                                             MoveKeysLock moveKeysLock,
	                                                                             std::vector<Optional<Key>> remoteDcIds,
	                                                                             const DDEnabledState* ddEnabledState,
	                                                                             SkipDDModeCheck skipDDModeCheck) {
		auto result = makeReference<InitialDataDistribution>();
		Key beginKey = allKeys.begin;

		bool succeeded{ false };

		Transaction tr(cx);

		if (ddLargeTeamEnabled()) {
			result->userRangeConfig = co_await DDConfiguration().userRangeConfig().getSnapshot(
			    SystemDBWriteLockedNow(cx.getReference()), allKeys.begin, allKeys.end);
		}
		std::map<UID, Optional<Key>> server_dc;
		std::map<std::vector<UID>, std::pair<std::vector<UID>, std::vector<UID>>> team_cache;
		std::vector<std::pair<StorageServerInterface, ProcessClass>> tss_servers;
		int numDataMoves = 0;

		Optional<Key> healthyZone = co_await getHealthyZone(cx, distributorId);
		result->initHealthyZoneValue = healthyZone;

		CODE_PROBE(
		    (bool)skipDDModeCheck, "DD Mode won't prevent read initial data distribution.", probe::decoration::rare);
		// Get the server list in its own try/catch block since it modifies result.  We don't want a subsequent failure
		// causing entries to be duplicated
		// Phase 1: Single transaction to read server list and all persisted data moves
		double serverListAndDataMoveReadStart = now();
		while (true) {
			numDataMoves = 0;
			server_dc.clear();
			result->allServers.clear();
			result->dataMoveMap = KeyRangeMap<std::shared_ptr<DataMove>>(std::make_shared<DataMove>());
			result->auditStates.clear();
			tss_servers.clear();
			team_cache.clear();
			succeeded = false;
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				result->mode = 1;
				Optional<Value> mode = co_await tr.get(dataDistributionModeKey);
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					rd >> result->mode;
				}
				if ((!skipDDModeCheck && !result->mode) || !ddEnabledState->isEnabled()) {
					// DD can be disabled persistently (result->mode = 0) or transiently (isEnabled() = 0)
					TraceEvent(SevDebug, "GetInitialDataDistribution_DisabledDD").log();
					co_return result;
				}

				result->bulkLoadMode = 0;
				Optional<Value> bulkLoadMode = co_await tr.get(bulkLoadModeKey);
				if (bulkLoadMode.present()) {
					BinaryReader rd(bulkLoadMode.get(), Unversioned());
					rd >> result->bulkLoadMode;
				}
				TraceEvent(SevInfo, "DDBulkLoadEngineInitMode").detail("Mode", result->bulkLoadMode);

				result->bulkDumpMode = 0;
				Optional<Value> bulkDumpMode = co_await tr.get(bulkDumpModeKey);
				if (bulkDumpMode.present()) {
					BinaryReader rd(bulkDumpMode.get(), Unversioned());
					rd >> result->bulkDumpMode;
				}
				TraceEvent(SevInfo, "DDBulkDumpInitMode").detail("Mode", result->bulkDumpMode);

				Future<std::vector<ProcessData>> workers = getWorkers(&tr);
				Future<RangeResult> serverList = tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
				co_await (success(workers) && success(serverList));
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

				// If SHARD_ENCODE is off, rewrite any shard-encoded metadata to old format.
				if (!SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA) {
					if (co_await rewriteShardEncodedMetadata(tr, distributorId)) {
						continue; // Committed a rewrite — re-read from the top
					}
				}

				double dataMoveReadStart = now();
				RangeResult dms = co_await tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY);
				if (now() - dataMoveReadStart > 5.0) {
					TraceEvent(SevWarn, "DDInitSlowDataMoveRead", distributorId)
					    .detail("ElapsedSeconds", now() - dataMoveReadStart);
				}
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
					result->dataMoveMap.insert(meta.ranges.front(), dataMove);
					++numDataMoves;
				}

				succeeded = true;

				TraceEvent("DDInitServerListAndDataMoveReadComplete", distributorId)
				    .detail("NumDataMoves", numDataMoves)
				    .detail("NumServers", result->allServers.size())
				    .detail("ElapsedSeconds", now() - serverListAndDataMoveReadStart);

				break;
			} catch (Error& e) {
				err = e;
				TraceEvent("GetInitialTeamsRetry", distributorId).error(err);
			}
			co_await tr.onError(err);
			ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in this
			                    // loop
		}

		// If keyServers is too large to read in a single transaction, then we will have to break this process up into
		// multiple transactions. In that case, each iteration should begin where the previous left off
		// Scan keyServers in batches to build the shard map
		double keyServerScanStart = now();
		double lastScanLogTime = now();
		int scanBatchCount = 0;
		while (beginKey < allKeys.end) {
			CODE_PROBE(beginKey > allKeys.begin, "Multi-transactional getInitialDataDistribution");
			while (true) {
				succeeded = false;
				Error err;
				try {
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					co_await checkMoveKeysLockReadOnly(&tr, moveKeysLock, ddEnabledState);
					RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					RangeResult keyServers = co_await krmGetRanges(&tr,
					                                               keyServersPrefix,
					                                               KeyRangeRef(beginKey, allKeys.end),
					                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
					                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES);
					succeeded = true;

					std::vector<UID> src, dest, last;
					UID srcId, destId;

					// for each range
					for (int i = 0; i < keyServers.size() - 1; i++) {
						decodeKeyServersValue(UIDtoTagMap, keyServers[i].value, src, dest, srcId, destId);
						DDShardInfo info(keyServers[i].key, srcId, destId);
						if (!remoteDcIds.empty()) {
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
							if (!dest.empty()) {
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
							if (!dest.empty()) {
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
					scanBatchCount++;
					if (now() - lastScanLogTime >= 30.0) {
						lastScanLogTime = now();
						TraceEvent("DDInitKeyServerScanProgress", distributorId)
						    .detail("BeginKey", beginKey)
						    .detail("Batches", scanBatchCount)
						    .detail("ShardsScanned", result->shards.size())
						    .detail("ElapsedSeconds", now() - keyServerScanStart);
					}
					break;
				} catch (Error& e) {
					err = e;
					TraceEvent("GetInitialTeamsKeyServersRetry", distributorId).error(err);
				}
				co_await tr.onError(err);
				ASSERT(!succeeded); // We shouldn't be retrying if we have already started modifying result in
				                    // this loop
			}

			tr.reset();
		}

		// a dummy shard at the end with no keys or servers makes life easier for trackInitialShards()
		result->shards.push_back(DDShardInfo(allKeys.end));

		TraceEvent("DDInitKeyServerScanComplete", distributorId)
		    .detail("NumShards", result->shards.size())
		    .detail("ElapsedSeconds", now() - keyServerScanStart);

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

		co_return result;
	}

	static Future<Void> waitForDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
		Transaction tr(cx);
		while (true) {
			co_await delay(SERVER_KNOBS->DD_ENABLED_CHECK_DELAY, TaskPriority::DataDistribution);

			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			Error err;
			bool hasErr = false;
			try {
				Optional<Value> mode = co_await tr.get(dataDistributionModeKey);
				if (!mode.present() && ddEnabledState->isEnabled()) {
					TraceEvent("WaitForDDEnabledSucceeded").log();
					co_return;
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
						co_return;
					}
				}

				tr.reset();
			} catch (Error& e) {
				err = e;
				hasErr = true;
			}
			if (hasErr) {
				co_await tr.onError(err);
			}
		}
	}

	static Future<bool> isDataDistributionEnabled(Database cx, const DDEnabledState* ddEnabledState) {
		Transaction tr(cx);
		while (true) {
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

			Error err;
			try {
				Optional<Value> mode = co_await tr.get(dataDistributionModeKey);
				if (!mode.present() && ddEnabledState->isEnabled())
					co_return true;
				if (mode.present()) {
					BinaryReader rd(mode.get(), Unversioned());
					int m;
					rd >> m;
					if (m && ddEnabledState->isEnabled()) {
						TraceEvent(SevDebug, "IsDDEnabledSucceeded")
						    .detail("Mode", m)
						    .detail("IsDDEnabled", ddEnabledState->isEnabled());
						co_return true;
					}
				}
				// SOMEDAY: Write a wrapper in MoveKeys.actor.h
				Optional<Value> readVal = co_await tr.get(moveKeysLockOwnerKey);
				UID currentOwner =
				    readVal.present() ? BinaryReader::fromStringRef<UID>(readVal.get(), Unversioned()) : UID();
				if (ddEnabledState->isEnabled() && (currentOwner != dataDistributionModeLock)) {
					TraceEvent(SevDebug, "IsDDEnabledSucceeded")
					    .detail("CurrentOwner", currentOwner)
					    .detail("DDModeLock", dataDistributionModeLock)
					    .detail("IsDDEnabled", ddEnabledState->isEnabled());
					co_return true;
				}
				TraceEvent(SevDebug, "IsDDEnabledFailed")
				    .detail("CurrentOwner", currentOwner)
				    .detail("DDModeLock", dataDistributionModeLock)
				    .detail("IsDDEnabled", ddEnabledState->isEnabled());
				co_return false;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> pollMoveKeysLock(Database cx, MoveKeysLock lock, const DDEnabledState* ddEnabledState) {
		while (true) {
			co_await delay(SERVER_KNOBS->MOVEKEYS_LOCK_POLLING_DELAY);
			Transaction tr(cx);
			while (true) {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Error err;
				try {
					co_await checkMoveKeysLockReadOnly(&tr, lock, ddEnabledState);
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}
	}

	static Future<Optional<Value>> readRebalanceDDIgnoreKey(Database cx) {
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

				Optional<Value> res = co_await tr.get(rebalanceDDIgnoreKey);
				co_return res;
			} catch (Error& e) {
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> waitDDTeamInfoPrintSignal(Database cx) {
		static auto* counters = makeCounters("/dd/waitDDTeamInfoPrintSignal");
		ReadYourWritesTransaction tr(cx);
		while (true) {
			counters->started->increment(1);
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Future<Void> watchFuture = tr.watch(triggerDDTeamInfoPrintKey);
				co_await tr.commit();
				counters->committed->increment(1);
				co_await watchFuture;
				co_return;
			} catch (Error& e) {
				counters->aborted->increment(1);
				err = e;
			}
			co_await tr.onError(err);
		}
	}

	static Future<Void> waitForAllDataRemoved(Database cx,
	                                          UID serverID,
	                                          Version addedVersion,
	                                          Reference<ShardsAffectedByTeamFailure> shardsAffectedByTeamFailure) {
		auto tr = makeReference<ReadYourWritesTransaction>(cx);
		while (true) {
			Error err;
			bool hasErr = false;
			try {
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				Version ver = co_await tr->getReadVersion();

				// we cannot remove a server immediately after adding it, because a perfectly timed cluster recovery
				// could cause us to not store the mutations sent to the short lived storage server.
				if (ver > addedVersion + SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS) {
					bool canRemove = co_await canRemoveStorageServer(tr, serverID);
					auto shards = shardsAffectedByTeamFailure->getNumberOfShards(serverID);
					TraceEvent(SevVerbose, "WaitForAllDataRemoved")
					    .detail("Server", serverID)
					    .detail("CanRemove", canRemove)
					    .detail("Shards", shards);
					ASSERT_GE(shards, 0);
					if (canRemove && shards == 0) {
						co_return;
					}
				}

				// Wait for any change to the serverKeys for this server
				co_await delay(SERVER_KNOBS->ALL_DATA_REMOVED_DELAY, TaskPriority::DataDistribution);
				tr->reset();
			} catch (Error& e) {
				err = e;
				hasErr = true;
			}
			if (hasErr) {
				co_await tr->onError(err);
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

Future<Optional<HealthMetrics::StorageStats>> DDTxnProcessor::getStorageStats(const UID& id,
                                                                              double maxStaleness) const {
	return cx->getStorageStats(id, maxStaleness);
}
