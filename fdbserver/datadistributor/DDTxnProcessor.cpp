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
#include "fdbclient/RunRYWTransaction.h"
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

	// Rollback rewrite invoked at DD init when the migration target is
	// old format: converts shard-encoded metadata back to old format so
	// `audit_storage metadata_encoding` can reach ROLLBACK COMPLETE.
	// See design/shard-encode-location-metadata.md for the full design.
	//
	// Three phases: (1) clear DataMoveMetaData, (2) rewrite keyServers
	// (paginated, re-entrant), (3) rewrite each SS's serverKeys KRM. A
	// completion sentinel lets later DD inits fast-path skip.
	//
	// Two non-obvious properties:
	//  - The Phase 3 serverKeys rewrite is required for convergence;
	//    without it, new-format serverKeys only drain opportunistically
	//    as DD moves shards, so ROLLBACK COMPLETE may never be reached.
	//  - Phase 3 is NOT re-entrant: it drains all SSes in one DD-init
	//    invocation (up to ~3.5h serial on a 250k-shard cluster) before
	//    setting the sentinel. Rare, opt-in, runs per rollback event
	//    (configure shard_metadata_migration=enabled).
	// Phase 1 of rewriteShardEncodedMetadata: clear all persisted
	// DataMoveMetaData entries. New-format dataMoves are meaningless to
	// old-format DD; on rollback DD needs a clean slate to plan moves.
	// Any clear commits on the caller's tr (batched with the sentinel-
	// clear that the top-level function queued on the same tr). Returns
	// true if any were cleared — caller re-enters until this returns
	// false.
	static Future<bool> clearShardEncodedDataMoves(Transaction& tr, UID distributorId) {
		RangeResult dmsCheck = co_await tr.getRange(dataMoveKeys, CLIENT_KNOBS->TOO_MANY);
		ASSERT(!dmsCheck.more && dmsCheck.size() < CLIENT_KNOBS->TOO_MANY);
		if (dmsCheck.empty()) {
			co_return false;
		}
		TraceEvent(SevWarnAlways, "DDInitCancellingShardEncodedMoves", distributorId).detail("Count", dmsCheck.size());
		tr.clear(dataMoveKeys);
		co_await tr.commit();
		tr.reset();
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		co_return true;
	}

	// Phase 2: rewrite one batch (SHARD_ENCODE_REWRITE_KS_BATCH_SIZE
	// entries) of shard-encoded keyServers entries to old format. Caller
	// owns `beginKey` and re-enters until this returns false, so the
	// cursor persists across calls to cover the whole prefix.
	static Future<bool> rewriteShardEncodedKeyServers(Transaction& tr, UID distributorId, Key& beginKey) {
		RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
		ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);

		bool rewroteAny = false;
		RangeResult ksEntries =
		    co_await tr.getRange(KeyRangeRef(beginKey, keyServersEnd), SERVER_KNOBS->SHARD_ENCODE_REWRITE_KS_BATCH_SIZE);
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
		// Compute the next cursor, but DO NOT publish it to the caller-owned
		// `beginKey` until AFTER a successful commit below. This tr carries
		// the caller's DD-init read set (serverList/workers/mode keys), so
		// its commit can throw not_committed on a conflict — common under
		// churn (e.g. Attrition changing the server list). If we advanced
		// beginKey before committing, the caller's retry would resume from
		// the already-advanced cursor and permanently skip this batch's
		// unconverted new-format entries. That is the root cause of the
		// KeyServersNew residual / early-seal (seeds 2611177188, 1561219216):
		// only the FIRST batch leaked, because after tr.reset() below later
		// batches carry a small read set and rarely conflict.
		const bool atEnd = !ksEntries.more;
		Key nextBegin = ksEntries.empty() ? beginKey : keyAfter(ksEntries.back().key);
		if (atEnd) {
			nextBegin = keyServersPrefix; // defensive: caller won't re-enter after false
		}
		if (!rewroteAny) {
			// Only safe to declare Phase 2 done once the whole prefix is
			// walked. If more pages remain we must re-enter — returning
			// false here would reach ROLLBACK COMPLETE while unrewritten
			// entries remain further along the prefix.
			if (atEnd) {
				co_return false; // done; leave tr for the caller's sentinel-clear commit
			}
			beginKey = nextBegin; // read-only page (no commit to fail): safe to advance
			tr.reset(); // no writes this page; drop read set before re-entering
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			co_return true;
		}
		TraceEvent(SevInfo, "DDInitRewritingShardEncodedMetadata", distributorId)
		    .detail("KeyServersEntries", ksEntries.size())
		    .detail("More", ksEntries.more);
		co_await tr.commit();
		beginKey = nextBegin; // publish the cursor ONLY after the durable commit
		tr.reset();
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		co_return true;
	}

	// Phase 3 per-SS: rewrite one SS's serverKeys KRM from new-format
	// entries to old-format constants. Uses ONE FRESH TRANSACTION PER
	// krmSetRangeCoalescing CALL — same pattern natural DD moves use in
	// MoveKeys.cpp:99. See rewriteShardEncodedMetadata's Phase 3 comment
	// for the correctness argument. Returns the number of ranges
	// rewritten across the entire SS (paginates internally so callers
	// don't need to re-invoke to finish one SS).
	static Future<int64_t> rewriteOneServerKeysKRM(Database cx, UID ssId, UID distributorId, bool& fullyDrained) {
		Key mapPrefix = serverKeysPrefixFor(ssId);
		int64_t rewritesForThisSS = 0;
		double ssStart = now();
		int scanIdx = 0;
		// True unless we bail below with possible residue (no-progress or
		// scan-limit break). The caller must NOT seal the completion
		// sentinel if any SS returns fullyDrained=false, else it would
		// report ROLLBACK COMPLETE while new-format serverKeys remain.
		fullyDrained = true;

		// Repeat full KRM scans until a complete scan finds no new-format
		// spans. A single forward paginated pass can leave residue (a
		// no-progress break under a small KRM_GET_RANGE_LIMIT, a partial
		// page, or coalescing shifting boundaries mid-pass), so we must not
		// return "drained" until a whole scan rewrote nothing.
		loop {
			scanIdx++;
			int64_t rewritesThisScan = 0;
			bool noProgress = false;
			Key beginKey = allKeys.begin;
			int pageIdx = 0;

			while (beginKey < allKeys.end) {
				// Read one page of this SS's KRM in its own fresh snapshot
				// transaction. KRM_GET_RANGE_LIMIT bounds page size; under
				// buggify this can be as small as 10 entries.
				RangeResult ranges;
				bool morePages = false;
				{
					Transaction readTr(cx);
					readTr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					readTr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
					readTr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

					// Guard: krmGetRanges hangs on an empty KRM prefix
					// (freshly registered SS with no assignments yet).
					if (beginKey == allKeys.begin) {
						RangeResult probe = co_await readTr.getRange(KeyRangeRef(mapPrefix, strinc(mapPrefix)), 1);
						if (probe.empty()) {
							co_return rewritesForThisSS;
						}
					}

					ranges = co_await krmGetRanges(&readTr,
					                               mapPrefix,
					                               KeyRangeRef(beginKey, allKeys.end),
					                               CLIENT_KNOBS->KRM_GET_RANGE_LIMIT,
					                               CLIENT_KNOBS->KRM_GET_RANGE_LIMIT_BYTES);
					morePages = ranges.more;
				}

				if (scanIdx == 1 && pageIdx == 0) {
					TraceEvent(SevInfo, "DDShardEncodeRollbackPhase3SSStart", distributorId)
					    .detail("SS", ssId)
					    .detail("FirstPageEntries", ranges.size());
				}

				for (int i = 0; i + 1 < ranges.size(); i++) {
					const ValueRef& v = ranges[i].value;
					if (isServerKeysUnassigned(v) || isServerKeysOldFormatAssigned(v)) {
						continue;
					}
					bool assigned = false;
					bool emptyRange = false;
					DataMoveType dataMoveType = DataMoveType::LOGICAL;
					DataMovementReason dataMoveReason = DataMovementReason::INVALID;
					UID id;
					try {
						decodeServerKeysValue(v, assigned, emptyRange, dataMoveType, id, dataMoveReason);
					} catch (Error& e) {
						// Malformed serverKeys value from a partial upgrade
						// or corrupted state — skip this span, don't abort
						// DD init. The audit tool will report the residual
						// entry via its own scan.
						TraceEvent(SevWarnAlways, "DDShardEncodeRollbackSkipUndecodable", distributorId)
						    .detail("SS", ssId)
						    .detail("Key", ranges[i].key)
						    .detail("Error", e.code());
						continue;
					}
					Value oldValue = !assigned ? serverKeysFalse : emptyRange ? serverKeysTrueEmptyRange : serverKeysTrue;
					KeyRange span = KeyRangeRef(ranges[i].key, ranges[i + 1].key);

					// One transaction per span. Retry on transient errors
					// via the standard idiom.
					loop {
						Transaction spanTr(cx);
						spanTr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
						spanTr.setOption(FDBTransactionOptions::LOCK_AWARE);
						Error err;
						try {
							co_await krmSetRangeCoalescing(&spanTr, mapPrefix, span, allKeys, oldValue);
							co_await spanTr.commit();
							break;
						} catch (Error& e) {
							err = e;
						}
						co_await spanTr.onError(err);
					}
					rewritesThisScan++;
				}

				if (!morePages) {
					break;
				}
				if (ranges.size() == 0) {
					// morePages with size==0 shouldn't happen; guard against infinite loop.
					break;
				}
				Key nextBegin = ranges.back().key;
				// Monotonic-progress guard: if the KRM returned a page
				// that doesn't advance beginKey (degenerate KRM under
				// BUGGIFY KRM_GET_RANGE_LIMIT=10), stop and mark this SS
				// not fully drained (fullyDrained=false below) so the
				// caller leaves the sentinel unset and a later DD init
				// retries rather than falsely reporting ROLLBACK COMPLETE.
				if (!(beginKey < nextBegin)) {
					TraceEvent(SevWarnAlways, "DDShardEncodeRollbackPhase3NoProgress", distributorId)
					    .detail("SS", ssId)
					    .detail("BeginKey", beginKey)
					    .detail("NextBegin", nextBegin)
					    .detail("PageEntries", ranges.size());
					noProgress = true;
					break;
				}
				beginKey = nextBegin;
				pageIdx++;
			}

			rewritesForThisSS += rewritesThisScan;
			if (rewritesThisScan == 0) {
				break; // a complete scan found nothing new-format → SS drained
			}
			if (noProgress) {
				fullyDrained = false; // bailed with possible residue; caller must not seal
				break;
			}
			if (scanIdx >= 8) {
				fullyDrained = false; // bailed with possible residue; caller must not seal
				TraceEvent(SevWarnAlways, "DDShardEncodeRollbackPhase3SSScanLimit", distributorId)
				    .detail("SS", ssId)
				    .detail("Rewrites", rewritesForThisSS);
				break;
			}
		}

		TraceEvent(SevInfo, "DDShardEncodeRollbackPhase3SSDone", distributorId)
		    .detail("SS", ssId)
		    .detail("Rewrites", rewritesForThisSS)
		    .detail("Scans", scanIdx)
		    .detail("ElapsedSec", now() - ssStart);
		co_return rewritesForThisSS;
	}

	// Set the shard-encode migration sentinel to `value` in its own
	// transaction, retrying on transient errors. Load-bearing commit —
	// audit tools and future DD inits read this key as the authoritative
	// migration-complete signal.
	static Future<Void> commitShardEncodeMigrationSentinel(Database cx, Value value) {
		co_await runRYWTransactionVoid(cx, [value](Reference<ReadYourWritesTransaction> tr) -> Future<Void> {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
			tr->setOption(FDBTransactionOptions::LOCK_AWARE);
			tr->set(shardEncodeMigrationCompleteKey, value);
			return Void();
		});
	}

	static Future<bool> rewriteShardEncodedMetadata(Transaction& tr, UID distributorId, Key& phase2Cursor) {
		Database cx = tr.getDatabase();
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);

		// Fast-path: sentinel says already rolled back → skip everything.
		// Cost per DD init when the cluster has been at knob=false for a
		// while is a single key read.
		Optional<Value> marker = co_await tr.get(shardEncodeMigrationCompleteKey);
		if (marker.present() && marker.get() == shardEncodeMigrationValueOld) {
			TraceEvent(SevInfo, "DDShardEncodeRewriteSkipped", distributorId)
			    .detail("Direction", "rollback")
			    .detail("Reason", "SentinelSaysOld");
			co_return false;
		}
		TraceEvent(SevInfo, "DDShardEncodeRewriteBegin", distributorId)
		    .detail("Direction", "rollback")
		    .detail("KnobValue", SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA)
		    .detail("PreviousSentinel", marker.present() ? marker.get() : "absent"_sr);

		// Clear the sentinel so any observer during the rewrite (audit
		// tool) sees "in progress" rather than a stale "complete"
		// marker. This clear is committed alongside whichever phase's
		// first commit fires (or as a standalone commit before Phase 3
		// if 1 & 2 are no-ops).
		tr.clear(shardEncodeMigrationCompleteKey);

		if (co_await clearShardEncodedDataMoves(tr, distributorId)) {
			co_return true; // committed; caller re-enters
		}

		if (co_await rewriteShardEncodedKeyServers(tr, distributorId, phase2Cursor)) {
			co_return true; // committed; caller re-enters
		}

		// No Phase 1 or Phase 2 work. Commit the sentinel-clear, then
		// Phase 3 opens per-span transactions. Re-read serverList until
		// stable to close the register-during-migration race — see
		// comment before the do/while loop below.
		co_await tr.commit();
		tr.reset();
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

		// Phase 3: for each SS, rewrite its serverKeys KRM. Uses ONE
		// FRESH TRANSACTION PER krmSetRangeCoalescing CALL — the same
		// pattern natural DD moves use in MoveKeys.cpp:99. Correctness
		// argument: krmSetRangeCoalescing uses Snapshot::True reads to
		// compute coalescing boundaries; NativeAPI Transaction does not
		// surface prior same-tx writes to those reads, so batching
		// calls in one tx produces fragmentation (see
		// KeyRangeMap.cpp:302 assertion; observed in v4/v5/v6-fix
		// pre-sentinel iterations). One-tx-per-span sidesteps the RYW
		// dependency entirely.
		//
		// Loop until the serverList is stable across a full Phase 3
		// pass. Closes the race where an SS registers between the
		// snapshot and the sentinel commit — its serverKeys entries
		// might otherwise be missed while sentinel="old" is set. In the
		// steady case this runs once (empty second pass).
		int64_t totalPhase3Rewrites = 0;
		double phase3Start = now();
		std::set<UID> processed;
		int passes = 0;
		bool ssListStabilized = false;
		// Cleared if any per-SS drain bails with possible residue (no-progress
		// / scan-limit). We only seal the sentinel when every processed SS
		// fully drained.
		bool allDrained = true;
		while (true) {
			passes++;
			Transaction slTr(cx);
			slTr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			slTr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
			slTr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			RangeResult serverList = co_await slTr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY);
			ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
			int64_t passRewrites = 0;
			int newSSCount = 0;
			for (const auto& serverKv : serverList) {
				UID ssId = decodeServerListValue(serverKv.value).id();
				if (!processed.insert(ssId).second) {
					continue; // Already processed in a prior pass
				}
				newSSCount++;
				bool ssDrained = true;
				passRewrites += co_await rewriteOneServerKeysKRM(cx, ssId, distributorId, ssDrained);
				if (!ssDrained) {
					allDrained = false;
				}
			}
			totalPhase3Rewrites += passRewrites;
			if (newSSCount == 0) {
				// No SSes appeared since the last pass — safe to seal.
				ssListStabilized = true;
				break;
			}
			TraceEvent(SevInfo, "DDShardEncodeRollbackPhase3Repass", distributorId)
			    .detail("Pass", passes)
			    .detail("NewSSes", newSSCount)
			    .detail("PassRewrites", passRewrites);
			// Bounded safety net: if an operator is registering SSes
			// continuously, break out after a reasonable number of
			// passes and let the next DD init retry.
			if (passes >= 8) {
				TraceEvent(SevWarnAlways, "DDShardEncodeRollbackPhase3PassLimit", distributorId)
				    .detail("Passes", passes)
				    .detail("Reason", "SSListNotStabilizing");
				break;
			}
		}

		if (!ssListStabilized || !allDrained) {
			// Either an SS registered after our last pass (list not stable),
			// or a per-SS drain bailed with possible residue (no-progress /
			// scan-limit). Leave the sentinel unset so a later DD init
			// re-scans — sealing "old" here would falsely report "safe to
			// downgrade". Return false (not true) so we don't immediately
			// re-enter and spin on the same churn.
			TraceEvent(SevWarnAlways, "DDShardEncodeRollbackPhase3Incomplete", distributorId)
			    .detail("Phase3Rewrites", totalPhase3Rewrites)
			    .detail("Phase3Passes", passes)
			    .detail("SSListStabilized", ssListStabilized)
			    .detail("AllDrained", allDrained)
			    .detail("Phase3ElapsedSec", now() - phase3Start);
			co_return false;
		}

		// All phases drained. Set sentinel = "old" so subsequent DD
		// inits fast-path skip.
		co_await commitShardEncodeMigrationSentinel(cx, shardEncodeMigrationValueOld);
		TraceEvent(SevInfo, "DDShardEncodeRewriteComplete", distributorId)
		    .detail("Direction", "rollback")
		    .detail("Phase3Rewrites", totalPhase3Rewrites)
		    .detail("Phase3Passes", passes)
		    .detail("Phase3ElapsedSec", now() - phase3Start);

		// Return true if we did any Phase 3 work (caller restart is
		// cheap — sentinel fast-path skip on re-entry).
		co_return totalPhase3Rewrites > 0;
	}

	// When the effective encoding target is "encoded" (new format) on DD
	// init -- i.e. shard_metadata_format=encoded, or the
	// SHARD_ENCODE_LOCATION_METADATA knob when the config is UNSET -- clear
	// any stale "sentinel=='old'" that a prior rollback
	// (rewriteShardEncodedMetadata) committed. This runs unconditionally on
	// the forward path (independent of shard_metadata_migration): without
	// it, natural DD moves during the forward window would write new-format
	// entries while the sentinel still claims "old"; a subsequent rollback
	// (target back to original) would see sentinel=="old" in
	// rewriteShardEncodedMetadata's fast-path, skip the rewrite, and leave
	// those new-format entries in place -- old-format DD would then trip on
	// them.
	//
	// We do NOT actively rewrite old-format entries to new format on
	// re-forward: new-format code paths (decodeServerKeysValue,
	// decodeKeyServersValue) explicitly accept old-format inputs, and
	// natural DD moves gradually rewrite as ranges move. Trade: audit
	// tool's FORWARD COMPLETE terminal state converges over the natural-
	// move timescale rather than in a bounded post-flip window.
	//
	// Idempotent -- safe on a fresh cluster (sentinel absent) and on
	// repeat DD inits. Steady-state cost when the target is encoded: one
	// system-key read per DD init, zero commits after the initial clear.
	static Future<bool> clearStaleShardEncodedRewriteSentinel(Transaction& tr, UID distributorId) {
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);

		Optional<Value> marker = co_await tr.get(shardEncodeMigrationCompleteKey);
		if (!marker.present()) {
			co_return false;
		}
		tr.clear(shardEncodeMigrationCompleteKey);
		co_await tr.commit();
		TraceEvent(SevInfo, "DDShardEncodeSentinelCleared", distributorId).detail("PreviousValue", marker.get());
		tr.reset();
		tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
		tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
		co_return true;
	}

	// Read keyservers, return unique set of teams
	static Future<Reference<InitialDataDistribution>> getInitialDataDistribution(Database cx,
	                                                                             UID distributorId,
	                                                                             MoveKeysLock moveKeysLock,
	                                                                             std::vector<Optional<Key>> remoteDcIds,
	                                                                             const DDEnabledState* ddEnabledState,
	                                                                             SkipDDModeCheck skipDDModeCheck,
	                                                                             DatabaseConfiguration dbConfig) {
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

		// Reuse DD's already-loaded DatabaseConfiguration (passed in from
		// loadDatabaseConfiguration) rather than a per-init read here.
		// The rewrite is gated on shard_metadata_migration below; with the
		// default (UNSET) config no rewrite runs. NOTE this is an
		// intentional change from the legacy behavior where the
		// SHARD_ENCODE_LOCATION_METADATA knob being false alone triggered
		// the rollback rewrite — the knob now only selects the target
		// format as a fallback when shard_metadata_format is UNSET.
		bool migrationEnabled =
		    dbConfig.shardMetadataMigration == DatabaseConfiguration::ShardMetadataMigration::ENABLED;
		bool targetIsNewFormat;
		if (dbConfig.shardMetadataFormat != DatabaseConfiguration::ShardMetadataFormat::UNSET) {
			targetIsNewFormat =
			    (dbConfig.shardMetadataFormat == DatabaseConfiguration::ShardMetadataFormat::ENCODED);
		} else {
			targetIsNewFormat = SERVER_KNOBS->SHARD_ENCODE_LOCATION_METADATA;
		}
		// Phase 2 keyServers cursor — persists across re-entries so the
		// rewrite paginates the whole prefix instead of rescanning from
		// the head each time.
		Key phase2Cursor = keyServersPrefix;

		CODE_PROBE((bool)skipDDModeCheck, "DD Mode won't prevent read initial data distribution.");
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

				// Rewrite metadata that doesn't match the current effective
				// format (rollback direction — actively drains new-format
				// entries to old), or invalidate any stale rollback-complete
				// sentinel on re-forward so a subsequent rollback doesn't
				// fast-path skip incorrectly. On re-forward we rely on
				// natural DD moves to eventually rewrite entries in new
				// format; new-format code paths accept old-format inputs,
				// so mixed state is safe.
				//
				// The rollback (active rewrite) branch is GATED on the two
				// DatabaseConfiguration options (default UNSET), resolved once
				// above the retry loop. The forward branch's stale-sentinel
				// clear runs UNCONDITIONALLY (independent of
				// shard_metadata_migration). Direction is implicit -- DD
				// converges toward the format specified by
				// shard_metadata_format.
				//
				// Coexistence with the legacy SHARD_ENCODE_LOCATION_METADATA
				// knob: if shard_metadata_format is UNSET, DD falls back to
				// the knob value (true -> encoded, false -> original)
				// as the target. If shard_metadata_migration is UNSET, no
				// migration runs — the knob being false no longer triggers
				// a rewrite on its own (an intentional change from the
				// legacy knob-only trigger).
				if (!targetIsNewFormat) {
					// Rollback direction: the active rewrite is opt-in via
					// shard_metadata_migration.
					if (migrationEnabled) {
						if (co_await rewriteShardEncodedMetadata(tr, distributorId, phase2Cursor)) {
							continue; // Committed a rewrite — re-read from the top
						}
					}
				} else {
					// Forward direction: ALWAYS clear a stale rollback-complete
					// sentinel, independent of shard_metadata_migration. The
					// forward direction has no active rewrite, but if a prior
					// rollback left sentinel=="old" and the operator re-forwards
					// with migration disabled, the sentinel must still be cleared
					// — otherwise a later rollback reads the stale "old" via
					// rewriteShardEncodedMetadata's fast-path and skips the
					// rewrite, so the new-format entries written while forward
					// never converge. clearStaleShardEncodedRewriteSentinel is a
					// single-key read (a no-op when the sentinel is absent, i.e.
					// on clusters that never rolled back), so this stays a no-op
					// for never-migrated clusters.
					if (co_await clearStaleShardEncodedRewriteSentinel(tr, distributorId)) {
						continue; // Committed a clear — re-read from the top
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

		if (targetIsNewFormat && numDataMoves > 0) {
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
    SkipDDModeCheck skipDDModeCheck,
    const DatabaseConfiguration& configuration) {
	return DDTxnProcessorImpl::getInitialDataDistribution(
	    cx, distributorId, moveKeysLock, remoteDcIds, ddEnabledState, skipDDModeCheck, configuration);
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
