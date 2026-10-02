/*
 * ConsistencyCheckUrgent.cpp
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

#include <math.h>
#include "boost/lexical_cast.hpp"

#include "flow/IRandom.h"
#include "flow/ProcessEvents.h"
#include "fdbclient/NativeAPI.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.h"
#include "flow/IRateControl.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/core/Knobs.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/ManagementAPI.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbserver/consistencyscan/ConsistencyScan.h"
#include "flow/network.h"
#include "fdbrpc/SimulatorProcessInfo.h"

// The ConsistencyCheckUrgent workload is designed to support the consistency check
// urgent feature, a distributed version of the consistency check which emphasizes
// the completeness of checking and the distributed fashion.
// The ConsistencyCheckUrgent workload emphasizes the completeness of data consistency check ---
// if any shard is failed to check, this information will be propagated to the user.
// To support to the distributed fashion, the ConsistencyCheckUrgent workload takes
// rangesToCheck and consistencyCheckerId as the input and check the data consistency of
// the input ranges.
// On the other hand, the ConsistencyCheck workload is used for a single-threaded consistency
// check feature and includes many checks other than the data consistency check, such as
// shard size estimation evaluation. However, the ConsistencyCheck workload cannot guarantee
// the complete check and users cannot specify input range to check. Therefore, the
// ConsistencyCheck workload cannot meet the requirement of the consistency check urgent feature.
struct ConsistencyCheckUrgentWorkload : TestWorkload {
	static constexpr auto NAME = "ConsistencyCheckUrgent";

	int64_t consistencyCheckerId;

	explicit ConsistencyCheckUrgentWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		consistencyCheckerId = sharedRandomNumber;
	}

	Future<std::vector<std::pair<KeyRange, Value>>> getKeyLocationsForRangeList(Database cx,
	                                                                            std::vector<KeyRange> ranges,
	                                                                            ConsistencyCheckUrgentWorkload* self) {
		// Get the scope of the input list of ranges
		Key beginKeyToReadKeyServer;
		Key endKeyToReadKeyServer;
		for (int i = 0; i < ranges.size(); i++) {
			if (i == 0 || ranges[i].begin < beginKeyToReadKeyServer) {
				beginKeyToReadKeyServer = ranges[i].begin;
			}
			if (i == 0 || ranges[i].end > endKeyToReadKeyServer) {
				endKeyToReadKeyServer = ranges[i].end;
			}
		}
		TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterGetKeyLocationsForRangeList")
		    .detail("RangeBegin", beginKeyToReadKeyServer)
		    .detail("RangeEnd", endKeyToReadKeyServer)
		    .detail("ClientCount", self->clientCount)
		    .detail("ClientId", self->clientId);
		// Read KeyServer space within the scope and add shards intersecting with the input ranges
		std::vector<std::pair<KeyRange, Value>> res;
		Transaction tr(cx);
		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				KeyRange rangeToRead = Standalone(KeyRangeRef(beginKeyToReadKeyServer, endKeyToReadKeyServer));
				RangeResult readResult = co_await krmGetRanges(&tr,
				                                               keyServersPrefix,
				                                               rangeToRead,
				                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
				                                               SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES);
				for (int i = 0; i < readResult.size() - 1; ++i) {
					KeyRange rangeToCheck = Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key));
					Value valueToCheck = Standalone(readResult[i].value);
					bool toAdd = false;
					for (const auto& range : ranges) {
						if (rangeToCheck.intersects(range)) {
							toAdd = true;
							break;
						}
					}
					if (toAdd) {
						res.push_back(std::make_pair(rangeToCheck, valueToCheck));
					}
					beginKeyToReadKeyServer = readResult[i + 1].key;
				}
				if (beginKeyToReadKeyServer >= endKeyToReadKeyServer) {
					break;
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterRetryGetKeyLocationsForRangeList")
				    .error(err)
				    .detail("ClientCount", self->clientCount)
				    .detail("ClientId", self->clientId);
				co_await tr.onError(err);
			}
		}
		co_return res;
	}

	Future<Void> checkDataConsistencyUrgent(Database cx,
	                                        std::vector<KeyRange> rangesToCheck,
	                                        ConsistencyCheckUrgentWorkload* self,
	                                        int consistencyCheckEpoch) {
		// Get shard locations of the input rangesToCheck
		std::vector<std::pair<KeyRange, Value>> shardLocationPairList;
		int retryCount = 0;
		while (true) {
			Error err;
			try {
				shardLocationPairList.clear();
				shardLocationPairList = co_await self->getKeyLocationsForRangeList(cx, rangesToCheck, self);
				break;
			} catch (Error& e) {
				err = e;
			}
			if (err.code() == error_code_actor_cancelled) {
				throw err;
			}
			TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterGetKeyLocationListFailed")
			    .error(err)
			    .detail("RetryCount", retryCount)
			    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
			    .detail("ClientId", self->clientId)
			    .detail("ClientCount", self->clientCount)
			    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
			co_await delay(5.0);
			retryCount++;
			if (retryCount > 50) {
				throw timed_out();
			}
		}

		TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterStartTask")
		    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
		    .detail("RangeToCheck", rangesToCheck.size())
		    .detail("ShardToCheck", shardLocationPairList.size())
		    .detail("ClientCount", self->clientCount)
		    .detail("ClientId", self->clientId)
		    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);

		// Do consistency check shard by shard
		Reference<IRateControl> rateLimiter =
		    makeReference<SpeedLimit>(CLIENT_KNOBS->CONSISTENCY_CHECK_RATE_LIMIT_MAX, 1);
		KeyRangeMap<bool> failedRanges; // Used to collect failed ranges in the current checkDataConsistency
		failedRanges.insert(allKeys, false); // Initialized with false and will set any failed range as true later
		// Which will be used to start the next consistencyCheckEpoch of the checkDataConsistency
		int64_t numShardThisClient = shardLocationPairList.size();
		int64_t numShardToCheck = -1;
		int64_t numCompleteShards = 0;
		int64_t numFailedShards = 0;
		for (int shardIdx = 0; shardIdx < shardLocationPairList.size(); ++shardIdx) {
			numShardToCheck = numShardThisClient - shardIdx;
			KeyRangeRef range = shardLocationPairList[shardIdx].first;
			// Step 1: Get source server id of the shard
			std::vector<UID> sourceStorageServers;
			std::vector<UID> destStorageServers;
			std::unordered_map<UID, Tag> storageServerToTagMap; // populated only when version vector is enabled
			retryCount = 0;
			while (true) {
				Error err;
				try {
					Transaction tr(cx);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					RangeResult UIDtoTagMap = co_await tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY);
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					decodeKeyServersValue(UIDtoTagMap,
					                      shardLocationPairList[shardIdx].second,
					                      sourceStorageServers,
					                      destStorageServers,
					                      false);
					if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
						storageServerToTagMap.reserve(UIDtoTagMap.size());
						for (auto& it : UIDtoTagMap) {
							storageServerToTagMap[decodeServerTagKey(it.key)] = decodeServerTagValue(it.value);
						}
					}
					break;
				} catch (Error& e) {
					err = e;
				}
				if (err.code() == error_code_actor_cancelled) {
					throw err;
				}
				TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterGetUIDtoTagMapFailed")
				    .error(err)
				    .detail("RetryCount", retryCount)
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount)
				    .detail("ShardBegin", range.begin)
				    .detail("ShardEnd", range.end)
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
				co_await delay(5.0);
				retryCount++;
				if (retryCount > 50) {
					throw timed_out();
				}
			}

			if (sourceStorageServers.empty()) {
				TraceEvent(SevWarnAlways, "ConsistencyCheckUrgent_TesterEmptySourceServers")
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount)
				    .detail("ShardBegin", range.begin)
				    .detail("ShardEnd", range.end)
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
				numCompleteShards++;
				continue; // Skip to the next shard
			} else if (sourceStorageServers.size() == 1) {
				TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterSingleReplica")
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount)
				    .detail("ShardBegin", range.begin)
				    .detail("ShardEnd", range.end)
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
				numCompleteShards++;
				continue; // Skip to the next shard
			}

			// Step 2: Get server interfaces
			std::vector<UID> storageServers = sourceStorageServers; // We check source server
			std::vector<StorageServerInterface> storageServerInterfaces;
			retryCount = 0;
			while (true) {
				Error err;
				try {
					Transaction tr(cx);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					std::vector<Future<Optional<Value>>> serverListEntries;
					serverListEntries.reserve(storageServers.size());
					for (int s = 0; s < storageServers.size(); s++)
						serverListEntries.push_back(tr.get(serverListKeyFor(storageServers[s])));
					std::vector<Optional<Value>> serverListValues = co_await getAll(serverListEntries);
					for (int s = 0; s < serverListValues.size(); s++) {
						if (!serverListValues[s].present()) {
							TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterGetServerInterfaceFailed")
							    .detail("SSID", storageServers[s])
							    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
							    .detail("ClientId", self->clientId)
							    .detail("ClientCount", self->clientCount)
							    .detail("ShardBegin", range.begin)
							    .detail("ShardEnd", range.end)
							    .detail("RetryCount", retryCount)
							    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
							throw operation_failed();
						}
						storageServerInterfaces.push_back(decodeServerListValue(serverListValues[s].get()));
					}
					if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
						for (int j = 0; j < storageServers.size(); j++) {
							auto iter = storageServerToTagMap.find(storageServers[j]);
							ASSERT_WE_THINK(iter != storageServerToTagMap.end());
							// Note: This workload doesn't use the NativeAPI getRange() API for reading
							// data, so we will need to explicitly populate the (ssid, tag) mapping in
							// "cx" - this is so version vector APIs can correctly fetch the latest commit
							// versions of storage servers.
							cx->addSSIdTagMapping(storageServerInterfaces[j].id(), iter->second);
						}
					}
					break;
				} catch (Error& e) {
					err = e;
				}
				if (err.code() == error_code_actor_cancelled) {
					throw err;
				}
				co_await delay(5.0);
				retryCount++;
				if (retryCount > 10) {
					// SS could be removed from the cluster
					throw timed_out();
				}
			}

			// Step 3: Read a limited number of entries at a time, repeating until all keys in the shard have been read
			int64_t totalReadAmount = 0;
			int64_t shardReadAmount = 0;
			int64_t shardKeyCompared = 0;
			bool valueAvailableToCheck = true;
			Reverse reverse(SERVER_KNOBS->CONSISTENCY_CHECK_BACKWARD_READ);
			KeySelector cursor = reverse ? firstGreaterOrEqual(range.end) : firstGreaterOrEqual(range.begin);
			while (true) {
				Error err;
				try {
					std::vector<ErrorOr<GetKeyValuesReply>> readReplies =
					    co_await readFromAllStorageServers(cx, storageServerInterfaces, range, cursor, reverse);

					for (int j = 0; j < readReplies.size(); j++) {
						ErrorOr<GetKeyValuesReply> rangeResult = readReplies[j];
						if (!isSuccessReply(rangeResult)) {
							valueAvailableToCheck = false;
							TraceEvent e(SevInfo, "ConsistencyCheckUrgent_TesterGetRangeError");
							e.suppressFor(60.0);
							e.detail("ResultPresent", rangeResult.present());
							e.detail("StorageServer", storageServerInterfaces[j].uniqueID);
							if (rangeResult.present()) {
								e.detail("ErrorPresent", rangeResult.get().error.present());
								if (rangeResult.get().error.present()) {
									e.detail("Error", rangeResult.get().error.get().what());
								}
							} else {
								e.detail("ResultNotPresentWithError", rangeResult.getError().what());
								if (g_network->isSimulated() &&
								    g_simulator->getProcessByAddress(storageServerInterfaces[j].address())->failed) {
									e.detail("MachineFailed", "True");
								}
							}
							break;
						}
					}

					// FIXME: it seems like we should be able to move this after the if(!valueAvailableToCheck) thing
					// We've also already checked that none of the replies were failed, so the only place we should
					// actually get an error should be a legitimate failure. I think
					RangeConsistencyResult rangeConsistencyResult;
					if (valueAvailableToCheck) {
						rangeConsistencyResult =
						    checkRangeReplies(storageServerInterfaces, readReplies, range, cursor, reverse);
						if (!rangeConsistencyResult.success) {
							// checkRangeReplies() sets success = false both when it detects a dead storage
							// server (e.g. during a forced recovery) and when it detects a genuine data
							// mismatch -- rangeConsistencyResult.isFailed distinguishes the two, but this
							// workload doesn't currently check it, so both cases are treated alike: retried
							// like any other unavailable-data case above rather than failed outright. A
							// genuine mismatch will keep failing on retry until
							// CONSISTENCY_CHECK_URGENT_RETRY_DEPTH_MAX is exhausted, at which point
							// consistency_check_urgent_task_failed() is raised.
							valueAvailableToCheck = false;
						} else {
							totalReadAmount = rangeConsistencyResult.totalReadAmount;
							if (rangeConsistencyResult.firstValidServer >= 0) {
								shardKeyCompared +=
								    readReplies[rangeConsistencyResult.firstValidServer].get().data.size();
							}
						}
					}

					if (!valueAvailableToCheck) {
						failedRanges.insert(range, true);
						TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterShardAddedToRetry")
						    .suppressFor(60.0)
						    .setMaxEventLength(-1)
						    .setMaxFieldLength(-1)
						    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
						    .detail("ClientId", self->clientId)
						    .detail("ClientCount", self->clientCount)
						    .detail("ShardBegin", range.begin)
						    .detail("ShardEnd", range.end)
						    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
						break;
					}

					// RateKeeping
					co_await rateLimiter->getAllowance(totalReadAmount);

					shardReadAmount += totalReadAmount;

					// Advance to the next set of entries
					ASSERT(rangeConsistencyResult.firstValidServer >= 0);
					if (rangeConsistencyResult.nextKey.present()) {
						if (reverse) {
							cursor = firstGreaterOrEqual(rangeConsistencyResult.nextKey.get());
						} else {
							cursor = firstGreaterThan(rangeConsistencyResult.nextKey.get());
							ASSERT(cursor.getKey() != allKeys.end);
						}
					} else {
						break;
					}

				} catch (Error& e) {
					err = e;
				}
				if (!err.isValid()) {
					continue;
				}
				if (err.code() == error_code_actor_cancelled) {
					throw err;
				}
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterRetryDataConsistency").error(err);
				co_await delay(5.0);
			}

			// Step 4: if the shard failed to check, add it to retry queue
			// Otherwise, persist the progress
			if (!valueAvailableToCheck) {
				// Any shard for this case has been added to failedRanges which will be retried
				// by the next consistencyCheckEpoch
				numFailedShards++;
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterShardFailed")
				    .suppressFor(60.0)
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount)
				    .detail("NumCompletedShards", numCompleteShards)
				    .detail("NumFailedShards", numFailedShards)
				    .detail("NumShardThisClient", numShardThisClient)
				    .detail("NumShardToCheckThisEpoch", numShardToCheck - 1)
				    .detail("ShardBegin", range.begin)
				    .detail("ShardEnd", range.end)
				    .detail("ReplicaCount", storageServers.size())
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch)
				    .detail("ShardBytesRead", shardReadAmount)
				    .detail("ShardKeysCompared", shardKeyCompared);
			} else {
				numCompleteShards++;
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterShardComplete")
				    .setMaxEventLength(-1)
				    .setMaxFieldLength(-1)
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount)
				    .detail("ShardBegin", range.begin)
				    .detail("ShardEnd", range.end)
				    .detail("ReplicaCount", storageServers.size())
				    .detail("NumCompletedShards", numCompleteShards)
				    .detail("NumFailedShards", numFailedShards)
				    .detail("NumShardThisClient", numShardThisClient)
				    .detail("NumShardToCheckThisEpoch", numShardToCheck - 1)
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch)
				    .detail("ShardBytesRead", shardReadAmount)
				    .detail("ShardKeysCompared", shardKeyCompared);
			}
		}

		// For the failed ranges, trigger next epoch of checkDataConsistencyUrgent
		failedRanges.coalesce(allKeys);
		std::vector<KeyRange> failedRangesToCheck;
		KeyRangeMap<bool>::Ranges failedRangesList = failedRanges.ranges();
		for (auto failedRangesIter = failedRangesList.begin(); failedRangesIter != failedRangesList.end();
		     ++failedRangesIter) {
			if (failedRangesIter->value()) {
				failedRangesToCheck.push_back(failedRangesIter->range());
			}
		}
		if (!failedRangesToCheck.empty()) { // Retry for any failed shard
			if (consistencyCheckEpoch < CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RETRY_DEPTH_MAX) {
				co_await delay(60.0); // Backoff 1 min
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterRetryFailedRanges")
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("FailedCollectedRangeCount", failedRangesToCheck.size())
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount);
				co_await self->checkDataConsistencyUrgent(cx, failedRangesToCheck, self, consistencyCheckEpoch + 1);
			} else {
				TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterRetryDepthMax")
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("FailedCollectedRangeCount", failedRangesToCheck.size())
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount);
				throw consistency_check_urgent_task_failed(); // Notify the checker that this tester is failed
			}
			// When we are able to persist progress, we simply give up retrying when retry too many times
			// The failed ranges will be picked up by the next round of the consistency checker urgent
		}
		if (consistencyCheckEpoch == 0) {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterEndTask")
			    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
			    .detail("ShardCount", shardLocationPairList.size())
			    .detail("ClientId", self->clientId)
			    .detail("ClientCount", self->clientCount);
		}
	}

	Future<Void> _start(Database cx) {
		try {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterStart")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("ClientCount", clientCount)
			    .detail("ClientId", clientId);
			if (rangesToCheck.empty()) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterExit")
				    .detail("Reason", "AssignedEmptyRangeToCheck")
				    .detail("ConsistencyCheckerId", consistencyCheckerId)
				    .detail("ClientCount", clientCount)
				    .detail("ClientId", clientId);
				co_return;
			}
			if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterMimicFailure")
				    .detail("ClientCount", clientCount)
				    .detail("ClientId", clientId);
				throw operation_failed(); // mimic tester failure
			}
			co_await checkDataConsistencyUrgent(cx,
			                                    rangesToCheck,
			                                    this,
			                                    /*consistencyCheckEpoch=*/0);
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterExit")
			    .detail("Reason", "CompleteCheck")
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("ClientCount", clientCount)
			    .detail("ClientId", clientId);
		} catch (Error& e) {
			std::string reason;
			Severity sev = SevInfo;
			if (e.code() == error_code_actor_cancelled) {
				throw e;
			} else if (e.code() == error_code_timed_out) {
				reason = "Operation retried too many times";
			} else if (e.code() == error_code_consistency_check_urgent_task_failed) {
				reason = "Retry failed ranges for too many times";
			} else {
				reason = "Unexpected failure";
				sev = SevWarnAlways;
			}
			TraceEvent(sev, "ConsistencyCheckUrgent_TesterExit")
			    .error(e)
			    .detail("Reason", reason)
			    .detail("ConsistencyCheckerId", consistencyCheckerId)
			    .detail("ClientCount", clientCount)
			    .detail("ClientId", clientId);
			throw consistency_check_urgent_task_failed();
		}
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		TraceEvent("ConsistencyCheckUrgent_EnterWorkload").detail("ConsistencyCheckerId", consistencyCheckerId);
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ConsistencyCheckUrgentWorkload> ConsistencyCheckUrgentWorkloadFactory;
