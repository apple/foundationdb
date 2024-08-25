/*
 * ConsistencyCheckUrgent.actor.cpp
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

#include <math.h>
#include "boost/lexical_cast.hpp"

#include "flow/IRandom.h"
#include "flow/ProcessEvents.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/IRateControl.h"
#include "fdbrpc/simulator.h"
#include "fdbserver/Knobs.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/network.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "flow/actorcompiler.h" // This must be the last #include.

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

	ConsistencyCheckUrgentWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		consistencyCheckerId = sharedRandomNumber;
	}

	ACTOR Future<std::vector<std::pair<KeyRange, Value>>>
	getKeyLocationsForRangeList(Database cx, std::vector<KeyRange> ranges, ConsistencyCheckUrgentWorkload* self) {
		// Get the scope of the input list of ranges
		state Key beginKeyToReadKeyServer;
		state Key endKeyToReadKeyServer;
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
		state std::vector<std::pair<KeyRange, Value>> res;
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				KeyRange rangeToRead = Standalone(KeyRangeRef(beginKeyToReadKeyServer, endKeyToReadKeyServer));
				RangeResult readResult = wait(krmGetRanges(&tr,
				                                           keyServersPrefix,
				                                           rangeToRead,
				                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT,
				                                           SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES));
				for (int i = 0; i < readResult.size() - 1; ++i) {
					KeyRange rangeToCheck = Standalone(KeyRangeRef(readResult[i].key, readResult[i + 1].key));
					Value valueToCheck = Standalone(readResult[i].value);
					bool toAdd = false;
					for (const auto& range : ranges) {
						if (rangeToCheck.intersects(range) == true) {
							toAdd = true;
							break;
						}
					}
					if (toAdd == true) {
						res.push_back(std::make_pair(rangeToCheck, valueToCheck));
					}
					beginKeyToReadKeyServer = readResult[i + 1].key;
				}
				if (beginKeyToReadKeyServer >= endKeyToReadKeyServer) {
					break;
				}
			} catch (Error& e) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterRetryGetKeyLocationsForRangeList")
				    .error(e)
				    .detail("ClientCount", self->clientCount)
				    .detail("ClientId", self->clientId);
				wait(tr.onError(e));
			}
		}
		return res;
	}

	ACTOR Future<Version> getVersion(Database cx) {
		loop {
			state Transaction tr(cx);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				Version version = wait(tr.getReadVersion());
				return version;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> checkDataConsistencyUrgent(Database cx,
	                                              std::vector<KeyRange> rangesToCheck,
	                                              ConsistencyCheckUrgentWorkload* self,
	                                              int consistencyCheckEpoch) {
		// Get shard locations of the input rangesToCheck
		state std::vector<std::pair<KeyRange, Value>> shardLocationPairList;
		state int retryCount = 0;
		loop {
			try {
				shardLocationPairList.clear();
				wait(store(shardLocationPairList, self->getKeyLocationsForRangeList(cx, rangesToCheck, self)));
				break;
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled) {
					throw e;
				}
				TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterGetKeyLocationListFailed")
				    .error(e)
				    .detail("RetryCount", retryCount)
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount)
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
				wait(delay(5.0));
				retryCount++;
				if (retryCount > 50) {
					throw timed_out();
				}
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
		state Reference<IRateControl> rateLimiter =
		    Reference<IRateControl>(new SpeedLimit(CLIENT_KNOBS->CONSISTENCY_CHECK_RATE_LIMIT_MAX, 1));
		state KeyRangeMap<bool> failedRanges; // Used to collect failed ranges in the current checkDataConsistency
		failedRanges.insert(allKeys, false); // Initialized with false and will set any failed range as true later
		// Which will be used to start the next consistencyCheckEpoch of the checkDataConsistency
		state int64_t numShardThisClient = shardLocationPairList.size();
		state int64_t numShardToCheck = -1;
		state int64_t numCompleteShards = 0;
		state int64_t numFailedShards = 0;
		state int shardIdx = 0;
		for (; shardIdx < shardLocationPairList.size(); ++shardIdx) {
			numShardToCheck = numShardThisClient - shardIdx;
			state KeyRangeRef range = shardLocationPairList[shardIdx].first;
			// Step 1: Get source server id of the shard
			state std::vector<UID> sourceStorageServers;
			state std::vector<UID> destStorageServers;
			retryCount = 0;
			loop {
				try {
					Transaction tr(cx);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
					ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
					decodeKeyServersValue(UIDtoTagMap,
					                      shardLocationPairList[shardIdx].second,
					                      sourceStorageServers,
					                      destStorageServers,
					                      false);
					break;
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}
					TraceEvent(SevWarn, "ConsistencyCheckUrgent_TesterGetUIDtoTagMapFailed")
					    .error(e)
					    .detail("RetryCount", retryCount)
					    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
					    .detail("ClientId", self->clientId)
					    .detail("ClientCount", self->clientCount)
					    .detail("ShardBegin", range.begin)
					    .detail("ShardEnd", range.end)
					    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch);
					wait(delay(5.0));
					retryCount++;
					if (retryCount > 50) {
						throw timed_out();
					}
				}
			}

			if (sourceStorageServers.size() == 0) {
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
			state std::vector<UID> storageServers = sourceStorageServers; // We check source server
			state std::vector<StorageServerInterface> storageServerInterfaces;
			retryCount = 0;
			loop {
				try {
					Transaction tr(cx);
					tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
					tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					tr.setOption(FDBTransactionOptions::LOCK_AWARE);
					std::vector<Future<Optional<Value>>> serverListEntries;
					for (int s = 0; s < storageServers.size(); s++)
						serverListEntries.push_back(tr.get(serverListKeyFor(storageServers[s])));
					state std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));
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
					break;
				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}
					wait(delay(5.0));
					retryCount++;
					if (retryCount > 50) {
						throw timed_out();
					}
				}
			}

			// Step 3: Read a limited number of entries at a time, repeating until all keys in the shard have been read
			state int64_t totalReadAmount = 0;
			state int64_t shardReadAmount = 0;
			state int64_t shardKeyCompared = 0;
			state bool valueAvailableToCheck = true;
			state KeySelector begin = firstGreaterOrEqual(range.begin);
			loop {
				try {
					// Get the min version of the storage servers
					Version version = wait(self->getVersion(cx));

					state GetKeyValuesRequest req;
					req.begin = begin;
					req.end = firstGreaterOrEqual(range.end);
					req.limit = 1e4;
					req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
					req.version = version;
					req.tags = TagSet();

					// Try getting the entries in the specified range
					state std::vector<Future<ErrorOr<GetKeyValuesReply>>> keyValueFutures;
					state int j = 0;
					for (j = 0; j < storageServerInterfaces.size(); j++) {
						resetReply(req);
						keyValueFutures.push_back(
						    storageServerInterfaces[j].getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
					}
					wait(waitForAll(keyValueFutures));

					for (j = 0; j < keyValueFutures.size(); j++) {
						ErrorOr<GetKeyValuesReply> rangeResult = keyValueFutures[j].get();
						if (!rangeResult.present() || rangeResult.get().error.present()) {
							valueAvailableToCheck = false;
							TraceEvent e(SevInfo, "ConsistencyCheckUrgent_TesterGetRangeError");
							e.detail("ResultPresent", rangeResult.present());
							if (rangeResult.present()) {
								e.detail("ErrorPresent", rangeResult.get().error.present());
								if (rangeResult.get().error.present()) {
									e.detail("Error", rangeResult.get().error.get().what());
								}
							} else {
								e.detail("ResultNotPresentWithError", rangeResult.getError().what());
							}
							break;
						}
					}
					if (!valueAvailableToCheck) {
						failedRanges.insert(range, true);
						TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterShardAddedToRetry")
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

					state int firstValidServer = -1;
					totalReadAmount = 0;
					for (j = 0; j < keyValueFutures.size(); j++) {
						ErrorOr<GetKeyValuesReply> rangeResult = keyValueFutures[j].get();
						ASSERT(rangeResult.present() && !rangeResult.get().error.present());

						state GetKeyValuesReply current = rangeResult.get();
						totalReadAmount += current.data.expectedSize();
						// If we haven't encountered a valid storage server yet, then mark this as the baseline
						// to compare against
						if (firstValidServer == -1) {
							firstValidServer = j;
							GetKeyValuesReply reference = keyValueFutures[firstValidServer].get().get();
							shardKeyCompared += current.data.size();
						} else {
							// Compare this shard against the first
							GetKeyValuesReply reference = keyValueFutures[firstValidServer].get().get();
							if (current.data != reference.data || current.more != reference.more) {
								// Data for trace event
								// The number of keys unique to the current shard
								int currentUniques = 0;
								// The number of keys unique to the reference shard
								int referenceUniques = 0;
								// The number of keys in both shards with conflicting values
								int valueMismatches = 0;
								// The number of keys in both shards with matching values
								int matchingKVPairs = 0;
								// Last unique key on the current shard
								KeyRef currentUniqueKey;
								// Last unique key on the reference shard
								KeyRef referenceUniqueKey;
								// Last value mismatch
								KeyRef valueMismatchKey;

								// Loop indeces
								int currentI = 0;
								int referenceI = 0;
								while (currentI < current.data.size() || referenceI < reference.data.size()) {
									if (currentI >= current.data.size()) {
										referenceUniqueKey = reference.data[referenceI].key;
										referenceUniques++;
										referenceI++;
									} else if (referenceI >= reference.data.size()) {
										currentUniqueKey = current.data[currentI].key;
										currentUniques++;
										currentI++;
									} else {
										KeyValueRef currentKV = current.data[currentI];
										KeyValueRef referenceKV = reference.data[referenceI];
										if (currentKV.key == referenceKV.key) {
											if (currentKV.value == referenceKV.value)
												matchingKVPairs++;
											else {
												valueMismatchKey = currentKV.key;
												valueMismatches++;
											}
											currentI++;
											referenceI++;
										} else if (currentKV.key < referenceKV.key) {
											currentUniqueKey = currentKV.key;
											currentUniques++;
											currentI++;
										} else {
											referenceUniqueKey = referenceKV.key;
											referenceUniques++;
											referenceI++;
										}
									}
								}

								TraceEvent(SevError, "ConsistencyCheck_DataInconsistent")
								    .setMaxEventLength(-1)
								    .setMaxFieldLength(-1)
								    .detail(format("StorageServer%d", j).c_str(), storageServers[j].toString())
								    .detail(format("StorageServer%d", firstValidServer).c_str(),
								            storageServers[firstValidServer].toString())
								    .detail("RangeBegin", req.begin.getKey())
								    .detail("RangeEnd", req.end.getKey())
								    .detail("VersionNumber", req.version)
								    .detail(format("Server%dUniques", j).c_str(), currentUniques)
								    .detail(format("Server%dUniqueKey", j).c_str(), currentUniqueKey)
								    .detail(format("Server%dUniques", firstValidServer).c_str(), referenceUniques)
								    .detail(format("Server%dUniqueKey", firstValidServer).c_str(), referenceUniqueKey)
								    .detail("ValueMismatches", valueMismatches)
								    .detail("ValueMismatchKey", valueMismatchKey)
								    .detail("MatchingKVPairs", matchingKVPairs)
								    .detail("IsTSS",
								            storageServerInterfaces[j].isTss() ||
								                    storageServerInterfaces[firstValidServer].isTss()
								                ? "True"
								                : "False")
								    .detail("ShardBegin", range.begin)
								    .detail("ShardEnd", range.end);
							}
						}
					}

					// RateKeeping
					wait(rateLimiter->getAllowance(totalReadAmount));

					shardReadAmount += totalReadAmount;

					// Advance to the next set of entries
					ASSERT(firstValidServer != -1);
					if (keyValueFutures[firstValidServer].get().get().more) {
						VectorRef<KeyValueRef> result = keyValueFutures[firstValidServer].get().get().data;
						ASSERT(result.size() > 0);
						begin = firstGreaterThan(result[result.size() - 1].key);
						ASSERT(begin.getKey() != allKeys.end);
					} else {
						break;
					}

				} catch (Error& e) {
					if (e.code() == error_code_actor_cancelled) {
						throw e;
					}
					TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterRetryDataConsistency").error(e);
					wait(delay(5.0));
				}
			}

			// Step 4: if the shard failed to check, add it to retry queue
			// Otherwise, persist the progress
			if (!valueAvailableToCheck) {
				// Any shard for this case has been added to failedRanges which will be retried
				// by the next consistencyCheckEpoch
				numFailedShards++;
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterShardFailed")
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
		state std::vector<KeyRange> failedRangesToCheck;
		state KeyRangeMap<bool>::Ranges failedRangesList = failedRanges.ranges();
		state KeyRangeMap<bool>::iterator failedRangesIter = failedRangesList.begin();
		for (; failedRangesIter != failedRangesList.end(); ++failedRangesIter) {
			if (failedRangesIter->value()) {
				failedRangesToCheck.push_back(failedRangesIter->range());
			}
		}
		if (failedRangesToCheck.size() > 0) { // Retry for any failed shard
			if (consistencyCheckEpoch < CLIENT_KNOBS->CONSISTENCY_CHECK_URGENT_RETRY_DEPTH_MAX) {
				wait(delay(60.0)); // Backoff 1 min
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterRetryFailedRanges")
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("FailedCollectedRangeCount", failedRangesToCheck.size())
				    .detail("ConsistencyCheckEpoch", consistencyCheckEpoch)
				    .detail("ClientId", self->clientId)
				    .detail("ClientCount", self->clientCount);
				wait(self->checkDataConsistencyUrgent(cx, failedRangesToCheck, self, consistencyCheckEpoch + 1));
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
		return Void();
	}

	ACTOR Future<Void> _start(Database cx, ConsistencyCheckUrgentWorkload* self) {
		try {
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterStart")
			    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
			    .detail("ClientCount", self->clientCount)
			    .detail("ClientId", self->clientId);
			if (self->rangesToCheck.size() == 0) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterExit")
				    .detail("Reason", "AssignedEmptyRangeToCheck")
				    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
				    .detail("ClientCount", self->clientCount)
				    .detail("ClientId", self->clientId);
				return Void();
			}
			if (g_network->isSimulated() && deterministicRandom()->coinflip()) {
				TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterMimicFailure")
				    .detail("ClientCount", self->clientCount)
				    .detail("ClientId", self->clientId);
				throw operation_failed(); // mimic tester failure
			}
			wait(self->checkDataConsistencyUrgent(cx,
			                                      self->rangesToCheck,
			                                      self,
			                                      /*consistencyCheckEpoch=*/0));
			TraceEvent(SevInfo, "ConsistencyCheckUrgent_TesterExit")
			    .detail("Reason", "CompleteCheck")
			    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
			    .detail("ClientCount", self->clientCount)
			    .detail("ClientId", self->clientId);
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
			    .detail("ConsistencyCheckerId", self->consistencyCheckerId)
			    .detail("ClientCount", self->clientCount)
			    .detail("ClientId", self->clientId);
			throw consistency_check_urgent_task_failed();
		}
		return Void();
	}

	Future<Void> setup(Database const& cx) override { return Void(); }

	Future<Void> start(Database const& cx) override {
		TraceEvent("ConsistencyCheckUrgent_EnterWorkload").detail("ConsistencyCheckerId", consistencyCheckerId);
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}
};

WorkloadFactory<ConsistencyCheckUrgentWorkload> ConsistencyCheckUrgentWorkloadFactory;
