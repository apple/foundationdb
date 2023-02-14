/*
 * ConsistencyScan.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

#include "fdbrpc/TenantInfo.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "flow/IRandom.h"
#include "flow/IndexedSet.h"
#include "fdbrpc/FailureMonitor.h"
#include "fdbrpc/Smoother.h"
#include "fdbrpc/simulator.h"
#include "fdbclient/DatabaseContext.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/TagThrottle.actor.h"
#include "fdbserver/Knobs.h"
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/RatekeeperInterface.h"
#include "fdbserver/ServerDBInfo.h"
#include "fdbserver/WaitFailure.h"
#include "fdbserver/TesterInterface.actor.h"
#include "flow/DeterministicRandom.h"
#include "flow/Trace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

// Core of the data consistency checking (checkDataConsistency) and many of the supporting functions are shared between
// the ConsistencyScan role and the ConsistencyCheck workload. They are currently part of this file. ConsistencyScan
// role's main goal is to simply validate data across all shards, while ConsistencyCheck workload does more than that.
// Potentially a re-factor candidate!

struct ConsistencyScanData {
	UID id;
	Database db;

	DatabaseConfiguration configuration;
	PromiseStream<Future<Void>> addActor;

	// TODO: Consider holding a ConsistencyScanInfo object to use as its state, as many of the members are the same.
	int64_t restart = 1;
	int64_t maxRate = 0;
	int64_t targetInterval = 0;
	int64_t bytesReadInPrevRound = 0;
	int finishedRounds = 0;
	KeyRef progressKey;
	AsyncVar<bool> consistencyScanEnabled = false;

	ConsistencyScanData(UID id, Database db) : id(id), db(db) {}
};

// Gets a version at which to read from the storage servers
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

void testFailure(std::string message, bool performQuiescentChecks, bool isError) {
	TraceEvent failEvent(isError ? SevError : SevWarn, "TestFailure");
	if (performQuiescentChecks)
		failEvent.detail("Workload", "QuiescentCheck");
	else
		failEvent.detail("Workload", "ConsistencyCheck");

	failEvent.detail("Reason", "Consistency check: " + message);
}

// Get a list of storage servers(persisting keys within range "kr") from the master and compares them with the
// TLogs. If this is a quiescent check, then each commit proxy needs to respond, otherwise only one needs to
// respond. Returns false if there is a failure (in this case, keyServersPromise will never be set)
ACTOR Future<bool> getKeyServers(
    Database cx,
    Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServersPromise,
    KeyRangeRef kr,
    bool performQuiescentChecks) {
	state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers;

	// Try getting key server locations from the master proxies
	state std::vector<Future<ErrorOr<GetKeyServerLocationsReply>>> keyServerLocationFutures;
	state Key begin = kr.begin;
	state Key end = kr.end;
	state int limitKeyServers = BUGGIFY ? 1 : 100;
	state Span span(SpanContext(deterministicRandom()->randomUniqueID(), deterministicRandom()->randomUInt64()),
	                "WL:ConsistencyCheck"_loc);

	while (begin < end) {
		state Reference<CommitProxyInfo> commitProxyInfo =
		    wait(cx->getCommitProxiesFuture(UseProvisionalProxies::False));
		keyServerLocationFutures.clear();
		for (int i = 0; i < commitProxyInfo->size(); i++)
			keyServerLocationFutures.push_back(
			    commitProxyInfo->get(i, &CommitProxyInterface::getKeyServersLocations)
			        .getReplyUnlessFailedFor(
			            GetKeyServerLocationsRequest(
			                span.context, TenantInfo(), begin, end, limitKeyServers, false, latestVersion, Arena()),
			            2,
			            0));

		state bool keyServersInsertedForThisIteration = false;
		choose {
			when(wait(waitForAll(keyServerLocationFutures))) {
				// Read the key server location results
				for (int i = 0; i < keyServerLocationFutures.size(); i++) {
					ErrorOr<GetKeyServerLocationsReply> shards = keyServerLocationFutures[i].get();

					// If performing quiescent check, then all master proxies should be reachable.  Otherwise, only
					// one needs to be reachable
					if (performQuiescentChecks && !shards.present()) {
						TraceEvent("ConsistencyCheck_CommitProxyUnavailable")
						    .error(shards.getError())
						    .detail("CommitProxyID", commitProxyInfo->getId(i));
						testFailure("Commit proxy unavailable", performQuiescentChecks, true);
						return false;
					}

					// Get the list of shards if one was returned.  If not doing a quiescent check, we can break if
					// it is. If we are doing a quiescent check, then we only need to do this for the first shard.
					if (shards.present() && !keyServersInsertedForThisIteration) {
						keyServers.insert(keyServers.end(), shards.get().results.begin(), shards.get().results.end());
						keyServersInsertedForThisIteration = true;
						begin = shards.get().results.back().first.end;

						if (!performQuiescentChecks)
							break;
					}
				} // End of For
			}
			when(wait(cx->onProxiesChanged())) {}
		} // End of choose

		if (!keyServersInsertedForThisIteration) // Retry the entire workflow
			wait(delay(1.0));

	} // End of while

	keyServersPromise.send(keyServers);
	return true;
}

// Retrieves the locations of all shards in the database
// Returns false if there is a failure (in this case, keyLocationPromise will never be set)
ACTOR Future<bool> getKeyLocations(Database cx,
                                   std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> shards,
                                   Promise<Standalone<VectorRef<KeyValueRef>>> keyLocationPromise,
                                   bool performQuiescentChecks) {
	state Standalone<VectorRef<KeyValueRef>> keyLocations;
	state Key beginKey = allKeys.begin.withPrefix(keyServersPrefix);
	state Key endKey = allKeys.end.withPrefix(keyServersPrefix);
	state int i = 0;
	state Transaction onErrorTr(cx); // This transaction exists only to access onError and its backoff behavior

	// If the responses are too big, we may use multiple requests to get the key locations.  Each request begins
	// where the last left off
	for (; i < shards.size(); i++) {
		while (beginKey < std::min<KeyRef>(shards[i].first.end, endKey)) {
			try {
				Version version = wait(getVersion(cx));

				GetKeyValuesRequest req;
				req.begin = firstGreaterOrEqual(beginKey);
				req.end = firstGreaterOrEqual(std::min<KeyRef>(shards[i].first.end, endKey));
				req.limit = SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT;
				req.limitBytes = SERVER_KNOBS->MOVE_KEYS_KRM_LIMIT_BYTES;
				req.version = version;
				req.tags = TagSet();

				// Try getting the shard locations from the key servers
				state std::vector<Future<ErrorOr<GetKeyValuesReply>>> keyValueFutures;
				for (const auto& kv : shards[i].second) {
					resetReply(req);
					if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
						cx->getLatestCommitVersion(kv, req.version, req.ssLatestCommitVersions);
					}
					keyValueFutures.push_back(kv.getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
				}

				wait(waitForAll(keyValueFutures));

				int firstValidStorageServer = -1;

				// Read the shard location results
				for (int j = 0; j < keyValueFutures.size(); j++) {
					ErrorOr<GetKeyValuesReply> reply = keyValueFutures[j].get();

					if (!reply.present() || reply.get().error.present()) {
						// If no storage servers replied, then throw all_alternatives_failed to force a retry
						if (firstValidStorageServer < 0 && j == keyValueFutures.size() - 1)
							throw all_alternatives_failed();
					}

					// If this is the first storage server, store the locations to send back to the caller
					else if (firstValidStorageServer < 0) {
						firstValidStorageServer = j;

						// Otherwise, compare the data to the results from the first storage server.  If they are
						// different, then the check fails
					} else if (reply.get().data != keyValueFutures[firstValidStorageServer].get().get().data ||
					           reply.get().more != keyValueFutures[firstValidStorageServer].get().get().more) {
						TraceEvent("ConsistencyCheck_InconsistentKeyServers")
						    .detail("StorageServer1", shards[i].second[firstValidStorageServer].id())
						    .detail("StorageServer2", shards[i].second[j].id());
						testFailure("Key servers inconsistent", performQuiescentChecks, true);
						return false;
					}
				}

				auto keyValueResponse = keyValueFutures[firstValidStorageServer].get().get();
				RangeResult currentLocations = krmDecodeRanges(
				    keyServersPrefix,
				    KeyRangeRef(beginKey.removePrefix(keyServersPrefix),
				                std::min<KeyRef>(shards[i].first.end, endKey).removePrefix(keyServersPrefix)),
				    RangeResultRef(keyValueResponse.data, keyValueResponse.more));

				if (keyValueResponse.data.size() && beginKey == keyValueResponse.data[0].key) {
					keyLocations.push_back_deep(keyLocations.arena(), currentLocations[0]);
				}

				if (currentLocations.size() > 2) {
					keyLocations.append_deep(keyLocations.arena(), &currentLocations[1], currentLocations.size() - 2);
				}

				// Next iteration should pick up where we left off
				ASSERT(currentLocations.size() > 1);
				if (!keyValueResponse.more) {
					beginKey = shards[i].first.end;
				} else {
					beginKey = keyValueResponse.data.end()[-1].key;
				}

				// If this is the last iteration, then push the allKeys.end KV pair
				if (beginKey >= endKey)
					keyLocations.push_back_deep(keyLocations.arena(), currentLocations.end()[-1]);
			} catch (Error& e) {
				state Error err = e;
				wait(onErrorTr.onError(err));
				TraceEvent("ConsistencyCheck_RetryGetKeyLocations").error(err);
			}
		}
	}

	keyLocationPromise.send(keyLocations);
	return true;
}

// Retrieves a vector of the storage servers' estimates for the size of a particular shard
// If a storage server can't be reached, its estimate will be -1
// If there is an error, then the returned vector will have 0 size
ACTOR Future<std::vector<int64_t>> getStorageSizeEstimate(std::vector<StorageServerInterface> storageServers,
                                                          KeyRangeRef shard) {
	state std::vector<int64_t> estimatedBytes;

	state WaitMetricsRequest req;
	req.keys = shard;
	req.max.bytes = -1;
	req.min.bytes = 0;

	state std::vector<Future<ErrorOr<StorageMetrics>>> metricFutures;

	try {
		// Check the size of the shard on each storage server
		for (int i = 0; i < storageServers.size(); i++) {
			resetReply(req);
			metricFutures.push_back(storageServers[i].waitMetrics.getReplyUnlessFailedFor(req, 2, 0));
		}

		// Wait for the storage servers to respond
		wait(waitForAll(metricFutures));

		int firstValidStorageServer = -1;

		// Retrieve the size from the storage server responses
		for (int i = 0; i < storageServers.size(); i++) {
			ErrorOr<StorageMetrics> reply = metricFutures[i].get();

			// If the storage server doesn't reply, then return -1
			if (!reply.present()) {
				TraceEvent("ConsistencyCheck_FailedToFetchMetrics")
				    .error(reply.getError())
				    .detail("Begin", printable(shard.begin))
				    .detail("End", printable(shard.end))
				    .detail("StorageServer", storageServers[i].id())
				    .detail("IsTSS", storageServers[i].isTss() ? "True" : "False");
				estimatedBytes.push_back(-1);
			}

			// Add the result to the list of estimates
			else if (reply.present()) {
				int64_t numBytes = reply.get().bytes;
				estimatedBytes.push_back(numBytes);
				if (firstValidStorageServer < 0)
					firstValidStorageServer = i;
				else if (estimatedBytes[firstValidStorageServer] != numBytes) {
					TraceEvent("ConsistencyCheck_InconsistentStorageMetrics")
					    .detail("ByteEstimate1", estimatedBytes[firstValidStorageServer])
					    .detail("ByteEstimate2", numBytes)
					    .detail("Begin", shard.begin)
					    .detail("End", shard.end)
					    .detail("StorageServer1", storageServers[firstValidStorageServer].id())
					    .detail("StorageServer2", storageServers[i].id())
					    .detail("IsTSS",
					            storageServers[i].isTss() || storageServers[firstValidStorageServer].isTss() ? "True"
					                                                                                         : "False");
				}
			}
		}
	} catch (Error& e) {
		TraceEvent("ConsistencyCheck_ErrorFetchingMetrics")
		    .error(e)
		    .detail("Begin", printable(shard.begin))
		    .detail("End", printable(shard.end));
		estimatedBytes.clear();
	}

	return estimatedBytes;
}

ACTOR Future<int64_t> getDatabaseSize(Database cx) {
	state Transaction tr(cx);
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	loop {
		try {
			StorageMetrics metrics =
			    wait(tr.getDatabase()->getStorageMetrics(KeyRangeRef(allKeys.begin, keyServersPrefix), 100000));
			return metrics.bytes;
		} catch (Error& e) {
			wait(tr.onError(e));
		}
	}
}

// Checks that the data in each shard is the same on each storage server that it resides on.  Also performs some
// sanity checks on the sizes of shards and storage servers. Returns false if there is a failure
// TODO: Future optimization: Use streaming reads
ACTOR Future<bool> checkDataConsistency(Database cx,
                                        VectorRef<KeyValueRef> keyLocations,
                                        DatabaseConfiguration configuration,
                                        std::map<UID, StorageServerInterface> tssMapping,
                                        bool performQuiescentChecks,
                                        bool performTSSCheck,
                                        bool firstClient,
                                        bool failureIsError,
                                        int clientId,
                                        int clientCount,
                                        bool distributed,
                                        bool shuffleShards,
                                        int shardSampleFactor,
                                        int64_t sharedRandomNumber,
                                        int64_t repetitions,
                                        int64_t* bytesReadInPrevRound,
                                        int restart,
                                        int64_t maxRate,
                                        int64_t targetInterval,
                                        KeyRef progressKey) {
	// Stores the total number of bytes on each storage server
	// In a distributed test, this will be an estimated size
	state std::map<UID, int64_t> storageServerSizes;

	// Iterate through each shard, checking its values on all of its storage servers
	// If shardSampleFactor > 1, then not all shards are processed
	// Also, in a distributed data consistency check, each client processes a subset of the shards
	// Note: this may cause some shards to be processed more than once or not at all in a non-quiescent database
	state int effectiveClientCount = distributed ? clientCount : 1;
	state int i = clientId * (shardSampleFactor + 1);
	state int64_t rateLimitForThisRound =
	    *bytesReadInPrevRound == 0
	        ? maxRate
	        : std::min(maxRate, static_cast<int64_t>(ceil(*bytesReadInPrevRound / (float)targetInterval)));
	ASSERT(rateLimitForThisRound >= 0 && rateLimitForThisRound <= maxRate);
	TraceEvent("ConsistencyCheck_RateLimitForThisRound").detail("RateLimit", rateLimitForThisRound);
	state Reference<IRateControl> rateLimiter = Reference<IRateControl>(new SpeedLimit(rateLimitForThisRound, 1));
	state double rateLimiterStartTime = now();
	state int64_t bytesReadInthisRound = 0;
	state bool resume = !(restart || shuffleShards);
	state bool testResult = true;

	state double dbSize = 100e12;
	if (g_network->isSimulated()) {
		// This call will get all shard ranges in the database, which is too expensive on real clusters.
		int64_t _dbSize = wait(getDatabaseSize(cx));
		dbSize = _dbSize;
	}

	state std::vector<KeyRangeRef> ranges;

	for (int k = 0; k < keyLocations.size() - 1; k++) {
		// TODO: check if this is sufficient
		if (resume && keyLocations[k].key < progressKey) {
			TraceEvent("ConsistencyCheck_SkippingRange")
			    .detail("KeyBegin", keyLocations[k].key.toString())
			    .detail("KeyEnd", keyLocations[k + 1].key.toString())
			    .detail("PrevKey", progressKey.toString());
			continue;
		}
		KeyRangeRef range(keyLocations[k].key, keyLocations[k + 1].key);
		ranges.push_back(range);
	}

	state std::vector<int> shardOrder;
	shardOrder.reserve(ranges.size());
	for (int k = 0; k < ranges.size(); k++)
		shardOrder.push_back(k);
	if (shuffleShards) {
		DeterministicRandom sharedRandom(sharedRandomNumber + repetitions);
		sharedRandom.randomShuffle(shardOrder);
	}

	for (; i < ranges.size(); i++) {
		state int shard = shardOrder[i];

		state KeyRangeRef range = ranges[shard];
		state std::vector<UID> sourceStorageServers;
		state std::vector<UID> destStorageServers;
		state Transaction tr(cx);
		tr.setOption(FDBTransactionOptions::LOCK_AWARE);
		state int bytesReadInRange = 0;

		RangeResult UIDtoTagMap = wait(tr.getRange(serverTagKeys, CLIENT_KNOBS->TOO_MANY));
		ASSERT(!UIDtoTagMap.more && UIDtoTagMap.size() < CLIENT_KNOBS->TOO_MANY);
		decodeKeyServersValue(UIDtoTagMap, keyLocations[shard].value, sourceStorageServers, destStorageServers, false);

		// If the destStorageServers is non-empty, then this shard is being relocated
		state bool isRelocating = destStorageServers.size() > 0;

		// In a quiescent database, check that the team size is the same as the desired team size
		if (firstClient && performQuiescentChecks &&
		    sourceStorageServers.size() != configuration.usableRegions * configuration.storageTeamSize) {
			TraceEvent("ConsistencyCheck_InvalidTeamSize")
			    .detail("ShardBegin", printable(range.begin))
			    .detail("ShardEnd", printable(range.end))
			    .detail("SourceTeamSize", sourceStorageServers.size())
			    .detail("DestServerSize", destStorageServers.size())
			    .detail("ConfigStorageTeamSize", configuration.storageTeamSize)
			    .detail("UsableRegions", configuration.usableRegions);
			// Record the server reponsible for the problematic shards
			int k = 0;
			for (auto& id : sourceStorageServers) {
				TraceEvent("IncorrectSizeTeamInfo").detail("ServerUID", id).detail("TeamIndex", k++);
			}
			testFailure("Invalid team size", performQuiescentChecks, failureIsError);
			return false;
		}

		state std::vector<UID> storageServers = (isRelocating) ? destStorageServers : sourceStorageServers;
		state std::vector<StorageServerInterface> storageServerInterfaces;

		loop {
			try {
				std::vector<Future<Optional<Value>>> serverListEntries;
				serverListEntries.reserve(storageServers.size());
				for (int s = 0; s < storageServers.size(); s++)
					serverListEntries.push_back(tr.get(serverListKeyFor(storageServers[s])));
				state std::vector<Optional<Value>> serverListValues = wait(getAll(serverListEntries));
				for (int s = 0; s < serverListValues.size(); s++) {
					if (serverListValues[s].present())
						storageServerInterfaces.push_back(decodeServerListValue(serverListValues[s].get()));
					else if (performQuiescentChecks)
						testFailure(
						    "/FF/serverList changing in a quiescent database", performQuiescentChecks, failureIsError);
				}

				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		// add TSS to end of list, if configured and if not relocating
		if (!isRelocating && performTSSCheck) {
			int initialSize = storageServers.size();
			for (int i = 0; i < initialSize; i++) {
				auto tssPair = tssMapping.find(storageServers[i]);
				if (tssPair != tssMapping.end()) {
					CODE_PROBE(true, "TSS checked in consistency check");
					storageServers.push_back(tssPair->second.id());
					storageServerInterfaces.push_back(tssPair->second);
				}
			}
		}

		state std::vector<int64_t> estimatedBytes = wait(getStorageSizeEstimate(storageServerInterfaces, range));

		// Gets permitted size range of shard
		int64_t maxShardSize = getMaxShardSize(dbSize);
		state ShardSizeBounds shardBounds = getShardSizeBounds(range, maxShardSize);

		if (firstClient) {
			// If there was an error retrieving shard estimated size
			if (performQuiescentChecks && estimatedBytes.size() == 0)
				testFailure("Error fetching storage metrics", performQuiescentChecks, failureIsError);

			// If running a distributed test, storage server size is an accumulation of shard estimates
			else if (distributed && firstClient)
				for (int j = 0; j < storageServers.size(); j++)
					storageServerSizes[storageServers[j]] += std::max(estimatedBytes[j], (int64_t)0);
		}

		// The first client may need to skip the rest of the loop contents if it is just processing this shard to
		// get a size estimate
		if (!firstClient || shard % (effectiveClientCount * shardSampleFactor) == 0) {
			state int shardKeys = 0;
			state int shardBytes = 0;
			state int sampledBytes = 0;
			state int splitBytes = 0;
			state int firstKeySampledBytes = 0;
			state int sampledKeys = 0;
			state int sampledKeysWithProb = 0;
			state double shardVariance = 0;
			state bool canSplit = false;
			state Key lastSampleKey;
			state Key lastStartSampleKey;
			state int64_t totalReadAmount = 0;

			state KeySelector begin = firstGreaterOrEqual(range.begin);
			state Transaction onErrorTr(cx); // This transaction exists only to access onError and its backoff behavior

			// Read a limited number of entries at a time, repeating until all keys in the shard have been read
			loop {
				try {
					lastSampleKey = lastStartSampleKey;

					// Get the min version of the storage servers
					Version version = wait(getVersion(cx));

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
					TraceEvent("ConsistencyCheck_StoringGetFutures").detail("SSISize", storageServerInterfaces.size());
					for (j = 0; j < storageServerInterfaces.size(); j++) {
						resetReply(req);
						if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
							cx->getLatestCommitVersion(
							    storageServerInterfaces[j], req.version, req.ssLatestCommitVersions);
						}
						keyValueFutures.push_back(
						    storageServerInterfaces[j].getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
					}

					wait(waitForAll(keyValueFutures));

					// Read the resulting entries
					state int firstValidServer = -1;
					totalReadAmount = 0;
					for (j = 0; j < storageServerInterfaces.size(); j++) {
						ErrorOr<GetKeyValuesReply> rangeResult = keyValueFutures[j].get();

						// Compare the results with other storage servers
						if (rangeResult.present() && !rangeResult.get().error.present()) {
							state GetKeyValuesReply current = rangeResult.get();
							TraceEvent("ConsistencyCheck_GetKeyValuesStream")
							    .detail("DataSize", current.data.size())
							    .detail(format("StorageServer%d", j).c_str(), storageServers[j].toString());
							totalReadAmount += current.data.expectedSize();
							// If we haven't encountered a valid storage server yet, then mark this as the baseline
							// to compare against
							if (firstValidServer == -1) {
								TraceEvent("ConsistencyCheck_FirstValidServer").detail("Iter", j);
								firstValidServer = j;
								// Compare this shard against the first
							} else {
								GetKeyValuesReply reference = keyValueFutures[firstValidServer].get().get();

								if (current.data != reference.data || current.more != reference.more) {
									// Be especially verbose if in simulation
									if (g_network->isSimulated()) {
										int invalidIndex = -1;
										printf("\n%sSERVER %d (%s); shard = %s - %s:\n",
										       "",
										       j,
										       storageServerInterfaces[j].address().toString().c_str(),
										       printable(req.begin.getKey()).c_str(),
										       printable(req.end.getKey()).c_str());
										for (int k = 0; k < current.data.size(); k++) {
											printf("%d. %s => %s\n",
											       k,
											       printable(current.data[k].key).c_str(),
											       printable(current.data[k].value).c_str());
											if (invalidIndex < 0 && (k >= reference.data.size() ||
											                         current.data[k].key != reference.data[k].key ||
											                         current.data[k].value != reference.data[k].value))
												invalidIndex = k;
										}

										printf("\n%sSERVER %d (%s); shard = %s - %s:\n",
										       "",
										       firstValidServer,
										       storageServerInterfaces[firstValidServer].address().toString().c_str(),
										       printable(req.begin.getKey()).c_str(),
										       printable(req.end.getKey()).c_str());
										for (int k = 0; k < reference.data.size(); k++) {
											printf("%d. %s => %s\n",
											       k,
											       printable(reference.data[k].key).c_str(),
											       printable(reference.data[k].value).c_str());
											if (invalidIndex < 0 && (k >= current.data.size() ||
											                         reference.data[k].key != current.data[k].key ||
											                         reference.data[k].value != current.data[k].value))
												invalidIndex = k;
										}

										printf("\nMISMATCH AT %d\n\n", invalidIndex);
									}

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

									TraceEvent("ConsistencyCheck_DataInconsistent")
									    .detail(format("StorageServer%d", j).c_str(), storageServers[j].toString())
									    .detail(format("StorageServer%d", firstValidServer).c_str(),
									            storageServers[firstValidServer].toString())
									    .detail("ShardBegin", req.begin.getKey())
									    .detail("ShardEnd", req.end.getKey())
									    .detail("VersionNumber", req.version)
									    .detail(format("Server%dUniques", j).c_str(), currentUniques)
									    .detail(format("Server%dUniqueKey", j).c_str(), currentUniqueKey)
									    .detail(format("Server%dUniques", firstValidServer).c_str(), referenceUniques)
									    .detail(format("Server%dUniqueKey", firstValidServer).c_str(),
									            referenceUniqueKey)
									    .detail("ValueMismatches", valueMismatches)
									    .detail("ValueMismatchKey", valueMismatchKey)
									    .detail("MatchingKVPairs", matchingKVPairs)
									    .detail("IsTSS",
									            storageServerInterfaces[j].isTss() ||
									                    storageServerInterfaces[firstValidServer].isTss()
									                ? "True"
									                : "False");

									if ((g_network->isSimulated() &&
									     g_simulator->tssMode != ISimulator::TSSMode::EnabledDropMutations) ||
									    (!storageServerInterfaces[j].isTss() &&
									     !storageServerInterfaces[firstValidServer].isTss())) {
										testFailure("Data inconsistent", performQuiescentChecks, true);
										testResult = false;
									}
								}
							}
						}

						// If the data is not available and we aren't relocating this shard
						else if (!isRelocating) {
							Error e = rangeResult.isError() ? rangeResult.getError() : rangeResult.get().error.get();

							TraceEvent("ConsistencyCheck_StorageServerUnavailable")
							    .errorUnsuppressed(e)
							    .suppressFor(1.0)
							    .detail("StorageServer", storageServers[j])
							    .detail("ShardBegin", printable(range.begin))
							    .detail("ShardEnd", printable(range.end))
							    .detail("Address", storageServerInterfaces[j].address())
							    .detail("UID", storageServerInterfaces[j].id())
							    .detail("Quiesced", performQuiescentChecks)
							    .detail("GetKeyValuesToken",
							            storageServerInterfaces[j].getKeyValues.getEndpoint().token)
							    .detail("IsTSS", storageServerInterfaces[j].isTss() ? "True" : "False");

							if (e.code() == error_code_request_maybe_delivered) {
								// SS in the team may be removed and we get this error.
								return false;
							}
							// All shards should be available in quiscence
							if (performQuiescentChecks && !storageServerInterfaces[j].isTss()) {
								testFailure("Storage server unavailable", performQuiescentChecks, failureIsError);
								return false;
							}
						}
					}

					if (firstValidServer >= 0) {
						state VectorRef<KeyValueRef> data = keyValueFutures[firstValidServer].get().get().data;

						// Persist the last key of the range we just verified as the progressKey
						if (data.size()) {
							state Reference<ReadYourWritesTransaction> csInfoTr =
							    makeReference<ReadYourWritesTransaction>(cx);
							progressKey = data[data.size() - 1].key;
							loop {
								try {
									csInfoTr->reset();
									csInfoTr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

									state Optional<Value> val = wait(ConsistencyScanInfo::getInfo(csInfoTr));
									wait(csInfoTr->commit());
									if (val.present()) {
										ConsistencyScanInfo consistencyScanInfo =
										    ObjectReader::fromStringRef<ConsistencyScanInfo>(val.get(),
										                                                     IncludeVersion());
										consistencyScanInfo.progress_key = progressKey;
										csInfoTr->reset();
										csInfoTr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
										wait(ConsistencyScanInfo::setInfo(csInfoTr, consistencyScanInfo));
										wait(csInfoTr->commit());
									}
									break;
								} catch (Error& e) {
									wait(csInfoTr->onError(e));
								}
							}
						}

						// Calculate the size of the shard, the variance of the shard size estimate, and the correct
						// shard size estimate
						for (int k = 0; k < data.size(); k++) {
							ByteSampleInfo sampleInfo = isKeyValueInSample(data[k]);
							shardBytes += sampleInfo.size;
							double itemProbability = ((double)sampleInfo.size) / sampleInfo.sampledSize;
							if (itemProbability < 1)
								shardVariance +=
								    itemProbability * (1 - itemProbability) * pow((double)sampleInfo.sampledSize, 2);

							if (sampleInfo.inSample) {
								sampledBytes += sampleInfo.sampledSize;
								if (!canSplit && sampledBytes >= shardBounds.min.bytes &&
								    data[k].key.size() <= CLIENT_KNOBS->SPLIT_KEY_SIZE_LIMIT &&
								    sampledBytes <=
								        shardBounds.max.bytes * CLIENT_KNOBS->STORAGE_METRICS_UNFAIR_SPLIT_LIMIT / 2) {
									canSplit = true;
									splitBytes = sampledBytes;
								}

								/*TraceEvent("ConsistencyCheck_ByteSample").detail("ShardBegin", printable(range.begin)).detail("ShardEnd", printable(range.end))
								  .detail("SampledBytes", sampleInfo.sampledSize).detail("Key",
								  printable(data[k].key)).detail("KeySize", data[k].key.size()).detail("ValueSize",
								  data[k].value.size());*/

								// In data distribution, the splitting process ignores the first key in a shard.
								// Thus, we shouldn't consider it when validating the upper bound of estimated shard
								// sizes
								if (k == 0)
									firstKeySampledBytes += sampleInfo.sampledSize;

								sampledKeys++;
								if (itemProbability < 1) {
									sampledKeysWithProb++;
								}
							}
						}

						// Accumulate number of keys in this shard
						shardKeys += data.size();
					}
					// after requesting each shard, enforce rate limit based on how much data will likely be read
					if (rateLimitForThisRound > 0) {
						TraceEvent("ConsistencyCheck_RateLimit")
						    .detail("RateLimitForThisRound", rateLimitForThisRound)
						    .detail("TotalAmountRead", totalReadAmount);
						wait(rateLimiter->getAllowance(totalReadAmount));
						TraceEvent("ConsistencyCheck_AmountRead1").detail("TotalAmountRead", totalReadAmount);
						// Set ratelimit to max allowed if current round has been going on for a while
						if (now() - rateLimiterStartTime > 1.1 * targetInterval && rateLimitForThisRound != maxRate) {
							rateLimitForThisRound = maxRate;
							rateLimiter = Reference<IRateControl>(new SpeedLimit(rateLimitForThisRound, 1));
							rateLimiterStartTime = now();
							TraceEvent(SevInfo, "ConsistencyCheck_RateLimitSetMaxForThisRound")
							    .detail("RateLimit", rateLimitForThisRound);
						}
					}
					bytesReadInRange += totalReadAmount;
					bytesReadInthisRound += totalReadAmount;
					TraceEvent("ConsistencyCheck_BytesRead")
					    .detail("BytesReadInRange", bytesReadInRange)
					    .detail("BytesReadInthisRound", bytesReadInthisRound);

					// Advance to the next set of entries
					if (firstValidServer >= 0 && keyValueFutures[firstValidServer].get().get().more) {
						VectorRef<KeyValueRef> result = keyValueFutures[firstValidServer].get().get().data;
						ASSERT(result.size() > 0);
						begin = firstGreaterThan(result[result.size() - 1].key);
						ASSERT(begin.getKey() != allKeys.end);
						lastStartSampleKey = lastSampleKey;
					} else
						break;
				} catch (Error& e) {
					state Error err = e;
					wait(onErrorTr.onError(err));
					TraceEvent("ConsistencyCheck_RetryDataConsistency").error(err);
				}
			}

			canSplit = canSplit && sampledBytes - splitBytes >= shardBounds.min.bytes && sampledBytes > splitBytes;

			// Update the size of all storage servers containing this shard
			// This is only done in a non-distributed consistency check; the distributed check uses shard size
			// estimates
			if (!distributed)
				for (int j = 0; j < storageServers.size(); j++)
					storageServerSizes[storageServers[j]] += shardBytes;

			// If the storage servers' sampled estimate of shard size is different from ours
			if (performQuiescentChecks) {
				for (int j = 0; j < estimatedBytes.size(); j++) {
					if (estimatedBytes[j] >= 0 && estimatedBytes[j] != sampledBytes) {
						TraceEvent("ConsistencyCheck_IncorrectEstimate")
						    .detail("EstimatedBytes", estimatedBytes[j])
						    .detail("CorrectSampledBytes", sampledBytes)
						    .detail("StorageServer", storageServers[j])
						    .detail("IsTSS", storageServerInterfaces[j].isTss() ? "True" : "False");

						if (!storageServerInterfaces[j].isTss()) {
							testFailure("Storage servers had incorrect sampled estimate",
							            performQuiescentChecks,
							            failureIsError);
						}

						break;
					} else if (estimatedBytes[j] < 0 && ((g_network->isSimulated() &&
					                                      g_simulator->tssMode <= ISimulator::TSSMode::EnabledNormal) ||
					                                     !storageServerInterfaces[j].isTss())) {
						// Ignore a non-responding TSS outside of simulation, or if tss fault injection is enabled
						break;
					}
				}
			}

			// Compute the difference between the shard size estimate and its actual size.  If it is sufficiently
			// large, then fail
			double stdDev = sqrt(shardVariance);

			double failErrorNumStdDev = 7;
			int estimateError = abs(shardBytes - sampledBytes);

			// Only perform the check if there are sufficient keys to get a distribution that should resemble a
			// normal distribution
			if (sampledKeysWithProb > 30 && estimateError > failErrorNumStdDev * stdDev) {
				double numStdDev = estimateError / sqrt(shardVariance);
				TraceEvent("ConsistencyCheck_InaccurateShardEstimate")
				    .detail("Min", shardBounds.min.bytes)
				    .detail("Max", shardBounds.max.bytes)
				    .detail("Estimate", sampledBytes)
				    .detail("Actual", shardBytes)
				    .detail("NumStdDev", numStdDev)
				    .detail("Variance", shardVariance)
				    .detail("StdDev", stdDev)
				    .detail("ShardBegin", printable(range.begin))
				    .detail("ShardEnd", printable(range.end))
				    .detail("NumKeys", shardKeys)
				    .detail("NumSampledKeys", sampledKeys)
				    .detail("NumSampledKeysWithProb", sampledKeysWithProb);

				testFailure(format("Shard size is more than %f std dev from estimate", failErrorNumStdDev),
				            performQuiescentChecks,
				            failureIsError);
			}

			// In a quiescent database, check that the (estimated) size of the shard is within permitted bounds
			// Min and max shard sizes have a 3 * shardBounds.permittedError.bytes cushion for error since shard
			// sizes are not precise Shard splits ignore the first key in a shard, so its size shouldn't be
			// considered when checking the upper bound 0xff shards are not checked
			if (canSplit && sampledKeys > 5 && performQuiescentChecks && !range.begin.startsWith(keyServersPrefix) &&
			    (sampledBytes < shardBounds.min.bytes - 3 * shardBounds.permittedError.bytes ||
			     sampledBytes - firstKeySampledBytes > shardBounds.max.bytes + 3 * shardBounds.permittedError.bytes)) {
				TraceEvent("ConsistencyCheck_InvalidShardSize")
				    .detail("Min", shardBounds.min.bytes)
				    .detail("Max", shardBounds.max.bytes)
				    .detail("Size", shardBytes)
				    .detail("EstimatedSize", sampledBytes)
				    .detail("ShardBegin", printable(range.begin))
				    .detail("ShardEnd", printable(range.end))
				    .detail("ShardCount", ranges.size())
				    .detail("SampledKeys", sampledKeys);
				testFailure(format("Shard size in quiescent database is too %s",
				                   (sampledBytes < shardBounds.min.bytes) ? "small" : "large"),
				            performQuiescentChecks,
				            failureIsError);
				return false;
			}
		}

		if (bytesReadInRange > 0) {
			TraceEvent("ConsistencyCheck_ReadRange")
			    .suppressFor(1.0)
			    .detail("Range", range)
			    .detail("BytesRead", bytesReadInRange);
		}
	}

	*bytesReadInPrevRound = bytesReadInthisRound;
	return testResult;
}

ACTOR Future<Void> runDataValidationCheck(ConsistencyScanData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);
	state ConsistencyScanInfo csInfo = ConsistencyScanInfo();
	csInfo.consistency_scan_enabled = true;
	csInfo.restart = self->restart;
	csInfo.max_rate = self->maxRate;
	csInfo.target_interval = self->targetInterval;
	csInfo.last_round_start = StorageMetadataType::currentTime();
	try {
		// Get a list of key servers; verify that the TLogs and master all agree about who the key servers are
		state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServerPromise;
		state std::map<UID, StorageServerInterface> tssMapping;
		bool keyServerResult = wait(getKeyServers(self->db, keyServerPromise, keyServersKeys, false));
		if (keyServerResult) {
			state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers =
			    keyServerPromise.getFuture().get();

			// Get the locations of all the shards in the database
			state Promise<Standalone<VectorRef<KeyValueRef>>> keyLocationPromise;
			bool keyLocationResult = wait(getKeyLocations(self->db, keyServers, keyLocationPromise, false));
			if (keyLocationResult) {
				state Standalone<VectorRef<KeyValueRef>> keyLocations = keyLocationPromise.getFuture().get();

				// Check that each shard has the same data on all storage servers that it resides on
				wait(::success(checkDataConsistency(self->db,
				                                    keyLocations,
				                                    self->configuration,
				                                    tssMapping,
				                                    false /* quiescentCheck */,
				                                    false /* tssCheck */,
				                                    true /* firstClient */,
				                                    false /* failureIsError */,
				                                    0 /* clientId */,
				                                    1 /* clientCount */,
				                                    false /* distributed */,
				                                    false /* shuffleShards */,
				                                    1 /* shardSampleFactor */,
				                                    deterministicRandom()->randomInt64(0, 10000000),
				                                    self->finishedRounds /* repetitions */,
				                                    &(self->bytesReadInPrevRound),
				                                    self->restart,
				                                    self->maxRate,
				                                    self->targetInterval,
				                                    self->progressKey)));
			}
		}
	} catch (Error& e) {
		if (e.code() == error_code_transaction_too_old || e.code() == error_code_future_version ||
		    e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
		    e.code() == error_code_process_behind || e.code() == error_code_actor_cancelled)
			TraceEvent("ConsistencyScan_Retry").error(e); // FIXME: consistency check does not retry in this case
		else
			throw;
	}

	TraceEvent("ConsistencyScan_FinishedCheck");

	//  Update the ConsistencyScanInfo object and persist to the database
	csInfo.last_round_finish = StorageMetadataType::currentTime();
	csInfo.finished_rounds = self->finishedRounds + 1;
	auto duration = csInfo.last_round_finish - csInfo.last_round_start;
	csInfo.smoothed_round_duration.setTotal((double)duration);
	csInfo.progress_key = self->progressKey;
	csInfo.bytes_read_prev_round = self->bytesReadInPrevRound;
	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
			wait(ConsistencyScanInfo::setInfo(tr, csInfo));
			wait(tr->commit());
			break;
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}

	return Void();
}

ACTOR Future<Void> watchConsistencyScanInfoKey(ConsistencyScanData* self) {
	state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self->db);

	loop {
		try {
			tr->reset();
			tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);

			state Optional<Value> val = wait(ConsistencyScanInfo::getInfo(tr));
			if (val.present()) {
				ConsistencyScanInfo consistencyScanInfo =
				    ObjectReader::fromStringRef<ConsistencyScanInfo>(val.get(), IncludeVersion());
				self->restart = consistencyScanInfo.restart;
				self->maxRate = consistencyScanInfo.max_rate;
				self->targetInterval = consistencyScanInfo.target_interval;
				self->progressKey = consistencyScanInfo.progress_key;
				self->bytesReadInPrevRound = consistencyScanInfo.bytes_read_prev_round;
				self->finishedRounds = consistencyScanInfo.finished_rounds;
				self->consistencyScanEnabled.set(consistencyScanInfo.consistency_scan_enabled);
				//TraceEvent("ConsistencyScan_WatchGotVal", self->id)
				//  .detail("Enabled", consistencyScanInfo.consistency_scan_enabled)
				//  .detail("MaxRateRead", consistencyScanInfo.max_rate)
				//  .detail("MaxRateSelf", self->maxRate);
			}
			state Future<Void> watch = tr->watch(consistencyScanInfoKey);
			wait(tr->commit());
			wait(watch);
		} catch (Error& e) {
			wait(tr->onError(e));
		}
	}
}

ACTOR Future<Void> consistencyScan(ConsistencyScanInterface csInterf, Reference<AsyncVar<ServerDBInfo> const> dbInfo) {
	state ConsistencyScanData self(csInterf.id(),
	                               openDBOnServer(dbInfo, TaskPriority::DefaultEndpoint, LockAware::True));
	state Promise<Void> err;
	state Future<Void> collection = actorCollection(self.addActor.getFuture());
	state ConsistencyScanInfo csInfo = ConsistencyScanInfo();

	TraceEvent("ConsistencyScan_Starting", csInterf.id()).log();

	// Randomly enable consistencyScan in simulation
	if (g_network->isSimulated()) {
		if (deterministicRandom()->random01() < 0.5) {
			csInfo.consistency_scan_enabled = false;
		} else {
			csInfo.consistency_scan_enabled = true;
			csInfo.restart = false;
			csInfo.max_rate = 50e6;
			csInfo.target_interval = 24 * 7 * 60 * 60;
		}
		TraceEvent("SimulatedConsistencyScanConfigRandom")
		    .detail("ConsistencyScanEnabled", csInfo.consistency_scan_enabled)
		    .detail("MaxRate", csInfo.max_rate)
		    .detail("Interval", csInfo.target_interval);
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(self.db);
		loop {
			try {
				tr->reset();
				tr->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				wait(ConsistencyScanInfo::setInfo(tr, csInfo));
				wait(tr->commit());
				break;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	self.addActor.send(waitFailureServer(csInterf.waitFailure.getFuture()));
	self.addActor.send(traceRole(Role::CONSISTENCYSCAN, csInterf.id()));
	self.addActor.send(watchConsistencyScanInfoKey(&self));

	loop {
		if (self.consistencyScanEnabled.get()) {
			try {
				loop choose {
					when(wait(runDataValidationCheck(&self))) {
						TraceEvent("ConsistencyScan_Done", csInterf.id()).log();
						return Void();
					}
					when(HaltConsistencyScanRequest req = waitNext(csInterf.haltConsistencyScan.getFuture())) {
						req.reply.send(Void());
						TraceEvent("ConsistencyScan_Halted", csInterf.id()).detail("ReqID", req.requesterID);
						break;
					}
					when(wait(err.getFuture())) {}
					when(wait(collection)) {
						ASSERT(false);
						throw internal_error();
					}
				}
			} catch (Error& err) {
				if (err.code() == error_code_actor_cancelled) {
					TraceEvent("ConsistencyScan_ActorCanceled", csInterf.id()).errorUnsuppressed(err);
					return Void();
				}
				TraceEvent("ConsistencyScan_Died", csInterf.id()).errorUnsuppressed(err);
			}
		} else {
			TraceEvent("ConsistencyScan_WaitingForConfigChange", self.id).log();
			wait(self.consistencyScanEnabled.onChange());
		}
	}
}
