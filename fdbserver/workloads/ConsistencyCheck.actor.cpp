/*
 * ConsistencyCheck.actor.cpp
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
#include "fdbserver/StorageMetrics.actor.h"
#include "fdbserver/DataDistribution.actor.h"
#include "fdbserver/QuietDatabase.h"
#include "fdbserver/TSSMappingUtil.actor.h"
#include "flow/DeterministicRandom.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/StorageServerInterface.h"
#include "flow/network.h"
#include "fdbrpc/SimulatorProcessInfo.h"

#include "flow/actorcompiler.h" // This must be the last #include.

// #define SevCCheckInfo SevVerbose
#define SevCCheckInfo SevInfo

struct ConsistencyCheckWorkload : TestWorkload {
	struct OnTimeout {
		ConsistencyCheckWorkload& self;
		explicit OnTimeout(ConsistencyCheckWorkload& self) : self(self) {}
		void operator()(StringRef name, std::any const& msg, Error const& e) {
			TraceEvent(SevError, "ConsistencyCheckFailure")
			    .error(e)
			    .detail("EventName", name)
			    .detail("EventMessage", std::any_cast<StringRef>(msg))
			    .log();
		}
	};

	static constexpr auto NAME = "ConsistencyCheck";
	// Whether or not we should perform checks that will only pass if the database is in a quiescent state
	bool performQuiescentChecks;

	// Whether or not perform consistency check between storage cache servers and storage servers
	bool performCacheCheck;

	// Whether or not to perform consistency check between storage servers and pair TSS
	bool performTSSCheck;

	// How long to wait for the database to go quiet before failing (if doing quiescent checks)
	double quiescentWaitTimeout;

	// If true, then perform all checks on this client.  The first client is the only one to perform all of the fast
	// checks All other clients will perform slow checks if this test is distributed
	bool firstClient;

	// If true, then the expensive checks will be distributed to multiple clients
	bool distributed;

	// Determines how many shards are checked for consistency: out of every <shardSampleFactor> shards, 1 will be
	// checked
	int shardSampleFactor;

	// The previous data distribution mode
	int oldDataDistributionMode;

	// If true, then any failure of the consistency check will be logged as SevError.  Otherwise, it will be logged as
	// SevWarn
	bool failureIsError;

	// Max number of bytes per second to read from each storage server
	int rateLimitMax;

	// DataSet Size
	int64_t bytesReadInPreviousRound;

	// Randomize shard order with each iteration if true
	bool shuffleShards;

	bool success;

	// Number of times this client has run its portion of the consistency check
	int64_t repetitions;

	// Whether to continuously perform the consistency check
	bool indefinite;

	// Whether to suspendConsistencyCheck
	AsyncVar<bool> suspendConsistencyCheck;

	Future<Void> monitorConsistencyCheckSettingsActor;

	OnTimeout onTimeout;
	ProcessEvents::Event onTimeoutEvent;

	ConsistencyCheckWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), onTimeout(*this), onTimeoutEvent({ "Timeout"_sr, "TracedTooManyLines"_sr }, onTimeout) {
		performQuiescentChecks = getOption(options, "performQuiescentChecks"_sr, false);
		performCacheCheck = getOption(options, "performCacheCheck"_sr, false);
		performTSSCheck = getOption(options, "performTSSCheck"_sr, true);
		quiescentWaitTimeout = getOption(options, "quiescentWaitTimeout"_sr, 600.0);
		distributed = getOption(options, "distributed"_sr, true);
		shardSampleFactor = std::max(getOption(options, "shardSampleFactor"_sr, 1), 1);
		failureIsError = getOption(options, "failureIsError"_sr, false);
		rateLimitMax = getOption(options, "rateLimitMax"_sr, 0);
		shuffleShards = getOption(options, "shuffleShards"_sr, false);
		indefinite = getOption(options, "indefinite"_sr, false);
		suspendConsistencyCheck.set(true);

		success = true;

		firstClient = clientId == 0;

		repetitions = 0;
		bytesReadInPreviousRound = 0;
	}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	ACTOR Future<Void> _setup(Database cx, ConsistencyCheckWorkload* self) {
		// If performing quiescent checks, wait for the database to go quiet
		if (self->firstClient && self->performQuiescentChecks) {
			if (g_network->isSimulated()) {
				wait(timeKeeperSetDisable(cx));
			}

			try {
				wait(timeoutError(quietDatabase(cx, self->dbInfo, "ConsistencyCheckStart", 0, 1e5, 0, 0),
				                  self->quiescentWaitTimeout)); // FIXME: should be zero?
				if (g_network->isSimulated()) {
					g_simulator->quiesced = true;
					TraceEvent("ConsistencyCheckQuiesced").detail("Quiesced", g_simulator->quiesced);
				}
			} catch (Error& e) {
				TraceEvent("ConsistencyCheck_QuietDatabaseError").error(e);
				self->testFailure("Unable to achieve a quiet database");
				self->performQuiescentChecks = false;
			}
		}

		self->monitorConsistencyCheckSettingsActor = self->monitorConsistencyCheckSettings(cx, self);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		TraceEvent("ConsistencyCheck").log();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override { return success; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	void testFailure(std::string message, bool isError = false) {
		success = false;

		TraceEvent failEvent((failureIsError || isError) ? SevError : SevWarn, "TestFailure");
		if (performQuiescentChecks)
			failEvent.detail("Workload", "QuiescentCheck");
		else
			failEvent.detail("Workload", "ConsistencyCheck");

		failEvent.detail("Reason", "Consistency check: " + message);
	}

	ACTOR Future<Void> monitorConsistencyCheckSettings(Database cx, ConsistencyCheckWorkload* self) {
		loop {
			state ReadYourWritesTransaction tr(cx);
			try {
				tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
				tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				state Optional<Value> ccSuspendVal = wait(tr.get(fdbShouldConsistencyCheckBeSuspended));
				bool ccSuspend = ccSuspendVal.present()
				                     ? BinaryReader::fromStringRef<bool>(ccSuspendVal.get(), Unversioned())
				                     : false;
				self->suspendConsistencyCheck.set(ccSuspend);
				state Future<Void> watchCCSuspendFuture = tr.watch(fdbShouldConsistencyCheckBeSuspended);
				wait(tr.commit());
				wait(watchCCSuspendFuture);
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	ACTOR Future<Void> _start(Database cx, ConsistencyCheckWorkload* self) {
		loop {
			while (self->suspendConsistencyCheck.get()) {
				TraceEvent("ConsistencyCheck_Suspended").log();
				wait(self->suspendConsistencyCheck.onChange());
			}
			TraceEvent("ConsistencyCheck_StartingOrResuming").log();
			choose {
				when(wait(self->runCheck(cx, self))) {
					if (!self->indefinite)
						break;
					self->repetitions++;
					wait(delay(5.0));
				}
				when(wait(self->suspendConsistencyCheck.onChange())) {}
			}
		}
		if (self->firstClient && g_network->isSimulated() && self->performQuiescentChecks) {
			g_simulator->quiesced = false;
			TraceEvent("ConsistencyCheckQuiescedEnd").detail("Quiesced", g_simulator->quiesced);
		}
		return Void();
	}

	ACTOR Future<Void> runCheck(Database cx, ConsistencyCheckWorkload* self) {
		CODE_PROBE(self->performQuiescentChecks, "Quiescent consistency check");
		CODE_PROBE(!self->performQuiescentChecks, "Non-quiescent consistency check");
		state double consistenyCheckerBeginTime = now();

		if (self->firstClient || self->distributed) {
			try {
				state DatabaseConfiguration configuration;
				state std::map<UID, StorageServerInterface> tssMapping;

				state Transaction tr(cx);
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				loop {
					try {
						if (self->performTSSCheck) {
							tssMapping.clear();
							wait(readTSSMapping(&tr, &tssMapping));
						}
						RangeResult res = wait(tr.getRange(configKeys, 1000));
						if (res.size() == 1000) {
							TraceEvent("ConsistencyCheck_TooManyConfigOptions").log();
							self->testFailure("Read too many configuration options");
						}
						for (int i = 0; i < res.size(); i++)
							configuration.set(res[i].key, res[i].value);
						break;
					} catch (Error& e) {
						wait(tr.onError(e));
					}
				}

				// Perform quiescence-only checks
				if (self->firstClient && self->performQuiescentChecks) {
					// Check for undesirable servers (storage servers with exact same network address or using the wrong
					// key value store type)
					state bool hasUndesirableServers = wait(self->checkForUndesirableServers(cx, configuration, self));

					// Check that nothing is in-flight or in queue in data distribution
					int64_t inDataDistributionQueue = wait(getDataDistributionQueueSize(cx, self->dbInfo, true));
					if (inDataDistributionQueue > 0) {
						TraceEvent("ConsistencyCheck_NonZeroDataDistributionQueue")
						    .detail("QueueSize", inDataDistributionQueue);
						self->testFailure("Non-zero data distribution queue/in-flight size");
					}

					// Check that the number of process (and machine) teams is no larger than
					// the allowed maximum number of teams
					bool teamCollectionValid = wait(getTeamCollectionValid(cx, self->dbInfo));
					if (!teamCollectionValid) {
						TraceEvent(SevError, "ConsistencyCheck_TooManyTeams").log();
						self->testFailure("The number of process or machine teams is larger than the allowed maximum "
						                  "number of teams");
					}

					// Check that nothing is in the TLog queues
					std::pair<int64_t, int64_t> maxTLogQueueInfo = wait(getTLogQueueInfo(cx, self->dbInfo));
					if (maxTLogQueueInfo.first > 1e5) // FIXME: Should be zero?
					{
						TraceEvent("ConsistencyCheck_NonZeroTLogQueue").detail("MaxQueueSize", maxTLogQueueInfo.first);
						self->testFailure("Non-zero tlog queue size");
					}

					if (maxTLogQueueInfo.second > 30e6) {
						TraceEvent("ConsistencyCheck_PoppedVersionLag")
						    .detail("PoppedVersionLag", maxTLogQueueInfo.second);
						self->testFailure("large popped version lag");
					}

					// Check that nothing is in the storage server queues
					try {
						int64_t maxStorageServerQueueSize =
						    wait(getMaxStorageServerQueueSize(cx, self->dbInfo, invalidVersion));
						if (maxStorageServerQueueSize > 0) {
							TraceEvent("ConsistencyCheck_ExceedStorageServerQueueLimit")
							    .detail("MaxQueueSize", maxStorageServerQueueSize);
							self->testFailure("Storage server queue size exceeds limit");
						}
					} catch (Error& e) {
						if (e.code() == error_code_attribute_not_found) {
							TraceEvent("ConsistencyCheck_StorageQueueSizeError")
							    .error(e)
							    .detail("Reason", "Could not read queue size");

							// This error occurs if we have undesirable servers; in that case just report the
							// undesirable servers error
							if (!hasUndesirableServers)
								self->testFailure("Could not read storage queue size");
						} else
							throw;
					}

					wait(::success(self->checkForStorage(cx, configuration, tssMapping, self)));
					wait(::success(self->checkForExtraDataStores(cx, self)));
					wait(::success(self->checkStorageMetadata(cx, self)));

					// Check blob workers are operating as expected
					if (configuration.blobGranulesEnabled) {
						bool blobWorkersCorrect = wait(self->checkBlobWorkers(cx, configuration, self));
						if (!blobWorkersCorrect)
							self->testFailure("Blob workers incorrect");
					}

					// Check that each machine is operating as its desired class
					bool usingDesiredClasses = wait(self->checkUsingDesiredClasses(cx, self));
					if (!usingDesiredClasses)
						self->testFailure("Cluster has machine(s) not using requested classes");

					bool workerListCorrect = wait(self->checkWorkerList(cx, self));
					if (!workerListCorrect)
						self->testFailure("Worker list incorrect");

					bool coordinatorsCorrect = wait(self->checkCoordinators(cx));
					if (!coordinatorsCorrect)
						self->testFailure("Coordinators incorrect");

					bool consistencyScanStopped = wait(self->checkConsistencyScan(cx));
					if (!consistencyScanStopped)
						self->testFailure("Consistency scan active");

					// FIXME: re-enable this check!
					// bool singleSingletons = self->checkSingleSingletons(self, configuration);
					// if (!singleSingletons)
					// 	self->testFailure("Cluster has multiple instances of a singleton!");
				}

				// Get a list of key servers; verify that the TLogs and master all agree about who the key servers are
				state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServerPromise;
				bool keyServerResult = wait(getKeyServers(cx,
				                                          keyServerPromise,
				                                          keyServersKeys,
				                                          self->performQuiescentChecks,
				                                          self->failureIsError,
				                                          &self->success));
				if (keyServerResult) {
					state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> keyServers =
					    keyServerPromise.getFuture().get();

					// Get the locations of all the shards in the database
					state Promise<Standalone<VectorRef<KeyValueRef>>> keyLocationPromise;
					bool keyLocationResult = wait(getKeyLocations(
					    cx, keyServers, keyLocationPromise, self->performQuiescentChecks, &self->success));
					if (keyLocationResult) {
						state Standalone<VectorRef<KeyValueRef>> keyLocations = keyLocationPromise.getFuture().get();

						// Check that each shard has the same data on all storage servers that it resides on
						wait(checkDataConsistency(cx,
						                          keyLocations,
						                          configuration,
						                          tssMapping,
						                          self->performQuiescentChecks,
						                          self->performTSSCheck,
						                          self->firstClient,
						                          self->failureIsError,
						                          self->clientId,
						                          self->clientCount,
						                          self->distributed,
						                          self->shuffleShards,
						                          self->shardSampleFactor,
						                          self->sharedRandomNumber,
						                          self->repetitions,
						                          &(self->bytesReadInPreviousRound),
						                          true,
						                          self->rateLimitMax,
						                          CLIENT_KNOBS->CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME,
						                          &self->success));

						// Cache consistency check
						if (self->performCacheCheck) {
							wait(self->checkCacheConsistency(cx, keyLocations, self));
						}
					}
				}
			} catch (Error& e) {
				if (e.code() == error_code_transaction_too_old || e.code() == error_code_future_version ||
				    e.code() == error_code_wrong_shard_server || e.code() == error_code_all_alternatives_failed ||
				    e.code() == error_code_process_behind || e.code() == error_code_actor_cancelled) {
					TraceEvent("ConsistencyCheck_Retry")
					    .error(e); // FIXME: consistency check does not retry in this case
				} else {
					self->testFailure(format("Error %d - %s", e.code(), e.name()));
				}
			}
		}

		TraceEvent("ConsistencyCheck_FinishedCheck")
		    .detail("Repetitions", self->repetitions)
		    .detail("TimeSpan", now() - consistenyCheckerBeginTime);

		return Void();
	}

	// Check the data consistency between storage cache servers and storage servers
	// keyLocations: all key/value pairs persisted in the database, reused from previous consistency check on all
	// storage servers
	ACTOR Future<Void> checkCacheConsistency(Database cx,
	                                         VectorRef<KeyValueRef> keyLocations,
	                                         ConsistencyCheckWorkload* self) {
		state Promise<Standalone<VectorRef<KeyValueRef>>> cacheKeyPromise;
		state Promise<Standalone<VectorRef<KeyValueRef>>> cacheServerKeyPromise;
		state Promise<Standalone<VectorRef<KeyValueRef>>> serverListKeyPromise;
		state Promise<Standalone<VectorRef<KeyValueRef>>> serverTagKeyPromise;
		state Standalone<VectorRef<KeyValueRef>> cacheKey; // "\xff/storageCache/[[begin]]" := "[[vector<uint16_t>]]"
		state Standalone<VectorRef<KeyValueRef>>
		    cacheServer; // "\xff/storageCacheServer/[[UID]] := StorageServerInterface"
		state Standalone<VectorRef<KeyValueRef>>
		    serverList; // "\xff/serverList/[[serverID]]" := "[[StorageServerInterface]]"
		state Standalone<VectorRef<KeyValueRef>> serverTag; // "\xff/serverTag/[[serverID]]" = "[[Tag]]"

		std::vector<Future<bool>> cacheResultsPromise;
		cacheResultsPromise.push_back(self->fetchKeyValuesFromSS(cx, self, storageCacheKeys, cacheKeyPromise, true));
		cacheResultsPromise.push_back(
		    self->fetchKeyValuesFromSS(cx, self, storageCacheServerKeys, cacheServerKeyPromise, false));
		cacheResultsPromise.push_back(
		    self->fetchKeyValuesFromSS(cx, self, serverListKeys, serverListKeyPromise, false));
		cacheResultsPromise.push_back(self->fetchKeyValuesFromSS(cx, self, serverTagKeys, serverTagKeyPromise, false));
		std::vector<bool> cacheResults = wait(getAll(cacheResultsPromise));
		if (std::all_of(cacheResults.begin(), cacheResults.end(), [](bool success) { return success; })) {
			cacheKey = cacheKeyPromise.getFuture().get();
			cacheServer = cacheServerKeyPromise.getFuture().get();
			serverList = serverListKeyPromise.getFuture().get();
			serverTag = serverTagKeyPromise.getFuture().get();
		} else {
			TraceEvent(SevDebug, "CheckCacheConsistencyFailed")
			    .detail("CacheKey", boost::lexical_cast<std::string>(cacheResults[0]))
			    .detail("CacheServerKey", boost::lexical_cast<std::string>(cacheResults[1]))
			    .detail("ServerListKey", boost::lexical_cast<std::string>(cacheResults[2]))
			    .detail("ServerTagKey", boost::lexical_cast<std::string>(cacheResults[3]));
			self->success = false;
			return Void();
		}

		state int rateLimitForThisRound =
		    self->bytesReadInPreviousRound == 0
		        ? self->rateLimitMax
		        : std::min(
		              self->rateLimitMax,
		              static_cast<int>(ceil(self->bytesReadInPreviousRound /
		                                    (float)CLIENT_KNOBS->CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME)));
		ASSERT(rateLimitForThisRound >= 0 && rateLimitForThisRound <= self->rateLimitMax);
		TraceEvent("CacheConsistencyCheck_RateLimitForThisRound").detail("RateLimit", rateLimitForThisRound);
		state Reference<IRateControl> rateLimiter = Reference<IRateControl>(new SpeedLimit(rateLimitForThisRound, 1));
		state double rateLimiterStartTime = now();
		state int bytesReadInRange = 0;

		// Get all cache storage servers' interfaces
		// Note: currently, all storage cache servers cache the same data
		// Thus, no need to differentiate them for now
		state std::vector<StorageServerInterface> cacheServerInterfaces;
		for (const auto& kv : cacheServer) {
			StorageServerInterface cacheServer = decodeServerListValue(kv.value);
			// Uniqueness
			ASSERT(std::find(cacheServerInterfaces.begin(), cacheServerInterfaces.end(), cacheServer) ==
			       cacheServerInterfaces.end());
			cacheServerInterfaces.push_back(cacheServer);
		}
		TraceEvent(SevDebug, "CheckCacheConsistencyCacheServers")
		    .detail("CacheSSInterfaces", describe(cacheServerInterfaces));
		// Construct a key range map where the value for each range,
		// if the range is cached, then a list of cache server interfaces plus one storage server interfaces(randomly
		// pick) if not cached, empty
		state KeyRangeMap<std::vector<StorageServerInterface>> cachedKeysLocationMap;
		// First, for any range is cached, update the list to have all cache storage interfaces
		for (int k = 0; k < cacheKey.size(); k++) {
			std::vector<uint16_t> serverIndices;
			decodeStorageCacheValue(cacheKey[k].value, serverIndices);
			// non-empty means this is the start of a cached range
			if (serverIndices.size()) {
				KeyRangeRef range(cacheKey[k].key, (k < cacheKey.size() - 1) ? cacheKey[k + 1].key : allKeys.end);
				cachedKeysLocationMap.insert(range, cacheServerInterfaces);
				TraceEvent(SevDebug, "CheckCacheConsistency").detail("CachedRange", range).detail("Index", k);
			}
		}
		// Second, insert corresponding storage servers into the list
		// Here we need to construct a UID2SS map
		state std::map<UID, StorageServerInterface> UIDtoSSMap;
		for (const auto& kv : serverList) {
			UID serverId = decodeServerListKey(kv.key);
			UIDtoSSMap[serverId] = decodeServerListValue(kv.value);
			TraceEvent(SevDebug, "CheckCacheConsistencyStorageServer").detail("UID", serverId);
		}
		// Now, for each shard, check if it is cached,
		// if cached, add storage servers that persist the data into the list
		for (int k = 0; k < keyLocations.size() - 1; k++) {
			KeyRangeRef range(keyLocations[k].key, keyLocations[k + 1].key);
			std::vector<UID> sourceStorageServers;
			std::vector<UID> destStorageServers;
			decodeKeyServersValue(RangeResultRef(serverTag, false),
			                      keyLocations[k].value,
			                      sourceStorageServers,
			                      destStorageServers,
			                      false);
			bool isRelocating = destStorageServers.size() > 0;
			std::vector<UID> storageServers = (isRelocating) ? destStorageServers : sourceStorageServers;
			std::vector<StorageServerInterface> storageServerInterfaces;
			for (const auto& UID : storageServers) {
				storageServerInterfaces.push_back(UIDtoSSMap[UID]);
			}
			std::vector<StorageServerInterface> allSS(cacheServerInterfaces);
			allSS.insert(allSS.end(), storageServerInterfaces.begin(), storageServerInterfaces.end());

			// The shard may overlap with several cached ranges
			// only insert the storage server interface on cached ranges

			// Since range.end is next range.begin, and both begins are Key(), so we always hold this condition
			// Note: both ends are allKeys.end
			auto begin_iter = cachedKeysLocationMap.rangeContaining(range.begin);
			ASSERT(begin_iter->begin() == range.begin);
			// Split the range to maintain the condition
			auto end_iter = cachedKeysLocationMap.rangeContaining(range.end);
			if (end_iter->begin() != range.end) {
				cachedKeysLocationMap.insert(KeyRangeRef(end_iter->begin(), range.end), end_iter->value());
			}
			for (auto iter = cachedKeysLocationMap.rangeContaining(range.begin);
			     iter != cachedKeysLocationMap.rangeContaining(range.end);
			     ++iter) {
				if (iter->value().size()) {
					// randomly pick one for check since the data are guaranteed to be consistent on any SS
					iter->value().push_back(deterministicRandom()->randomChoice(storageServerInterfaces));
				}
			}
		}

		// Once having the KeyRangeMap, iterate all cached ranges and verify consistency
		state RangeMap<Key, std::vector<StorageServerInterface>, KeyRangeRef>::Ranges iter_ranges =
		    cachedKeysLocationMap.containedRanges(allKeys);
		state RangeMap<Key, std::vector<StorageServerInterface>, KeyRangeRef>::iterator iter = iter_ranges.begin();
		state std::vector<StorageServerInterface> iter_ss;
		state int effectiveClientCount = (self->distributed) ? self->clientCount : 1;
		state int increment = self->distributed ? effectiveClientCount * self->shardSampleFactor : 1;
		state int shard = 0; // index used for splitting work on different clients
		// move the index to the first responsible cached range
		while (shard < self->clientId * self->shardSampleFactor && iter != iter_ranges.end()) {
			if (iter->value().empty()) {
				++iter;
				continue;
			}
			++iter;
			++shard;
		}
		for (; iter != iter_ranges.end(); ++iter) {
			iter_ss = iter->value();
			if (iter_ss.empty())
				continue;
			if (shard % increment != (self->clientId * self->shardSampleFactor) % increment) {
				++shard;
				continue;
			}
			// TODO: in the future, run a check based on estimated data size like the existing storage servers'
			// consistency check on the first client
			state Key lastSampleKey;
			state Key lastStartSampleKey;
			state int64_t totalReadAmount = 0;

			state KeySelector begin = firstGreaterOrEqual(iter->begin());
			state Transaction onErrorTr(cx); // This transaction exists only to access onError and its backoff behavior

			// Read a limited number of entries at a time, repeating until all keys in the shard have been read
			loop {
				try {
					lastSampleKey = lastStartSampleKey;

					// Get the min version of the storage servers
					Version version = wait(getVersion(cx));

					state GetKeyValuesRequest req;
					req.begin = begin;
					req.end = firstGreaterOrEqual(iter->end());
					req.limit = 1e4;
					req.limitBytes = CLIENT_KNOBS->REPLY_BYTE_LIMIT;
					req.version = version;
					req.tags = TagSet();
					req.options = ReadOptions(debugRandom()->randomUniqueID());
					DisabledTraceEvent("CCD", req.options.get().debugID.get()).detail("Version", version);

					// Try getting the entries in the specified range
					state std::vector<Future<ErrorOr<GetKeyValuesReply>>> keyValueFutures;
					state int j = 0;
					for (j = 0; j < iter_ss.size(); j++) {
						resetReply(req);
						if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
							cx->getLatestCommitVersion(iter_ss[j], req.version, req.ssLatestCommitVersions);
						}
						keyValueFutures.push_back(iter_ss[j].getKeyValues.getReplyUnlessFailedFor(req, 2, 0));
					}

					wait(waitForAll(keyValueFutures));
					TraceEvent(SevDebug, "CheckCacheConsistencyComparison")
					    .detail("Begin", req.begin)
					    .detail("End", req.end)
					    .detail("SSInterfaces", describe(iter_ss));

					// Read the resulting entries
					state int firstValidServer = -1;
					totalReadAmount = 0;
					for (j = 0; j < keyValueFutures.size(); j++) {
						ErrorOr<GetKeyValuesReply> rangeResult = keyValueFutures[j].get();
						// if (rangeResult.isError()) {
						//	throw rangeResult.getError();
						// }

						// Compare the results with other storage servers
						if (rangeResult.present() && !rangeResult.get().error.present()) {
							state GetKeyValuesReply current = rangeResult.get();
							totalReadAmount += current.data.expectedSize();
							TraceEvent(SevDebug, "CheckCacheConsistencyResult")
							    .detail("SSInterface", iter_ss[j].uniqueID);
							// If we haven't encountered a valid storage server yet, then mark this as the baseline
							// to compare against
							if (firstValidServer == -1)
								firstValidServer = j;

							// Compare this shard against the first
							else {
								GetKeyValuesReply reference = keyValueFutures[firstValidServer].get().get();

								if (current.data != reference.data || current.more != reference.more) {
									// Be especially verbose if in simulation
									if (g_network->isSimulated()) {
										int invalidIndex = -1;
										printf("\nSERVER %d (%s); shard = %s - %s:\n",
										       j,
										       iter_ss[j].address().toString().c_str(),
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

										printf("\nSERVER %d (%s); shard = %s - %s:\n",
										       firstValidServer,
										       iter_ss[firstValidServer].address().toString().c_str(),
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

									// Loop indexes
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

									TraceEvent("CacheConsistencyCheck_DataInconsistent")
									    .detail(format("StorageServer%d", j).c_str(), iter_ss[j].toString())
									    .detail(format("StorageServer%d", firstValidServer).c_str(),
									            iter_ss[firstValidServer].toString())
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
									    .detail("MatchingKVPairs", matchingKVPairs);

									self->testFailure("Data inconsistent", true);
								}
							}
						}
					}

					// after requesting each shard, enforce rate limit based on how much data will likely be read
					if (rateLimitForThisRound > 0) {
						wait(rateLimiter->getAllowance(totalReadAmount));
						// Set ratelimit to max allowed if current round has been going on for a while
						if (now() - rateLimiterStartTime >
						        1.1 * CLIENT_KNOBS->CONSISTENCY_CHECK_ONE_ROUND_TARGET_COMPLETION_TIME &&
						    rateLimitForThisRound != self->rateLimitMax) {
							rateLimitForThisRound = self->rateLimitMax;
							rateLimiter = Reference<IRateControl>(new SpeedLimit(rateLimitForThisRound, 1));
							rateLimiterStartTime = now();
							TraceEvent(SevInfo, "CacheConsistencyCheck_RateLimitSetMaxForThisRound")
							    .detail("RateLimit", rateLimitForThisRound);
						}
					}
					bytesReadInRange += totalReadAmount;

					// Advance to the next set of entries
					if (firstValidServer >= 0 && keyValueFutures[firstValidServer].get().get().more) {
						VectorRef<KeyValueRef> result = keyValueFutures[firstValidServer].get().get().data;
						ASSERT(result.size() > 0);
						begin = firstGreaterThan(result[result.size() - 1].key);
						ASSERT(begin.getKey() != allKeys.end);
						lastStartSampleKey = lastSampleKey;
						TraceEvent(SevDebug, "CacheConsistencyCheckNextBeginKey").detail("Key", begin);
					} else
						break;
				} catch (Error& e) {
					state Error err = e;
					wait(onErrorTr.onError(err));
					TraceEvent("CacheConsistencyCheck_RetryDataConsistency").error(err);
				}
			}

			if (bytesReadInRange > 0) {
				TraceEvent("CacheConsistencyCheck_ReadRange")
				    .suppressFor(1.0)
				    .detail("Range", iter->range())
				    .detail("BytesRead", bytesReadInRange);
			}
		}
		return Void();
	}

	// Directly fetch key/values from storage servers through GetKeyValuesRequest
	// In particular, avoid transaction-based read which may read data from storage cache servers
	// range: all key/values in the range will be fetched
	// removePrefix: if true, remove the prefix of the range, e.g. \xff/storageCacheServer/
	ACTOR Future<bool> fetchKeyValuesFromSS(Database cx,
	                                        ConsistencyCheckWorkload* self,
	                                        KeyRangeRef range,
	                                        Promise<Standalone<VectorRef<KeyValueRef>>> resultPromise,
	                                        bool removePrefix) {
		// get shards paired with corresponding storage servers
		state Promise<std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>>> keyServerPromise;
		bool keyServerResult = wait(getKeyServers(
		    cx, keyServerPromise, range, self->performQuiescentChecks, self->failureIsError, &self->success));
		if (!keyServerResult)
			return false;
		state std::vector<std::pair<KeyRange, std::vector<StorageServerInterface>>> shards =
		    keyServerPromise.getFuture().get();

		// key/value pairs in the given range
		state Standalone<VectorRef<KeyValueRef>> result;
		state Key beginKey = allKeys.begin.withPrefix(range.begin);
		state Key endKey = allKeys.end.withPrefix(range.begin);
		state int i; // index
		state Transaction onErrorTr(cx); // This transaction exists only to access onError and its backoff behavior

		// If the responses are too big, we may use multiple requests to get the key locations.  Each request begins
		// where the last left off
		for (i = 0; i < shards.size(); i++) {
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

					// Fetch key/values from storage servers
					// Here we read from all storage servers and make sure results are consistent
					// Note: this maybe duplicate but to make sure all storage servers available in a quiescent database
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
							// If the storage server didn't reply in a quiescent database, then the check fails
							if (self->performQuiescentChecks) {
								TraceEvent("CacheConsistencyCheck_KeyServerUnavailable")
								    .detail("StorageServer", shards[i].second[j].id().toString().c_str());
								self->testFailure("Key server unavailable");
								return false;
							}

							// If no storage servers replied, then throw all_alternatives_failed to force a retry
							else if (firstValidStorageServer < 0 && j == keyValueFutures.size() - 1)
								throw all_alternatives_failed();
						}

						// If this is the first storage server, store the locations to send back to the caller
						else if (firstValidStorageServer < 0) {
							firstValidStorageServer = j;

							// Otherwise, compare the data to the results from the first storage server.  If they are
							// different, then the check fails
						} else if (reply.get().data != keyValueFutures[firstValidStorageServer].get().get().data ||
						           reply.get().more != keyValueFutures[firstValidStorageServer].get().get().more) {
							TraceEvent("CacheConsistencyCheck_InconsistentKeyServers")
							    .detail("StorageServer1", shards[i].second[firstValidStorageServer].id())
							    .detail("StorageServer2", shards[i].second[j].id());
							self->testFailure("Key servers inconsistent", true);
							return false;
						}
					}

					auto keyValueResponse = keyValueFutures[firstValidStorageServer].get().get();

					for (const auto& kv : keyValueResponse.data) {
						result.push_back_deep(
						    result.arena(),
						    KeyValueRef(removePrefix ? kv.key.removePrefix(range.begin) : kv.key, kv.value));
					}

					// Next iteration should pick up where we left off
					ASSERT(result.size() >= 1);
					if (!keyValueResponse.more) {
						beginKey = shards[i].first.end;
					} else {
						beginKey = keyAfter(keyValueResponse.data.end()[-1].key);
					}
				} catch (Error& e) {
					state Error err = e;
					wait(onErrorTr.onError(err));
					TraceEvent("CacheConsistencyCheck_RetryGetKeyLocations").error(err);
				}
			}
		}

		resultPromise.send(result);
		return true;
	}

	// Comparison function used to compare map elements by value
	template <class K, class T>
	static bool compareByValue(std::pair<K, T> a, std::pair<K, T> b) {
		return a.second < b.second;
	}

	// Returns true if any storage servers have the exact same network address or are not using the correct key value
	// store type
	ACTOR Future<bool> checkForUndesirableServers(Database cx,
	                                              DatabaseConfiguration configuration,
	                                              ConsistencyCheckWorkload* self) {
		state int i;
		state int j;
		state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));
		state std::string wiggleLocalityKeyValue = configuration.perpetualStorageWiggleLocality;
		state std::vector<std::pair<Optional<Value>, Optional<Value>>> wiggleLocalityKeyValues =
		    ParsePerpetualStorageWiggleLocality(configuration.perpetualStorageWiggleLocality);

		// Check each pair of storage servers for an address match
		for (i = 0; i < storageServers.size(); i++) {
			// Check that each storage server has the correct key value store type
			ReplyPromise<KeyValueStoreType> typeReply;
			ErrorOr<KeyValueStoreType> keyValueStoreType =
			    wait(storageServers[i].getKeyValueStoreType.getReplyUnlessFailedFor(typeReply, 2, 0));

			if (!keyValueStoreType.present()) {
				TraceEvent("ConsistencyCheck_ServerUnavailable").detail("ServerID", storageServers[i].id());
				self->testFailure("Storage server unavailable");
			} else if (configuration.perpetualStoreType.isValid()) {
				// Perpetual storage wiggle is used to migrate storage. Check that the matched storage servers are
				// correctly migrated.
				if (wiggleLocalityKeyValue == "0" ||
				    localityMatchInList(wiggleLocalityKeyValues, storageServers[i].locality)) {
					if (keyValueStoreType.get() != configuration.perpetualStoreType) {
						TraceEvent("ConsistencyCheck_WrongKeyValueStoreType")
						    .detail("ServerID", storageServers[i].id())
						    .detail("StoreType", keyValueStoreType.get().toString())
						    .detail("DesiredType", configuration.perpetualStoreType.toString())
						    .detail("IsPerpetualStoreType", true);
						self->testFailure("Storage server has wrong key-value store type");
						return true;
					}
				} else if ((!storageServers[i].isTss() &&
				            keyValueStoreType.get() != configuration.storageServerStoreType) ||
				           (storageServers[i].isTss() &&
				            keyValueStoreType.get() != configuration.testingStorageServerStoreType)) {
					TraceEvent("ConsistencyCheck_WrongKeyValueStoreType")
					    .detail("ServerID", storageServers[i].id())
					    .detail("StoreType", keyValueStoreType.get().toString())
					    .detail("DesiredType", configuration.perpetualStoreType.toString())
					    .detail("IsPerpetualStoreType", false);
					self->testFailure("Storage server has wrong key-value store type");
					return true;
				}
			} else if (((!storageServers[i].isTss() &&
			             keyValueStoreType.get() != configuration.storageServerStoreType) ||
			            (storageServers[i].isTss() &&
			             keyValueStoreType.get() != configuration.testingStorageServerStoreType)) &&
			           (wiggleLocalityKeyValue == "0" ||
			            localityMatchInList(wiggleLocalityKeyValues, storageServers[i].locality))) {
				TraceEvent("ConsistencyCheck_WrongKeyValueStoreType")
				    .detail("ServerID", storageServers[i].id())
				    .detail("StoreType", keyValueStoreType.get().toString())
				    .detail("DesiredType", configuration.storageServerStoreType.toString())
				    .detail("IsPerpetualStoreType", false);
				self->testFailure("Storage server has wrong key-value store type");
				return true;
			}

			// Check each pair of storage servers for an address match
			for (j = i + 1; j < storageServers.size(); j++) {
				if (storageServers[i].address() == storageServers[j].address()) {
					TraceEvent("ConsistencyCheck_UndesirableServer")
					    .detail("StorageServer1", storageServers[i].id())
					    .detail("StorageServer2", storageServers[j].id())
					    .detail("Address", storageServers[i].address());
					self->testFailure("Multiple storage servers have the same address");
					return true;
				}
			}
		}

		return false;
	}

	// Every storage server should have it metadata populated and no metadata leak when the database reach the quiescent
	// state
	ACTOR Future<bool> checkStorageMetadata(Database cx, ConsistencyCheckWorkload* self) {
		state KeyBackedObjectMap<UID, StorageMetadataType, decltype(IncludeVersion())> metadataMap(
		    serverMetadataKeys.begin, IncludeVersion());
		state std::vector<StorageServerInterface> servers;
		state std::unordered_map<UID, StorageMetadataType> id_ssi;
		state Transaction tr(cx);
		loop {
			servers.clear();
			id_ssi.clear();
			tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
			tr.setOption(FDBTransactionOptions::LOCK_AWARE);
			try {
				state KeyBackedRangeResult<std::pair<UID, StorageMetadataType>> metadata =
				    wait(metadataMap.getRange(&tr, {}, {}, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!metadata.more && metadata.results.size() < CLIENT_KNOBS->TOO_MANY);
				RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
				ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT_EQ(metadata.results.size(), serverList.size());
				id_ssi = std::unordered_map<UID, StorageMetadataType>(metadata.results.begin(), metadata.results.end());
				servers.reserve(serverList.size());
				for (int i = 0; i < serverList.size(); i++)
					servers.push_back(decodeServerListValue(serverList[i].value));
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}

		for (auto& ssi : servers) {
			ASSERT(id_ssi.count(ssi.id()));
		}
		return true;
	}

	// Returns false if any worker that should have a storage server does not have one
	ACTOR Future<bool> checkForStorage(Database cx,
	                                   DatabaseConfiguration configuration,
	                                   std::map<UID, StorageServerInterface> tssMapping,
	                                   ConsistencyCheckWorkload* self) {
		state std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
		state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));
		std::vector<Optional<Key>> missingStorage; // vector instead of a set to get the count

		for (int i = 0; i < workers.size(); i++) {
			NetworkAddress addr = workers[i].interf.stableAddress();
			if (!configuration.isExcludedServer(workers[i].interf.addresses(), workers[i].interf.locality) &&
			    (workers[i].processClass == ProcessClass::StorageClass ||
			     workers[i].processClass == ProcessClass::UnsetClass)) {
				bool found = false;
				for (int j = 0; j < storageServers.size(); j++) {
					if (storageServers[j].stableAddress() == addr) {
						found = true;
						break;
					}
				}
				if (!found) {
					TraceEvent("ConsistencyCheck_NoStorage")
					    .detail("Address", addr)
					    .detail("ProcessId", workers[i].interf.locality.processId())
					    .detail("ProcessClassEqualToStorageClass",
					            (int)(workers[i].processClass == ProcessClass::StorageClass));
					missingStorage.push_back(workers[i].interf.locality.dcId());
				}
			}
		}

		int missingDc0 = configuration.regions.size() == 0
		                     ? 0
		                     : std::count(missingStorage.begin(), missingStorage.end(), configuration.regions[0].dcId);
		int missingDc1 = configuration.regions.size() < 2
		                     ? 0
		                     : std::count(missingStorage.begin(), missingStorage.end(), configuration.regions[1].dcId);

		if ((configuration.regions.size() == 0 && missingStorage.size()) ||
		    (configuration.regions.size() == 1 && missingDc0) ||
		    (configuration.regions.size() == 2 && configuration.usableRegions == 1 && missingDc0 && missingDc1) ||
		    (configuration.regions.size() == 2 && configuration.usableRegions > 1 && (missingDc0 || missingDc1))) {

			// TODO could improve this check by also ensuring DD is currently recruiting a TSS by using quietdb?
			bool couldExpectMissingTss = (configuration.desiredTSSCount - tssMapping.size()) > 0;

			int countMissing = missingStorage.size();
			int acceptableTssMissing = 1;
			if (configuration.regions.size() == 1) {
				countMissing = missingDc0;
			} else if (configuration.regions.size() == 2) {
				if (configuration.usableRegions == 1) {
					// all processes should be missing from 1, so take the number missing from the other
					countMissing = std::min(missingDc0, missingDc1);
				} else if (configuration.usableRegions == 2) {
					countMissing = missingDc0 + missingDc1;
					acceptableTssMissing = 2;
				} else {
					ASSERT(false); // in case fdb ever adds 3+ region support?
				}
			}

			if (!couldExpectMissingTss || countMissing > acceptableTssMissing) {
				self->testFailure("No storage server on worker");
				return false;
			} else {
				TraceEvent(SevWarn, "ConsistencyCheck_TSSMissing").log();
			}
		}

		return true;
	}

	ACTOR Future<bool> checkForExtraDataStores(Database cx, ConsistencyCheckWorkload* self) {
		state std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
		state std::vector<StorageServerInterface> storageServers = wait(getStorageServers(cx));
		state std::vector<WorkerInterface> coordWorkers = wait(getCoordWorkers(cx, self->dbInfo));
		auto& db = self->dbInfo->get();
		state std::vector<TLogInterface> logs = db.logSystemConfig.allPresentLogs();

		state std::vector<WorkerDetails>::iterator itr;
		state bool foundExtraDataStore = false;
		state std::vector<struct ProcessInfo*> protectedProcessesToKill;

		state std::map<NetworkAddress, std::set<UID>> statefulProcesses;
		for (const auto& ss : storageServers) {
			statefulProcesses[ss.address()].insert(ss.id());
			// A process may have two addresses (same ip, different ports)
			if (ss.secondaryAddress().present()) {
				statefulProcesses[ss.secondaryAddress().get()].insert(ss.id());
			}
			TraceEvent(SevCCheckInfo, "StatefulProcess")
			    .detail("StorageServer", ss.id())
			    .detail("PrimaryAddress", ss.address().toString())
			    .detail("SecondaryAddress",
			            ss.secondaryAddress().present() ? ss.secondaryAddress().get().toString() : "Unset");
		}
		for (const auto& log : logs) {
			statefulProcesses[log.address()].insert(log.id());
			if (log.secondaryAddress().present()) {
				statefulProcesses[log.secondaryAddress().get()].insert(log.id());
			}
			TraceEvent(SevCCheckInfo, "StatefulProcess")
			    .detail("Log", log.id())
			    .detail("PrimaryAddress", log.address().toString())
			    .detail("SecondaryAddress",
			            log.secondaryAddress().present() ? log.secondaryAddress().get().toString() : "Unset");
		}
		// Coordinators are also stateful processes
		for (const auto& cWorker : coordWorkers) {
			statefulProcesses[cWorker.address()].insert(cWorker.id());
			if (cWorker.secondaryAddress().present()) {
				statefulProcesses[cWorker.secondaryAddress().get()].insert(cWorker.id());
			}
			TraceEvent(SevCCheckInfo, "StatefulProcess")
			    .detail("Coordinator", cWorker.id())
			    .detail("PrimaryAddress", cWorker.address().toString())
			    .detail("SecondaryAddress",
			            cWorker.secondaryAddress().present() ? cWorker.secondaryAddress().get().toString() : "Unset");
		}

		for (itr = workers.begin(); itr != workers.end(); ++itr) {
			ErrorOr<Standalone<VectorRef<UID>>> stores =
			    wait(itr->interf.diskStoreRequest.getReplyUnlessFailedFor(DiskStoreRequest(false), 2, 0));
			if (stores.isError()) {
				TraceEvent("ConsistencyCheck_GetDataStoreFailure")
				    .error(stores.getError())
				    .detail("Address", itr->interf.address());
				self->testFailure("Failed to get data stores");
				return false;
			}

			TraceEvent(SevCCheckInfo, "ConsistencyCheck_ExtraDataStore")
			    .detail("Worker", itr->interf.id().toString())
			    .detail("PrimaryAddress", itr->interf.address().toString())
			    .detail("SecondaryAddress",
			            itr->interf.secondaryAddress().present() ? itr->interf.secondaryAddress().get().toString()
			                                                     : "Unset");
			for (const auto& id : stores.get()) {
				if (statefulProcesses[itr->interf.address()].count(id)) {
					continue;
				}
				// For extra data store
				TraceEvent("ConsistencyCheck_ExtraDataStore")
				    .detail("Address", itr->interf.address())
				    .detail("DataStoreID", id);
				if (g_network->isSimulated()) {
					// FIXME: this is hiding the fact that we can recruit a new storage server on a location the has
					// files left behind by a previous failure
					// this means that the process is wasting disk space until the process is rebooting
					ISimulator::ProcessInfo* p = g_simulator->getProcessByAddress(itr->interf.address());
					// Note: itr->interf.address() may not equal to p->address() because role's endpoint's primary
					// addr can be swapped by choosePrimaryAddress() based on its peer's tls config.
					TraceEvent("ConsistencyCheck_RebootProcess")
					    .detail("Address",
					            itr->interf.address()) // worker's primary address (i.e., the first address)
					    .detail("ProcessPrimaryAddress", p->address)
					    .detail("ProcessAddresses", p->addresses.toString())
					    .detail("DataStoreID", id)
					    .detail("Protected", g_simulator->protectedAddresses.count(itr->interf.address()))
					    .detail("Reliable", p->isReliable())
					    .detail("ReliableInfo", p->getReliableInfo())
					    .detail("KillOrRebootProcess", p->address);
					if (p->isReliable()) {
						g_simulator->rebootProcess(p, ISimulator::KillType::RebootProcess);
					} else {
						g_simulator->killProcess(p, ISimulator::KillType::KillInstantly);
					}
				}

				foundExtraDataStore = true;
			}
		}

		if (foundExtraDataStore) {
			self->testFailure("Extra data stores present on workers");
			return false;
		}

		return true;
	}

	// Checks if the blob workers are "correct".
	// Returns false if ANY of the following
	// - any blob worker is on a diff DC than the blob manager/CC, or
	// - any worker that should have a blob worker does not have exactly one, or
	// - any worker that should NOT have a blob worker does indeed have one
	ACTOR Future<bool> checkBlobWorkers(Database cx,
	                                    DatabaseConfiguration configuration,
	                                    ConsistencyCheckWorkload* self) {
		state std::vector<BlobWorkerInterface> blobWorkers = wait(getBlobWorkers(cx, true));
		state std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));

		// process addr -> num blob workers on that process
		state std::unordered_map<NetworkAddress, int> blobWorkersByAddr;
		Optional<Key> ccDcId;
		NetworkAddress ccAddr = self->dbInfo->get().clusterInterface.clientInterface.address();

		// get the CC's DCID
		for (const auto& worker : workers) {
			if (ccAddr == worker.interf.address()) {
				ccDcId = worker.interf.locality.dcId();
				break;
			}
		}

		if (!ccDcId.present()) {
			TraceEvent("ConsistencyCheck_DidNotFindCC");
			return false;
		}

		for (const auto& bwi : blobWorkers) {
			if (bwi.locality.dcId() != ccDcId) {
				TraceEvent("ConsistencyCheck_BWOnDiffDcThanCC")
				    .detail("BWID", bwi.id())
				    .detail("BwDcId", bwi.locality.dcId())
				    .detail("CcDcId", ccDcId);
				return false;
			}
			blobWorkersByAddr[bwi.stableAddress()]++;
		}

		int numBlobWorkerProcesses = 0;
		for (const auto& worker : workers) {
			NetworkAddress addr = worker.interf.stableAddress();
			bool inCCDc = worker.interf.locality.dcId() == ccDcId;
			if (!configuration.isExcludedServer(worker.interf.addresses(), worker.interf.locality)) {
				if (worker.processClass == ProcessClass::BlobWorkerClass) {
					numBlobWorkerProcesses++;

					// this is a worker with processClass == BWClass, so should have exactly one blob worker if it's in
					// the same DC
					int desiredBlobWorkersOnAddr = inCCDc ? 1 : 0;

					if (blobWorkersByAddr[addr] != desiredBlobWorkersOnAddr) {
						TraceEvent("ConsistencyCheck_WrongBWCountOnBWClass")
						    .detail("Address", addr)
						    .detail("NumBlobWorkersOnAddr", blobWorkersByAddr[addr])
						    .detail("DesiredBlobWorkersOnAddr", desiredBlobWorkersOnAddr)
						    .detail("BwDcId", worker.interf.locality.dcId())
						    .detail("CcDcId", ccDcId);
						return false;
					}
				} else {
					// this is a worker with processClass != BWClass, so there should be no BWs on it
					if (blobWorkersByAddr[addr] > 0) {
						TraceEvent("ConsistencyCheck_BWOnNonBWClass").detail("Address", addr);
						return false;
					}
				}
			} else if (blobWorkersByAddr[addr] > 0) {
				TraceEvent("ConsistencyCheck_BWOnExcludedAddr").detail("Address", addr);
				return false;
			}
		}
		return numBlobWorkerProcesses > 0;
	}

	ACTOR Future<bool> checkWorkerList(Database cx, ConsistencyCheckWorkload* self) {
		if (!g_simulator->extraDatabases.empty()) {
			return true;
		}

		std::vector<WorkerDetails> workers = wait(getWorkers(self->dbInfo));
		std::set<NetworkAddress> workerAddresses;

		for (const auto& it : workers) {
			NetworkAddress addr = it.interf.tLog.getEndpoint().addresses.getTLSAddress();
			ISimulator::ProcessInfo* info = g_simulator->getProcessByAddress(addr);
			if (!info || info->failed) {
				TraceEvent("ConsistencyCheck_FailedWorkerInList").detail("Addr", it.interf.address());
				return false;
			}
			workerAddresses.insert(NetworkAddress(addr.ip, addr.port, true, addr.isTLS()));
		}

		std::vector<ISimulator::ProcessInfo*> all = g_simulator->getAllProcesses();
		for (int i = 0; i < all.size(); i++) {
			if (all[i]->isReliable() && all[i]->name == std::string("Server") &&
			    all[i]->startingClass != ProcessClass::TesterClass &&
			    all[i]->startingClass != ProcessClass::SimHTTPServerClass &&
			    all[i]->protocolVersion == g_network->protocolVersion()) {
				if (!workerAddresses.count(all[i]->address)) {
					TraceEvent("ConsistencyCheck_WorkerMissingFromList").detail("Addr", all[i]->address);
					return false;
				}
			}
		}

		return true;
	}

	static ProcessClass::Fitness getBestAvailableFitness(
	    const std::vector<ProcessClass::ClassType>& availableClassTypes,
	    ProcessClass::ClusterRole role) {
		ProcessClass::Fitness bestAvailableFitness = ProcessClass::NeverAssign;
		for (auto classType : availableClassTypes) {
			bestAvailableFitness = std::min(
			    bestAvailableFitness, ProcessClass(classType, ProcessClass::InvalidSource).machineClassFitness(role));
		}

		return bestAvailableFitness;
	}

	template <class T>
	static std::string getOptionalString(Optional<T> opt) {
		if (opt.present())
			return opt.get().toString();
		return "NotSet";
	}

	ACTOR Future<bool> checkCoordinators(Database cx) {
		state Transaction tr(cx);
		loop {
			try {
				tr.setOption(FDBTransactionOptions::LOCK_AWARE);
				Optional<Value> currentKey = wait(tr.get(coordinatorsKey));

				if (!currentKey.present()) {
					TraceEvent("ConsistencyCheck_NoCoordinatorKey").log();
					return false;
				}

				ClusterConnectionString old(currentKey.get().toString());
				state std::vector<NetworkAddress> oldCoordinators = wait(old.tryResolveHostnames());

				std::vector<ProcessData> workers = wait(::getWorkers(&tr));

				std::map<NetworkAddress, LocalityData> addr_locality;
				for (auto w : workers) {
					addr_locality[w.address] = w.locality;
				}

				std::set<Optional<Standalone<StringRef>>> checkDuplicates;
				for (const auto& addr : oldCoordinators) {
					auto findResult = addr_locality.find(addr);
					if (findResult != addr_locality.end()) {
						if (checkDuplicates.count(findResult->second.zoneId())) {
							TraceEvent("ConsistencyCheck_BadCoordinator")
							    .detail("Addr", addr)
							    .detail("NotFound", findResult == addr_locality.end());
							return false;
						}
						checkDuplicates.insert(findResult->second.zoneId());
					}
				}

				return true;
			} catch (Error& e) {
				wait(tr.onError(e));
			}
		}
	}

	// Returns true if all machines in the cluster that specified a desired class are operating in that class
	ACTOR Future<bool> checkUsingDesiredClasses(Database cx, ConsistencyCheckWorkload* self) {
		state Optional<Key> expectedPrimaryDcId;
		state Optional<Key> expectedRemoteDcId;
		state DatabaseConfiguration config = wait(getDatabaseConfiguration(cx));
		state std::vector<WorkerDetails> allWorkers = wait(getWorkers(self->dbInfo));
		state std::vector<WorkerDetails> nonExcludedWorkers =
		    wait(getWorkers(self->dbInfo, GetWorkersRequest::NON_EXCLUDED_PROCESSES_ONLY));
		auto& db = self->dbInfo->get();

		std::map<NetworkAddress, WorkerDetails> allWorkerProcessMap;
		std::map<Optional<Key>, std::vector<ProcessClass::ClassType>> dcToAllClassTypes;
		for (const auto& worker : allWorkers) {
			allWorkerProcessMap[worker.interf.address()] = worker;
			Optional<Key> dc = worker.interf.locality.dcId();
			if (!dcToAllClassTypes.count(dc))
				dcToAllClassTypes.insert({});
			dcToAllClassTypes[dc].push_back(worker.processClass.classType());
		}

		std::map<NetworkAddress, WorkerDetails> nonExcludedWorkerProcessMap;
		std::map<Optional<Key>, std::vector<ProcessClass::ClassType>> dcToNonExcludedClassTypes;
		for (const auto& worker : nonExcludedWorkers) {
			nonExcludedWorkerProcessMap[worker.interf.address()] = worker;
			Optional<Key> dc = worker.interf.locality.dcId();
			if (!dcToNonExcludedClassTypes.count(dc))
				dcToNonExcludedClassTypes.insert({});
			dcToNonExcludedClassTypes[dc].push_back(worker.processClass.classType());
		}

		if (!allWorkerProcessMap.count(db.clusterInterface.clientInterface.address())) {
			TraceEvent("ConsistencyCheck_CCNotInWorkerList")
			    .detail("CCAddress", db.clusterInterface.clientInterface.address().toString());
			return false;
		}
		if (!allWorkerProcessMap.count(db.master.address())) {
			TraceEvent("ConsistencyCheck_MasterNotInWorkerList")
			    .detail("MasterAddress", db.master.address().toString());
			return false;
		}

		Optional<Key> ccDcId =
		    allWorkerProcessMap[db.clusterInterface.clientInterface.address()].interf.locality.dcId();
		Optional<Key> masterDcId = allWorkerProcessMap[db.master.address()].interf.locality.dcId();

		if (ccDcId != masterDcId) {
			TraceEvent("ConsistencyCheck_CCAndMasterNotInSameDC")
			    .detail("ClusterControllerDcId", getOptionalString(ccDcId))
			    .detail("MasterDcId", getOptionalString(masterDcId));
			return false;
		}
		// Check if master and cluster controller are in the desired DC for fearless cluster when running under
		// simulation
		// FIXME: g_simulator->datacenterDead could return false positives. Relaxing checks until it is fixed.
		if (g_network->isSimulated() && config.usableRegions > 1 && g_simulator->primaryDcId.present() &&
		    !g_simulator->datacenterDead(g_simulator->primaryDcId) &&
		    !g_simulator->datacenterDead(g_simulator->remoteDcId)) {
			expectedPrimaryDcId = config.regions[0].dcId;
			expectedRemoteDcId = config.regions[1].dcId;
			// If the priorities are equal, either could be the primary
			if (config.regions[0].priority == config.regions[1].priority) {
				expectedPrimaryDcId = masterDcId;
				expectedRemoteDcId = config.regions[0].dcId == expectedPrimaryDcId.get() ? config.regions[1].dcId
				                                                                         : config.regions[0].dcId;
			}

			if (ccDcId != expectedPrimaryDcId) {
				TraceEvent("ConsistencyCheck_ClusterControllerDcNotBest")
				    .detail("PreferredDcId", getOptionalString(expectedPrimaryDcId))
				    .detail("ExistingDcId", getOptionalString(ccDcId));
				return false;
			}
			if (masterDcId != expectedPrimaryDcId) {
				TraceEvent("ConsistencyCheck_MasterDcNotBest")
				    .detail("PreferredDcId", getOptionalString(expectedPrimaryDcId))
				    .detail("ExistingDcId", getOptionalString(masterDcId));
				return false;
			}
		}

		// Check CC
		ProcessClass::Fitness bestClusterControllerFitness =
		    getBestAvailableFitness(dcToNonExcludedClassTypes[ccDcId], ProcessClass::ClusterController);
		if (!nonExcludedWorkerProcessMap.count(db.clusterInterface.clientInterface.address()) ||
		    nonExcludedWorkerProcessMap[db.clusterInterface.clientInterface.address()].processClass.machineClassFitness(
		        ProcessClass::ClusterController) != bestClusterControllerFitness) {
			TraceEvent("ConsistencyCheck_ClusterControllerNotBest")
			    .detail("BestClusterControllerFitness", bestClusterControllerFitness)
			    .detail("ExistingClusterControllerFit",
			            nonExcludedWorkerProcessMap.count(db.clusterInterface.clientInterface.address())
			                ? nonExcludedWorkerProcessMap[db.clusterInterface.clientInterface.address()]
			                      .processClass.machineClassFitness(ProcessClass::ClusterController)
			                : -1);
			return false;
		}

		// Check Master
		ProcessClass::Fitness bestMasterFitness =
		    getBestAvailableFitness(dcToNonExcludedClassTypes[masterDcId], ProcessClass::Master);
		if (bestMasterFitness == ProcessClass::NeverAssign) {
			bestMasterFitness = getBestAvailableFitness(dcToAllClassTypes[masterDcId], ProcessClass::Master);
			if (bestMasterFitness != ProcessClass::NeverAssign) {
				bestMasterFitness = ProcessClass::ExcludeFit;
			}
		}

		if ((!nonExcludedWorkerProcessMap.count(db.master.address()) &&
		     bestMasterFitness != ProcessClass::ExcludeFit) ||
		    nonExcludedWorkerProcessMap[db.master.address()].processClass.machineClassFitness(ProcessClass::Master) !=
		        bestMasterFitness) {
			TraceEvent("ConsistencyCheck_MasterNotBest")
			    .detail("BestMasterFitness", bestMasterFitness)
			    .detail("ExistingMasterFit",
			            nonExcludedWorkerProcessMap.count(db.master.address())
			                ? nonExcludedWorkerProcessMap[db.master.address()].processClass.machineClassFitness(
			                      ProcessClass::Master)
			                : -1);
			return false;
		}

		// Check commit proxy
		ProcessClass::Fitness bestCommitProxyFitness =
		    getBestAvailableFitness(dcToNonExcludedClassTypes[masterDcId], ProcessClass::CommitProxy);
		for (const auto& commitProxy : db.client.commitProxies) {
			if (!nonExcludedWorkerProcessMap.count(commitProxy.address()) ||
			    nonExcludedWorkerProcessMap[commitProxy.address()].processClass.machineClassFitness(
			        ProcessClass::CommitProxy) != bestCommitProxyFitness) {
				TraceEvent("ConsistencyCheck_CommitProxyNotBest")
				    .detail("BestCommitProxyFitness", bestCommitProxyFitness)
				    .detail("ExistingCommitProxyFitness",
				            nonExcludedWorkerProcessMap.count(commitProxy.address())
				                ? nonExcludedWorkerProcessMap[commitProxy.address()].processClass.machineClassFitness(
				                      ProcessClass::CommitProxy)
				                : -1);
				return false;
			}
		}

		// Check grv proxy
		ProcessClass::Fitness bestGrvProxyFitness =
		    getBestAvailableFitness(dcToNonExcludedClassTypes[masterDcId], ProcessClass::GrvProxy);
		for (const auto& grvProxy : db.client.grvProxies) {
			if (!nonExcludedWorkerProcessMap.count(grvProxy.address()) ||
			    nonExcludedWorkerProcessMap[grvProxy.address()].processClass.machineClassFitness(
			        ProcessClass::GrvProxy) != bestGrvProxyFitness) {
				TraceEvent("ConsistencyCheck_GrvProxyNotBest")
				    .detail("BestGrvProxyFitness", bestGrvProxyFitness)
				    .detail("ExistingGrvProxyFitness",
				            nonExcludedWorkerProcessMap.count(grvProxy.address())
				                ? nonExcludedWorkerProcessMap[grvProxy.address()].processClass.machineClassFitness(
				                      ProcessClass::GrvProxy)
				                : -1);
				return false;
			}
		}

		// Check resolver
		ProcessClass::Fitness bestResolverFitness =
		    getBestAvailableFitness(dcToNonExcludedClassTypes[masterDcId], ProcessClass::Resolver);
		for (const auto& resolver : db.resolvers) {
			if (!nonExcludedWorkerProcessMap.count(resolver.address()) ||
			    nonExcludedWorkerProcessMap[resolver.address()].processClass.machineClassFitness(
			        ProcessClass::Resolver) != bestResolverFitness) {
				TraceEvent("ConsistencyCheck_ResolverNotBest")
				    .detail("BestResolverFitness", bestResolverFitness)
				    .detail("ExistingResolverFitness",
				            nonExcludedWorkerProcessMap.count(resolver.address())
				                ? nonExcludedWorkerProcessMap[resolver.address()].processClass.machineClassFitness(
				                      ProcessClass::Resolver)
				                : -1);
				return false;
			}
		}

		// Check LogRouter
		if (g_network->isSimulated() && config.usableRegions > 1 && g_simulator->primaryDcId.present() &&
		    !g_simulator->datacenterDead(g_simulator->primaryDcId) &&
		    !g_simulator->datacenterDead(g_simulator->remoteDcId)) {
			for (auto& tlogSet : db.logSystemConfig.tLogs) {
				if (!tlogSet.isLocal && tlogSet.logRouters.size()) {
					for (auto& logRouter : tlogSet.logRouters) {
						if (!nonExcludedWorkerProcessMap.count(logRouter.interf().address())) {
							TraceEvent("ConsistencyCheck_LogRouterNotInNonExcludedWorkers")
							    .detail("Id", logRouter.id());
							return false;
						}
						if (logRouter.interf().filteredLocality.dcId() != expectedRemoteDcId) {
							TraceEvent("ConsistencyCheck_LogRouterNotBestDC")
							    .detail("expectedDC", getOptionalString(expectedRemoteDcId))
							    .detail("ActualDC", getOptionalString(logRouter.interf().filteredLocality.dcId()));
							return false;
						}
					}
				}
			}
		}

		// Check DataDistributor
		ProcessClass::Fitness fitnessLowerBound =
		    allWorkerProcessMap[db.master.address()].processClass.machineClassFitness(ProcessClass::DataDistributor);
		if (db.distributor.present() &&
		    (!nonExcludedWorkerProcessMap.count(db.distributor.get().address()) ||
		     nonExcludedWorkerProcessMap[db.distributor.get().address()].processClass.machineClassFitness(
		         ProcessClass::DataDistributor) > fitnessLowerBound)) {
			TraceEvent("ConsistencyCheck_DistributorNotBest")
			    .detail("DataDistributorFitnessLowerBound", fitnessLowerBound)
			    .detail(
			        "ExistingDistributorFitness",
			        nonExcludedWorkerProcessMap.count(db.distributor.get().address())
			            ? nonExcludedWorkerProcessMap[db.distributor.get().address()].processClass.machineClassFitness(
			                  ProcessClass::DataDistributor)
			            : -1);
			return false;
		}

		// Check Ratekeeper
		if (db.ratekeeper.present() &&
		    (!nonExcludedWorkerProcessMap.count(db.ratekeeper.get().address()) ||
		     nonExcludedWorkerProcessMap[db.ratekeeper.get().address()].processClass.machineClassFitness(
		         ProcessClass::Ratekeeper) > fitnessLowerBound)) {
			TraceEvent("ConsistencyCheck_RatekeeperNotBest")
			    .detail("BestRatekeeperFitness", fitnessLowerBound)
			    .detail(
			        "ExistingRatekeeperFitness",
			        nonExcludedWorkerProcessMap.count(db.ratekeeper.get().address())
			            ? nonExcludedWorkerProcessMap[db.ratekeeper.get().address()].processClass.machineClassFitness(
			                  ProcessClass::Ratekeeper)
			            : -1);
			return false;
		}

		// Check BlobManager
		if (config.blobGranulesEnabled && db.blobManager.present() &&
		    (!nonExcludedWorkerProcessMap.count(db.blobManager.get().address()) ||
		     nonExcludedWorkerProcessMap[db.blobManager.get().address()].processClass.machineClassFitness(
		         ProcessClass::BlobManager) > fitnessLowerBound)) {
			TraceEvent("ConsistencyCheck_BlobManagerNotBest")
			    .detail("BestBlobManagerFitness", fitnessLowerBound)
			    .detail(
			        "ExistingBlobManagerFitness",
			        nonExcludedWorkerProcessMap.count(db.blobManager.get().address())
			            ? nonExcludedWorkerProcessMap[db.blobManager.get().address()].processClass.machineClassFitness(
			                  ProcessClass::BlobManager)
			            : -1);
			return false;
		}

		// Check BlobMigrator
		if (config.blobGranulesEnabled && db.blobMigrator.present() &&
		    (!nonExcludedWorkerProcessMap.count(db.blobMigrator.get().address()) ||
		     nonExcludedWorkerProcessMap[db.blobMigrator.get().address()].processClass.machineClassFitness(
		         ProcessClass::BlobMigrator) > fitnessLowerBound)) {
			TraceEvent("ConsistencyCheck_BlobMigratorNotBest")
			    .detail("BestBlobMigratorFitness", fitnessLowerBound)
			    .detail(
			        "ExistingBlobMigratorFitness",
			        nonExcludedWorkerProcessMap.count(db.blobMigrator.get().address())
			            ? nonExcludedWorkerProcessMap[db.blobMigrator.get().address()].processClass.machineClassFitness(
			                  ProcessClass::BlobMigrator)
			            : -1);
			return false;
		}

		// Check EncryptKeyProxy
		if (config.encryptionAtRestMode.isEncryptionEnabled() && db.client.encryptKeyProxy.present() &&
		    (!nonExcludedWorkerProcessMap.count(db.client.encryptKeyProxy.get().address()) ||
		     nonExcludedWorkerProcessMap[db.client.encryptKeyProxy.get().address()].processClass.machineClassFitness(
		         ProcessClass::EncryptKeyProxy) > fitnessLowerBound)) {
			TraceEvent("ConsistencyCheck_EncryptKeyProxyNotBest")
			    .detail("BestEncryptKeyProxyFitness", fitnessLowerBound)
			    .detail("ExistingEncryptKeyProxyFitness",
			            nonExcludedWorkerProcessMap.count(db.client.encryptKeyProxy.get().address())
			                ? nonExcludedWorkerProcessMap[db.client.encryptKeyProxy.get().address()]
			                      .processClass.machineClassFitness(ProcessClass::EncryptKeyProxy)
			                : -1);
			return false;
		}

		// Check ConsistencyScan
		if (db.consistencyScan.present() &&
		    (!nonExcludedWorkerProcessMap.count(db.consistencyScan.get().address()) ||
		     nonExcludedWorkerProcessMap[db.consistencyScan.get().address()].processClass.machineClassFitness(
		         ProcessClass::ConsistencyScan) > fitnessLowerBound)) {
			TraceEvent("ConsistencyCheck_ConsistencyScanNotBest")
			    .detail("BestConsistencyScanFitness", fitnessLowerBound)
			    .detail("ExistingConsistencyScanFitness",
			            nonExcludedWorkerProcessMap.count(db.consistencyScan.get().address())
			                ? nonExcludedWorkerProcessMap[db.consistencyScan.get().address()]
			                      .processClass.machineClassFitness(ProcessClass::ConsistencyScan)
			                : -1);
			return false;
		}

		// TODO: Check Tlog

		return true;
	}

	// returns true if stopped, false otherwise
	ACTOR Future<bool> checkConsistencyScan(Database cx) {
		if (!g_network->isSimulated()) {
			return true;
		}
		state Reference<ReadYourWritesTransaction> tr = makeReference<ReadYourWritesTransaction>(cx);
		state ConsistencyScanState cs;
		loop {
			try {
				SystemDBWriteLockedNow(cx.getReference())->setOptions(tr);
				ConsistencyScanState::Config config = wait(cs.config().getD(tr));
				return !config.enabled;
			} catch (Error& e) {
				wait(tr->onError(e));
			}
		}
	}

	bool checkSingleSingleton(std::vector<ISimulator::ProcessInfo*> const& allProcesses,
	                          TraceEvent& ev,
	                          std::string const& role,
	                          int expectedCount) {
		// FIXME: this doesn't actually check that there aren't multiple of the same role running on the same process
		// either
		int count = 0;
		for (int i = 0; i < allProcesses.size(); i++) {
			if (g_simulator->hasRole(allProcesses[i]->address, role)) {
				count++;
				ev.detail(role + std::to_string(count), allProcesses[i]->address.toString());
			}
		}
		ev.detail(role + "Count", count).detail(role + "ExpectedCount", expectedCount);
		if (count != expectedCount) {
			fmt::print("ConsistencyCheck failure: incorrect number {0} of singleton {1} running (expected {2})\n",
			           count,
			           role,
			           expectedCount);
		}
		return count == expectedCount;
	}

	// checks that there is only one instance of each singleton running in the cluster in simulation
	bool checkSingleSingletons(ConsistencyCheckWorkload* self, DatabaseConfiguration config) {
		if (!g_network->isSimulated()) {
			return true;
		}

		CODE_PROBE(self->performQuiescentChecks, "Checking for single singletons");

		std::vector<ISimulator::ProcessInfo*> allProcesses = g_simulator->getAllProcesses();

		bool success = true;
		TraceEvent ev("CheckSingletons");

		success &= self->checkSingleSingleton(allProcesses, ev, "Ratekeeper", 1);
		success &= self->checkSingleSingleton(allProcesses, ev, "DataDistributor", 1);
		success &= self->checkSingleSingleton(allProcesses, ev, "ConsistencyScan", 1);
		success &= self->checkSingleSingleton(allProcesses, ev, "BlobManager", config.blobGranulesEnabled ? 1 : 0);

		// FIXME: add blob migrator once it's always on
		// success &= self->checkSingleSingleton(allProcesses, ev, "BlobMigrator", TODO ? 1 : 0);

		success &= self->checkSingleSingleton(
		    allProcesses,
		    ev,
		    "EncryptKeyProxy",
		    config.encryptionAtRestMode.isEncryptionEnabled() || SERVER_KNOBS->ENABLE_REST_KMS_COMMUNICATION ? 1 : 0);

		if (!success) {
			// TODO REMOVE
			fmt::print("ConsistencyCheck singletons: roles map:\n");
			for (int i = 0; i < allProcesses.size(); i++) {
				fmt::print(
				    "{0}: {1}\n", allProcesses[i]->address.toString(), g_simulator->getRoles(allProcesses[i]->address));
			}
		}

		return success;
	}
};

WorkloadFactory<ConsistencyCheckWorkload> ConsistencyCheckWorkloadFactory;
