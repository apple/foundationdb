/*
 * GrvProxyServer.cpp
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

#include <array>

#include "fdbclient/DatabaseContext.h"
#include "fdbclient/GlobalConfig.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/Notified.h"
#include "fdbclient/TransactionLineage.h"
#include "fdbclient/Tuple.h"
#include "fdbclient/CommitProxyInterface.h"
#include "fdbclient/GrvProxyInterface.h"
#include "fdbclient/StorageServerInterface.h"
#include "fdbclient/VersionVector.h"
#include "fdbserver/grvproxy/GrvProxyServer.h"
#include "HealthMetricsRequestServer.h"
#include "GrvQueueDelay.h"
#include "GrvProxyStarvation.h"
#include "GrvTransactionRateInfo.h"
#include "fdbserver/logsystem/LogSystem.h"
#include "fdbserver/logsystem/LogSystemFactory.h"
#include "fdbserver/logsystem/LogSystemDiskQueueAdapter.h"
#include "fdbserver/core/WaitFailure.h"
#include "fdbserver/core/WorkerInterface.h"
#include "fdbrpc/sim_validation.h"
#include "flow/Buggify.h"
#include "flow/IRandom.h"
#include "flow/Trace.h"
#include "flow/UnitTest.h"
#include "flow/flow.h"
#include "flow/CoroUtils.h"
#include "flow/genericactors.h"

struct GrvProxyStats {
	CounterCollection cc;
	Counter txnRequestIn;
	Counter txnRequestOut;
	Counter txnRequestErrors;
	Counter txnStartIn;
	Counter txnStartOut;
	Counter txnStartBatch;
	Counter txnSystemPriorityStartIn;
	Counter txnSystemPriorityStartOut;
	Counter txnBatchPriorityStartIn;
	Counter txnBatchPriorityStartOut;
	Counter txnDefaultPriorityStartIn;
	Counter txnDefaultPriorityStartOut;
	Counter txnThrottled;
	Counter updatesFromRatekeeper;
	Counter leaseTimeouts;
	int systemGRVQueueSize;
	int defaultGRVQueueSize;
	int batchGRVQueueSize;
	int versionVectorSize;
	int versionVectorEncodedBytes;
	double transactionRateAllowed;
	double batchTransactionRateAllowed;
	double transactionLimit;
	double batchTransactionLimit;
	// how much of the GRV requests queue was processed in one attempt to hand out read version.
	double percentageOfDefaultGRVQueueProcessed;
	double percentageOfBatchGRVQueueProcessed;

	bool lastBatchQueueThrottled;
	bool lastDefaultQueueThrottled;
	double batchThrottleStartTime;
	double defaultThrottleStartTime;

	LatencySample defaultTxnGRVTimeInQueue;
	LatencySample batchTxnGRVTimeInQueue;

	LatencyBands grvLatencyBands;
	LatencySample grvLatencySample; // GRV latency metric sample of default priority
	LatencySample grvBatchLatencySample; // GRV latency metric sample of batched priority

	Future<Void> logger;

	int recentRequests;
	Deque<int> requestBuckets;
	double lastBucketBegin;
	double bucketInterval;
	Reference<Histogram> grvConfirmEpochLiveDist;
	Reference<Histogram> grvGetCommittedVersionRpcDist;

	void updateRequestBuckets() {
		while (now() - lastBucketBegin > bucketInterval) {
			lastBucketBegin += bucketInterval;
			recentRequests -= requestBuckets.front();
			requestBuckets.pop_front();
			requestBuckets.push_back(0);
		}
	}

	void addRequest(int transactionCount) {
		updateRequestBuckets();
		recentRequests += transactionCount;
		requestBuckets.back() += transactionCount;
	}

	int getRecentRequests() {
		updateRequestBuckets();
		return recentRequests /
		       (FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE - (lastBucketBegin + bucketInterval - now()));
	}

	// Current stats maintained for a given grv proxy server
	explicit GrvProxyStats(UID id)
	  : cc("GrvProxyStats", id.toString()),

	    txnRequestIn("TxnRequestIn", cc), txnRequestOut("TxnRequestOut", cc), txnRequestErrors("TxnRequestErrors", cc),
	    txnStartIn("TxnStartIn", cc), txnStartOut("TxnStartOut", cc), txnStartBatch("TxnStartBatch", cc),
	    txnSystemPriorityStartIn("TxnSystemPriorityStartIn", cc),
	    txnSystemPriorityStartOut("TxnSystemPriorityStartOut", cc),
	    txnBatchPriorityStartIn("TxnBatchPriorityStartIn", cc),
	    txnBatchPriorityStartOut("TxnBatchPriorityStartOut", cc),
	    txnDefaultPriorityStartIn("TxnDefaultPriorityStartIn", cc),
	    txnDefaultPriorityStartOut("TxnDefaultPriorityStartOut", cc), txnThrottled("TxnThrottled", cc),
	    updatesFromRatekeeper("UpdatesFromRatekeeper", cc), leaseTimeouts("LeaseTimeouts", cc), systemGRVQueueSize(0),
	    defaultGRVQueueSize(0), batchGRVQueueSize(0), versionVectorSize(0), versionVectorEncodedBytes(0),
	    transactionRateAllowed(0), batchTransactionRateAllowed(0), transactionLimit(0), batchTransactionLimit(0),
	    percentageOfDefaultGRVQueueProcessed(0), percentageOfBatchGRVQueueProcessed(0), lastBatchQueueThrottled(false),
	    lastDefaultQueueThrottled(false), batchThrottleStartTime(0.0), defaultThrottleStartTime(0.0),
	    defaultTxnGRVTimeInQueue("DefaultTxnGRVTimeInQueue",
	                             id,
	                             SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                             SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    batchTxnGRVTimeInQueue("BatchTxnGRVTimeInQueue",
	                           id,
	                           SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                           SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    grvLatencyBands("GRVLatencyBands", id, SERVER_KNOBS->STORAGE_LOGGING_DELAY),
	    grvLatencySample("GRVLatencyMetrics",
	                     id,
	                     SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                     SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    grvBatchLatencySample("GRVBatchLatencyMetrics",
	                          id,
	                          SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
	                          SERVER_KNOBS->LATENCY_SKETCH_ACCURACY),
	    recentRequests(0), lastBucketBegin(now()),
	    bucketInterval(FLOW_KNOBS->BASIC_LOAD_BALANCE_UPDATE_RATE / FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS),
	    grvConfirmEpochLiveDist(
	        Histogram::getHistogram("GrvProxy"_sr, "GrvConfirmEpochLive"_sr, Histogram::Unit::milliseconds)),
	    grvGetCommittedVersionRpcDist(
	        Histogram::getHistogram("GrvProxy"_sr, "GrvGetCommittedVersionRpc"_sr, Histogram::Unit::milliseconds)) {
		// The rate at which the limit(budget) is allowed to grow.
		specialCounter(cc, "SystemGRVQueueSize", [this]() { return this->systemGRVQueueSize; });
		specialCounter(cc, "DefaultGRVQueueSize", [this]() { return this->defaultGRVQueueSize; });
		specialCounter(cc, "BatchGRVQueueSize", [this]() { return this->batchGRVQueueSize; });
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
			specialCounter(cc, "VersionVectorSize", [this]() { return this->versionVectorSize; });
			specialCounter(cc, "VersionVectorEncodedBytes", [this]() { return this->versionVectorEncodedBytes; });
		}
		specialCounter(
		    cc, "SystemAndDefaultTxnRateAllowed", [this]() { return int64_t(this->transactionRateAllowed); });
		specialCounter(
		    cc, "BatchTransactionRateAllowed", [this]() { return int64_t(this->batchTransactionRateAllowed); });
		specialCounter(cc, "SystemAndDefaultTxnLimit", [this]() { return int64_t(this->transactionLimit); });
		specialCounter(cc, "BatchTransactionLimit", [this]() { return int64_t(this->batchTransactionLimit); });
		specialCounter(cc, "PercentageOfDefaultGRVQueueProcessed", [this]() {
			return int64_t(100 * this->percentageOfDefaultGRVQueueProcessed);
		});
		specialCounter(cc, "PercentageOfBatchGRVQueueProcessed", [this]() {
			return int64_t(100 * this->percentageOfBatchGRVQueueProcessed);
		});

		logger = cc.traceCounters("GrvProxyMetrics", id, SERVER_KNOBS->WORKER_LOGGING_INTERVAL, "GrvProxyMetrics");
		for (int i = 0; i < FLOW_KNOBS->BASIC_LOAD_BALANCE_BUCKETS; i++) {
			requestBuckets.push_back(0);
		}
	}
};

struct GrvProxyData {
	GrvProxyInterface proxy;
	UID dbgid;

	GrvProxyStats stats;
	MasterInterface master;
	PublicRequestStream<GetReadVersionRequest> getConsistentReadVersion;
	Reference<LogSystem> logSystem;

	Database cx;
	Reference<AsyncVar<ServerDBInfo> const> db;

	Optional<LatencyBandConfig> latencyBandConfig;
	double lastStartCommit;
	double lastCommitLatency;
	std::unique_ptr<LatencySample> versionVectorSizeOnGRVReply;
	int updateCommitRequests;
	NotifiedDouble lastCommitTime;

	Version version;
	Version minKnownCommittedVersion; // we should ask master for this version.

	// Cache of the latest commit versions of storage servers.
	VersionVector ssVersionVectorCache;

	void updateLatencyBandConfig(Optional<LatencyBandConfig> newLatencyBandConfig) {
		if (newLatencyBandConfig.present() != latencyBandConfig.present() ||
		    (newLatencyBandConfig.present() &&
		     newLatencyBandConfig.get().grvConfig != latencyBandConfig.get().grvConfig)) {
			TraceEvent("LatencyBandGrvUpdatingConfig").detail("Present", newLatencyBandConfig.present());
			stats.grvLatencyBands.clearBands();
			if (newLatencyBandConfig.present()) {
				for (auto band : newLatencyBandConfig.get().grvConfig.bands) {
					stats.grvLatencyBands.addThreshold(band);
				}
			}
		}

		latencyBandConfig = newLatencyBandConfig;
	}

	GrvProxyData(UID dbgid,
	             MasterInterface master,
	             PublicRequestStream<GetReadVersionRequest> getConsistentReadVersion,
	             Reference<AsyncVar<ServerDBInfo> const> db)
	  : dbgid(dbgid), stats(dbgid), master(master), getConsistentReadVersion(getConsistentReadVersion),
	    cx(openDBOnServer(db, TaskPriority::DefaultEndpoint, LockAware::True)), db(db), lastStartCommit(0),
	    lastCommitLatency(SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION), updateCommitRequests(0), lastCommitTime(0),
	    version(0), minKnownCommittedVersion(invalidVersion) {
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
			versionVectorSizeOnGRVReply =
			    std::make_unique<LatencySample>("VersionVectorSizeOnGRVReply",
			                                    dbgid,
			                                    SERVER_KNOBS->LATENCY_METRICS_LOGGING_INTERVAL,
			                                    SERVER_KNOBS->LATENCY_SKETCH_ACCURACY);
		}
	}
};

// Older FDB versions used different keys for client profiling data. This
// function performs a one-time migration of data in these keys to the new
// global configuration key space.
Future<Void> globalConfigMigrate(GrvProxyData* grvProxyData) {
	Key migratedKey("\xff\x02/fdbClientInfo/migrated/"_sr);
	auto tr = makeReference<ReadYourWritesTransaction>(grvProxyData->cx);
	try {
		while (true) {
			tr->setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);

			Error err;
			try {
				Optional<Value> migrated = co_await tr->get(migratedKey);
				if (migrated.present()) {
					// Already performed migration.
					break;
				}

				Optional<Value> sampleRate = co_await tr->get(Key("\xff\x02/fdbClientInfo/client_txn_sample_rate/"_sr));
				Optional<Value> sizeLimit = co_await tr->get(Key("\xff\x02/fdbClientInfo/client_txn_size_limit/"_sr));

				tr->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// The value doesn't matter too much, as long as the key is set.
				tr->set(migratedKey.contents(), "1"_sr);
				if (sampleRate.present()) {
					const double sampleRateDbl =
					    BinaryReader::fromStringRef<double>(sampleRate.get().contents(), Unversioned());
					Tuple rate = Tuple::makeTuple(sampleRateDbl);
					tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSampleRate), rate.pack());
				}
				if (sizeLimit.present()) {
					const int64_t sizeLimitInt =
					    BinaryReader::fromStringRef<int64_t>(sizeLimit.get().contents(), Unversioned());
					Tuple size = Tuple::makeTuple(sizeLimitInt);
					tr->set(GlobalConfig::prefixedKey(fdbClientInfoTxnSizeLimit), size.pack());
				}

				co_await tr->commit();
				break;
			} catch (Error& e) {
				// Multiple GRV proxies may attempt this migration at the same
				// time, sometimes resulting in aborts due to conflicts.
				TraceEvent(SevInfo, "GlobalConfigRetryableMigrationError").errorUnsuppressed(e).suppressFor(1.0);
				err = e;
			}
			co_await tr->onError(err);
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// Catch non-retryable errors (and do nothing).
		TraceEvent(SevWarnAlways, "GlobalConfigMigrationError").error(e);
	}
}

struct GlobalConfigSnapshot {
	Versionstamp stamp{ 0, 0 };
	RangeResult data;

	void update(const Optional<Value>& versionstamp, const RangeResult& rows, Version readVersion) {
		// Clients replace their entire cache with these rows.
		if (rows.more) {
			throw serialization_failed();
		}
		if (versionstamp.present()) {
			const auto newStamp = GlobalConfig::decodeVersionstamp(versionstamp.get(), readVersion);
			if (!newStamp.present() || newStamp.get() < stamp) {
				throw serialization_failed();
			}
			stamp = newStamp.get();
		}
		data = rows;
	}

	Future<Void> read(ReadYourWritesTransaction& tr, int pageSize) {
		pageSize = std::max(1, pageSize);
		auto versionFuture = tr.get(globalConfigVersionKey);
		auto firstPage = tr.getRange(globalConfigDataKeys, pageSize);
		Optional<Value> versionstamp = co_await versionFuture;
		RangeResult rows = co_await firstPage;
		while (rows.more) {
			RangeResult page = co_await tr.getRange(
			    rows.nextBeginKeySelector(), firstGreaterOrEqual(globalConfigDataKeys.end), pageSize);
			rows.arena().dependsOn(page.arena());
			rows.append(rows.arena(), page.begin(), page.size());
			rows.more = page.more;
			rows.readThrough = page.readThrough;
			rows.readThroughEnd = page.readThroughEnd;
		}
		const Version readVersion = co_await tr.getReadVersion();
		update(versionstamp, rows, readVersion);
	}
};

// Periodically refresh local copy of global configuration.
Future<Void> globalConfigRefresh(GrvProxyData* grvProxyData, GlobalConfigSnapshot* snapshot) {
	auto tr = makeReference<ReadYourWritesTransaction>(grvProxyData->cx);
	try {
		while (true) {
			Error err;
			try {
				tr->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				co_await snapshot->read(*tr, CLIENT_KNOBS->TOO_MANY);
				co_return;
			} catch (Error& e) {
				err = e;
			}
			co_await tr->onError(err);
		}
	} catch (Error& e) {
		if (e.code() == error_code_actor_cancelled) {
			throw;
		}
		// Non-retryable error: keep the last-good cache and retry next refresh.
		TraceEvent(SevWarnAlways, "GlobalConfigRefreshError").errorUnsuppressed(e).suppressFor(60.0);
	}
}

TEST_CASE("/fdbserver/GrvProxyServer/GlobalConfigVersionKeyRejectsUndersizedValue") {
	GlobalConfigSnapshot snapshot;
	RangeResult at41;
	at41.push_back_deep(at41.arena(),
	                    KeyValueRef("A"_sr.withPrefix(globalConfigKeysPrefix), Tuple::makeTuple(true).pack()));
	const Versionstamp stampAt41(41, 7);
	snapshot.update(BinaryWriter::toValue(stampAt41, Unversioned()), at41, 41);
	ASSERT(snapshot.stamp == stampAt41);
	ASSERT(snapshot.data == at41);

	auto checkRejected = [&](const Value& damaged) {
		try {
			snapshot.update(damaged, RangeResult(), 42);
			ASSERT(false);
		} catch (Error& e) {
			ASSERT_EQ(e.code(), error_code_serialization_failed);
		}
		ASSERT(snapshot.stamp == stampAt41);
		ASSERT(snapshot.data == at41);
	};
	// The clear at 42 removes A. Dropping one leading zero would mislabel the empty snapshot as version 10752.
	auto stamp = BinaryWriter::toValue(Versionstamp(42, 0), Unversioned());
	for (int i = 1; i <= stamp.size(); ++i) {
		checkRejected(stamp.substr(i));
	}
	checkRejected(Value());
	checkRejected(stamp.withSuffix("\x00"_sr));
	for (Version invalid : std::array<Version, 3>{ (Version(1) << 62) + 42, 0, invalidVersion }) {
		checkRejected(BinaryWriter::toValue(Versionstamp(invalid, 0), Unversioned()));
	}
	checkRejected(BinaryWriter::toValue(Versionstamp(40, 0), Unversioned()));
	checkRejected(BinaryWriter::toValue(Versionstamp(41, 0), Unversioned()));

	RangeResult at43;
	at43.push_back_deep(at43.arena(),
	                    KeyValueRef("B"_sr.withPrefix(globalConfigKeysPrefix), Tuple::makeTuple(true).pack()));
	snapshot.update(BinaryWriter::toValue(Versionstamp(43, 7), Unversioned()), at43, 50);
	ASSERT(snapshot.stamp == Versionstamp(43, 7));
	ASSERT(snapshot.data == at43);
	return Void();
}

TEST_CASE("/fdbserver/GrvProxyServer/GlobalConfigSnapshotMissingVersion") {
	GlobalConfigSnapshot snapshot;
	RangeResult rows;
	rows.push_back_deep(rows.arena(),
	                    KeyValueRef("A"_sr.withPrefix(globalConfigKeysPrefix), Tuple::makeTuple(true).pack()));
	snapshot.update({}, rows, 1);
	ASSERT(snapshot.stamp == Versionstamp(0, 0));
	ASSERT(snapshot.data == rows);
	snapshot.update(BinaryWriter::toValue(Versionstamp(1, 0), Unversioned()), RangeResult(), 41);
	ASSERT(snapshot.stamp == Versionstamp(1, 0));
	ASSERT(snapshot.data.empty());
	snapshot.update({}, rows, 42);
	ASSERT(snapshot.stamp == Versionstamp(1, 0));
	ASSERT(snapshot.data == rows);
	snapshot.update({}, RangeResult(), 42);
	ASSERT(snapshot.stamp == Versionstamp(1, 0));
	ASSERT(snapshot.data.empty());
	return Void();
}

TEST_CASE("/fdbserver/GrvProxyServer/GlobalConfigSnapshotReadsAllPages") {
	GrvProxyInterface proxy;
	proxy.provisional = false;
	proxy.initEndpoints();
	CommitProxyInterface commitProxy;
	commitProxy.provisional = false;
	commitProxy.initEndpoints();
	ClientDBInfo info;
	info.id = UID(1, 1);
	info.grvProxies.push_back(proxy);
	info.commitProxies.push_back(commitProxy);
	auto clientInfo = makeReference<AsyncVar<ClientDBInfo>>(info);
	auto db = DatabaseContext::create(clientInfo, Never(), LocalityData(), EnableLocalityLoadBalance::False);
	StorageServerInterface storage;
	storage.initEndpoints();
	IFailureMonitor::failureMonitor().setStatus(storage.address(), FailureStatus(false));
	IFailureMonitor::failureMonitor().setStatus(proxy.address(), FailureStatus(false));
	db->setCachedLocation(allKeys, { storage });

	const auto enabled = Tuple::makeTuple(true).pack();
	RangeResult rows;
	for (KeyRef key : { "A"_sr, "B"_sr, "Y"_sr, "Z"_sr }) {
		rows.push_back_deep(
		    rows.arena(),
		    KeyValueRef(key.withPrefix(globalConfigKeysPrefix), key == "A"_sr ? "\xff"_sr : enabled.contents()));
	}
	GlobalConfigSnapshot snapshot;
	snapshot.data.append_deep(snapshot.data.arena(), rows.begin() + 2, 2);
	snapshot.stamp = Versionstamp(41, 0);
	db->globalConfig->init(Reference<AsyncVar<ClientDBInfo> const>(clientInfo), std::addressof(clientInfo->get()));
	auto nextRefresh = [&] { return timeoutError(waitAndForward(proxy.refreshGlobalConfig.getFuture()), 5.0); };
	auto initial = co_await nextRefresh();
	initial.reply.send(GlobalConfigRefreshReply{ snapshot.data.arena(), snapshot.stamp.version, snapshot.data });
	co_await timeoutError(db->globalConfig->onInitialized(), 5.0);
	ASSERT(db->globalConfig->get<bool>("Z"_sr, false));

	VersionHistory at42(42);
	at42.mutations.emplace_back_deep(at42.mutations.arena(), MutationRef(MutationRef::SetValue, "A"_sr, "\xff"_sr));
	at42.mutations.emplace_back_deep(at42.mutations.arena(), MutationRef(MutationRef::SetValue, "B"_sr, enabled));
	info.id = UID(1, 2);
	info.history = { VersionHistory(41), at42 };
	auto changed = db->globalConfig->onChange();
	clientInfo->set(info);
	auto recovery = co_await nextRefresh();
	ASSERT_EQ(recovery.lastKnown, 41);

	auto readPages = [&](Version version, Error lastPageError = {}) -> Future<Void> {
		const auto previous = snapshot;
		ReadYourWritesTransaction tr(db);
		tr.setOption(FDBTransactionOptions::ACCESS_SYSTEM_KEYS);
		tr.setVersion(version);
		tr.set(globalConfigVersionKey, BinaryWriter::toValue(Versionstamp(version, 0), Unversioned()));
		auto read = snapshot.read(tr, 3);
		auto first = co_await timeoutError(waitAndForward(storage.getKeyValues.getFuture()), 5.0);
		ASSERT_EQ(first.version, version);
		ASSERT_EQ(first.limit, 3);
		ASSERT_EQ(first.begin.getKey(), globalConfigDataKeys.begin);
		GetKeyValuesReply page;
		page.version = version;
		page.data.append_deep(page.arena, rows.begin(), 3);
		page.more = true;
		first.reply.send(page);

		auto next = co_await timeoutError(race(read, storage.getKeyValues.getFuture()), 5.0);
		if (next.index() == 1) {
			auto last = std::get<1>(next);
			ASSERT_EQ(last.version, first.version);
			ASSERT_EQ(last.begin.getKey(), keyAfter(rows[2].key));
			ASSERT(snapshot.stamp == previous.stamp);
			ASSERT(snapshot.data == previous.data);
			if (lastPageError.isValid()) {
				if (lastPageError.code() == error_code_actor_cancelled) {
					read.cancel();
				} else {
					last.reply.sendError(lastPageError);
				}
				auto result = co_await timeoutError(errorOr(read), 5.0);
				ASSERT(result.isError());
				ASSERT_EQ(result.getError().code(), lastPageError.code());
				ASSERT(snapshot.stamp == previous.stamp);
				ASSERT(snapshot.data == previous.data);
				co_return;
			}
			GetKeyValuesReply tail;
			tail.version = version;
			tail.data.push_back_deep(tail.arena, rows[3]);
			last.reply.send(tail);
		}
		co_await timeoutError(read, 5.0);
		ASSERT(!lastPageError.isValid());
	};
	co_await readPages(42);
	ASSERT_EQ(snapshot.stamp.version, 42);
	recovery.reply.send(GlobalConfigRefreshReply{ snapshot.data.arena(), snapshot.stamp.version, snapshot.data });
	co_await timeoutError(changed, 5.0);
	ASSERT(!db->globalConfig->get("A"_sr).isValid());
	ASSERT(db->globalConfig->get<bool>("B"_sr, false));
	ASSERT(db->globalConfig->get<bool>("Y"_sr, false));
	ASSERT(db->globalConfig->get<bool>("Z"_sr, false));
	co_await readPages(43, transaction_too_old());
	co_await readPages(44, actor_cancelled());
}

// Handle common GlobalConfig transactions on the server side, because not all
// clients are allowed to read system keys. Eventually, this could become its
// own role.
Future<Void> globalConfigRequestServer(GrvProxyData* grvProxyData, GrvProxyInterface grvProxy) {
	ActorCollection actors(false);
	Future<Void> refreshFuture; // so there is only one running attempt
	GlobalConfigSnapshot snapshot;

	// Attempt to refresh the global configuration while the migration is
	// ongoing. This is a small optimization to avoid waiting for the migration
	// actor to complete.
	refreshFuture =
	    timeout(globalConfigRefresh(grvProxyData, &snapshot), SERVER_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT, Void()) &&
	    delay(SERVER_KNOBS->GLOBAL_CONFIG_REFRESH_INTERVAL);

	// Run one-time migration to support upgrades.
	co_await timeout(globalConfigMigrate(grvProxyData), SERVER_KNOBS->GLOBAL_CONFIG_MIGRATE_TIMEOUT);

	while (true) {
		auto res = co_await race(grvProxy.refreshGlobalConfig.getFuture(), refreshFuture, actors.getResult());
		if (res.index() == 0) {
			GlobalConfigRefreshRequest refresh = std::get<0>(std::move(res));

			// Must have an up to date copy of global configuration in
			// order to serve it to the client (up to date from the clients
			// point of view. The client learns the version through a
			// ClientDBInfo update).
			if (refresh.lastKnown <= snapshot.stamp.version) {
				refresh.reply.send(
				    GlobalConfigRefreshReply{ snapshot.data.arena(), snapshot.stamp.version, snapshot.data });
			} else {
				refresh.reply.sendError(future_version());
			}
		} else if (res.index() == 1) {
			// refreshFuture fired
			refreshFuture = timeout(globalConfigRefresh(grvProxyData, &snapshot),
			                        SERVER_KNOBS->GLOBAL_CONFIG_REFRESH_TIMEOUT,
			                        Void()) &&
			                delay(SERVER_KNOBS->GLOBAL_CONFIG_REFRESH_INTERVAL);
		} else if (res.index() == 2) {
			// actors.getResult() should never return
			ASSERT(false);
		}
	}
}

// Get transaction rate info from RateKeeper.
Future<Void> getRate(UID myID,
                     Reference<AsyncVar<ServerDBInfo> const> db,
                     int64_t* inTransactionCount,
                     int64_t* inBatchTransactionCount,
                     GrvTransactionRateInfo* transactionRateInfo,
                     GrvTransactionRateInfo* batchTransactionRateInfo,
                     GrvRateLeaseState* rateLeaseState,
                     HealthMetricsRequestServer* healthMetricsServer,
                     TransactionTagMap<uint64_t>* transactionTagCounter,
                     PrioritizedTransactionTagMap<ClientTagThrottleLimits>* clientThrottledTags,
                     GrvProxyStats* stats,
                     GrvProxyData* proxyData) {
	Future<Void> nextRequestTimer = Never();
	Future<Void> leaseTimeout = Never();
	Future<GetRateInfoReply> reply = Never();
	double lastDetailedReply = 0.0; // request detailed metrics immediately
	bool expectingDetailedReply = false;
	// state int64_t lastTC = 0;

	if (db->get().ratekeeper.present()) {
		nextRequestTimer = Void();
	}
	while (true) {
		auto res = co_await race(db->onChange(), nextRequestTimer, reply, leaseTimeout);
		if (res.index() == 0) {

			if (db->get().ratekeeper.present()) {
				TraceEvent("ProxyRatekeeperChanged", myID).detail("RKID", db->get().ratekeeper.get().id());
				nextRequestTimer = Void(); // trigger GetRate request
			} else {
				TraceEvent("ProxyRatekeeperDied", myID).log();
				nextRequestTimer = Never();
				reply = Never();
			}
		} else if (res.index() == 1) {

			nextRequestTimer = Never();
			bool detailed = now() - lastDetailedReply > SERVER_KNOBS->DETAILED_METRIC_UPDATE_RATE;

			// Receive ratekeeper replies at socket priority so incoming GRVs cannot starve rate-lease updates.
			reply = brokenPromiseToNever(
			    db->get().ratekeeper.get().getRateInfo.getReply(GetRateInfoRequest(myID,
			                                                                       *inTransactionCount,
			                                                                       *inBatchTransactionCount,
			                                                                       proxyData->version,
			                                                                       *transactionTagCounter,
			                                                                       detailed),
			                                                    TaskPriority::ReadSocket));
			transactionTagCounter->clear();
			expectingDetailedReply = detailed;
		} else if (res.index() == 2) {
			GetRateInfoReply rep = std::get<2>(std::move(res));

			reply = Never();

			transactionRateInfo->setRate(rep.transactionRate);
			batchTransactionRateInfo->setRate(rep.batchTransactionRate);
			*rateLeaseState = GrvRateLeaseState::Active;
			stats->transactionRateAllowed = rep.transactionRate;
			stats->batchTransactionRateAllowed = rep.batchTransactionRate;
			++stats->updatesFromRatekeeper;
			// TraceEvent("GrvProxyRate", myID).detail("Rate", rep.transactionRate).detail("BatchRate", rep.batchTransactionRate).detail("Lease", rep.leaseDuration).detail("ReleasedTransactions", *inTransactionCount - lastTC);
			// lastTC = *inTransactionCount;
			leaseTimeout = delay(rep.leaseDuration);
			nextRequestTimer = delayJittered(rep.leaseDuration / 2);
			healthMetricsServer->update(rep.healthMetrics, expectingDetailedReply);
			if (expectingDetailedReply) {
				lastDetailedReply = now();
			}

			// Replace our throttles with what was sent by ratekeeper. Because we do this,
			// we are not required to expire tags out of the map
			if (rep.clientThrottledTags.present()) {
				*clientThrottledTags = std::move(rep.clientThrottledTags.get());
			}
		} else if (res.index() == 3) {

			transactionRateInfo->disable();
			batchTransactionRateInfo->disable();
			*rateLeaseState = GrvRateLeaseState::Expired;
			++stats->leaseTimeouts;
			TraceEvent(SevWarn, "GrvProxyRateLeaseExpired", myID).suppressFor(5.0);
			//TraceEvent("GrvProxyRate", myID).detail("Rate", 0.0).detail("BatchRate", 0.0).detail("Lease", 0);
			leaseTimeout = Never();
		} else {
			UNREACHABLE();
		}
	}
}

// Respond with an error to the GetReadVersion request when the GRV limit is hit.
void proxyGRVThresholdExceeded(const GetReadVersionRequest* req, GrvProxyStats* stats) {
	++stats->txnRequestErrors;
	req->reply.sendError(grv_proxy_memory_limit_exceeded());
	if (req->priority == TransactionPriority::IMMEDIATE) {
		TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "ProxyGRVThresholdExceededSystem")
		    .suppressFor(60);
	} else if (req->priority == TransactionPriority::DEFAULT) {
		TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "ProxyGRVThresholdExceededDefault")
		    .suppressFor(60);
	} else {
		TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "ProxyGRVThresholdExceededBatch")
		    .suppressFor(60);
	}
}

// Drop a GetReadVersion request from a queue, by responding an error to the request.
void dropRequestFromQueue(Deque<GetReadVersionRequest>* queue,
                          GrvProxyStats* stats,
                          GrvQueueTransactionCounts* queueTransactionCounts) {
	proxyGRVThresholdExceeded(&queue->front(), stats);
	++stats->txnRequestOut;
	queueTransactionCounts->remove(queue->front());
	queue->pop_front();
}

void rejectForMaxGrvQueueDelay(GetReadVersionRequest const& req, GrvProxyStats* stats, double estimatedRemainingDelay) {
	double elapsedQueueDelay = std::max(0.0, now() - req.requestTime());
	TraceEvent("ProxyGRVQueueDelayExceeded")
	    .suppressFor(5.0)
	    .detail("Priority", static_cast<int>(req.priority))
	    .detail("TransactionCount", req.transactionCount)
	    .detail("ElapsedQueueDelay", elapsedQueueDelay)
	    .detail("EstimatedRemainingDelay", estimatedRemainingDelay)
	    .detail("MaxGrvQueueDelayMS", req.maxGrvQueueDelayMS.orDefault(-1));
	req.reply.sendError(transaction_grv_queue_rejected());
	stats->txnThrottled += req.transactionCount;
	++stats->txnRequestErrors;
}

bool rejectIncomingForMaxGrvQueueDelay(GetReadVersionRequest const& req,
                                       GrvQueueTransactionCounts const& queueTransactionCounts,
                                       GrvProxyStats* stats,
                                       GrvTransactionRateInfo const* normalRateInfo,
                                       GrvTransactionRateInfo const* batchRateInfo,
                                       GrvRateLeaseState rateLeaseState) {
	// Requests with this option should be rejected before normal queue accounting
	// or insertion. The estimate uses normal-rate work for every request and adds
	// batch-rate work for batch-priority requests.
	if (!req.maxGrvQueueDelayMS.present()) {
		return false;
	}
	if (req.priority >= TransactionPriority::IMMEDIATE) {
		return false;
	}

	auto estimate = estimateRemainingGrvQueueDelay(
	    req.priority, req.transactionCount, queueTransactionCounts, normalRateInfo, batchRateInfo);
	if (estimate.batchRateDelay.present()) {
		if (shouldRejectForMaxGrvQueueDelay(req, estimate.batchRateDelay.get(), rateLeaseState)) {
			rejectForMaxGrvQueueDelay(req, stats, estimate.batchRateDelay.get());
			return true;
		}
	}

	if (shouldRejectForMaxGrvQueueDelay(req, estimate.normalRateDelay, rateLeaseState)) {
		rejectForMaxGrvQueueDelay(req, stats, estimate.normalRateDelay);
		return true;
	}

	return false;
}

// Put a GetReadVersion request into the queue corresponding to its priority.
Future<Void> queueGetReadVersionRequests(Reference<AsyncVar<ServerDBInfo> const> db,
                                         Deque<GetReadVersionRequest>* systemQueue,
                                         Deque<GetReadVersionRequest>* defaultQueue,
                                         Deque<GetReadVersionRequest>* batchQueue,
                                         FutureStream<GetReadVersionRequest> readVersionRequests,
                                         PromiseStream<Void> GRVTimer,
                                         double* lastGRVTime,
                                         double* GRVBatchTime,
                                         FutureStream<double> normalGRVLatency,
                                         GrvProxyStats* stats,
                                         GrvTransactionRateInfo* normalRateInfo,
                                         GrvTransactionRateInfo* batchRateInfo,
                                         GrvQueueTransactionCounts* queueTransactionCounts,
                                         GrvRateLeaseState const* rateLeaseState) {
	getCurrentLineage()->modify(&TransactionLineage::operation) =
	    TransactionLineage::Operation::GetConsistentReadVersion;
	while (true) {
		auto res = co_await race(readVersionRequests, normalGRVLatency);
		if (res.index() == 0) {
			GetReadVersionRequest req = std::get<0>(std::move(res));

			// auto lineage = make_scoped_lineage(&TransactionLineage::txID, req.spanContext.first());
			// getCurrentLineage()->modify(&TransactionLineage::txID) =
			// WARNING: this code is run at a high priority, so it needs to do as little work as possible
			bool canBeQueued = true;
			if (stats->txnRequestIn.getValue() - stats->txnRequestOut.getValue() >
			        SERVER_KNOBS->START_TRANSACTION_MAX_QUEUE_SIZE ||
			    // Occasionally inject queue pressure in simulation to test queue overflow handling
			    (g_network->isSimulated() && !g_simulator->speedUpSimulation && buggify(0.01))) {
				// When the limit is hit, try to drop requests from the lower priority queues.
				if (req.priority == TransactionPriority::BATCH) {
					canBeQueued = false;
				} else if (req.priority == TransactionPriority::DEFAULT) {
					if (!batchQueue->empty()) {
						dropRequestFromQueue(batchQueue, stats, queueTransactionCounts);
						--stats->batchGRVQueueSize;
					} else {
						canBeQueued = false;
					}
				} else {
					if (!batchQueue->empty()) {
						dropRequestFromQueue(batchQueue, stats, queueTransactionCounts);
						--stats->batchGRVQueueSize;
					} else if (!defaultQueue->empty()) {
						dropRequestFromQueue(defaultQueue, stats, queueTransactionCounts);
						--stats->defaultGRVQueueSize;
					} else {
						canBeQueued = false;
					}
				}
			}
			if (!canBeQueued) {
				proxyGRVThresholdExceeded(&req, stats);
			} else {
				if (rejectIncomingForMaxGrvQueueDelay(
				        req, *queueTransactionCounts, stats, normalRateInfo, batchRateInfo, *rateLeaseState)) {
					continue;
				}

				stats->addRequest(req.transactionCount);

				if (req.debugID.present()) {
					g_traceBatch.addEvent("TransactionDebug",
					                      req.debugID.get().first(),
					                      "GrvProxyServer.queueTransactionStartRequests.Before",
					                      req.spanContext.traceID,
					                      req.spanContext.spanID);
				}

				if (systemQueue->empty() && defaultQueue->empty() && batchQueue->empty()) {
					forwardPromise(Uncancellable{},
					               GRVTimer,
					               delayJittered(std::max(0.0, *GRVBatchTime - (now() - *lastGRVTime)),
					                             TaskPriority::ProxyGRVTimer));
				}

				if (req.priority >= TransactionPriority::IMMEDIATE) {
					++stats->txnRequestIn;
					stats->txnStartIn += req.transactionCount;
					stats->txnSystemPriorityStartIn += req.transactionCount;
					++stats->systemGRVQueueSize;
					queueTransactionCounts->add(req);
					systemQueue->push_back(req);
				} else if (req.priority >= TransactionPriority::DEFAULT) {
					++stats->txnRequestIn;
					stats->txnStartIn += req.transactionCount;
					stats->txnDefaultPriorityStartIn += req.transactionCount;
					++stats->defaultGRVQueueSize;
					queueTransactionCounts->add(req);
					defaultQueue->push_back(req);
				} else {
					// Return error for batch_priority GRV requests
					int64_t proxiesCount = std::max((int)db->get().client.grvProxies.size(), 1);
					if (batchRateInfo->getRate() <= (1.0 / proxiesCount)) {
						req.reply.sendError(batch_transaction_throttled());
						stats->txnThrottled += req.transactionCount;
					} else {
						++stats->txnRequestIn;
						stats->txnStartIn += req.transactionCount;
						stats->txnBatchPriorityStartIn += req.transactionCount;
						++stats->batchGRVQueueSize;
						queueTransactionCounts->add(req);
						batchQueue->push_back(req);
					}
				}
			}
		}
		// Dynamic batching monitors reply latencies.
		else if (res.index() == 1) {
			double reply_latency = std::get<1>(std::move(res));

			double target_latency = reply_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_LATENCY_FRACTION;
			*GRVBatchTime = std::max(
			    SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN,
			    std::min(SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MAX,
			             target_latency * SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA +
			                 *GRVBatchTime * (1 - SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_SMOOTHER_ALPHA)));
		} else {
			UNREACHABLE();
		}
	}
}

Future<Void> updateLastCommit(GrvProxyData* self, Optional<UID> debugID = Optional<UID>()) {
	double confirmStart = now();
	self->lastStartCommit = confirmStart;
	self->updateCommitRequests++;
	co_await self->logSystem->confirmEpochLive(debugID);
	self->updateCommitRequests--;
	self->lastCommitLatency = now() - confirmStart;
	self->lastCommitTime = std::max(self->lastCommitTime.get(), confirmStart);
}

Future<Void> lastCommitUpdater(GrvProxyData* self, PromiseStream<Future<Void>> addActor) {
	while (true) {
		double interval = std::max(SERVER_KNOBS->MIN_CONFIRM_INTERVAL,
		                           (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION - self->lastCommitLatency) / 2.0);
		double elapsed = now() - self->lastStartCommit;
		if (elapsed < interval) {
			co_await delay(interval + 0.0001 - elapsed);
		} else {
			// May want to change the default value of MAX_COMMIT_UPDATES since we don't have
			if (self->updateCommitRequests < SERVER_KNOBS->MAX_COMMIT_UPDATES) {
				addActor.send(updateLastCommit(self));
			} else {
				TraceEvent(g_network->isSimulated() ? SevInfo : SevWarnAlways, "TooManyLastCommitUpdates")
				    .suppressFor(1.0);
				self->lastStartCommit = now();
			}
		}
	}
}

Optional<UID> getDebugID(Optional<BatchDebugIDs> debugIDs) {
	return debugIDs.present() ? Optional<UID>(debugIDs.get().debugID) : Optional<UID>();
}

Future<GetReadVersionReply> getLiveCommittedVersion(std::vector<SpanContext> spanContexts,
                                                    GrvProxyData* grvProxyData,
                                                    uint32_t flags,
                                                    Optional<BatchDebugIDs> debugIDs,
                                                    int transactionCount,
                                                    int systemTransactionCount,
                                                    int defaultPriTransactionCount,
                                                    int batchPriTransactionCount) {
	// Returns a version which (1) is committed, and (2) is >= the latest version reported committed (by a commit
	// response) when this request was sent (1) The version returned is the committedVersion of some proxy at some point
	// before the request returns, so it is committed. (2) No proxy on our list reported committed a higher version
	// before this request was received, because then its committedVersion would have been higher,
	//     and no other proxy could have already committed anything without first ending the epoch
	Span span("GP:getLiveCommittedVersion"_loc);
	for (const SpanContext& spanContext : spanContexts) {
		span.addLink(spanContext);
	}
	++grvProxyData->stats.txnStartBatch;

	double grvStart = now();
	Optional<UID> debugID = getDebugID(debugIDs);
	Future<GetRawCommittedVersionReply> replyFromMasterFuture;
	// Receive master replies at socket priority so incoming GRVs cannot starve an already-arrived reply.
	replyFromMasterFuture = grvProxyData->master.getLiveCommittedVersion.getReply(
	    GetRawCommittedVersionRequest(span.context, debugID, grvProxyData->ssVersionVectorCache.getMaxVersion()),
	    TaskPriority::ReadSocket);

	if (!SERVER_KNOBS->ALWAYS_CAUSAL_READ_RISKY && !(flags & GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY)) {
		co_await transformError(updateLastCommit(grvProxyData, debugID), broken_promise(), tlog_failed());
	} else if (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > 0 &&
	           now() - SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > grvProxyData->lastCommitTime.get()) {
		co_await grvProxyData->lastCommitTime.whenAtLeast(now() - SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION);
	}

	double grvConfirmEpochLive = now();
	grvProxyData->stats.grvConfirmEpochLiveDist->sampleSeconds(grvConfirmEpochLive - grvStart);
	if (debugID.present()) {
		g_traceBatch.addEvent("TransactionDebug",
		                      debugID.get().first(),
		                      "GrvProxyServer.getLiveCommittedVersion.confirmEpochLive",
		                      debugIDs.get().debugTraceID,
		                      debugIDs.get().debugSpanID);
	}

	GetRawCommittedVersionReply repFromMaster =
	    co_await transformError(replyFromMasterFuture, broken_promise(), master_failed());
	grvProxyData->version = std::max(grvProxyData->version, repFromMaster.version);
	grvProxyData->minKnownCommittedVersion =
	    std::max(grvProxyData->minKnownCommittedVersion, repFromMaster.minKnownCommittedVersion);
	if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
		grvProxyData->ssVersionVectorCache.applyDelta(repFromMaster.ssVersionVectorDelta);
		grvProxyData->stats.versionVectorSize = grvProxyData->ssVersionVectorCache.size();
		grvProxyData->stats.versionVectorEncodedBytes = grvProxyData->ssVersionVectorCache.getEncodedSize();
	}
	grvProxyData->stats.grvGetCommittedVersionRpcDist->sampleSeconds(now() - grvConfirmEpochLive);
	GetReadVersionReply rep;
	rep.version = repFromMaster.version;
	rep.locked = repFromMaster.locked;
	rep.metadataVersion = repFromMaster.metadataVersion;
	rep.processBusyTime =
	    FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION *
	    std::min((std::numeric_limits<int>::max() / FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION) - 1,
	             grvProxyData->stats.getRecentRequests());
	rep.processBusyTime += FLOW_KNOBS->BASIC_LOAD_BALANCE_COMPUTE_PRECISION *
	                       (g_network->isSimulated() ? deterministicRandom()->random01()
	                                                 : g_network->networkInfo.metrics.lastRunLoopBusyness);

	if (debugID.present()) {
		g_traceBatch.addEvent("TransactionDebug",
		                      debugID.get().first(),
		                      "GrvProxyServer.getLiveCommittedVersion.After",
		                      debugIDs.get().debugTraceID,
		                      debugIDs.get().debugSpanID);
	}

	grvProxyData->stats.txnStartOut += transactionCount;
	grvProxyData->stats.txnSystemPriorityStartOut += systemTransactionCount;
	grvProxyData->stats.txnDefaultPriorityStartOut += defaultPriTransactionCount;
	grvProxyData->stats.txnBatchPriorityStartOut += batchPriTransactionCount;

	co_return rep;
}

// Returns the current read version (or minimum known committed version if requested),
// to each request in the provided list. Also check if the request should be throttled.
// Update GRV statistics according to the request's priority.
Future<Void> sendGrvReplies(Future<GetReadVersionReply> replyFuture,
                            std::vector<GetReadVersionRequest> requests,
                            GrvProxyData* grvProxyData,
                            GrvProxyStats* stats,
                            Version minKnownCommittedVersion,
                            PrioritizedTransactionTagMap<ClientTagThrottleLimits> clientThrottledTags,
                            int64_t midShardSize = 0) {
	GetReadVersionReply reply = co_await replyFuture;
	Version replyVersion = reply.version;

	double end = g_network->timer();
	for (GetReadVersionRequest const& request : requests) {
		double duration = end - request.requestTime();
		if (request.priority == TransactionPriority::BATCH) {
			stats->grvBatchLatencySample.addMeasurement(duration);
		}

		if (request.priority == TransactionPriority::DEFAULT) {
			stats->grvLatencySample.addMeasurement(duration);
		}

		if (request.priority >= TransactionPriority::DEFAULT) {
			stats->grvLatencyBands.addMeasurement(duration);
		}

		if (request.flags & GetReadVersionRequest::FLAG_USE_MIN_KNOWN_COMMITTED_VERSION) {
			// Only backup worker may infrequently use this flag.
			reply.version = minKnownCommittedVersion;
		} else {
			reply.version = replyVersion;
		}
		reply.midShardSize = midShardSize;
		reply.tagThrottleInfo.clear();
		if (SERVER_KNOBS->ENABLE_VERSION_VECTOR) {
			grvProxyData->ssVersionVectorCache.getDelta(request.maxVersion, reply.ssVersionVectorDelta);
			grvProxyData->versionVectorSizeOnGRVReply->addMeasurement(reply.ssVersionVectorDelta.size());
		}
		reply.proxyId = grvProxyData->dbgid;

		if (request.isTagged()) {
			auto& priorityThrottledTags = clientThrottledTags[request.priority];
			for (const auto& tag : request.tags) {
				auto tagItr = priorityThrottledTags.find(tag.first);
				if (tagItr != priorityThrottledTags.end()) {
					if (tagItr->second.expiration > now()) {
						if (tagItr->second.tpsRate == std::numeric_limits<double>::max()) {
							CODE_PROBE(true, "Auto TPS rate is unlimited");
						} else {
							CODE_PROBE(true, "GRV proxy returning tag throttle");
							reply.tagThrottleInfo[tag.first] = tagItr->second;
						}
					} else {
						// This isn't required, but we might as well
						CODE_PROBE(true, "GRV proxy expiring tag throttle");
						priorityThrottledTags.erase(tagItr);
					}
				}
			}
		}

		if (stats->lastBatchQueueThrottled) {
			// Check if this throttling has been sustained for a certain amount of time to avoid false positives
			if (now() - stats->batchThrottleStartTime > CLIENT_KNOBS->GRV_SUSTAINED_THROTTLING_THRESHOLD) {
				reply.rkBatchThrottled = true;
			}
		}
		if (stats->lastDefaultQueueThrottled) {
			// Check if this throttling has been sustained for a certain amount of time to avoid false positives
			if (now() - stats->defaultThrottleStartTime > CLIENT_KNOBS->GRV_SUSTAINED_THROTTLING_THRESHOLD) {
				// Consider the batch queue throttled if the default is throttled
				// to deal with a potential lull in activity for that priority.
				// Avoids mistakenly thinking batch is unthrottled while default is still throttled.
				reply.rkBatchThrottled = true;
				reply.rkDefaultThrottled = true;
			}
		}
		request.reply.send(reply);
		++stats->txnRequestOut;
	}
}

Future<Void> monitorDDMetricsChanges(int64_t* midShardSize, Reference<AsyncVar<ServerDBInfo> const> db) {
	Future<Void> nextRequestTimer = Never();
	Future<GetDataDistributorMetricsReply> nextReply = Never();

	if (db->get().distributor.present()) {
		nextRequestTimer = Void();
	}
	while (true) {
		try {
			auto res = co_await race(db->onChange(), nextRequestTimer, nextReply);
			if (res.index() == 0) {
				if (db->get().distributor.present()) {
					TraceEvent("DataDistributorChanged", db->get().id).detail("DDID", db->get().distributor.get().id());
					nextRequestTimer = Void();
				} else {
					TraceEvent("DataDistributorDied", db->get().id);
					nextRequestTimer = Never();
				}
				nextReply = Never();
			} else if (res.index() == 1) {
				nextRequestTimer = Never();
				if (db->get().distributor.present()) {
					nextReply = brokenPromiseToNever(db->get().distributor.get().dataDistributorMetrics.getReply(
					    GetDataDistributorMetricsRequest(normalKeys, CLIENT_KNOBS->TOO_MANY, true)));
				} else {
					nextReply = Never();
				}
			} else if (res.index() == 2) {
				GetDataDistributorMetricsReply reply = std::get<2>(std::move(res));

				nextReply = Never();
				ASSERT(reply.midShardSize.present());
				*midShardSize = reply.midShardSize.get();
				nextRequestTimer = delay(CLIENT_KNOBS->MID_SHARD_SIZE_MAX_STALENESS);
			} else {
				UNREACHABLE();
			}
		} catch (Error& e) {
			TraceEvent("DDMidShardSizeUpdateFail").error(e);
			if (e.code() != error_code_timed_out && e.code() != error_code_dd_not_found) {
				throw;
			}
			nextRequestTimer = delay(CLIENT_KNOBS->MID_SHARD_SIZE_MAX_STALENESS);
			nextReply = Never();
		}
	}
}

static Future<Void> grvProxyProgressCanary(Reference<GrvProxyStarvationDetector> starvationDetector) {
	while (true) {
		// Run below every priority needed to release and complete a GRV. If this canary runs, the GRV path is not
		// being starved by higher-priority work.
		co_await delay(SERVER_KNOBS->GRV_PROXY_PROGRESS_CHECK_INTERVAL, TaskPriority::GetLiveCommittedVersion);
		// Simulation does not schedule by priority. Occasionally skip this lower-priority progress signal so a rare
		// run of consecutive skips simulates CPU starvation and exercises the watchdog.
		if (!buggify()) {
			starvationDetector->recordProgress();
		}
	}
}

static Future<Void> grvProxyStarvationWatchdog(UID proxyId, Reference<GrvProxyStarvationDetector> starvationDetector) {
	while (true) {
		// Flow transport pings run at ReadSocket. If this check runs while the lower-priority canary does not,
		// the process still looks healthy to transport but cannot make GRV progress.
		co_await delay(SERVER_KNOBS->GRV_PROXY_PROGRESS_CHECK_INTERVAL, TaskPriority::ReadSocket);
		if (starvationDetector->checkForStarvation()) {
			TraceEvent(SevWarnAlways, "GrvProxyCpuStarved", proxyId)
			    .detail("ConsecutiveMissedProgressChecks", starvationDetector->getConsecutiveMisses())
			    .detail("ProgressCheckInterval", SERVER_KNOBS->GRV_PROXY_PROGRESS_CHECK_INTERVAL)
			    .detail("ProgressPriority", static_cast<int>(TaskPriority::GetLiveCommittedVersion));
			throw failed_to_progress();
		}
	}
}

static Future<Void> transactionStarter(GrvProxyInterface proxy,
                                       Reference<AsyncVar<ServerDBInfo> const> db,
                                       PromiseStream<Future<Void>> addActor,
                                       GrvProxyData* grvProxyData,
                                       HealthMetricsRequestServer* healthMetricsServer) {
	double lastGRVTime = 0;
	PromiseStream<Void> GRVTimer;
	double GRVBatchTime = SERVER_KNOBS->START_TRANSACTION_BATCH_INTERVAL_MIN;

	int64_t transactionCount = 0;
	int64_t batchTransactionCount = 0;
	GrvTransactionRateInfo normalRateInfo(SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW,
	                                      SERVER_KNOBS->START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET,
	                                      /*rate=*/10);
	GrvTransactionRateInfo batchRateInfo(SERVER_KNOBS->START_TRANSACTION_RATE_WINDOW,
	                                     SERVER_KNOBS->START_TRANSACTION_MAX_EMPTY_QUEUE_BUDGET,
	                                     /*rate=*/0);
	GrvRateLeaseState rateLeaseState = GrvRateLeaseState::Unknown;

	Deque<GetReadVersionRequest> systemQueue;
	Deque<GetReadVersionRequest> defaultQueue;
	Deque<GetReadVersionRequest> batchQueue;
	GrvQueueTransactionCounts queueTransactionCounts;

	TransactionTagMap<uint64_t> transactionTagCounter;
	PrioritizedTransactionTagMap<ClientTagThrottleLimits> clientThrottledTags;

	PromiseStream<double> normalGRVLatency;

	int64_t midShardSize = SERVER_KNOBS->MIN_SHARD_BYTES;
	getCurrentLineage()->modify(&TransactionLineage::operation) =
	    TransactionLineage::Operation::GetConsistentReadVersion;
	addActor.send(monitorDDMetricsChanges(&midShardSize, db));

	addActor.send(getRate(proxy.id(),
	                      db,
	                      &transactionCount,
	                      &batchTransactionCount,
	                      &normalRateInfo,
	                      &batchRateInfo,
	                      &rateLeaseState,
	                      healthMetricsServer,
	                      &transactionTagCounter,
	                      &clientThrottledTags,
	                      &grvProxyData->stats,
	                      grvProxyData));
	addActor.send(queueGetReadVersionRequests(db,
	                                          &systemQueue,
	                                          &defaultQueue,
	                                          &batchQueue,
	                                          proxy.getConsistentReadVersion.getFuture(),
	                                          GRVTimer,
	                                          &lastGRVTime,
	                                          &GRVBatchTime,
	                                          normalGRVLatency.getFuture(),
	                                          &grvProxyData->stats,
	                                          &normalRateInfo,
	                                          &batchRateInfo,
	                                          &queueTransactionCounts,
	                                          &rateLeaseState));

	while (std::find(db->get().client.grvProxies.begin(), db->get().client.grvProxies.end(), proxy) ==
	       db->get().client.grvProxies.end()) {
		co_await db->onChange();
	}

	ASSERT(db->get().recoveryState >=
	       RecoveryState::ACCEPTING_COMMITS); // else potentially we could return uncommitted read versions from master.
	TraceEvent("GrvProxyReadyForTxnStarts", proxy.id());

	while (true) {
		co_await GRVTimer.getFuture();
		// Select zero or more transactions to start
		double t = now();
		double elapsed = now() - lastGRVTime;
		lastGRVTime = t;

		// Resolve a possible indeterminate multiplication with infinite transaction rate
		if (elapsed == 0) {
			elapsed = 1e-15;
		}

		normalRateInfo.startReleaseWindow();
		batchRateInfo.startReleaseWindow();

		grvProxyData->stats.transactionLimit = normalRateInfo.getLimit();
		grvProxyData->stats.batchTransactionLimit = batchRateInfo.getLimit();

		int transactionsStarted[2] = { 0, 0 };
		int systemTransactionsStarted[2] = { 0, 0 };
		int defaultPriTransactionsStarted[2] = { 0, 0 };
		int batchPriTransactionsStarted[2] = { 0, 0 };

		std::vector<std::vector<GetReadVersionRequest>> start(
		    2); // start[0] is transactions starting with !(flags&CAUSAL_READ_RISKY), start[1] is transactions starting
		        // with flags&CAUSAL_READ_RISKY
		Optional<BatchDebugIDs> debugIDs;

		int requestsToStart = 0;

		uint32_t defaultQueueSize = defaultQueue.size();
		uint32_t batchQueueSize = batchQueue.size();
		while (requestsToStart < SERVER_KNOBS->START_TRANSACTION_MAX_REQUESTS_TO_START) {
			Deque<GetReadVersionRequest>* transactionQueue;
			if (!systemQueue.empty()) {
				transactionQueue = &systemQueue;
			} else if (!defaultQueue.empty()) {
				transactionQueue = &defaultQueue;
			} else if (!batchQueue.empty()) {
				transactionQueue = &batchQueue;
			} else {
				break;
			}

			auto& req = transactionQueue->front();
			int tc = req.transactionCount;

			if (req.priority < TransactionPriority::DEFAULT &&
			    !batchRateInfo.canStart(transactionsStarted[0] + transactionsStarted[1], tc)) {
				break;
			} else if (req.priority < TransactionPriority::IMMEDIATE &&
			           !normalRateInfo.canStart(transactionsStarted[0] + transactionsStarted[1], tc)) {
				break;
			}

			if (req.debugID.present()) {
				if (!debugIDs.present()) {
					debugIDs = BatchDebugIDs{ nondeterministicRandom()->randomUniqueID(),
						                      req.spanContext.traceID,
						                      req.spanContext.spanID };
				}
				Optional<UID> debugID = getDebugID(debugIDs);
				g_traceBatch.addAttach("TransactionAttachID",
				                       req.debugID.get().first(),
				                       debugID.get().first(),
				                       req.spanContext.traceID,
				                       req.spanContext.spanID);
			}

			transactionsStarted[req.flags & 1] += tc;
			double currentTime = g_network->timer();
			if (req.priority >= TransactionPriority::IMMEDIATE) {
				systemTransactionsStarted[req.flags & 1] += tc;
				--grvProxyData->stats.systemGRVQueueSize;
			} else if (req.priority >= TransactionPriority::DEFAULT) {
				defaultPriTransactionsStarted[req.flags & 1] += tc;
				grvProxyData->stats.defaultTxnGRVTimeInQueue.addMeasurement(currentTime - req.requestTime());
				--grvProxyData->stats.defaultGRVQueueSize;
			} else {
				batchPriTransactionsStarted[req.flags & 1] += tc;
				grvProxyData->stats.batchTxnGRVTimeInQueue.addMeasurement(currentTime - req.requestTime());
				--grvProxyData->stats.batchGRVQueueSize;
			}
			for (const auto& [tag, count] : req.tags) {
				transactionTagCounter[tag] += count;
			}
			queueTransactionCounts.remove(req);
			start[req.flags & 1].push_back(std::move(req));
			static_assert(GetReadVersionRequest::FLAG_CAUSAL_READ_RISKY == 1, "Implementation dependent on flag value");
			transactionQueue->pop_front();
			requestsToStart++;
		}
		if (!batchQueue.empty()) {
			if (!grvProxyData->stats.lastBatchQueueThrottled) {
				grvProxyData->stats.lastBatchQueueThrottled = true;
				grvProxyData->stats.batchThrottleStartTime = now();
			}
		} else {
			grvProxyData->stats.lastBatchQueueThrottled = false;
		}
		if (!defaultQueue.empty()) {
			if (!grvProxyData->stats.lastDefaultQueueThrottled) {
				grvProxyData->stats.lastDefaultQueueThrottled = true;
				grvProxyData->stats.defaultThrottleStartTime = now();
			}
		} else {
			grvProxyData->stats.lastDefaultQueueThrottled = false;
		}

		if (!systemQueue.empty() || !defaultQueue.empty() || !batchQueue.empty()) {
			forwardPromise(
			    Uncancellable{},
			    GRVTimer,
			    delayJittered(SERVER_KNOBS->START_TRANSACTION_BATCH_QUEUE_CHECK_INTERVAL, TaskPriority::ProxyGRVTimer));
		}

		/*TraceEvent("GRVBatch", proxy.id())
		    .detail("Elapsed", elapsed)
		    .detail("NTransactionToStart", nTransactionsToStart)
		    .detail("TransactionRate", transactionRate)
		    .detail("TransactionQueueSize", transactionQueue.size())
		    .detail("NumTransactionsStarted", transactionsStarted[0] + transactionsStarted[1])
		    .detail("NumSystemTransactionsStarted", systemTransactionsStarted[0] + systemTransactionsStarted[1])
		    .detail("NumNonSystemTransactionsStarted", transactionsStarted[0] + transactionsStarted[1] -
		                                                systemTransactionsStarted[0] - systemTransactionsStarted[1])
		    .detail("TransactionBudget", transactionBudget)
		    .detail("BatchTransactionBudget", batchTransactionBudget);*/

		int systemTotalStarted = systemTransactionsStarted[0] + systemTransactionsStarted[1];
		int normalTotalStarted = defaultPriTransactionsStarted[0] + defaultPriTransactionsStarted[1];
		int batchTotalStarted = batchPriTransactionsStarted[0] + batchPriTransactionsStarted[1];

		transactionCount += transactionsStarted[0] + transactionsStarted[1];
		batchTransactionCount += batchTotalStarted;

		normalRateInfo.endReleaseWindow(
		    systemTotalStarted + normalTotalStarted, systemQueue.empty() && defaultQueue.empty(), elapsed);
		batchRateInfo.endReleaseWindow(systemTotalStarted + normalTotalStarted + batchTotalStarted,
		                               systemQueue.empty() && defaultQueue.empty() && batchQueue.empty(),
		                               elapsed);

		if (debugIDs.present()) {
			Optional<UID> debugID = getDebugID(debugIDs);
			g_traceBatch.addEvent("TransactionDebug",
			                      debugID.get().first(),
			                      "GrvProxyServer.transactionStarter.AskLiveCommittedVersionFromMaster",
			                      debugIDs.get().debugTraceID,
			                      debugIDs.get().debugSpanID);
		}

		int defaultGRVProcessed = 0;
		int batchGRVProcessed = 0;
		for (int i = 0; i < start.size(); i++) {
			if (!start[i].empty()) {
				std::vector<SpanContext> spanContexts;
				spanContexts.reserve(start[i].size());
				for (const GetReadVersionRequest& request : start[i]) {
					spanContexts.push_back(request.spanContext);
				}

				Future<GetReadVersionReply> readVersionReply = getLiveCommittedVersion(spanContexts,
				                                                                       grvProxyData,
				                                                                       i,
				                                                                       debugIDs,
				                                                                       transactionsStarted[i],
				                                                                       systemTransactionsStarted[i],
				                                                                       defaultPriTransactionsStarted[i],
				                                                                       batchPriTransactionsStarted[i]);
				addActor.send(sendGrvReplies(readVersionReply,
				                             start[i],
				                             grvProxyData,
				                             &grvProxyData->stats,
				                             grvProxyData->minKnownCommittedVersion,
				                             clientThrottledTags,
				                             midShardSize));

				// Use normal priority transaction's GRV latency to dynamically calculate transaction batching interval.
				if (i == 0) {
					addActor.send(timeReply(readVersionReply, normalGRVLatency));
				}
				defaultGRVProcessed += defaultPriTransactionsStarted[i];
				batchGRVProcessed += batchPriTransactionsStarted[i];
			}
		}

		grvProxyData->stats.percentageOfDefaultGRVQueueProcessed =
		    defaultQueueSize ? (double)defaultGRVProcessed / defaultQueueSize : 1;
		grvProxyData->stats.percentageOfBatchGRVQueueProcessed =
		    batchQueueSize ? (double)batchGRVProcessed / batchQueueSize : 1;
	}
}

Future<Void> grvProxyServerCore(GrvProxyInterface proxy,
                                MasterInterface master,
                                LifetimeToken masterLifetime,
                                Reference<AsyncVar<ServerDBInfo> const> db) {
	GrvProxyData grvProxyData(proxy.id(), master, proxy.getConsistentReadVersion, db);

	PromiseStream<Future<Void>> addActor;
	Future<Void> onError = actorCollection(addActor.getFuture());

	HealthMetricsRequestServer healthMetricsServer(proxy);

	addActor.send(waitFailureServer(proxy.waitFailure.getFuture()));
	addActor.send(traceRole(Role::GRV_PROXY, proxy.id()));

	TraceEvent("GrvProxyServerCore", proxy.id())
	    .detail("MasterId", master.id())
	    .detail("MasterLifetime", masterLifetime.toString())
	    .detail("RecoveryCount", db->get().recoveryCount);

	// Wait until we can load the "real" logsystem, since we don't support switching them currently
	while (!(masterLifetime.isEqual(grvProxyData.db->get().masterLifetime) &&
	         grvProxyData.db->get().recoveryState >= RecoveryState::ACCEPTING_COMMITS)) {
		co_await grvProxyData.db->onChange();
	}
	// Do we need to wait for any db info change? Yes. To update latency band.
	Future<Void> dbInfoChange = grvProxyData.db->onChange();
	grvProxyData.logSystem = makeLogSystemFromServerDBInfo(proxy.id(), grvProxyData.db->get(), false, addActor);

	grvProxyData.updateLatencyBandConfig(grvProxyData.db->get().latencyBandConfig);
	// Start before transactionStarter waits to observe this proxy in dbInfo. The interface can already be visible to
	// clients at that point, so waiting for GrvProxyReadyForTxnStarts would leave a startup starvation window.
	auto starvationDetector =
	    makeReference<GrvProxyStarvationDetector>(SERVER_KNOBS->GRV_PROXY_MAX_MISSED_PROGRESS_CHECKS);
	addActor.send(grvProxyProgressCanary(starvationDetector));
	addActor.send(grvProxyStarvationWatchdog(proxy.id(), starvationDetector));

	addActor.send(transactionStarter(proxy, grvProxyData.db, addActor, &grvProxyData, &healthMetricsServer));
	addActor.send(healthMetricsServer.run());
	addActor.send(globalConfigRequestServer(&grvProxyData, proxy));

	if (SERVER_KNOBS->REQUIRED_MIN_RECOVERY_DURATION > 0) {
		addActor.send(lastCommitUpdater(&grvProxyData, addActor));
	}

	while (true) {
		co_await waitOrError(dbInfoChange, onError);
		dbInfoChange = grvProxyData.db->onChange();

		if (masterLifetime.isEqual(grvProxyData.db->get().masterLifetime) &&
		    grvProxyData.db->get().recoveryState >= RecoveryState::RECOVERY_TRANSACTION) {
			grvProxyData.logSystem = makeLogSystemFromServerDBInfo(proxy.id(), grvProxyData.db->get(), false, addActor);
		}
		grvProxyData.updateLatencyBandConfig(grvProxyData.db->get().latencyBandConfig);
	}
}

Future<Void> checkRemoved(Reference<AsyncVar<ServerDBInfo> const> db,
                          uint64_t recoveryCount,
                          GrvProxyInterface myInterface) {
	while (true) {
		if (db->get().recoveryCount >= recoveryCount &&
		    std::find(db->get().client.grvProxies.begin(), db->get().client.grvProxies.end(), myInterface) ==
		        db->get().client.grvProxies.end()) {
			throw worker_removed();
		}
		co_await db->onChange();
	}
}

Future<Void> grvProxyServer(GrvProxyInterface proxy,
                            InitializeGrvProxyRequest req,
                            Reference<AsyncVar<ServerDBInfo> const> db) {
	try {
		Future<Void> core = grvProxyServerCore(proxy, req.master, req.masterLifetime, db);
		co_await (core || checkRemoved(db, req.recoveryCount, proxy));
	} catch (Error& e) {
		Severity sev = e.code() == error_code_failed_to_progress ? SevWarnAlways : SevInfo;
		TraceEvent(sev, "GrvProxyTerminated", proxy.id()).errorUnsuppressed(e);
		ASSERT(e.code() !=
		       error_code_broken_promise); // all broken_promise should be transformed to the correct error code
		CODE_PROBE(e.code() == error_code_master_failed, "GrvProxyServer master failed");
		CODE_PROBE(e.code() == error_code_tlog_failed, "GrvProxyServer tlog failed", probe::decoration::rare);
		CODE_PROBE(e.code() == error_code_failed_to_progress, "GRV proxy failed to progress");
		if (e.code() != error_code_worker_removed && e.code() != error_code_tlog_stopped &&
		    e.code() != error_code_tlog_failed && e.code() != error_code_coordinators_changed &&
		    e.code() != error_code_coordinated_state_conflict && e.code() != error_code_new_coordinators_timed_out &&
		    e.code() != error_code_master_failed && e.code() != error_code_failed_to_progress) {
			throw;
		}
	}
}
