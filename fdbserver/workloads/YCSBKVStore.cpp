/*
 * YCSBKVStore.cpp
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

// YCSB directly against a single storage engine instance in the tester process, without a cluster, to compare the
// engines themselves. A test either loads a store file (optionally in random key order and/or followed by vacuum()
// to finish the engine's deferred work) or reopens one loaded by an earlier test (skipLoad), and then runs a fixed
// mix of reads and updates for testDuration. Reports load throughput, storage amplification, and overall and
// per-window latency distributions.

#include <optional>
#include <string>
#include <vector>

#if defined(__linux__)
#include <fcntl.h>
#include <unistd.h>
#endif

#include "fdbclient/FDBTypes.h"
#include "fdbclient/KnobValue.h"
#include "fdbrpc/DDSketch.h"
#include "fdbrpc/simulator.h"
#include "fdbrpc/SimulatorProcessInfo.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/ServerDBInfo.h"
#include "fdbserver/kvstore/IKeyValueStore.h"
#include "fdbserver/tester/workloads.h"
#include "flow/CodeProbe.h"
#include "flow/FeistelPermutation.h"
#include "flow/Platform.h"
#include "YCSBCommon.h"

namespace {

// Server knobs set for as long as a store is open and then put back, since tests run one after another in the same
// tester process. Knobs that engines read when opening a store are the only way to configure them per store.
class ScopedServerKnobs {
public:
	ScopedServerKnobs() = default;
	ScopedServerKnobs(ScopedServerKnobs const&) = delete;
	ScopedServerKnobs& operator=(ScopedServerKnobs const&) = delete;
	~ScopedServerKnobs() { restore(); }

	void set(std::string const& name, ParsedKnobValue const& value) {
		original_.emplace_back(name, KnobValueRef::create(SERVER_KNOBS->getKnob(name)));
		setServerKnob(name, KnobValueRef::create(value));
	}

	void restore() {
		for (auto it = original_.rbegin(); it != original_.rend(); ++it) {
			setServerKnob(it->first, it->second);
		}
		original_.clear();
	}

private:
	std::vector<std::pair<std::string, Standalone<KnobValueRef>>> original_;
};

// Evicts a file from the OS page cache, returning whether it existed
bool dropOsPageCache(std::string const& path) {
#if defined(__linux__)
	int const fd = ::open(path.c_str(), O_RDONLY);
	if (fd < 0) {
		return false;
	}
	// Dirty pages can't be evicted, so make sure there are none
	::fdatasync(fd);
	int const err = ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED);
	::close(fd);
	if (err != 0) {
		TraceEvent(SevError, "YCSBKVStoreDropOsPageCacheFailed").detail("Path", path).detail("Error", err);
		throw platform_error();
	}
	return true;
#else
	return false;
#endif
}

} // namespace

struct YCSBKVStoreWorkload : YCSBBase {
	static constexpr auto NAME = "YCSBKVStore";

	// config
	double testDuration{ 0.0 }; // 0 = load only
	int numActors{ 0 };
	int commitInterval{ 0 }; // commit every N staged writes
	int targetTps{ 0 }; // 0 = unlimited
	KeyValueStoreType storeType;
	std::string filename; // empty = generated, and deleted when done; otherwise kept, and recreated unless skipLoad
	bool disposeStore{ false };
	bool skipLoad{ false };
	bool loadRandomOrder{ false };
	uint64_t loadShuffleSeed{ 0 }; // fixed rather than seeded per run so the load order is identical across runs
	int64_t loadBatchBytes{ 0 }; // commit every N logical bytes during the load
	bool vacuum{ false };
	// Keep the engine from doing any background restructuring while the store is open, so reads see the layout the
	// load left. Only RocksDB does background work without commits.
	bool pauseBackgroundWork{ false };
	bool dropCaches{ false }; // evict the in-process and OS page caches before the measured phase
	int64_t pageCacheBytes{ 0 };

	// state
	IKeyValueStore* kvs{ nullptr };
	std::string fn;
	ScopedServerKnobs storeKnobs;
	bool done{ false };
	int64_t pendingWrites{ 0 };
	// Wakes the committer. Not a Promise the committer replaces when it wakes, since it wakes inside send() and that
	// would destroy the promise mid-send.
	AsyncTrigger commitReady;
	std::optional<YCSBZipfianGenerator> zipfian;

	// metrics
	PerfIntCounter operations{ "Operations" };
	PerfIntCounter totalCommits{ "Commits" };
	DDSketch<double> latencies, readLatencies, updateLatencies, commitLatencies;
	std::unique_ptr<LatencySample> windowCommitLatencies;
	double testDurationActual{ 0.0 };
	int64_t loadLogicalBytes{ 0 };
	double loadDuration{ 0.0 };
	double vacuumDuration{ 0.0 };
	int64_t loadPhysicalBytes{ 0 };
	// space in the store's files that is free for reuse, so amplification can be reported with and without it
	int64_t loadReusableBytes{ 0 };

	explicit YCSBKVStoreWorkload(WorkloadContext const& wcx) : YCSBBase(wcx) {
		testDuration = getOption(options, "testDuration"_sr, 10.0);
		ASSERT(testDuration >= 0);
		numActors = getOption(options, "numActors"_sr, 4);
		ASSERT(numActors > 0);
		commitInterval = getOption(options, "commitInterval"_sr, 1000);
		targetTps = getOption(options, "tps"_sr, 0);
		ASSERT(targetTps >= 0);

		storeType = KeyValueStoreType::fromString(getOption(options, "storeType"_sr, "ssd-redwood-1"_sr).toString());
		ASSERT(storeType == KeyValueStoreType::SSD_REDWOOD_V1 || storeType == KeyValueStoreType::SSD_BTREE_V2 ||
		       storeType == KeyValueStoreType::SSD_ROCKSDB_V1 || storeType == KeyValueStoreType::MEMORY);

		filename = getOption(options, "filename"_sr, Value()).toString();
		disposeStore = filename.empty();
		skipLoad = getOption(options, "skipLoad"_sr, false);
		ASSERT(!skipLoad || !filename.empty());

		std::string const loadOrder = getOption(options, "loadOrder"_sr, "sequential"_sr).toString();
		ASSERT(loadOrder == "sequential" || loadOrder == "random");
		loadRandomOrder = loadOrder == "random";
		loadShuffleSeed = getOption(options, "loadShuffleSeed"_sr, static_cast<uint64_t>(0x9c5b));
		loadBatchBytes = getOption(options, "loadBatchBytes"_sr, static_cast<int64_t>(10e6));
		ASSERT(loadBatchBytes > 0);
		vacuum = getOption(options, "vacuum"_sr, false);
		ASSERT(!skipLoad || !vacuum);
		ASSERT(!vacuum || storeType != KeyValueStoreType::MEMORY);
		pauseBackgroundWork = getOption(options, "pauseBackgroundWork"_sr, false);
		ASSERT(!(pauseBackgroundWork && vacuum));
		dropCaches = getOption(options, "dropCaches"_sr, false);
		ASSERT(!dropCaches || storeType != KeyValueStoreType::MEMORY);

		bool const isSQLite = storeType == KeyValueStoreType::SSD_BTREE_V2;
		pageCacheBytes = getOption(options,
		                           "pageCacheBytes"_sr,
		                           isSQLite ? FLOW_KNOBS->PAGE_CACHE_4K : static_cast<int64_t>(500LL * 1024 * 1024));
		ASSERT(pageCacheBytes > 0);
		// SQLite shares the process-wide page cache, so its size can only be set when the process starts (and in
		// simulation SIM_PAGE_CACHE_4K applies instead)
		ASSERT(!isSQLite || g_network->isSimulated() || pageCacheBytes == FLOW_KNOBS->PAGE_CACHE_4K);

		if (distribution == YCSB_ZIPFIAN) {
			zipfian.emplace(0, nodeCount - 1, zipfConstant);
		}
	}

	uint64_t nextRow() const {
		if (distribution == YCSB_ZIPFIAN) {
			return zipfian->next();
		}
		return deterministicRandom()->randomInt64(0, nodeCount);
	}

	// Called after each staged write; signals the committer once commitInterval writes are staged
	void recordWrite() {
		if (++pendingWrites >= commitInterval) {
			commitReady.trigger();
		}
	}

	static std::string dataDir() {
		return g_network->isSimulated() ? g_simulator->getCurrentProcess()->dataFolder : ".";
	}

	// Engines that use fn as a directory rather than a file name
	bool storeIsDirectory() const { return storeType == KeyValueStoreType::SSD_ROCKSDB_V1; }

	void deleteStoreFiles() const {
		if (storeIsDirectory()) {
			platform::eraseDirectoryRecursive(fn);
		} else {
			deleteFile(fn);
			deleteFile(fn + "-wal");
		}
	}

	std::vector<std::string> storeFilePaths() const {
		if (storeIsDirectory()) {
			std::vector<std::string> paths;
			for (std::string const& f : platform::listFiles(fn)) {
				paths.push_back(joinPath(fn, f));
			}
			return paths;
		}
		if (storeType == KeyValueStoreType::SSD_BTREE_V2) {
			return { fn, fn + "-wal" };
		}
		return { fn };
	}

	Future<Void> openStore() {
		storeKnobs.restore();
		if (storeType == KeyValueStoreType::SSD_ROCKSDB_V1) {
			storeKnobs.set("rocksdb_block_cache_size", pageCacheBytes);
			storeKnobs.set("rocksdb_disable_auto_compactions", pauseBackgroundWork);
		}
		kvs = openKVStore(
		    storeType, fn, deterministicRandom()->randomUniqueID(), 500e6, false, false, {}, pageCacheBytes);
		co_await kvs->init();
	}

	Future<Void> closeStore(bool dispose) {
		Future<Void> closed = kvs->onClosed();
		if (dispose) {
			kvs->dispose();
		} else {
			kvs->close();
		}
		co_await closed;
		kvs = nullptr;
		storeKnobs.restore();
	}

	Future<Void> setup(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}

		if (filename.empty()) {
			fn = joinPath(dataDir(), "ycsb-kv-" + deterministicRandom()->randomUniqueID().toString());
		} else {
			fn = filename;
		}
		if (!skipLoad) {
			// each load starts from a fresh store, even if an earlier run left one behind
			deleteStoreFiles();
		}
		co_await openStore();

		if (skipLoad) {
			CODE_PROBE(true, "YCSBKVStore reusing previously loaded store");
			co_await verifyLoaded();
			co_return;
		}

		co_await load();
		if (vacuum) {
			co_await vacuumStore();
		}

		StorageBytes const sb = kvs->getStorageBytes();
		loadPhysicalBytes = sb.used;
		loadReusableBytes = sb.available - sb.free;
		ASSERT(loadReusableBytes >= 0 && loadReusableBytes <= loadPhysicalBytes);
		TraceEvent("YCSBKVStoreSetup")
		    .detail("Rows", nodeCount)
		    .detail("KVs", kvCount())
		    .detail("Variant", variant == YCSB_MULTI_CF ? "multiCF" : "singleCF")
		    .detail("StoreType", storeType.toString())
		    .detail("LoadOrder", loadRandomOrder ? "random" : "sequential")
		    .detail("LoadDuration", loadDuration)
		    .detail("Vacuum", vacuum)
		    .detail("VacuumDuration", vacuumDuration)
		    .detail("LogicalBytes", loadLogicalBytes)
		    .detail("PhysicalBytes", loadPhysicalBytes)
		    .detail("ReusableBytes", loadReusableBytes)
		    .detail("StorageAmp", static_cast<double>(loadPhysicalBytes) / loadLogicalBytes)
		    .detail("LiveStorageAmp", static_cast<double>(loadPhysicalBytes - loadReusableBytes) / loadLogicalBytes);
	}

	Future<Void> load() {
		// The random order applies to rows, and a multiCF row's fields are written together in one commit
		uint64_t const kvsPerRow = kvCount() / nodeCount;
		std::optional<FeistelPermutation> permutation;
		if (loadRandomOrder) {
			CODE_PROBE(true, "YCSBKVStore random order load");
			permutation.emplace(nodeCount, loadShuffleSeed);
		}
		double const start = now();
		int64_t batchBytes = 0;
		for (uint64_t i = 0; i < nodeCount; i++) {
			uint64_t const row = permutation ? (*permutation)(i) : i;
			for (uint64_t f = 0; f < kvsPerRow; f++) {
				Key const k = keyForIndex(row * kvsPerRow + f);
				Value const v = randomValue();
				kvs->set(KeyValueRef(k, v));
				batchBytes += k.size() + v.size();
			}
			if (batchBytes >= loadBatchBytes || i + 1 == nodeCount) {
				co_await kvs->commit();
				loadLogicalBytes += batchBytes;
				batchBytes = 0;
			}
		}
		loadDuration = now() - start;
	}

	Future<Void> vacuumStore() {
		StorageBytes const before = kvs->getStorageBytes();
		double const start = now();
		if (storeType == KeyValueStoreType::SSD_BTREE_V2) {
			// SQLite has no deferred work after an insert-only load
			CODE_PROBE(true, "YCSBKVStore vacuum is a no-op for SQLite");
		} else {
			CODE_PROBE(before.temp > 0, "YCSBKVStore vacuum with deferred work outstanding");
			co_await kvs->vacuum();
			ASSERT_EQ(kvs->getStorageBytes().temp, 0);
		}
		vacuumDuration = now() - start;
		StorageBytes const after = kvs->getStorageBytes();
		TraceEvent("YCSBKVStoreVacuum")
		    .detail("Duration", vacuumDuration)
		    .detail("TempBytesBefore", before.temp)
		    .detail("TempBytesAfter", after.temp)
		    .detail("UsedBytesBefore", before.used)
		    .detail("UsedBytesAfter", after.used);
	}

	// Fail fast if a reused store is missing or was loaded with a different layout or size
	Future<Void> verifyLoaded() {
		int const expected = variant == YCSB_MULTI_CF ? fieldCount : 1;
		for (uint64_t const row : { static_cast<uint64_t>(0), nodeCount - 1 }) {
			RangeResult const res = co_await kvs->readRange(prefixRange(rowKey(row)), fieldCount + 1);
			if (res.size() != expected) {
				TraceEvent(SevError, "YCSBKVStoreReusedStoreMismatch")
				    .detail("Filename", fn)
				    .detail("Row", row)
				    .detail("Expected", expected)
				    .detail("Found", res.size());
			}
			ASSERT_EQ(res.size(), expected);
		}
	}

	Future<Void> start(Database const& cx) override {
		if (clientId != 0) {
			co_return;
		}
		CODE_PROBE(testDuration == 0, "YCSBKVStore load only");
		if (testDuration > 0) {
			co_await runMeasured();
		}
		co_await closeStore(disposeStore);
	}

	Future<Void> runMeasured() {
		if (zipfian) {
			co_await zipfian->init();
		}
		if (dropCaches) {
			// Reopening empties the engine's own cache (SQLite's pages are evicted once its file closes)
			co_await closeStore(false);
			co_await openStore();
			// Simulation has no OS page cache
			int osDroppedFiles = 0;
			if (!g_network->isSimulated()) {
				for (std::string const& path : storeFilePaths()) {
					osDroppedFiles += dropOsPageCache(path);
				}
#if defined(__linux__)
				ASSERT_GT(osDroppedFiles, 0);
#endif
			}
			CODE_PROBE(true, "YCSBKVStore drop caches before measured phase");
			TraceEvent("YCSBKVStoreDropCaches")
			    .detail("StoreType", storeType.toString())
			    .detail("OsDroppedFiles", osDroppedFiles);
		}

		double const startTime = now();
		UID const windowId = deterministicRandom()->randomUniqueID();
		TraceEvent("YCSBKVStoreStart").detail("WindowID", windowId);
		startLatencyWindows(NAME, windowId);
		if (updateWeight > 0) {
			windowCommitLatencies = makeWindowSample(std::string(NAME) + "CommitWindowLatency", windowId);
		}
		std::vector<Future<Void>> actors;
		actors.reserve(numActors);
		for (int i = 0; i < numActors; i++) {
			actors.push_back(runActor());
		}
		// Read-only runs never commit, so the engine can't restructure the store underneath them
		Future<Void> committer = updateWeight > 0 ? runCommitter() : Future<Void>(Void());

		co_await waitForAll(actors);

		// signal the committer to do a final flush and exit
		done = true;
		commitReady.trigger();
		co_await committer;
		stopLatencyWindows();
		windowCommitLatencies.reset();

		testDurationActual = now() - startTime;
		TraceEvent("YCSBKVStoreEnd")
		    .detail("TestDuration", testDurationActual)
		    .detail("Operations", operations.getValue())
		    .detail("Commits", totalCommits.getValue());
	}

	Future<Void> runActor() {
		double const stopAt = now() + testDuration;
		double lastTime = now();
		double const targetActorTps = targetTps > 0 ? static_cast<double>(targetTps) / numActors : 0.0;
		// Simulation fires delays within TIME_EPS without advancing time, so now() < stopAt would spin
		while (stopAt - now() > INetwork::TIME_EPS) {
			// Unlimited only yields off simulation, so a fixed per-op delay doesn't cap throughput; simulation keeps
			// a delay since cached reads complete without advancing simulated time
			Future<Void> rateDelay = targetActorTps > 0         ? poisson(&lastTime, 1.0 / targetActorTps)
			                         : g_network->isSimulated() ? delay(0.001)
			                                                    : yield();
			YCSBOperation const opType =
			    deterministicRandom()->randomInt(0, readWeight + updateWeight) < readWeight ? YCSB_READ : YCSB_UPDATE;
			uint64_t const row = nextRow();

			double const t = timer();
			if (opType == YCSB_READ) {
				if (variant == YCSB_MULTI_CF) {
					RangeResult const res = co_await kvs->readRange(prefixRange(rowKey(row)), fieldCount + 1);
					ASSERT_EQ(res.size(), fieldCount);
				} else {
					Optional<Value> const res = co_await kvs->readValue(rowKey(row));
					ASSERT(res.present());
				}
				readLatencies.addSample(timer() - t);
			} else {
				Key const key = variant == YCSB_MULTI_CF
				                    ? fieldKey(row, deterministicRandom()->randomInt(0, fieldCount))
				                    : rowKey(row);
				kvs->set(KeyValueRef(key, randomValue()));
				recordWrite();
				updateLatencies.addSample(timer() - t);
			}
			double const latency = timer() - t;
			latencies.addSample(latency);
			addWindowLatency(opType, latency);
			++operations;
			co_await race(rateDelay, delayUntil(stopAt));
		}
	}

	// Commits every commitInterval staged writes. A single instance runs per workload. A trigger while a commit is in
	// flight is dropped, but every further staged write triggers again, and done ends the loop with a final flush.
	Future<Void> runCommitter() {
		while (!done) {
			co_await commitReady.onTrigger();
			pendingWrites = 0;
			// the done-triggered pass is the final flush, not steady state
			bool const finalFlush = done;
			double const t = timer();
			co_await kvs->commit();
			double const latency = timer() - t;
			commitLatencies.addSample(latency);
			if (!finalFlush) {
				windowCommitLatencies->addMeasurement(latency);
			}
			++totalCommits;
		}
		// flush any writes staged after the last threshold-triggered commit
		co_await kvs->commit();
	}

	Future<bool> check(Database const& cx) override { return true; }

	static void addLatencyMetrics(std::vector<PerfMetric>& m, DDSketch<double>& sketch, std::string const& prefix) {
		m.emplace_back(prefix + " Mean Latency (us)", 1e6 * sketch.mean(), Averaged::True);
		m.emplace_back(prefix + " Median Latency (us)", 1e6 * sketch.median(), Averaged::True);
		m.emplace_back(prefix + " 90% Latency (us)", 1e6 * sketch.percentile(0.90), Averaged::True);
		m.emplace_back(prefix + " 99% Latency (us)", 1e6 * sketch.percentile(0.99), Averaged::True);
		m.emplace_back(prefix + " 99.9% Latency (us)", 1e6 * sketch.percentile(0.999), Averaged::True);
		m.emplace_back(prefix + " Max Latency (us)", 1e6 * sketch.max(), Averaged::True);
	}

	void getMetrics(std::vector<PerfMetric>& m) override {
		if (clientId != 0) {
			return;
		}
		if (loadLogicalBytes > 0) {
			double const logicalMB = loadLogicalBytes / 1e6;
			m.emplace_back("Load Duration (s)", loadDuration, Averaged::False);
			m.emplace_back("Load MB/s", logicalMB / loadDuration, Averaged::False);
			if (vacuum) {
				m.emplace_back("Vacuum Duration (s)", vacuumDuration, Averaged::False);
				if (vacuumDuration > 0) {
					m.emplace_back("Vacuum MB/s", logicalMB / vacuumDuration, Averaged::False);
				}
			}
			m.emplace_back("Logical Bytes", loadLogicalBytes, Averaged::False);
			m.emplace_back("Physical Bytes", loadPhysicalBytes, Averaged::False);
			m.emplace_back("Reusable Bytes", loadReusableBytes, Averaged::False);
			m.emplace_back("Storage Amp", static_cast<double>(loadPhysicalBytes) / loadLogicalBytes, Averaged::False);
			m.emplace_back("Live Storage Amp",
			               static_cast<double>(loadPhysicalBytes - loadReusableBytes) / loadLogicalBytes,
			               Averaged::False);
		}
		if (testDuration == 0) {
			return;
		}
		m.push_back(operations.getMetric());
		m.push_back(totalCommits.getMetric());
		if (testDurationActual > 0) {
			m.emplace_back("Ops/sec", operations.getValue() / testDurationActual, Averaged::False);
		}
		addLatencyMetrics(m, latencies, "Total");
		if (readWeight > 0) {
			addLatencyMetrics(m, readLatencies, "Read");
		}
		if (updateWeight > 0) {
			addLatencyMetrics(m, updateLatencies, "Update");
			addLatencyMetrics(m, commitLatencies, "Commit");
		}
	}
};

WorkloadFactory<YCSBKVStoreWorkload> YCSBKVStoreWorkloadFactory;
