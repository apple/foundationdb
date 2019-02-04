#include "flow/actorcompiler.h"
#include "flow/ContinuousSample.h"
#include "fdbclient/NativeAPI.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.h"
#include "workloads.h"
#include "BulkSetup.actor.h"
#include "fdbserver/ClusterRecruitmentInterface.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/DeterministicRandom.h"

#include <boost/lexical_cast.hpp>

#undef FLOW_ACOMPILER_STATE
#define FLOW_ACOMPILER_STATE 1

/*
 * Heavy read/write load test
 *
 * 3 types of test client:
 * ranged read
 * ranged clears + writes
 * random read/write
 *
 * The total traffic is determined by latency and transactions/second parameters
 *
 * The percent of transactions doing ranged reads and ranged writes are
 * specified by parameters (*ActorPercent).  The rest of the transactions do
 * random read/writes
 *
 * The 2 ranged clients have a 'skew' dial (*PercentSkew), specifying how much
 * ranged read/ranged write transactions should be skewed to the same key space
 * (+/- jitter)
 *
 * The number of hot spots is parameterized. The traffic specified to be skewed
 * is evenly distributed among the hospots.  There is an option to share the
 * hotspots across test servers (useCommonSkewKeys).  If set, every test server
 * sets up its test actors with the same skew start keys. Otherwise, each test
 * server (generally 4-9) will choose different skew start keys, essentially
 * giving you an actual number of hotspots = (NumHotSpot param) * (num test
 * servers)
 *
 * The ranged clients parameterize their key ranges and the jitter for key range
 * size and key range start. That is, each ranged read will have a range
 * somewhere between the min and max range param.  Skewed ranged reads also has
 * jitter for its start key- it will start somewhere between the randomly chosen
 * skew key and the SkewStartRange.
 *
 * The skewed ranged writes start points are chosen randomly so that the ranged
 * write range will overlap with some portion of the chosen skewed minimum read
 * range.
 *
 * The random read/writes behave similarly to the ReadWrite test, and provide
 * more random load to this test
 */

const int sampleSize = 10000;
static Future<Version> nextRV;
static Version lastRV = invalidVersion;

ACTOR static Future<Version>
getNextRV(Database db)
{
    state Transaction tr(db);
    loop
    {
        try {
            Version v = wait(tr.getReadVersion());
            return v;
        } catch (Error& e) {
            wait(tr.onError(e));
        }
    }
}
static Future<Version>
getInconsistentReadVersion(Database const& db)
{
    if (!nextRV.isValid() || nextRV.isReady()) { // if no getNextRV() running
        if (nextRV.isValid())
            lastRV = nextRV.get();
        nextRV = getNextRV(db);
    }
    if (lastRV == invalidVersion)
        return nextRV;
    else
        return lastRV;
}

struct WritesSkewedRangedReadsWorkload : TestWorkload
{
    IRandom* common_tester_g_random;
    uint64_t nodeCount;
    int64_t nodePrefix;
    int actorCount, keyCount, keyBytes, maxValueBytes, minValueBytes;
    int readsPerTransactionA, writesPerTransactionA;
    int readsPerTransactionB, writesPerTransactionB;
    int extraReadConflictRangesPerTransaction,
        extraWriteConflictRangesPerTransaction;
    double testDuration, transactionsPerSecond, alpha, warmingDelay, loadTime,
        maxInsertRate, debugInterval, debugTime;
    double metricsStart, metricsDuration, clientBegin;
    std::string valueString;

    bool dependentReads;
    bool enableReadLatencyLogging;
    double periodicLoggingInterval;
    bool cancelWorkersAtDuration;
    bool inconsistentReads;
    bool adjacentReads;
    bool adjacentWrites;
    bool rampUpLoad;
    int rampSweepCount;
    double hotKeyFraction, forceHotProbability;
    bool rangeReads;
    bool useRYW;

    bool logProgress;
    double rangedReadActorPercent, rangedReadPercentSkewed;
    int rangedReadsPerTransaction, rangedReadMinKeyRange, rangedReadMaxKeyRange;
    int rangedReadSkewStartRange, rangedReadSkewNumHotSpots;
    ContinuousSample<double> readRangeLatencies;

    double rangedWriteActorPercent, rangedWritePercentSkewed;
    int rangedWriteMinKeyRange, rangedWriteMaxKeyRange;
    int rangedReadSkewMigrationRate;
    bool useCommonSkewKeys;
    bool rangedWriteBlind;
    bool rangedReadDoSkewMigration;

    vector<Future<Void>> clients;
    PerfIntCounter aTransactions, bTransactions, retries;
    ContinuousSample<double> latencies, readLatencies, commitLatencies,
        commitRangedLatencies, GRVLatencies, fullReadLatencies;
    double readLatencyTotal;
    int readLatencyCount;
    PerfIntCounter rangedReadTransactions, rangedReadRetries;
    PerfIntCounter clearRewriteRangeTransactions, clearRewriteRangeRetries;

    vector<uint64_t> insertionCountsToMeasure;
    vector<pair<uint64_t, double>> ratesAtKeyCounts;

    vector<PerfMetric> periodicMetrics;

    WritesSkewedRangedReadsWorkload(WorkloadContext const& wcx)
      : TestWorkload(wcx)
      , latencies(sampleSize)
      , readLatencies(sampleSize)
      , fullReadLatencies(sampleSize)
      , commitLatencies(sampleSize)
      , commitRangedLatencies(sampleSize)
      , GRVLatencies(sampleSize)
      , readLatencyTotal(0)
      , readLatencyCount(0)
      , loadTime(0.0)
      , dependentReads(false)
      , adjacentReads(false)
      , adjacentWrites(false)
      , clientBegin(0)
      , aTransactions("A Transactions")
      , bTransactions("B Transactions")
      , retries("Retries")
      , rangedReadTransactions("Read Range Transactions")
      , rangedReadRetries("Read Range Retries")
      , clearRewriteRangeTransactions("Commit Range Transactions")
      , clearRewriteRangeRetries("Commit Range Retries")
      , readRangeLatencies(sampleSize)
    {
        common_tester_g_random = new DeterministicRandom(sharedRandomNumber);
        bool discardEdgeMeasurements;
        std::string workloadName = "SkewedRandomRangedReadWrite";
#ifndef GETOPTION
#define GETOPTION(PNAME, DVAL) \
        PNAME = getOption(options, LiteralStringRef(#PNAME), DVAL)
#define GETOPTION_D(VNAME, PNAME, DVAL) \
        VNAME = getOption(options, LiteralStringRef(#PNAME), DVAL)
#endif
        GETOPTION(testDuration, 10.0);
        GETOPTION(transactionsPerSecond, 5000.0);
        transactionsPerSecond = transactionsPerSecond / clientCount;
        double allowedLatency;
        GETOPTION(allowedLatency, 0.250);
        actorCount = ceil(transactionsPerSecond * allowedLatency);
        GETOPTION_D(actorCount, actorCountPerTester, actorCount);
        GETOPTION(readsPerTransactionA, 10);
        GETOPTION(writesPerTransactionA, 0);
        GETOPTION(readsPerTransactionB, 1);
        GETOPTION(writesPerTransactionB, 9);
        GETOPTION(alpha, 0.1);
        GETOPTION(extraReadConflictRangesPerTransaction, 0);
        GETOPTION(extraWriteConflictRangesPerTransaction, 0);
        GETOPTION(nodeCount, (uint64_t)100000);
        GETOPTION(nodePrefix, (int64_t)-1);
        GETOPTION(keyBytes, 16);
        GETOPTION_D(maxValueBytes, valueBytes, 96);
        GETOPTION(minValueBytes, maxValueBytes);
        GETOPTION(metricsStart, 0.0);
        GETOPTION(metricsDuration, testDuration);
        GETOPTION(dependentReads, false);
        GETOPTION(discardEdgeMeasurements, true);
        GETOPTION(warmingDelay, 0.0);
        GETOPTION(maxInsertRate, 1e12);
        GETOPTION(debugInterval, 0.0);
        GETOPTION(debugTime, 0.0);
        GETOPTION(enableReadLatencyLogging, false);
        GETOPTION(periodicLoggingInterval, 5.0);
        GETOPTION(cancelWorkersAtDuration, true);
        GETOPTION(inconsistentReads, false);
        GETOPTION(adjacentReads, false);
        GETOPTION(adjacentWrites, false);
        GETOPTION(rampUpLoad, false);
        GETOPTION(useRYW, false);
        GETOPTION(rampSweepCount, 1);
        GETOPTION(rangeReads, false);
        GETOPTION(logProgress, false);
        GETOPTION(rangedReadActorPercent, 25.0);
        GETOPTION(rangedReadPercentSkewed, 50.0);
        GETOPTION(rangedReadsPerTransaction, 10);
        GETOPTION(rangedReadMinKeyRange, 100);
        GETOPTION(rangedReadMaxKeyRange, 200);
        GETOPTION(rangedReadSkewStartRange, 10);
        GETOPTION(rangedReadSkewNumHotSpots, 1);
        GETOPTION(rangedReadDoSkewMigration, false);
        GETOPTION(rangedReadSkewMigrationRate, 100);
        GETOPTION(rangedWriteActorPercent, 25.0);
        GETOPTION(rangedWritePercentSkewed, 50.0);
        GETOPTION(rangedWriteMinKeyRange, 100);
        GETOPTION(rangedWriteMaxKeyRange, 200);
        GETOPTION(rangedWriteBlind, false);
        GETOPTION(useCommonSkewKeys, false);

        keyBytes = std::max(keyBytes, 4);
        ASSERT(minValueBytes <= maxValueBytes);
        valueString = std::string(maxValueBytes, '.');
        if (nodePrefix > 0) {
            keyBytes += 16;
        }
        if (discardEdgeMeasurements) {
            // discardEdgeMeasurements keeps the metrics from the middle 3/4 of
            // the test
            metricsStart += testDuration * 0.125;
            metricsDuration *= 0.75;
        }

        // Validate that keyForIndex() is monotonic
        for (int i = 0; i < 30; i++) {
            int64_t a = g_random->randomInt64(0, nodeCount);
            int64_t b = g_random->randomInt64(0, nodeCount);
            ASSERT((a < b) == (keyForIndex(a) < keyForIndex(b)));
        }

        vector<std::string> insertionCountsToMeasureString =
            getOption(options, LiteralStringRef("insertionCountsToMeasure"),
                      vector<std::string>());
        for (int i = 0; i < insertionCountsToMeasureString.size(); i++) {
            try {
                uint64_t count = boost::lexical_cast<uint64_t>(
                    insertionCountsToMeasureString[i]);
                insertionCountsToMeasure.push_back(count);
            } catch (...) {
            }
        }

        {
            // with P(hotTrafficFraction) an access is directed to one of a
            // fraction
            //   of hot keys, else it is directed to a disjoint set of cold keys
            hotKeyFraction =
                getOption(options, LiteralStringRef("hotKeyFraction"), 0.0);
            double hotTrafficFraction =
                getOption(options, LiteralStringRef("hotTrafficFraction"), 0.0);
            ASSERT(hotKeyFraction >= 0 && hotTrafficFraction <= 1);
            ASSERT(hotKeyFraction <=
                   hotTrafficFraction); // hot keys should be actually hot!
            // p(Cold key) = (1-FHP) * (1-hkf)
            // p(Cold key) = (1-htf)
            // solving for FHP gives:
            forceHotProbability =
                (hotTrafficFraction - hotKeyFraction) / (1 - hotKeyFraction);
        }
    }

    std::string description() override { return "WritesSkewedRangedReads"; }
    Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
    Future<Void> start(Database const& cx) override { return _start(cx, this); }

    ACTOR static Future<bool> traceDumpWorkers(
        Reference<AsyncVar<ServerDBInfo>> db)
    {
        try {
            loop
            {
	        ErrorOr<vector<std::pair<WorkerInterface, ProcessClass>>> workerList =
                    wait(db->get().clusterInterface.getWorkers.tryGetReply(
                        GetWorkersRequest()));
                if (workerList.present()) {
                    std::vector<Future<ErrorOr<Void>>> dumpRequests;
                    for (int i = 0; i < workerList.get().size(); i++)
                        dumpRequests.push_back(
                            workerList.get()[i].first.traceBatchDumpRequest.tryGetReply(
                                    TraceBatchDumpRequest()));
                    wait(waitForAll(dumpRequests));
                    return true;
                }
                wait(delay(1.0));
            }
        } catch (Error& e) {
            TraceEvent(SevError, "FailedToDumpWorkers").error(e);
            throw;
        }
    }

    Future<bool> check(Database const& cx) override
    {
        TraceEvent("SkewedRandomRangedReadWriteCheck");
        clients.clear();

        if (!cancelWorkersAtDuration && now() < metricsStart + metricsDuration)
            metricsDuration = now() - metricsStart;

        g_traceBatch.dump();
        if (clientId == 0)
            return traceDumpWorkers(dbInfo);
        else
            return true;
    }

    void getMetrics(vector<PerfMetric>& m) override
    {
        TraceEvent("SkewedRandomRangedReadWriteGetMetrics");

        double duration = metricsDuration;
        int reads = (aTransactions.getValue() * readsPerTransactionA) +
                    (bTransactions.getValue() * readsPerTransactionB);
        int writes = (aTransactions.getValue() * writesPerTransactionA) +
                     (bTransactions.getValue() * writesPerTransactionB);
        m.push_back(PerfMetric("Measured Duration", duration, true));
        m.push_back(PerfMetric(
            "Transactions/sec",
            (aTransactions.getValue() + bTransactions.getValue()) / duration,
            false));
        m.push_back(
            PerfMetric("Operations/sec", ((reads + writes) / duration), false));
        m.push_back(aTransactions.getMetric());
        m.push_back(bTransactions.getMetric());
        m.push_back(rangedReadTransactions.getMetric());
        m.push_back(retries.getMetric());
        m.push_back(rangedReadTransactions.getMetric());
        m.push_back(retries.getMetric());
        m.push_back(rangedReadRetries.getMetric());
        m.push_back(clearRewriteRangeTransactions.getMetric());
        m.push_back(clearRewriteRangeRetries.getMetric());
        m.push_back(rangedReadRetries.getMetric());
        m.push_back(PerfMetric("Mean load time (seconds)", loadTime, true));
        m.push_back(PerfMetric("Read rows", reads, false));
        m.push_back(PerfMetric("Write rows", writes, false));

        if (!rampUpLoad) {
            m.push_back(
                PerfMetric("Mean Latency (ms)", 1000 * latencies.mean(), true));
            m.push_back(PerfMetric("Median Latency (ms, averaged)",
                                   1000 * latencies.median(), true));
            m.push_back(PerfMetric("90% Latency (ms, averaged)",
                                   1000 * latencies.percentile(0.90), true));
            m.push_back(PerfMetric("98% Latency (ms, averaged)",
                                   1000 * latencies.percentile(0.98), true));

            m.push_back(PerfMetric("Mean Row Read Latency (ms)",
                                   1000 * readLatencies.mean(), true));
            m.push_back(PerfMetric("Median Row Read Latency (ms, averaged)",
                                   1000 * readLatencies.median(), true));

            m.push_back(PerfMetric("Mean Total Read Latency (ms)",
                                   1000 * fullReadLatencies.mean(), true));
            m.push_back(PerfMetric("Median Total Read Latency (ms, averaged)",
                                   1000 * fullReadLatencies.median(), true));

            m.push_back(PerfMetric("Mean GRV Latency (ms)",
                                   1000 * GRVLatencies.mean(), true));
            m.push_back(PerfMetric("Median GRV Latency (ms, averaged)",
                                   1000 * GRVLatencies.median(), true));

            m.push_back(PerfMetric("Mean Commit Latency (ms)",
                                   1000 * commitLatencies.mean(), true));
            m.push_back(PerfMetric("Median Commit Latency (ms, averaged)",
                                   1000 * commitLatencies.median(), true));
            m.push_back(PerfMetric("Mean Commit Range Latency (ms)",
                                   1000 * commitRangedLatencies.mean(), true));
            m.push_back(PerfMetric("Median Commit Range Latency (ms, averaged)",
                                   1000 * commitRangedLatencies.median(),
                                   true));

            m.push_back(PerfMetric("Mean Read Range Latency (ms)",
                                   1000 * readRangeLatencies.mean(), true));
            m.push_back(PerfMetric("Median Read Range Latency (ms, averaged)",
                                   1000 * readRangeLatencies.median(), true));
            m.push_back(PerfMetric("5% Read Range Latency (ms, averaged)",
                                   1000 * readRangeLatencies.percentile(.05),
                                   true));
            m.push_back(PerfMetric("95% Read Range Latency (ms, averaged)",
                                   1000 * readRangeLatencies.percentile(.95),
                                   true));
        }

        m.push_back(PerfMetric("Read rows/sec", reads / duration, false));
        m.push_back(PerfMetric("Write rows/sec", writes / duration, false));
        m.push_back(PerfMetric(
            "Bytes read/sec",
            (reads * (keyBytes + (minValueBytes + maxValueBytes) * 0.5)) /
                duration,
            false));
        m.push_back(PerfMetric(
            "Bytes written/sec",
            (writes * (keyBytes + (minValueBytes + maxValueBytes) * 0.5)) /
                duration,
            false));
        m.insert(m.end(), periodicMetrics.begin(), periodicMetrics.end());

        vector<pair<uint64_t, double>>::iterator ratesItr =
            ratesAtKeyCounts.begin();
        for (; ratesItr != ratesAtKeyCounts.end(); ratesItr++)
            m.push_back(PerfMetric(
                format("%ld keys imported bytes/sec", ratesItr->first),
                ratesItr->second, false));
    }

    Value randomValue()
    {
        return StringRef((uint8_t*)valueString.c_str(),
                         g_random->randomInt(minValueBytes, maxValueBytes + 1));
    }

    Key keyForIndex(uint64_t index)
    {
        Key result = makeString(keyBytes);
        uint8_t* data = mutateString(result);
        memset(data, '.', keyBytes);

        int idx = 0;
        if (nodePrefix > 0) {
            emplaceIndex(data, 0, nodePrefix);
            idx += 16;
        }

        double d = double(index) / nodeCount;
        emplaceIndex(data, idx, *(int64_t*)&d);

        return result;
    }

    // Return random key with specified range
    KeyRange getRandomKeyRange(uint64_t sizeLimit)
    {
        uint64_t startLocation = g_random->randomInt(0, nodeCount);
        uint64_t scale = g_random->randomInt(0, g_random->randomInt(2, 5) *
                                                    g_random->randomInt(2, 5));
        uint64_t endLocation =
            startLocation +
            g_random->randomInt(
                0, 1 + std::min(sizeLimit, std::min(nodeCount - startLocation,
                                                    (uint64_t)(1) << scale)));

        return KeyRangeRef(keyForIndex(startLocation),
                           keyForIndex(endLocation));
    }

    Standalone<KeyValueRef> operator()(uint64_t n)
    {
        return KeyValueRef(keyForIndex(n), randomValue());
    }

    ACTOR static Future<Void> tracePeriodically(
        WritesSkewedRangedReadsWorkload* self)
    {
        state double start = now();
        state double elapsed = 0.0;
        state int64_t last_ops = 0;
        loop
        {
            elapsed += self->periodicLoggingInterval;
            wait(delayUntil(start + elapsed));
            if (self->readLatencyCount)
                TraceEvent("RwRowReadLatency")
                    .detail("MilliSeconds", self->readLatencyTotal /
                                      self->readLatencyCount * 1000.)
                    .detail("Count", self->readLatencyCount)
                    .detail("Elapsed", elapsed);

            TraceEvent("RwGRVLatency")
                .detail("MilliSeconds", 1000 * self->GRVLatencies.mean());
            TraceEvent("RwCommitLatency")
                .detail("MilliSeconds", 1000 * self->commitLatencies.mean());
            TraceEvent("RwCommitRangedLatency")
                .detail("MilliSeconds", 1000 * self->commitRangedLatencies.mean());

            int64_t ops =
                (self->aTransactions.getValue() *
                 (self->readsPerTransactionA + self->writesPerTransactionA)) +
                (self->bTransactions.getValue() *
                 (self->readsPerTransactionB + self->writesPerTransactionB));
            bool recordBegin = self->shouldRecord(std::max(
                now() - self->periodicLoggingInterval, self->clientBegin));
            bool recordEnd = self->shouldRecord(now());
            if (recordBegin && recordEnd) {
                std::string ts = format("T=%04.0fs:", elapsed);
                self->periodicMetrics.push_back(PerfMetric(
                    ts + "Operations/sec",
                    (ops - last_ops) / self->periodicLoggingInterval, false));

                if (self->rampUpLoad) {
                    self->periodicMetrics.push_back(
                        PerfMetric(ts + "Mean Latency (ms)",
                                   1000 * self->latencies.mean(), true));
                    self->periodicMetrics.push_back(
                        PerfMetric(ts + "Median Latency (ms, averaged)",
                                   1000 * self->latencies.median(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "5% Latency (ms, averaged)",
                        1000 * self->latencies.percentile(.02), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "95% Latency (ms, averaged)",
                        1000 * self->latencies.percentile(.95), true));

                    self->periodicMetrics.push_back(
                        PerfMetric(ts + "Mean Row Read Latency (ms)",
                                   1000 * self->readLatencies.mean(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "Median Row Read Latency (ms, averaged)",
                        1000 * self->readLatencies.median(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "5% Row Read Latency (ms, averaged)",
                        1000 * self->readLatencies.percentile(.05), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "95% Row Read Latency (ms, averaged)",
                        1000 * self->readLatencies.percentile(.95), true));

                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "Mean Total Read Latency (ms)",
                        1000 * self->fullReadLatencies.mean(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "Median Total Read Latency (ms, averaged)",
                        1000 * self->fullReadLatencies.median(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "5% Total Read Latency (ms, averaged)",
                        1000 * self->fullReadLatencies.percentile(.05), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "95% Total Read Latency (ms, averaged)",
                        1000 * self->fullReadLatencies.percentile(.95), true));

                    self->periodicMetrics.push_back(
                        PerfMetric(ts + "Mean GRV Latency (ms)",
                                   1000 * self->GRVLatencies.mean(), true));
                    self->periodicMetrics.push_back(
                        PerfMetric(ts + "Median GRV Latency (ms, averaged)",
                                   1000 * self->GRVLatencies.median(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "5% GRV Latency (ms, averaged)",
                        1000 * self->GRVLatencies.percentile(.05), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "95% GRV Latency (ms, averaged)",
                        1000 * self->GRVLatencies.percentile(.95), true));

                    self->periodicMetrics.push_back(
                        PerfMetric(ts + "Mean Commit Latency (ms)",
                                   1000 * self->commitLatencies.mean(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "Median Commit Latency (ms, averaged)",
                        1000 * self->commitLatencies.median(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "Mean Commit Range Latency (ms)",
                        1000 * self->commitRangedLatencies.mean(), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "Median Commit Range Latency (ms, averaged)",
                        1000 * self->commitRangedLatencies.median(), true));

                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "5% Commit Latency (ms, averaged)",
                        1000 * self->commitLatencies.percentile(.05), true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "95% Commit Latency (ms, averaged)",
                        1000 * self->commitLatencies.percentile(.95), true));

                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "5% Commit Ranged Latency (ms, averaged)",
                        1000 * self->commitRangedLatencies.percentile(.05),
                        true));
                    self->periodicMetrics.push_back(PerfMetric(
                        ts + "95% Commit Ranged Latency (ms, averaged)",
                        1000 * self->commitRangedLatencies.percentile(.95),
                        true));
                }
            }
            last_ops = ops;

            // if(self->rampUpLoad) {
            self->latencies.clear();
            self->readLatencies.clear();
            self->fullReadLatencies.clear();
            self->GRVLatencies.clear();
            self->commitLatencies.clear();
            self->commitRangedLatencies.clear();
            //}

            self->readLatencyTotal = 0.0;
            self->readLatencyCount = 0;
        }
    }

    ACTOR static Future<Void> logLatency(Future<Optional<Value>> f,
                                         ContinuousSample<double>* latencies,
                                         double* totalLatency,
                                         int* latencyCount, bool shouldRecord)
    {
        state double readBegin = now();
        Optional<Value> value = wait(f);
        if (shouldRecord) {
            double latency = now() - readBegin;
            *totalLatency += latency;
            ++*latencyCount;
            latencies->addSample(latency);
        }
        return Void();
    }

    ACTOR static Future<Void> logLatency(Future<Standalone<RangeResultRef>> f,
                                         ContinuousSample<double>* latencies,
                                         double* totalLatency,
                                         int* latencyCount, bool shouldRecord)
    {
        state double readBegin = now();
        Standalone<RangeResultRef> value = wait(f);
        if (shouldRecord) {
            double latency = now() - readBegin;
            *totalLatency += latency;
            ++*latencyCount;
            latencies->addSample(latency);
        }
        return Void();
    }

    ACTOR template <class Trans>
    Future<Void> readOp(Trans* tr, std::vector<int> keys,
                        WritesSkewedRangedReadsWorkload* self,
                        bool shouldRecord)
    {
        if (!keys.size())
            return Void();
        if (!self->dependentReads) {
            if (self->rangeReads) {
                Standalone<VectorRef<KeyRangeRef>> keyRanges;
                for (int op = 0; op < keys.size(); op++) {
                    keyRanges.push_back_deep(
                        keyRanges.arena(),
                        KeyRangeRef(self->keyForIndex(keys[op]),
                                    strinc(self->keyForIndex(keys[op]))));
                }
                wait(self->readRangeOp(tr, keyRanges, self, shouldRecord));
            } else {
                std::vector<Future<Void>> readers;
                for (int op = 0; op < keys.size(); op++)
                    readers.push_back(logLatency(
                        tr->get(self->keyForIndex(keys[op])),
                        &self->readLatencies, &self->readLatencyTotal,
                        &self->readLatencyCount, shouldRecord));
                wait(waitForAll(readers));
            }
        } else {
            state int op;
            for (op = 0; op < keys.size(); op++)
              wait(
                    logLatency(tr->get(self->keyForIndex(keys[op])),
                               &self->readLatencies, &self->readLatencyTotal,
                               &self->readLatencyCount, shouldRecord));
        }
        return Void();
    }

    // Note: any ACTOR that returns a Future MUST have a wait() call of some
    // sort
    ACTOR template <class Trans>
    Future<Void> readRangeOp(Trans* tr,
                             Standalone<VectorRef<KeyRangeRef>> keyRanges,
                             WritesSkewedRangedReadsWorkload* self,
                             bool shouldRecord)
    {
        std::vector<Future<Void>> readers;
        state Standalone<VectorRef<KeyRangeRef>> theKeyRanges = keyRanges;

        for (auto kr : theKeyRanges) {
            readers.push_back(
                logLatency(tr->getRange(kr, GetRangeLimits(-1, 1000000)),
                           &self->readLatencies, &self->readLatencyTotal,
                           &self->readLatencyCount, shouldRecord));
        }
        wait(waitForAll(readers));

        return Void();
    }

    ACTOR Future<Void> _setup(Database cx,
                              WritesSkewedRangedReadsWorkload* self)
    {
        state Promise<double> loadTime;
        state Promise<vector<pair<uint64_t, double>>> ratesAtKeyCounts;

        wait(bulkSetup(cx, self, self->nodeCount, loadTime,
                           self->insertionCountsToMeasure.empty(),
                           self->warmingDelay, self->maxInsertRate,
                           self->insertionCountsToMeasure, ratesAtKeyCounts));

        self->loadTime = loadTime.getFuture().get();
        self->ratesAtKeyCounts = ratesAtKeyCounts.getFuture().get();

        return Void();
    }

    ACTOR Future<Void> _start(Database cx,
                              WritesSkewedRangedReadsWorkload* self)
    {
        // Read one record from the database to warm the cache of keyServers
        state std::vector<int> keys;
        keys.push_back(g_random->randomInt(0, self->nodeCount));
        state double startTime = now();
        loop
        {
            state Transaction tr(cx);
            try {
              wait(self->readOp(&tr, keys, self, false));
              wait(tr.warmRange(cx, allKeys));
                break;
            } catch (Error& e) {
              wait(tr.onError(e));
            }
        }
        wait(delay(std::max(0.1, 1.0 - (now() - startTime))));

        vector<Future<Void>> clients;
        if (self->enableReadLatencyLogging)
            clients.push_back(tracePeriodically(self));

        // Figure out number of normal rw actors, ranged read actors, and skewed
        // ranged read actors
        double rangedReadPercent =
            std::min(self->rangedReadActorPercent / 100, (double)1);
        double rangedReadSkewPercent =
            std::min(self->rangedReadPercentSkewed / 100, (double)1);
        int rangedReadActorTotalCount = rangedReadPercent * self->actorCount;
        int rangedReadActorSkewedCount =
            rangedReadActorTotalCount * rangedReadSkewPercent;

        double rangedWritePercent =
            std::min(self->rangedWriteActorPercent / 100, (double)1);
        double rangedWriteSkewPercent =
            std::min(self->rangedWritePercentSkewed / 100, (double)1);
        int rangedWriteActorTotalCount = rangedWritePercent * self->actorCount;
        int rangedWriteActorSkewedCount =
            rangedWriteActorTotalCount * rangedWriteSkewPercent;

        int rwActorCount =
            std::max(self->actorCount - rangedReadActorTotalCount -
                         rangedWriteActorTotalCount,
                     0);
        int rangedReadActorCount =
            rangedReadActorTotalCount - rangedReadActorSkewedCount;
        int rangedWriteActorCount =
            rangedWriteActorTotalCount - rangedWriteActorSkewedCount;
        TraceEvent("SkewedRandomRangedReadWriteStart")
            .detail("ReadPercent", rangedReadPercent)
            .detail("WritePercent", rangedWritePercent)
            .detail("NormalRangedReadActors", rangedReadActorCount)
            .detail("SkewedRangedReadActors", rangedReadActorSkewedCount)
            .detail("NormalRangedWriteActors", rangedWriteActorCount)
            .detail("SkewedRangedWriteActors", rangedWriteActorSkewedCount)
            .detail("RwActorCount", rwActorCount);
        printf("SkewedRandomRangedReadWrite percent:%f rangedWrite percent:%f "
               "normal ranged read actors:%d"
               " skewed ranged actors:%d normal ranged write actors:%d skewed "
               "ranged write"
               " actors:%d num rw actors:%d\n",
               rangedReadPercent, rangedWritePercent, rangedReadActorCount,
               rangedReadActorSkewedCount, rangedWriteActorCount,
               rangedWriteActorSkewedCount, rwActorCount);
        self->clientBegin = now();
        for (int c = 0; c < rwActorCount; c++) {
            Future<Void> worker;
            if (self->useRYW)
                worker = self->randomReadWriteClient<ReadYourWritesTransaction>(
                    cx, self, self->actorCount / self->transactionsPerSecond);
            else
                worker = self->randomReadWriteClient<Transaction>(
                    cx, self, self->actorCount / self->transactionsPerSecond);
            clients.push_back(worker);
        }
        for (int c = 0; c < rangedReadActorCount; c++) {
            Future<Void> worker;
            worker = self->rangedReadClient<Transaction>(
                cx, self, self->actorCount / self->transactionsPerSecond, false,
                0);
            clients.push_back(worker);
        }
        for (int c = 0; c < rangedWriteActorCount; c++) {
            Future<Void> worker;
            worker = self->rangedClearAndWriteClient<Transaction>(
                cx, self, Standalone<VectorRef<int>>(),
                self->actorCount / self->transactionsPerSecond, false);
            clients.push_back(worker);
        }
        // Determine the start keys for each hot spot
        // If specified, use the same skewed key values for each test client
        int numHotSpots = std::max(self->rangedReadSkewNumHotSpots, 1);
        Standalone<VectorRef<int>> skewedKeys;
        for (int k = 0; k < numHotSpots; k++) {
            // If specified, ensure entire range will fit even at its maximum
            // value and start point
            skewedKeys.push_back(
                skewedKeys.arena(),
                self->getRandomKey(self->nodeCount -
                                       (self->rangedReadMaxKeyRange +
                                        self->rangedReadSkewStartRange),
                                   self->useCommonSkewKeys));
            TraceEvent("SkewedRandomRangedReadWriteAddingSkewedKey")
                .detail("StartIndex", skewedKeys[k]);
            printf("SkewedRandomRangedReadWrite adding skewed key start "
                   "index:%d\n",
                   skewedKeys[k]);
        }
        for (int c = 0; c < rangedReadActorSkewedCount; c++) {
            Future<Void> worker;
            int skewedKeyIndex = c % numHotSpots;
            worker = self->rangedReadClient<Transaction>(
                cx, self, self->actorCount / self->transactionsPerSecond, true,
                skewedKeys[skewedKeyIndex]);
            clients.push_back(worker);
        }
        for (int c = 0; c < rangedWriteActorSkewedCount; c++) {
            Future<Void> worker;
            worker = self->rangedClearAndWriteClient<Transaction>(
                cx, self, skewedKeys,
                self->actorCount / self->transactionsPerSecond, true);
            clients.push_back(worker);
        }

        if (!self->cancelWorkersAtDuration)
            self->clients = clients; // Don't cancel them until check()
        // Currently, logProgress and cancelWorkers options are mutually
        // exclusive

        //        if (self->logProgress) {
        //          wait(delayLogProgress(self->testDuration,
        //                                               "WriteSkewedRangedReadsTester"));
        //        } else {
          wait(
                self->cancelWorkersAtDuration
                    ? timeout(waitForAll(clients), self->testDuration, Void())
                    : delay(self->testDuration));
          //        }
        return Void();
    }

    bool shouldRecord() { return shouldRecord(now()); }

    bool shouldRecord(double checkTime)
    {
        double timeSinceStart = checkTime - clientBegin;
        return timeSinceStart >= metricsStart &&
               timeSinceStart < (metricsStart + metricsDuration);
    }

    int getRandomKey(int nodeCount, bool commonAcrossTesters = false)
    {
        IRandom* my_g_random =
            commonAcrossTesters ? common_tester_g_random : g_random;

        if (forceHotProbability &&
            my_g_random->random01() < forceHotProbability)
            return my_g_random->randomInt(0, nodeCount * hotKeyFraction) /
                   hotKeyFraction; // spread hot keys over keyspace
        else
            return my_g_random->randomInt(0, nodeCount);
    }

    // Periodically performs ranged reads for calculated key ranges
    // Will either choose start key randomly across whole key space, or will
    // always choose a key near the designated hot spot key (skew option) Clear
    // and write key range will be between min and max rangedRead KeyRange
    // parameters For skew option, the start key adds jitter based on the
    // SkewStartRange option
    ACTOR template <class Trans>
    Future<Void> rangedReadClient(Database cx,
                                  WritesSkewedRangedReadsWorkload* self,
                                  double delay, bool skewed, int skewedKey)
    {
        state double startTime = now();
        state double lastTime = now();
        state double GRVStartTime;
        state UID debugID;
        state bool isSkewed = skewed;
        state double lastSkewMigrateTime = now();
        state double skewMigrationDistance = 0;
        state int startSkewedKey = skewedKey;
        state int actingSkewedKey = skewedKey;
        state int reads = self->rangedReadsPerTransaction;
        loop
        {
          wait(poisson(&lastTime, delay));
            double sweepDuration = self->testDuration / self->rampSweepCount;
            double numSweeps = (now() - startTime) / sweepDuration;
            int currentSweep = (int)numSweeps;
            if (self->rangedReadDoSkewMigration && isSkewed) {
                double skewIncreaseBy = (lastTime - lastSkewMigrateTime) *
                                        self->rangedReadSkewMigrationRate;
                // wait til next time if increase will be less than 1
                if (skewIncreaseBy > 1) {
                    skewMigrationDistance += skewIncreaseBy;
                    actingSkewedKey =
                        (startSkewedKey + (int)skewMigrationDistance) %
                        (self->nodeCount - (self->rangedReadMaxKeyRange +
                                            self->rangedReadSkewStartRange));
                    lastSkewMigrateTime = now();
                }
            }

            if (!self->rampUpLoad ||
                (currentSweep % 2 == 0 &&
                 g_random->random01() <= numSweeps - currentSweep) ||
                (currentSweep % 2 == 1 &&
                 g_random->random01() <= 1 - (numSweeps - currentSweep))) {

                state Trans tr(cx);
                ++self->rangedReadTransactions;
                debugID = UID();
                loop
                {
                    try {
                        GRVStartTime = now();
                        Version v = wait(tr.getReadVersion());
                        // measuring latency of grabbing Read Version...
                        if (self->shouldRecord())
                            self->GRVLatencies.addSample(now() - GRVStartTime);
                        state double readStart = now();
                        Standalone<VectorRef<KeyRangeRef>> keyRanges;
                        int startKey, endKey;
                        for (auto op = 0; op < reads; op++) {
                            if (isSkewed) {
                                int startAdd = g_random->randomInt(
                                    0, self->rangedReadSkewStartRange);
                                int keyRange = g_random->randomInt(
                                    self->rangedReadMinKeyRange,
                                    self->rangedReadMaxKeyRange);
                                startKey = actingSkewedKey + startAdd;
                                endKey = startKey + keyRange;
                            } else {
                                startKey = self->getRandomKey(self->nodeCount);
                                int keyRange = g_random->randomInt(
                                    self->rangedReadMinKeyRange,
                                    self->rangedReadMaxKeyRange);
                                endKey = startKey + keyRange;
                            }

                            keyRanges.push_back_deep(
                                keyRanges.arena(),
                                KeyRangeRef(self->keyForIndex(startKey),
                                            self->keyForIndex(endKey)));
                        }
                        wait(self->readRangeOp(
                            &tr, keyRanges, self, self->shouldRecord()));
                        if (self->shouldRecord())
                            self->readRangeLatencies.addSample(now() -
                                                               readStart);
                        break;
                    } catch (Error& e) {
                        wait(tr.onError(e));
                        if (self->shouldRecord()) {
                            ++self->retries;
                            ++self->rangedReadRetries;
                        }
                    }
                }
            }
        }
    }

    // Periodically performs clears and random writes for calculated key ranges
    // Will either choose randomly across whole key space, or will always choose
    // a range that begins or ends in one of the hot spot ranges (skew option)
    // Clear and write key range will be between min and max rangedWrite
    // KeyRange parameters
    ACTOR template <class Trans>
    Future<Void> rangedClearAndWriteClient(
        Database cx, WritesSkewedRangedReadsWorkload* self,
        Standalone<VectorRef<int>> skewedReadKeys, double delay, bool skewed)
    {
        state double startTime = now();
        state double lastTime = now();
        state UID debugID;
        state bool isSkewed = skewed;
        state double lastSkewMigrateTime = now();
        state double skewMigrationDistance = 0;
        state Standalone<VectorRef<int>> startSkewedKeys =
            skewedReadKeys; // no copy, uses same arena that was created for
                            // parameter, but state ensures it sticks around
        state Standalone<VectorRef<int>> actingSkewedKeys;
        actingSkewedKeys.append(actingSkewedKeys.arena(),
                                skewedReadKeys.begin(),
                                skewedReadKeys.size()); // deep copy of vector
        loop
        {
            wait(poisson(&lastTime, delay));
            double sweepDuration = self->testDuration / self->rampSweepCount;
            double numSweeps = (now() - startTime) / sweepDuration;
            int currentSweep = (int)numSweeps;
            if (self->rangedReadDoSkewMigration && isSkewed) {
                double skewIncreaseBy = (lastTime - lastSkewMigrateTime) *
                                        self->rangedReadSkewMigrationRate;
                // wait til next time if increase will be less than 1
                if (skewIncreaseBy > 1) {
                    skewMigrationDistance += skewIncreaseBy;
                    // todo: implement VectorRef clear()
                    while (actingSkewedKeys.size())
                        actingSkewedKeys.pop_back();
                    for (auto k : startSkewedKeys) {
                        actingSkewedKeys.push_back(
                            actingSkewedKeys.arena(),
                            (k + (int)skewMigrationDistance) %
                                (self->nodeCount -
                                 (self->rangedReadMaxKeyRange +
                                  self->rangedReadSkewStartRange)));
                    }
                    lastSkewMigrateTime = now();
                }
            }
            if (!self->rampUpLoad ||
                (currentSweep % 2 == 0 &&
                 g_random->random01() <= numSweeps - currentSweep) ||
                (currentSweep % 2 == 1 &&
                 g_random->random01() <= 1 - (numSweeps - currentSweep))) {
                state Trans tr(cx);
                ++self->clearRewriteRangeTransactions;
                debugID = UID();
                loop
                {
                    try {
                        Version v = wait(tr.getReadVersion());
                        state double clearStart = now();
                        int startKey, endKey, numKeys;
                        numKeys =
                            g_random->randomInt(self->rangedWriteMinKeyRange,
                                                self->rangedWriteMaxKeyRange);
                        if (isSkewed) {
                            int skewedReadKeyId = 0;
                            if (self->rangedReadSkewNumHotSpots > 1) {
                                skewedReadKeyId = g_random->randomInt(
                                    0, self->rangedReadSkewNumHotSpots - 1);
                            }
                            int skewedReadKey =
                                actingSkewedKeys[skewedReadKeyId];
                            startKey = g_random->randomInt(
                                0,
                                std::max(0, skewedReadKey -
                                                self->rangedWriteMinKeyRange -
                                                1));
                        } else {
                            startKey = self->getRandomKey(self->nodeCount);
                        }
                        endKey = startKey + numKeys;
                        state KeyRange keyRange =
                            KeyRangeRef(self->keyForIndex(
                                            startKey), // must be standalone???
                                        self->keyForIndex(endKey));
                        vector<Value> values;
                        for (int i = 0; i < numKeys; i++) {
                            values.push_back(self->randomValue());
                        }
                        if (!self->rangedWriteBlind) {
                            logLatency(
                                tr.getRange(keyRange,
                                            GetRangeLimits(-1, 1000000)),
                                &self->readLatencies, &self->readLatencyTotal,
                                &self->readLatencyCount, self->shouldRecord());
                        }
                        tr.clear(keyRange);
                        // write new values in that range
                        for (int i = 0; i < numKeys; i++) {
                            tr.set(self->keyForIndex(startKey + i), values[i]);
                        }
                        state double commitStart = now();
                        wait(tr.commit());

                        if (self->shouldRecord())
                            self->commitRangedLatencies.addSample(now() -
                                                                  commitStart);
                        break;
                    } catch (Error& e) {
                        wait(tr.onError(e));
                        if (self->shouldRecord()) {
                            ++self->retries;
                            ++self->clearRewriteRangeRetries;
                        }
                    }
                }
                tr = Trans();
            }
        }
    }

    ACTOR template <class Trans>
    Future<Void> randomReadWriteClient(Database cx,
                                       WritesSkewedRangedReadsWorkload* self,
                                       double delay)
    {
        state double startTime = now();
        state double lastTime = now();
        state double GRVStartTime;
        state UID debugID;

        loop
        {
            wait(poisson(&lastTime, delay));

            double sweepDuration = self->testDuration / self->rampSweepCount;
            double numSweeps = (now() - startTime) / sweepDuration;
            int currentSweep = (int)numSweeps;

            if (!self->rampUpLoad ||
                (currentSweep % 2 == 0 &&
                 g_random->random01() <= numSweeps - currentSweep) ||
                (currentSweep % 2 == 1 &&
                 g_random->random01() <= 1 - (numSweeps - currentSweep))) {
                state double tstart = now();
                state bool aTransaction = g_random->random01() > self->alpha;

                state vector<int> keys;
                state vector<Value> values;
                state vector<KeyRange> extra_ranges;
                int reads = aTransaction ? self->readsPerTransactionA
                                         : self->readsPerTransactionB;
                state int writes = aTransaction ? self->writesPerTransactionA
                                                : self->writesPerTransactionB;
                state int extra_read_conflict_ranges =
                    writes ? self->extraReadConflictRangesPerTransaction : 0;
                state int extra_write_conflict_ranges =
                    writes ? self->extraWriteConflictRangesPerTransaction : 0;
                if (!self->adjacentReads) {
                    for (int op = 0; op < reads; op++)
                        keys.push_back(self->getRandomKey(self->nodeCount));
                } else {
                    int startKey = self->getRandomKey(self->nodeCount - reads);
                    for (int op = 0; op < reads; op++)
                        keys.push_back(startKey + op);
                }

                for (int op = 0; op < writes; op++)
                    values.push_back(self->randomValue());

                for (int op = 0; op < extra_read_conflict_ranges +
                                          extra_write_conflict_ranges;
                     op++)
                    extra_ranges.push_back(
                        singleKeyRange(g_random->randomUniqueID().toString()));

                state Trans tr(cx);
                if (tstart - self->clientBegin > self->debugTime &&
                    tstart - self->clientBegin <=
                        self->debugTime + self->debugInterval) {
                    debugID = g_random->randomUniqueID();
                    tr.debugTransaction(debugID);
                    g_traceBatch.addEvent(
                        "TransactionDebug", debugID.first(),
                        "WritesSkewedRangedReads.randomReadWriteClient.Before");
                } else {
                    debugID = UID();
                }
                loop
                {
                    try {
                        GRVStartTime = now();
                        Version v = wait(self->inconsistentReads
                                             ? getInconsistentReadVersion(cx)
                                             : tr.getReadVersion());
                        if (self->inconsistentReads)
                            tr.setVersion(v);

                        if (self->shouldRecord())
                            self->GRVLatencies.addSample(now() - GRVStartTime);

                        state double readStart = now();
                        wait(self->readOp(&tr, keys, self,
                                                       self->shouldRecord()));

                        if (self->shouldRecord())
                            self->fullReadLatencies.addSample(now() -
                                                              readStart);

                        if (!writes)
                            break;

                        if (self->adjacentWrites) {
                            int startKey =
                                self->getRandomKey(self->nodeCount - writes);
                            for (int op = 0; op < writes; op++)
                                tr.set(self->keyForIndex(startKey + op),
                                       values[op]);
                        } else {
                            for (int op = 0; op < writes; op++)
                                tr.set(self->keyForIndex(
                                           self->getRandomKey(self->nodeCount)),
                                       values[op]);
                        }
                        for (int op = 0; op < extra_read_conflict_ranges; op++)
                            tr.addReadConflictRange(extra_ranges[op]);
                        for (int op = 0; op < extra_write_conflict_ranges; op++)
                            tr.addWriteConflictRange(
                                extra_ranges[op + extra_read_conflict_ranges]);

                        state double commitStart = now();
                        wait(tr.commit());

                        if (self->shouldRecord())
                            self->commitLatencies.addSample(now() -
                                                            commitStart);
                        break;
                    } catch (Error& e) {
                        wait(tr.onError(e));
                        if (self->shouldRecord())
                            ++self->retries;
                    }
                }

                if (debugID != UID())
                    g_traceBatch.addEvent(
                        "TransactionDebug", debugID.first(),
                        "WritesSkewedRangedReads.randomReadWriteClient.After");

                tr = Trans();
                if (self->shouldRecord()) {
                    if (aTransaction)
                        ++self->aTransactions;
                    else
                        ++self->bTransactions;
                    double latency = now() - tstart;
                    self->latencies.addSample(latency);
                }
            }
        }
    }
};

WorkloadFactory<WritesSkewedRangedReadsWorkload>
    WritesSkewedRangedReadsWorkloadFactory("WritesSkewedRangedReads");
