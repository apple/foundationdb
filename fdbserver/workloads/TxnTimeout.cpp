/*
 * TxnTimeout.cpp
 *
 * This workload validates that FoundationDB's transaction timeout mechanisms work correctly
 * by creating long-running transactions that intentionally approach (but don't exceed) the
 * configured transaction lifetime limits.
 *
 * Test Strategy:
 * 1. Configures transaction lifetime via MAX_READ/WRITE_TRANSACTION_LIFE_VERSIONS knobs
 * 2. Creates transactions that perform read-modify-write operations with artificial delays
 * 3. Ensures transactions complete successfully when staying within timeout bounds
 * 4. Detects and reports any errors
 *
 * The workload runs with failure injection disabled to ensure consistent timeout behavior
 * and verify that the timeout enforcement is working as designed without interference from
 * other failure modes.
 */

#include "fdbclient/FDBTypes.h"
#include "fdbrpc/PerfMetric.h"
#include "fdbserver/core/Knobs.h"
#include "fdbserver/core/ServerDBInfo.actor.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/Optional.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

struct TxnTimeout : TestWorkload {
	static constexpr auto NAME = "TxnTimeout";

	// Configuration parameters
	double testDuration{ 0.0 }; // Total duration of the test in seconds
	int actorsPerClient{ 0 }; // Number of concurrent transaction actors per test client
	int nodeCountPerClientPerActor{ 0 }; // Number of unique keys each actor operates on
	double txnMinDuration{ 0.0 }; // Target minimum duration for each transaction (seconds)

	// Metrics tracked during test execution
	int txnsTotal{ 0 }; // Total number of transactions attempted
	int txnsSucceeded{ 0 }; // Number of transactions that completed successfully
	int txnsFailed{ 0 }; // Number of transactions that failed with unexpected errors

	TxnTimeout(const WorkloadContext& wctx) : TestWorkload(wctx) {
		// Parse workload configuration from TOML test definition
		testDuration = getOption(options, "testDuration"_sr, 120.0);
		actorsPerClient = getOption(options, "actorsPerClient"_sr, 1);
		nodeCountPerClientPerActor = getOption(options, "nodeCountPerClientPerActor"_sr, 100);
		txnMinDuration = getOption(options, "txnMinDuration"_sr, 5.0);
	}

	Future<Void> setup(const Database& db) override {
		TraceEvent("TxnTimeoutSetup")
		    .detail("TestDuration", testDuration)
		    .detail("ActorsPerClient", actorsPerClient)
		    .detail("NodeCountPerClientPerActor", nodeCountPerClientPerActor)
		    .detail("TxnMinDuration", txnMinDuration)
		    .detail("MaxReadTxnLifeVersions", SERVER_KNOBS->MAX_READ_TRANSACTION_LIFE_VERSIONS)
		    .detail("MaxWriteTxnLifeVersions", SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS);
		return Void();
	}

	static bool runTest() { return g_network->isSimulated() && !isGeneralBuggifyEnabled(); }

	Future<Void> start(const Database& db) override {
		if (!runTest()) {
			return Void();
		}

		return timeout(reportErrors(workload(this, db), "TxnTimeoutError"), testDuration, Void());
	}

	Future<bool> check(const Database& db) override {
		if (!runTest()) {
			return true;
		}

		// Test succeeds if all transactions completed without unexpected timeout failures
		if (txnsFailed > 0 || txnsSucceeded == 0 || txnsSucceeded != txnsTotal) {
			TraceEvent(SevError, "TxnTimeoutCheckFailure")
			    .detail("TxnsSucceeded", txnsSucceeded)
			    .detail("TxnsFailed", txnsFailed)
			    .detail("TxnsTotal", txnsTotal)
			    .detail("Reason",
			            txnsFailed > 0 ? "UnexpectedTimeoutErrors"
			                           : (txnsSucceeded == 0 ? "NoSuccessfulTransactions" : "CountMismatch"));
			return false;
		}

		return true;
	}

	void getMetrics(std::vector<PerfMetric>& m) override {}

	// Disable all failure injection to ensure clean timeout behavior testing
	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	// Generates a consistent key format for the workload
	// Format: "txntimeout_c{clientId}_a{actorIdx}_n{nodeIdx}"
	// This ensures the same key is used during populate and transaction phases
	static Key makeKey(int clientId, int actorIdx, int nodeIdx) {
		return Key(format("txntimeout_c%d_a%d_n%d", clientId, actorIdx, nodeIdx));
	}

	// Initializes the database with test data for each actor to operate on
	// Each actor creates nodeCountPerClientPerActor keys initialized to value "0"
	// Keys are batched into transactions for efficiency
	Future<Void> populateDatabase(Database db, int actorIdx) {
		int nodeIdx = 0;
		// Batch size is 1/4 of total keys, resulting in 4 batches per actor
		int batchSize = std::max(1, nodeCountPerClientPerActor / 4);

		while (nodeIdx < nodeCountPerClientPerActor) {
			Transaction tr(db);
			while (true) {
				Error err;
				try {
					// Batch up to batchSize keys in a single transaction
					int batchEnd = std::min(nodeIdx + batchSize, nodeCountPerClientPerActor);
					for (int i = nodeIdx; i < batchEnd; i++) {
						Key key = makeKey(clientId, actorIdx, i);
						tr.set(key, "0"_sr);
					}
					co_await tr.commit();
					nodeIdx = batchEnd;
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tr.onError(err);
			}
		}

		TraceEvent("TxnTimeoutPopulateComplete")
		    .detail("ClientId", clientId)
		    .detail("ActorIdx", actorIdx)
		    .detail("KeysCreated", nodeIdx);
	}

	// Runs database population concurrently across actors and clients
	Future<Void> populateDatabaseAllActors(Database db) {
		std::vector<Future<Void>> populationActors;
		for (int actorIdx = 0; actorIdx < actorsPerClient; ++actorIdx) {
			populationActors.push_back(populateDatabase(db, actorIdx));
		}
		co_await waitForAll(populationActors);
		TraceEvent("TxnTimeoutPopulateAllComplete").detail("ClientId", clientId);
	}

	/*
	 * Transaction client actor that performs read-modify-write operations with intentional delays.
	 *
	 * Each transaction:
	 * 1. Gets a read version and reads a value from the database
	 * 2. Waits until txnMinDuration seconds have elapsed (artificially extending the transaction)
	 * 3. Writes an incremented value back
	 * 4. Commits the transaction
	 *
	 * The goal is to test that transactions can stay open for txnMinDuration seconds
	 * without hitting transaction_too_old errors, as long as that duration is within
	 * the configured MAX_*_TRANSACTION_LIFE_VERSIONS bounds.
	 *
	 * Error Handling:
	 * - Expected errors during recovery (future_version, commit_unknown_result, process_behind) are tolerated
	 * - Version jumps due to recovery (>MAX_WRITE_TRANSACTION_LIFE_VERSIONS) are tolerated
	 * - Any other transaction_too_old or similar timeout errors are counted as failures
	 */
	Future<Void> txnClient(Database db, int actorIdx) {
		int nodeIdx = 0;
		double workloadStartTime = now();

		// Run transactions for 80% of test duration to allow time for cleanup
		double runDuration = testDuration * 0.8;

		while (true) {
			// Cycle through all keys for this actor
			if (nodeIdx == nodeCountPerClientPerActor) {
				nodeIdx = 0;
			}

			// Stop when we've reached the target run duration
			if (now() - workloadStartTime > runDuration) {
				TraceEvent("TxnTimeoutClientComplete")
				    .detail("ClientId", clientId)
				    .detail("ActorIdx", actorIdx)
				    .detail("Duration", now() - workloadStartTime);
				break;
			}

			Transaction tr(db);
			Version readVersion = 0;
			double txnStartTime = now();
			txnsTotal++;

			while (true) {
				Error caughtErr;
				try {
					// Generate the same key pattern as in populate phase
					Key key = makeKey(clientId, actorIdx, nodeIdx);

					// Get read version and read the current value
					double readStartTime = now();
					Version rv = co_await tr.getReadVersion();
					readVersion = rv;

					Optional<Value> val = co_await tr.get(key);
					double readDuration = now() - readStartTime;

					// Artificial delay to extend transaction lifetime to target duration
					// This is the core of the test: keeping transactions open longer than the usual 5 seconds
					if (txnMinDuration > readDuration) {
						co_await delay(txnMinDuration - readDuration);
					}

					// Perform write operation (increment counter)
					int currentVal = std::stoi(val.get().toString());
					std::string newVal = std::to_string(currentVal + 1);
					tr.set(key, StringRef(newVal));

					// Commit and measure total transaction latency
					co_await tr.commit();
					double txnLatency = now() - txnStartTime;

					txnsSucceeded++;
					TraceEvent("TxnTimeoutTxnSuccess")
					    .detail("ClientId", clientId)
					    .detail("ActorIdx", actorIdx)
					    .detail("Key", key)
					    .detail("OldValue", currentVal)
					    .detail("TxnLatency", txnLatency)
					    .detail("ReadVersion", readVersion);

					nodeIdx++;
					break;

				} catch (Error& e) {
					caughtErr = e;
				}
				Error err = caughtErr;
				bool isExpectedError = err.code() == error_code_future_version ||
				                       err.code() == error_code_commit_unknown_result ||
				                       err.code() == error_code_process_behind;

				TraceEvent(isExpectedError ? SevInfo : SevWarn, "TxnTimeoutTxnError")
				    .detail("ClientId", clientId)
				    .detail("ActorIdx", actorIdx)
				    .detail("RecoveryState", dbInfo->get().recoveryState)
				    .detail("ReadVersion", readVersion)
				    .errorUnsuppressed(err);

				co_await tr.onError(err);

				// Check if version jumped significantly (e.g stale read version, recovery)
				Transaction rvTr(db);
				Version newReadVersion = co_await rvTr.getReadVersion();
				// The version delta is "best guess" because the newReadVersion could itself be stale, therefore
				// the delta could be smaller than (sequencer commit version - readVersion)
				Version versionDelta = newReadVersion - readVersion;
				const bool isHighVersionJump = versionDelta > SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;
				const double txnDuration = now() - txnStartTime;
				const bool tooMuchTimeHasPassed =
				    txnDuration >
				    ((double)SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS / SERVER_KNOBS->VERSIONS_PER_SECOND);

				if (!isExpectedError && !isHighVersionJump && !tooMuchTimeHasPassed) {
					txnsFailed++;
					TraceEvent(SevError, "TxnTimeoutUnexpectedFailure")
					    .detail("ClientId", clientId)
					    .detail("ActorIdx", actorIdx)
					    .detail("OldReadVersion", readVersion)
					    .detail("NewReadVersion", newReadVersion)
					    .detail("VersionDelta", versionDelta)
					    .detail("TxnDuration", txnDuration)
					    .errorUnsuppressed(err);
				} else {
					TraceEvent("TxnTimeoutExpectedFailure")
					    .detail("ClientId", clientId)
					    .detail("ActorIdx", actorIdx)
					    .detail("OldReadVersion", readVersion)
					    .detail("NewReadVersion", newReadVersion)
					    .detail("VersionDelta", versionDelta)
					    .detail("IsExpectedError", isExpectedError)
					    .detail("IsHighVersionJump", isHighVersionJump);
				}

				txnStartTime = now();
			}
		}
	}

	/*
	 * Main workload orchestration.
	 *
	 * Phase 1: Populate the database with initial test data
	 * Phase 2: Run concurrent transaction clients that test timeout behavior
	 * Phase 3: Report final metrics
	 */
	Future<Void> workload(TxnTimeout* self, Database db) {
		TraceEvent("TxnTimeoutWorkloadStart")
		    .detail("ClientId", self->clientId)
		    .detail("TestDuration", self->testDuration)
		    .detail("ActorsPerClient", self->actorsPerClient);

		// Phase 1: Initialize database with test data
		co_await populateDatabaseAllActors(db);

		// Phase 2: Run transaction clients that test timeout behavior
		std::vector<Future<Void>> txnClients;
		for (int actorIdx = 0; actorIdx < self->actorsPerClient; ++actorIdx) {
			txnClients.emplace_back(txnClient(db, actorIdx));
		}
		co_await waitForAll(txnClients);

		// Phase 3: Report final metrics
		TraceEvent("TxnTimeoutWorkloadComplete")
		    .detail("ClientId", self->clientId)
		    .detail("TxnsSucceeded", self->txnsSucceeded)
		    .detail("TxnsFailed", self->txnsFailed)
		    .detail("TxnsTotal", self->txnsTotal)
		    .detail("SuccessRate", self->txnsTotal > 0 ? (double)self->txnsSucceeded / self->txnsTotal : 0.0);
	}
};

WorkloadFactory<TxnTimeout> TxnTimeoutWorkloadFactory;
