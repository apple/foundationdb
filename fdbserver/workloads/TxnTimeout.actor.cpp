/*
 * TxnTimeout.actor.cpp
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
#include "fdbserver/Knobs.h"
#include "fdbserver/ServerDBInfo.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/Buggify.h"
#include "flow/Error.h"
#include "flow/Optional.h"
#include "flow/Trace.h"
#include "flow/flow.h"
#include "flow/genericactors.actor.h"

#include "flow/actorcompiler.h" // This must be the last #include.

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
	static std::string makeKey(int clientId, int actorIdx, int nodeIdx) {
		return std::format("txntimeout_c{}_a{}_n{}", clientId, actorIdx, nodeIdx);
	}

	// Initializes the database with test data for each actor to operate on
	// Each actor creates nodeCountPerClientPerActor keys initialized to value "0"
	ACTOR static Future<Void> populateDatabase(TxnTimeout* self, Database db, int actorIdx) {
		state int nodeIdx = 0;
		for (; nodeIdx < self->nodeCountPerClientPerActor; nodeIdx++) {
			state Transaction tr(db);
			loop {
				try {
					// Generate unique key per client/actor/node combination
					state std::string key = makeKey(self->clientId, actorIdx, nodeIdx);
					tr.set(StringRef(key), "0"_sr);
					wait(tr.commit());
					break;
				} catch (Error& e) {
					wait(tr.onError(e));
				}
			}
		}

		TraceEvent("TxnTimeoutPopulateComplete")
		    .detail("ClientId", self->clientId)
		    .detail("ActorIdx", actorIdx)
		    .detail("KeysCreated", nodeIdx);
		return Void();
	}

	// Runs database population concurrently across actors and clients
	ACTOR static Future<Void> populateDatabaseAllActors(TxnTimeout* self, Database db) {
		state std::vector<Future<Void>> populationActors;
		for (int actorIdx = 0; actorIdx < self->actorsPerClient; ++actorIdx) {
			populationActors.push_back(populateDatabase(self, db, actorIdx));
		}
		wait(waitForAll(populationActors));
		TraceEvent("TxnTimeoutPopulateAllComplete").detail("ClientId", self->clientId);
		return Void();
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
	ACTOR static Future<Void> txnClient(TxnTimeout* self, Database db, int actorIdx) {
		state int nodeIdx = 0;
		state double workloadStartTime = now();

		// Run transactions for 80% of test duration to allow time for cleanup
		state double runDuration = self->testDuration * 0.8;

		loop {
			// Cycle through all keys for this actor
			if (nodeIdx == self->nodeCountPerClientPerActor) {
				nodeIdx = 0;
			}

			// Stop when we've reached the target run duration
			if (now() - workloadStartTime > runDuration) {
				TraceEvent("TxnTimeoutClientComplete")
				    .detail("ClientId", self->clientId)
				    .detail("ActorIdx", actorIdx)
				    .detail("Duration", now() - workloadStartTime);
				break;
			}

			state Transaction tr(db);
			state Version readVersion = 0;
			state double txnStartTime = now();
			self->txnsTotal++;

			loop {
				try {
					// Generate the same key pattern as in populate phase
					state std::string key = makeKey(self->clientId, actorIdx, nodeIdx);

					// Get read version and read the current value
					state double readStartTime = now();
					Version rv = wait(tr.getReadVersion());
					readVersion = rv;

					state Optional<Value> val = wait(tr.get(StringRef(key)));
					state double readDuration = now() - readStartTime;

					// Artificial delay to extend transaction lifetime to target duration
					// This is the core of the test: keeping transactions open longer than the usual 5 seconds
					if (self->txnMinDuration > readDuration) {
						wait(delay(self->txnMinDuration - readDuration));
					}

					// Perform write operation (increment counter)
					state int currentVal = std::stoi(val.get().toString());
					state std::string newVal = std::to_string(currentVal + 1);
					tr.set(StringRef(key), StringRef(newVal));

					// Commit and measure total transaction latency
					wait(tr.commit());
					state double txnLatency = now() - txnStartTime;

					self->txnsSucceeded++;
					TraceEvent("TxnTimeoutTxnSuccess")
					    .detail("ClientId", self->clientId)
					    .detail("ActorIdx", actorIdx)
					    .detail("Key", key)
					    .detail("OldValue", currentVal)
					    .detail("TxnLatency", txnLatency)
					    .detail("ReadVersion", readVersion);

					nodeIdx++;
					break;

				} catch (Error& e) {
					state Error err = e;
					state bool isExpectedError = err.code() == error_code_future_version ||
					                             err.code() == error_code_commit_unknown_result ||
					                             err.code() == error_code_process_behind;

					TraceEvent(isExpectedError ? SevInfo : SevWarn, "TxnTimeoutTxnError")
					    .detail("ClientId", self->clientId)
					    .detail("ActorIdx", actorIdx)
					    .detail("RecoveryState", self->dbInfo->get().recoveryState)
					    .detail("ReadVersion", readVersion)
					    .errorUnsuppressed(err);

					wait(tr.onError(err));

					// Check if version jumped significantly (e.g stale read version, recovery)
					Version newReadVersion = wait(tr.getReadVersion());
					Version versionDelta = newReadVersion - readVersion;
					const bool isHighVersionJump = versionDelta > SERVER_KNOBS->MAX_WRITE_TRANSACTION_LIFE_VERSIONS;

					if (!isExpectedError && !isHighVersionJump) {
						self->txnsFailed++;
						TraceEvent(SevError, "TxnTimeoutUnexpectedFailure")
						    .detail("ClientId", self->clientId)
						    .detail("ActorIdx", actorIdx)
						    .detail("OldReadVersion", readVersion)
						    .detail("NewReadVersion", newReadVersion)
						    .detail("VersionDelta", versionDelta)
						    .detail("TxnDuration", now() - txnStartTime)
						    .errorUnsuppressed(err);
					} else {
						TraceEvent("TxnTimeoutExpectedFailure")
						    .detail("ClientId", self->clientId)
						    .detail("ActorIdx", actorIdx)
						    .detail("OldReadVersion", readVersion)
						    .detail("NewReadVersion", newReadVersion)
						    .detail("VersionDelta", versionDelta)
						    .detail("IsExpectedError", isExpectedError)
						    .detail("IsHighVersionJump", isHighVersionJump);
					}
				}
			}
		}
		return Void();
	}

	/*
	 * Main workload orchestration.
	 *
	 * Phase 1: Populate the database with initial test data
	 * Phase 2: Run concurrent transaction clients that test timeout behavior
	 * Phase 3: Report final metrics
	 */
	ACTOR Future<Void> workload(TxnTimeout* self, Database db) {
		TraceEvent("TxnTimeoutWorkloadStart")
		    .detail("ClientId", self->clientId)
		    .detail("TestDuration", self->testDuration)
		    .detail("ActorsPerClient", self->actorsPerClient);

		// Phase 1: Initialize database with test data
		wait(populateDatabaseAllActors(self, db));

		// Phase 2: Run transaction clients that test timeout behavior
		state std::vector<Future<Void>> txnClients;
		for (int actorIdx = 0; actorIdx < self->actorsPerClient; ++actorIdx) {
			txnClients.emplace_back(txnClient(self, db, actorIdx));
		}
		wait(waitForAll(txnClients));

		// Phase 3: Report final metrics
		TraceEvent("TxnTimeoutWorkloadComplete")
		    .detail("ClientId", self->clientId)
		    .detail("TxnsSucceeded", self->txnsSucceeded)
		    .detail("TxnsFailed", self->txnsFailed)
		    .detail("TxnsTotal", self->txnsTotal)
		    .detail("SuccessRate", self->txnsTotal > 0 ? (double)self->txnsSucceeded / self->txnsTotal : 0.0);

		return Void();
	}
};

WorkloadFactory<TxnTimeout> TxnTimeoutWorkloadFactory;
