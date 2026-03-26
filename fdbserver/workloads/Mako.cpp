/*
 * Mako.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/core/TesterInterface.h"
#include "fdbserver/tester/workloads.actor.h"
#include "BulkSetup.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/zipf.h"
#include "crc32/crc32c.h"

enum {
	OP_GETREADVERSION,
	OP_GET,
	OP_GETRANGE,
	OP_SGET,
	OP_SGETRANGE,
	OP_UPDATE,
	OP_INSERT,
	OP_INSERTRANGE,
	OP_CLEAR,
	OP_SETCLEAR,
	OP_CLEARRANGE,
	OP_SETCLEARRANGE,
	OP_COMMIT,
	MAX_OP
};
enum { OP_COUNT, OP_RANGE };
struct MakoWorkload : TestWorkload {
	static constexpr auto NAME = "Mako";

	uint64_t rowCount, seqNumLen, sampleSize, actorCountPerClient, keyBytes, maxValueBytes, minValueBytes, csSize,
	    csCount, csPartitionSize, csStepSizeInPartition;
	double testDuration, loadTime, warmingDelay, maxInsertRate, transactionsPerSecond, allowedLatency,
	    periodicLoggingInterval, zipfConstant;
	bool enableLogging, commitGet, populateData, runBenchmark, preserveData, zipf, checksumVerification,
	    doChecksumVerificationOnly, latencyForLocalOperation;
	PerfIntCounter xacts, retries, conflicts, commits, totalOps;
	std::vector<PerfIntCounter> opCounters;
	std::vector<uint64_t> insertionCountsToMeasure;
	std::vector<std::pair<uint64_t, double>> ratesAtKeyCounts;
	std::string operationsSpec;
	// store operations to execute
	int operations[MAX_OP][2];
	// used for periodically tracing
	std::vector<PerfMetric> periodicMetrics;
	// store latency of each operation with sampling
	std::vector<DDSketch<double>> opLatencies;
	// key used to store checkSum for given key range
	std::vector<Key> csKeys;
	// key prefix of for all generated keys
	std::string keyPrefix;
	int KEYPREFIXLEN;
	const std::array<std::string, MAX_OP> opNames = { "GRV",       "GET",      "GETRANGE",   "SGET",
		                                              "SGETRANGE", "UPDATE",   "INSERT",     "INSERTRANGE",
		                                              "CLEAR",     "SETCLEAR", "CLEARRANGE", "SETCLEARRANGE",
		                                              "COMMIT" };
	MakoWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), loadTime(0.0), xacts("Transactions"), retries("Retries"), conflicts("Conflicts"),
	    commits("Commits"), totalOps("Operations") {
		// init parameters from test file
		// Number of rows populated
		rowCount = getOption(options, "rows"_sr, (uint64_t)10000);
		// Test duration in seconds
		testDuration = getOption(options, "testDuration"_sr, 30.0);
		warmingDelay = getOption(options, "warmingDelay"_sr, 0.0);
		maxInsertRate = getOption(options, "maxInsertRate"_sr, 1e12);
		// Flag to control whether to populate data into database
		populateData = getOption(options, "populateData"_sr, true);
		// Flag to control whether to run benchmark
		runBenchmark = getOption(options, "runBenchmark"_sr, true);
		// Flag to control whether to clean data in the database
		preserveData = getOption(options, "preserveData"_sr, true);
		// If true, force commit for read-only transactions
		commitGet = getOption(options, "commitGet"_sr, false);
		// If true, log latency for set, clear and clearrange
		latencyForLocalOperation = getOption(options, "latencyForLocalOperation"_sr, false);
		// Target total transaction-per-second (TPS) of all clients
		transactionsPerSecond = getOption(options, "transactionsPerSecond"_sr, 100000.0) / clientCount;
		actorCountPerClient = getOption(options, "actorCountPerClient"_sr, 16);
		// Sampling rate (1 sample / <sampleSize> ops) for latency stats
		sampleSize = getOption(options, "sampleSize"_sr, rowCount / 100);
		// If true, record latency metrics per periodicLoggingInterval; For details, see tracePeriodically()
		enableLogging = getOption(options, "enableLogging"_sr, false);
		periodicLoggingInterval = getOption(options, "periodicLoggingInterval"_sr, 5.0);
		// All the generated keys will start with the specified prefix
		keyPrefix = getOption(options, "keyPrefix"_sr, "mako"_sr).toString();
		KEYPREFIXLEN = keyPrefix.size();
		// If true, the workload will picking up keys which are zipfian distributed
		zipf = getOption(options, "zipf"_sr, false);
		zipfConstant = getOption(options, "zipfConstant"_sr, 0.99);
		// Specified length of keys and length range of values
		keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);
		maxValueBytes = getOption(options, "valueBytes"_sr, 16);
		minValueBytes = getOption(options, "minValueBytes"_sr, maxValueBytes);
		ASSERT(minValueBytes <= maxValueBytes);
		// The inserted key is formatted as: fixed prefix('mako') + sequential number + padding('x')
		// assume we want to insert 10000 rows with keyBytes set to 16,
		// then the key goes from 'mako00000xxxxxxx' to 'mako09999xxxxxxx'
		seqNumLen = digits(rowCount);
		// check keyBytes, maxValueBytes is valid
		ASSERT(seqNumLen + KEYPREFIXLEN <= keyBytes);
		// user input: a sequence of operations to be executed; e.g. "g10i5" means to do GET 10 times and Insert 5 times
		// One operation type is defined as "<Type><Count>" or "<Type><Count>:<Range>".
		// When Count is omitted, it's equivalent to setting it to 1.  (e.g. "g" is equivalent to "g1")
		// Multiple operation types can be concatenated.  (e.g. "g9u1" = 9 GETs and 1 update)
		// For RANGE operations, "Range" needs to be specified in addition to "Count".
		// Below are all allowed inputs:
		// g – GET
		// gr – GET RANGE
		// sg – Snapshot GET
		// sgr – Snapshot GET RANGE
		// u – Update (= GET followed by SET)
		// i – Insert (= SET with a new key)
		// ir – Insert Range (Sequential)
		// c – CLEAR
		// sc – SET & CLEAR
		// cr – CLEAR RANGE
		// scr – SET & CLEAR RANGE
		// grv – GetReadVersion()
		// Every transaction is committed unless it contains only GET / GET RANGE operations.
		operationsSpec = getOption(options, "operations"_sr, "g100"_sr).contents().toString();
		//  parse the sequence and extract operations to be executed
		parseOperationsSpec();
		for (int i = 0; i < MAX_OP; ++i) {
			// initialize per-operation latency record
			opLatencies.push_back(DDSketch<double>());
			// initialize per-operation counter
			opCounters.push_back(PerfIntCounter(opNames[i]));
		}
		if (zipf) {
			zipfian_generator3(0, (int)rowCount - 1, zipfConstant);
		}
		// Added for checksum verification
		csSize = getOption(options, "csSize"_sr, rowCount / 100);
		ASSERT(csSize <= rowCount);
		csCount = getOption(options, "csCount"_sr, 0);
		checksumVerification = (csCount != 0);
		doChecksumVerificationOnly = getOption(options, "doChecksumVerificationOnly"_sr, false);
		if (doChecksumVerificationOnly)
			ASSERT(checksumVerification); // csCount should be non-zero when you do checksum verification only
		if (csCount) {
			csPartitionSize = rowCount / csSize;
			ASSERT(csCount <= csPartitionSize);
			csStepSizeInPartition = csPartitionSize / csCount;
			for (int i = 0; i < csCount; ++i) {
				csKeys.emplace_back(format((keyPrefix + "_crc32c_%u_%u").c_str(), i, rowCount));
			}
		}
	}

	Future<Void> setup(Database const& cx) override {
		if (doChecksumVerificationOnly)
			return Void();
		return _setup(cx);
	}

	Future<Void> start(Database const& cx) override {
		if (doChecksumVerificationOnly)
			return Void();
		return _start(cx);
	}

	Future<bool> check(Database const& cx) override {
		if (!checksumVerification) {
			return true;
		}
		// verify checksum consistency
		return dochecksumVerification(cx);
	}

	// disable the default timeout setting
	double getCheckTimeout() const override { return std::numeric_limits<double>::max(); }

	void getMetrics(std::vector<PerfMetric>& m) override {
		// metrics of population process
		if (populateData) {
			m.emplace_back("Mean load time (seconds)", loadTime, Averaged::True);
			// The importing rate of keys, controlled by parameter "insertionCountsToMeasure"
			for (auto ratesItr = ratesAtKeyCounts.begin(); ratesItr != ratesAtKeyCounts.end(); ++ratesItr) {
				m.emplace_back(
				    format("%lld keys imported bytes/sec", ratesItr->first), ratesItr->second, Averaged::False);
			}
		}
		// benchmark
		if (runBenchmark) {
			m.emplace_back("Measured Duration", testDuration, Averaged::True);
			m.push_back(xacts.getMetric());
			m.emplace_back("Transactions/sec", xacts.getValue() / testDuration, Averaged::True);
			m.push_back(totalOps.getMetric());
			m.emplace_back("Operations/sec", totalOps.getValue() / testDuration, Averaged::True);
			m.push_back(conflicts.getMetric());
			m.emplace_back("Conflicts/sec", conflicts.getValue() / testDuration, Averaged::True);
			m.push_back(retries.getMetric());

			// count of each operation
			for (int i = 0; i < MAX_OP; ++i) {
				m.push_back(opCounters[i].getMetric());
			}

			// Meaningful Latency metrics
			const int opExecutedAtOnce[] = { OP_GETREADVERSION, OP_GET, OP_GETRANGE, OP_SGET, OP_SGETRANGE, OP_COMMIT };
			for (const int& op : opExecutedAtOnce) {
				m.emplace_back("Mean " + opNames[op] + " Latency (us)", 1e6 * opLatencies[op].mean(), Averaged::True);
				m.emplace_back(
				    "Max " + opNames[op] + " Latency (us, averaged)", 1e6 * opLatencies[op].max(), Averaged::True);
				m.emplace_back(
				    "Min " + opNames[op] + " Latency (us, averaged)", 1e6 * opLatencies[op].min(), Averaged::True);
			}
			// Latency for local operations if needed
			if (latencyForLocalOperation) {
				const int localOp[] = { OP_INSERT, OP_CLEAR, OP_CLEARRANGE };
				for (const int& op : localOp) {
					TraceEvent(SevDebug, "LocalLatency")
					    .detail("Name", opNames[op])
					    .detail("Size", opLatencies[op].getPopulationSize());
					m.emplace_back(
					    "Mean " + opNames[op] + " Latency (us)", 1e6 * opLatencies[op].mean(), Averaged::True);
					m.emplace_back(
					    "Max " + opNames[op] + " Latency (us, averaged)", 1e6 * opLatencies[op].max(), Averaged::True);
					m.emplace_back(
					    "Min " + opNames[op] + " Latency (us, averaged)", 1e6 * opLatencies[op].min(), Averaged::True);
				}
			}

			// insert logging metrics if exists
			m.insert(m.end(), periodicMetrics.begin(), periodicMetrics.end());
		}
	}
	static std::string randStr(int len) {
		std::string result(len, '.');
		for (int i = 0; i < len; ++i) {
			result[i] = deterministicRandom()->randomAlphaNumeric();
		}
		return result;
	}

	static void randStr(char* str, int len) {
		for (int i = 0; i < len; ++i) {
			str[i] = deterministicRandom()->randomAlphaNumeric();
		}
	}

	Value randomValue() {
		const int length = deterministicRandom()->randomInt(minValueBytes, maxValueBytes + 1);
		std::string valueString = randStr(length);
		return StringRef(reinterpret_cast<const uint8_t*>(valueString.c_str()), length);
	}

	Key keyForIndex(uint64_t ind) {
		Key result = makeString(keyBytes);
		char* data = reinterpret_cast<char*>(mutateString(result));
		format((keyPrefix + "%0*d").c_str(), seqNumLen, ind).copy(data, KEYPREFIXLEN + seqNumLen);
		for (int i = KEYPREFIXLEN + seqNumLen; i < keyBytes; ++i)
			data[i] = 'x';
		return result;
	}

	/* number of digits */
	static uint64_t digits(uint64_t num) {
		uint64_t digits = 0;
		while (num > 0) {
			num /= 10;
			digits++;
		}
		return digits;
	}

	void updateCSFlags(std::vector<bool>& flags, uint64_t startIdx, uint64_t endIdx) {
		// We deal with cases where rowCount % csCount != 0 and csPartitionSize % csSize != 0;
		// In particular, all keys with index in range [csSize * csPartitionSize, rowCount) will not be used for
		// checksum By the same way, for any i in range [0, csSize): keys with index in range [ i*csPartitionSize,
		// i*csPartitionSize + csCount*csStepSizeInPartition) will not be used for checksum
		uint64_t boundary = csSize * csPartitionSize;
		if (startIdx >= boundary)
			return;
		else if (endIdx > boundary)
			endIdx = boundary;

		// If all checksums need to be updated, just return
		if (std::all_of(flags.begin(), flags.end(), [](bool flag) { return flag; }))
			return;

		if (startIdx + 1 == endIdx) {
			// single key case
			startIdx = startIdx % csPartitionSize;
			if ((startIdx < csCount * csStepSizeInPartition) && (startIdx % csStepSizeInPartition == 0)) {
				flags.at(startIdx / csStepSizeInPartition) = true;
			}
		} else {
			// key range case
			uint64_t count = csCount;
			uint64_t base = (startIdx / csPartitionSize) * csPartitionSize;
			startIdx -= base;
			endIdx -= base;
			uint64_t startStepIdx = std::min(startIdx / csStepSizeInPartition, csCount - 1);

			// if changed range size is more than one csPartitionSize, which means every checksum needs to be updated
			if ((endIdx - startIdx) < csPartitionSize) {
				uint64_t endStepIdx;
				if (endIdx > csPartitionSize) {
					endStepIdx = csCount + std::min((endIdx - 1 - csPartitionSize) / csStepSizeInPartition, csCount);
				} else {
					endStepIdx = std::min((endIdx - 1) / csStepSizeInPartition, csCount - 1);
				}
				// All the left boundary of csStep should be updated
				// Also, check the startIdx whether it is the left boundary of a csStep
				if (startIdx == csStepSizeInPartition * startStepIdx)
					flags[startStepIdx] = true;
				count = endStepIdx - startStepIdx;
			}
			for (int i = 1; i <= count; ++i) {
				flags[(startStepIdx + i) % csCount] = true;
			}
		}
	}
	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n), randomValue()); }

	Future<Void> tracePeriodically() {
		double start = timer();
		double elapsed = 0.0;
		int64_t last_ops = 0;
		int64_t last_xacts = 0;

		while (true) {
			elapsed += periodicLoggingInterval;
			co_await delayUntil(start + elapsed);
			TraceEvent((description() + "_CommitLatency").c_str())
			    .detail("Mean", opLatencies[OP_COMMIT].mean())
			    .detail("Median", opLatencies[OP_COMMIT].median())
			    .detail("Percentile5", opLatencies[OP_COMMIT].percentile(.05))
			    .detail("Percentile95", opLatencies[OP_COMMIT].percentile(.95))
			    .detail("Count", opCounters[OP_COMMIT].getValue())
			    .detail("Elapsed", elapsed);
			TraceEvent((description() + "_GRVLatency").c_str())
			    .detail("Mean", opLatencies[OP_GETREADVERSION].mean())
			    .detail("Median", opLatencies[OP_GETREADVERSION].median())
			    .detail("Percentile5", opLatencies[OP_GETREADVERSION].percentile(.05))
			    .detail("Percentile95", opLatencies[OP_GETREADVERSION].percentile(.95))
			    .detail("Count", opCounters[OP_GETREADVERSION].getValue());

			std::string ts = format("T=%04.0fs: ", elapsed);
			periodicMetrics.emplace_back(
			    ts + "Transactions/sec", (xacts.getValue() - last_xacts) / periodicLoggingInterval, Averaged::False);
			periodicMetrics.emplace_back(
			    ts + "Operations/sec", (totalOps.getValue() - last_ops) / periodicLoggingInterval, Averaged::False);

			last_xacts = xacts.getValue();
			last_ops = totalOps.getValue();
		}
	}
	Future<Void> _setup(Database cx) {
		// use all the clients to populate data
		if (populateData) {
			Promise<double> loadTime;
			Promise<std::vector<std::pair<uint64_t, double>>> ratesAtKeyCounts;

			co_await bulkSetup(cx,
			                   this,
			                   rowCount,
			                   loadTime,
			                   insertionCountsToMeasure.empty(),
			                   warmingDelay,
			                   maxInsertRate,
			                   insertionCountsToMeasure,
			                   ratesAtKeyCounts);

			// This is the setup time
			this->loadTime = loadTime.getFuture().get();
			// This is the rates of importing keys
			this->ratesAtKeyCounts = ratesAtKeyCounts.getFuture().get();
		}
		// Use one client to initialize checksums
		if (checksumVerification && clientId == 0) {
			co_await generateChecksum(cx);
		}
	}

	Future<Void> _start(Database cx) {
		// TODO: Do I need to read data to warm the cache of the keySystem like ReadWrite.cpp (line 465)?
		if (runBenchmark) {
			co_await _runBenchmark(cx);
		}
		if (!preserveData && clientId == 0) {
			co_await cleanup(cx);
		}
	}

	Future<Void> _runBenchmark(Database cx) {
		std::vector<Future<Void>> clients;
		clients.reserve(actorCountPerClient);
		for (int c = 0; c < actorCountPerClient; ++c) {
			clients.push_back(makoClient(cx, actorCountPerClient / transactionsPerSecond, c));
		}

		if (enableLogging)
			clients.push_back(tracePeriodically());

		co_await timeout(waitForAll(clients), testDuration, Void());
	}

	Future<Void> makoClient(Database cx, double delay, int actorIndex) {

		Key rkey;
		Key rkey2;
		Value rval;
		ReadYourWritesTransaction tr(cx);
		bool doCommit{ false };
		int i{ 0 };
		int count{ 0 };
		uint64_t range{ 0 };
		uint64_t indBegin{ 0 };
		uint64_t indEnd{ 0 };
		uint64_t rangeLen{ 0 };
		KeyRangeRef rkeyRangeRef;
		std::vector<int> perOpCount(MAX_OP, 0);
		// flag at index-i indicates whether checksum-i need to be updated
		std::vector<bool> csChangedFlags(csCount, false);
		double lastTime = timer();
		double commitStart{ 0 };

		TraceEvent("ClientStarting")
		    .detail("ActorIndex", actorIndex)
		    .detail("ClientIndex", clientId)
		    .detail("NumActors", actorCountPerClient);

		while (true) {
			// used for throttling
			co_await poisson(&lastTime, delay);
			Error err;
			try {
				// user-defined value: whether commit read-only ops or not; default is false
				doCommit = commitGet;
				for (i = 0; i < MAX_OP; ++i) {
					if (i == OP_COMMIT)
						continue;
					for (count = 0; count < operations[i][0]; ++count) {
						range = operations[i][1];
						rangeLen = digits(range);
						// generate random key-val pair for operation
						indBegin = getRandomKeyIndex(rowCount);
						rkey = keyForIndex(indBegin);
						rval = randomValue();
						indEnd = std::min(indBegin + range, rowCount);
						rkey2 = keyForIndex(indEnd);
						// KeyRangeRef(min, maxPlusOne)
						rkeyRangeRef = KeyRangeRef(rkey, rkey2);

						// used for mako-level consistency check
						if (checksumVerification) {
							if (i == OP_INSERT | i == OP_UPDATE | i == OP_CLEAR) {
								updateCSFlags(csChangedFlags, indBegin, indBegin + 1);
							} else if (i == OP_CLEARRANGE) {
								updateCSFlags(csChangedFlags, indBegin, indEnd);
							}
						}

						if (i == OP_GETREADVERSION) {
							co_await logLatency(tr.getReadVersion(), &opLatencies[i]);
						} else if (i == OP_GET) {
							co_await logLatency(tr.get(rkey, Snapshot::False), &opLatencies[i]);
						} else if (i == OP_GETRANGE) {
							co_await logLatency(tr.getRange(rkeyRangeRef, CLIENT_KNOBS->TOO_MANY, Snapshot::False),
							                    &opLatencies[i]);
						} else if (i == OP_SGET) {
							co_await logLatency(tr.get(rkey, Snapshot::True), &opLatencies[i]);
						} else if (i == OP_SGETRANGE) {
							// do snapshot get range here
							co_await logLatency(tr.getRange(rkeyRangeRef, CLIENT_KNOBS->TOO_MANY, Snapshot::True),
							                    &opLatencies[i]);
						} else if (i == OP_UPDATE) {
							co_await logLatency(tr.get(rkey, Snapshot::False), &opLatencies[OP_GET]);
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.set(rkey, rval);
								opLatencies[OP_INSERT].addSample(timer() - opBegin);
							} else {
								tr.set(rkey, rval);
							}
							doCommit = true;
						} else if (i == OP_INSERT) {
							// generate an (almost) unique key here, it starts with 'mako' and then comes with
							// randomly generated characters
							randStr(reinterpret_cast<char*>(mutateString(rkey)) + KEYPREFIXLEN,
							        keyBytes - KEYPREFIXLEN);
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.set(rkey, rval);
								opLatencies[OP_INSERT].addSample(timer() - opBegin);
							} else {
								tr.set(rkey, rval);
							}
							doCommit = true;
						} else if (i == OP_INSERTRANGE) {
							char* rkeyPtr = reinterpret_cast<char*>(mutateString(rkey));
							randStr(rkeyPtr + KEYPREFIXLEN, keyBytes - KEYPREFIXLEN);
							for (int range_i = 0; range_i < range; ++range_i) {
								format("%0.*d", rangeLen, range_i).copy(rkeyPtr + keyBytes - rangeLen, rangeLen);
								if (latencyForLocalOperation) {
									double opBegin = timer();
									tr.set(rkey, randomValue());
									opLatencies[OP_INSERT].addSample(timer() - opBegin);
								} else {
									tr.set(rkey, randomValue());
								}
							}
							doCommit = true;
						} else if (i == OP_CLEAR) {
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(rkey);
								opLatencies[OP_CLEAR].addSample(timer() - opBegin);
							} else {
								tr.clear(rkey);
							}
							doCommit = true;
						} else if (i == OP_SETCLEAR) {
							randStr(reinterpret_cast<char*>(mutateString(rkey)) + KEYPREFIXLEN,
							        keyBytes - KEYPREFIXLEN);
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.set(rkey, rval);
								opLatencies[OP_INSERT].addSample(timer() - opBegin);
							} else {
								tr.set(rkey, rval);
							}
							co_await updateCSBeforeCommit(&tr, &csChangedFlags);
							// commit the change and update metrics
							commitStart = timer();
							co_await tr.commit();
							opLatencies[OP_COMMIT].addSample(timer() - commitStart);
							++perOpCount[OP_COMMIT];
							tr.reset();
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(rkey);
								opLatencies[OP_CLEAR].addSample(timer() - opBegin);
							} else {
								tr.clear(rkey);
							}
							doCommit = true;
						} else if (i == OP_CLEARRANGE) {
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(rkeyRangeRef);
								opLatencies[OP_CLEARRANGE].addSample(timer() - opBegin);
							} else {
								tr.clear(rkeyRangeRef);
							}
							doCommit = true;
						} else if (i == OP_SETCLEARRANGE) {
							char* rkeyPtr = reinterpret_cast<char*>(mutateString(rkey));
							randStr(rkeyPtr + KEYPREFIXLEN, keyBytes - KEYPREFIXLEN);
							std::string scr_start_key;
							std::string scr_end_key;
							KeyRangeRef scr_key_range_ref;
							for (int range_i = 0; range_i < range; ++range_i) {
								format("%0.*d", rangeLen, range_i).copy(rkeyPtr + keyBytes - rangeLen, rangeLen);
								if (latencyForLocalOperation) {
									double opBegin = timer();
									tr.set(rkey, randomValue());
									opLatencies[OP_INSERT].addSample(timer() - opBegin);
								} else {
									tr.set(rkey, randomValue());
								}
								if (range_i == 0)
									scr_start_key = rkey.toString();
							}
							scr_end_key = rkey.toString();
							scr_key_range_ref = KeyRangeRef(KeyRef(scr_start_key), KeyRef(scr_end_key));
							co_await updateCSBeforeCommit(&tr, &csChangedFlags);
							commitStart = timer();
							co_await tr.commit();
							opLatencies[OP_COMMIT].addSample(timer() - commitStart);
							++perOpCount[OP_COMMIT];
							tr.reset();
							if (latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(scr_key_range_ref);
								opLatencies[OP_CLEARRANGE].addSample(timer() - opBegin);
							} else {
								tr.clear(scr_key_range_ref);
							}
							doCommit = true;
						}
						++perOpCount[i];
					}
				}

				if (doCommit) {
					co_await updateCSBeforeCommit(&tr, &csChangedFlags);
					commitStart = timer();
					co_await tr.commit();
					opLatencies[OP_COMMIT].addSample(timer() - commitStart);
					++perOpCount[OP_COMMIT];
				}
				// successfully finish the transaction, update metrics
				++xacts;
				for (int op = 0; op < MAX_OP; ++op) {
					opCounters[op] += perOpCount[op];
					totalOps += perOpCount[op];
				}
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("FailedToExecOperations").error(err);
			if (err.code() == error_code_operation_cancelled)
				throw err;
			else if (err.code() == error_code_not_committed)
				++conflicts;
			co_await tr.onError(err);
			++retries;
			// reset all the operations' counters to 0
			std::fill(perOpCount.begin(), perOpCount.end(), 0);
			tr.reset();
		}
	}

	Future<Void> cleanup(Database cx) {
		// clear all data starts with 'mako' in the database
		std::string keyPrefix(this->keyPrefix);
		ReadYourWritesTransaction tr(cx);

		while (true) {
			Error err;
			try {
				tr.clear(prefixRange(keyPrefix));
				co_await tr.commit();
				TraceEvent("CleanUpMakoRelatedData").detail("KeyPrefix", this->keyPrefix);
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("FailedToCleanData").error(err);
			co_await tr.onError(err);
		}
	}
	template <class T>
	static Future<Void> logLatency(Future<T> f, DDSketch<double>* opLatencies) {
		double opBegin = timer();
		co_await f;
		opLatencies->addSample(timer() - opBegin);
	}

	int64_t getRandomKeyIndex(uint64_t rowCount) {
		int64_t randomKeyIndex;
		if (zipf) {
			randomKeyIndex = zipfian_next();
		} else {
			randomKeyIndex = deterministicRandom()->randomInt64(0, rowCount);
		}
		return randomKeyIndex;
	}
	void parseOperationsSpec() {
		const char* ptr = operationsSpec.c_str();
		int op = 0;
		int rangeop = 0;
		int num;
		int error = 0;

		for (op = 0; op < MAX_OP; op++) {
			operations[op][OP_COUNT] = 0;
			operations[op][OP_RANGE] = 0;
		}

		op = 0;
		while (*ptr) {
			if (strncmp(ptr, "grv", 3) == 0) {
				op = OP_GETREADVERSION;
				ptr += 3;
			} else if (strncmp(ptr, "gr", 2) == 0) {
				op = OP_GETRANGE;
				rangeop = 1;
				ptr += 2;
			} else if (strncmp(ptr, "g", 1) == 0) {
				op = OP_GET;
				ptr++;
			} else if (strncmp(ptr, "sgr", 3) == 0) {
				op = OP_SGETRANGE;
				rangeop = 1;
				ptr += 3;
			} else if (strncmp(ptr, "sg", 2) == 0) {
				op = OP_SGET;
				ptr += 2;
			} else if (strncmp(ptr, "u", 1) == 0) {
				op = OP_UPDATE;
				ptr++;
			} else if (strncmp(ptr, "ir", 2) == 0) {
				op = OP_INSERTRANGE;
				rangeop = 1;
				ptr += 2;
			} else if (strncmp(ptr, "i", 1) == 0) {
				op = OP_INSERT;
				ptr++;
			} else if (strncmp(ptr, "cr", 2) == 0) {
				op = OP_CLEARRANGE;
				rangeop = 1;
				ptr += 2;
			} else if (strncmp(ptr, "c", 1) == 0) {
				op = OP_CLEAR;
				ptr++;
			} else if (strncmp(ptr, "scr", 3) == 0) {
				op = OP_SETCLEARRANGE;
				rangeop = 1;
				ptr += 3;
			} else if (strncmp(ptr, "sc", 2) == 0) {
				op = OP_SETCLEAR;
				ptr += 2;
			} else {
				error = 1;
				break;
			}

			/* count */
			num = 0;
			if ((*ptr < '0') || (*ptr > '9')) {
				num = 1; /* if omitted, set it to 1 */
			} else {
				while ((*ptr >= '0') && (*ptr <= '9')) {
					num = num * 10 + *ptr - '0';
					ptr++;
				}
			}
			/* set count */
			operations[op][OP_COUNT] = num;

			if (rangeop) {
				if (*ptr != ':') {
					error = 1;
					break;
				} else {
					ptr++; /* skip ':' */
					num = 0;
					if ((*ptr < '0') || (*ptr > '9')) {
						error = 1;
						break;
					}
					while ((*ptr >= '0') && (*ptr <= '9')) {
						num = num * 10 + *ptr - '0';
						ptr++;
					}
					/* set range */
					operations[op][OP_RANGE] = num;
				}
			}
			rangeop = 0;
		}

		if (error) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "InvalidTransactionSpecification")
			    .detail("operations", operationsSpec);
		}
	}

	Future<uint32_t> calcCheckSum(ReadYourWritesTransaction* tr, int csIndex) {
		uint32_t result = 0;
		int i{ 0 };
		Key csKey;
		for (i = 0; i < csSize; ++i) {
			int idx = csIndex * csStepSizeInPartition + i * csPartitionSize;
			csKey = keyForIndex(idx);
			Optional<Value> temp = co_await tr->get(csKey);
			if (temp.present()) {
				Value val = temp.get();
				result = crc32c_append(result, val.begin(), val.size());
			} else {
				// If the key does not exists, we just use the key itself not the value to calculate checkSum
				result = crc32c_append(result, csKey.begin(), csKey.size());
			}
		}
		co_return result;
	}

	Future<bool> dochecksumVerification(Database cx) {
		ReadYourWritesTransaction tr(cx);
		int csIdx{ 0 };
		Value csValue;

		while (true) {
			Error err;
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				for (csIdx = 0; csIdx < csCount; ++csIdx) {
					Optional<Value> temp = co_await tr.get(csKeys[csIdx]);
					if (!temp.present()) {
						TraceEvent(SevError, "TestFailure")
						    .detail("Reason", "NoExistingChecksum")
						    .detail("missedChecksumIndex", csIdx);
						co_return false;
					} else {
						csValue = temp.get();
						ASSERT(csValue.size() == sizeof(uint32_t));
						uint32_t calculatedCS = co_await calcCheckSum(&tr, csIdx);
						uint32_t existingCS = *(reinterpret_cast<const uint32_t*>(csValue.begin()));
						if (existingCS != calculatedCS) {
							TraceEvent(SevError, "TestFailure")
							    .detail("Reason", "ChecksumVerificationFailure")
							    .detail("ChecksumIndex", csIdx)
							    .detail("ExistingChecksum", existingCS)
							    .detail("CurrentChecksum", calculatedCS);
							co_return false;
						}
						TraceEvent("ChecksumVerificationPass")
						    .detail("ChecksumIndex", csIdx)
						    .detail("ChecksumValue", existingCS);
					}
				}
				co_return true;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("FailedToCalculateChecksum").error(err).detail("ChecksumIndex", csIdx);
			co_await tr.onError(err);
		}
	}

	Future<Void> generateChecksum(Database cx) {
		ReadYourWritesTransaction tr(cx);
		int csIdx{ 0 };
		while (true) {
			Error err;
			try {
				for (csIdx = 0; csIdx < csCount; ++csIdx) {
					Optional<Value> temp = co_await tr.get(csKeys[csIdx]);
					if (temp.present())
						TraceEvent("DuplicatePopulationOnSamePrefix").detail("KeyPrefix", keyPrefix);
					co_await updateCheckSum(&tr, csIdx);
				}
				co_await tr.commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent("FailedToGenerateChecksumForPopulatedData").error(err);
			co_await tr.onError(err);
		}
	}

	Future<Void> updateCheckSum(ReadYourWritesTransaction* tr, int csIdx) {
		uint32_t csVal = co_await calcCheckSum(tr, csIdx);
		TraceEvent("UpdateCheckSum").detail("ChecksumIndex", csIdx).detail("Checksum", csVal);
		tr->set(csKeys[csIdx], ValueRef(reinterpret_cast<const uint8_t*>(&csVal), sizeof(uint32_t)));
	}

	Future<Void> updateCSBeforeCommit(ReadYourWritesTransaction* tr, std::vector<bool>* flags) {
		if (!checksumVerification)
			co_return;

		for (int csIdx = 0; csIdx < csCount; ++csIdx) {
			if ((*flags)[csIdx]) {
				co_await updateCheckSum(tr, csIdx);
				(*flags)[csIdx] = false;
			}
		}
	}
};

WorkloadFactory<MakoWorkload> MakoloadFactory;
