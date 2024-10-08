/*
 * Mako.actor.cpp
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

#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/zipf.h"
#include "crc32/crc32c.h"
#include "flow/actorcompiler.h"

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
		return _setup(cx, this);
	}

	Future<Void> start(Database const& cx) override {
		if (doChecksumVerificationOnly)
			return Void();
		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		if (!checksumVerification) {
			return true;
		}
		// verify checksum consistency
		return dochecksumVerification(cx, this);
	}

	// disable the default timeout setting
	double getCheckTimeout() const override { return std::numeric_limits<double>::max(); }

	void getMetrics(std::vector<PerfMetric>& m) override {
		// metrics of population process
		if (populateData) {
			m.emplace_back("Mean load time (seconds)", loadTime, Averaged::True);
			// The importing rate of keys, controlled by parameter "insertionCountsToMeasure"
			auto ratesItr = ratesAtKeyCounts.begin();
			for (; ratesItr != ratesAtKeyCounts.end(); ratesItr++) {
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

	static void updateCSFlags(MakoWorkload* self, std::vector<bool>& flags, uint64_t startIdx, uint64_t endIdx) {
		// We deal with cases where rowCount % csCount != 0 and csPartitionSize % csSize != 0;
		// In particular, all keys with index in range [csSize * csPartitionSize, rowCount) will not be used for
		// checksum By the same way, for any i in range [0, csSize): keys with index in range [ i*csPartitionSize,
		// i*csPartitionSize + csCount*csStepSizeInPartition) will not be used for checksum
		uint64_t boundary = self->csSize * self->csPartitionSize;
		if (startIdx >= boundary)
			return;
		else if (endIdx > boundary)
			endIdx = boundary;

		// If all checksums need to be updated, just return
		if (std::all_of(flags.begin(), flags.end(), [](bool flag) { return flag; }))
			return;

		if (startIdx + 1 == endIdx) {
			// single key case
			startIdx = startIdx % self->csPartitionSize;
			if ((startIdx < self->csCount * self->csStepSizeInPartition) &&
			    (startIdx % self->csStepSizeInPartition == 0)) {
				flags.at(startIdx / self->csStepSizeInPartition) = true;
			}
		} else {
			// key range case
			uint64_t count = self->csCount;
			uint64_t base = (startIdx / self->csPartitionSize) * self->csPartitionSize;
			startIdx -= base;
			endIdx -= base;
			uint64_t startStepIdx = std::min(startIdx / self->csStepSizeInPartition, self->csCount - 1);

			// if changed range size is more than one csPartitionSize, which means every checksum needs to be updated
			if ((endIdx - startIdx) < self->csPartitionSize) {
				uint64_t endStepIdx;
				if (endIdx > self->csPartitionSize) {
					endStepIdx =
					    self->csCount +
					    std::min((endIdx - 1 - self->csPartitionSize) / self->csStepSizeInPartition, self->csCount);
				} else {
					endStepIdx = std::min((endIdx - 1) / self->csStepSizeInPartition, self->csCount - 1);
				}
				// All the left boundary of csStep should be updated
				// Also, check the startIdx whether it is the left boundary of a csStep
				if (startIdx == self->csStepSizeInPartition * startStepIdx)
					flags[startStepIdx] = true;
				count = endStepIdx - startStepIdx;
			}
			for (int i = 1; i <= count; ++i) {
				flags[(startStepIdx + i) % self->csCount] = true;
			}
		}
	}
	Standalone<KeyValueRef> operator()(uint64_t n) { return KeyValueRef(keyForIndex(n), randomValue()); }

	ACTOR static Future<Void> tracePeriodically(MakoWorkload* self) {
		state double start = timer();
		state double elapsed = 0.0;
		state int64_t last_ops = 0;
		state int64_t last_xacts = 0;

		loop {
			elapsed += self->periodicLoggingInterval;
			wait(delayUntil(start + elapsed));
			TraceEvent((self->description() + "_CommitLatency").c_str())
			    .detail("Mean", self->opLatencies[OP_COMMIT].mean())
			    .detail("Median", self->opLatencies[OP_COMMIT].median())
			    .detail("Percentile5", self->opLatencies[OP_COMMIT].percentile(.05))
			    .detail("Percentile95", self->opLatencies[OP_COMMIT].percentile(.95))
			    .detail("Count", self->opCounters[OP_COMMIT].getValue())
			    .detail("Elapsed", elapsed);
			TraceEvent((self->description() + "_GRVLatency").c_str())
			    .detail("Mean", self->opLatencies[OP_GETREADVERSION].mean())
			    .detail("Median", self->opLatencies[OP_GETREADVERSION].median())
			    .detail("Percentile5", self->opLatencies[OP_GETREADVERSION].percentile(.05))
			    .detail("Percentile95", self->opLatencies[OP_GETREADVERSION].percentile(.95))
			    .detail("Count", self->opCounters[OP_GETREADVERSION].getValue());

			std::string ts = format("T=%04.0fs: ", elapsed);
			self->periodicMetrics.emplace_back(ts + "Transactions/sec",
			                                   (self->xacts.getValue() - last_xacts) / self->periodicLoggingInterval,
			                                   Averaged::False);
			self->periodicMetrics.emplace_back(ts + "Operations/sec",
			                                   (self->totalOps.getValue() - last_ops) / self->periodicLoggingInterval,
			                                   Averaged::False);

			last_xacts = self->xacts.getValue();
			last_ops = self->totalOps.getValue();
		}
	}
	ACTOR Future<Void> _setup(Database cx, MakoWorkload* self) {
		// use all the clients to populate data
		if (self->populateData) {
			state Promise<double> loadTime;
			state Promise<std::vector<std::pair<uint64_t, double>>> ratesAtKeyCounts;

			wait(bulkSetup(cx,
			               self,
			               self->rowCount,
			               loadTime,
			               self->insertionCountsToMeasure.empty(),
			               self->warmingDelay,
			               self->maxInsertRate,
			               self->insertionCountsToMeasure,
			               ratesAtKeyCounts));

			// This is the setup time
			self->loadTime = loadTime.getFuture().get();
			// This is the rates of importing keys
			self->ratesAtKeyCounts = ratesAtKeyCounts.getFuture().get();
		}
		// Use one client to initialize checksums
		if (self->checksumVerification && self->clientId == 0) {
			wait(generateChecksum(cx, self));
		}

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, MakoWorkload* self) {
		// TODO: Do I need to read data to warm the cache of the keySystem like ReadWrite.actor.cpp (line 465)?
		if (self->runBenchmark) {
			wait(self->_runBenchmark(cx, self));
		}
		if (!self->preserveData && self->clientId == 0) {
			wait(self->cleanup(cx, self));
		}
		return Void();
	}

	ACTOR Future<Void> _runBenchmark(Database cx, MakoWorkload* self) {
		std::vector<Future<Void>> clients;
		clients.reserve(self->actorCountPerClient);
		for (int c = 0; c < self->actorCountPerClient; ++c) {
			clients.push_back(self->makoClient(cx, self, self->actorCountPerClient / self->transactionsPerSecond, c));
		}

		if (self->enableLogging)
			clients.push_back(tracePeriodically(self));

		wait(timeout(waitForAll(clients), self->testDuration, Void()));
		return Void();
	}

	ACTOR Future<Void> makoClient(Database cx, MakoWorkload* self, double delay, int actorIndex) {

		state Key rkey, rkey2;
		state Value rval;
		state ReadYourWritesTransaction tr(cx);
		state bool doCommit;
		state int i, count;
		state uint64_t range, indBegin, indEnd, rangeLen;
		state KeyRangeRef rkeyRangeRef;
		state std::vector<int> perOpCount(MAX_OP, 0);
		// flag at index-i indicates whether checksum-i need to be updated
		state std::vector<bool> csChangedFlags(self->csCount, false);
		state double lastTime = timer();
		state double commitStart;

		TraceEvent("ClientStarting")
		    .detail("ActorIndex", actorIndex)
		    .detail("ClientIndex", self->clientId)
		    .detail("NumActors", self->actorCountPerClient);

		loop {
			// used for throttling
			wait(poisson(&lastTime, delay));
			try {
				// user-defined value: whether commit read-only ops or not; default is false
				doCommit = self->commitGet;
				for (i = 0; i < MAX_OP; ++i) {
					if (i == OP_COMMIT)
						continue;
					for (count = 0; count < self->operations[i][0]; ++count) {
						range = self->operations[i][1];
						rangeLen = digits(range);
						// generate random key-val pair for operation
						indBegin = self->getRandomKeyIndex(self->rowCount);
						rkey = self->keyForIndex(indBegin);
						rval = self->randomValue();
						indEnd = std::min(indBegin + range, self->rowCount);
						rkey2 = self->keyForIndex(indEnd);
						// KeyRangeRef(min, maxPlusOne)
						rkeyRangeRef = KeyRangeRef(rkey, rkey2);

						// used for mako-level consistency check
						if (self->checksumVerification) {
							if (i == OP_INSERT | i == OP_UPDATE | i == OP_CLEAR) {
								updateCSFlags(self, csChangedFlags, indBegin, indBegin + 1);
							} else if (i == OP_CLEARRANGE) {
								updateCSFlags(self, csChangedFlags, indBegin, indEnd);
							}
						}

						if (i == OP_GETREADVERSION) {
							wait(logLatency(tr.getReadVersion(), &self->opLatencies[i]));
						} else if (i == OP_GET) {
							wait(logLatency(tr.get(rkey, Snapshot::False), &self->opLatencies[i]));
						} else if (i == OP_GETRANGE) {
							wait(logLatency(tr.getRange(rkeyRangeRef, CLIENT_KNOBS->TOO_MANY, Snapshot::False),
							                &self->opLatencies[i]));
						} else if (i == OP_SGET) {
							wait(logLatency(tr.get(rkey, Snapshot::True), &self->opLatencies[i]));
						} else if (i == OP_SGETRANGE) {
							// do snapshot get range here
							wait(logLatency(tr.getRange(rkeyRangeRef, CLIENT_KNOBS->TOO_MANY, Snapshot::True),
							                &self->opLatencies[i]));
						} else if (i == OP_UPDATE) {
							wait(logLatency(tr.get(rkey, Snapshot::False), &self->opLatencies[OP_GET]));
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.set(rkey, rval);
								self->opLatencies[OP_INSERT].addSample(timer() - opBegin);
							} else {
								tr.set(rkey, rval);
							}
							doCommit = true;
						} else if (i == OP_INSERT) {
							// generate an (almost) unique key here, it starts with 'mako' and then comes with randomly
							// generated characters
							randStr(reinterpret_cast<char*>(mutateString(rkey)) + self->KEYPREFIXLEN,
							        self->keyBytes - self->KEYPREFIXLEN);
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.set(rkey, rval);
								self->opLatencies[OP_INSERT].addSample(timer() - opBegin);
							} else {
								tr.set(rkey, rval);
							}
							doCommit = true;
						} else if (i == OP_INSERTRANGE) {
							char* rkeyPtr = reinterpret_cast<char*>(mutateString(rkey));
							randStr(rkeyPtr + self->KEYPREFIXLEN, self->keyBytes - self->KEYPREFIXLEN);
							for (int range_i = 0; range_i < range; ++range_i) {
								format("%0.*d", rangeLen, range_i).copy(rkeyPtr + self->keyBytes - rangeLen, rangeLen);
								if (self->latencyForLocalOperation) {
									double opBegin = timer();
									tr.set(rkey, self->randomValue());
									self->opLatencies[OP_INSERT].addSample(timer() - opBegin);
								} else {
									tr.set(rkey, self->randomValue());
								}
							}
							doCommit = true;
						} else if (i == OP_CLEAR) {
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(rkey);
								self->opLatencies[OP_CLEAR].addSample(timer() - opBegin);
							} else {
								tr.clear(rkey);
							}
							doCommit = true;
						} else if (i == OP_SETCLEAR) {
							randStr(reinterpret_cast<char*>(mutateString(rkey)) + self->KEYPREFIXLEN,
							        self->keyBytes - self->KEYPREFIXLEN);
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.set(rkey, rval);
								self->opLatencies[OP_INSERT].addSample(timer() - opBegin);
							} else {
								tr.set(rkey, rval);
							}
							wait(self->updateCSBeforeCommit(&tr, self, &csChangedFlags));
							// commit the change and update metrics
							commitStart = timer();
							wait(tr.commit());
							self->opLatencies[OP_COMMIT].addSample(timer() - commitStart);
							++perOpCount[OP_COMMIT];
							tr.reset();
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(rkey);
								self->opLatencies[OP_CLEAR].addSample(timer() - opBegin);
							} else {
								tr.clear(rkey);
							}
							doCommit = true;
						} else if (i == OP_CLEARRANGE) {
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(rkeyRangeRef);
								self->opLatencies[OP_CLEARRANGE].addSample(timer() - opBegin);
							} else {
								tr.clear(rkeyRangeRef);
							}
							doCommit = true;
						} else if (i == OP_SETCLEARRANGE) {
							char* rkeyPtr = reinterpret_cast<char*>(mutateString(rkey));
							randStr(rkeyPtr + self->KEYPREFIXLEN, self->keyBytes - self->KEYPREFIXLEN);
							state std::string scr_start_key;
							state std::string scr_end_key;
							state KeyRangeRef scr_key_range_ref;
							for (int range_i = 0; range_i < range; ++range_i) {
								format("%0.*d", rangeLen, range_i).copy(rkeyPtr + self->keyBytes - rangeLen, rangeLen);
								if (self->latencyForLocalOperation) {
									double opBegin = timer();
									tr.set(rkey, self->randomValue());
									self->opLatencies[OP_INSERT].addSample(timer() - opBegin);
								} else {
									tr.set(rkey, self->randomValue());
								}
								if (range_i == 0)
									scr_start_key = rkey.toString();
							}
							scr_end_key = rkey.toString();
							scr_key_range_ref = KeyRangeRef(KeyRef(scr_start_key), KeyRef(scr_end_key));
							wait(self->updateCSBeforeCommit(&tr, self, &csChangedFlags));
							commitStart = timer();
							wait(tr.commit());
							self->opLatencies[OP_COMMIT].addSample(timer() - commitStart);
							++perOpCount[OP_COMMIT];
							tr.reset();
							if (self->latencyForLocalOperation) {
								double opBegin = timer();
								tr.clear(scr_key_range_ref);
								self->opLatencies[OP_CLEARRANGE].addSample(timer() - opBegin);
							} else {
								tr.clear(scr_key_range_ref);
							}
							doCommit = true;
						}
						++perOpCount[i];
					}
				}

				if (doCommit) {
					wait(self->updateCSBeforeCommit(&tr, self, &csChangedFlags));
					commitStart = timer();
					wait(tr.commit());
					self->opLatencies[OP_COMMIT].addSample(timer() - commitStart);
					++perOpCount[OP_COMMIT];
				}
				// successfully finish the transaction, update metrics
				++self->xacts;
				for (int op = 0; op < MAX_OP; ++op) {
					self->opCounters[op] += perOpCount[op];
					self->totalOps += perOpCount[op];
				}
			} catch (Error& e) {
				TraceEvent("FailedToExecOperations").error(e);
				if (e.code() == error_code_operation_cancelled)
					throw;
				else if (e.code() == error_code_not_committed)
					++self->conflicts;

				wait(tr.onError(e));
				++self->retries;
			}
			// reset all the operations' counters to 0
			std::fill(perOpCount.begin(), perOpCount.end(), 0);
			tr.reset();
		}
	}

	ACTOR Future<Void> cleanup(Database cx, MakoWorkload* self) {
		// clear all data starts with 'mako' in the database
		state std::string keyPrefix(self->keyPrefix);
		state ReadYourWritesTransaction tr(cx);

		loop {
			try {
				tr.clear(prefixRange(keyPrefix));
				wait(tr.commit());
				TraceEvent("CleanUpMakoRelatedData").detail("KeyPrefix", self->keyPrefix);
				break;
			} catch (Error& e) {
				TraceEvent("FailedToCleanData").error(e);
				wait(tr.onError(e));
			}
		}

		return Void();
	}
	ACTOR template <class T>
	static Future<Void> logLatency(Future<T> f, DDSketch<double>* opLatencies) {
		state double opBegin = timer();
		wait(success(f));
		opLatencies->addSample(timer() - opBegin);
		return Void();
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

	ACTOR static Future<uint32_t> calcCheckSum(ReadYourWritesTransaction* tr, MakoWorkload* self, int csIndex) {
		state uint32_t result = 0;
		state int i;
		state Key csKey;
		for (i = 0; i < self->csSize; ++i) {
			int idx = csIndex * self->csStepSizeInPartition + i * self->csPartitionSize;
			csKey = self->keyForIndex(idx);
			Optional<Value> temp = wait(tr->get(csKey));
			if (temp.present()) {
				Value val = temp.get();
				result = crc32c_append(result, val.begin(), val.size());
			} else {
				// If the key does not exists, we just use the key itself not the value to calculate checkSum
				result = crc32c_append(result, csKey.begin(), csKey.size());
			}
		}
		return result;
	}

	ACTOR static Future<bool> dochecksumVerification(Database cx, MakoWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		state int csIdx;
		state Value csValue;

		loop {
			try {
				tr.setOption(FDBTransactionOptions::READ_LOCK_AWARE);
				for (csIdx = 0; csIdx < self->csCount; ++csIdx) {
					Optional<Value> temp = wait(tr.get(self->csKeys[csIdx]));
					if (!temp.present()) {
						TraceEvent(SevError, "TestFailure")
						    .detail("Reason", "NoExistingChecksum")
						    .detail("missedChecksumIndex", csIdx);
						return false;
					} else {
						csValue = temp.get();
						ASSERT(csValue.size() == sizeof(uint32_t));
						uint32_t calculatedCS = wait(calcCheckSum(&tr, self, csIdx));
						uint32_t existingCS = *(reinterpret_cast<const uint32_t*>(csValue.begin()));
						if (existingCS != calculatedCS) {
							TraceEvent(SevError, "TestFailure")
							    .detail("Reason", "ChecksumVerificationFailure")
							    .detail("ChecksumIndex", csIdx)
							    .detail("ExistingChecksum", existingCS)
							    .detail("CurrentChecksum", calculatedCS);
							return false;
						}
						TraceEvent("ChecksumVerificationPass")
						    .detail("ChecksumIndex", csIdx)
						    .detail("ChecksumValue", existingCS);
					}
				}
				return true;
			} catch (Error& e) {
				TraceEvent("FailedToCalculateChecksum").error(e).detail("ChecksumIndex", csIdx);
				wait(tr.onError(e));
			}
		}
	}

	ACTOR static Future<Void> generateChecksum(Database cx, MakoWorkload* self) {
		state ReadYourWritesTransaction tr(cx);
		state int csIdx;
		loop {
			try {
				for (csIdx = 0; csIdx < self->csCount; ++csIdx) {
					Optional<Value> temp = wait(tr.get(self->csKeys[csIdx]));
					if (temp.present())
						TraceEvent("DuplicatePopulationOnSamePrefix").detail("KeyPrefix", self->keyPrefix);
					wait(self->updateCheckSum(&tr, self, csIdx));
				}
				wait(tr.commit());
				break;
			} catch (Error& e) {
				TraceEvent("FailedToGenerateChecksumForPopulatedData").error(e);
				wait(tr.onError(e));
			}
		}
		return Void();
	}

	ACTOR static Future<Void> updateCheckSum(ReadYourWritesTransaction* tr, MakoWorkload* self, int csIdx) {
		state uint32_t csVal = wait(calcCheckSum(tr, self, csIdx));
		TraceEvent("UpdateCheckSum").detail("ChecksumIndex", csIdx).detail("Checksum", csVal);
		tr->set(self->csKeys[csIdx], ValueRef(reinterpret_cast<const uint8_t*>(&csVal), sizeof(uint32_t)));
		return Void();
	}

	ACTOR static Future<Void> updateCSBeforeCommit(ReadYourWritesTransaction* tr,
	                                               MakoWorkload* self,
	                                               std::vector<bool>* flags) {
		if (!self->checksumVerification)
			return Void();

		state int csIdx;
		for (csIdx = 0; csIdx < self->csCount; ++csIdx) {
			if ((*flags)[csIdx]) {
				wait(updateCheckSum(tr, self, csIdx));
				(*flags)[csIdx] = false;
			}
		}
		return Void();
	}
};

WorkloadFactory<MakoWorkload> MakoloadFactory;
