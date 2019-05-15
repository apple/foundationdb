#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/workloads/Mako.h"
#include "flow/actorcompiler.h"

static const char* opNames[MAX_OP] = {"GRV", "GET", "GETRANGE", "SGET", "SGETRANGE", "UPDATE", "INSERT", "INSERTRANGE", "CLEAR", "SETCLEAR", "CLEARRANGE", "SETCLEARRANGE", "COMMIT"};

struct MakoWorkload : KVWorkload {
	uint64_t rowCount, seqNumLen, sampleSize, actorCountPerClient;
	double testDuration, loadTime, warmingDelay, maxInsertRate, transactionsPerSecond, allowedLatency, periodicLoggingInterval;
	double metricsStart, metricsDuration;
	bool enableLogging, commitGet;
	PerfIntCounter xacts, retries, conflicts, commits, totalOps;
	vector<PerfIntCounter> opCounters;
	vector<uint64_t> insertionCountsToMeasure;
	vector<pair<uint64_t, double>> ratesAtKeyCounts;
	std::string valueString, unparsedTxStr, mode;

	//store operations to execute
	int operations[MAX_OP][2];

	// used for periodically tracing
	vector<PerfMetric> periodicMetrics;
	Standalone<VectorRef<ContinuousSample<double>>> opLatencies;

	MakoWorkload(WorkloadContext const& wcx)
	: KVWorkload(wcx),
	xacts("Transactions"), retries("Retries"), conflicts("Conflicts"), commits("Commits"), totalOps("Operations"),
	loadTime(0.0)
	{
		// init parameters from test file
		// Number of rows populated
		rowCount = getOption(options, LiteralStringRef("rows"), 10000);
		// Test duration in seconds
		testDuration = getOption(options, LiteralStringRef("testDuration"), 30.0);
		warmingDelay = getOption(options, LiteralStringRef("warmingDelay"), 0.0);
		maxInsertRate = getOption(options, LiteralStringRef("maxInsertRate"), 1e12);
		// One of the following modes must be specified:
		// clean : Clean up existing data; build : Populate data; run : Run the benchmark
		mode = getOption(options, LiteralStringRef("mode"), LiteralStringRef("build")).contents().toString();
		// If true, force commit for read-only transactions
		commitGet = getOption(options, LiteralStringRef("commitGet"), false);
		// Target total transaction-per-second (TPS) of all clients
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 1000.0) / clientCount;
		double allowedLatency = getOption(options, LiteralStringRef("allowedLatency"), 0.250);
		actorCountPerClient = ceil(transactionsPerSecond * allowedLatency);
		actorCountPerClient = getOption(options, LiteralStringRef("actorCountPerClient"), actorCountPerClient);
		// Sampling rate (1 sample / <sampleSize> ops) for latency stats
        sampleSize = getOption(options, LiteralStringRef("sampling"), 10);
		// If true, record latency metrics per periodicLoggingInterval; For details, see tracePeriodically()
		periodicLoggingInterval = getOption( options, LiteralStringRef("periodicLoggingInterval"), 5.0 );
		enableLogging = getOption(options, LiteralStringRef("enableLogging"), false);
		// Start time of benchmark
		metricsStart = getOption( options, LiteralStringRef("metricsStart"), 0.0 );
		// The duration of benchmark
		metricsDuration = getOption( options, LiteralStringRef("metricsDuration"), testDuration );
		// used to store randomly generated value
		valueString = std::string(maxValueBytes, '.');
		// The inserted key is formatted as: fixed prefix('mako') + sequential number + padding('x')
		// assume we want to insert 10000 rows with keyBytes set to 16, 
		// then the key goes from 'mako00000xxxxxxx' to 'mako09999xxxxxxx'
		seqNumLen = digits(rowCount) + KEYPREFIXLEN;
		// check keyBytes, maxValueBytes is valid
		ASSERT(seqNumLen <= keyBytes);
		ASSERT(keyBytes <= BUFFERSIZE);
		ASSERT(maxValueBytes <= BUFFERSIZE);
		// user input: a sequence of operations to be executed; e.g. "g10i5" means to do GET 10 times and Insert 5 times
		unparsedTxStr = getOption(options, LiteralStringRef("operations"), LiteralStringRef("g100")).contents().toString();
		//  parse the sequence and extract operations to be executed
		parse_transaction();
		for (int i = 0; i < MAX_OP; ++i) {
			// initilize per-operation latency record
			opLatencies.push_back(opLatencies.arena(), ContinuousSample<double>(rowCount / sampleSize));
			// initialize per-operation counter
			opCounters.push_back(PerfIntCounter(opNames[i]));
		}
	}

	std::string description() override {
		// Mako is a simple workload to measure the performance of FDB.
		// The primary purpose of this benchmark is to generate consistent performance results
		return "Mako";
	}

	Future<Void> setup(Database const& cx) override {
		// populate data when we are in "build" mode and use one client to do it
		if (mode.compare("build") == 0 && clientId == 0)
			return _setup(cx, this);
		return Void();
	}

	Future<Void> start(Database const& cx) override {
		// TODO: Do I need to read data to warm the cache of the keySystem like ReadWrite.actor.cpp (line 465)?
		if (mode == "clean"){
			if (clientId == 0)
				return cleanup(cx, this);
			else
				return Void();
		}

		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		return true;
	}

	void getMetrics(vector<PerfMetric>& m) override {
		if (mode == "clean"){
			return;
		}
		// Add metrics of population process
		if (mode == "build" && clientId == 0){
			m.push_back( PerfMetric( "Mean load time (seconds)", loadTime, true ) );
			// The importing rate of keys, controlled by parameter "insertionCountsToMeasure"
			auto ratesItr = ratesAtKeyCounts.begin();
			for(; ratesItr != ratesAtKeyCounts.end(); ratesItr++){
				m.push_back(PerfMetric(format("%ld keys imported bytes/sec", ratesItr->first), ratesItr->second, false));
			}
		}
		m.push_back(PerfMetric("Measured Duration", metricsDuration, true));
		m.push_back(xacts.getMetric());
		m.push_back(PerfMetric("Transactions/sec", xacts.getValue() / testDuration, true));
		m.push_back(totalOps.getMetric());
		m.push_back(PerfMetric("Operations/sec", totalOps.getValue() / testDuration, true));
		m.push_back(conflicts.getMetric());
		m.push_back(PerfMetric("Conflicts/sec", conflicts.getValue() / testDuration, true));
		m.push_back(retries.getMetric());

		// count of each operation
		for (int i = 0; i < MAX_OP; ++i){
			m.push_back(opCounters[i].getMetric());
		}

		// Meaningful Latency metrics
		const int opExecutedAtOnce[] = {OP_GETREADVERSION, OP_GET, OP_GETRANGE, OP_SGET, OP_SGETRANGE, OP_COMMIT};
		for (const int& op : opExecutedAtOnce){
			m.push_back(PerfMetric("Mean " + std::string(opNames[op]) +" Latency (ms)", 1000 * opLatencies[op].mean(), true));
			m.push_back(PerfMetric("Max " + std::string(opNames[op]) + " Latency (ms, averaged)", 1000 * opLatencies[op].max(), true));
			m.push_back(PerfMetric("Min " + std::string(opNames[op]) + " Latency (ms, averaged)", 1000 * opLatencies[op].min(), true));
		}

		//insert logging metrics if exists
		m.insert(m.end(), periodicMetrics.begin(), periodicMetrics.end());
	}
	static void randStr(std::string& str, const int& len) {
		static char randomStr[BUFFERSIZE];
		for (int i = 0; i < len; ++i) {
			randomStr[i] = '!' + g_random->randomInt(0, 'z' - '!'); /* generage a char from '!' to 'z' */
		}
		str.assign(randomStr, len);
	}

	static void randStr(char *str, const int& len){
		for (int i = 0; i < len; ++i) {
			str[i] = '!' + g_random->randomInt(0, 'z'-'!');  /* generage a char from '!' to 'z' */
		}
	}

	Value randomValue() {
		// TODO : add limit on maxvaluebytes
		const int length = g_random->randomInt(minValueBytes, maxValueBytes + 1);
		randStr(valueString, length);
		return StringRef((uint8_t*)valueString.c_str(), length);
	}

	Key ind2key(const uint64_t& ind) {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		sprintf((char*)data, "mako%0*d", seqNumLen, ind);
		for (int i = 4 + seqNumLen; i < keyBytes; ++i)
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
	Standalone<KeyValueRef> operator()(uint64_t n) {
		return KeyValueRef(ind2key(n), randomValue());
	}

	ACTOR static Future<Void> tracePeriodically( MakoWorkload *self){
		state double start = now();
		state double elapsed = 0.0;
		state int64_t last_ops = 0;
		state int64_t last_xacts = 0;

		loop {
			elapsed += self->periodicLoggingInterval;
			wait( delayUntil(start + elapsed));
			// TODO: if there is any other traceEvent to add
			TraceEvent((self->description() + "_CommitLatency").c_str()).detail("Mean", self->opLatencies[OP_COMMIT].mean()).detail("Median", self->opLatencies[OP_COMMIT].median()).detail("Percentile5", self->opLatencies[OP_COMMIT].percentile(.05)).detail("Percentile95", self->opLatencies[OP_COMMIT].percentile(.95)).detail("Count", self->opCounters[OP_COMMIT].getValue()).detail("Elapsed", elapsed);
			TraceEvent((self->description() + "_GRVLatency").c_str()).detail("Mean", self->opLatencies[OP_GETREADVERSION].mean()).detail("Median", self->opLatencies[OP_GETREADVERSION].median()).detail("Percentile5", self->opLatencies[OP_GETREADVERSION].percentile(.05)).detail("Percentile95", self->opLatencies[OP_GETREADVERSION].percentile(.95)).detail("Count", self->opCounters[OP_GETREADVERSION].getValue());
			
			std::string ts = format("T=%04.0fs: ", elapsed);
			self->periodicMetrics.push_back(PerfMetric(ts + "Transactions/sec", (self->xacts.getValue() - last_xacts) / self->periodicLoggingInterval, false));
			self->periodicMetrics.push_back(PerfMetric(ts + "Operations/sec", (self->totalOps.getValue() - last_ops) / self->periodicLoggingInterval, false));

			last_xacts = self->xacts.getValue();
			last_ops = self->totalOps.getValue();
		}
	}
	ACTOR Future<Void> _setup(Database cx, MakoWorkload* self) {

		state Promise<double> loadTime;
		state Promise<vector<pair<uint64_t, double>>> ratesAtKeyCounts;

		wait(bulkSetup(cx, self, self->rowCount, loadTime, self->insertionCountsToMeasure.empty(), self->warmingDelay,
		               self->maxInsertRate, self->insertionCountsToMeasure, ratesAtKeyCounts));

		// This is the setup time 
		self->loadTime = loadTime.getFuture().get();
		// This is the rates of importing keys
		self->ratesAtKeyCounts = ratesAtKeyCounts.getFuture().get();

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, MakoWorkload* self) {
		vector<Future<Void>> clients;
		for (int c = 0; c < self->actorCountPerClient; ++c) {
			clients.push_back(self->makoClient(cx, self, self->actorCountPerClient / self->transactionsPerSecond, c));
		}

		if (self->enableLogging)
			clients.push_back(tracePeriodically(self));

		wait( timeout( waitForAll( clients ), self->testDuration, Void() ) );
		return Void();
	}

	ACTOR Future<Void> makoClient(Database cx, MakoWorkload* self, double delay, int actorIndex) {

		state Key rkey;
		state Value rval;
		state Transaction tr(cx);
		state bool doCommit;
		state int i, count;
		state uint64_t range, indBegin, indEnd, rangeLen;
		state double lastTime = now();
		state double commitStart;
		state KeyRangeRef rkeyRangeRef;
		state vector<int> perOpCount(MAX_OP, 0);

		TraceEvent("ClientStarting").detail("AcotrIndex", actorIndex).detail("ClientIndex", self->clientId).detail("NumActors", self->actorCountPerClient);

		loop {
			// used for throttling
			wait(poisson(&lastTime, delay));
			try{
				// user-defined value: whether commit read-only ops or not; default is false
				doCommit = self->commitGet;
				for (i = 0; i < MAX_OP; ++i) {
					if (i == OP_COMMIT) 
						continue;
					for (count = 0; count < self->operations[i][0]; ++count) {
						// generate random key-val pair for operation
						rkey = self->ind2key(self->getRandomKey(self->rowCount));
						rval = self->randomValue();

						range = (self->operations[i][1] > 0) ? self->operations[i][1] : 1;
						rangeLen = digits(range);
						indBegin = self->getRandomKey(self->rowCount);
						// KeyRangeRef(min, maxPlusOne)
						indEnd = std::min(indBegin + range, self->rowCount);
						rkeyRangeRef = KeyRangeRef(self->ind2key(indBegin), self->ind2key(indEnd));

						if (i == OP_GETREADVERSION){
							state double getReadVersionStart = now();
							Version v = wait(tr.getReadVersion());
							self->opLatencies[i].addSample(now() - getReadVersionStart);
						}
						else if (i == OP_GET){
							wait(logLatency(tr.get(rkey, false), &self->opLatencies[i]));
						} else if (i == OP_GETRANGE){
							wait(logLatency(tr.getRange(rkeyRangeRef, GetRangeLimits(-1,8000)), &self->opLatencies[i]));
						}
						else if (i == OP_SGET){
							wait(logLatency(tr.get(rkey, true), &self->opLatencies[i]));
						} else if (i == OP_SGETRANGE){
							//do snapshot get range here
							wait(logLatency(tr.getRange(rkeyRangeRef, GetRangeLimits(-1,8000), true), &self->opLatencies[i]));
						} else if (i == OP_UPDATE){
							// get followed by set, TODO: do I need to add counter of get here
							wait(logLatency(tr.get(rkey, false), &self->opLatencies[OP_GET]));
							tr.set(rkey, rval, true);
							doCommit = true;
						} else if (i == OP_INSERT){
							// generate a unique key here
							std::string unique_key;
							randStr(unique_key, self->keyBytes-KEYPREFIXLEN);
							tr.set(StringRef(unique_key), rval, true);
							doCommit = true;
						} else if (i == OP_INSERTRANGE){
							Key tempKey = makeString(self->keyBytes-KEYPREFIXLEN);
							uint8_t *keyPtr = mutateString(tempKey);
							randStr((char*) keyPtr, self->keyBytes-KEYPREFIXLEN);
							for (int range_i = 0; range_i < range; ++range_i){
								sprintf((char*) keyPtr + self->keyBytes - KEYPREFIXLEN - rangeLen, "%0.*d", rangeLen, range_i);
								tr.set(tempKey, self->randomValue(), true);
							}
							doCommit = true;
						} else if (i == OP_CLEAR){
							tr.clear(rkey, true);
							doCommit = true;
						} else if(i == OP_SETCLEAR){
							state std::string st_key;
							randStr(st_key, self->keyBytes-KEYPREFIXLEN);
							tr.set(StringRef(st_key), rval, true);
							// commit the change and update metrics
							commitStart = now();
							wait(tr.commit());
							self->opLatencies[OP_COMMIT].addSample(now() - commitStart);
							++perOpCount[OP_COMMIT];
							tr.reset();
							tr.clear(st_key, true);
							doCommit = true;
						} else if (i == OP_CLEARRANGE){
							tr.clear(rkeyRangeRef, true);
							doCommit = true;
						} else if (i == OP_SETCLEARRANGE){
							// Key tempKey[self->keyBytes-KEYPREFIXLEN+1];
							Key tempKey = makeString(self->keyBytes-KEYPREFIXLEN);
							uint8_t *keyPtr = mutateString(tempKey);
							randStr((char*) keyPtr, self->keyBytes-KEYPREFIXLEN);
							state std::string scr_start_key;
							state std::string scr_end_key;
							for (int range_i = 0; range_i < range; ++range_i){
								sprintf((char*) keyPtr + self->keyBytes - KEYPREFIXLEN - rangeLen, "%0.*d", rangeLen, range_i);
								tr.set(tempKey, self->randomValue(), true);
								if (range_i == 0)
									scr_start_key = tempKey.toString();
							}
							scr_end_key = tempKey.toString();
							commitStart = now();
							wait(tr.commit());
							self->opLatencies[OP_COMMIT].addSample(now() - commitStart);
							++perOpCount[OP_COMMIT];
							tr.reset();
							tr.clear(KeyRangeRef(StringRef(scr_start_key), StringRef(scr_end_key)), true);
							doCommit = true;
						}
						++perOpCount[i];
					}
				}

				if (doCommit) {
					commitStart = now();
					wait(tr.commit());
					self->opLatencies[OP_COMMIT].addSample(now() - commitStart);
					++perOpCount[OP_COMMIT];
				}
				// successfully finish the transaction, update metrics
				++self->xacts;
				for (int op = 0; op < MAX_OP; ++op){
					self->opCounters[op] += perOpCount[op];
					self->totalOps += perOpCount[op];
				}
			} catch (Error& e) {
				// TODO: to see how to log the error here
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

	ACTOR Future<Void> cleanup(Database cx, MakoWorkload* self){
		// TODO: check the length of possible longest key
		state std::string startKey("mako\x00");
		state std::string endKey("mako\xff");
		state Transaction tr(cx);

		loop{
			try {
				tr.clear(KeyRangeRef(KeyRef(startKey), KeyRef(endKey)));
				wait(tr.commit());
				break;
			} catch (Error &e){
				wait(tr.onError(e));
				tr.reset();
			}
		}

		return Void();
	}

	ACTOR static Future<Void> logLatency(Future<Optional<Value>> f, ContinuousSample<double>* opLatencies) {
		state double opBegin = now();
		Optional<Value> value = wait(f);

		double latency = now() - opBegin;
		opLatencies->addSample(latency);
		return Void();
	}

	ACTOR static Future<Void> logLatency(Future<Standalone<RangeResultRef>> f, ContinuousSample<double>* opLatencies){
		state double opBegin = now();
		Standalone<RangeResultRef> value = wait(f);

		double latency = now() - opBegin;
		opLatencies->addSample(latency);
		return Void();
	}

	int64_t getRandomKey(uint64_t rowCount) {
		// TODO: support other distribution like zipf
		return g_random->randomInt64(0, rowCount); 
	}
	int parse_transaction() {
		const char *ptr = unparsedTxStr.c_str();
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
			fprintf(stderr, "ERROR: invalid transaction specification %s\n", ptr);
			return -1;
		}

		return 0;
	}
};

WorkloadFactory<MakoWorkload> MakoloadFactory("Mako");
