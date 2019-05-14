#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/workloads/Mako.h"
#include "flow/actorcompiler.h"

static const std::string opNames[] = {"GRV", "GET", "GETRANGE", "SGET", "SGETRANGE", "UPDATE", "INSERT", "CLEAR", "CLEARRANGE", "COMMIT"};

struct MakoWorkload : KVWorkload {
	uint64_t rowCount, seqNumLen, sampleSize, actorCount;
	double testDuration, loadTime, warmingDelay, maxInsertRate, transactionsPerSecond, allowedLatency, periodicLoggingInterval;

	double metricsStart, metricsDuration;

	bool enableLogging, cancelWorkersAtDuration, commitGet;

	vector<Future<Void>> clients;

	PerfIntCounter xacts, retries, conflicts, commits, totalOps;
	vector<PerfIntCounter> opCounters;
	vector<uint64_t> insertionCountsToMeasure;
	vector<pair<uint64_t, double>> ratesAtKeyCounts;

	std::string valueString;

	// used for parse usr input tx and store
	int operations[MAX_OP][2];
	std::string unparsed_tx_str, mode;


	// used for periodically trace metrics
	vector<PerfMetric> periodicMetrics;
	Standalone<VectorRef<ContinuousSample<double>>> opLatencies;

	MakoWorkload(WorkloadContext const& wcx)
	: KVWorkload(wcx),
	xacts("Transactions"), retries("Retries"), conflicts("Conflicts"), commits("Commits"), totalOps("Operations"),
	loadTime(0.0)
	{
		// init for metrics

		// get parameters
		rowCount = getOption(options, LiteralStringRef("rows"), 10000);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 30.0);
		warmingDelay = getOption(options, LiteralStringRef("warmingDelay"), 0.0);
		maxInsertRate = getOption(options, LiteralStringRef("maxInsertRate"), 1e12);
		mode = getOption(options, LiteralStringRef("mode"), LiteralStringRef("build")).contents().toString();
		commitGet = getOption(options, LiteralStringRef("commitGet"), false);

		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 1000.0) / clientCount;
		double allowedLatency = getOption(options, LiteralStringRef("allowedLatency"), 0.250);
		actorCount = ceil(transactionsPerSecond * allowedLatency);
		actorCount = getOption(options, LiteralStringRef("actorCountPerTester"), actorCount);
        sampleSize = getOption(options, LiteralStringRef("sampling"), 10);
		enableLogging = getOption(options, LiteralStringRef("enableLogging"), false);
		cancelWorkersAtDuration = getOption( options, LiteralStringRef("cancelWorkersAtDuration"), false );

		valueString = std::string(maxValueBytes, '.');

		periodicLoggingInterval = getOption( options, LiteralStringRef("periodicLoggingInterval"), 5.0 );

		metricsStart = getOption( options, LiteralStringRef("metricsStart"), 0.0 );
		metricsDuration = getOption( options, LiteralStringRef("metricsDuration"), testDuration );

		seqNumLen = digits(rowCount) + 4;
		ASSERT(seqNumLen <= keyBytes);

		// Operations
		unparsed_tx_str =
		    getOption(options, LiteralStringRef("operations"), LiteralStringRef("g100")).contents().toString();
		parse_transaction();

		for (int i = 0; i < MAX_OP; ++i) {
			// initilize per-operation latency record
			opLatencies.push_back(opLatencies.arena(), ContinuousSample<double>(rowCount / sampleSize));
			// initialize per-operation counter
			opCounters.push_back(PerfIntCounter(opNames[i]));
		}
	}

	std::string description() override { return "Mako"; }

	Future<Void> setup(Database const& cx) override {
		// only populate data when we are in build mode
		if (mode.compare("build"))
			return Void();

		return _setup(cx, this); 
	}

	Future<Void> start(Database const& cx) override {
		// TODO: may need to delete this one
		// only use one client for performence measurement

		// TODO: Do I need to read record to warm the cache of the keySystem
		if (mode == "clean"){
			if (clientId == 0)
				return cleanup(cx, this);
			else
				return Void();
		}

		return _start(cx, this);
	}

	Future<bool> check(Database const& cx) override {
		if(!cancelWorkersAtDuration && now() < metricsStart + metricsDuration)
			metricsDuration = now() - metricsStart;

		return true;
	}

	void getMetrics(vector<PerfMetric>& m) override {
		if (mode == "clean")
			return;
		else if (mode == "build"){
			m.push_back( PerfMetric( "Mean load time (seconds)", loadTime, true ) );
			// TODO: figure out this metric
			auto ratesItr = ratesAtKeyCounts.begin();
			for(; ratesItr != ratesAtKeyCounts.end(); ratesItr++)
				m.push_back(PerfMetric(format("%ld keys imported bytes/sec", ratesItr->first), ratesItr->second, false));
				return;
		}
		// metrics for mode = "run"
		m.push_back(PerfMetric("Measured Duration", metricsDuration, true));
		m.push_back(xacts.getMetric());
		m.push_back(PerfMetric("Transactions/sec", xacts.getValue() / testDuration, true));
		m.push_back(totalOps.getMetric());
		m.push_back(PerfMetric("Operations/sec", totalOps.getValue() / testDuration, true));
		m.push_back(conflicts.getMetric());
		m.push_back(PerfMetric("Conflicts/sec", conflicts.getValue() / testDuration, true));
		m.push_back(retries.getMetric());
		
		// m.push_back(commits.getMetric());
		// m.push_back(PerfMetric())

		// give per-operation level metrics
		for (int i = 0; i < MAX_OP; ++i){
			m.push_back(opCounters[i].getMetric());
		}

		m.push_back(PerfMetric("Mean GetReadVersion Latency (ms)", 1000 * opLatencies[OP_GETREADVERSION].mean(), true));
		m.push_back(PerfMetric("Max GetReadVersion Latency (ms, averaged)", 1000 * opLatencies[OP_GETREADVERSION].max(), true));
		m.push_back(PerfMetric("Min GetReadVersion Latency (ms, averaged)", 1000 * opLatencies[OP_GETREADVERSION].min(), true));

		m.push_back(PerfMetric("Mean GET Latency (ms)", 1000 * opLatencies[OP_GET].mean(), true));
		m.push_back(PerfMetric("Max GET Latency (ms, averaged)", 1000 * opLatencies[OP_GET].max(), true));
		m.push_back(PerfMetric("Min GET Latency (ms, averaged)", 1000 * opLatencies[OP_GET].min(), true));

		m.push_back(PerfMetric("Mean GET RANGE Latency (ms)", 1000 * opLatencies[OP_GETRANGE].mean(), true));
		m.push_back(PerfMetric("Max GET RANGE Latency (ms, averaged)", 1000 * opLatencies[OP_GETRANGE].max(), true));
		m.push_back(PerfMetric("Min GET RANGE Latency (ms, averaged)", 1000 * opLatencies[OP_GETRANGE].min(), true));

		//insert logging metrics
		m.insert(m.end(), periodicMetrics.begin(), periodicMetrics.end());
	}
	static void randStr(std::string& str, const int& len) {
		char randomStr[len];
		for (int i = 0; i < len; ++i) {
			randomStr[i] = '!' + g_random->randomInt(0, 'z' - '!'); /* generage a char from '!' to 'z' */
		}
		str.assign(randomStr, len);
	}

	static void randStr(char *str, const int& len){
		for (int i = 0; i < len-1; ++i) {
			str[i] = '!' + g_random->randomInt(0, 'z'-'!');  /* generage a char from '!' to 'z' */
		}
		// here len contains the null character
		str[len-1] = '\0';
	}
	Value randomValue() {
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
		for (int c = 0; c < self->actorCount; ++c) {
			// TODO: to see if we need throttling here, 
			clients.push_back(self->makoClient(cx, self, self->actorCount / self->transactionsPerSecond, c));
		}

		if (self->enableLogging)
			clients.push_back(tracePeriodically(self));

		if (!self->cancelWorkersAtDuration) 
			self->clients = clients;
		wait( timeout( waitForAll( clients ), self->testDuration, Void() ) );
		return Void();
	}

	ACTOR Future<Void> makoClient(Database cx, MakoWorkload* self, double delay, int actorIndex) {

		state Key rkey;
		state Value rval;
		state Transaction tr(cx);
		state bool doCommit;
		state int i, count;
		state uint64_t range, indBegin, indEnd;
		state double lastTime = now();
		state KeyRangeRef rkeyRangeRef;
		state vector<int> perOpCount(MAX_OP, 0);

		TraceEvent("ClientStarting").detail("AcotrIndex", actorIndex).detail("ClientIndex", self->clientId).detail("NumActors", self->actorCount);

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
							// get followed by set
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
							char tempKey[self->keyBytes-KEYPREFIXLEN+1];
							int rangeLen = digits(range);
							randStr(tempKey, self->keyBytes-KEYPREFIXLEN+1);
							for (int range_i = 0; range_i < range; ++range_i){
								sprintf(tempKey + self->keyBytes - KEYPREFIXLEN - rangeLen, "%0.*d", rangeLen, range_i);
								tr.set(StringRef(std::string(tempKey)), self->randomValue(), true);
							}
							doCommit = true;
						} else if (i == OP_CLEAR){
							// happens locally, we only care the tps for these ops
							tr.clear(rkey, true);
							doCommit = true;
						} else if(i == OP_SETCLEAR){
							state std::string st_key;
							randStr(st_key, self->keyBytes-KEYPREFIXLEN);
							tr.set(StringRef(st_key), rval, true);
							// commit the change
							wait(tr.commit());
							tr.reset();
							tr.clear(st_key, true);
							doCommit = true;
						} else if (i == OP_CLEARRANGE){
							tr.clear(rkeyRangeRef, true);
							doCommit = true;
						} else if (i == OP_SETCLEARRANGE){
							char tempKey[self->keyBytes-KEYPREFIXLEN+1];
							int rangeLen = digits(range);
							randStr(tempKey, self->keyBytes-KEYPREFIXLEN+1);
							state std::string scr_start_key;
							state std::string scr_end_key;
							for (int range_i = 0; range_i < range; ++range_i){
								sprintf(tempKey + self->keyBytes - KEYPREFIXLEN - rangeLen, "%0.*d", rangeLen, range_i);
								tr.set(StringRef(std::string(tempKey)), self->randomValue(), true);
								if (range_i == 0)
									scr_start_key = std::string(tempKey);
							}
							scr_end_key = std::string(tempKey);
							wait(tr.commit());
							tr.reset();
							tr.clear(KeyRangeRef(StringRef(scr_start_key), StringRef(scr_end_key)), true);
							doCommit = true;
						}
						++perOpCount[i];
					}
				}

				if (doCommit) {
					state double commitStart = now();
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
		int len = unparsed_tx_str.length();
		int i = 0;
		int op = 0;
		int rangeop = 0;
		int num;
		int error = 0;

		for (op = 0; op < MAX_OP; op++) {
			operations[op][OP_COUNT] = 0;
			operations[op][OP_RANGE] = 0;
		}

		op = 0;
		while (i < len) {
			if (unparsed_tx_str[i] == 'g') {
				if ((i + 1 < len) && (unparsed_tx_str[i + 1] == 'r')) {
					if ((i + 2 < len) && (unparsed_tx_str[i + 2] == 'v')) {
						op = OP_GETREADVERSION;
						i += 3;
					} else {
						op = OP_GETRANGE;
						rangeop = 1;
						i += 2;
					}
				} else {
					op = OP_GET;
					i++;
				}
			} else if (unparsed_tx_str[i] == 's') {
				if ((i + 1 >= len) || (unparsed_tx_str[i + 1] != 'g')) {
					error = 1;
					break;
				}
				if ((i + 2 < len) && (unparsed_tx_str[i + 2] == 'r')) {
					op = OP_SGETRANGE;
					rangeop = 1;
					i += 3;
				} else {
					op = OP_SGET;
					i += 2;
				}
			} else if (unparsed_tx_str[i] == 'c') {
				if ((i + 1 >= len) || (unparsed_tx_str[i + 1] != 'l')) {
					error = 1;
					break;
				}
				if ((i + 2 < len) && (unparsed_tx_str[i + 2] == 'r')) {
					op = OP_CLEARRANGE;
					rangeop = 1;
					i += 3;
				} else {
					op = OP_CLEAR;
					i += 2;
				}
			} else if (unparsed_tx_str[i] == 'u') {
				op = OP_UPDATE;
				i++;
			} else if (unparsed_tx_str[i] == 'i') {
				op = OP_INSERT;
				i++;
			}

			/* count */
			num = 0;
			if ((unparsed_tx_str[i] < '0') || (unparsed_tx_str[i] > '9')) {
				num = 1; /* if omitted, set it to 1 */
			} else {
				while ((unparsed_tx_str[i] >= '0') && (unparsed_tx_str[i] <= '9')) {
					num = num * 10 + unparsed_tx_str[i] - '0';
					i++;
				}
			}
			/* set count */
			operations[op][OP_COUNT] = num;

			if (rangeop) {
				if (!unparsed_tx_str[i] || (unparsed_tx_str[i] != ':')) {
					error = 1;
					break;
				} else {
					i++; /* skip ':' */
					num = 0;
					if ((!unparsed_tx_str[i]) || ((unparsed_tx_str[i] < '0') || (unparsed_tx_str[i] > '9'))) {
						error = 1;
						break;
					}
					while ((unparsed_tx_str[i] >= '0') && (unparsed_tx_str[i] <= '9')) {
						num = num * 10 + unparsed_tx_str[i] - '0';
						i++;
					}
					/* set range */
					operations[op][OP_RANGE] = num;
				}
			}
			rangeop = 0;
		}

		if (error) {
			fprintf(stderr, "ERROR: invalid transaction specification %s\n", unparsed_tx_str.c_str());
			return -1;
		}

		return 0;
	}
};

WorkloadFactory<MakoWorkload> MakoloadFactory("Mako");
