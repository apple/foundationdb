
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "workloads.actor.h"
#include "fdbserver/workloads/BulkSetup.actor.h"
#include "fdbserver/workloads/Mako.h"
#include "flow/actorcompiler.h"


static std::string opNames[] = {"GRV", "GET", "GETRANGE", "SGET", "SGETRANGE", "UPDATE", "INSERT", "CLEAR", "CLEARRANGE", "COMMIT"};

struct MakoWorkload : KVWorkload {
	uint64_t rowCount, seqNumLen, sampleSize, actorPerClient;
	double testDuration, loadTime, warmingDelay, maxInsertRate, transactionsPerSecond, allowedLatency, periodicLoggingInterval;

	double metricsStart, metricsDuration;

	bool enableLogging, cancelWorkersAtDuration;

	vector<Future<Void>> clients;

	PerfIntCounter xacts, errors, conflicts, commits, totalOps;
	vector<PerfIntCounter> opCounters;
	vector<uint64_t> insertionCountsToMeasure;
	vector<pair<uint64_t, double>> ratesAtKeyCounts;

	std::string valueString;

	// used for parse usr input tx and store
	int operations[MAX_OP][2];
	std::string unparsed_tx_str;


	// used for periodically trace metrics
	vector<PerfMetric> periodicMetrics;
	Standalone<VectorRef<ContinuousSample<double>>> opLatencies;

	MakoWorkload(WorkloadContext const& wcx)
	: KVWorkload(wcx), xacts("Transactions"), errors("Errors"), conflicts("Conflicts"), commits("Commits"), totalOps("Operations") {
		// init for metrics

		// get parameters
		rowCount = getOption(options, LiteralStringRef("rows"), 10000);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 30.0);
		warmingDelay = getOption(options, LiteralStringRef("warmingDelay"), 0.0);
		maxInsertRate = getOption(options, LiteralStringRef("maxInsertRate"), 1e12);

		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 1000.0) / clientCount;
		double allowedLatency = getOption(options, LiteralStringRef("allowedLatency"), 0.250);
		actorCount = ceil(transactionsPerSecond * allowedLatency);
		actorCount = getOption(options, LiteralStringRef("actorCountPerTester"), actorCount);
        actorPerClient = getOption(options, LiteralStringRef("actorPerClient"), 1);
        sampleSize = getOption(options, LiteralStringRef("sampling"), 10);
		enableLogging = getOption(options, LiteralStringRef("enableLogging"), false);
		cancelWorkersAtDuration = getOption( options, LiteralStringRef("cancelWorkersAtDuration"), false );

		// valueString = (char *) malloc(sizeof(char) * (maxValueBytes+1));
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

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }

	Future<Void> start(Database const& cx) override {
		// only use one client for performence measurement
		if (clientId == 0)
			return _start(cx, this);
		return Void();
	}

	Future<bool> check(Database const& cx) override {
		// TODO: to see whether it needs to check anything here
		if (clientId)
			return true;

		if(!cancelWorkersAtDuration && now() < metricsStart + metricsDuration)
			metricsDuration = now() - metricsStart;

		return true;
	}

	void getMetrics(vector<PerfMetric>& m) override {
		if (clientId)
			return;
		m.push_back(PerfMetric("Measured Duration", metricsDuration, true));
		m.push_back(xacts.getMetric());
		m.push_back(PerfMetric("Transactions/sec", xacts.getValue() / testDuration, true));
		m.push_back(totalOps.getMetric());
		m.push_back(PerfMetric("Operations/sec", totalOps.getValue() / testDuration, true));
		m.push_back(conflicts.getMetric());
		m.push_back(PerfMetric("Conflicts/sec", conflicts.getValue() / testDuration, true));
		m.push_back(errors.getMetric());
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

	ACTOR static Future<Void> tracePeriodically( MakoWorkload *self){
		state double start = now();
		state double elapsed = 0.0;
		state int64_t last_ops = 0;
		state int64_t last_xacts = 0;

		loop {
			elapsed += self->periodicLoggingInterval;
			wait( delayUntil(start + elapsed));
			// TODO: if there is any other traceEvent to add
			TraceEvent((self->description() + "_CommitLatency").c_str()).detail("Mean", self->opLatencies[OP_COMMIT].mean()).detail("Median", self->opLatencies[OP_COMMIT].median());

			std::string ts = format("T=%04.0fs: ", elapsed);
			self->periodicMetrics.push_back(PerfMetric(ts + "Transactions/sec", (self->xacts.getValue() - last_xacts) / self->periodicLoggingInterval, false));
			self->periodicMetrics.push_back(PerfMetric(ts + "Operations/sec", (self->totalOps.getValue() - last_ops) / self->periodicLoggingInterval, false));

			last_xacts = self->xacts.getValue();
			last_ops = self->totalOps.getValue();
		}
	}

	static void randstr(std::string& str, int len) {
		// TODO: which way is better?
		char randomStr[len];
		for (int i = 0; i < len; i++) {
			randomStr[i] = '!' + g_random->randomInt(0, 'z' - '!'); /* generage a char from '!' to 'z' */
		}
		str.assign(randomStr, len);
	}
	Value randomValue() {
		int length = g_random->randomInt(minValueBytes, maxValueBytes + 1);
		randstr(valueString, length);
		return StringRef((uint8_t*)valueString.c_str(), length);
	}

	Key ind2key(uint64_t ind) {
		// TODO: lookup how mutateString/makeString works
		// TODO: See how standalone passed as value
		// TODO: See do we need null for this mutatestring
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		sprintf((char*)data, "mako%0*d", seqNumLen, ind);
		for (int i = 4 + seqNumLen; i < keyBytes; i++) data[i] = 'x';
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
		// return KeyValueRef( keyForIndex( n, false ), randomValue() );
		return KeyValueRef(ind2key(n), randomValue());
	}

	ACTOR Future<Void> _setup(Database cx, MakoWorkload* self) {

		state Promise<double> loadTime;
		state Promise<vector<pair<uint64_t, double>>> ratesAtKeyCounts;

		wait(bulkSetup(cx, self, self->rowCount, loadTime, self->insertionCountsToMeasure.empty(), self->warmingDelay,
		               self->maxInsertRate, self->insertionCountsToMeasure, ratesAtKeyCounts));

		// TODO: Understand what these two paras do
		self->loadTime = loadTime.getFuture().get();
		self->ratesAtKeyCounts = ratesAtKeyCounts.getFuture().get();

		return Void();
	}

	ACTOR Future<Void> _start(Database cx, MakoWorkload* self) {
		vector<Future<Void>> clients;
		for (int c = 0; c < self->actorPerClient; ++c) {
			clients.push_back(self->makoClient(cx, self, self->actorPerClient / self->transactionsPerSecond));
		}

		if (self->enableLogging)
			clients.push_back(tracePeriodically(self));

		if (!self->cancelWorkersAtDuration) 
			self->clients = clients;
		wait( timeout( waitForAll( clients ), self->testDuration, Void() ) );
		return Void();
	}

	ACTOR Future<Void> makoClient(Database cx, MakoWorkload* self, double unused_delay) {

		state Key rkey;
		state Value rval;
		state Transaction tr(cx);
		state bool doCommit;
		state int i, count;
		state uint64_t range, indBegin, indEnd;
		state KeyRangeRef rkeyRangeRef;
		// TODO: try to find out why I cannot use int array here with state
		state vector<int> perOpCount(MAX_OP, 0);

		loop {
			try{
				// default to not commit for each transaction
				doCommit = false;
				for (i = 0; i < MAX_OP; ++i) {
					// TODO: check if operations initilize correctly
					if (i == OP_COMMIT) continue;
					// generate random key-val pair for operation
					for (count = 0; count < self->operations[i][0]; ++count) {
						// TODO: check if cast is fine
						rkey = self->ind2key(self->getRandomKey(self->rowCount));
						// TODO: check this one safe since it only uses valueString
						rval = self->randomValue();

						// only used when range-oriented operations
						range = (self->operations[i][1] > 0) ? self->operations[i][1] : 1;
						indBegin = self->getRandomKey(self->rowCount);
						indEnd = std::min(indBegin + range - 1, self->rowCount);
						rkeyRangeRef = KeyRangeRef(self->ind2key(indBegin), self->ind2key(indEnd));
						//TODO: switch does not work fine in ACTOR framework, is it a bug?
						if (i == OP_GETREADVERSION){
							state double getReadVersionStart = now();
							Version v = wait(tr.getReadVersion());
							self->opLatencies[i].addSample(now() - getReadVersionStart);
						}
						else if (i == OP_GET){
							wait(logLatency(tr.get(rkey, false), &self->opLatencies[i]));
						} else if (i == OP_GETRANGE){
							// TODO: why does it use strinc here
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
							randstr(unique_key, self->keyBytes-6);
							StringRef unique_keyRef(unique_key);
							// TODO: check if here is a problem, and look at the new operator for Arena
							tr.set(unique_keyRef, rval, true);
							doCommit = true;
						} else if (i == OP_CLEAR){
							// happens locally, we only care the tps for these ops
							tr.clear(rkey, true);
							doCommit = true;
						} else if (i == OP_CLEARRANGE){
							tr.clear(rkeyRangeRef, true);
							doCommit = true;
						}
						++perOpCount[i];
					}
				}
				// TODO: to see whether we need to record only the middle 3/4 records
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
				// TODO: add count for retry, see onError code to see if it cancels the job
				if (e.code() == error_code_operation_cancelled)
					throw;
				
				++self->errors;
				if (e.code() == error_code_not_committed)
					++self->conflicts;
				wait(tr.onError(e));
			}
			// reset all the operations' counters to 0
			std::fill(perOpCount.begin(), perOpCount.end(), 0);
			tr.reset();
		}
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

	int64_t getRandomKey(uint64_t rowCount) { return g_random->randomInt64(0, rowCount); }
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

		// if (args->verbose == VERBOSE_DEBUG) {
		//     for (op = 0; op < MAX_OP; op++) {
		//     printf("DEBUG: OP: %d: %d: %d\n", op, args->txnspec.ops[op][0],
		//             args->txnspec.ops[op][1]);
		//     }
		// }

		return 0;
	}
};

WorkloadFactory<MakoWorkload> MakoloadFactory("Mako");
