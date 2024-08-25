/*
 * Throughput.actor.cpp
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

#include "fdbrpc/DDSketch.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbserver/TesterInterface.h"
#include "fdbserver/WorkerInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/ActorCollection.h"
#include "fdbrpc/Smoother.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ITransactor : ReferenceCounted<ITransactor> {
	struct Stats {
		int64_t reads, writes, retries, transactions;
		double totalLatency, grvLatency, rowReadLatency, commitLatency;
		Stats()
		  : reads(0), writes(0), retries(0), transactions(0), totalLatency(0), grvLatency(0), rowReadLatency(0),
		    commitLatency(0) {}
		void operator+=(Stats const& s) {
			reads += s.reads;
			writes += s.writes;
			retries += s.retries;
			transactions += s.transactions;
			totalLatency += s.totalLatency;
			grvLatency += s.grvLatency;
			rowReadLatency += s.rowReadLatency;
			commitLatency += s.commitLatency;
		}
	};

	virtual Future<Void> doTransaction(Database const&, Stats* stats) = 0;
	virtual ~ITransactor() {}
};

struct RWTransactor : ITransactor {
	int reads, writes;
	int minValueBytes, maxValueBytes;
	std::string valueString;
	int keyCount, keyBytes;

	RWTransactor(int reads, int writes, int keyCount, int keyBytes, int minValueBytes, int maxValueBytes)
	  : reads(reads), writes(writes), minValueBytes(minValueBytes), maxValueBytes(maxValueBytes), keyCount(keyCount),
	    keyBytes(keyBytes) {
		ASSERT(minValueBytes <= maxValueBytes);
		valueString = std::string(maxValueBytes, '.');
	}

	Key randomKey() {
		Key result = makeString(keyBytes);
		uint8_t* data = mutateString(result);
		memset(data, '.', keyBytes);

		double d = double(deterministicRandom()->randomInt(0, keyCount)) / keyCount;
		emplaceIndex(data, 0, *(int64_t*)&d);

		return result;
	}

	Value randomValue() {
		return StringRef((const uint8_t*)valueString.c_str(),
		                 deterministicRandom()->randomInt(minValueBytes, maxValueBytes + 1));
	};

	Future<Void> doTransaction(Database const& db, Stats* stats) override {
		return rwTransaction(db, Reference<RWTransactor>::addRef(this), stats);
	}

	ACTOR static Future<Optional<Value>> getLatency(Future<Optional<Value>> f, double* t) {
		Optional<Value> v = wait(f);
		*t += now();
		return v;
	}

	ACTOR static Future<Void> rwTransaction(Database db, Reference<RWTransactor> self, Stats* stats) {
		state std::vector<Key> keys;
		state std::vector<Value> values;
		state Transaction tr(db);

		for (int op = 0; op < self->reads || op < self->writes; op++)
			keys.push_back(self->randomKey());
		values.reserve(self->writes);
		for (int op = 0; op < self->writes; op++)
			values.push_back(self->randomValue());

		loop {
			try {
				state double t_start = now();
				wait(success(tr.getReadVersion()));
				state double t_rv = now();
				state double rrLatency = -t_rv * self->reads;

				state std::vector<Future<Optional<Value>>> reads;
				reads.reserve(self->reads);
				for (int i = 0; i < self->reads; i++)
					reads.push_back(getLatency(tr.get(keys[i]), &rrLatency));
				wait(waitForAll(reads));
				for (int i = 0; i < self->writes; i++)
					tr.set(keys[i], values[i]);
				state double t_beforeCommit = now();
				wait(tr.commit());

				stats->transactions++;
				stats->reads += self->reads;
				stats->writes += self->writes;
				stats->grvLatency += t_rv - t_start;
				stats->commitLatency += now() - t_beforeCommit;
				stats->rowReadLatency += rrLatency / self->reads;
				break;
			} catch (Error& e) {
				wait(tr.onError(e));
				stats->retries++;
			}
		}

		return Void();
	}
};

struct ABTransactor : ITransactor {
	Reference<ITransactor> a, b;
	double alpha; // 0.0 = all a, 1.0 = all b

	ABTransactor(double alpha, Reference<ITransactor> a, Reference<ITransactor> b) : a(a), b(b), alpha(alpha) {}

	Future<Void> doTransaction(Database const& db, Stats* stats) override {
		return deterministicRandom()->random01() >= alpha ? a->doTransaction(db, stats) : b->doTransaction(db, stats);
	}
};

struct SweepTransactor : ITransactor {
	// Runs a linearly-changing workload that changes from A-type to B-type over
	//    the specified duration--the timer starts at the first transaction.
	Reference<ITransactor> a, b;
	double startTime;
	double startDelay;
	double duration;

	SweepTransactor(double duration, double startDelay, Reference<ITransactor> a, Reference<ITransactor> b)
	  : a(a), b(b), startTime(-1), startDelay(startDelay), duration(duration) {}

	Future<Void> doTransaction(Database const& db, Stats* stats) override {
		if (startTime == -1)
			startTime = now() + startDelay;

		double alpha;
		double n = now();
		if (n < startTime)
			alpha = 0;
		else if (n > startTime + duration)
			alpha = 1;
		else
			alpha = (n - startTime) / duration;

		return deterministicRandom()->random01() >= alpha ? a->doTransaction(db, stats) : b->doTransaction(db, stats);
	}
};

struct IMeasurer : ReferenceCounted<IMeasurer> {
	// This could be an ITransactor, but then it needs an actor to wait for the transaction to actually finish
	virtual Future<Void> start() { return Void(); }
	virtual void addTransaction(ITransactor::Stats* stats, double now) = 0;
	virtual void getMetrics(std::vector<PerfMetric>& m) = 0;
	IMeasurer& operator=(IMeasurer const&) {
		return *this;
	} // allow copy operator for non-reference counted instances of subclasses

	virtual ~IMeasurer() {}
};

struct MeasureSinglePeriod : IMeasurer {
	double delay, duration;
	double startT;

	DDSketch<double> totalLatency, grvLatency, rowReadLatency, commitLatency;
	ITransactor::Stats stats; // totalled over the period

	MeasureSinglePeriod(double delay, double duration)
	  : delay(delay), duration(duration), totalLatency(), grvLatency(), rowReadLatency(), commitLatency() {}

	Future<Void> start() override {
		startT = now();
		return Void();
	}
	void addTransaction(ITransactor::Stats* st, double now) override {
		if (!(now >= startT + delay && now < startT + delay + duration))
			return;

		totalLatency.addSample(st->totalLatency);
		grvLatency.addSample(st->grvLatency);
		rowReadLatency.addSample(st->rowReadLatency);

		if (st->commitLatency > 0) {
			commitLatency.addSample(st->commitLatency);
		}

		stats += *st;
	}
	void getMetrics(std::vector<PerfMetric>& m) override {
		double measureDuration = duration;
		m.emplace_back("Transactions/sec", stats.transactions / measureDuration, Averaged::False);
		m.emplace_back("Retries/sec", stats.retries / measureDuration, Averaged::False);
		m.emplace_back("Operations/sec", (stats.reads + stats.writes) / measureDuration, Averaged::False);
		m.emplace_back("Read rows/sec", stats.reads / measureDuration, Averaged::False);
		m.emplace_back("Write rows/sec", stats.writes / measureDuration, Averaged::False);

		m.emplace_back("Mean Latency (ms)", 1000 * totalLatency.mean(), Averaged::True);
		m.emplace_back("Median Latency (ms, averaged)", 1000 * totalLatency.median(), Averaged::True);
		m.emplace_back("90% Latency (ms, averaged)", 1000 * totalLatency.percentile(0.90), Averaged::True);
		m.emplace_back("98% Latency (ms, averaged)", 1000 * totalLatency.percentile(0.98), Averaged::True);

		m.emplace_back("Mean Row Read Latency (ms)", 1000 * rowReadLatency.mean(), Averaged::True);
		m.emplace_back("Median Row Read Latency (ms, averaged)", 1000 * rowReadLatency.median(), Averaged::True);
		m.emplace_back("Mean GRV Latency (ms)", 1000 * grvLatency.mean(), Averaged::True);
		m.emplace_back("Median GRV Latency (ms, averaged)", 1000 * grvLatency.median(), Averaged::True);
		m.emplace_back("Mean Commit Latency (ms)", 1000 * commitLatency.mean(), Averaged::True);
		m.emplace_back("Median Commit Latency (ms, averaged)", 1000 * commitLatency.median(), Averaged::True);
	}
};

struct MeasurePeriodically : IMeasurer {
	double period;
	std::set<std::string> includeMetrics;
	MeasureSinglePeriod msp, msp0;
	std::vector<PerfMetric> accumulatedMetrics;

	MeasurePeriodically(double period, std::set<std::string> includeMetrics)
	  : period(period), includeMetrics(includeMetrics), msp(0, period), msp0(0, period) {}

	Future<Void> start() override {
		msp.start();
		return periodicActor(this);
	}
	void addTransaction(ITransactor::Stats* st, double now) override { msp.addTransaction(st, now); }
	void getMetrics(std::vector<PerfMetric>& m) override {
		m.insert(m.end(), accumulatedMetrics.begin(), accumulatedMetrics.end());
	}
	void nextPeriod(double t) {
		// output stats
		std::string prefix = format("T=%04.0fs:", t);
		std::vector<PerfMetric> m;
		msp.getMetrics(m);
		for (auto i = m.begin(); i != m.end(); ++i)
			if (includeMetrics.count(i->name())) {
				accumulatedMetrics.push_back(i->withPrefix(prefix));
			}

		// reset stats
		msp = msp0;
		msp.start();
	}

	ACTOR static Future<Void> periodicActor(MeasurePeriodically* self) {
		state double startT = now();
		state double elapsed = 0;
		loop {
			elapsed += self->period;
			wait(delayUntil(startT + elapsed));
			self->nextPeriod(elapsed);
		}
	}
};

struct MeasureMulti : IMeasurer {
	std::vector<Reference<IMeasurer>> ms;
	Future<Void> start() override {
		std::vector<Future<Void>> s;
		for (auto m = ms.begin(); m != ms.end(); ++m)
			s.push_back((*m)->start());
		return waitForAll(s);
	}
	void addTransaction(ITransactor::Stats* stats, double now) override {
		for (auto m = ms.begin(); m != ms.end(); ++m)
			(*m)->addTransaction(stats, now);
	}
	void getMetrics(std::vector<PerfMetric>& metrics) override {
		for (auto m = ms.begin(); m != ms.end(); ++m)
			(*m)->getMetrics(metrics);
	}
};

struct ThroughputWorkload : TestWorkload {
	static constexpr auto NAME = "Throughput";

	double targetLatency, testDuration, Pgain, Igain;
	Reference<ITransactor> op;
	Reference<IMeasurer> measurer;

	int activeActors;
	double totalLatencyIntegral, totalTransactionsIntegral, startT;

	ThroughputWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), activeActors(0), totalLatencyIntegral(0), totalTransactionsIntegral(0) {
		auto multi = makeReference<MeasureMulti>();
		measurer = multi;

		targetLatency = getOption(options, "targetLatency"_sr, 0.05);

		int keyCount = getOption(options, "nodeCount"_sr, (uint64_t)100000);
		int keyBytes = std::max(getOption(options, "keyBytes"_sr, 16), 16);
		int maxValueBytes = getOption(options, "valueBytes"_sr, 100);
		int minValueBytes = getOption(options, "minValueBytes"_sr, maxValueBytes);
		double sweepDuration = getOption(options, "sweepDuration"_sr, 0);
		double sweepDelay = getOption(options, "sweepDelay"_sr, 0);

		auto AType = Reference<ITransactor>(new RWTransactor(getOption(options, "readsPerTransactionA"_sr, 10),
		                                                     getOption(options, "writesPerTransactionA"_sr, 0),
		                                                     keyCount,
		                                                     keyBytes,
		                                                     minValueBytes,
		                                                     maxValueBytes));
		auto BType = Reference<ITransactor>(new RWTransactor(getOption(options, "readsPerTransactionB"_sr, 5),
		                                                     getOption(options, "writesPerTransactionB"_sr, 5),
		                                                     keyCount,
		                                                     keyBytes,
		                                                     minValueBytes,
		                                                     maxValueBytes));

		if (sweepDuration > 0) {
			op = Reference<ITransactor>(new SweepTransactor(sweepDuration, sweepDelay, AType, BType));
		} else {
			op = Reference<ITransactor>(new ABTransactor(getOption(options, "alpha"_sr, 0.1), AType, BType));
		}

		double measureDelay = getOption(options, "measureDelay"_sr, 50.0);
		double measureDuration = getOption(options, "measureDuration"_sr, 10.0);
		multi->ms.push_back(Reference<IMeasurer>(new MeasureSinglePeriod(measureDelay, measureDuration)));

		double measurePeriod = getOption(options, "measurePeriod"_sr, 0.0);
		std::vector<std::string> periodicMetrics =
		    getOption(options, "measurePeriodicMetrics"_sr, std::vector<std::string>());
		if (measurePeriod) {
			ASSERT(periodicMetrics.size() != 0);
			multi->ms.push_back(Reference<IMeasurer>(new MeasurePeriodically(
			    measurePeriod, std::set<std::string>(periodicMetrics.begin(), periodicMetrics.end()))));
		}

		Pgain = getOption(options, "ProportionalGain"_sr, 0.1);
		Igain = getOption(options, "IntegralGain"_sr, 0.005);

		testDuration = measureDelay + measureDuration;
		// testDuration = getOption( options, "testDuration"_sr, measureDelay + measureDuration );
	}

	Future<Void> setup(Database const& cx) override {
		return Void(); // No setup for now - use a separate workload to do setup
	}

	Future<Void> start(Database const& cx) override {
		startT = now();
		PromiseStream<Future<Void>> add;
		Future<Void> ac = actorCollection(add.getFuture(), &activeActors);
		Future<Void> r = timeout(measurer->start() && ac, testDuration, Void());
		ASSERT(!ac.isReady()); // ... because else the following line would create an unbreakable reference cycle
		add.send(throughputActor(cx, this, add));
		return r;
	}

	Future<bool> check(Database const& cx) override { return true; }

	ACTOR static Future<Void> throughputActor(Database db, ThroughputWorkload* self, PromiseStream<Future<Void>> add) {
		state double before = now();
		state ITransactor::Stats stats;
		wait(self->op->doTransaction(db, &stats));
		state double after = now();

		wait(delay(0.0));
		stats.totalLatency = after - before;
		self->measurer->addTransaction(&stats, after);

		self->totalLatencyIntegral += after - before;
		self->totalTransactionsIntegral += 1;

		double error = after - before - self->targetLatency;
		// Ideally ierror would be integral [avg. transaction latency - targetLatency] dt.
		// Actually we calculate integral[ transaction latency - targetLatency ] dtransaction and change units.
		double ierror = (self->totalLatencyIntegral - self->totalTransactionsIntegral * self->targetLatency) /
		                self->totalTransactionsIntegral * (after - self->startT);

		double desiredSuccessors = 1 - (error * self->Pgain + ierror * self->Igain) / self->targetLatency;

		// if (deterministicRandom()->random01() < .001) TraceEvent("ThroughputControl").detail("Error",
		// error).detail("IError", ierror).detail("DesiredSuccessors", desiredSuccessors).detail("ActiveActors",
		// self->activeActors);

		desiredSuccessors = std::min(desiredSuccessors, 2.0);

		// SOMEDAY: How can we prevent the number of actors on different clients from diverging?

		int successors = deterministicRandom()->random01() + desiredSuccessors;
		if (successors < 1 && self->activeActors <= 1)
			successors = 1;
		if (successors > 1 && self->activeActors >= 200000)
			successors = 1;
		for (int s = 0; s < successors; s++)
			add.send(throughputActor(db, self, add));
		return Void();
	}

	void getMetrics(std::vector<PerfMetric>& m) override { measurer->getMetrics(m); }
};
WorkloadFactory<ThroughputWorkload> ThroughputWorkloadFactory;
