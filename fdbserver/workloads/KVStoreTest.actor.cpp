/*
 * KVStoreTest.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#include <ctime>
#include <cinttypes>
#include "fdbclient/FDBTypes.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbserver/IKeyValueStore.h"
#include "flow/ActorCollection.h"
#include "flow/Arena.h"
#include "flow/FastRef.h"
#include "flow/IRandom.h"
#include "flow/actorcompiler.h" // This must be the last #include.

extern IKeyValueStore* makeDummyKeyValueStore();

template <class T>
class TestHistogram {
public:
	TestHistogram(int minSamples = 100) : minSamples(minSamples) { reset(); }

	void reset() {
		N = 0;
		samplingRate = 1.0;
		sum = T();
		sumSQ = T();
	};

	void addSample(const T& x) {
		if (!N) {
			minSample = maxSample = x;
		} else {
			if (x < minSample)
				minSample = x;
			if (maxSample < x)
				maxSample = x;
		}
		sum += x;
		sumSQ += x * x;
		N++;
		if (deterministicRandom()->random01() < samplingRate) {
			samples.push_back(x);
			if (samples.size() == minSamples * 2) {
				deterministicRandom()->randomShuffle(samples);
				samples.resize(minSamples);
				samplingRate /= 2;
			}
		}
	}
	//	void addHistogram(const Histrogram& h2);

	T mean() const { return sum * (1.0 / N); } // exact
	const T& min() const { return minSample; }
	const T& max() const { return maxSample; }
	T stdDev() const {
		if (!N)
			return T();
		return sqrt((sumSQ * N - sum * sum) * (1.0 / (N * (N - 1))));
	}
	T percentileEstimate(double p) {
		ASSERT(p <= 1 && p >= 0);
		int size = samples.size();
		if (!size)
			return T();
		if (size == 1)
			return samples[0];
		std::sort(samples.begin(), samples.end());
		double fi = p * double(size - 1);
		int li = p * double(size - 1);
		if (li == size - 1)
			return samples.back();
		double alpha = fi - li;
		return samples[li] * (1 - alpha) + samples[li + 1] * alpha;
	}
	T medianEstimate() { return percentileEstimate(0.5); }
	uint64_t samplesCount() const { return N; }

private:
	int minSamples;
	double samplingRate;
	std::vector<T> samples;
	T minSample;
	T maxSample;
	T sum;
	T sumSQ;
	uint64_t N;
};
struct ClearRangeResultWriter : public IReadRangeResultWriter {
	// Result writer used for testing range reads with clear ranges
	int rowLimit, byteLimit;
	Standalone<RangeResultRef> result;
	Key clearStart, clearEnd;
	bool forward = true;

	ClearRangeResultWriter(int rowLimit, int byteLimit) : rowLimit(rowLimit), byteLimit(byteLimit) {
		if (this->rowLimit < 0) {
			forward = false;
			this->rowLimit = -this->rowLimit;
		}
	}

	void setClearRange(Key clearStart, Key clearEnd) {
		this->clearStart = clearStart;
		this->clearEnd = clearEnd;
	}

	void clearResults() { result = Standalone<RangeResultRef>(); }

	std::variant<bool, KeyRef> operator()(Optional<KeyValueRef> kv) override {
		/*
		    - What this writer does is pass through any values which are not part of a clear range
		    - If we encounter a key that falls within a clear range then we pass either the begining or the end
		      of the range (depending on the sign of the limit)
		*/
		if (rowLimit <= 0 || byteLimit <= 0)
			return false;
		if (!kv.present())
			return false;
		if (clearStart.toString() != "" && clearEnd.toString() != "" && kv.get().key >= clearStart &&
		    kv.get().key < clearEnd) {
			if (forward)
				return clearEnd;
			else
				return clearStart;
		}
		rowLimit--;
		result.push_back_deep(result.arena(), kv.get());
		byteLimit -= (sizeof(KeyValueRef) + kv.get().expectedSize());
		return true;
	}

	Arena& getArena() override { return result.arena(); }
};

struct KVTest {
	IKeyValueStore* store;
	Version startVersion;
	Version lastSet;
	Version lastCommit;
	Version lastDurable;
	Map<Key, IndexedSet<Version, NoMetric>> allSets;
	int nodeCount, keyBytes;
	bool dispose;

	explicit KVTest(int nodeCount, bool dispose, int keyBytes)
	  : store(nullptr), dispose(dispose), startVersion(Version(time(nullptr)) << 30), lastSet(startVersion),
	    lastCommit(startVersion), lastDurable(startVersion), nodeCount(nodeCount), keyBytes(keyBytes) {}
	~KVTest() { close(); }
	void close() {
		if (store) {
			TraceEvent("KVTestDestroy");
			if (dispose)
				store->dispose();
			else
				store->close();
			store = 0;
		}
	}

	Version get(KeyRef key, Version version) {
		auto s = allSets.find(key);
		if (s == allSets.end())
			return startVersion;
		auto& sets = s->value;
		auto it = sets.lastLessOrEqual(version);
		return it != sets.end() ? *it : startVersion;
	}
	void set(KeyValueRef kv) {
		store->set(kv);
		auto s = allSets.find(kv.key);
		if (s == allSets.end()) {
			allSets.insert(MapPair<Key, IndexedSet<Version, NoMetric>>(Key(kv.key), IndexedSet<Version, NoMetric>()));
			s = allSets.find(kv.key);
		}
		s->value.insert(lastSet, NoMetric());
	}

	Key randomKey() { return makeKey(deterministicRandom()->randomInt(0, nodeCount)); }
	Key makeKey(Version value) {
		Key k;
		((KeyRef&)k) = KeyRef(new (k.arena()) uint8_t[keyBytes], keyBytes);
		memcpy((uint8_t*)k.begin(), doubleToTestKey(value).begin(), 16);
		memset((uint8_t*)k.begin() + 16, '.', keyBytes - 16);
		return k;
	}
};

ACTOR Future<Void> testKVRead(KVTest* test, Key key, TestHistogram<float>* latency, PerfIntCounter* count) {
	// state Version s1 = test->lastCommit;
	state Version s2 = test->lastDurable;

	state double begin = timer();
	Optional<Value> val = wait(test->store->readValue(key));
	latency->addSample(timer() - begin);
	++*count;
	Version v = val.present() ? BinaryReader::fromStringRef<Version>(val.get(), Unversioned()) : test->startVersion;
	if (v < test->startVersion)
		v = test->startVersion; // ignore older data from the database

	// ASSERT( s1 <= v || test->get(key, s1)==v );  // Plan A
	ASSERT(s2 <= v || test->get(key, s2) == v); // Causal consistency
	ASSERT(v <= test->lastCommit); // read committed
	// ASSERT( v <= test->lastSet );  // read uncommitted
	return Void();
}

ACTOR Future<Void> testKVReadSaturation(KVTest* test, TestHistogram<float>* latency, PerfIntCounter* count) {
	while (true) {
		state double begin = timer();
		Optional<Value> val = wait(test->store->readValue(test->randomKey()));
		latency->addSample(timer() - begin);
		++*count;
		wait(delay(0));
	}
}

ACTOR Future<Void> testKVCommit(KVTest* test, TestHistogram<float>* latency, PerfIntCounter* count) {
	state Version v = test->lastSet;
	test->lastCommit = v;
	state double begin = timer();
	wait(test->store->commit());
	++*count;
	latency->addSample(timer() - begin);
	test->lastDurable = std::max(test->lastDurable, v);
	return Void();
}

Future<Void> testKVStore(struct KVStoreTestWorkload* const&);

struct KVStoreTestWorkload : TestWorkload {
	bool enabled, saturation;
	double testDuration, operationsPerSecond;
	double commitFraction, setFraction;
	int nodeCount, keyBytes, valueBytes;
	bool doSetup, doClear, doCount, doResultWriterRangeRead;
	std::string filename;
	PerfIntCounter reads, sets, commits;
	TestHistogram<float> readLatency, commitLatency;
	double setupTook;
	std::string storeType;

	KVStoreTestWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), reads("Reads"), sets("Sets"), commits("Commits"), setupTook(0) {
		enabled = !clientId; // only do this on the "first" client
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		operationsPerSecond = getOption(options, LiteralStringRef("operationsPerSecond"), 100e3);
		commitFraction = getOption(options, LiteralStringRef("commitFraction"), .001);
		setFraction = getOption(options, LiteralStringRef("setFraction"), .1);
		nodeCount = getOption(options, LiteralStringRef("nodeCount"), 100000);
		keyBytes = getOption(options, LiteralStringRef("keyBytes"), 8);
		valueBytes = getOption(options, LiteralStringRef("valueBytes"), 8);
		doSetup = getOption(options, LiteralStringRef("setup"), false);
		doClear = getOption(options, LiteralStringRef("clear"), false);
		doCount = getOption(options, LiteralStringRef("count"), false);
		doResultWriterRangeRead = getOption(options, LiteralStringRef("resultWriterRangeRead"), false);
		filename = getOption(options, LiteralStringRef("filename"), Value()).toString();
		saturation = getOption(options, LiteralStringRef("saturation"), false);
		storeType = getOption(options, LiteralStringRef("storeType"), LiteralStringRef("ssd")).toString();
	}
	std::string description() const override { return "KVStoreTest"; }
	Future<Void> setup(Database const& cx) override { return Void(); }
	Future<Void> start(Database const& cx) override {
		if (enabled)
			return testKVStore(this);
		return Void();
	}
	Future<bool> check(Database const& cx) override { return true; }
	void metricsFromHistogram(vector<PerfMetric>& m, std::string name, TestHistogram<float>& h) const {
		m.push_back(PerfMetric("Min " + name, 1000.0 * h.min(), true));
		m.push_back(PerfMetric("Average " + name, 1000.0 * h.mean(), true));
		m.push_back(PerfMetric("Median " + name, 1000.0 * h.medianEstimate(), true));
		m.push_back(PerfMetric("95%% " + name, 1000.0 * h.percentileEstimate(0.95), true));
		m.push_back(PerfMetric("Max " + name, 1000.0 * h.max(), true));
	}
	void getMetrics(vector<PerfMetric>& m) override {
		if (setupTook)
			m.push_back(PerfMetric("SetupTook", setupTook, false));

		m.push_back(reads.getMetric());
		m.push_back(sets.getMetric());
		m.push_back(commits.getMetric());
		metricsFromHistogram(m, "Read Latency (ms)", readLatency);
		metricsFromHistogram(m, "Commit Latency (ms)", commitLatency);
	}
};

WorkloadFactory<KVStoreTestWorkload> KVStoreTestWorkloadFactory("KVStoreTest");

ACTOR Future<Void> testRangeReadClearRanges(KVStoreTestWorkload* workload, KVTest* ptest) {
	/*
	    - This test attempts to simulate a range read with various clear ranges
	    - First we ask our old range read function to read a range of keys, lets say [a,m]
	    - Then we randomly pick a subset of these keys, lets say [d,i) and designate this as our "clear range"
	    - We then feed the range [a, m] to the new range read function and get the result
	    - We then assert if the result contains all the keys in the range [a, m] except for those in [d, i)
	*/
	state KVTest& test = *ptest;
	state Key key;
	state int byteLimt = 1 << 30;
	state int rowLimit = 1000;
	state int count = 0;

	while (true) {
		// get the inital range of keys from the old read range function
		state KeyRangeRef range = KeyRangeRef(key, LiteralStringRef("\xff\xff\xff\xff"));
		state Standalone<RangeResultRef> kv = wait(test.store->readRange(range, rowLimit, byteLimt));
		state Reference<ClearRangeResultWriter> resultWriter =
		    makeReference<ClearRangeResultWriter>(rowLimit, byteLimt);

		if (kv.size() == 0)
			break;
		// pick a random range to designate as our clear range
		int rand1 = deterministicRandom()->randomInt(0, kv.size()),
		    rand2 = deterministicRandom()->randomInt(0, kv.size());
		state int clearStartIdx = std::min(rand1, rand2);
		state int clearEndIdx = std::max(rand1, rand2);
		// get the results from our new range read function with the range above
		resultWriter->setClearRange(kv[clearStartIdx].key, kv[clearEndIdx].key);
		wait(test.store->readRange(KeyRangeRef(kv[0].key, keyAfter(kv[kv.size() - 1].key)), resultWriter, rowLimit));
		ASSERT(resultWriter->result.size() == kv.size() - (clearEndIdx - clearStartIdx));
		int curIdx = 0;
		// assert our range read function contains only the keys which are not part of the selected clear range
		for (int i = 0; i < kv.size(); i++) {
			if (i < clearStartIdx || i >= clearEndIdx) {
				ASSERT(kv[i].key == resultWriter->result[curIdx].key);
				ASSERT(kv[i].value == resultWriter->result[curIdx].value);
				curIdx++;
			}
		}
		ASSERT(curIdx == resultWriter->result.size());
		count += resultWriter->result.size();
		if (kv.size() < rowLimit)
			break;
		key = keyAfter(kv[kv.size() - 1].key);
	}
	TraceEvent("KVStoreRangeReadClearCount").detail("Count", count);

	rowLimit = -1000;
	key = LiteralStringRef("\xff\xff\xff\xff");
	count = 0;

	// same logic as above except we reverse the direction of the range read results (desc order)
	while (true) {
		Key start;
		state KeyRangeRef range2 = KeyRangeRef(start, key);
		state Standalone<RangeResultRef> kv2 = wait(test.store->readRange(range2, rowLimit, byteLimt));
		state Reference<ClearRangeResultWriter> resultWriter2 =
		    makeReference<ClearRangeResultWriter>(rowLimit, byteLimt);

		if (kv2.size() == 0)
			break;
		int rand1 = deterministicRandom()->randomInt(0, kv2.size()),
		    rand2 = deterministicRandom()->randomInt(0, kv2.size());
		state int clearStartIdx2 = std::min(rand1, rand2);
		state int clearEndIdx2 = std::max(rand1, rand2);
		resultWriter2->setClearRange(kv2[clearEndIdx2].key, kv2[clearStartIdx2].key);
		wait(
		    test.store->readRange(KeyRangeRef(kv2[kv2.size() - 1].key, keyAfter(kv2[0].key)), resultWriter2, rowLimit));
		int curIdx = 0;
		ASSERT(resultWriter2->result.size() == kv2.size() - (clearEndIdx2 - clearStartIdx2));
		for (int i = 0; i < kv2.size(); i++) {
			if (i <= clearStartIdx2 || i > clearEndIdx2) {
				ASSERT(kv2[i].key == resultWriter2->result[curIdx].key);
				ASSERT(kv2[i].value == resultWriter2->result[curIdx].value);
				curIdx++;
			}
		}
		ASSERT(curIdx == resultWriter2->result.size());
		count += resultWriter2->result.size();
		if (kv2.size() < -rowLimit)
			break;
		key = kv2[kv2.size() - 1].key;
	}
	TraceEvent("KVStoreRangeReadClearCountRev").detail("Count", count);

	return Void();
}

ACTOR Future<Void> testReadRangeEdgeCases(KVStoreTestWorkload* workload, KVTest* ptest) {
	/*
	  - Tests various edge cases with the new result writer range read interface
	*/
	state KVTest& test = *ptest;
	state Key key;
	state int limit = 1000;
	state int byteLimt = 1 << 30;
	state int curIdx = 0;
	state KeyRangeRef range = KeyRangeRef(key, LiteralStringRef("\xff\xff\xff\xff"));
	state Standalone<RangeResultRef> kv = wait(test.store->readRange(range, limit, byteLimt));
	state Reference<ClearRangeResultWriter> resultWriter = makeReference<ClearRangeResultWriter>(limit, byteLimt);

	// Tests with a clear range at the begining of the read range
	resultWriter->setClearRange(kv[0].key, kv[kv.size() / 2 - 1].key);
	wait(test.store->readRange(KeyRangeRef(kv[0].key, keyAfter(kv[kv.size() - 1].key)), resultWriter, limit));
	for (int i = 0; i < kv.size(); i++) {
		if (i >= kv.size() / 2 - 1) {
			ASSERT(kv[i].key == resultWriter->result[curIdx].key);
			ASSERT(kv[i].value == resultWriter->result[curIdx].value);
			curIdx++;
		}
	}
	ASSERT(resultWriter->result.size() > 0);
	ASSERT(curIdx == resultWriter->result.size());

	resultWriter->clearResults();

	// Tests with a clear range at the end of the read range
	resultWriter->setClearRange(kv[kv.size() / 2 - 1].key, keyAfter(kv[kv.size() - 1].key));
	wait(test.store->readRange(KeyRangeRef(kv[0].key, keyAfter(kv[kv.size() - 1].key)), resultWriter, limit));
	curIdx = 0;
	for (int i = 0; i < kv.size(); i++) {
		if (i < kv.size() / 2 - 1) {
			ASSERT(kv[i].key == resultWriter->result[curIdx].key);
			ASSERT(kv[i].value == resultWriter->result[curIdx].value);
			curIdx++;
		}
	}
	ASSERT(resultWriter->result.size() > 0);
	ASSERT(curIdx == resultWriter->result.size());

	return Void();
}

ACTOR Future<Void> testRangeReadResultWriterPassThrough(KVStoreTestWorkload* workload, KVTest* ptest) {
	/*
	    - This test attempts to simulate a regular read range
	    - We enter a range of keys ["", "\XFF"] into both the old and new read range functions
	    - Once we get the results from the two we assert that they are identical
	*/
	state KVTest& test = *ptest;
	state Key key = LiteralStringRef("\xff\xff\xff\xff");
	state int byteLimt = 1 << 30;
	state int rowLimit = -1000;
	state int count = 0;

	// get the range in reverse alph order (desc)
	while (true) {
		Key start;
		state KeyRangeRef range = KeyRangeRef(start, key);
		// result of range read using exisitng API
		state Standalone<RangeResultRef> kv = wait(test.store->readRange(range, rowLimit, byteLimt));
		state Reference<ClearRangeResultWriter> resultWriter =
		    makeReference<ClearRangeResultWriter>(rowLimit, byteLimt);
		// result of range read using the resultWriter interface
		wait(test.store->readRange(range, resultWriter, rowLimit));

		// make sure the result from both range read functions are the same
		ASSERT(resultWriter->result.size() == kv.size());
		for (int i = 0; i < kv.size(); i++) {
			ASSERT(kv[i].key == resultWriter->result[i].key);
			ASSERT(kv[i].value == resultWriter->result[i].value);
		}
		count += resultWriter->result.size();
		if (kv.size() < -rowLimit)
			break;
		key = kv[kv.size() - 1].key;
	}
	TraceEvent("KVStoreRangeReadCount").detail("Count", count);
	ASSERT(count == 100000);

	// same logic as above except we reverse the direction of the range read results (asc order)
	count = 0;
	rowLimit = 1000;
	state Key start;
	while (true) {
		state KeyRangeRef range2 = KeyRangeRef(start, LiteralStringRef("\xff\xff\xff\xff"));
		state Standalone<RangeResultRef> kv2 = wait(test.store->readRange(range2, rowLimit, byteLimt));
		state Reference<ClearRangeResultWriter> resultWriter2 =
		    makeReference<ClearRangeResultWriter>(rowLimit, byteLimt);
		wait(test.store->readRange(range2, resultWriter2, rowLimit));
		ASSERT(resultWriter2->result.size() == kv2.size());
		for (int i = 0; i < kv2.size(); i++) {
			ASSERT(kv2[i].key == resultWriter2->result[i].key);
			ASSERT(kv2[i].value == resultWriter2->result[i].value);
		}
		count += resultWriter2->result.size();
		if (kv2.size() < rowLimit)
			break;
		start = keyAfter(kv2[kv2.size() - 1].key);
	}
	TraceEvent("KVStoreRangeReadCountRev").detail("Count", count);
	ASSERT(count == 100000);

	return Void();
}

ACTOR Future<Void> testKVStoreMain(KVStoreTestWorkload* workload, KVTest* ptest) {
	state KVTest& test = *ptest;
	state ActorCollectionNoErrors ac;
	state std::deque<Future<Void>> reads;
	state BinaryWriter wr(Unversioned());
	state int64_t commitsStarted = 0;
	// test.store = makeDummyKeyValueStore();
	state int extraBytes = workload->valueBytes - sizeof(test.lastSet);
	state int i;
	ASSERT(extraBytes >= 0);
	state char* extraValue = new char[extraBytes];
	memset(extraValue, '.', extraBytes);

	// Basic read range
	if (workload->doCount) {
		state int64_t count = 0;
		state Key k;
		state double cst = timer();
		while (true) {
			Standalone<RangeResultRef> kv =
			    wait(test.store->readRange(KeyRangeRef(k, LiteralStringRef("\xff\xff\xff\xff")), 1000));
			count += kv.size();
			if (kv.size() < 1000)
				break;
			k = keyAfter(kv[kv.size() - 1].key);
		}
		double elapsed = timer() - cst;
		TraceEvent("KVStoreCount").detail("Count", count).detail("Took", elapsed);
		printf("Counted: %" PRId64 " in %0.1fs\n", count, elapsed);
	}

	// Seed the storage engine
	if (workload->doSetup) {
		wr << Version(0);
		wr.serializeBytes(extraValue, extraBytes);

		printf("Building %d nodes: ", workload->nodeCount);
		state double setupBegin = timer();
		state Future<Void> lastCommit = Void();
		for (i = 0; i < workload->nodeCount; i++) {
			test.store->set(KeyValueRef(test.makeKey(i), wr.toValue()));
			if (!((i + 1) % 10000) || i + 1 == workload->nodeCount) {
				wait(lastCommit);
				lastCommit = test.store->commit();
				printf("ETA: %f seconds\n", (timer() - setupBegin) / i * (workload->nodeCount - i));
			}
		}
		wait(lastCommit);
		workload->setupTook = timer() - setupBegin;
		TraceEvent("KVStoreSetup").detail("Count", workload->nodeCount).detail("Took", workload->setupTook);
	}

	if (workload->doResultWriterRangeRead && test.store->usesReadRangeResultWriter()) {
		// Only run these tests if the ResultWriter Interface has been implemented for the storage engines
		wait(testRangeReadResultWriterPassThrough(workload, ptest));
		wait(testRangeReadClearRanges(workload, ptest));
		wait(testReadRangeEdgeCases(workload, ptest));
	}

	state double t = now();
	state double stopAt = t + workload->testDuration;
	// Read Saturation
	if (workload->saturation) {
		if (workload->commitFraction) {
			while (now() < stopAt) {
				for (int s = 0; s < 1 / workload->commitFraction; s++) {
					++test.lastSet;
					BinaryWriter wr(Unversioned());
					wr << test.lastSet;
					wr.serializeBytes(extraValue, extraBytes);
					test.set(KeyValueRef(test.randomKey(), wr.toValue()));
					++workload->sets;
				}
				++commitsStarted;
				wait(testKVCommit(&test, &workload->commitLatency, &workload->commits));
			}
		} else {
			vector<Future<Void>> actors;
			actors.reserve(100);
			for (int a = 0; a < 100; a++)
				actors.push_back(testKVReadSaturation(&test, &workload->readLatency, &workload->reads));
			wait(timeout(waitForAll(actors), workload->testDuration, Void()));
		}
	} else {
		while (t < stopAt) {
			double end = now();
			loop {
				t += 1.0 / workload->operationsPerSecond;
				double op = deterministicRandom()->random01();
				if (op < workload->commitFraction) {
					// Commit
					if (workload->commits.getValue() == commitsStarted) {
						++commitsStarted;
						ac.add(testKVCommit(&test, &workload->commitLatency, &workload->commits));
					}
				} else if (op < workload->commitFraction + workload->setFraction) {
					// Set
					++test.lastSet;
					BinaryWriter wr(Unversioned());
					wr << test.lastSet;
					wr.serializeBytes(extraValue, extraBytes);
					test.set(KeyValueRef(test.randomKey(), wr.toValue()));
					++workload->sets;
				} else {
					// Read
					ac.add(testKVRead(&test, test.randomKey(), &workload->readLatency, &workload->reads));
				}
				if (t >= end)
					break;
			}
			wait(delayUntil(t));
		}
	}

	// Remove all seeded data
	if (workload->doClear) {
		state int chunk = 1000000;
		t = timer();
		for (i = 0; i < workload->nodeCount; i += chunk) {
			test.store->clear(KeyRangeRef(test.makeKey(i), test.makeKey(i + chunk)));
			wait(test.store->commit());
		}
		TraceEvent("KVStoreClear").detail("Took", timer() - t);
	}

	return Void();
}

ACTOR Future<Void> testKVStore(KVStoreTestWorkload* workload) {
	state KVTest test(workload->nodeCount, !workload->filename.size(), workload->keyBytes);
	state Error err;

	// wait( delay(1) );
	TraceEvent("GO");

	UID id = deterministicRandom()->randomUniqueID();
	std::string fn = workload->filename.size() ? workload->filename : id.toString();
	if (workload->storeType == "ssd")
		test.store = keyValueStoreSQLite(fn, id, KeyValueStoreType::SSD_BTREE_V2);
	else if (workload->storeType == "ssd-1")
		test.store = keyValueStoreSQLite(fn, id, KeyValueStoreType::SSD_BTREE_V1);
	else if (workload->storeType == "ssd-2")
		test.store = keyValueStoreSQLite(fn, id, KeyValueStoreType::SSD_REDWOOD_V1);
	else if (workload->storeType == "ssd-redwood-experimental")
		test.store = keyValueStoreRedwoodV1(fn, id);
	else if (workload->storeType == "ssd-rocksdb-experimental")
		test.store = keyValueStoreRocksDB(fn, id, KeyValueStoreType::SSD_ROCKSDB_V1);
	else if (workload->storeType == "memory")
		test.store = keyValueStoreMemory(fn, id, 500e6);
	else if (workload->storeType == "memory-radixtree-beta")
		test.store = keyValueStoreMemory(fn, id, 500e6, "fdr", KeyValueStoreType::MEMORY_RADIXTREE);
	else
		ASSERT(false);

	wait(test.store->init());

	state Future<Void> main = testKVStoreMain(workload, &test);
	try {
		choose {
			when(wait(main)) {}
			when(wait(test.store->getError())) { ASSERT(false); }
		}
	} catch (Error& e) {
		err = e;
	}
	main.cancel();

	Future<Void> c = test.store->onClosed();
	test.close();
	wait(c);
	if (err.code() != invalid_error_code)
		throw err;
	return Void();
}
