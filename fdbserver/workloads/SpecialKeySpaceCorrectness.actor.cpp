#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

class SPSCTestImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit SPSCTestImpl(KeyRef start, KeyRef end) : SpecialKeyRangeBaseImpl(start, end) {}
	virtual Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                                    KeyRangeRef kr) const {
		ASSERT(range.contains(kr));
		auto resultFuture = ryw->getRange(kr, CLIENT_KNOBS->TOO_MANY);
		// all keys are written to RYW, since GRV is set, the read should happen locally
		ASSERT(resultFuture.isReady());
		return resultFuture.getValue();
	}
};

struct SpecialKeySpaceCorrectnessWorkload : TestWorkload {

	int actorCount, minKeysPerRange, maxKeysPerRange, rangeCount, keyBytes, valBytes;
	double testDuration, absoluteRandomProb, transactionsPerSecond;

	PerfIntCounter wrongResults, keysCount;

	Reference<ReadYourWritesTransaction> ryw; // used to store all populated data
	std::vector<std::shared_ptr<SPSCTestImpl>> impls;

	Standalone<VectorRef<KeyRangeRef>> keys;

	SpecialKeySpaceCorrectnessWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), wrongResults("Wrong Results"), keysCount("Number of generated keys") {
		minKeysPerRange = getOption(options, LiteralStringRef("minKeysPerRange"), 1);
		maxKeysPerRange = getOption(options, LiteralStringRef("maxKeysPerRange"), 100);
		rangeCount = getOption(options, LiteralStringRef("rangeCount"), 10);
		keyBytes = getOption(options, LiteralStringRef("keyBytes"), 16);
		valBytes = getOption(options, LiteralStringRef("valueBytes"), 16);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
		transactionsPerSecond = getOption(options, LiteralStringRef("transactionsPerSecond"), 100.0);
		actorCount = getOption(options, LiteralStringRef("actorCount"), 1);
		absoluteRandomProb = getOption(options, LiteralStringRef("absoluteRandomProb"), 0.5);
	}

	virtual std::string description() { return "SpecialKeySpaceCorrectness"; }
	virtual Future<Void> setup(Database const& cx) { return _setup(cx, this); }
	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) { return wrongResults.getValue() == 0; }
	virtual void getMetrics(std::vector<PerfMetric>& m) {}

	// disable the default timeout setting
	double getCheckTimeout() override { return std::numeric_limits<double>::max(); }

	Future<Void> _setup(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		if (self->clientId == 0) {
			self->ryw = Reference(new ReadYourWritesTransaction(cx));
			self->ryw->setVersion(100);
			self->ryw->clear(normalKeys);
			// generate key ranges
			for (int i = 0; i < self->rangeCount; ++i) {
				std::string baseKey = deterministicRandom()->randomAlphaNumeric(i + 1);
				Key startKey(baseKey + "/");
				Key endKey(baseKey + "/\xff");
				self->keys.push_back_deep(self->keys.arena(), KeyRangeRef(startKey, endKey));
				self->impls.push_back(std::make_shared<SPSCTestImpl>(startKey, endKey));
				cx->specialKeySpace->registerKeyRange(self->keys.back(), self->impls.back().get());
				// generate keys in each key range
				int keysInRange = deterministicRandom()->randomInt(self->minKeysPerRange, self->maxKeysPerRange + 1);
				self->keysCount += keysInRange;
				for (int j = 0; j < keysInRange; ++j) {
					self->ryw->set(Key(deterministicRandom()->randomAlphaNumeric(self->keyBytes)).withPrefix(startKey),
					               Value(deterministicRandom()->randomAlphaNumeric(self->valBytes)));
				}
			}
		}
		return Void();
	}
	ACTOR Future<Void> _start(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		if (self->clientId == 0) wait(timeout(self->getRangeCallActor(cx, self), self->testDuration, Void()));
		return Void();
	}

	ACTOR Future<Void> getRangeCallActor(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		state double lastTime = now();
		loop {
			wait(poisson(&lastTime, 1.0 / self->transactionsPerSecond));
			state bool reverse = deterministicRandom()->random01() < 0.5;
			state GetRangeLimits limit = self->randomLimits();
			state KeySelector begin = self->randomKeySelector();
			state KeySelector end = self->randomKeySelector();
			auto correctResultFuture = self->ryw->getRange(begin, end, limit, false, reverse);
			ASSERT(correctResultFuture.isReady());
			auto correctResult = correctResultFuture.getValue();
			auto testResultFuture = cx->specialKeySpace->getRange(self->ryw, begin, end, limit, false, reverse);
			ASSERT(testResultFuture.isReady());
			auto testResult = testResultFuture.getValue();

			// check the consistency of results
			if (!self->compareRangeResult(correctResult, testResult)) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Results from getRange are inconsistent")
				    .detail("Begin", begin.toString())
				    .detail("End", end.toString())
				    .detail("LimitRows", limit.rows)
				    .detail("LimitBytes", limit.bytes)
				    .detail("Reverse", reverse);
				++self->wrongResults;
			}
		}
	}

	bool compareRangeResult(Standalone<RangeResultRef>& res1, Standalone<RangeResultRef>& res2) {
		if ((res1.more != res2.more) || (res1.readToBegin != res2.readToBegin) ||
		    (res1.readThroughEnd != res2.readThroughEnd)) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "RangeResultRef flags are inconsistent")
			    .detail("More", res1.more)
			    .detail("ReadToBegin", res1.readToBegin)
			    .detail("ReadThroughEnd", res1.readThroughEnd)
			    .detail("More2", res2.more)
			    .detail("ReadToBegin2", res2.readToBegin)
			    .detail("ReadThroughEnd2", res2.readThroughEnd);
			return false;
		}
		if (res1.size() != res2.size()) {
			TraceEvent(SevError, "TestFailure")
			    .detail("Reason", "Results' sizes are inconsistent")
			    .detail("CorrestResultSize", res1.size())
			    .detail("TestResultSize", res2.size());
			return false;
		}
		for (int i = 0; i < res1.size(); ++i) {
			if (res1[i] != res2[i]) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "Elements are inconsistent")
				    .detail("Index", i)
				    .detail("CorrectKey", printable(res1[i].key))
				    .detail("TestKey", printable(res2[i].key))
				    .detail("CorrectValue", printable(res1[i].value))
				    .detail("TestValue", printable(res2[i].value));
				return false;
			}
		}
		return true;
	}

	KeySelector randomKeySelector() {
		Key randomKey;
		// TODO : add randomness to pickup existing keys
		if (deterministicRandom()->random01() < absoluteRandomProb) {
			Key prefix;
			if (deterministicRandom()->random01() < absoluteRandomProb)
				// prefix length is randomly generated
				prefix =
				    Key(deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, rangeCount + 1)) +
				        "/");
			else
				// pick up an existing prefix
				prefix = keys[deterministicRandom()->randomInt(0, rangeCount)].begin;
			randomKey = Key(deterministicRandom()->randomAlphaNumeric(keyBytes)).withPrefix(prefix);
		} else {
			// pick up existing keys from registered key ranges
			KeyRangeRef randomKeyRangeRef = keys[deterministicRandom()->randomInt(0, keys.size())];
			randomKey = deterministicRandom()->random01() < 0.5 ? randomKeyRangeRef.begin : randomKeyRangeRef.end;
		}
		// return Key(deterministicRandom()->randomAlphaNumeric(keyBytes)).withPrefix(prefix);
		// covers corner cases where offset points outside the key space
		int offset = deterministicRandom()->randomInt(-keysCount.getValue() - 1, keysCount.getValue() + 2);
		bool orEqual = deterministicRandom()->random01() < 0.5;
		return KeySelectorRef(randomKey, orEqual, offset);
	}

	GetRangeLimits randomLimits() {
		// TODO : fix knobs for row_unlimited
		int rowLimits = deterministicRandom()->randomInt(1, keysCount.getValue() + 1);
		// The largest key's bytes is longest prefix bytes + 1(for '/') + generated key bytes
		// 8 here refers to bytes of KeyValueRef
		int byteLimits = deterministicRandom()->randomInt(
		    1, keysCount.getValue() * (keyBytes + (rangeCount + 1) + valBytes + 8) + 1);
		// TODO : check setRequestLimits in RYW
		return GetRangeLimits(rowLimits, byteLimits);
	}
};

WorkloadFactory<SpecialKeySpaceCorrectnessWorkload> SpecialKeySpaceCorrectnessFactory("SpecialKeySpaceCorrectness");