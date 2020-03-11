#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/TesterInterface.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

const KeyRef startSuffix = LiteralStringRef("/");
const KeyRef endSuffix = LiteralStringRef("/\xff");
class SPSCTestImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit SPSCTestImpl(KeyRef start, KeyRef end) : SpecialKeyRangeBaseImpl(start, end) {}
	virtual Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                                    KeyRangeRef kr) const {
		ASSERT(range.contains(kr));
		auto result = ryw->getRange(kr, GetRangeLimits());
		// ASSERT(resultFuture.isReady());
		return result;
	}
};

struct SpecialKeySpaceCorrectnessWorkload : TestWorkload {

	int actorCount, minKeysPerRange, maxKeysPerRange, rangeCount, keyBytes, valBytes;
	double testDuration, absoluteRandomProb;
	std::vector<Future<Void>> clients;

	PerfIntCounter wrongResults, keysCount;

	Reference<ReadYourWritesTransaction> ryw; // used to store all populated data
	std::vector<SPSCTestImpl> impls;

	Standalone<VectorRef<KeyRangeRef>> keys;

	SpecialKeySpaceCorrectnessWorkload(WorkloadContext const& wcx)
	  : TestWorkload(wcx), wrongResults("Wrong Results"), keysCount("Number of generated keys") {
		minKeysPerRange = getOption(options, LiteralStringRef("minKeysPerRange"), 1);
		maxKeysPerRange = getOption(options, LiteralStringRef("maxKeysPerRange"), 100);
		rangeCount = getOption(options, LiteralStringRef("rangeCount"), 10);
		keyBytes = getOption(options, LiteralStringRef("keyBytes"), 16);
		valBytes = getOption(options, LiteralStringRef("valueBytes"), 16);
		testDuration = getOption(options, LiteralStringRef("testDuration"), 10.0);
        actorCount = getOption( options, LiteralStringRef("actorCount"), 50 );
		absoluteRandomProb = getOption(options, LiteralStringRef("absoluteRandomProb"), 0.5);
	}

	virtual std::string description() { return "SpecialKeySpaceCorrectness"; }
	virtual Future<Void> setup(Database const& cx) { return _setup(cx, this); }
	virtual Future<Void> start(Database const& cx) { return _start(cx, this); }
	virtual Future<bool> check(Database const& cx) { return wrongResults.getValue() == 0; }
	virtual void getMetrics(std::vector<PerfMetric>& m) {}

	// disable the default timeout setting
	double getCheckTimeout() override { return std::numeric_limits<double>::max(); }

	ACTOR Future<Void> _setup(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		// self->ryw = Reference(new ReadYourWritesTransaction(cx));
		// // generate key ranges
		// for (int i = 0; i < self->rangeCount; ++i) {
		// 	std::string baseKey = deterministicRandom()->randomAlphaNumeric(i + 1);
		// 	Key startKey(baseKey + "/");
		// 	Key endKey(baseKey + "/\xff");
		// 	self->keys.push_back_deep(self->keys.arena(), KeyRangeRef(startKey, endKey));
		// 	self->impls.emplace_back(startKey, endKey);
		// 	cx->specialKeySpace->registerKeyRange(self->keys.back(), &self->impls.back());
		// 	// generate keys in each key range
		// 	int keysInRange = deterministicRandom()->randomInt(self->minKeysPerRange, self->maxKeysPerRange + 1);
		// 	self->keysCount += keysInRange;
		// 	for (int j = 0; j < keysInRange; ++j) {
		// 		self->ryw->set(Key(deterministicRandom()->randomAlphaNumeric(self->keyBytes)).withPrefix(startKey),
		// 		               Value(deterministicRandom()->randomAlphaNumeric(self->valBytes)));
		// 	}
		// }
		return Void();
	}
	ACTOR Future<Void> _start(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
        self->ryw = Reference(new ReadYourWritesTransaction(cx));
		// generate key ranges
		for (int i = 0; i < self->rangeCount; ++i) {
			std::string baseKey = deterministicRandom()->randomAlphaNumeric(i + 1);
			Key startKey(baseKey + "/");
			Key endKey(baseKey + "/\xff");
			self->keys.push_back_deep(self->keys.arena(), KeyRangeRef(startKey, endKey));
			self->impls.emplace_back(startKey, endKey);
			cx->specialKeySpace->registerKeyRange(self->keys.back(), &self->impls.back());
			// generate keys in each key range
			int keysInRange = deterministicRandom()->randomInt(self->minKeysPerRange, self->maxKeysPerRange + 1);
			self->keysCount += keysInRange;
			for (int j = 0; j < keysInRange; ++j) {
				self->ryw->set(Key(deterministicRandom()->randomAlphaNumeric(self->keyBytes)).withPrefix(startKey),
				               Value(deterministicRandom()->randomAlphaNumeric(self->valBytes)));
			}
		}

        std::vector<Future<Void>> clients;
        for(int c = 0; c < self->actorCount; c++) {
			clients.push_back(self->getRangeCallActor(cx, self));
		}
		wait(timeout(waitForAll(clients), self->testDuration, Void()));
		return Void();
	}

	ACTOR Future<Void> getRangeCallActor(Database cx, SpecialKeySpaceCorrectnessWorkload* self) {
		loop {
			state bool reverse = deterministicRandom()->random01() < 0.5;
			state GetRangeLimits limit = self->randomLimits();
			state KeySelector begin = self->randomKeySelector();
			state KeySelector end = self->randomKeySelector();
            KeyRange kr = KeyRangeRef(LiteralStringRef("test"), LiteralStringRef("test2"));
			// state Standalone<RangeResultRef> correctResult = wait(self->ryw->getRange(begin, end, limit, false, reverse));
            Standalone<RangeResultRef> tempResult = wait(self->ryw->getRange(kr, 1));
			// ASSERT(correctResultFuture.isReady());
			// auto correctResult = correctResultFuture.getValue();
			// auto testResultFuture = cx->specialKeySpace->getRange(self->ryw, begin, end, limit, false, reverse);
			// ASSERT(testResultFuture.isReady());
			// auto testResult = testResultFuture.getValue();

			// // check the same
			// if (!self->compareRangeResult(correctResult, testResult)) {
			// 	// TODO : log here
			// 	// TraceEvent("WrongGetRangeResult"). detail("KeySeleco")
			// 	++self->wrongResults;
			// }
		}
	}

	bool compareRangeResult(Standalone<RangeResultRef>& res1, Standalone<RangeResultRef>& res2) {
		if (res1.size() != res2.size()) return false;
		for (int i = 0; i < res1.size(); ++i) {
			if (res1[i] != res2[i]) return false;
		}
		return true;
	}

	KeySelector randomKeySelector() {
		Key prefix;
		if (deterministicRandom()->random01() < absoluteRandomProb)
			prefix = Key(
			    deterministicRandom()->randomAlphaNumeric(deterministicRandom()->randomInt(1, rangeCount + 1)) + "/");
		else
			prefix = keys[deterministicRandom()->randomInt(1, rangeCount + 1)].begin;
		Key suffix;
		// TODO : add randomness to pickup existing keys
		// if (deterministicRandom()->random01() < absoluteRandomProb)
		suffix = Key(deterministicRandom()->randomAlphaNumeric(keyBytes));
		// return Key(deterministicRandom()->randomAlphaNumeric(keyBytes)).withPrefix(prefix);
		// TODO : test corner case here if offset points out
		int offset = deterministicRandom()->randomInt(-keysCount.getValue() - 1, keysCount.getValue() + 1);
		bool orEqual = deterministicRandom()->random01() < 0.5;
		return KeySelectorRef(suffix.withPrefix(prefix), orEqual, offset);
	}

	GetRangeLimits randomLimits() {
		if (deterministicRandom()->random01() < 0.5) return GetRangeLimits();
		int rowLimits = deterministicRandom()->randomInt(1, keysCount.getValue() + 1);
		// TODO : add random bytes limit here
        // TODO : setRequestLimits in RYW
		return GetRangeLimits(rowLimits);
	}

	// void updateGetGetRangeParas(KeySelector& begin, KeySelector& end, GetRangeLimits& limits, bool& reverse) {
	//     reverse = deterministicRandom()->random01() < 0.5;
	// }
};

WorkloadFactory<SpecialKeySpaceCorrectnessWorkload> SpecialKeySpaceCorrectnessFactory("SpecialKeySpaceCorrectness");