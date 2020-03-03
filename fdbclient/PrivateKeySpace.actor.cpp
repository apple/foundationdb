#include "fdbclient/PrivateKeySpace.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
ACTOR Future<Optional<Value>> getActor(PrivateKeySpace* pks, Reference<ReadYourWritesTransaction> ryw, KeyRef key) {
	// use getRange to workaround this
	Standalone<RangeResultRef> result = wait(pks->getRange(
	    ryw, KeySelector(firstGreaterOrEqual(key)), KeySelector(firstGreaterOrEqual(keyAfter(key))), GetRangeLimits()));
	ASSERT(result.size() <= 1);
	if (result.size()) {
		return Optional<Value>(result[0].value);
	} else {
		return Optional<Value>();
	}
}

// This function will normalize the given KeySelector to a standard KeySelector:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is not in this private key range, it will move as far as possible to adjust the offset to 1
// It does have overhead here since we query all keys twice in the worst case.
// However, moving the KeySelector while handling other parameters like limits makes the code much more complex and hard
// to maintain Seperate each part to make the code easy to understand and more compact
ACTOR Future<Void> normalizeKeySelectorActor(const PrivateKeyRangeBaseImpl* pkrImpl,
                                             Reference<ReadYourWritesTransaction> ryw, KeySelector* ks) {
	ASSERT(!ks->orEqual); // should be removed before calling
	ASSERT(ks->offset != 1); // The function is never called when KeySelector is already normalized

	state KeyRangeRef range = pkrImpl->getKeyRange();
	state Key startKey(range.begin);
	state Key endKey(range.end);

	if (ks->offset < 1) {
		// less than the given key
		if (range.contains(ks->getKey())) endKey = keyAfter(ks->getKey());
	} else {
		// greater than the given key
		if (range.contains(ks->getKey())) startKey = ks->getKey();
	}

	TraceEvent("NormalizeKeySelector")
	    .detail("OriginalKey", ks->getKey())
	    .detail("OriginalOffset", ks->offset)
	    .detail("PrivateKeyRangeStart", range.begin)
	    .detail("PrivateKeyRangeEnd", range.end);

	Standalone<RangeResultRef> result = wait(pkrImpl->getRange(ryw, KeyRangeRef(startKey, endKey)));
	// TODO : KeySelector::setKey has byte limit according to the knobs, customize it if needed
	if (ks->offset < 1) {
		if (result.size() >= 1 - ks->offset) {
			ks->setKey(KeyRef(ks->arena(), result[result.size() - (1 - ks->offset)].key));
			ks->offset = 1;
		} else {
			ks->setKey(KeyRef(ks->arena(), result[0].key));
			ks->offset += result.size();
		}
	} else {
		if (result.size() >= ks->offset) {
			ks->setKey(KeyRef(ks->arena(), result[ks->offset - 1].key));
			ks->offset = 1;
		} else {
			ks->setKey(KeyRef(ks->arena(), keyAfter(result[result.size() - 1].key)));
			ks->offset -= result.size();
		}
	}
	TraceEvent("NormalizeKeySelector")
	    .detail("NormalizedKey", ks->getKey())
	    .detail("NormalizedOffset", ks->offset)
	    .detail("PrivateKeyRangeStart", range.begin)
	    .detail("PrivateKeyRangeEnd", range.end);
	return Void();
}

ACTOR Future<Standalone<RangeResultRef>> getRangeAggregationActor(PrivateKeySpace* pks,
                                                                  Reference<ReadYourWritesTransaction> ryw,
                                                                  KeySelector begin, KeySelector end,
                                                                  GetRangeLimits limits, bool reverse) {
	// This function handles ranges which cover more than one keyrange and aggregates all results
	// KeySelector, GetRangeLimits and reverse are all handled here

	// make sure orEqual == false
	if (begin.orEqual) begin.removeOrEqual(begin.arena());
	if (end.orEqual) end.removeOrEqual(end.arena());

	// make sure offset == 1
	state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Iterator iter =
	    pks->getKeyRangeMap().rangeContaining(begin.getKey());
	while (begin.offset != 1 && iter != pks->getKeyRangeMap().ranges().begin()) {
		if (iter->value() != nullptr) wait(normalizeKeySelectorActor(iter->value(), ryw, &begin));
		begin.offset < 1 ? --iter : ++iter;
	}
	if (begin.offset != 1) {
		// The Key Selector points to key outside the whole private key space
		TraceEvent(SevError, "IllegalBeginKeySelector")
		    .detail("TerminateKey", begin.getKey())
		    .detail("TerminateOffset", begin.offset);
	}
	iter = pks->getKeyRangeMap().rangeContaining(end.getKey());
	while (end.offset != 1 && iter != pks->getKeyRangeMap().ranges().end()) {
		if (iter->value() != nullptr) wait(normalizeKeySelectorActor(iter->value(), ryw, &end));
		end.offset < 1 ? --iter : ++iter;
	}
	if (end.offset != 1) {
		// The Key Selector points to key outside the whole private key space
		TraceEvent(SevError, "IllegalEndKeySelector")
		    .detail("TerminateKey", end.getKey())
		    .detail("TerminateOffset", end.offset);
	}
	// return if range inverted
	if (begin.offset >= end.offset && begin.getKey() >= end.getKey()) {
		TEST(true);
		return Standalone<RangeResultRef>();
	}
	state Standalone<RangeResultRef> result;
	state RangeMap<Key, PrivateKeyRangeBaseImpl*, KeyRangeRef>::Ranges ranges =
	    pks->getKeyRangeMap().intersectingRanges(KeyRangeRef(begin.getKey(), end.getKey()));
	// TODO : workaround to write this two together to make the code compact
	// The issue here is boost::iterator_range<> doest not provide rbegin(), rend()
	iter = reverse ? ranges.end() : ranges.begin();
	if (reverse) {
		while (iter != ranges.begin()) {
			--iter;
			if (iter->value() == nullptr) continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
			// limits handler
			for (int i = pairs.size() - 1; i >= 0; --i) {
				result.push_back_deep(result.arena(), pairs[i]);
				// TODO : the behavior here is even the last kv makes bytes larger than specified,
				// it is still returned and set limits.bytes to zero
				limits.decrement(pairs[i]);
				if (limits.isReached()) return result;
			}
		}
	} else {
		for (iter = ranges.begin(); iter != ranges.end(); ++iter) {
			if (iter->value() == nullptr) continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
			// limits handler
			for (const KeyValueRef& kv : pairs) {
				result.push_back_deep(result.arena(), kv);
				// TODO : behavior here is even the last kv makes bytes larger than specified,
				// it is still returned and set limits.bytes to zero
				limits.decrement(kv);
				if (limits.isReached()) return result;
			}
		}
	}
	return result;
}

} // namespace
Future<Standalone<RangeResultRef>> PrivateKeySpace::getRange(Reference<ReadYourWritesTransaction> ryw,
                                                             KeySelector begin, KeySelector end, GetRangeLimits limits,
                                                             bool snapshot, bool reverse) {
	// validate limits here
	if (!limits.isValid()) return range_limits_invalid();
	if (limits.isReached()) {
		TEST(true); // read limit 0
		return Standalone<RangeResultRef>();
	}
	// ignore snapshot, which is not used
	return getRangeAggregationActor(this, ryw, begin, end, limits, reverse);
}

Future<Optional<Value>> PrivateKeySpace::get(Reference<ReadYourWritesTransaction> ryw, const Key& key, bool snapshot) {
	// ignore snapshot, which is not used
	return getActor(this, ryw, key);
}
