/*
 * SpecialKeySpace.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2020 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/SpecialKeySpace.actor.h"
#include "flow/UnitTest.h"
#include "flow/actorcompiler.h" // This must be the last #include.

std::unordered_map<SpecialKeySpace::MODULE, KeyRange> SpecialKeySpace::moduleToBoundary = {
	{ SpecialKeySpace::MODULE::TRANSACTION,
	  KeyRangeRef(LiteralStringRef("\xff\xff/transaction/"), LiteralStringRef("\xff\xff/transaction0")) },
	{ SpecialKeySpace::MODULE::WORKERINTERFACE,
	  KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"), LiteralStringRef("\xff\xff/worker_interfaces0")) },
	{ SpecialKeySpace::MODULE::STATUSJSON, singleKeyRange(LiteralStringRef("\xff\xff/status/json")) },
	{ SpecialKeySpace::MODULE::CONNECTIONSTRING, singleKeyRange(LiteralStringRef("\xff\xff/connection_string")) },
	{ SpecialKeySpace::MODULE::CLUSTERFILEPATH, singleKeyRange(LiteralStringRef("\xff\xff/cluster_file_path")) },
	{ SpecialKeySpace::MODULE::METRICS,
	  KeyRangeRef(LiteralStringRef("\xff\xff/metrics/"), LiteralStringRef("\xff\xff/metrics0")) }
};

// This function will move the given KeySelector as far as possible to the standard form:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is not in the underlying key range, it will move over the range
ACTOR Future<Void> moveKeySelectorOverRangeActor(const SpecialKeyRangeBaseImpl* skrImpl,
                                                 Reference<ReadYourWritesTransaction> ryw, KeySelector* ks) {
	ASSERT(!ks->orEqual); // should be removed before calling
	ASSERT(ks->offset != 1); // never being called if KeySelector is already normalized

	state Key startKey(skrImpl->getKeyRange().begin);
	state Key endKey(skrImpl->getKeyRange().end);

	if (ks->offset < 1) {
		// less than the given key
		if (skrImpl->getKeyRange().contains(ks->getKey())) endKey = ks->getKey();
	} else {
		// greater than the given key
		if (skrImpl->getKeyRange().contains(ks->getKey())) startKey = ks->getKey();
	}
	ASSERT(startKey < endKey); // Note : startKey never equals endKey here

	TraceEvent(SevDebug, "NormalizeKeySelector")
	    .detail("OriginalKey", ks->getKey())
	    .detail("OriginalOffset", ks->offset)
	    .detail("SpecialKeyRangeStart", skrImpl->getKeyRange().begin)
	    .detail("SpecialKeyRangeEnd", skrImpl->getKeyRange().end);

	Standalone<RangeResultRef> result = wait(skrImpl->getRange(ryw, KeyRangeRef(startKey, endKey)));
	if (result.size() == 0) {
		TraceEvent(SevDebug, "ZeroElementsIntheRange").detail("Start", startKey).detail("End", endKey);
		return Void();
	}
	// Note : KeySelector::setKey has byte limit according to the knobs, customize it if needed
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
	TraceEvent(SevDebug, "NormalizeKeySelector")
	    .detail("NormalizedKey", ks->getKey())
	    .detail("NormalizedOffset", ks->offset)
	    .detail("SpecialKeyRangeStart", skrImpl->getKeyRange().begin)
	    .detail("SpecialKeyRangeEnd", skrImpl->getKeyRange().end);
	return Void();
}

void onModuleRead(const Reference<ReadYourWritesTransaction>& ryw, SpecialKeySpace::MODULE module,
                  Optional<SpecialKeySpace::MODULE>& lastModuleRead) {
	if (ryw && !ryw->specialKeySpaceRelaxed() && lastModuleRead.present() && lastModuleRead.get() != module) {
		throw special_keys_cross_module_read();
	}
	lastModuleRead = module;
}

// This function will normalize the given KeySelector to a standard KeySelector:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is outside the whole space, it will move to the begin or the end
// It does have overhead here since we query all keys twice in the worst case.
// However, moving the KeySelector while handling other parameters like limits makes the code much more complex and hard
// to maintain; Thus, separate each part to make the code easy to understand and more compact
ACTOR Future<Void> normalizeKeySelectorActor(SpecialKeySpace* sks, Reference<ReadYourWritesTransaction> ryw,
                                             KeySelector* ks, Optional<SpecialKeySpace::MODULE>* lastModuleRead,
                                             int* actualOffset, Standalone<RangeResultRef>* result) {
	state RangeMap<Key, SpecialKeyRangeBaseImpl*, KeyRangeRef>::Iterator iter =
	    ks->offset < 1 ? sks->getImpls().rangeContainingKeyBefore(ks->getKey())
	                   : sks->getImpls().rangeContaining(ks->getKey());
	while ((ks->offset < 1 && iter != sks->getImpls().ranges().begin()) ||
	       (ks->offset > 1 && iter != sks->getImpls().ranges().end())) {
		onModuleRead(ryw, sks->getModules().rangeContaining(iter->begin())->value(), *lastModuleRead);
		if (iter->value() != nullptr) {
			wait(moveKeySelectorOverRangeActor(iter->value(), ryw, ks));
		}
		ks->offset < 1 ? --iter : ++iter;
	}
	*actualOffset = ks->offset;
	if (iter == sks->getImpls().ranges().begin())
		ks->setKey(sks->getKeyRange().begin);
	else if (iter == sks->getImpls().ranges().end())
		ks->setKey(sks->getKeyRange().end);

	if (!ks->isFirstGreaterOrEqual()) {
		// The Key Selector points to key outside the whole special key space
		TraceEvent(SevInfo, "KeySelectorPointsOutside")
		    .detail("TerminateKey", ks->getKey())
		    .detail("TerminateOffset", ks->offset);
		if (ks->offset < 1 && iter == sks->getImpls().ranges().begin())
			result->readToBegin = true;
		else
			result->readThroughEnd = true;
		ks->offset = 1;
	}
	return Void();
}

ACTOR Future<Standalone<RangeResultRef>> SpecialKeySpace::checkModuleFound(SpecialKeySpace* sks,
                                                                           Reference<ReadYourWritesTransaction> ryw,
                                                                           KeySelector begin, KeySelector end,
                                                                           GetRangeLimits limits, bool reverse) {
	std::pair<Standalone<RangeResultRef>, Optional<SpecialKeySpace::MODULE>> result =
	    wait(SpecialKeySpace::getRangeAggregationActor(sks, ryw, begin, end, limits, reverse));
	if (ryw && !ryw->specialKeySpaceRelaxed()) {
		auto module = result.second;
		if (!module.present() || module.get() == SpecialKeySpace::MODULE::UNKNOWN) {
			throw special_keys_no_module_found();
		}
	}
	return result.first;
}

ACTOR Future<std::pair<Standalone<RangeResultRef>, Optional<SpecialKeySpace::MODULE>>>
SpecialKeySpace::getRangeAggregationActor(SpecialKeySpace* sks, Reference<ReadYourWritesTransaction> ryw,
                                          KeySelector begin, KeySelector end, GetRangeLimits limits, bool reverse) {
	// This function handles ranges which cover more than one keyrange and aggregates all results
	// KeySelector, GetRangeLimits and reverse are all handled here
	state Standalone<RangeResultRef> result;
	state RangeMap<Key, SpecialKeyRangeBaseImpl*, KeyRangeRef>::Iterator iter;
	state int actualBeginOffset;
	state int actualEndOffset;
	state Optional<SpecialKeySpace::MODULE> lastModuleRead;

	wait(normalizeKeySelectorActor(sks, ryw, &begin, &lastModuleRead, &actualBeginOffset, &result));
	wait(normalizeKeySelectorActor(sks, ryw, &end, &lastModuleRead, &actualEndOffset, &result));
	// Handle all corner cases like what RYW does
	// return if range inverted
	if (actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
		TEST(true);
		return std::make_pair(RangeResultRef(false, false), lastModuleRead);
	}
	// If touches begin or end, return with readToBegin and readThroughEnd flags
	if (begin.getKey() == sks->range.end || end.getKey() == sks->range.begin) {
		TEST(true);
		return std::make_pair(result, lastModuleRead);
	}
	state RangeMap<Key, SpecialKeyRangeBaseImpl*, KeyRangeRef>::Ranges ranges =
	    sks->impls.intersectingRanges(KeyRangeRef(begin.getKey(), end.getKey()));
	// TODO : workaround to write this two together to make the code compact
	// The issue here is boost::iterator_range<> doest not provide rbegin(), rend()
	iter = reverse ? ranges.end() : ranges.begin();
	if (reverse) {
		while (iter != ranges.begin()) {
			--iter;
			onModuleRead(ryw, sks->getModules().rangeContaining(iter->begin())->value(), lastModuleRead);
			if (iter->value() == nullptr) continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
			result.arena().dependsOn(pairs.arena());
			// limits handler
			for (int i = pairs.size() - 1; i >= 0; --i) {
				result.push_back(result.arena(), pairs[i]);
				// Note : behavior here is even the last k-v pair makes total bytes larger than specified, it's still
				// returned. In other words, the total size of the returned value (less the last entry) will be less
				// than byteLimit
				limits.decrement(pairs[i]);
				if (limits.isReached()) {
					result.more = true;
					result.readToBegin = false;
					return std::make_pair(result, lastModuleRead);
				};
			}
		}
	} else {
		for (iter = ranges.begin(); iter != ranges.end(); ++iter) {
			onModuleRead(ryw, sks->getModules().rangeContaining(iter->begin())->value(), lastModuleRead);
			if (iter->value() == nullptr) continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			Standalone<RangeResultRef> pairs = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
			result.arena().dependsOn(pairs.arena());
			// limits handler
			for (int i = 0; i < pairs.size(); ++i) {
				result.push_back(result.arena(), pairs[i]);
				// Note : behavior here is even the last k-v pair makes total bytes larger than specified, it's still
				// returned. In other words, the total size of the returned value (less the last entry) will be less
				// than byteLimit
				limits.decrement(pairs[i]);
				if (limits.isReached()) {
					result.more = true;
					result.readThroughEnd = false;
					return std::make_pair(result, lastModuleRead);
				};
			}
		}
	}
	return std::make_pair(result, lastModuleRead);
}

Future<Standalone<RangeResultRef>> SpecialKeySpace::getRange(Reference<ReadYourWritesTransaction> ryw,
                                                             KeySelector begin, KeySelector end, GetRangeLimits limits,
                                                             bool reverse) {
	// validate limits here
	if (!limits.isValid()) return range_limits_invalid();
	if (limits.isReached()) {
		TEST(true); // read limit 0
		return Standalone<RangeResultRef>();
	}
	// make sure orEqual == false
	begin.removeOrEqual(begin.arena());
	end.removeOrEqual(end.arena());

	if (begin.offset >= end.offset && begin.getKey() >= end.getKey()) {
		TEST(true); // range inverted
		return Standalone<RangeResultRef>();
	}

	return checkModuleFound(this, ryw, begin, end, limits, reverse);
}

ACTOR Future<Optional<Value>> SpecialKeySpace::getActor(SpecialKeySpace* sks, Reference<ReadYourWritesTransaction> ryw,
                                                        KeyRef key) {
	// use getRange to workaround this
	Standalone<RangeResultRef> result =
	    wait(sks->getRange(ryw, KeySelector(firstGreaterOrEqual(key)), KeySelector(firstGreaterOrEqual(keyAfter(key))),
	                       GetRangeLimits(CLIENT_KNOBS->TOO_MANY), false));
	ASSERT(result.size() <= 1);
	if (result.size()) {
		return Optional<Value>(result[0].value);
	} else {
		return Optional<Value>();
	}
}

Future<Optional<Value>> SpecialKeySpace::get(Reference<ReadYourWritesTransaction> ryw, const Key& key) {
	return getActor(this, ryw, key);
}

ReadConflictRangeImpl::ReadConflictRangeImpl(KeyRangeRef kr) : SpecialKeyRangeBaseImpl(kr) {}

ACTOR static Future<Standalone<RangeResultRef>> getReadConflictRangeImpl(Reference<ReadYourWritesTransaction> ryw,
                                                                         KeyRange kr) {
	wait(ryw->pendingReads());
	return ryw->getReadConflictRangeIntersecting(kr);
}

Future<Standalone<RangeResultRef>> ReadConflictRangeImpl::getRange(Reference<ReadYourWritesTransaction> ryw,
                                                                   KeyRangeRef kr) const {
	return getReadConflictRangeImpl(ryw, kr);
}

WriteConflictRangeImpl::WriteConflictRangeImpl(KeyRangeRef kr) : SpecialKeyRangeBaseImpl(kr) {}

Future<Standalone<RangeResultRef>> WriteConflictRangeImpl::getRange(Reference<ReadYourWritesTransaction> ryw,
                                                                    KeyRangeRef kr) const {
	return ryw->getWriteConflictRangeIntersecting(kr);
}

ConflictingKeysImpl::ConflictingKeysImpl(KeyRangeRef kr) : SpecialKeyRangeBaseImpl(kr) {}

Future<Standalone<RangeResultRef>> ConflictingKeysImpl::getRange(Reference<ReadYourWritesTransaction> ryw,
                                                                 KeyRangeRef kr) const {
	Standalone<RangeResultRef> result;
	if (ryw->getTransactionInfo().conflictingKeys) {
		auto krMapPtr = ryw->getTransactionInfo().conflictingKeys.get();
		auto beginIter = krMapPtr->rangeContaining(kr.begin);
		if (beginIter->begin() != kr.begin) ++beginIter;
		auto endIter = krMapPtr->rangeContaining(kr.end);
		for (auto it = beginIter; it != endIter; ++it) {
			// it->begin() is stored in the CoalescedKeyRangeMap in TransactionInfo
			// it->value() is always constants in SystemData.cpp
			// Thus, push_back() can be used
			result.push_back(result.arena(), KeyValueRef(it->begin(), it->value()));
		}
		if (endIter->begin() != kr.end)
			result.push_back(result.arena(), KeyValueRef(endIter->begin(), endIter->value()));
	}
	return result;
}

ACTOR Future<Standalone<RangeResultRef>> ddStatsGetRangeActor(Reference<ReadYourWritesTransaction> ryw,
                                                              KeyRangeRef kr) {
	try {
		auto keys = kr.removePrefix(ddStatsRange.begin);
		Standalone<VectorRef<DDMetricsRef>> resultWithoutPrefix =
		    wait(waitDataDistributionMetricsList(ryw->getDatabase(), keys, CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT));
		Standalone<RangeResultRef> result;
		for (const auto& ddMetricsRef : resultWithoutPrefix) {
			// each begin key is the previous end key, thus we only encode the begin key in the result
			KeyRef beginKey = ddMetricsRef.beginKey.withPrefix(ddStatsRange.begin, result.arena());
			// Use json string encoded in utf-8 to encode the values, easy for adding more fields in the future
			json_spirit::mObject statsObj;
			statsObj["ShardBytes"] = ddMetricsRef.shardBytes;
			std::string statsString =
			    json_spirit::write_string(json_spirit::mValue(statsObj), json_spirit::Output_options::raw_utf8);
			ValueRef bytes(result.arena(), statsString);
			result.push_back(result.arena(), KeyValueRef(beginKey, bytes));
		}
		return result;
	} catch (Error& e) {
		throw;
	}
}

DDStatsRangeImpl::DDStatsRangeImpl(KeyRangeRef kr) : SpecialKeyRangeBaseImpl(kr) {}

Future<Standalone<RangeResultRef>> DDStatsRangeImpl::getRange(Reference<ReadYourWritesTransaction> ryw,
                                                              KeyRangeRef kr) const {
	return ddStatsGetRangeActor(ryw, kr);
}

class SpecialKeyRangeTestImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit SpecialKeyRangeTestImpl(KeyRangeRef kr, const std::string& prefix, int size)
	  : SpecialKeyRangeBaseImpl(kr), prefix(prefix), size(size) {
		ASSERT(size > 0);
		for (int i = 0; i < size; ++i) {
			kvs.push_back_deep(kvs.arena(),
			                   KeyValueRef(getKeyForIndex(i), deterministicRandom()->randomAlphaNumeric(16)));
		}
	}

	KeyValueRef getKeyValueForIndex(int idx) { return kvs[idx]; }

	Key getKeyForIndex(int idx) { return Key(prefix + format("%010d", idx)).withPrefix(range.begin); }
	int getSize() { return size; }
	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                            KeyRangeRef kr) const override {
		int startIndex = 0, endIndex = size;
		while (startIndex < size && kvs[startIndex].key < kr.begin) ++startIndex;
		while (endIndex > startIndex && kvs[endIndex - 1].key >= kr.end) --endIndex;
		if (startIndex == endIndex)
			return Standalone<RangeResultRef>();
		else
			return Standalone<RangeResultRef>(RangeResultRef(kvs.slice(startIndex, endIndex), false));
	}

private:
	Standalone<VectorRef<KeyValueRef>> kvs;
	std::string prefix;
	int size;
};

TEST_CASE("/fdbclient/SpecialKeySpace/Unittest") {
	SpecialKeySpace sks(normalKeys.begin, normalKeys.end);
	SpecialKeyRangeTestImpl pkr1(KeyRangeRef(LiteralStringRef("/cat/"), LiteralStringRef("/cat/\xff")), "small", 10);
	SpecialKeyRangeTestImpl pkr2(KeyRangeRef(LiteralStringRef("/dog/"), LiteralStringRef("/dog/\xff")), "medium", 100);
	SpecialKeyRangeTestImpl pkr3(KeyRangeRef(LiteralStringRef("/pig/"), LiteralStringRef("/pig/\xff")), "large", 1000);
	sks.registerKeyRange(SpecialKeySpace::MODULE::TESTONLY, pkr1.getKeyRange(), &pkr1);
	sks.registerKeyRange(SpecialKeySpace::MODULE::TESTONLY, pkr2.getKeyRange(), &pkr2);
	sks.registerKeyRange(SpecialKeySpace::MODULE::TESTONLY, pkr3.getKeyRange(), &pkr3);
	auto nullRef = Reference<ReadYourWritesTransaction>();
	// get
	{
		auto resultFuture = sks.get(nullRef, LiteralStringRef("/cat/small0000000009"));
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue().get();
		ASSERT(result == pkr1.getKeyValueForIndex(9).value);
		auto emptyFuture = sks.get(nullRef, LiteralStringRef("/cat/small0000000010"));
		ASSERT(emptyFuture.isReady());
		auto emptyResult = emptyFuture.getValue();
		ASSERT(!emptyResult.present());
	}
	// general getRange
	{
		KeySelector start = KeySelectorRef(LiteralStringRef("/elepant"), false, -9);
		KeySelector end = KeySelectorRef(LiteralStringRef("/frog"), false, +11);
		auto resultFuture = sks.getRange(nullRef, start, end, GetRangeLimits());
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue();
		ASSERT(result.size() == 20);
		ASSERT(result[0].key == pkr2.getKeyForIndex(90));
		ASSERT(result[result.size() - 1].key == pkr3.getKeyForIndex(9));
	}
	// KeySelector points outside
	{
		KeySelector start = KeySelectorRef(pkr3.getKeyForIndex(999), true, -1110);
		KeySelector end = KeySelectorRef(pkr1.getKeyForIndex(0), false, +1112);
		auto resultFuture = sks.getRange(nullRef, start, end, GetRangeLimits());
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue();
		ASSERT(result.size() == 1110);
		ASSERT(result[0].key == pkr1.getKeyForIndex(0));
		ASSERT(result[result.size() - 1].key == pkr3.getKeyForIndex(999));
	}
	// GetRangeLimits with row limit
	{
		KeySelector start = KeySelectorRef(pkr2.getKeyForIndex(0), true, 0);
		KeySelector end = KeySelectorRef(pkr3.getKeyForIndex(0), false, 0);
		auto resultFuture = sks.getRange(nullRef, start, end, GetRangeLimits(2));
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue();
		ASSERT(result.size() == 2);
		ASSERT(result[0].key == pkr2.getKeyForIndex(0));
		ASSERT(result[1].key == pkr2.getKeyForIndex(1));
	}
	// GetRangeLimits with byte limit
	{
		KeySelector start = KeySelectorRef(pkr2.getKeyForIndex(0), true, 0);
		KeySelector end = KeySelectorRef(pkr3.getKeyForIndex(0), false, 0);
		auto resultFuture = sks.getRange(nullRef, start, end, GetRangeLimits(10, 100));
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue();
		int bytes = 0;
		for (int i = 0; i < result.size() - 1; ++i) bytes += 8 + pkr2.getKeyValueForIndex(i).expectedSize();
		ASSERT(bytes < 100);
		ASSERT(bytes + 8 + pkr2.getKeyValueForIndex(result.size()).expectedSize() >= 100);
	}
	// reverse test with overlapping key range
	{
		KeySelector start = KeySelectorRef(pkr2.getKeyForIndex(0), true, 0);
		KeySelector end = KeySelectorRef(pkr3.getKeyForIndex(999), true, +1);
		auto resultFuture = sks.getRange(nullRef, start, end, GetRangeLimits(1100), true);
		ASSERT(resultFuture.isReady());
		auto result = resultFuture.getValue();
		for (int i = 0; i < pkr3.getSize(); ++i) ASSERT(result[i] == pkr3.getKeyValueForIndex(pkr3.getSize() - 1 - i));
		for (int i = 0; i < pkr2.getSize(); ++i)
			ASSERT(result[i + pkr3.getSize()] == pkr2.getKeyValueForIndex(pkr2.getSize() - 1 - i));
	}
	return Void();
}
