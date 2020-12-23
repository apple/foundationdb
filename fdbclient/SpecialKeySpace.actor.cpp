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
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/StatusClient.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
const std::string kTracingTransactionIdKey = "transaction_id";
const std::string kTracingTokenKey = "token";
}

std::unordered_map<SpecialKeySpace::MODULE, KeyRange> SpecialKeySpace::moduleToBoundary = {
	{ SpecialKeySpace::MODULE::TRANSACTION,
	  KeyRangeRef(LiteralStringRef("\xff\xff/transaction/"), LiteralStringRef("\xff\xff/transaction0")) },
	{ SpecialKeySpace::MODULE::WORKERINTERFACE,
	  KeyRangeRef(LiteralStringRef("\xff\xff/worker_interfaces/"), LiteralStringRef("\xff\xff/worker_interfaces0")) },
	{ SpecialKeySpace::MODULE::STATUSJSON, singleKeyRange(LiteralStringRef("\xff\xff/status/json")) },
	{ SpecialKeySpace::MODULE::CONNECTIONSTRING, singleKeyRange(LiteralStringRef("\xff\xff/connection_string")) },
	{ SpecialKeySpace::MODULE::CLUSTERFILEPATH, singleKeyRange(LiteralStringRef("\xff\xff/cluster_file_path")) },
	{ SpecialKeySpace::MODULE::METRICS,
	  KeyRangeRef(LiteralStringRef("\xff\xff/metrics/"), LiteralStringRef("\xff\xff/metrics0")) },
	{ SpecialKeySpace::MODULE::MANAGEMENT,
	  KeyRangeRef(LiteralStringRef("\xff\xff/management/"), LiteralStringRef("\xff\xff/management0")) },
	{ SpecialKeySpace::MODULE::ERRORMSG, singleKeyRange(LiteralStringRef("\xff\xff/error_message")) },
	{ SpecialKeySpace::MODULE::CONFIGURATION,
	  KeyRangeRef(LiteralStringRef("\xff\xff/configuration/"), LiteralStringRef("\xff\xff/configuration0")) },
	{ SpecialKeySpace::MODULE::TRACING,
	  KeyRangeRef(LiteralStringRef("\xff\xff/tracing/"), LiteralStringRef("\xff\xff/tracing0")) }
};

std::unordered_map<std::string, KeyRange> SpecialKeySpace::managementApiCommandToRange = {
	{ "exclude", KeyRangeRef(LiteralStringRef("excluded/"), LiteralStringRef("excluded0"))
	                 .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "failed", KeyRangeRef(LiteralStringRef("failed/"), LiteralStringRef("failed0"))
	                .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "lock", singleKeyRange(LiteralStringRef("db_locked")).withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "consistencycheck", singleKeyRange(LiteralStringRef("consistency_check_suspended"))
	                          .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) }
};

std::set<std::string> SpecialKeySpace::options = { "excluded/force", "failed/force" };

std::set<std::string> SpecialKeySpace::tracingOptions = { kTracingTransactionIdKey, kTracingTokenKey };

Standalone<RangeResultRef> rywGetRange(ReadYourWritesTransaction* ryw, const KeyRangeRef& kr,
                                       const Standalone<RangeResultRef>& res);

// This function will move the given KeySelector as far as possible to the standard form:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is not in the underlying key range, it will move over the range
// The cache object is used to cache the first read result from the rpc call during the key resolution,
// then when we need to do key resolution or result filtering,
// we, instead of rpc call, read from this cache object have consistent results
ACTOR Future<Void> moveKeySelectorOverRangeActor(const SpecialKeyRangeReadImpl* skrImpl, ReadYourWritesTransaction* ryw,
                                                 KeySelector* ks, Optional<Standalone<RangeResultRef>>* cache) {
	ASSERT(!ks->orEqual); // should be removed before calling
	ASSERT(ks->offset != 1); // never being called if KeySelector is already normalized

	state Key startKey(skrImpl->getKeyRange().begin);
	state Key endKey(skrImpl->getKeyRange().end);
	state Standalone<RangeResultRef> result;

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

	if (skrImpl->isAsync()) {
		const SpecialKeyRangeAsyncImpl* ptr = dynamic_cast<const SpecialKeyRangeAsyncImpl*>(skrImpl);
		Standalone<RangeResultRef> result_ = wait(ptr->getRange(ryw, KeyRangeRef(startKey, endKey), cache));
		result = result_;
	} else {
		Standalone<RangeResultRef> result_ = wait(skrImpl->getRange(ryw, KeyRangeRef(startKey, endKey)));
		result = result_;
	}

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
			ks->setKey(KeyRef(
			    ks->arena(),
			    keyAfter(result[result.size() - 1].key))); // TODO : the keyAfter will just return if key == \xff\xff
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

// This function will normalize the given KeySelector to a standard KeySelector:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is outside the whole space, it will move to the begin or the end
// It does have overhead here since we query all keys twice in the worst case.
// However, moving the KeySelector while handling other parameters like limits makes the code much more complex and hard
// to maintain; Thus, separate each part to make the code easy to understand and more compact
// Boundary is the range of the legal key space, which, by default is the range of the module
// And (\xff\xff, \xff\xff\xff) if SPECIAL_KEY_SPACE_RELAXED is turned on
ACTOR Future<Void> normalizeKeySelectorActor(SpecialKeySpace* sks, ReadYourWritesTransaction* ryw, KeySelector* ks,
                                             KeyRangeRef boundary, int* actualOffset,
                                             Standalone<RangeResultRef>* result,
                                             Optional<Standalone<RangeResultRef>>* cache) {
	// If offset < 1, where we need to move left, iter points to the range containing at least one smaller key
	// (It's a wasting of time to walk through the range whose begin key is same as ks->key)
	// (rangeContainingKeyBefore itself handles the case where ks->key == Key())
	// Otherwise, we only need to move right if offset > 1, iter points to the range containing the key
	// Since boundary.end is always a key in the RangeMap, it is always safe to move right
	state RangeMap<Key, SpecialKeyRangeReadImpl*, KeyRangeRef>::iterator iter =
	    ks->offset < 1 ? sks->getReadImpls().rangeContainingKeyBefore(ks->getKey())
	                   : sks->getReadImpls().rangeContaining(ks->getKey());
	while ((ks->offset < 1 && iter->begin() >= boundary.begin) || (ks->offset > 1 && iter->begin() < boundary.end)) {
		if (iter->value() != nullptr) {
			wait(moveKeySelectorOverRangeActor(iter->value(), ryw, ks, cache));
		}
		// Check if we can still move the iterator left
		if (ks->offset < 1) {
			if (iter == sks->getReadImpls().ranges().begin()) {
				break;
			} else {
				--iter;
			}
		} else if (ks->offset > 1) {
			// Always safe to move right
			++iter;
		}
	}
	*actualOffset = ks->offset;

	if (!ks->isFirstGreaterOrEqual()) {
		TraceEvent(SevDebug, "ReadToBoundary")
		    .detail("TerminateKey", ks->getKey())
		    .detail("TerminateOffset", ks->offset);
		// If still not normalized after moving to the boundary, 
		// let key selector clamp up to the boundary
		if (ks->offset < 1) {
			result->readToBegin = true;
			ks->setKey(boundary.begin);
		}
		else {
			result->readThroughEnd = true;
			ks->setKey(boundary.end);
		}
		ks->offset = 1;
	}
	return Void();
}

SpecialKeySpace::SpecialKeySpace(KeyRef spaceStartKey, KeyRef spaceEndKey, bool testOnly)
  : range(KeyRangeRef(spaceStartKey, spaceEndKey)), readImpls(nullptr, spaceEndKey), writeImpls(nullptr, spaceEndKey),
    modules(testOnly ? SpecialKeySpace::MODULE::TESTONLY : SpecialKeySpace::MODULE::UNKNOWN, spaceEndKey) {
	// Default begin of KeyRangeMap is Key(), insert the range to update start key
	readImpls.insert(range, nullptr);
	writeImpls.insert(range, nullptr);
	if (!testOnly) modulesBoundaryInit(); // testOnly is used in the correctness workload
}

void SpecialKeySpace::modulesBoundaryInit() {
	for (const auto& pair : moduleToBoundary) {
		ASSERT(range.contains(pair.second));
		// Make sure the module is not overlapping with any registered read modules
		// Note: same like ranges, one module's end cannot be another module's start, relax the condition if needed
		ASSERT(modules.rangeContaining(pair.second.begin) == modules.rangeContaining(pair.second.end) &&
		       modules[pair.second.begin] == SpecialKeySpace::MODULE::UNKNOWN);
		modules.insert(pair.second, pair.first);
		// Note: Due to underlying implementation, the insertion here is important to make cross_module_read being
		// handled correctly
		readImpls.insert(pair.second, nullptr);
		writeImpls.insert(pair.second, nullptr);
	}
}

ACTOR Future<Standalone<RangeResultRef>> SpecialKeySpace::checkRYWValid(SpecialKeySpace* sks,
                                                                        ReadYourWritesTransaction* ryw,
                                                                        KeySelector begin, KeySelector end,
                                                                        GetRangeLimits limits, bool reverse) {
	ASSERT(ryw);
	choose {
		when(Standalone<RangeResultRef> result =
		         wait(SpecialKeySpace::getRangeAggregationActor(sks, ryw, begin, end, limits, reverse))) {
			return result;
		}
		when(wait(ryw->resetFuture())) { throw internal_error(); }
	}
}

ACTOR Future<Standalone<RangeResultRef>> SpecialKeySpace::getRangeAggregationActor(SpecialKeySpace* sks,
                                                                                   ReadYourWritesTransaction* ryw,
                                                                                   KeySelector begin, KeySelector end,
                                                                                   GetRangeLimits limits,
                                                                                   bool reverse) {
	// This function handles ranges which cover more than one keyrange and aggregates all results
	// KeySelector, GetRangeLimits and reverse are all handled here
	state Standalone<RangeResultRef> result;
	state Standalone<RangeResultRef> pairs;
	state RangeMap<Key, SpecialKeyRangeReadImpl*, KeyRangeRef>::iterator iter;
	state int actualBeginOffset;
	state int actualEndOffset;
	state KeyRangeRef moduleBoundary;
	// used to cache result from potential first read
	state Optional<Standalone<RangeResultRef>> cache;

	if (ryw->specialKeySpaceRelaxed()) {
		moduleBoundary = sks->range;
	} else {
		auto beginIter = sks->getModules().rangeContaining(begin.getKey());
		if (beginIter->begin() <= end.getKey() && end.getKey() <= beginIter->end()) {
			if (beginIter->value() == SpecialKeySpace::MODULE::UNKNOWN)
				throw special_keys_no_module_found();
			else
				moduleBoundary = beginIter->range();
		} else {
			TraceEvent(SevInfo, "SpecialKeyCrossModuleRead")
			    .detail("Begin", begin.toString())
			    .detail("End", end.toString())
			    .detail("BoundaryBegin", beginIter->begin())
			    .detail("BoundaryEnd", beginIter->end());
			throw special_keys_cross_module_read();
		}
	}

	wait(normalizeKeySelectorActor(sks, ryw, &begin, moduleBoundary, &actualBeginOffset, &result, &cache));
	wait(normalizeKeySelectorActor(sks, ryw, &end, moduleBoundary, &actualEndOffset, &result, &cache));
	// Handle all corner cases like what RYW does
	// return if range inverted
	if (actualBeginOffset >= actualEndOffset && begin.getKey() >= end.getKey()) {
		TEST(true); // inverted range
		return RangeResultRef(false, false);
	}
	// If touches begin or end, return with readToBegin and readThroughEnd flags
	if (begin.getKey() == moduleBoundary.end || end.getKey() == moduleBoundary.begin) {
		TEST(true); // query touches begin or end
		return result;
	}
	state RangeMap<Key, SpecialKeyRangeReadImpl*, KeyRangeRef>::Ranges ranges =
	    sks->getReadImpls().intersectingRanges(KeyRangeRef(begin.getKey(), end.getKey()));
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
			if (iter->value()->isAsync() && cache.present()) {
				const SpecialKeyRangeAsyncImpl* ptr = dynamic_cast<const SpecialKeyRangeAsyncImpl*>(iter->value());
				Standalone<RangeResultRef> pairs_ = wait(ptr->getRange(ryw, KeyRangeRef(keyStart, keyEnd), &cache));
				pairs = pairs_;
			} else {
				Standalone<RangeResultRef> pairs_ = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
				pairs = pairs_;
			}
			result.arena().dependsOn(pairs.arena());
			// limits handler
			for (int i = pairs.size() - 1; i >= 0; --i) {
				ASSERT(iter->range().contains(pairs[i].key));
				result.push_back(result.arena(), pairs[i]);
				// Note : behavior here is even the last k-v pair makes total bytes larger than specified, it's still
				// returned. In other words, the total size of the returned value (less the last entry) will be less
				// than byteLimit
				limits.decrement(pairs[i]);
				if (limits.isReached()) {
					result.more = true;
					result.readToBegin = false;
					return result;
				};
			}
		}
	} else {
		for (iter = ranges.begin(); iter != ranges.end(); ++iter) {
			if (iter->value() == nullptr) continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			if (iter->value()->isAsync() && cache.present()) {
				const SpecialKeyRangeAsyncImpl* ptr = dynamic_cast<const SpecialKeyRangeAsyncImpl*>(iter->value());
				Standalone<RangeResultRef> pairs_ = wait(ptr->getRange(ryw, KeyRangeRef(keyStart, keyEnd), &cache));
				pairs = pairs_;
			} else {
				Standalone<RangeResultRef> pairs_ = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd)));
				pairs = pairs_;
			}
			result.arena().dependsOn(pairs.arena());
			// limits handler
			for (int i = 0; i < pairs.size(); ++i) {
				ASSERT(iter->range().contains(pairs[i].key));
				result.push_back(result.arena(), pairs[i]);
				// Note : behavior here is even the last k-v pair makes total bytes larger than specified, it's still
				// returned. In other words, the total size of the returned value (less the last entry) will be less
				// than byteLimit
				limits.decrement(pairs[i]);
				if (limits.isReached()) {
					result.more = true;
					result.readThroughEnd = false;
					return result;
				};
			}
		}
	}
	return result;
}

Future<Standalone<RangeResultRef>> SpecialKeySpace::getRange(ReadYourWritesTransaction* ryw, KeySelector begin,
                                                             KeySelector end, GetRangeLimits limits, bool reverse) {
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

	return checkRYWValid(this, ryw, begin, end, limits, reverse);
}

ACTOR Future<Optional<Value>> SpecialKeySpace::getActor(SpecialKeySpace* sks, ReadYourWritesTransaction* ryw,
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

Future<Optional<Value>> SpecialKeySpace::get(ReadYourWritesTransaction* ryw, const Key& key) {
	return getActor(this, ryw, key);
}

void SpecialKeySpace::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	if (!ryw->specialKeySpaceChangeConfiguration()) throw special_keys_write_disabled();
	auto impl = writeImpls[key];
	if (impl == nullptr) {
		TraceEvent(SevDebug, "SpecialKeySpaceNoWriteModuleFound")
		    .detail("Key", key.toString())
		    .detail("Value", value.toString());
		throw special_keys_no_write_module_found();
	}
	return impl->set(ryw, key, value);
}

void SpecialKeySpace::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	if (!ryw->specialKeySpaceChangeConfiguration()) throw special_keys_write_disabled();
	if (range.empty()) return;
	auto begin = writeImpls[range.begin];
	auto end = writeImpls.rangeContainingKeyBefore(range.end)->value();
	if (begin != end) {
		TraceEvent(SevDebug, "SpecialKeySpaceCrossModuleClear").detail("Range", range.toString());
		throw special_keys_cross_module_clear(); // ban cross module clear
	} else if (begin == nullptr) {
		TraceEvent(SevDebug, "SpecialKeySpaceNoWriteModuleFound").detail("Range", range.toString());
		throw special_keys_no_write_module_found();
	}
	return begin->clear(ryw, range);
}

void SpecialKeySpace::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	if (!ryw->specialKeySpaceChangeConfiguration()) throw special_keys_write_disabled();
	auto impl = writeImpls[key];
	if (impl == nullptr) throw special_keys_no_write_module_found();
	return impl->clear(ryw, key);
}

bool validateSnakeCaseNaming(const KeyRef& k) {
	KeyRef key(k);
	// Remove prefix \xff\xff
	ASSERT(key.startsWith(specialKeys.begin));
	key = key.removePrefix(specialKeys.begin);
	// Suffix can be \xff\xff or \x00 in single key range
	if (key.endsWith(specialKeys.begin))
		key = key.removeSuffix(specialKeys.end);
	else if (key.endsWith(LiteralStringRef("\x00")))
		key = key.removeSuffix(LiteralStringRef("\x00"));
	for (const char& c : key.toString()) {
		// only small letters, numbers, '/', '_' is allowed
		ASSERT((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '/' || c == '_');
	}
	return true;
}

void SpecialKeySpace::registerKeyRange(SpecialKeySpace::MODULE module, SpecialKeySpace::IMPLTYPE type,
                                       const KeyRangeRef& kr, SpecialKeyRangeReadImpl* impl) {
	// module boundary check
	if (module == SpecialKeySpace::MODULE::TESTONLY) {
		ASSERT(normalKeys.contains(kr));
	} else {
		ASSERT(moduleToBoundary.at(module).contains(kr));
		ASSERT(validateSnakeCaseNaming(kr.begin) &&
		       validateSnakeCaseNaming(kr.end)); // validate keys follow snake case naming style
	}
	// make sure the registered range is not overlapping with existing ones
	// Note: kr.end should not be the same as another range's begin, although it should work even they are the same
	for (auto iter = readImpls.rangeContaining(kr.begin); true; ++iter) {
		ASSERT(iter->value() == nullptr);
		if (iter == readImpls.rangeContaining(kr.end))
			break; // Note: relax the condition that the end can be another range's start, if needed
	}
	readImpls.insert(kr, impl);
	// if rw, it means the module can do both read and write
	if (type == SpecialKeySpace::IMPLTYPE::READWRITE) {
		// since write impls are always subset of read impls,
		// no need to check overlapped registration
		auto rwImpl = dynamic_cast<SpecialKeyRangeRWImpl*>(impl);
		ASSERT(rwImpl);
		writeImpls.insert(kr, rwImpl);
	}
}

Key SpecialKeySpace::decode(const KeyRef& key) {
	auto impl = writeImpls[key];
	ASSERT(impl != nullptr);
	return impl->decode(key);
}

KeyRange SpecialKeySpace::decode(const KeyRangeRef& kr) {
	// Only allow to decode key range in the same underlying impl range
	auto begin = writeImpls.rangeContaining(kr.begin);
	ASSERT(begin->value() != nullptr);
	auto end = writeImpls.rangeContainingKeyBefore(kr.end);
	ASSERT(begin == end);
	return KeyRangeRef(begin->value()->decode(kr.begin), begin->value()->decode(kr.end));
}

ACTOR Future<Void> commitActor(SpecialKeySpace* sks, ReadYourWritesTransaction* ryw) {
	state RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::Ranges ranges =
	    ryw->getSpecialKeySpaceWriteMap().containedRanges(specialKeys);
	state RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::iterator iter = ranges.begin();
	state std::set<SpecialKeyRangeRWImpl*> writeModulePtrs;
	while (iter != ranges.end()) {
		std::pair<bool, Optional<Value>> entry = iter->value();
		if (entry.first) {
			auto modulePtr = sks->getRWImpls().rangeContaining(iter->begin())->value();
			writeModulePtrs.insert(modulePtr);
		}
		++iter;
	}
	state std::set<SpecialKeyRangeRWImpl*>::const_iterator it;
	for (it = writeModulePtrs.begin(); it != writeModulePtrs.end(); ++it) {
		Optional<std::string> msg = wait((*it)->commit(ryw));
		if (msg.present()) {
			ryw->setSpecialKeySpaceErrorMsg(msg.get());
			TraceEvent(SevDebug, "SpecialKeySpaceManagemetnAPIError")
			    .detail("Reason", msg.get())
			    .detail("Range", (*it)->getKeyRange().toString());
			throw special_keys_api_failure();
		}
	}
	return Void();
}

Future<Void> SpecialKeySpace::commit(ReadYourWritesTransaction* ryw) {
	return commitActor(this, ryw);
}

SKSCTestImpl::SKSCTestImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> SKSCTestImpl::getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const {
	ASSERT(range.contains(kr));
	auto resultFuture = ryw->getRange(kr, CLIENT_KNOBS->TOO_MANY);
	// all keys are written to RYW, since GRV is set, the read should happen locally
	ASSERT(resultFuture.isReady());
	auto result = resultFuture.getValue();
	ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
	auto kvs = resultFuture.getValue();
	return rywGetRange(ryw, kr, kvs);
}

Future<Optional<std::string>> SKSCTestImpl::commit(ReadYourWritesTransaction* ryw) {
	ASSERT(false);
	return Optional<std::string>();
}

ReadConflictRangeImpl::ReadConflictRangeImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

ACTOR static Future<Standalone<RangeResultRef>> getReadConflictRangeImpl(ReadYourWritesTransaction* ryw, KeyRange kr) {
	wait(ryw->pendingReads());
	return ryw->getReadConflictRangeIntersecting(kr);
}

Future<Standalone<RangeResultRef>> ReadConflictRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                   KeyRangeRef kr) const {
	return getReadConflictRangeImpl(ryw, kr);
}

WriteConflictRangeImpl::WriteConflictRangeImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

Future<Standalone<RangeResultRef>> WriteConflictRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                    KeyRangeRef kr) const {
	return ryw->getWriteConflictRangeIntersecting(kr);
}

ConflictingKeysImpl::ConflictingKeysImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

Future<Standalone<RangeResultRef>> ConflictingKeysImpl::getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const {
	Standalone<RangeResultRef> result;
	if (ryw->getTransactionInfo().conflictingKeys) {
		auto krMapPtr = ryw->getTransactionInfo().conflictingKeys.get();
		auto beginIter = krMapPtr->rangeContaining(kr.begin);
		if (beginIter->begin() != kr.begin) ++beginIter;
		auto endIter = krMapPtr->rangeContaining(kr.end);
		for (auto it = beginIter; it != endIter; ++it) {
			result.push_back_deep(result.arena(), KeyValueRef(it->begin(), it->value()));
		}
		if (endIter->begin() != kr.end)
			result.push_back_deep(result.arena(), KeyValueRef(endIter->begin(), endIter->value()));
	}
	return result;
}

ACTOR Future<Standalone<RangeResultRef>> ddMetricsGetRangeActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	loop {
		try {
			auto keys = kr.removePrefix(ddStatsRange.begin);
			Standalone<VectorRef<DDMetricsRef>> resultWithoutPrefix = wait(
			    waitDataDistributionMetricsList(ryw->getDatabase(), keys, CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT));
			Standalone<RangeResultRef> result;
			for (const auto& ddMetricsRef : resultWithoutPrefix) {
				// each begin key is the previous end key, thus we only encode the begin key in the result
				KeyRef beginKey = ddMetricsRef.beginKey.withPrefix(ddStatsRange.begin, result.arena());
				// Use json string encoded in utf-8 to encode the values, easy for adding more fields in the future
				json_spirit::mObject statsObj;
				statsObj["shard_bytes"] = ddMetricsRef.shardBytes;
				std::string statsString =
				    json_spirit::write_string(json_spirit::mValue(statsObj), json_spirit::Output_options::raw_utf8);
				ValueRef bytes(result.arena(), statsString);
				result.push_back(result.arena(), KeyValueRef(beginKey, bytes));
			}
			return result;
		} catch (Error& e) {
			state Error err(e);
			if (e.code() == error_code_dd_not_found) {
				TraceEvent(SevWarnAlways, "DataDistributorNotPresent")
				    .detail("Operation", "DDMetricsReqestThroughSpecialKeys");
				wait(delayJittered(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				continue;
			}
			throw err;
		}
	}
}

DDStatsRangeImpl::DDStatsRangeImpl(KeyRangeRef kr) : SpecialKeyRangeAsyncImpl(kr) {}

Future<Standalone<RangeResultRef>> DDStatsRangeImpl::getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const {
	return ddMetricsGetRangeActor(ryw, kr);
}

Key SpecialKeySpace::getManagementApiCommandOptionSpecialKey(const std::string& command, const std::string& option) {
	Key prefix = LiteralStringRef("options/").withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin);
	auto pair = command + "/" + option;
	ASSERT(options.find(pair) != options.end());
	return prefix.withSuffix(pair);
}

ManagementCommandsOptionsImpl::ManagementCommandsOptionsImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> ManagementCommandsOptionsImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                           KeyRangeRef kr) const {
	Standalone<RangeResultRef> result;
	// Since we only have limit number of options, a brute force loop here is enough
	for (const auto& option : SpecialKeySpace::getManagementApiOptionsSet()) {
		auto key = getKeyRange().begin.withSuffix(option);
		// ignore all invalid keys
		auto r = ryw->getSpecialKeySpaceWriteMap()[key];
		if (kr.contains(key) && r.first && r.second.present()) {
			result.push_back(result.arena(), KeyValueRef(key, ValueRef()));
			result.arena().dependsOn(key.arena());
		}
	}
	return result;
}

void ManagementCommandsOptionsImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	std::string option = key.removePrefix(getKeyRange().begin).toString();
	// ignore all invalid keys
	if (SpecialKeySpace::getManagementApiOptionsSet().find(option) !=
	    SpecialKeySpace::getManagementApiOptionsSet().end()) {
		TraceEvent(SevDebug, "ManagementApiOption").detail("Option", option).detail("Key", key);
		ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(value)));
	}
}

void ManagementCommandsOptionsImpl::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	ryw->getSpecialKeySpaceWriteMap().rawErase(range);
}

void ManagementCommandsOptionsImpl::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	std::string option = key.removePrefix(getKeyRange().begin).toString();
	// ignore all invalid keys
	if (SpecialKeySpace::getManagementApiOptionsSet().find(option) !=
	    SpecialKeySpace::getManagementApiOptionsSet().end()) {
		ryw->getSpecialKeySpaceWriteMap().rawErase(singleKeyRange(key));
	}
}

Future<Optional<std::string>> ManagementCommandsOptionsImpl::commit(ReadYourWritesTransaction* ryw) {
	// Nothing to do, keys should be used by other impls' commit callback
	return Optional<std::string>();
}

Standalone<RangeResultRef> rywGetRange(ReadYourWritesTransaction* ryw, const KeyRangeRef& kr,
                                       const Standalone<RangeResultRef>& res) {
	// "res" is the read result regardless of your writes, if ryw disabled, return immediately
	if (ryw->readYourWritesDisabled()) return res;
	// If ryw enabled, we update it with writes from the transaction
	Standalone<RangeResultRef> result;
	RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::Ranges ranges =
	    ryw->getSpecialKeySpaceWriteMap().containedRanges(kr);
	RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::iterator iter = ranges.begin();
	auto iter2 = res.begin();
	result.arena().dependsOn(res.arena());
	while (iter != ranges.end() || iter2 != res.end()) {
		if (iter == ranges.end()) {
			result.push_back(result.arena(), KeyValueRef(iter2->key, iter2->value));
			++iter2;
		} else if (iter2 == res.end()) {
			// insert if it is a set entry
			std::pair<bool, Optional<Value>> entry = iter->value();
			if (entry.first && entry.second.present()) {
				result.push_back_deep(result.arena(), KeyValueRef(iter->begin(), entry.second.get()));
			}
			++iter;
		} else if (iter->range().contains(iter2->key)) {
			std::pair<bool, Optional<Value>> entry = iter->value();
			// if this is a valid range either for set or clear, move iter2 outside the range
			if (entry.first) {
				// insert if this is a set entry
				if (entry.second.present())
					result.push_back_deep(result.arena(), KeyValueRef(iter->begin(), entry.second.get()));
				// move iter2 outside the range
				while (iter2 != res.end() && iter->range().contains(iter2->key)) ++iter2;
			}
			++iter;
		} else if (iter->begin() > iter2->key) {
			result.push_back(result.arena(), KeyValueRef(iter2->key, iter2->value));
			++iter2;
		} else if (iter->end() <= iter2->key) {
			// insert if it is a set entry
			std::pair<bool, Optional<Value>> entry = iter->value();
			if (entry.first && entry.second.present()) {
				result.push_back_deep(result.arena(), KeyValueRef(iter->begin(), entry.second.get()));
			}
			++iter;
		}
	}
	return result;
}

// read from those readwrite modules in which special keys have one-to-one mapping with real persisted keys
ACTOR Future<Standalone<RangeResultRef>> rwModuleWithMappingGetRangeActor(ReadYourWritesTransaction* ryw,
                                                                          const SpecialKeyRangeRWImpl* impl,
                                                                          KeyRangeRef kr) {
	Standalone<RangeResultRef> resultWithoutPrefix =
	    wait(ryw->getTransaction().getRange(ryw->getDatabase()->specialKeySpace->decode(kr), CLIENT_KNOBS->TOO_MANY));
	ASSERT(!resultWithoutPrefix.more && resultWithoutPrefix.size() < CLIENT_KNOBS->TOO_MANY);
	Standalone<RangeResultRef> result;
	for (const KeyValueRef& kv : resultWithoutPrefix)
		result.push_back_deep(result.arena(), KeyValueRef(impl->encode(kv.key), kv.value));
	return rywGetRange(ryw, kr, result);
}

ExcludeServersRangeImpl::ExcludeServersRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> ExcludeServersRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                     KeyRangeRef kr) const {
	return rwModuleWithMappingGetRangeActor(ryw, this, kr);
}

void ExcludeServersRangeImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	// ignore value
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(ValueRef())));
}

Key ExcludeServersRangeImpl::decode(const KeyRef& key) const {
	return key.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
	    .withPrefix(LiteralStringRef("\xff/conf/"));
}

Key ExcludeServersRangeImpl::encode(const KeyRef& key) const {
	return key.removePrefix(LiteralStringRef("\xff/conf/"))
	    .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
}

bool parseNetWorkAddrFromKeys(ReadYourWritesTransaction* ryw, bool failed, std::vector<AddressExclusion>& addresses,
                              std::set<AddressExclusion>& exclusions, Optional<std::string>& msg) {
	KeyRangeRef range = failed ? SpecialKeySpace::getManamentApiCommandRange("failed")
	                           : SpecialKeySpace::getManamentApiCommandRange("exclude");
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
	auto iter = ranges.begin();
	while (iter != ranges.end()) {
		auto entry = iter->value();
		// only check for exclude(set) operation, include(clear) are not checked
		TraceEvent(SevDebug, "ParseNetworkAddress")
		    .detail("Valid", entry.first)
		    .detail("Set", entry.second.present())
		    .detail("Key", iter->begin().toString());
		if (entry.first && entry.second.present()) {
			Key address = iter->begin().removePrefix(range.begin);
			auto a = AddressExclusion::parse(address);
			if (!a.isValid()) {
				std::string error = "ERROR: \'" + address.toString() + "\' is not a valid network endpoint address\n";
				if (address.toString().find(":tls") != std::string::npos)
					error += "        Do not include the `:tls' suffix when naming a process\n";
				msg = ManagementAPIError::toJsonString(
				    false, entry.second.present() ? (failed ? "exclude failed" : "exclude") : "include", error);
				return false;
			}
			addresses.push_back(a);
			exclusions.insert(a);
		}
		++iter;
	}
	return true;
}

ACTOR Future<bool> checkExclusion(Database db, std::vector<AddressExclusion>* addresses,
                                  std::set<AddressExclusion>* exclusions, bool markFailed, Optional<std::string>* msg) {

	if (markFailed) {
		state bool safe;
		try {
			bool _safe = wait(checkSafeExclusions(db, *addresses));
			safe = _safe;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled) throw;
			TraceEvent("CheckSafeExclusionsError").error(e);
			safe = false;
		}
		if (!safe) {
			std::string temp = "ERROR: It is unsafe to exclude the specified servers at this time.\n"
			                   "Please check that this exclusion does not bring down an entire storage team.\n"
			                   "Please also ensure that the exclusion will keep a majority of coordinators alive.\n"
			                   "You may add more storage processes or coordinators to make the operation safe.\n"
			                   "Call set(\"0xff0xff/management/failed/<ADDRESS...>\", ...) to exclude without "
			                   "performing safety checks.\n";
			*msg = ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", temp);
			return false;
		}
	}
	StatusObject status = wait(StatusClient::statusFetcher(db));
	state std::string errorString =
	    "ERROR: Could not calculate the impact of this exclude on the total free space in the cluster.\n"
	    "Please try the exclude again in 30 seconds.\n"
	    "Call set(\"0xff0xff/management/options/exclude/force\", ...) first to exclude without checking free "
	    "space.\n";

	StatusObjectReader statusObj(status);

	StatusObjectReader statusObjCluster;
	if (!statusObj.get("cluster", statusObjCluster)) {
		*msg = ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
		return false;
	}

	StatusObjectReader processesMap;
	if (!statusObjCluster.get("processes", processesMap)) {
		*msg = ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
		return false;
	}

	state int ssTotalCount = 0;
	state int ssExcludedCount = 0;
	state double worstFreeSpaceRatio = 1.0;
	try {
		for (auto proc : processesMap.obj()) {
			bool storageServer = false;
			StatusArray rolesArray = proc.second.get_obj()["roles"].get_array();
			for (StatusObjectReader role : rolesArray) {
				if (role["role"].get_str() == "storage") {
					storageServer = true;
					break;
				}
			}
			// Skip non-storage servers in free space calculation
			if (!storageServer) continue;

			StatusObjectReader process(proc.second);
			std::string addrStr;
			if (!process.get("address", addrStr)) {
				*msg = ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
				return false;
			}
			NetworkAddress addr = NetworkAddress::parse(addrStr);
			bool excluded =
			    (process.has("excluded") && process.last().get_bool()) || addressExcluded(*exclusions, addr);
			ssTotalCount++;
			if (excluded) ssExcludedCount++;

			if (!excluded) {
				StatusObjectReader disk;
				if (!process.get("disk", disk)) {
					*msg =
					    ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
					return false;
				}

				int64_t total_bytes;
				if (!disk.get("total_bytes", total_bytes)) {
					*msg =
					    ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
					return false;
				}

				int64_t free_bytes;
				if (!disk.get("free_bytes", free_bytes)) {
					*msg =
					    ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
					return false;
				}

				worstFreeSpaceRatio = std::min(worstFreeSpaceRatio, double(free_bytes) / total_bytes);
			}
		}
	} catch (...) // std::exception
	{
		*msg = ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", errorString);
		return false;
	}

	if (ssExcludedCount == ssTotalCount ||
	    (1 - worstFreeSpaceRatio) * ssTotalCount / (ssTotalCount - ssExcludedCount) > 0.9) {
		std::string temp = "ERROR: This exclude may cause the total free space in the cluster to drop below 10%.\n"
		                   "Call set(\"0xff0xff/management/options/exclude/force\", ...) first to exclude without "
		                   "checking free space.\n";
		*msg = ManagementAPIError::toJsonString(false, markFailed ? "exclude failed" : "exclude", temp);
		return false;
	}
	return true;
}

void includeServers(ReadYourWritesTransaction* ryw) {
	ryw->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	ryw->setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	// includeServers might be used in an emergency transaction, so make sure it is retry-self-conflicting and
	// CAUSAL_WRITE_RISKY
	ryw->setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
	std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	// for exluded servers
	auto ranges =
	    ryw->getSpecialKeySpaceWriteMap().containedRanges(SpecialKeySpace::getManamentApiCommandRange("exclude"));
	auto iter = ranges.begin();
	Transaction& tr = ryw->getTransaction();
	while (iter != ranges.end()) {
		auto entry = iter->value();
		if (entry.first && !entry.second.present()) {
			tr.addReadConflictRange(singleKeyRange(excludedServersVersionKey));
			tr.set(excludedServersVersionKey, versionKey);
			tr.clear(ryw->getDatabase()->specialKeySpace->decode(iter->range()));
		}
		++iter;
	}
	// for failed servers
	ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(SpecialKeySpace::getManamentApiCommandRange("failed"));
	iter = ranges.begin();
	while (iter != ranges.end()) {
		auto entry = iter->value();
		if (entry.first && !entry.second.present()) {
			tr.addReadConflictRange(singleKeyRange(failedServersVersionKey));
			tr.set(failedServersVersionKey, versionKey);
			tr.clear(ryw->getDatabase()->specialKeySpace->decode(iter->range()));
		}
		++iter;
	}
}

ACTOR Future<Optional<std::string>> excludeCommitActor(ReadYourWritesTransaction* ryw, bool failed) {
	// parse network addresses
	state Optional<std::string> result;
	state std::vector<AddressExclusion> addresses;
	state std::set<AddressExclusion> exclusions;
	if (!parseNetWorkAddrFromKeys(ryw, failed, addresses, exclusions, result)) return result;
	// If force option is not set, we need to do safety check
	auto force = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandOptionSpecialKey(
	    failed ? "failed" : "excluded", "force")];
	// only do safety check when we have servers to be excluded and the force option key is not set
	if (addresses.size() && !(force.first && force.second.present())) {
		bool safe = wait(checkExclusion(ryw->getDatabase(), &addresses, &exclusions, failed, &result));
		if (!safe) return result;
	}
	excludeServers(ryw->getTransaction(), addresses, failed);
	includeServers(ryw);

	return result;
}

Future<Optional<std::string>> ExcludeServersRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	return excludeCommitActor(ryw, false);
}

FailedServersRangeImpl::FailedServersRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> FailedServersRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                    KeyRangeRef kr) const {
	return rwModuleWithMappingGetRangeActor(ryw, this, kr);
}

void FailedServersRangeImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	// ignore value
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(ValueRef())));
}

Key FailedServersRangeImpl::decode(const KeyRef& key) const {
	return key.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
	    .withPrefix(LiteralStringRef("\xff/conf/"));
}

Key FailedServersRangeImpl::encode(const KeyRef& key) const {
	return key.removePrefix(LiteralStringRef("\xff/conf/"))
	    .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
}

Future<Optional<std::string>> FailedServersRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	return excludeCommitActor(ryw, true);
}

ACTOR Future<Standalone<RangeResultRef>> ExclusionInProgressActor(ReadYourWritesTransaction* ryw, KeyRef prefix,
                                                                  KeyRangeRef kr) {
	state Standalone<RangeResultRef> result;
	state Transaction& tr = ryw->getTransaction();
	tr.setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // necessary?
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);

	state std::vector<AddressExclusion> excl = wait((getExcludedServers(&tr)));
	state std::set<AddressExclusion> exclusions(excl.begin(), excl.end());
	state std::set<NetworkAddress> inProgressExclusion;
	// Just getting a consistent read version proves that a set of tlogs satisfying the exclusions has completed
	// recovery Check that there aren't any storage servers with addresses violating the exclusions
	state Standalone<RangeResultRef> serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
	ASSERT(!serverList.more && serverList.size() < CLIENT_KNOBS->TOO_MANY);

	for (auto& s : serverList) {
		auto addresses = decodeServerListValue(s.value).getKeyValues.getEndpoint().addresses;
		if (addressExcluded(exclusions, addresses.address)) {
			inProgressExclusion.insert(addresses.address);
		}
		if (addresses.secondaryAddress.present() && addressExcluded(exclusions, addresses.secondaryAddress.get())) {
			inProgressExclusion.insert(addresses.secondaryAddress.get());
		}
	}

	Optional<Standalone<StringRef>> value = wait(tr.get(logsKey));
	ASSERT(value.present());
	auto logs = decodeLogsValue(value.get());
	for (auto const& log : logs.first) {
		if (log.second == NetworkAddress() || addressExcluded(exclusions, log.second)) {
			inProgressExclusion.insert(log.second);
		}
	}
	for (auto const& log : logs.second) {
		if (log.second == NetworkAddress() || addressExcluded(exclusions, log.second)) {
			inProgressExclusion.insert(log.second);
		}
	}

	// sort and remove :tls
	std::set<std::string> inProgressAddresses;
	for (auto const& address : inProgressExclusion) {
		inProgressAddresses.insert(formatIpPort(address.ip, address.port));
	}

	for (auto const& address : inProgressAddresses) {
		Key addrKey = prefix.withSuffix(address);
		if (kr.contains(addrKey)) {
			result.push_back(result.arena(), KeyValueRef(addrKey, ValueRef()));
			result.arena().dependsOn(addrKey.arena());
		}
	}
	return result;
}

ExclusionInProgressRangeImpl::ExclusionInProgressRangeImpl(KeyRangeRef kr) : SpecialKeyRangeAsyncImpl(kr) {}

Future<Standalone<RangeResultRef>> ExclusionInProgressRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                          KeyRangeRef kr) const {
	return ExclusionInProgressActor(ryw, getKeyRange().begin, kr);
}

ACTOR Future<Standalone<RangeResultRef>> getProcessClassActor(ReadYourWritesTransaction* ryw, KeyRef prefix,
                                                              KeyRangeRef kr) {
	vector<ProcessData> _workers = wait(getWorkers(&ryw->getTransaction()));
	auto workers = _workers; // strip const
	// Note : the sort by string is anti intuition, ex. 1.1.1.1:11 < 1.1.1.1:5
	std::sort(workers.begin(), workers.end(), [](const ProcessData& lhs, const ProcessData& rhs) {
		return formatIpPort(lhs.address.ip, lhs.address.port) < formatIpPort(rhs.address.ip, rhs.address.port);
	});
	Standalone<RangeResultRef> result;
	for (auto& w : workers) {
		// exclude :tls in keys even the network addresss is TLS
		KeyRef k(prefix.withSuffix(formatIpPort(w.address.ip, w.address.port), result.arena()));
		if (kr.contains(k)) {
			ValueRef v(result.arena(), w.processClass.toString());
			result.push_back(result.arena(), KeyValueRef(k, v));
		}
	}
	return rywGetRange(ryw, kr, result);
}

ACTOR Future<Optional<std::string>> processClassCommitActor(ReadYourWritesTransaction* ryw, KeyRangeRef range) {
	// enable related options
	ryw->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	ryw->setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	vector<ProcessData> workers = wait(
	    getWorkers(&ryw->getTransaction())); // make sure we use the Transaction object to avoid used_during_commit()

	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
	auto iter = ranges.begin();
	while (iter != ranges.end()) {
		auto entry = iter->value();
		// only loop through (set) operation, (clear) not exist
		if (entry.first && entry.second.present()) {
			// parse network address
			Key address = iter->begin().removePrefix(range.begin);
			AddressExclusion addr = AddressExclusion::parse(address);
			// parse class type
			ValueRef processClassType = entry.second.get();
			ProcessClass processClass(processClassType.toString(), ProcessClass::DBSource);
			// make sure we use the underlying Transaction object to avoid used_during_commit()
			bool foundChange = false;
			for (int i = 0; i < workers.size(); i++) {
				if (addr.excludes(workers[i].address)) {
					if (processClass.classType() != ProcessClass::InvalidClass)
						ryw->getTransaction().set(processClassKeyFor(workers[i].locality.processId().get()),
						                          processClassValue(processClass));
					else
						ryw->getTransaction().clear(processClassKeyFor(workers[i].locality.processId().get()));
					foundChange = true;
				}
			}
			if (foundChange)
				ryw->getTransaction().set(processClassChangeKey, deterministicRandom()->randomUniqueID().toString());
		}
		++iter;
	}
	return Optional<std::string>();
}

ProcessClassRangeImpl::ProcessClassRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> ProcessClassRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                   KeyRangeRef kr) const {
	return getProcessClassActor(ryw, getKeyRange().begin, kr);
}

Future<Optional<std::string>> ProcessClassRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	// Validate network address and process class type
	Optional<std::string> errorMsg;
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(getKeyRange());
	auto iter = ranges.begin();
	while (iter != ranges.end()) {
		auto entry = iter->value();
		// only check for setclass(set) operation, (clear) are forbidden thus not exist
		if (entry.first && entry.second.present()) {
			// validate network address
			Key address = iter->begin().removePrefix(range.begin);
			AddressExclusion addr = AddressExclusion::parse(address);
			if (!addr.isValid()) {
				std::string error = "ERROR: \'" + address.toString() + "\' is not a valid network endpoint address\n";
				if (address.toString().find(":tls") != std::string::npos)
					error += "        Do not include the `:tls' suffix when naming a process\n";
				errorMsg = ManagementAPIError::toJsonString(false, "setclass", error);
				return errorMsg;
			}
			// validate class type
			ValueRef processClassType = entry.second.get();
			ProcessClass processClass(processClassType.toString(), ProcessClass::DBSource);
			if (processClass.classType() == ProcessClass::InvalidClass &&
			    processClassType != LiteralStringRef("default")) {
				std::string error = "ERROR: \'" + processClassType.toString() + "\' is not a valid process class\n";
				errorMsg = ManagementAPIError::toJsonString(false, "setclass", error);
				return errorMsg;
			}
		}
		++iter;
	}
	return processClassCommitActor(ryw, getKeyRange());
}

void throwSpecialKeyApiFailure(ReadYourWritesTransaction* ryw, std::string command, std::string message) {
	auto msg = ManagementAPIError::toJsonString(false, command, message);
	ryw->setSpecialKeySpaceErrorMsg(msg);
	throw special_keys_api_failure();
}

void ProcessClassRangeImpl::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	return throwSpecialKeyApiFailure(ryw, "setclass", "Clear operation is meaningless thus forbidden for setclass");
}

void ProcessClassRangeImpl::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	return throwSpecialKeyApiFailure(ryw, "setclass",
	                                 "Clear range operation is meaningless thus forbidden for setclass");
}

ACTOR Future<Standalone<RangeResultRef>> getProcessClassSourceActor(ReadYourWritesTransaction* ryw, KeyRef prefix,
                                                                    KeyRangeRef kr) {
	vector<ProcessData> _workers = wait(getWorkers(&ryw->getTransaction()));
	auto workers = _workers; // strip const
	// Note : the sort by string is anti intuition, ex. 1.1.1.1:11 < 1.1.1.1:5
	std::sort(workers.begin(), workers.end(), [](const ProcessData& lhs, const ProcessData& rhs) {
		return formatIpPort(lhs.address.ip, lhs.address.port) < formatIpPort(rhs.address.ip, rhs.address.port);
	});
	Standalone<RangeResultRef> result;
	for (auto& w : workers) {
		// exclude :tls in keys even the network addresss is TLS
		Key k(prefix.withSuffix(formatIpPort(w.address.ip, w.address.port)));
		if (kr.contains(k)) {
			Value v(w.processClass.sourceString());
			result.push_back(result.arena(), KeyValueRef(k, v));
			result.arena().dependsOn(k.arena());
			result.arena().dependsOn(v.arena());
		}
	}
	return result;
}

ProcessClassSourceRangeImpl::ProcessClassSourceRangeImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

Future<Standalone<RangeResultRef>> ProcessClassSourceRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                         KeyRangeRef kr) const {
	return getProcessClassSourceActor(ryw, getKeyRange().begin, kr);
}

ACTOR Future<Standalone<RangeResultRef>> getLockedKeyActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(ryw->getTransaction().get(databaseLockedKey));
	Standalone<RangeResultRef> result;
	if (val.present()) {
		result.push_back_deep(result.arena(), KeyValueRef(kr.begin, val.get()));
	}
	return result;
}

LockDatabaseImpl::LockDatabaseImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> LockDatabaseImpl::getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const {
	// single key range, the queried range should always be the same as the underlying range
	ASSERT(kr == getKeyRange());
	auto lockEntry = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("lock")];
	if (!ryw->readYourWritesDisabled() && lockEntry.first) {
		// ryw enabled and we have written to the special key
		Standalone<RangeResultRef> result;
		if (lockEntry.second.present()) {
			result.push_back_deep(result.arena(), KeyValueRef(kr.begin, lockEntry.second.get()));
		}
		return result;
	} else {
		return getLockedKeyActor(ryw, kr);
	}
}

ACTOR Future<Optional<std::string>> lockDatabaseCommitActor(ReadYourWritesTransaction* ryw) {
	state Optional<std::string> msg;
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(ryw->getTransaction().get(databaseLockedKey));
	UID uid = deterministicRandom()->randomUniqueID();

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != uid) {
		// check database not locked
		// if locked already, throw error
		msg = ManagementAPIError::toJsonString(false, "lock", "Database has already been locked");
	} else if (!val.present()) {
		// lock database
		ryw->getTransaction().atomicOp(databaseLockedKey,
		                               BinaryWriter::toValue(uid, Unversioned())
		                                   .withPrefix(LiteralStringRef("0123456789"))
		                                   .withSuffix(LiteralStringRef("\x00\x00\x00\x00")),
		                               MutationRef::SetVersionstampedValue);
		ryw->getTransaction().addWriteConflictRange(normalKeys);
	}

	return msg;
}

ACTOR Future<Optional<std::string>> unlockDatabaseCommitActor(ReadYourWritesTransaction* ryw) {
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	Optional<Value> val = wait(ryw->getTransaction().get(databaseLockedKey));
	if (val.present()) {
		ryw->getTransaction().clear(singleKeyRange(databaseLockedKey));
	}
	return Optional<std::string>();
}

Future<Optional<std::string>> LockDatabaseImpl::commit(ReadYourWritesTransaction* ryw) {
	auto lockId = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("lock")].second;
	if (lockId.present()) {
		return lockDatabaseCommitActor(ryw);
	} else {
		return unlockDatabaseCommitActor(ryw);
	}
}

ACTOR Future<Standalone<RangeResultRef>> getConsistencyCheckKeyActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	Optional<Value> val = wait(ryw->getTransaction().get(fdbShouldConsistencyCheckBeSuspended));
	bool ccSuspendSetting = val.present() ? BinaryReader::fromStringRef<bool>(val.get(), Unversioned()) : false;
	Standalone<RangeResultRef> result;
	if (ccSuspendSetting) {
		result.push_back_deep(result.arena(), KeyValueRef(kr.begin, ValueRef()));
	}
	return result;
}

ConsistencyCheckImpl::ConsistencyCheckImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<Standalone<RangeResultRef>> ConsistencyCheckImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                  KeyRangeRef kr) const {
	// single key range, the queried range should always be the same as the underlying range
	ASSERT(kr == getKeyRange());
	auto entry = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck")];
	if (!ryw->readYourWritesDisabled() && entry.first) {
		// ryw enabled and we have written to the special key
		Standalone<RangeResultRef> result;
		if (entry.second.present()) {
			result.push_back_deep(result.arena(), KeyValueRef(kr.begin, entry.second.get()));
		}
		return result;
	} else {
		return getConsistencyCheckKeyActor(ryw, kr);
	}
}

Future<Optional<std::string>> ConsistencyCheckImpl::commit(ReadYourWritesTransaction* ryw) {
	auto entry =
	    ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck")].second;
	ryw->getTransaction().setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().set(fdbShouldConsistencyCheckBeSuspended,
	                          BinaryWriter::toValue(entry.present(), Unversioned()));
	return Optional<std::string>();
}

TracingOptionsImpl::TracingOptionsImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {
	TraceEvent("TracingOptionsImpl::TracingOptionsImpl").detail("Range", kr);
}

Future<Standalone<RangeResultRef>> TracingOptionsImpl::getRange(ReadYourWritesTransaction* ryw,
                                                                KeyRangeRef kr) const {
	Standalone<RangeResultRef> result;
	for (const auto& option : SpecialKeySpace::getTracingOptions()) {
		auto key = getKeyRange().begin.withSuffix(option);
		if (!kr.contains(key)) {
			continue;
		}

		if (key.endsWith(kTracingTransactionIdKey)) {
			result.push_back_deep(result.arena(), KeyValueRef(key, std::to_string(ryw->getTransactionInfo().spanID.first())));
		} else if (key.endsWith(kTracingTokenKey)) {
			result.push_back_deep(result.arena(), KeyValueRef(key, std::to_string(ryw->getTransactionInfo().spanID.second())));
		}
	}
	return result;
}

void TracingOptionsImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	if (ryw->getApproximateSize() > 0) {
		ryw->setSpecialKeySpaceErrorMsg("tracing options must be set first");
		ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>()));
		return;
	}

	if (key.endsWith(kTracingTransactionIdKey)) {
		ryw->setTransactionID(std::stoul(value.toString()));
	} else if (key.endsWith(kTracingTokenKey)) {
		if (value.toString() == "true") {
			ryw->setToken(deterministicRandom()->randomUInt64());
		} else if (value.toString() == "false") {
			ryw->setToken(0);
		} else {
			ryw->setSpecialKeySpaceErrorMsg("token must be set to true/false");
			throw special_keys_api_failure();
		}
	}
}

Future<Optional<std::string>> TracingOptionsImpl::commit(ReadYourWritesTransaction* ryw) {
	if (ryw->getSpecialKeySpaceWriteMap().size() > 0) {
		throw special_keys_api_failure();
	}
	return Optional<std::string>();
}

void TracingOptionsImpl::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	ryw->setSpecialKeySpaceErrorMsg("clear range disabled");
	throw special_keys_api_failure();
}

void TracingOptionsImpl::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	ryw->setSpecialKeySpaceErrorMsg("clear disabled");
	throw special_keys_api_failure();
}
