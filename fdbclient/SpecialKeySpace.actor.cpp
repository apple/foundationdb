/*
 * SpecialKeySpace.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#include "boost/lexical_cast.hpp"
#include "boost/algorithm/string.hpp"

#include <time.h>
#include <msgpack.hpp>

#include <exception>

#include "fdbclient/ActorLineageProfiler.h"
#include "fdbclient/ClusterConnectionMemoryRecord.h"
#include "fdbclient/FDBOptions.g.h"
#include "fdbclient/Knobs.h"
#include "fdbclient/ProcessInterface.h"
#include "fdbclient/GlobalConfig.actor.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "flow/Arena.h"
#include "flow/UnitTest.h"
#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/StatusClient.h"
#include "flow/actorcompiler.h" // This must be the last #include.

namespace {
const std::string kTracingTransactionIdKey = "transaction_id";
const std::string kTracingTokenKey = "token";

static bool isAlphaNumeric(const std::string& key) {
	// [A-Za-z0-9_]+
	if (!key.size())
		return false;
	for (const char& c : key) {
		if (!((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'))
			return false;
	}
	return true;
}
} // namespace

const KeyRangeRef TenantMapRangeImpl::submoduleRange = KeyRangeRef("tenant_map/"_sr, "tenant_map0"_sr);

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
	{ SpecialKeySpace::MODULE::GLOBALCONFIG,
	  KeyRangeRef(LiteralStringRef("\xff\xff/global_config/"), LiteralStringRef("\xff\xff/global_config0")) },
	{ SpecialKeySpace::MODULE::TRACING,
	  KeyRangeRef(LiteralStringRef("\xff\xff/tracing/"), LiteralStringRef("\xff\xff/tracing0")) },
	{ SpecialKeySpace::MODULE::ACTORLINEAGE,
	  KeyRangeRef(LiteralStringRef("\xff\xff/actor_lineage/"), LiteralStringRef("\xff\xff/actor_lineage0")) },
	{ SpecialKeySpace::MODULE::ACTOR_PROFILER_CONF,
	  KeyRangeRef(LiteralStringRef("\xff\xff/actor_profiler_conf/"),
	              LiteralStringRef("\xff\xff/actor_profiler_conf0")) }
};

std::unordered_map<std::string, KeyRange> SpecialKeySpace::managementApiCommandToRange = {
	{ "exclude",
	  KeyRangeRef(LiteralStringRef("excluded/"), LiteralStringRef("excluded0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "failed",
	  KeyRangeRef(LiteralStringRef("failed/"), LiteralStringRef("failed0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "excludedlocality",
	  KeyRangeRef(LiteralStringRef("excluded_locality/"), LiteralStringRef("excluded_locality0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "failedlocality",
	  KeyRangeRef(LiteralStringRef("failed_locality/"), LiteralStringRef("failed_locality0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "lock", singleKeyRange(LiteralStringRef("db_locked")).withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "consistencycheck",
	  singleKeyRange(LiteralStringRef("consistency_check_suspended"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "coordinators",
	  KeyRangeRef(LiteralStringRef("coordinators/"), LiteralStringRef("coordinators0"))
	      .withPrefix(moduleToBoundary[MODULE::CONFIGURATION].begin) },
	{ "advanceversion",
	  singleKeyRange(LiteralStringRef("min_required_commit_version"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "profile",
	  KeyRangeRef(LiteralStringRef("profiling/"), LiteralStringRef("profiling0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "maintenance",
	  KeyRangeRef(LiteralStringRef("maintenance/"), LiteralStringRef("maintenance0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "datadistribution",
	  KeyRangeRef(LiteralStringRef("data_distribution/"), LiteralStringRef("data_distribution0"))
	      .withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) },
	{ "tenantmap", TenantMapRangeImpl::submoduleRange.withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin) }
};

std::unordered_map<std::string, KeyRange> SpecialKeySpace::actorLineageApiCommandToRange = {
	{ "state",
	  KeyRangeRef(LiteralStringRef("state/"), LiteralStringRef("state0"))
	      .withPrefix(moduleToBoundary[MODULE::ACTORLINEAGE].begin) },
	{ "time",
	  KeyRangeRef(LiteralStringRef("time/"), LiteralStringRef("time0"))
	      .withPrefix(moduleToBoundary[MODULE::ACTORLINEAGE].begin) }
};

std::set<std::string> SpecialKeySpace::options = { "excluded/force",
	                                               "failed/force",
	                                               "excluded_locality/force",
	                                               "failed_locality/force" };

std::set<std::string> SpecialKeySpace::tracingOptions = { kTracingTransactionIdKey, kTracingTokenKey };

RangeResult rywGetRange(ReadYourWritesTransaction* ryw, const KeyRangeRef& kr, const RangeResult& res);

// This function will move the given KeySelector as far as possible to the standard form:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is not in the underlying key range, it will move over the range
// The cache object is used to cache the first read result from the rpc call during the key resolution,
// then when we need to do key resolution or result filtering,
// we, instead of rpc call, read from this cache object have consistent results
ACTOR Future<Void> moveKeySelectorOverRangeActor(const SpecialKeyRangeReadImpl* skrImpl,
                                                 ReadYourWritesTransaction* ryw,
                                                 KeySelector* ks,
                                                 Optional<RangeResult>* cache) {
	// should be removed before calling
	ASSERT(!ks->orEqual);

	// never being called if KeySelector is already normalized
	ASSERT(ks->offset != 1);

	state Key startKey(skrImpl->getKeyRange().begin);
	state Key endKey(skrImpl->getKeyRange().end);
	state RangeResult result;

	if (ks->offset < 1) {
		// less than the given key
		if (skrImpl->getKeyRange().contains(ks->getKey()))
			endKey = ks->getKey();
	} else {
		// greater than the given key
		if (skrImpl->getKeyRange().contains(ks->getKey()))
			startKey = ks->getKey();
	}

	// Note : startKey never equals endKey here
	ASSERT(startKey < endKey);

	TraceEvent(SevDebug, "NormalizeKeySelector")
	    .detail("OriginalKey", ks->getKey())
	    .detail("OriginalOffset", ks->offset)
	    .detail("SpecialKeyRangeStart", skrImpl->getKeyRange().begin)
	    .detail("SpecialKeyRangeEnd", skrImpl->getKeyRange().end);

	GetRangeLimits limitsHint(ks->offset >= 1 ? ks->offset : 1 - ks->offset);

	if (skrImpl->isAsync()) {
		const SpecialKeyRangeAsyncImpl* ptr = dynamic_cast<const SpecialKeyRangeAsyncImpl*>(skrImpl);
		RangeResult result_ = wait(ptr->getRange(ryw, KeyRangeRef(startKey, endKey), limitsHint, cache));
		result = result_;
	} else {
		RangeResult result_ = wait(skrImpl->getRange(ryw, KeyRangeRef(startKey, endKey), limitsHint));
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
			// TODO : the keyAfter will just return if key == \xff\xff
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

// This function will normalize the given KeySelector to a standard KeySelector:
// orEqual == false && offset == 1 (Standard form)
// If the corresponding key is outside the whole space, it will move to the begin or the end
// It does have overhead here since we query all keys twice in the worst case.
// However, moving the KeySelector while handling other parameters like limits makes the code much more complex and hard
// to maintain; Thus, separate each part to make the code easy to understand and more compact
// Boundary is the range of the legal key space, which, by default is the range of the module
// And (\xff\xff, \xff\xff\xff) if SPECIAL_KEY_SPACE_RELAXED is turned on
ACTOR Future<Void> normalizeKeySelectorActor(SpecialKeySpace* sks,
                                             ReadYourWritesTransaction* ryw,
                                             KeySelector* ks,
                                             KeyRangeRef boundary,
                                             int* actualOffset,
                                             RangeResult* result,
                                             Optional<RangeResult>* cache) {
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
		} else {
			result->readThroughEnd = true;
			ks->setKey(boundary.end);
		}
		ks->offset = 1;
	}
	return Void();
}

SpecialKeySpace::SpecialKeySpace(KeyRef spaceStartKey, KeyRef spaceEndKey, bool testOnly)
  : readImpls(nullptr, spaceEndKey),
    modules(testOnly ? SpecialKeySpace::MODULE::TESTONLY : SpecialKeySpace::MODULE::UNKNOWN, spaceEndKey),
    writeImpls(nullptr, spaceEndKey), range(KeyRangeRef(spaceStartKey, spaceEndKey)) {
	// Default begin of KeyRangeMap is Key(), insert the range to update start key
	readImpls.insert(range, nullptr);
	writeImpls.insert(range, nullptr);
	if (!testOnly) {
		// testOnly is used in the correctness workload
		modulesBoundaryInit();
	}
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

ACTOR Future<RangeResult> SpecialKeySpace::checkRYWValid(SpecialKeySpace* sks,
                                                         ReadYourWritesTransaction* ryw,
                                                         KeySelector begin,
                                                         KeySelector end,
                                                         GetRangeLimits limits,
                                                         Reverse reverse) {
	ASSERT(ryw);
	choose {
		when(RangeResult result =
		         wait(SpecialKeySpace::getRangeAggregationActor(sks, ryw, begin, end, limits, reverse))) {
			return result;
		}
		when(wait(ryw->resetFuture())) { throw internal_error(); }
	}
}

ACTOR Future<RangeResult> SpecialKeySpace::getRangeAggregationActor(SpecialKeySpace* sks,
                                                                    ReadYourWritesTransaction* ryw,
                                                                    KeySelector begin,
                                                                    KeySelector end,
                                                                    GetRangeLimits limits,
                                                                    Reverse reverse) {
	// This function handles ranges which cover more than one keyrange and aggregates all results
	// KeySelector, GetRangeLimits and reverse are all handled here
	state RangeResult result;
	state RangeResult pairs;
	state RangeMap<Key, SpecialKeyRangeReadImpl*, KeyRangeRef>::iterator iter;
	state int actualBeginOffset;
	state int actualEndOffset;
	state KeyRangeRef moduleBoundary;
	// used to cache result from potential first read
	state Optional<RangeResult> cache;

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
			    .detail("Begin", begin)
			    .detail("End", end)
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
			if (iter->value() == nullptr)
				continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			if (iter->value()->isAsync() && cache.present()) {
				const SpecialKeyRangeAsyncImpl* ptr = dynamic_cast<const SpecialKeyRangeAsyncImpl*>(iter->value());
				RangeResult pairs_ = wait(ptr->getRange(ryw, KeyRangeRef(keyStart, keyEnd), limits, &cache));
				pairs = pairs_;
			} else {
				RangeResult pairs_ = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd), limits));
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
			if (iter->value() == nullptr)
				continue;
			KeyRangeRef kr = iter->range();
			KeyRef keyStart = kr.contains(begin.getKey()) ? begin.getKey() : kr.begin;
			KeyRef keyEnd = kr.contains(end.getKey()) ? end.getKey() : kr.end;
			if (iter->value()->isAsync() && cache.present()) {
				const SpecialKeyRangeAsyncImpl* ptr = dynamic_cast<const SpecialKeyRangeAsyncImpl*>(iter->value());
				RangeResult pairs_ = wait(ptr->getRange(ryw, KeyRangeRef(keyStart, keyEnd), limits, &cache));
				pairs = pairs_;
			} else {
				RangeResult pairs_ = wait(iter->value()->getRange(ryw, KeyRangeRef(keyStart, keyEnd), limits));
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

Future<RangeResult> SpecialKeySpace::getRange(ReadYourWritesTransaction* ryw,
                                              KeySelector begin,
                                              KeySelector end,
                                              GetRangeLimits limits,
                                              Reverse reverse) {
	// validate limits here
	if (!limits.isValid())
		return range_limits_invalid();
	if (limits.isReached()) {
		TEST(true); // read limit 0
		return RangeResult();
	}
	// make sure orEqual == false
	begin.removeOrEqual(begin.arena());
	end.removeOrEqual(end.arena());

	if (begin.offset >= end.offset && begin.getKey() >= end.getKey()) {
		TEST(true); // range inverted
		return RangeResult();
	}

	return checkRYWValid(this, ryw, begin, end, limits, reverse);
}

ACTOR Future<Optional<Value>> SpecialKeySpace::getActor(SpecialKeySpace* sks,
                                                        ReadYourWritesTransaction* ryw,
                                                        KeyRef key) {
	// use getRange to workaround this
	RangeResult result = wait(sks->getRange(ryw,
	                                        KeySelector(firstGreaterOrEqual(key)),
	                                        KeySelector(firstGreaterOrEqual(keyAfter(key))),
	                                        GetRangeLimits(CLIENT_KNOBS->TOO_MANY),
	                                        Reverse::False));
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
	if (!ryw->specialKeySpaceChangeConfiguration())
		throw special_keys_write_disabled();
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
	if (!ryw->specialKeySpaceChangeConfiguration())
		throw special_keys_write_disabled();
	if (range.empty())
		return;
	auto begin = writeImpls[range.begin];
	auto end = writeImpls.rangeContainingKeyBefore(range.end)->value();
	if (begin != end) {
		TraceEvent(SevDebug, "SpecialKeySpaceCrossModuleClear").detail("Range", range);
		throw special_keys_cross_module_clear(); // ban cross module clear
	} else if (begin == nullptr) {
		TraceEvent(SevDebug, "SpecialKeySpaceNoWriteModuleFound").detail("Range", range);
		throw special_keys_no_write_module_found();
	}
	return begin->clear(ryw, range);
}

void SpecialKeySpace::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	if (!ryw->specialKeySpaceChangeConfiguration())
		throw special_keys_write_disabled();
	auto impl = writeImpls[key];
	if (impl == nullptr)
		throw special_keys_no_write_module_found();
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

void SpecialKeySpace::registerKeyRange(SpecialKeySpace::MODULE module,
                                       SpecialKeySpace::IMPLTYPE type,
                                       const KeyRangeRef& kr,
                                       SpecialKeyRangeReadImpl* impl) {
	// module boundary check
	if (module == SpecialKeySpace::MODULE::TESTONLY) {
		ASSERT(normalKeys.contains(kr));
	} else {
		ASSERT(moduleToBoundary.at(module).contains(kr));
		// validate keys follow snake case naming style
		ASSERT(validateSnakeCaseNaming(kr.begin) && validateSnakeCaseNaming(kr.end));
	}
	// make sure the registered range is not overlapping with existing ones
	// Note: kr.end should not be the same as another range's begin, although it should work even they are the same
	for (auto iter = readImpls.rangeContaining(kr.begin); true; ++iter) {
		ASSERT(iter->value() == nullptr);
		if (iter == readImpls.rangeContaining(kr.end)) {
			// Note: relax the condition that the end can be another range's start, if needed
			break;
		}
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
	state std::vector<SpecialKeyRangeRWImpl*> writeModulePtrs;
	std::unordered_set<SpecialKeyRangeRWImpl*> deduplicate;
	while (iter != ranges.end()) {
		std::pair<bool, Optional<Value>> entry = iter->value();
		if (entry.first) {
			auto modulePtr = sks->getRWImpls().rangeContaining(iter->begin())->value();
			auto [_, inserted] = deduplicate.insert(modulePtr);
			if (inserted) {
				writeModulePtrs.push_back(modulePtr);
			}
		}
		++iter;
	}
	state std::vector<SpecialKeyRangeRWImpl*>::const_iterator it;
	for (it = writeModulePtrs.begin(); it != writeModulePtrs.end(); ++it) {
		Optional<std::string> msg = wait((*it)->commit(ryw));
		if (msg.present()) {
			ryw->setSpecialKeySpaceErrorMsg(msg.get());
			TraceEvent(SevDebug, "SpecialKeySpaceManagementAPIError")
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

Future<RangeResult> SKSCTestImpl::getRange(ReadYourWritesTransaction* ryw,
                                           KeyRangeRef kr,
                                           GetRangeLimits limitsHint) const {
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

ACTOR static Future<RangeResult> getReadConflictRangeImpl(ReadYourWritesTransaction* ryw, KeyRange kr) {
	wait(ryw->pendingReads());
	return ryw->getReadConflictRangeIntersecting(kr);
}

Future<RangeResult> ReadConflictRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                    KeyRangeRef kr,
                                                    GetRangeLimits limitsHint) const {
	return getReadConflictRangeImpl(ryw, kr);
}

WriteConflictRangeImpl::WriteConflictRangeImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

Future<RangeResult> WriteConflictRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                     KeyRangeRef kr,
                                                     GetRangeLimits limitsHint) const {
	return ryw->getWriteConflictRangeIntersecting(kr);
}

ConflictingKeysImpl::ConflictingKeysImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

Future<RangeResult> ConflictingKeysImpl::getRange(ReadYourWritesTransaction* ryw,
                                                  KeyRangeRef kr,
                                                  GetRangeLimits limitsHint) const {
	RangeResult result;
	if (ryw->getTransactionState()->conflictingKeys) {
		auto krMapPtr = ryw->getTransactionState()->conflictingKeys.get();
		auto beginIter = krMapPtr->rangeContaining(kr.begin);
		if (beginIter->begin() != kr.begin)
			++beginIter;
		auto endIter = krMapPtr->rangeContaining(kr.end);
		for (auto it = beginIter; it != endIter; ++it) {
			result.push_back_deep(result.arena(), KeyValueRef(it->begin(), it->value()));
		}
		if (endIter->begin() != kr.end)
			result.push_back_deep(result.arena(), KeyValueRef(endIter->begin(), endIter->value()));
	}
	return result;
}

ACTOR Future<RangeResult> ddMetricsGetRangeActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	loop {
		try {
			auto keys = kr.removePrefix(ddStatsRange.begin);
			Standalone<VectorRef<DDMetricsRef>> resultWithoutPrefix = wait(
			    waitDataDistributionMetricsList(ryw->getDatabase(), keys, CLIENT_KNOBS->STORAGE_METRICS_SHARD_LIMIT));
			RangeResult result;
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

Future<RangeResult> DDStatsRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                               KeyRangeRef kr,
                                               GetRangeLimits limitsHint) const {
	return ddMetricsGetRangeActor(ryw, kr);
}

Key SpecialKeySpace::getManagementApiCommandOptionSpecialKey(const std::string& command, const std::string& option) {
	Key prefix = LiteralStringRef("options/").withPrefix(moduleToBoundary[MODULE::MANAGEMENT].begin);
	auto pair = command + "/" + option;
	ASSERT(options.find(pair) != options.end());
	return prefix.withSuffix(pair);
}

ManagementCommandsOptionsImpl::ManagementCommandsOptionsImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> ManagementCommandsOptionsImpl::getRange(ReadYourWritesTransaction* ryw,
                                                            KeyRangeRef kr,
                                                            GetRangeLimits limitsHint) const {
	RangeResult result;
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

RangeResult rywGetRange(ReadYourWritesTransaction* ryw, const KeyRangeRef& kr, const RangeResult& res) {
	// "res" is the read result regardless of your writes, if ryw disabled, return immediately
	if (ryw->readYourWritesDisabled())
		return res;
	// If ryw enabled, we update it with writes from the transaction
	RangeResult result;
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
				while (iter2 != res.end() && iter->range().contains(iter2->key))
					++iter2;
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
ACTOR Future<RangeResult> rwModuleWithMappingGetRangeActor(ReadYourWritesTransaction* ryw,
                                                           const SpecialKeyRangeRWImpl* impl,
                                                           KeyRangeRef kr) {
	RangeResult resultWithoutPrefix =
	    wait(ryw->getTransaction().getRange(ryw->getDatabase()->specialKeySpace->decode(kr), CLIENT_KNOBS->TOO_MANY));
	ASSERT(!resultWithoutPrefix.more && resultWithoutPrefix.size() < CLIENT_KNOBS->TOO_MANY);
	RangeResult result;
	for (const KeyValueRef& kv : resultWithoutPrefix)
		result.push_back_deep(result.arena(), KeyValueRef(impl->encode(kv.key), kv.value));
	return rywGetRange(ryw, kr, result);
}

ExcludeServersRangeImpl::ExcludeServersRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> ExcludeServersRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                      KeyRangeRef kr,
                                                      GetRangeLimits limitsHint) const {
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
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

bool parseNetWorkAddrFromKeys(ReadYourWritesTransaction* ryw,
                              bool failed,
                              std::vector<AddressExclusion>& addresses,
                              std::set<AddressExclusion>& exclusions,
                              Optional<std::string>& msg) {
	KeyRangeRef range = failed ? SpecialKeySpace::getManagementApiCommandRange("failed")
	                           : SpecialKeySpace::getManagementApiCommandRange("exclude");
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

ACTOR Future<bool> checkExclusion(Database db,
                                  std::vector<AddressExclusion>* addresses,
                                  std::set<AddressExclusion>* exclusions,
                                  bool markFailed,
                                  Optional<std::string>* msg) {

	if (markFailed) {
		state bool safe;
		try {
			bool _safe = wait(checkSafeExclusions(db, *addresses));
			safe = _safe;
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
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
			if (!storageServer)
				continue;

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
			if (excluded)
				ssExcludedCount++;

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
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	// includeServers might be used in an emergency transaction, so make sure it is retry-self-conflicting and
	// CAUSAL_WRITE_RISKY
	ryw->setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
	std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	// for exluded servers
	auto ranges =
	    ryw->getSpecialKeySpaceWriteMap().containedRanges(SpecialKeySpace::getManagementApiCommandRange("exclude"));
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
	ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(SpecialKeySpace::getManagementApiCommandRange("failed"));
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
	if (!parseNetWorkAddrFromKeys(ryw, failed, addresses, exclusions, result))
		return result;
	// If force option is not set, we need to do safety check
	auto force = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandOptionSpecialKey(
	    failed ? "failed" : "excluded", "force")];
	// only do safety check when we have servers to be excluded and the force option key is not set
	if (addresses.size() && !(force.first && force.second.present())) {
		bool safe = wait(checkExclusion(ryw->getDatabase(), &addresses, &exclusions, failed, &result));
		if (!safe)
			return result;
	}
	excludeServers(ryw->getTransaction(), addresses, failed);
	includeServers(ryw);

	return result;
}

Future<Optional<std::string>> ExcludeServersRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	return excludeCommitActor(ryw, false);
}

FailedServersRangeImpl::FailedServersRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> FailedServersRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                     KeyRangeRef kr,
                                                     GetRangeLimits limitsHint) const {
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
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

ACTOR Future<RangeResult> ExclusionInProgressActor(ReadYourWritesTransaction* ryw, KeyRef prefix, KeyRangeRef kr) {
	state RangeResult result;
	state Transaction& tr = ryw->getTransaction();
	tr.setOption(FDBTransactionOptions::RAW_ACCESS);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE); // necessary?
	tr.setOption(FDBTransactionOptions::LOCK_AWARE);

	state std::vector<AddressExclusion> excl = wait((getExcludedServers(&tr)));
	state std::set<AddressExclusion> exclusions(excl.begin(), excl.end());
	state std::set<NetworkAddress> inProgressExclusion;
	// Just getting a consistent read version proves that a set of tlogs satisfying the exclusions has completed
	// recovery Check that there aren't any storage servers with addresses violating the exclusions
	state RangeResult serverList = wait(tr.getRange(serverListKeys, CLIENT_KNOBS->TOO_MANY));
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

Future<RangeResult> ExclusionInProgressRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                           KeyRangeRef kr,
                                                           GetRangeLimits limitsHint) const {
	return ExclusionInProgressActor(ryw, getKeyRange().begin, kr);
}

ACTOR Future<RangeResult> getProcessClassActor(ReadYourWritesTransaction* ryw, KeyRef prefix, KeyRangeRef kr) {
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	std::vector<ProcessData> _workers = wait(getWorkers(&ryw->getTransaction()));
	auto workers = _workers; // strip const
	// Note : the sort by string is anti intuition, ex. 1.1.1.1:11 < 1.1.1.1:5
	std::sort(workers.begin(), workers.end(), [](const ProcessData& lhs, const ProcessData& rhs) {
		return formatIpPort(lhs.address.ip, lhs.address.port) < formatIpPort(rhs.address.ip, rhs.address.port);
	});
	RangeResult result;
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
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	std::vector<ProcessData> workers = wait(
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

Future<RangeResult> ProcessClassRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                    KeyRangeRef kr,
                                                    GetRangeLimits limitsHint) const {
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
	return throwSpecialKeyApiFailure(
	    ryw, "setclass", "Clear range operation is meaningless thus forbidden for setclass");
}

ACTOR Future<RangeResult> getProcessClassSourceActor(ReadYourWritesTransaction* ryw, KeyRef prefix, KeyRangeRef kr) {
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	std::vector<ProcessData> _workers = wait(getWorkers(&ryw->getTransaction()));
	auto workers = _workers; // strip const
	// Note : the sort by string is anti intuition, ex. 1.1.1.1:11 < 1.1.1.1:5
	std::sort(workers.begin(), workers.end(), [](const ProcessData& lhs, const ProcessData& rhs) {
		return formatIpPort(lhs.address.ip, lhs.address.port) < formatIpPort(rhs.address.ip, rhs.address.port);
	});
	RangeResult result;
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

Future<RangeResult> ProcessClassSourceRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                          KeyRangeRef kr,
                                                          GetRangeLimits limitsHint) const {
	return getProcessClassSourceActor(ryw, getKeyRange().begin, kr);
}

ACTOR Future<RangeResult> getLockedKeyActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<Value> val = wait(ryw->getTransaction().get(databaseLockedKey));
	RangeResult result;
	if (val.present()) {
		UID uid = UID::fromString(BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()).toString());
		result.push_back_deep(result.arena(), KeyValueRef(kr.begin, Value(uid.toString())));
	}
	return result;
}

LockDatabaseImpl::LockDatabaseImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> LockDatabaseImpl::getRange(ReadYourWritesTransaction* ryw,
                                               KeyRangeRef kr,
                                               GetRangeLimits limitsHint) const {
	// single key range, the queried range should always be the same as the underlying range
	ASSERT(kr == getKeyRange());
	auto lockEntry = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("lock")];
	if (!ryw->readYourWritesDisabled() && lockEntry.first) {
		// ryw enabled and we have written to the special key
		RangeResult result;
		if (lockEntry.second.present()) {
			result.push_back_deep(result.arena(), KeyValueRef(kr.begin, lockEntry.second.get()));
		}
		return result;
	} else {
		return getLockedKeyActor(ryw, kr);
	}
}

ACTOR Future<Optional<std::string>> lockDatabaseCommitActor(ReadYourWritesTransaction* ryw, UID uid) {
	state Optional<std::string> msg;
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<Value> val = wait(ryw->getTransaction().get(databaseLockedKey));

	if (val.present() && BinaryReader::fromStringRef<UID>(val.get().substr(10), Unversioned()) != uid) {
		// check database not locked
		// if locked already, throw error
		throw database_locked();
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
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<Value> val = wait(ryw->getTransaction().get(databaseLockedKey));
	if (val.present()) {
		ryw->getTransaction().clear(singleKeyRange(databaseLockedKey));
	}
	return Optional<std::string>();
}

Future<Optional<std::string>> LockDatabaseImpl::commit(ReadYourWritesTransaction* ryw) {
	auto lockId = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("lock")].second;
	if (lockId.present()) {
		std::string uidStr = lockId.get().toString();
		UID uid;
		try {
			uid = UID::fromString(uidStr);
		} catch (Error& e) {
			return Optional<std::string>(
			    ManagementAPIError::toJsonString(false, "lock", "Invalid UID hex string: " + uidStr));
		}
		return lockDatabaseCommitActor(ryw, uid);
	} else {
		return unlockDatabaseCommitActor(ryw);
	}
}

ACTOR Future<RangeResult> getConsistencyCheckKeyActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	ryw->getTransaction().setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	Optional<Value> val = wait(ryw->getTransaction().get(fdbShouldConsistencyCheckBeSuspended));
	bool ccSuspendSetting = val.present() ? BinaryReader::fromStringRef<bool>(val.get(), Unversioned()) : false;
	RangeResult result;
	if (ccSuspendSetting) {
		result.push_back_deep(result.arena(), KeyValueRef(kr.begin, ValueRef()));
	}
	return result;
}

ConsistencyCheckImpl::ConsistencyCheckImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> ConsistencyCheckImpl::getRange(ReadYourWritesTransaction* ryw,
                                                   KeyRangeRef kr,
                                                   GetRangeLimits limitsHint) const {
	// single key range, the queried range should always be the same as the underlying range
	ASSERT(kr == getKeyRange());
	auto entry = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck")];
	if (!ryw->readYourWritesDisabled() && entry.first) {
		// ryw enabled and we have written to the special key
		RangeResult result;
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
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	ryw->getTransaction().set(fdbShouldConsistencyCheckBeSuspended,
	                          BinaryWriter::toValue(entry.present(), Unversioned()));
	return Optional<std::string>();
}

GlobalConfigImpl::GlobalConfigImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

// Returns key-value pairs for each value stored in the global configuration
// framework within the range specified. The special-key-space getrange
// function should only be used for informational purposes. All values are
// returned as strings regardless of their true type.
Future<RangeResult> GlobalConfigImpl::getRange(ReadYourWritesTransaction* ryw,
                                               KeyRangeRef kr,
                                               GetRangeLimits limitsHint) const {
	RangeResult result;

	auto& globalConfig = GlobalConfig::globalConfig();
	KeyRangeRef modified =
	    KeyRangeRef(kr.begin.removePrefix(getKeyRange().begin), kr.end.removePrefix(getKeyRange().begin));
	std::map<KeyRef, Reference<ConfigValue>> values = globalConfig.get(modified);
	for (const auto& [key, config] : values) {
		Key prefixedKey = key.withPrefix(getKeyRange().begin);
		if (config.isValid() && config->value.has_value()) {
			if (config->value.type() == typeid(StringRef)) {
				result.push_back_deep(result.arena(),
				                      KeyValueRef(prefixedKey, std::any_cast<StringRef>(config->value).toString()));
			} else if (config->value.type() == typeid(int64_t)) {
				result.push_back_deep(result.arena(),
				                      KeyValueRef(prefixedKey, std::to_string(std::any_cast<int64_t>(config->value))));
			} else if (config->value.type() == typeid(bool)) {
				result.push_back_deep(result.arena(),
				                      KeyValueRef(prefixedKey, std::to_string(std::any_cast<bool>(config->value))));
			} else if (config->value.type() == typeid(float)) {
				result.push_back_deep(result.arena(),
				                      KeyValueRef(prefixedKey, std::to_string(std::any_cast<float>(config->value))));
			} else if (config->value.type() == typeid(double)) {
				result.push_back_deep(result.arena(),
				                      KeyValueRef(prefixedKey, std::to_string(std::any_cast<double>(config->value))));
			} else {
				ASSERT(false);
			}
		}
	}

	return result;
}

// Marks the key for insertion into global configuration.
void GlobalConfigImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(value)));
}

// Writes global configuration changes to durable memory. Also writes the
// changes made in the transaction to a recent history set, and updates the
// latest version which the global configuration was updated at.
ACTOR Future<Optional<std::string>> globalConfigCommitActor(GlobalConfigImpl* globalConfig,
                                                            ReadYourWritesTransaction* ryw) {
	state Transaction& tr = ryw->getTransaction();
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);

	// History should only contain three most recent updates. If it currently
	// has three items, remove the oldest to make room for a new item.
	RangeResult history = wait(tr.getRange(globalConfigHistoryKeys, CLIENT_KNOBS->TOO_MANY));
	constexpr int kGlobalConfigMaxHistorySize = 3;
	if (history.size() > kGlobalConfigMaxHistorySize - 1) {
		for (int i = 0; i < history.size() - (kGlobalConfigMaxHistorySize - 1); ++i) {
			tr.clear(history[i].key);
		}
	}

	VersionHistory vh{ 0 };

	// Transform writes from the special-key-space (\xff\xff/global_config/) to
	// the system key space (\xff/globalConfig/), and writes mutations to
	// latest version history.
	state RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::Ranges ranges =
	    ryw->getSpecialKeySpaceWriteMap().containedRanges(specialKeys);
	state RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::iterator iter = ranges.begin();
	while (iter != ranges.end()) {
		std::pair<bool, Optional<Value>> entry = iter->value();
		if (entry.first) {
			if (entry.second.present() && iter->begin().startsWith(globalConfig->getKeyRange().begin)) {
				Key bareKey = iter->begin().removePrefix(globalConfig->getKeyRange().begin);
				vh.mutations.emplace_back_deep(vh.mutations.arena(),
				                               MutationRef(MutationRef::SetValue, bareKey, entry.second.get()));

				Key systemKey = bareKey.withPrefix(globalConfigKeysPrefix);
				tr.set(systemKey, entry.second.get());
			} else if (!entry.second.present() && iter->range().begin.startsWith(globalConfig->getKeyRange().begin) &&
			           iter->range().end.startsWith(globalConfig->getKeyRange().begin)) {
				KeyRef bareRangeBegin = iter->range().begin.removePrefix(globalConfig->getKeyRange().begin);
				KeyRef bareRangeEnd = iter->range().end.removePrefix(globalConfig->getKeyRange().begin);
				vh.mutations.emplace_back_deep(vh.mutations.arena(),
				                               MutationRef(MutationRef::ClearRange, bareRangeBegin, bareRangeEnd));

				Key systemRangeBegin = bareRangeBegin.withPrefix(globalConfigKeysPrefix);
				Key systemRangeEnd = bareRangeEnd.withPrefix(globalConfigKeysPrefix);
				tr.clear(KeyRangeRef(systemRangeBegin, systemRangeEnd));
			}
		}
		++iter;
	}

	// Record the mutations in this commit into the global configuration history.
	Key historyKey = addVersionStampAtEnd(globalConfigHistoryPrefix);
	ObjectWriter historyWriter(IncludeVersion());
	historyWriter.serialize(vh);
	tr.atomicOp(historyKey, historyWriter.toStringRef(), MutationRef::SetVersionstampedKey);

	// Write version key to trigger update in cluster controller.
	tr.atomicOp(globalConfigVersionKey,
	            LiteralStringRef("0123456789\x00\x00\x00\x00"), // versionstamp
	            MutationRef::SetVersionstampedValue);

	return Optional<std::string>();
}

// Called when a transaction includes keys in the global configuration special-key-space range.
Future<Optional<std::string>> GlobalConfigImpl::commit(ReadYourWritesTransaction* ryw) {
	return globalConfigCommitActor(this, ryw);
}

// Marks the range for deletion from global configuration.
void GlobalConfigImpl::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	ryw->getSpecialKeySpaceWriteMap().insert(range, std::make_pair(true, Optional<Value>()));
}

// Marks the key for deletion from global configuration.
void GlobalConfigImpl::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>()));
}

TracingOptionsImpl::TracingOptionsImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> TracingOptionsImpl::getRange(ReadYourWritesTransaction* ryw,
                                                 KeyRangeRef kr,
                                                 GetRangeLimits limitsHint) const {
	RangeResult result;
	for (const auto& option : SpecialKeySpace::getTracingOptions()) {
		auto key = getKeyRange().begin.withSuffix(option);
		if (!kr.contains(key)) {
			continue;
		}

		if (key.endsWith(kTracingTransactionIdKey)) {
			result.push_back_deep(result.arena(),
			                      KeyValueRef(key, std::to_string(ryw->getTransactionState()->spanID.first())));
		} else if (key.endsWith(kTracingTokenKey)) {
			result.push_back_deep(result.arena(),
			                      KeyValueRef(key, std::to_string(ryw->getTransactionState()->spanID.second())));
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

CoordinatorsImpl::CoordinatorsImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> CoordinatorsImpl::getRange(ReadYourWritesTransaction* ryw,
                                               KeyRangeRef kr,
                                               GetRangeLimits limitsHint) const {
	RangeResult result;
	KeyRef prefix(getKeyRange().begin);
	auto cs = ryw->getDatabase()->getConnectionRecord()->getConnectionString();
	auto coordinator_processes = cs.coordinators();
	Key cluster_decription_key = prefix.withSuffix(LiteralStringRef("cluster_description"));
	if (kr.contains(cluster_decription_key)) {
		result.push_back_deep(result.arena(), KeyValueRef(cluster_decription_key, cs.clusterKeyName()));
	}
	// Note : the sort by string is anti intuition, ex. 1.1.1.1:11 < 1.1.1.1:5
	// include :tls in keys if the network addresss is TLS
	std::sort(coordinator_processes.begin(),
	          coordinator_processes.end(),
	          [](const NetworkAddress& lhs, const NetworkAddress& rhs) { return lhs.toString() < rhs.toString(); });
	std::string processes_str;
	for (const auto& w : coordinator_processes) {
		if (processes_str.size())
			processes_str += ",";
		processes_str += w.toString();
	}
	Key processes_key = prefix.withSuffix(LiteralStringRef("processes"));
	if (kr.contains(processes_key)) {
		result.push_back_deep(result.arena(), KeyValueRef(processes_key, Value(processes_str)));
	}
	return rywGetRange(ryw, kr, result);
}

ACTOR static Future<Optional<std::string>> coordinatorsCommitActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	state Reference<IQuorumChange> change;
	state ClusterConnectionString
	    conn; // We don't care about the Key here, it will be overrode in changeQuorumChecker().
	state std::vector<std::string> process_address_or_hostname_strs;
	state Optional<std::string> msg;
	state int index;
	state bool parse_error = false;

	// check update for coordinators
	Key processes_key = LiteralStringRef("processes").withPrefix(kr.begin);
	auto processes_entry = ryw->getSpecialKeySpaceWriteMap()[processes_key];
	if (processes_entry.first) {
		ASSERT(processes_entry.second.present()); // no clear should be seen here
		auto processesStr = processes_entry.second.get().toString();
		boost::split(process_address_or_hostname_strs, processesStr, [](char c) { return c == ','; });
		if (!process_address_or_hostname_strs.size()) {
			return ManagementAPIError::toJsonString(
			    false,
			    "coordinators",
			    "New coordinators\' processes are empty, please specify new processes\' network addresses with format "
			    "\"IP:PORT,IP:PORT,...,IP:PORT\" or \"HOSTNAME:PORT,HOSTNAME:PORT,...,HOSTNAME:PORT\"");
		}
		for (index = 0; index < process_address_or_hostname_strs.size(); index++) {
			try {
				if (Hostname::isHostname(process_address_or_hostname_strs[index])) {
					conn.hostnames.push_back(Hostname::parse(process_address_or_hostname_strs[index]));
					conn.status = ClusterConnectionString::ConnectionStringStatus::UNRESOLVED;
				} else {
					NetworkAddress a = NetworkAddress::parse(process_address_or_hostname_strs[index]);
					if (!a.isValid()) {
						parse_error = true;
					} else {
						conn.coords.push_back(a);
					}
				}
			} catch (Error& e) {
				TraceEvent(SevDebug, "SpecialKeysNetworkParseError").error(e);
				parse_error = true;
			}

			if (parse_error) {
				std::string error = "ERROR: \'" + process_address_or_hostname_strs[index] +
				                    "\' is not a valid network endpoint address\n";
				if (process_address_or_hostname_strs[index].find(":tls") != std::string::npos)
					error += "        Do not include the `:tls' suffix when naming a process\n";
				return ManagementAPIError::toJsonString(false, "coordinators", error);
			}
		}
	}

	wait(conn.resolveHostnames());
	if (conn.coordinators().size())
		change = specifiedQuorumChange(conn.coordinators());
	else
		change = noQuorumChange();

	// check update for cluster_description
	Key cluster_decription_key = LiteralStringRef("cluster_description").withPrefix(kr.begin);
	auto entry = ryw->getSpecialKeySpaceWriteMap()[cluster_decription_key];
	if (entry.first) {
		// check valid description [a-zA-Z0-9_]+
		if (entry.second.present() && isAlphaNumeric(entry.second.get().toString())) {
			// do the name change
			change = nameQuorumChange(entry.second.get().toString(), change);
		} else {
			// throw the error
			return Optional<std::string>(ManagementAPIError::toJsonString(
			    false, "coordinators", "Cluster description must match [A-Za-z0-9_]+"));
		}
	}

	ASSERT(change.isValid());

	TraceEvent(SevDebug, "SKSChangeCoordinatorsStart")
	    .detail("NewHostnames", conn.hostnames.size() ? describe(conn.hostnames) : "N/A")
	    .detail("NewAddresses", describe(conn.coordinators()))
	    .detail("Description", entry.first ? entry.second.get().toString() : "");

	Optional<CoordinatorsResult> r = wait(changeQuorumChecker(&ryw->getTransaction(), change, &conn));

	TraceEvent(SevDebug, "SKSChangeCoordinatorsFinish")
	    .detail("Result", r.present() ? static_cast<int>(r.get()) : -1); // -1 means success
	if (r.present()) {
		auto res = r.get();
		bool retriable = false;
		if (res == CoordinatorsResult::COORDINATOR_UNREACHABLE) {
			retriable = true;
		} else if (res == CoordinatorsResult::SUCCESS) {
			TraceEvent(SevError, "SpecialKeysForCoordinators").detail("UnexpectedSuccessfulResult", "");
			ASSERT(false);
		}
		msg = ManagementAPIError::toJsonString(retriable, "coordinators", ManagementAPI::generateErrorMessage(res));
	}
	return msg;
}

Future<Optional<std::string>> CoordinatorsImpl::commit(ReadYourWritesTransaction* ryw) {
	return coordinatorsCommitActor(ryw, getKeyRange());
}

void CoordinatorsImpl::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	return throwSpecialKeyApiFailure(ryw, "coordinators", "Clear range is meaningless thus forbidden for coordinators");
}

void CoordinatorsImpl::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	return throwSpecialKeyApiFailure(
	    ryw, "coordinators", "Clear operation is meaningless thus forbidden for coordinators");
}

CoordinatorsAutoImpl::CoordinatorsAutoImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

ACTOR static Future<RangeResult> CoordinatorsAutoImplActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	state RangeResult res;
	state std::string autoCoordinatorsKey;
	state Transaction& tr = ryw->getTransaction();

	tr.setOption(FDBTransactionOptions::LOCK_AWARE);
	tr.setOption(FDBTransactionOptions::RAW_ACCESS);
	tr.setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	tr.setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	Optional<Value> currentKey = wait(tr.get(coordinatorsKey));

	if (!currentKey.present()) {
		ryw->setSpecialKeySpaceErrorMsg(
		    ManagementAPIError::toJsonString(false, "auto_coordinators", "The coordinator key does not exist"));
		throw special_keys_api_failure();
	}
	state ClusterConnectionString old(currentKey.get().toString());
	state CoordinatorsResult result = CoordinatorsResult::SUCCESS;

	std::vector<NetworkAddress> _desiredCoordinators = wait(autoQuorumChange()->getDesiredCoordinators(
	    &tr,
	    old.coordinators(),
	    Reference<ClusterConnectionMemoryRecord>(new ClusterConnectionMemoryRecord(old)),
	    result));

	if (result == CoordinatorsResult::NOT_ENOUGH_MACHINES) {
		// we could get not_enough_machines if we happen to see the database while the cluster controller is updating
		// the worker list, so make sure it happens twice before returning a failure
		ryw->setSpecialKeySpaceErrorMsg(ManagementAPIError::toJsonString(
		    true,
		    "auto_coordinators",
		    "Too few fdbserver machines to provide coordination at the current redundancy level"));
		throw special_keys_api_failure();
	}

	for (const auto& address : _desiredCoordinators) {
		autoCoordinatorsKey += autoCoordinatorsKey.size() ? "," : "";
		autoCoordinatorsKey += address.toString();
	}
	res.push_back_deep(res.arena(), KeyValueRef(kr.begin, Value(autoCoordinatorsKey)));
	return res;
}

Future<RangeResult> CoordinatorsAutoImpl::getRange(ReadYourWritesTransaction* ryw,
                                                   KeyRangeRef kr,
                                                   GetRangeLimits limitsHint) const {
	// single key range, the queried range should always be the same as the underlying range
	ASSERT(kr == getKeyRange());
	return CoordinatorsAutoImplActor(ryw, kr);
}

ACTOR static Future<RangeResult> getMinCommitVersionActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<Value> val = wait(ryw->getTransaction().get(minRequiredCommitVersionKey));
	RangeResult result;
	if (val.present()) {
		Version minRequiredCommitVersion = BinaryReader::fromStringRef<Version>(val.get(), Unversioned());
		ValueRef version(result.arena(), boost::lexical_cast<std::string>(minRequiredCommitVersion));
		result.push_back_deep(result.arena(), KeyValueRef(kr.begin, version));
	}
	return result;
}

AdvanceVersionImpl::AdvanceVersionImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> AdvanceVersionImpl::getRange(ReadYourWritesTransaction* ryw,
                                                 KeyRangeRef kr,
                                                 GetRangeLimits limitsHint) const {
	// single key range, the queried range should always be the same as the underlying range
	ASSERT(kr == getKeyRange());
	auto entry = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("advanceversion")];
	if (!ryw->readYourWritesDisabled() && entry.first) {
		// ryw enabled and we have written to the special key
		RangeResult result;
		if (entry.second.present()) {
			result.push_back_deep(result.arena(), KeyValueRef(kr.begin, entry.second.get()));
		}
		return result;
	} else {
		return getMinCommitVersionActor(ryw, kr);
	}
}

ACTOR static Future<Optional<std::string>> advanceVersionCommitActor(ReadYourWritesTransaction* ryw, Version v) {
	// Max version we can set for minRequiredCommitVersionKey,
	// making sure the cluster can still be alive for 1000 years after the recovery
	static const Version maxAllowedVerion =
	    std::numeric_limits<int64_t>::max() - 1 - CLIENT_KNOBS->VERSIONS_PER_SECOND * 3600 * 24 * 365 * 1000;

	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	TraceEvent(SevDebug, "AdvanceVersion").detail("MaxAllowedVersion", maxAllowedVerion);
	if (v > maxAllowedVerion) {
		return ManagementAPIError::toJsonString(
		    false,
		    "advanceversion",
		    "The given version is larger than the maximum allowed value(2**63-1-version_per_second*3600*24*365*1000)");
	}
	Version rv = wait(ryw->getTransaction().getReadVersion());
	if (rv <= v) {
		ryw->getTransaction().set(minRequiredCommitVersionKey, BinaryWriter::toValue(v + 1, Unversioned()));
	} else {
		return ManagementAPIError::toJsonString(
		    false, "advanceversion", "Current read version is larger than the given version");
	}
	return Optional<std::string>();
}

Future<Optional<std::string>> AdvanceVersionImpl::commit(ReadYourWritesTransaction* ryw) {
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	auto minCommitVersion =
	    ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandPrefix("advanceversion")].second;
	if (minCommitVersion.present()) {
		try {
			// Version is int64_t
			Version v = boost::lexical_cast<int64_t>(minCommitVersion.get().toString());
			return advanceVersionCommitActor(ryw, v);
		} catch (boost::bad_lexical_cast& e) {
			return Optional<std::string>(ManagementAPIError::toJsonString(
			    false, "advanceversion", "Invalid version(int64_t) argument: " + minCommitVersion.get().toString()));
		}
	} else {
		ryw->getTransaction().clear(minRequiredCommitVersionKey);
	}
	return Optional<std::string>();
}

ClientProfilingImpl::ClientProfilingImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

ACTOR static Future<RangeResult> ClientProfilingGetRangeActor(ReadYourWritesTransaction* ryw,
                                                              KeyRef prefix,
                                                              KeyRangeRef kr) {
	state RangeResult result;
	// client_txn_sample_rate
	state Key sampleRateKey = LiteralStringRef("client_txn_sample_rate").withPrefix(prefix);

	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);

	if (kr.contains(sampleRateKey)) {
		auto entry = ryw->getSpecialKeySpaceWriteMap()[sampleRateKey];
		if (!ryw->readYourWritesDisabled() && entry.first) {
			// clear is forbidden
			ASSERT(entry.second.present());
			result.push_back_deep(result.arena(), KeyValueRef(sampleRateKey, entry.second.get()));
		} else {
			Optional<Value> f = wait(ryw->getTransaction().get(fdbClientInfoTxnSampleRate));
			std::string sampleRateStr = "default";
			if (f.present()) {
				const double sampleRateDbl = BinaryReader::fromStringRef<double>(f.get(), Unversioned());
				if (!std::isinf(sampleRateDbl)) {
					sampleRateStr = boost::lexical_cast<std::string>(sampleRateDbl);
				}
			}
			result.push_back_deep(result.arena(), KeyValueRef(sampleRateKey, Value(sampleRateStr)));
		}
	}
	// client_txn_size_limit
	state Key txnSizeLimitKey = LiteralStringRef("client_txn_size_limit").withPrefix(prefix);
	if (kr.contains(txnSizeLimitKey)) {
		auto entry = ryw->getSpecialKeySpaceWriteMap()[txnSizeLimitKey];
		if (!ryw->readYourWritesDisabled() && entry.first) {
			// clear is forbidden
			ASSERT(entry.second.present());
			result.push_back_deep(result.arena(), KeyValueRef(txnSizeLimitKey, entry.second.get()));
		} else {
			Optional<Value> f = wait(ryw->getTransaction().get(fdbClientInfoTxnSizeLimit));
			std::string sizeLimitStr = "default";
			if (f.present()) {
				const int64_t sizeLimit = BinaryReader::fromStringRef<int64_t>(f.get(), Unversioned());
				if (sizeLimit != -1) {
					sizeLimitStr = boost::lexical_cast<std::string>(sizeLimit);
				}
			}
			result.push_back_deep(result.arena(), KeyValueRef(txnSizeLimitKey, Value(sizeLimitStr)));
		}
	}
	return result;
}

// TODO : add limitation on set operation
Future<RangeResult> ClientProfilingImpl::getRange(ReadYourWritesTransaction* ryw,
                                                  KeyRangeRef kr,
                                                  GetRangeLimits limitsHint) const {
	return ClientProfilingGetRangeActor(ryw, getKeyRange().begin, kr);
}

Future<Optional<std::string>> ClientProfilingImpl::commit(ReadYourWritesTransaction* ryw) {
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);

	// client_txn_sample_rate
	Key sampleRateKey = LiteralStringRef("client_txn_sample_rate").withPrefix(getKeyRange().begin);
	auto rateEntry = ryw->getSpecialKeySpaceWriteMap()[sampleRateKey];

	if (rateEntry.first && rateEntry.second.present()) {
		std::string sampleRateStr = rateEntry.second.get().toString();
		double sampleRate;
		if (sampleRateStr == "default")
			sampleRate = std::numeric_limits<double>::infinity();
		else {
			try {
				sampleRate = boost::lexical_cast<double>(sampleRateStr);
			} catch (boost::bad_lexical_cast& e) {
				return Optional<std::string>(ManagementAPIError::toJsonString(
				    false, "profile", "Invalid transaction sample rate(double): " + sampleRateStr));
			}
		}
		ryw->getTransaction().set(fdbClientInfoTxnSampleRate, BinaryWriter::toValue(sampleRate, Unversioned()));
	}
	// client_txn_size_limit
	Key txnSizeLimitKey = LiteralStringRef("client_txn_size_limit").withPrefix(getKeyRange().begin);
	auto sizeLimitEntry = ryw->getSpecialKeySpaceWriteMap()[txnSizeLimitKey];
	if (sizeLimitEntry.first && sizeLimitEntry.second.present()) {
		std::string sizeLimitStr = sizeLimitEntry.second.get().toString();
		int64_t sizeLimit;
		if (sizeLimitStr == "default")
			sizeLimit = -1;
		else {
			try {
				sizeLimit = boost::lexical_cast<int64_t>(sizeLimitStr);
			} catch (boost::bad_lexical_cast& e) {
				return Optional<std::string>(ManagementAPIError::toJsonString(
				    false, "profile", "Invalid transaction size limit(int64_t): " + sizeLimitStr));
			}
		}
		ryw->getTransaction().set(fdbClientInfoTxnSizeLimit, BinaryWriter::toValue(sizeLimit, Unversioned()));
	}
	return Optional<std::string>();
}

void ClientProfilingImpl::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
	return throwSpecialKeyApiFailure(
	    ryw, "profile", "Clear range is forbidden for profile client. You can set it to default to disable profiling.");
}

void ClientProfilingImpl::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	return throwSpecialKeyApiFailure(
	    ryw,
	    "profile",
	    "Clear operation is forbidden for profile client. You can set it to default to disable profiling.");
}

ActorLineageImpl::ActorLineageImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

void parse(StringRef& val, int& i) {
	i = std::stoi(val.toString());
}

void parse(StringRef& val, double& d) {
	d = std::stod(val.toString());
}

void parse(StringRef& val, WaitState& w) {
	if (val == LiteralStringRef("disk") || val == LiteralStringRef("Disk")) {
		w = WaitState::Disk;
	} else if (val == LiteralStringRef("network") || val == LiteralStringRef("Network")) {
		w = WaitState::Network;
	} else if (val == LiteralStringRef("running") || val == LiteralStringRef("Running")) {
		w = WaitState::Running;
	} else {
		throw std::range_error("failed to parse run state");
	}
}

void parse(StringRef& val, time_t& t) {
	struct tm tm;
#ifdef _WIN32
	std::istringstream s(val.toString());
	s.imbue(std::locale(setlocale(LC_TIME, nullptr)));
	s >> std::get_time(&tm, "%FT%T%z");
	if (s.fail()) {
		throw std::invalid_argument("failed to parse ISO 8601 datetime");
	}
	long timezone;
	if (_get_timezone(&timezone) != 0) {
		throw std::runtime_error("failed to convert ISO 8601 datetime");
	}
	timezone = -timezone;
#else
	if (strptime(val.toString().c_str(), "%FT%T%z", &tm) == nullptr) {
		throw std::invalid_argument("failed to parse ISO 8601 datetime");
	}
	long timezone = tm.tm_gmtoff;
	t = timegm(&tm);
	if (t == -1) {
		throw std::runtime_error("failed to convert ISO 8601 datetime");
	}
	t -= timezone;
#endif
}

void parse(StringRef& val, NetworkAddress& a) {
	auto address = NetworkAddress::parse(val.toString());
	if (!address.isValid()) {
		throw std::invalid_argument("invalid host");
	}
	a = address;
}

// Base case function for parsing function below.
template <typename T>
void parse(std::vector<StringRef>::iterator it, std::vector<StringRef>::iterator end, T& t1) {
	if (it == end) {
		return;
	}
	parse(*it, t1);
}

// Given an iterator into a vector of string tokens, an iterator to the end of
// the search space in the vector (exclusive), and a list of references to
// types, parses each token in the vector into the associated type according to
// the order of the arguments.
//
// For example, given the vector ["1", "1.5", "127.0.0.1:4000"] and the
// argument list int a, double b, NetworkAddress c, after this function returns
// each parameter passed in will hold the parsed value from the token list.
//
// The appropriate parsing function must be implemented for the type you wish
// to parse. See the existing parsing functions above, and add your own if
// necessary.
template <typename T, typename... Types>
void parse(std::vector<StringRef>::iterator it, std::vector<StringRef>::iterator end, T& t1, Types&... remaining) {
	// Return as soon as all tokens have been parsed. This allows parameters
	// passed at the end to act as optional parameters -- they will only be set
	// if the value exists.
	if (it == end) {
		return;
	}

	try {
		parse(*it, t1);
		parse(++it, end, remaining...);
	} catch (Error& e) {
		throw e;
	} catch (std::exception& e) {
		throw e;
	}
}

ACTOR static Future<RangeResult> actorLineageGetRangeActor(ReadYourWritesTransaction* ryw,
                                                           KeyRef prefix,
                                                           KeyRangeRef kr) {
	state RangeResult result;

	// Set default values for all fields. The default will be used if the field
	// is missing in the key.
	state NetworkAddress host;
	state WaitState waitStateStart = WaitState{ 0 };
	state WaitState waitStateEnd = WaitState{ 2 };
	state time_t timeStart = 0;
	state time_t timeEnd = std::numeric_limits<time_t>::max();
	state int seqStart = 0;
	state int seqEnd = std::numeric_limits<int>::max();

	state std::vector<StringRef> beginValues = kr.begin.removePrefix(prefix).splitAny("/"_sr);
	state std::vector<StringRef> endValues = kr.end.removePrefix(prefix).splitAny("/"_sr);
	// Require index (either "state" or "time") and address:port.
	if (beginValues.size() < 2 || endValues.size() < 2) {
		ryw->setSpecialKeySpaceErrorMsg("missing required parameters (index, host)");
		throw special_keys_api_failure();
	}

	state NetworkAddress endRangeHost;
	try {
		if (SpecialKeySpace::getActorLineageApiCommandRange("state").contains(kr)) {
			// For the range \xff\xff/actor_lineage/state/ip:port/wait-state/time/seq
			parse(beginValues.begin() + 1, beginValues.end(), host, waitStateStart, timeStart, seqStart);
			if (kr.begin != kr.end) {
				parse(endValues.begin() + 1, endValues.end(), endRangeHost, waitStateEnd, timeEnd, seqEnd);
			}
		} else if (SpecialKeySpace::getActorLineageApiCommandRange("time").contains(kr)) {
			// For the range \xff\xff/actor_lineage/time/ip:port/time/wait-state/seq
			parse(beginValues.begin() + 1, beginValues.end(), host, timeStart, waitStateStart, seqStart);
			if (kr.begin != kr.end) {
				parse(endValues.begin() + 1, endValues.end(), endRangeHost, timeEnd, waitStateEnd, seqEnd);
			}
		} else {
			ryw->setSpecialKeySpaceErrorMsg("invalid index in actor_lineage");
			throw special_keys_api_failure();
		}
	} catch (Error& e) {
		if (e.code() != special_keys_api_failure().code()) {
			ryw->setSpecialKeySpaceErrorMsg("failed to parse key");
			throw special_keys_api_failure();
		} else {
			throw e;
		}
	}

	if (kr.begin != kr.end && host != endRangeHost) {
		// The client doesn't know about all the hosts, so a get range covering
		// multiple hosts has no way of knowing which IP:port combos to use.
		ryw->setSpecialKeySpaceErrorMsg("the host must remain the same on both ends of the range");
		throw special_keys_api_failure();
	}

	// Open endpoint to target process on each call. This can be optimized at
	// some point...
	state ProcessInterface process;
	process.getInterface = RequestStream<GetProcessInterfaceRequest>(Endpoint::wellKnown({ host }, WLTOKEN_PROCESS));
	ProcessInterface p = wait(retryBrokenPromise(process.getInterface, GetProcessInterfaceRequest{}));
	process = p;

	ActorLineageRequest actorLineageRequest;
	actorLineageRequest.waitStateStart = waitStateStart;
	actorLineageRequest.waitStateEnd = waitStateEnd;
	actorLineageRequest.timeStart = timeStart;
	actorLineageRequest.timeEnd = timeEnd;
	ActorLineageReply reply = wait(process.actorLineage.getReply(actorLineageRequest));

	time_t dt = 0;
	int seq = -1;
	for (const auto& sample : reply.samples) {
		time_t datetime = (time_t)sample.time;
		char buf[50];
		struct tm* tm;
		tm = localtime(&datetime);
		size_t size = strftime(buf, 50, "%FT%T%z", tm);
		std::string date(buf, size);

		seq = dt == datetime ? seq + 1 : 0;
		dt = datetime;

		for (const auto& [waitState, data] : sample.data) {
			if (seq < seqStart) {
				continue;
			} else if (seq >= seqEnd) {
				break;
			}

			std::ostringstream streamKey;
			if (SpecialKeySpace::getActorLineageApiCommandRange("state").contains(kr)) {
				streamKey << SpecialKeySpace::getActorLineageApiCommandPrefix("state").toString() << host.toString()
				          << "/" << to_string(waitState) << "/" << date;
			} else if (SpecialKeySpace::getActorLineageApiCommandRange("time").contains(kr)) {
				streamKey << SpecialKeySpace::getActorLineageApiCommandPrefix("time").toString() << host.toString()
				          << "/" << date << "/" << to_string(waitState);
			} else {
				ASSERT(false);
			}
			streamKey << "/" << seq;

			msgpack::object_handle oh = msgpack::unpack(data.data(), data.size());
			msgpack::object deserialized = oh.get();

			std::ostringstream stream;
			stream << deserialized;

			result.push_back_deep(result.arena(), KeyValueRef(streamKey.str(), stream.str()));
		}

		if (sample.data.size() == 0) {
			std::ostringstream streamKey;
			if (SpecialKeySpace::getActorLineageApiCommandRange("state").contains(kr)) {
				streamKey << SpecialKeySpace::getActorLineageApiCommandPrefix("state").toString() << host.toString()
				          << "/Running/" << date;
			} else if (SpecialKeySpace::getActorLineageApiCommandRange("time").contains(kr)) {
				streamKey << SpecialKeySpace::getActorLineageApiCommandPrefix("time").toString() << host.toString()
				          << "/" << date << "/Running";
			} else {
				ASSERT(false);
			}
			streamKey << "/" << seq;
			result.push_back_deep(result.arena(), KeyValueRef(streamKey.str(), "{}"_sr));
		}
	}

	return result;
}

Future<RangeResult> ActorLineageImpl::getRange(ReadYourWritesTransaction* ryw,
                                               KeyRangeRef kr,
                                               GetRangeLimits limitsHint) const {
	return actorLineageGetRangeActor(ryw, getKeyRange().begin, kr);
}

namespace {
std::string_view to_string_view(StringRef sr) {
	return std::string_view(reinterpret_cast<const char*>(sr.begin()), sr.size());
}
} // namespace

ActorProfilerConf::ActorProfilerConf(KeyRangeRef kr)
  : SpecialKeyRangeRWImpl(kr), config(ProfilerConfig::instance().getConfig()) {}

Future<RangeResult> ActorProfilerConf::getRange(ReadYourWritesTransaction* ryw,
                                                KeyRangeRef kr,
                                                GetRangeLimits limitsHint) const {
	RangeResult res;
	std::string_view begin(to_string_view(kr.begin.removePrefix(range.begin))),
	    end(to_string_view(kr.end.removePrefix(range.begin)));
	for (auto& p : config) {
		if (p.first > end) {
			break;
		} else if (p.first > begin) {
			KeyValueRef kv;
			kv.key = StringRef(res.arena(), p.first).withPrefix(kr.begin, res.arena());
			kv.value = StringRef(res.arena(), p.second);
			res.push_back(res.arena(), kv);
		}
	}
	return res;
}

void ActorProfilerConf::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	config[key.removePrefix(range.begin).toString()] = value.toString();
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(value)));
	didWrite = true;
}

void ActorProfilerConf::clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& kr) {
	std::string begin(kr.begin.removePrefix(range.begin).toString()), end(kr.end.removePrefix(range.begin).toString());
	auto first = config.lower_bound(begin);
	if (first == config.end()) {
		// nothing to clear
		return;
	}
	didWrite = true;
	auto last = config.upper_bound(end);
	config.erase(first, last);
}

void ActorProfilerConf::clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
	std::string k = key.removePrefix(range.begin).toString();
	auto iter = config.find(k);
	if (iter != config.end()) {
		config.erase(iter);
	}
	didWrite = true;
}

Future<Optional<std::string>> ActorProfilerConf::commit(ReadYourWritesTransaction* ryw) {
	Optional<std::string> res{};
	try {
		if (didWrite) {
			ProfilerConfig::instance().reset(config);
		}
		return res;
	} catch (ConfigError& err) {
		return Optional<std::string>{ err.description };
	}
}

MaintenanceImpl::MaintenanceImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

// Used to read the healthZoneKey
// If the key is persisted and the delayed read version is still larger than current read version,
// we will calculate the remaining time(truncated to integer, the same as fdbcli) and return back as the value
// If the zoneId is the special one `ignoreSSFailuresZoneString`,
// value will be 0 (same as fdbcli)
ACTOR static Future<RangeResult> MaintenanceGetRangeActor(ReadYourWritesTransaction* ryw,
                                                          KeyRef prefix,
                                                          KeyRangeRef kr) {
	state RangeResult result;
	// zoneId
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	Optional<Value> val = wait(ryw->getTransaction().get(healthyZoneKey));
	if (val.present()) {
		auto healthyZone = decodeHealthyZoneValue(val.get());
		if ((healthyZone.first == ignoreSSFailuresZoneString) ||
		    (healthyZone.second > ryw->getTransaction().getReadVersion().get())) {
			Key zone_key = healthyZone.first.withPrefix(prefix);
			double seconds = healthyZone.first == ignoreSSFailuresZoneString
			                     ? 0
			                     : (healthyZone.second - ryw->getTransaction().getReadVersion().get()) /
			                           CLIENT_KNOBS->CORE_VERSIONSPERSECOND;
			if (kr.contains(zone_key)) {
				result.push_back_deep(result.arena(),
				                      KeyValueRef(zone_key, Value(boost::lexical_cast<std::string>(seconds))));
			}
		}
	}
	return rywGetRange(ryw, kr, result);
}

Future<RangeResult> MaintenanceImpl::getRange(ReadYourWritesTransaction* ryw,
                                              KeyRangeRef kr,
                                              GetRangeLimits limitsHint) const {
	return MaintenanceGetRangeActor(ryw, getKeyRange().begin, kr);
}

// Commit the change to healthZoneKey
// We do not allow more than one zone to be set in maintenance in one transaction
// In addition, if the zoneId now is 'ignoreSSFailuresZoneString',
// which means the data distribution is disabled for storage failures.
// Only clear this specific key is allowed, any other operations will throw error
ACTOR static Future<Optional<std::string>> maintenanceCommitActor(ReadYourWritesTransaction* ryw, KeyRangeRef kr) {
	// read
	ryw->getTransaction().setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);
	ryw->getTransaction().setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	Optional<Value> val = wait(ryw->getTransaction().get(healthyZoneKey));
	Optional<std::pair<Key, Version>> healthyZone =
	    val.present() ? decodeHealthyZoneValue(val.get()) : Optional<std::pair<Key, Version>>();

	state RangeMap<Key, std::pair<bool, Optional<Value>>, KeyRangeRef>::Ranges ranges =
	    ryw->getSpecialKeySpaceWriteMap().containedRanges(kr);
	Key zoneId;
	double seconds;
	bool isSet = false;
	// Since maintenance only allows one zone at the same time,
	// if a transaction has more than one set operation on different zone keys,
	// the commit will throw an error
	for (auto iter = ranges.begin(); iter != ranges.end(); ++iter) {
		if (!iter->value().first)
			continue;
		if (iter->value().second.present()) {
			if (isSet)
				return Optional<std::string>(ManagementAPIError::toJsonString(
				    false, "maintenance", "Multiple zones given for maintenance, only one allowed at the same time"));
			isSet = true;
			zoneId = iter->begin().removePrefix(kr.begin);
			seconds = boost::lexical_cast<double>(iter->value().second.get().toString());
		} else {
			// if we already have set operation, then all clear operations will be meaningless, thus skip
			if (!isSet && healthyZone.present() && iter.range().contains(healthyZone.get().first.withPrefix(kr.begin)))
				ryw->getTransaction().clear(healthyZoneKey);
		}
	}

	if (isSet) {
		if (healthyZone.present() && healthyZone.get().first == ignoreSSFailuresZoneString) {
			std::string msg = "Maintenance mode cannot be used while data distribution is disabled for storage "
			                  "server failures.";
			return Optional<std::string>(ManagementAPIError::toJsonString(false, "maintenance", msg));
		} else if (seconds < 0) {
			std::string msg =
			    "The specified maintenance time " + boost::lexical_cast<std::string>(seconds) + " is a negative value";
			return Optional<std::string>(ManagementAPIError::toJsonString(false, "maintenance", msg));
		} else {
			TraceEvent(SevDebug, "SKSMaintenanceSet").detail("ZoneId", zoneId.toString());
			ryw->getTransaction().set(healthyZoneKey,
			                          healthyZoneValue(zoneId,
			                                           ryw->getTransaction().getReadVersion().get() +
			                                               (seconds * CLIENT_KNOBS->CORE_VERSIONSPERSECOND)));
		}
	}
	return Optional<std::string>();
}

Future<Optional<std::string>> MaintenanceImpl::commit(ReadYourWritesTransaction* ryw) {
	return maintenanceCommitActor(ryw, getKeyRange());
}

DataDistributionImpl::DataDistributionImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

// Read the system keys dataDistributionModeKey and rebalanceDDIgnoreKey
ACTOR static Future<RangeResult> DataDistributionGetRangeActor(ReadYourWritesTransaction* ryw,
                                                               KeyRef prefix,
                                                               KeyRangeRef kr) {
	state RangeResult result;
	// dataDistributionModeKey
	state Key modeKey = LiteralStringRef("mode").withPrefix(prefix);

	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);

	if (kr.contains(modeKey)) {
		auto entry = ryw->getSpecialKeySpaceWriteMap()[modeKey];
		if (ryw->readYourWritesDisabled() || !entry.first) {
			Optional<Value> f = wait(ryw->getTransaction().get(dataDistributionModeKey));
			int mode = -1;
			if (f.present()) {
				mode = BinaryReader::fromStringRef<int>(f.get(), Unversioned());
			}
			result.push_back_deep(result.arena(), KeyValueRef(modeKey, Value(boost::lexical_cast<std::string>(mode))));
		}
	}
	// rebalanceDDIgnoreKey
	state Key rebalanceIgnoredKey = LiteralStringRef("rebalance_ignored").withPrefix(prefix);
	if (kr.contains(rebalanceIgnoredKey)) {
		auto entry = ryw->getSpecialKeySpaceWriteMap()[rebalanceIgnoredKey];
		if (ryw->readYourWritesDisabled() || !entry.first) {
			Optional<Value> f = wait(ryw->getTransaction().get(rebalanceDDIgnoreKey));
			if (f.present()) {
				result.push_back_deep(result.arena(), KeyValueRef(rebalanceIgnoredKey, Value()));
			}
		}
	}
	return rywGetRange(ryw, kr, result);
}

Future<RangeResult> DataDistributionImpl::getRange(ReadYourWritesTransaction* ryw,
                                                   KeyRangeRef kr,
                                                   GetRangeLimits limitsHint) const {
	return DataDistributionGetRangeActor(ryw, getKeyRange().begin, kr);
}

Future<Optional<std::string>> DataDistributionImpl::commit(ReadYourWritesTransaction* ryw) {
	// there are two valid keys in the range
	// <prefix>/mode -> dataDistributionModeKey, the value is only allowed to be set as "0"(disable) or "1"(enable)
	// <prefix>/rebalance_ignored -> rebalanceDDIgnoreKey, value is unused thus empty
	ryw->getTransaction().setOption(FDBTransactionOptions::RAW_ACCESS);

	Optional<std::string> msg;
	KeyRangeRef kr = getKeyRange();
	Key modeKey = LiteralStringRef("mode").withPrefix(kr.begin);
	Key rebalanceIgnoredKey = LiteralStringRef("rebalance_ignored").withPrefix(kr.begin);
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(kr);
	for (auto iter = ranges.begin(); iter != ranges.end(); ++iter) {
		if (!iter->value().first)
			continue;
		if (iter->value().second.present()) {
			if (iter->range() == singleKeyRange(modeKey)) {
				try {
					int mode = boost::lexical_cast<int>(iter->value().second.get().toString());
					Value modeVal = BinaryWriter::toValue(mode, Unversioned());
					if (mode == 0 || mode == 1) {
						// Whenever configuration changes or DD related system keyspace is changed,
						// actor must grab the moveKeysLockOwnerKey and update moveKeysLockWriteKey.
						// This prevents concurrent write to the same system keyspace.
						// When the owner of the DD related system keyspace changes, DD will reboot
						BinaryWriter wrMyOwner(Unversioned());
						wrMyOwner << dataDistributionModeLock;
						ryw->getTransaction().set(moveKeysLockOwnerKey, wrMyOwner.toValue());
						BinaryWriter wrLastWrite(Unversioned());
						wrLastWrite << deterministicRandom()->randomUniqueID();
						ryw->getTransaction().set(moveKeysLockWriteKey, wrLastWrite.toValue());
						// set mode
						ryw->getTransaction().set(dataDistributionModeKey, modeVal);
					} else
						msg = ManagementAPIError::toJsonString(false,
						                                       "datadistribution",
						                                       "Please set the value of the data_distribution/mode to "
						                                       "0(disable) or 1(enable), other values are not allowed");
				} catch (boost::bad_lexical_cast& e) {
					msg = ManagementAPIError::toJsonString(false,
					                                       "datadistribution",
					                                       "Invalid datadistribution mode(int): " +
					                                           iter->value().second.get().toString());
				}
			} else if (iter->range() == singleKeyRange(rebalanceIgnoredKey)) {
				if (iter->value().second.get().size())
					msg =
					    ManagementAPIError::toJsonString(false,
					                                     "datadistribution",
					                                     "Value is unused for the data_distribution/rebalance_ignored "
					                                     "key, please set it to an empty value");
				else
					ryw->getTransaction().set(rebalanceDDIgnoreKey, LiteralStringRef("on"));
			} else {
				msg = ManagementAPIError::toJsonString(
				    false,
				    "datadistribution",
				    "Changing invalid keys, please read the documentation to check valid keys in the range");
			}
		} else {
			// clear
			if (iter->range().contains(modeKey))
				ryw->getTransaction().clear(dataDistributionModeKey);
			else if (iter->range().contains(rebalanceIgnoredKey))
				ryw->getTransaction().clear(rebalanceDDIgnoreKey);
		}
	}
	return msg;
}

// Clears the special management api keys excludeLocality and failedLocality.
void includeLocalities(ReadYourWritesTransaction* ryw) {
	ryw->setOption(FDBTransactionOptions::PRIORITY_SYSTEM_IMMEDIATE);
	ryw->setOption(FDBTransactionOptions::LOCK_AWARE);
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	ryw->setOption(FDBTransactionOptions::USE_PROVISIONAL_PROXIES);
	// includeLocalities might be used in an emergency transaction, so make sure it is retry-self-conflicting and
	// CAUSAL_WRITE_RISKY
	ryw->setOption(FDBTransactionOptions::CAUSAL_WRITE_RISKY);
	std::string versionKey = deterministicRandom()->randomUniqueID().toString();
	// for excluded localities
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(
	    SpecialKeySpace::getManagementApiCommandRange("excludedlocality"));
	Transaction& tr = ryw->getTransaction();
	for (auto& iter : ranges) {
		auto entry = iter.value();
		if (entry.first && !entry.second.present()) {
			tr.addReadConflictRange(singleKeyRange(excludedLocalityVersionKey));
			tr.set(excludedLocalityVersionKey, versionKey);
			tr.clear(ryw->getDatabase()->specialKeySpace->decode(iter.range()));
		}
	}
	// for failed localities
	ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(
	    SpecialKeySpace::getManagementApiCommandRange("failedlocality"));
	for (auto& iter : ranges) {
		auto entry = iter.value();
		if (entry.first && !entry.second.present()) {
			tr.addReadConflictRange(singleKeyRange(failedLocalityVersionKey));
			tr.set(failedLocalityVersionKey, versionKey);
			tr.clear(ryw->getDatabase()->specialKeySpace->decode(iter.range()));
		}
	}
}

// Reads the excludedlocality and failed locality keys using management api,
// parses them and returns the list.
bool parseLocalitiesFromKeys(ReadYourWritesTransaction* ryw,
                             bool failed,
                             std::unordered_set<std::string>& localities,
                             std::vector<AddressExclusion>& addresses,
                             std::set<AddressExclusion>& exclusions,
                             std::vector<ProcessData>& workers,
                             Optional<std::string>& msg) {
	KeyRangeRef range = failed ? SpecialKeySpace::getManagementApiCommandRange("failedlocality")
	                           : SpecialKeySpace::getManagementApiCommandRange("excludedlocality");
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
	auto iter = ranges.begin();
	while (iter != ranges.end()) {
		auto entry = iter->value();
		// only check for exclude(set) operation, include(clear) are not checked
		TraceEvent(SevDebug, "ParseLocalities")
		    .detail("Valid", entry.first)
		    .detail("Set", entry.second.present())
		    .detail("Key", iter->begin().toString());
		if (entry.first && entry.second.present()) {
			Key locality = iter->begin().removePrefix(range.begin);
			if (locality.startsWith(LocalityData::ExcludeLocalityPrefix) &&
			    locality.toString().find(':') != std::string::npos) {
				std::set<AddressExclusion> localityAddresses = getAddressesByLocality(workers, locality.toString());
				if (!localityAddresses.empty()) {
					std::copy(localityAddresses.begin(), localityAddresses.end(), back_inserter(addresses));
					exclusions.insert(localityAddresses.begin(), localityAddresses.end());
				}

				localities.insert(locality.toString());
			} else {
				std::string error = "ERROR: \'" + locality.toString() + "\' is not a valid locality\n";
				msg = ManagementAPIError::toJsonString(
				    false, entry.second.present() ? (failed ? "exclude failed" : "exclude") : "include", error);
				return false;
			}
		}
		++iter;
	}
	return true;
}

// On commit, parses the special exclusion keys and get the localities to be excluded, check for exclusions
// and add them to the exclusion list. Also, clears the special management api keys with includeLocalities.
ACTOR Future<Optional<std::string>> excludeLocalityCommitActor(ReadYourWritesTransaction* ryw, bool failed) {
	state Optional<std::string> result;
	state std::unordered_set<std::string> localities;
	state std::vector<AddressExclusion> addresses;
	state std::set<AddressExclusion> exclusions;

	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);

	state std::vector<ProcessData> workers = wait(getWorkers(&ryw->getTransaction()));
	if (!parseLocalitiesFromKeys(ryw, failed, localities, addresses, exclusions, workers, result))
		return result;
	// If force option is not set, we need to do safety check
	auto force = ryw->getSpecialKeySpaceWriteMap()[SpecialKeySpace::getManagementApiCommandOptionSpecialKey(
	    failed ? "failed_locality" : "excluded_locality", "force")];
	// only do safety check when we have localities to be excluded and the force option key is not set
	if (localities.size() && !(force.first && force.second.present())) {
		bool safe = wait(checkExclusion(ryw->getDatabase(), &addresses, &exclusions, failed, &result));
		if (!safe)
			return result;
	}

	excludeLocalities(ryw->getTransaction(), localities, failed);
	includeLocalities(ryw);

	return result;
}

ExcludedLocalitiesRangeImpl::ExcludedLocalitiesRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> ExcludedLocalitiesRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                          KeyRangeRef kr,
                                                          GetRangeLimits limitsHint) const {
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	return rwModuleWithMappingGetRangeActor(ryw, this, kr);
}

void ExcludedLocalitiesRangeImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	// ignore value
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(ValueRef())));
}

Key ExcludedLocalitiesRangeImpl::decode(const KeyRef& key) const {
	return key.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
	    .withPrefix(LiteralStringRef("\xff/conf/"));
}

Key ExcludedLocalitiesRangeImpl::encode(const KeyRef& key) const {
	return key.removePrefix(LiteralStringRef("\xff/conf/"))
	    .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
}

Future<Optional<std::string>> ExcludedLocalitiesRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	// exclude locality with failed option as false.
	return excludeLocalityCommitActor(ryw, false);
}

FailedLocalitiesRangeImpl::FailedLocalitiesRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> FailedLocalitiesRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                        KeyRangeRef kr,
                                                        GetRangeLimits limitsHint) const {
	ryw->setOption(FDBTransactionOptions::RAW_ACCESS);
	return rwModuleWithMappingGetRangeActor(ryw, this, kr);
}

void FailedLocalitiesRangeImpl::set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
	// ignore value
	ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(ValueRef())));
}

Key FailedLocalitiesRangeImpl::decode(const KeyRef& key) const {
	return key.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
	    .withPrefix(LiteralStringRef("\xff/conf/"));
}

Key FailedLocalitiesRangeImpl::encode(const KeyRef& key) const {
	return key.removePrefix(LiteralStringRef("\xff/conf/"))
	    .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
}

Future<Optional<std::string>> FailedLocalitiesRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	// exclude locality with failed option as true.
	return excludeLocalityCommitActor(ryw, true);
}

ACTOR Future<RangeResult> getTenantList(ReadYourWritesTransaction* ryw, KeyRangeRef kr, GetRangeLimits limitsHint) {
	state KeyRef managementPrefix =
	    kr.begin.substr(0,
	                    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin.size() +
	                        TenantMapRangeImpl::submoduleRange.begin.size());

	kr = kr.removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
	TenantNameRef beginTenant = kr.begin.removePrefix(TenantMapRangeImpl::submoduleRange.begin);

	TenantNameRef endTenant = kr.end;
	if (endTenant.startsWith(TenantMapRangeImpl::submoduleRange.begin)) {
		endTenant = endTenant.removePrefix(TenantMapRangeImpl::submoduleRange.begin);
	} else {
		endTenant = "\xff"_sr;
	}

	std::map<TenantName, TenantMapEntry> tenants =
	    wait(ManagementAPI::listTenantsTransaction(&ryw->getTransaction(), beginTenant, endTenant, limitsHint.rows));

	RangeResult results;
	for (auto tenant : tenants) {
		json_spirit::mObject tenantEntry;
		tenantEntry["id"] = tenant.second.id;
		tenantEntry["prefix"] = tenant.second.prefix.toString();
		std::string tenantEntryString = json_spirit::write_string(json_spirit::mValue(tenantEntry));
		ValueRef tenantEntryBytes(results.arena(), tenantEntryString);
		results.push_back(results.arena(),
		                  KeyValueRef(tenant.first.withPrefix(managementPrefix, results.arena()), tenantEntryBytes));
	}

	return results;
}

TenantMapRangeImpl::TenantMapRangeImpl(KeyRangeRef kr) : SpecialKeyRangeRWImpl(kr) {}

Future<RangeResult> TenantMapRangeImpl::getRange(ReadYourWritesTransaction* ryw,
                                                 KeyRangeRef kr,
                                                 GetRangeLimits limitsHint) const {
	return getTenantList(ryw, kr, limitsHint);
}

ACTOR Future<Void> deleteTenantRange(ReadYourWritesTransaction* ryw, TenantName beginTenant, TenantName endTenant) {
	std::map<TenantName, TenantMapEntry> tenants = wait(
	    ManagementAPI::listTenantsTransaction(&ryw->getTransaction(), beginTenant, endTenant, CLIENT_KNOBS->TOO_MANY));

	if (tenants.size() == CLIENT_KNOBS->TOO_MANY) {
		TraceEvent(SevWarn, "DeleteTenantRangeTooLange")
		    .detail("BeginTenant", beginTenant)
		    .detail("EndTenant", endTenant);
		ryw->setSpecialKeySpaceErrorMsg("too many tenants to range delete");
		throw special_keys_api_failure();
	}

	std::vector<Future<Void>> deleteFutures;
	for (auto tenant : tenants) {
		deleteFutures.push_back(ManagementAPI::deleteTenantTransaction(&ryw->getTransaction(), tenant.first));
	}

	wait(waitForAll(deleteFutures));
	return Void();
}

Future<Optional<std::string>> TenantMapRangeImpl::commit(ReadYourWritesTransaction* ryw) {
	auto ranges = ryw->getSpecialKeySpaceWriteMap().containedRanges(range);
	std::vector<Future<Void>> tenantManagementFutures;
	for (auto range : ranges) {
		if (!range.value().first) {
			continue;
		}

		TenantNameRef tenantName =
		    range.begin()
		        .removePrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
		        .removePrefix(TenantMapRangeImpl::submoduleRange.begin);

		if (range.value().second.present()) {
			tenantManagementFutures.push_back(
			    success(ManagementAPI::createTenantTransaction(&ryw->getTransaction(), tenantName)));
		} else {
			// For a single key clear, just issue the delete
			if (KeyRangeRef(range.begin(), range.end()).singleKeyRange()) {
				tenantManagementFutures.push_back(
				    ManagementAPI::deleteTenantTransaction(&ryw->getTransaction(), tenantName));
			} else {
				TenantNameRef endTenant = range.end().removePrefix(
				    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin);
				if (endTenant.startsWith(submoduleRange.begin)) {
					endTenant = endTenant.removePrefix(submoduleRange.begin);
				} else {
					endTenant = "\xff"_sr;
				}
				tenantManagementFutures.push_back(deleteTenantRange(ryw, tenantName, endTenant));
			}
		}
	}

	return tag(waitForAll(tenantManagementFutures), Optional<std::string>());
}
