/*
 * SpecialKeySpace.actor.h
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

#pragma once

#if defined(NO_INTELLISENSE) && !defined(FDBCLIENT_SPECIALKEYSPACE_ACTOR_G_H)
#define FDBCLIENT_SPECIALKEYSPACE_ACTOR_G_H
#include "fdbclient/SpecialKeySpace.actor.g.h"
#elif !defined(FDBCLIENT_SPECIALKEYSPACE_ACTOR_H)
#define FDBCLIENT_SPECIALKEYSPACE_ACTOR_H

#include "flow/flow.h"
#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include "fdbclient/KeyRangeMap.h"
#include "fdbclient/ReadYourWrites.h"
#include "flow/actorcompiler.h" // This must be the last #include.

class SpecialKeyRangeBaseImpl {
public:
	// Each derived class only needs to implement this simple version of getRange
	virtual Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                                    KeyRangeRef kr) const = 0;

	explicit SpecialKeyRangeBaseImpl(KeyRangeRef kr) : range(kr) {}
	KeyRangeRef getKeyRange() const { return range; }

	virtual ~SpecialKeyRangeBaseImpl() {}

protected:
	KeyRange range; // underlying key range for this function
};

class SpecialKeySpace {
public:
	enum class MODULE {
		UNKNOWN, // used for all unregistered range, a cross_module_read will happen if your range read contains any
		         // UNKKNOWN module range
		TESTONLY, // only used by tests
		TRANSACTION,
		WORKERINTERFACE,
		STATUSJSON,
		CLUSTERFILEPATH,
		CONNECTIONSTRING
	};

	Future<Optional<Value>> get(Reference<ReadYourWritesTransaction> ryw, const Key& key);

	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw, KeySelector begin,
	                                            KeySelector end, GetRangeLimits limits, bool reverse = false);

	SpecialKeySpace(KeyRef spaceStartKey = Key(), KeyRef spaceEndKey = normalKeys.end, bool test = true) {
		// Default value is nullptr, begin of KeyRangeMap is Key()
		impls = KeyRangeMap<SpecialKeyRangeBaseImpl*>(nullptr, spaceEndKey);
		range = KeyRangeRef(spaceStartKey, spaceEndKey);
		modules = KeyRangeMap<SpecialKeySpace::MODULE>(SpecialKeySpace::MODULE::UNKNOWN, spaceEndKey);
		if (!test) modulesBoundaryInit();
		// TODO : Handle testonly here
	}
	void modulesBoundaryInit() {
		for (const auto& pair : moduleToBoundary) {
			ASSERT(range.contains(pair.second));
			// Make sure the module is not overlapping with any registered modules
			ASSERT(modules.rangeContaining(pair.second.begin) == modules.rangeContaining(pair.second.end) &&
			       modules[pair.second.begin] == SpecialKeySpace::MODULE::UNKNOWN);
			modules.insert(pair.second, pair.first);
			impls.insert(pair.second, nullptr);
		}
	}
	void registerKeyRange(SpecialKeySpace::MODULE module, const KeyRangeRef& kr, SpecialKeyRangeBaseImpl* impl) {
		// range check
		// TODO: add range check not to be replaced by overlapped ones
		ASSERT(range.contains(kr));
		// make sure the registered range is not overlapping with existing ones
		// Note: kr.end should not be the same as another range's begin, although it should work even they are the same
		// ASSERT(impls.rangeContaining(kr.begin) == impls.rangeContaining(kr.end) && impls[kr.begin] == nullptr);
		for (auto iter = impls.rangeContaining(kr.begin); true; ++iter) {
			ASSERT(iter->value() == nullptr);
			if (iter == impls.rangeContaining(kr.end)) break; // relax the end to be another one's start if needed
		}
		impls.insert(kr, impl);
		// Set module for the range
		implToModule[impl] = module; // TODO : check do we really need this map
	}

	KeyRangeMap<SpecialKeyRangeBaseImpl*>& getImpls() { return impls; }
	KeyRangeMap<SpecialKeySpace::MODULE>& getModules() { return modules; }
	KeyRangeRef getKeyRange() const { return range; }
	const std::unordered_map<SpecialKeyRangeBaseImpl*, SpecialKeySpace::MODULE>& getImplToModuleMap() const {
		return implToModule;
	}

private:
	ACTOR static Future<Optional<Value>> getActor(SpecialKeySpace* sks, Reference<ReadYourWritesTransaction> ryw,
	                                              KeyRef key);

	ACTOR static Future<Standalone<RangeResultRef>> checkModuleFound(SpecialKeySpace* sks,
	                                                                 Reference<ReadYourWritesTransaction> ryw,
	                                                                 KeySelector begin, KeySelector end,
	                                                                 GetRangeLimits limits, bool reverse);
	ACTOR static Future<std::pair<Standalone<RangeResultRef>, Optional<SpecialKeySpace::MODULE>>>
	getRangeAggregationActor(SpecialKeySpace* sks, Reference<ReadYourWritesTransaction> ryw, KeySelector begin,
	                         KeySelector end, GetRangeLimits limits, bool reverse);

	KeyRangeMap<SpecialKeyRangeBaseImpl*> impls;
	KeyRangeMap<SpecialKeySpace::MODULE> modules;
	KeyRange range;
	std::unordered_map<SpecialKeyRangeBaseImpl*, SpecialKeySpace::MODULE> implToModule;

	static std::unordered_map<SpecialKeySpace::MODULE, KeyRange> moduleToBoundary;
};

// Use special key prefix "\xff\xff/transaction/conflicting_keys/<some_key>",
// to retrieve keys which caused latest not_committed(conflicting with another transaction) error.
// The returned key value pairs are interpretted as :
// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
// Currently, the conflicting keyranges returned are original read_conflict_ranges or union of them.
class ConflictingKeysImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit ConflictingKeysImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                            KeyRangeRef kr) const override;
};

class ReadConflictRangeImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit ReadConflictRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                            KeyRangeRef kr) const override;
};

class WriteConflictRangeImpl : public SpecialKeyRangeBaseImpl {
public:
	explicit WriteConflictRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(Reference<ReadYourWritesTransaction> ryw,
	                                            KeyRangeRef kr) const override;
};

#include "flow/unactorcompiler.h"
#endif
