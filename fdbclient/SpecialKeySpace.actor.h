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

class SpecialKeyRangeReadImpl {
public:
	// Each derived class only needs to implement this simple version of getRange
	virtual Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;

	explicit SpecialKeyRangeReadImpl(KeyRangeRef kr) : range(kr) {}
	KeyRangeRef getKeyRange() const { return range; }
	// true if the getRange call can emit more than one rpc calls,
	// we cache the results to keep consistency in the same getrange lifetime
	// TODO : give this function a more descriptive name
	virtual bool isAsync() const { return false; }

	virtual ~SpecialKeyRangeReadImpl() {}

protected:
	KeyRange range; // underlying key range for this function
};

class ManagementAPIError {
public:
	static std::string toJsonString(bool retriable, const std::string& command, const std::string& msg) {
		json_spirit::mObject errorObj;
		errorObj["retriable"] = retriable;
		errorObj["command"] = command;
		errorObj["message"] = msg;
		return json_spirit::write_string(json_spirit::mValue(errorObj), json_spirit::Output_options::raw_utf8);
	}

private:
	ManagementAPIError(){};
};

class SpecialKeyRangeRWImpl : public SpecialKeyRangeReadImpl {
public:
	virtual void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) {
		ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>(value)));
	}
	virtual void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) {
		ryw->getSpecialKeySpaceWriteMap().insert(range, std::make_pair(true, Optional<Value>()));
	}
	virtual void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) {
		ryw->getSpecialKeySpaceWriteMap().insert(key, std::make_pair(true, Optional<Value>()));
	}
	virtual Future<Optional<std::string>> commit(
	    ReadYourWritesTransaction* ryw) = 0; // all delayed async operations of writes in special-key-space
	// Given the special key to write, return the real key that needs to be modified
	virtual Key decode(const KeyRef& key) const {
		// Default implementation should never be used
		ASSERT(false);
		return key;
	}
	// Given the read key, return the corresponding special key
	virtual Key encode(const KeyRef& key) const {
		// Default implementation should never be used
		ASSERT(false);
		return key;
	};

	explicit SpecialKeyRangeRWImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

	virtual ~SpecialKeyRangeRWImpl() {}
};

class SpecialKeyRangeAsyncImpl : public SpecialKeyRangeReadImpl {
public:
	explicit SpecialKeyRangeAsyncImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const = 0;

	// calling with a cache object to have consistent results if we need to call rpc
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr,
	                                            Optional<Standalone<RangeResultRef>>* cache) const {
		return getRangeAsyncActor(this, ryw, kr, cache);
	}

	bool isAsync() const override { return true; }

	ACTOR static Future<Standalone<RangeResultRef>> getRangeAsyncActor(const SpecialKeyRangeReadImpl* skrAyncImpl,
	                                                                   ReadYourWritesTransaction* ryw, KeyRangeRef kr,
	                                                                   Optional<Standalone<RangeResultRef>>* cache) {
		ASSERT(skrAyncImpl->getKeyRange().contains(kr));
		ASSERT(cache != nullptr);
		if (!cache->present()) {
			// For simplicity, every time we need to cache, we read the whole range
			// Although sometimes the range can be narrowed,
			// there is not a general way to do it in complicated scenarios
			Standalone<RangeResultRef> result_ = wait(skrAyncImpl->getRange(ryw, skrAyncImpl->getKeyRange()));
			*cache = result_;
		}
		const auto& allResults = cache->get();
		int start = 0, end = allResults.size();
		while (start < allResults.size() && allResults[start].key < kr.begin) ++start;
		while (end > 0 && allResults[end - 1].key >= kr.end) --end;
		if (start < end) {
			Standalone<RangeResultRef> result = RangeResultRef(allResults.slice(start, end), false);
			result.arena().dependsOn(allResults.arena());
			return result;
		} else
			return Standalone<RangeResultRef>();
	}
};

class SpecialKeySpace {
public:
	enum class MODULE {
		CLUSTERFILEPATH,
		CONFIGURATION, // Configuration of the cluster
		CONNECTIONSTRING,
		ERRORMSG, // A single key space contains a json string which describes the last error in special-key-space
		MANAGEMENT, // Management-API
		METRICS, // data-distribution metrics
		TESTONLY, // only used by correctness tests
		TRANSACTION, // transaction related info, conflicting keys, read/write conflict range
		STATUSJSON,
		UNKNOWN, // default value for all unregistered range
		WORKERINTERFACE,
	};

	enum class IMPLTYPE {
		READONLY, // The underlying special key range can only be called with get and getRange
		READWRITE // The underlying special key range can be called with get, getRange, set, clear
	};

	SpecialKeySpace(KeyRef spaceStartKey = Key(), KeyRef spaceEndKey = normalKeys.end, bool testOnly = true);

	Future<Optional<Value>> get(ReadYourWritesTransaction* ryw, const Key& key);

	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeySelector begin, KeySelector end,
	                                            GetRangeLimits limits, bool reverse = false);

	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value);

	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range);

	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key);

	Future<Void> commit(ReadYourWritesTransaction* ryw);

	void registerKeyRange(SpecialKeySpace::MODULE module, SpecialKeySpace::IMPLTYPE type, const KeyRangeRef& kr,
	                      SpecialKeyRangeReadImpl* impl);

	Key decode(const KeyRef& key);
	KeyRange decode(const KeyRangeRef& kr);

	KeyRangeMap<SpecialKeyRangeReadImpl*>& getReadImpls() { return readImpls; }
	KeyRangeMap<SpecialKeyRangeRWImpl*>& getRWImpls() { return writeImpls; }
	KeyRangeMap<SpecialKeySpace::MODULE>& getModules() { return modules; }
	KeyRangeRef getKeyRange() const { return range; }
	static KeyRangeRef getModuleRange(SpecialKeySpace::MODULE module) { return moduleToBoundary.at(module); }
	static KeyRangeRef getManamentApiCommandRange(const std::string& command) {
		return managementApiCommandToRange.at(command);
	}
	static KeyRef getManagementApiCommandPrefix(const std::string& command) {
		return managementApiCommandToRange.at(command).begin;
	}
	static Key getManagementApiCommandOptionSpecialKey(const std::string& command, const std::string& option);
	static const std::set<std::string>& getManagementApiOptionsSet() { return options; }

private:
	ACTOR static Future<Optional<Value>> getActor(SpecialKeySpace* sks, ReadYourWritesTransaction* ryw, KeyRef key);

	ACTOR static Future<Standalone<RangeResultRef>> checkRYWValid(SpecialKeySpace* sks, ReadYourWritesTransaction* ryw,
	                                                              KeySelector begin, KeySelector end,
	                                                              GetRangeLimits limits, bool reverse);
	ACTOR static Future<Standalone<RangeResultRef>> getRangeAggregationActor(SpecialKeySpace* sks,
	                                                                         ReadYourWritesTransaction* ryw,
	                                                                         KeySelector begin, KeySelector end,
	                                                                         GetRangeLimits limits, bool reverse);

	KeyRangeMap<SpecialKeyRangeReadImpl*> readImpls;
	KeyRangeMap<SpecialKeySpace::MODULE> modules;
	KeyRangeMap<SpecialKeyRangeRWImpl*> writeImpls;
	KeyRange range; // key space range, (\xff\xff, \xff\xff\xff) in prod and (, \xff) in test

	static std::unordered_map<SpecialKeySpace::MODULE, KeyRange> moduleToBoundary;
	static std::unordered_map<std::string, KeyRange>
	    managementApiCommandToRange; // management command to its special keys' range
	static std::set<std::string> options; // "<command>/<option>"

	// Initialize module boundaries, used to handle cross_module_read
	void modulesBoundaryInit();
};

// Used for SpecialKeySpaceCorrectnessWorkload
class SKSCTestImpl : public SpecialKeyRangeRWImpl {
public:
	explicit SKSCTestImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

// Use special key prefix "\xff\xff/transaction/conflicting_keys/<some_key>",
// to retrieve keys which caused latest not_committed(conflicting with another transaction) error.
// The returned key value pairs are interpretted as :
// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
// Currently, the conflicting keyranges returned are original read_conflict_ranges or union of them.
class ConflictingKeysImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ConflictingKeysImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
};

class ReadConflictRangeImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ReadConflictRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
};

class WriteConflictRangeImpl : public SpecialKeyRangeReadImpl {
public:
	explicit WriteConflictRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
};

class DDStatsRangeImpl : public SpecialKeyRangeAsyncImpl {
public:
	explicit DDStatsRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
};

class ManagementCommandsOptionsImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ManagementCommandsOptionsImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class ExcludeServersRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ExcludeServersRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Key decode(const KeyRef& key) const override;
	Key encode(const KeyRef& key) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class FailedServersRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit FailedServersRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Key decode(const KeyRef& key) const override;
	Key encode(const KeyRef& key) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class ExclusionInProgressRangeImpl : public SpecialKeyRangeAsyncImpl {
public:
	explicit ExclusionInProgressRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
};

class ProcessClassRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ProcessClassRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class ProcessClassSourceRangeImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ProcessClassSourceRangeImpl(KeyRangeRef kr);
	Future<Standalone<RangeResultRef>> getRange(ReadYourWritesTransaction* ryw, KeyRangeRef kr) const override;
};

#include "flow/unactorcompiler.h"
#endif
