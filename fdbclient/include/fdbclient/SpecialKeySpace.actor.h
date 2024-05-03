/*
 * SpecialKeySpace.actor.h
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
	//
	// limitsHint can be used to reduce the amount of reading that the underlying
	// implementation needs to do.
	//
	// NOTE: care needs to be taken when using limitsHint. If the range in question
	// supports it, it is possible that some of the results may be removed when
	// merged with mutations from the same transaction. If that happens, the final
	// result may have fewer elements than the limit or even none at all if you didn't
	// read the entire range.
	//
	// TODO: implement the range reading in a loop so that the underlying implementation
	// can more naively fetch items up to the limit. If the merging deletes any entries,
	// then the next set of entries can be read.
	virtual Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                                     KeyRangeRef kr,
	                                     GetRangeLimits limitsHint) const = 0;

	explicit SpecialKeyRangeReadImpl(KeyRangeRef kr) : range(kr) {}
	KeyRangeRef getKeyRange() const { return range; }
	// true if the getRange call can emit more than one rpc calls,
	// we cache the results to keep consistency in the same getrange lifetime
	// TODO : give this function a more descriptive name
	virtual bool isAsync() const { return false; }

	virtual bool supportsTenants() const { return false; }

	virtual ~SpecialKeyRangeReadImpl() {}

protected:
	// underlying key range for this function
	KeyRange range;
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
	// all delayed async operations of writes in special-key-space
	virtual Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) = 0;

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

	~SpecialKeyRangeRWImpl() override {}
};

class SpecialKeyRangeAsyncImpl : public SpecialKeyRangeReadImpl {
public:
	explicit SpecialKeyRangeAsyncImpl(KeyRangeRef kr) : SpecialKeyRangeReadImpl(kr) {}

	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override = 0;

	// calling with a cache object to have consistent results if we need to call rpc
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint,
	                             KeyRangeMap<Optional<RangeResult>>* cache) const {
		return getRangeAsyncActor(this, ryw, kr, limitsHint, cache);
	}

	bool isAsync() const override { return true; }

	ACTOR static Future<RangeResult> getRangeAsyncActor(const SpecialKeyRangeReadImpl* skrAyncImpl,
	                                                    ReadYourWritesTransaction* ryw,
	                                                    KeyRangeRef kr,
	                                                    GetRangeLimits limits,
	                                                    KeyRangeMap<Optional<RangeResult>>* cache) {
		ASSERT(skrAyncImpl->getKeyRange().contains(kr));
		ASSERT(cache != nullptr);
		ASSERT(cache->rangeContaining(kr.begin) == cache->rangeContainingKeyBefore(kr.end));
		if (!(*cache)[kr.begin].present()) {
			// For simplicity, every time we need to cache, we read the whole range
			// Although sometimes the range can be narrowed,
			// there is not a general way to do it in complicated scenarios
			RangeResult result_ = wait(skrAyncImpl->getRange(ryw, skrAyncImpl->getKeyRange(), limits));
			cache->insert(skrAyncImpl->getKeyRange(), result_);
		}
		const auto& allResults = (*cache)[kr.begin].get();
		int start = 0, end = allResults.size();
		while (start < allResults.size() && allResults[start].key < kr.begin)
			++start;
		while (end > 0 && allResults[end - 1].key >= kr.end)
			--end;
		if (start < end) {
			RangeResult result = RangeResultRef(allResults.slice(start, end), false);
			result.arena().dependsOn(allResults.arena());
			return result;
		} else
			return RangeResult();
	}
};

class SpecialKeySpace {
public:
	enum class MODULE {
		ACTORLINEAGE, // Sampling data
		ACTOR_PROFILER_CONF, // profiler configuration
		CLUSTERFILEPATH,
		CLUSTERID, // An immutable UID for a cluster
		CONFIGURATION, // Configuration of the cluster
		CONNECTIONSTRING,
		ERRORMSG, // A single key space contains a json string which describes the last error in special-key-space
		GLOBALCONFIG, // Global configuration options synchronized to all nodes
		MANAGEMENT, // Management-API
		METRICS, // data-distribution metrics
		TESTONLY, // only used by correctness tests
		TRACING, // Distributed tracing options
		TRANSACTION, // transaction related info, conflicting keys, read/write conflict range
		STATUSJSON,
		UNKNOWN, // default value for all unregistered range
		WORKERINTERFACE,
		BULKLOADING,
	};

	enum class IMPLTYPE {
		READONLY, // The underlying special key range can only be called with get and getRange
		READWRITE // The underlying special key range can be called with get, getRange, set, clear
	};

	SpecialKeySpace(KeyRef spaceStartKey = Key(), KeyRef spaceEndKey = normalKeys.end, bool testOnly = true);

	Future<Optional<Value>> get(ReadYourWritesTransaction* ryw, const Key& key);

	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeySelector begin,
	                             KeySelector end,
	                             GetRangeLimits limits,
	                             Reverse = Reverse::False);

	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value);

	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range);

	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key);

	Future<Void> commit(ReadYourWritesTransaction* ryw);

	void registerKeyRange(SpecialKeySpace::MODULE module,
	                      SpecialKeySpace::IMPLTYPE type,
	                      const KeyRangeRef& kr,
	                      SpecialKeyRangeReadImpl* impl);

	Key decode(const KeyRef& key);
	KeyRange decode(const KeyRangeRef& kr);

	KeyRangeMap<SpecialKeyRangeReadImpl*>& getReadImpls() { return readImpls; }
	KeyRangeMap<SpecialKeyRangeRWImpl*>& getRWImpls() { return writeImpls; }
	KeyRangeMap<SpecialKeySpace::MODULE>& getModules() { return modules; }
	KeyRangeRef getKeyRange() const { return range; }
	static KeyRangeRef getModuleRange(SpecialKeySpace::MODULE module) { return moduleToBoundary.at(module); }
	static KeyRangeRef getManagementApiCommandRange(const std::string& command) {
		return managementApiCommandToRange.at(command);
	}
	static KeyRef getManagementApiCommandPrefix(const std::string& command) {
		return managementApiCommandToRange.at(command).begin;
	}
	static KeyRangeRef getActorLineageApiCommandRange(const std::string& command) {
		return actorLineageApiCommandToRange.at(command);
	}
	static KeyRef getActorLineageApiCommandPrefix(const std::string& command) {
		return actorLineageApiCommandToRange.at(command).begin;
	}
	static Key getManagementApiCommandOptionSpecialKey(const std::string& command, const std::string& option);
	static const std::set<std::string>& getManagementApiOptionsSet() { return options; }
	static const std::set<std::string>& getTracingOptions() { return tracingOptions; }

private:
	ACTOR static Future<Optional<Value>> getActor(SpecialKeySpace* sks, ReadYourWritesTransaction* ryw, KeyRef key);

	ACTOR static Future<RangeResult> checkRYWValid(SpecialKeySpace* sks,
	                                               ReadYourWritesTransaction* ryw,
	                                               KeySelector begin,
	                                               KeySelector end,
	                                               GetRangeLimits limits,
	                                               Reverse reverse);
	ACTOR static Future<RangeResult> getRangeAggregationActor(SpecialKeySpace* sks,
	                                                          ReadYourWritesTransaction* ryw,
	                                                          KeySelector begin,
	                                                          KeySelector end,
	                                                          GetRangeLimits limits,
	                                                          Reverse reverse);

	KeyRangeMap<SpecialKeyRangeReadImpl*> readImpls;
	KeyRangeMap<SpecialKeySpace::MODULE> modules;
	KeyRangeMap<SpecialKeyRangeRWImpl*> writeImpls;

	// key space range, (\xff\xff, \xff\xff\xff) in prod and (, \xff) in test
	KeyRange range;

	static std::unordered_map<SpecialKeySpace::MODULE, KeyRange> moduleToBoundary;

	// management command to its special keys' range
	static std::unordered_map<std::string, KeyRange> managementApiCommandToRange;
	static std::unordered_map<std::string, KeyRange> actorLineageApiCommandToRange;

	// "<command>/<option>"
	static std::set<std::string> options;
	static std::set<std::string> tracingOptions;

	// Initialize module boundaries, used to handle cross_module_read
	void modulesBoundaryInit();
};

// Used for SpecialKeySpaceCorrectnessWorkload
class SKSCTestRWImpl : public SpecialKeyRangeRWImpl {
public:
	explicit SKSCTestRWImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class SKSCTestAsyncReadImpl : public SpecialKeyRangeAsyncImpl {
public:
	explicit SKSCTestAsyncReadImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

// Use special key prefix "\xff\xff/transaction/conflicting_keys/<some_key>",
// to retrieve keys which caused latest not_committed(conflicting with another transaction) error.
// The returned key value pairs are interpreted as :
// prefix/<key1> : '1' - any keys equal or larger than this key are (probably) conflicting keys
// prefix/<key2> : '0' - any keys equal or larger than this key are (definitely) not conflicting keys
// Currently, the conflicting keyranges returned are original read_conflict_ranges or union of them.
class ConflictingKeysImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ConflictingKeysImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	bool supportsTenants() const override {
		CODE_PROBE(true, "Accessing conflicting keys in tenant");
		return true;
	};
};

class ReadConflictRangeImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ReadConflictRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	bool supportsTenants() const override {
		CODE_PROBE(true, "Accessing read conflict ranges in tenant");
		return true;
	};
};

class WriteConflictRangeImpl : public SpecialKeyRangeReadImpl {
public:
	explicit WriteConflictRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	bool supportsTenants() const override {
		CODE_PROBE(true, "Accessing write conflict ranges in tenant");
		return true;
	};
};

class DDStatsRangeImpl : public SpecialKeyRangeAsyncImpl {
public:
	explicit DDStatsRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class ManagementCommandsOptionsImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ManagementCommandsOptionsImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

// Special key management api for excluding localities (exclude_locality)
class ExcludedLocalitiesRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ExcludedLocalitiesRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Key decode(const KeyRef& key) const override;
	Key encode(const KeyRef& key) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

// Special key management api for excluding localities with failed option (failed_locality)
class FailedLocalitiesRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit FailedLocalitiesRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Key decode(const KeyRef& key) const override;
	Key encode(const KeyRef& key) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class ExcludeServersRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ExcludeServersRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Key decode(const KeyRef& key) const override;
	Key encode(const KeyRef& key) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class FailedServersRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit FailedServersRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Key decode(const KeyRef& key) const override;
	Key encode(const KeyRef& key) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class ExclusionInProgressRangeImpl : public SpecialKeyRangeAsyncImpl {
public:
	explicit ExclusionInProgressRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class ProcessClassRangeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ProcessClassRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class ProcessClassSourceRangeImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ProcessClassSourceRangeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class LockDatabaseImpl : public SpecialKeyRangeRWImpl {
public:
	explicit LockDatabaseImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class ConsistencyCheckImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ConsistencyCheckImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class GlobalConfigImpl : public SpecialKeyRangeRWImpl {
public:
	explicit GlobalConfigImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class TracingOptionsImpl : public SpecialKeyRangeRWImpl {
public:
	explicit TracingOptionsImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class CoordinatorsImpl : public SpecialKeyRangeRWImpl {
public:
	explicit CoordinatorsImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class CoordinatorsAutoImpl : public SpecialKeyRangeReadImpl {
public:
	explicit CoordinatorsAutoImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class AdvanceVersionImpl : public SpecialKeyRangeRWImpl {
public:
	explicit AdvanceVersionImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class VersionEpochImpl : public SpecialKeyRangeRWImpl {
public:
	explicit VersionEpochImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

// Deprecated as of 7.2
class ClientProfilingImpl : public SpecialKeyRangeRWImpl {
public:
	explicit ClientProfilingImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class ActorLineageImpl : public SpecialKeyRangeReadImpl {
public:
	explicit ActorLineageImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class ActorProfilerConf : public SpecialKeyRangeRWImpl {
	bool didWrite = false;
	std::map<std::string, std::string> config;

public:
	explicit ActorProfilerConf(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class MaintenanceImpl : public SpecialKeyRangeRWImpl {
public:
	explicit MaintenanceImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class DataDistributionImpl : public SpecialKeyRangeRWImpl {
public:
	explicit DataDistributionImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
};

class BulkLoadStatusImpl : public SpecialKeyRangeReadImpl {
public:
	explicit BulkLoadStatusImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class BulkLoadTaskImpl : public SpecialKeyRangeRWImpl {
public:
	explicit BulkLoadTaskImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class BulkLoadCancelImpl : public SpecialKeyRangeRWImpl {
public:
	explicit BulkLoadCancelImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class BulkLoadModeImpl : public SpecialKeyRangeRWImpl {
public:
	explicit BulkLoadModeImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
	void set(ReadYourWritesTransaction* ryw, const KeyRef& key, const ValueRef& value) override;
	Future<Optional<std::string>> commit(ReadYourWritesTransaction* ryw) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRangeRef& range) override;
	void clear(ReadYourWritesTransaction* ryw, const KeyRef& key) override;
};

class WorkerInterfacesSpecialKeyImpl : public SpecialKeyRangeReadImpl {
public:
	explicit WorkerInterfacesSpecialKeyImpl(KeyRangeRef kr);

	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

class FaultToleranceMetricsImpl : public SpecialKeyRangeReadImpl {
public:
	explicit FaultToleranceMetricsImpl(KeyRangeRef kr);
	Future<RangeResult> getRange(ReadYourWritesTransaction* ryw,
	                             KeyRangeRef kr,
	                             GetRangeLimits limitsHint) const override;
};

// If the underlying set of key-value pairs of a key space is not changing, then we expect repeating a read to give the
// same result. Additionally, we can generate the expected result of any read if that read is reading a subrange. This
// actor performs a read of an arbitrary subrange of [begin, end) and validates the results.
ACTOR Future<Void> validateSpecialSubrangeRead(ReadYourWritesTransaction* ryw,
                                               KeySelector begin,
                                               KeySelector end,
                                               GetRangeLimits limits,
                                               Reverse reverse,
                                               RangeResult result);

#include "flow/unactorcompiler.h"
#endif
