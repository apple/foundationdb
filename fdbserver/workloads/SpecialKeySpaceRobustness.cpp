/*
 * SpecialKeySpaceRobustness.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/SpecialKeySpace.h"
#include "fdbserver/tester/workloads.actor.h"
#include "flow/actorcompiler.h"

struct SpecialKeySpaceRobustnessWorkload : TestWorkload {
	static constexpr auto NAME = "SpecialKeySpaceRobustness";

	SpecialKeySpaceRobustnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> _setup(Database cx, SpecialKeySpaceRobustnessWorkload* self) { return Void(); }

	Future<Void> start(Database const& cx) override {
		// Only use one client to avoid potential conflicts on changing cluster configuration
		if (clientId == 0)
			co_await managementApiCorrectnessActor(cx, this);
	}

	bool getRangeResultInOrder(const RangeResult& result) {
		for (int i = 0; i < result.size() - 1; ++i) {
			if (result[i].key >= result[i + 1].key) {
				TraceEvent(SevError, "TestFailure")
				    .detail("Reason", "GetRangeResultNotInOrder")
				    .detail("Index", i)
				    .detail("Key1", result[i].key)
				    .detail("Key2", result[i + 1].key);
				return false;
			}
		}
		return true;
	}

	// A test utility that exclude the worker with `workerAddress` using `command` and return the value of `versionKey`.
	static Future<Optional<Value>> runExcludeAndGetVersionKey(Reference<ReadYourWritesTransaction> tx,
	                                                          std::string workerAddress,
	                                                          std::string command,
	                                                          KeyRef versionKey) {
		tx->reset();
		tx->setOption(FDBTransactionOptions::RAW_ACCESS);
		tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
		tx->set(Key(workerAddress).withPrefix(SpecialKeySpace::getManagementApiCommandPrefix(command)), ValueRef());
		co_await tx->commit();
		tx->reset();

		tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
		Optional<Value> versionKeyValue = co_await tx->get(versionKey);
		tx->reset();
		co_return versionKeyValue;
	}

	Future<Void> managementApiCorrectnessActor(Database cx, SpecialKeySpaceRobustnessWorkload* self) {
		// Management api related tests that can run during failure injections
		auto tx = makeReference<ReadYourWritesTransaction>(cx);
		// test ordered option keys
		{
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			for (const std::string& option : SpecialKeySpace::getManagementApiOptionsSet()) {
				tx->set(
				    "options/"_sr.withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)
				        .withSuffix(option),
				    ValueRef());
			}
			RangeResult result = co_await tx->getRange(
			    KeyRangeRef("options/"_sr, "options0"_sr)
			        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin),
			    CLIENT_KNOBS->TOO_MANY);
			ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(result.size() == SpecialKeySpace::getManagementApiOptionsSet().size());
			ASSERT(self->getRangeResultInOrder(result));
			tx->reset();
		}
		// "exclude" error message schema check
		Error err;
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->set("Invalid_Network_Address"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("exclude")),
			        ValueRef());
			co_await tx->commit();
			ASSERT(false);
		} catch (Error& e) {
			err = e;
		}
		if (err.code() == error_code_actor_cancelled)
			throw err;
		if (err.code() == error_code_special_keys_api_failure) {
			Optional<Value> errorMsg =
			    co_await tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin);
			ASSERT(errorMsg.present());
			std::string errorStr;
			auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
			auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
			// special_key_space_management_api_error_msg schema validation
			ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
			ASSERT(valueObj["command"].get_str() == "exclude" && !valueObj["retriable"].get_bool());
		} else {
			TraceEvent(SevDebug, "UnexpectedError").error(err).detail("Command", "Exclude");
			if (err.isValid()) {
				co_await tx->onError(err);
			}
		}
		tx->reset();
		// "Exclude" same address multiple times, and only the first excluson should trigger a system metadata update.
		{
			Error err;
			try {
				std::string excludeWorker;
				std::string excludeCommand;
				std::string failedCommand;
				KeyRef excludeVersionKey;
				KeyRef failedVersionKey;

				// Test exclude servers and exclude localities randomly.
				if (deterministicRandom()->coinflip()) {
					excludeWorker = "123.4.56.7:9876"; // Use a random address to not impact the cluster.
					excludeCommand = "exclude";
					failedCommand = "failed";
					excludeVersionKey = excludedServersVersionKey;
					failedVersionKey = failedServersVersionKey;
				} else {
					excludeWorker = "locality_zoneid:12345"; // Use a random locality to not impact the cluster.
					excludeCommand = "excludedlocality";
					failedCommand = "failedlocality";
					excludeVersionKey = excludedLocalityVersionKey;
					failedVersionKey = failedLocalityVersionKey;
				}

				TraceEvent(SevDebug, "ManagementAPITestExclude")
				    .detail("ExcludeWorker", excludeWorker)
				    .detail("ExcludeCommand", excludeCommand)
				    .detail("ExcludeFailedCommand", failedCommand);

				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> versionKey0 = co_await tx->get(excludeVersionKey);

				Optional<Value> versionKey1 =
				    co_await runExcludeAndGetVersionKey(tx, excludeWorker, excludeCommand, excludeVersionKey);
				ASSERT(versionKey1.present());
				ASSERT(versionKey0 != versionKey1);
				Optional<Value> versionKey2 =
				    co_await runExcludeAndGetVersionKey(tx, excludeWorker, excludeCommand, excludeVersionKey);
				ASSERT(versionKey2.present());
				// Exclude the same worker twice. The second exclusion shouldn't trigger a system metadata update.
				ASSERT(versionKey1 == versionKey2);

				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> versionKey3 = co_await tx->get(failedVersionKey);

				Optional<Value> versionKey4 =
				    co_await runExcludeAndGetVersionKey(tx, excludeWorker, failedCommand, failedVersionKey);
				ASSERT(versionKey4.present());
				ASSERT(versionKey3 != versionKey4);
				Optional<Value> versionKey5 =
				    co_await runExcludeAndGetVersionKey(tx, excludeWorker, failedCommand, failedVersionKey);
				ASSERT(versionKey5.present());
				// Exclude the same worker twice. The second exclusion shouldn't trigger a system metadata update.
				ASSERT(versionKey4 == versionKey5);

				tx->reset();
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				if (err.code() == error_code_actor_cancelled)
					throw err;
				if (err.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    co_await tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin);
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					// special_key_space_management_api_error_msg schema validation
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT((valueObj["command"].get_str() == "exclude" ||
					        valueObj["command"].get_str() == "exclude failed") &&
					       !valueObj["retriable"].get_bool());
				} else {
					TraceEvent(SevDebug, "UnexpectedError")
					    .error(err)
					    .detail("Command", "Exclude")
					    .detail("Test", "Repeated exclusions");
					if (err.isValid()) {
						co_await tx->onError(err);
					}
				}
			}
			tx->reset();
		}
		// "setclass"
		{
			Error err;
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// test getRange
				RangeResult result = co_await tx->getRange(
				    KeyRangeRef("process/class_type/"_sr, "process/class_type0"_sr)
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY);
				ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(result));
				// check correctness of classType of each process
				std::vector<ProcessData> workers = co_await getWorkers(&tx->getTransaction());
				if (workers.size()) {
					for (const auto& worker : workers) {
						Key addr =
						    Key("process/class_type/" + formatIpPort(worker.address.ip, worker.address.port))
						        .withPrefix(
						            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
						bool found = false;
						for (const auto& kv : result) {
							if (kv.key == addr) {
								ASSERT(kv.value.toString() == worker.processClass.toString());
								found = true;
								break;
							}
						}
						// Each process should find its corresponding element
						ASSERT(found);
					}
					ProcessData worker = deterministicRandom()->randomChoice(workers);
					Key addr =
					    Key("process/class_type/" + formatIpPort(worker.address.ip, worker.address.port))
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
					tx->set(addr, "InvalidProcessType"_sr);
					// test ryw
					Optional<Value> processType = co_await tx->get(addr);
					ASSERT(processType.present() && processType.get() == "InvalidProcessType"_sr);
					// test ryw disabled
					tx->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					Optional<Value> originalProcessType = co_await tx->get(addr);
					ASSERT(originalProcessType.present() &&
					       originalProcessType.get() == worker.processClass.toString());
					// test error handling (invalid value type)
					co_await tx->commit();
					ASSERT(false);
				} else {
					// If no worker process returned, skip the test
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest").log();
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				if (err.code() == error_code_actor_cancelled)
					throw err;
				if (err.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    co_await tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin);
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					// special_key_space_management_api_error_msg schema validation
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT(valueObj["command"].get_str() == "setclass" && !valueObj["retriable"].get_bool());
				} else {
					TraceEvent(SevDebug, "UnexpectedError").error(err).detail("Command", "Setclass");
					if (err.isValid()) {
						co_await tx->onError(err);
					}
				}
			}
			tx->reset();
		}
		// read class_source
		{
			Error err;
			try {
				// test getRange
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				RangeResult class_source_result = co_await tx->getRange(
				    KeyRangeRef("process/class_source/"_sr, "process/class_source0"_sr)
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY);
				ASSERT(!class_source_result.more && class_source_result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(class_source_result));
				// check correctness of classType of each process
				std::vector<ProcessData> workers = co_await getWorkers(&tx->getTransaction());
				if (workers.size()) {
					for (const auto& worker : workers) {
						Key addr =
						    Key("process/class_source/" + formatIpPort(worker.address.ip, worker.address.port))
						        .withPrefix(
						            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
						bool found = false;
						for (const auto& kv : class_source_result) {
							if (kv.key == addr) {
								ASSERT(kv.value.toString() == worker.processClass.sourceString());
								// Default source string is command_line
								ASSERT(kv.value == "command_line"_sr);
								found = true;
								break;
							}
						}
						// Each process should find its corresponding element
						ASSERT(found);
					}
					ProcessData worker = deterministicRandom()->randomChoice(workers);
					std::string address = formatIpPort(worker.address.ip, worker.address.port);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(
					    Key("process/class_type/" + address)
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
					    Value(worker.processClass.toString())); // Set it as the same class type as before, thus
					                                            // only class source will be changed
					co_await tx->commit();
					tx->reset();
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> class_source = co_await tx->get(
					    Key("process/class_source/" + address)
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin));
					TraceEvent(SevDebug, "SetClassSourceDebug")
					    .detail("Address", address)
					    .detail("Present", class_source.present())
					    .detail("ClassSource", class_source.present() ? class_source.get().toString() : "__Nothing");
					// Very rarely, we get an empty worker list, thus no class_source data
					if (class_source.present()) {
						if (class_source.get() == "command_line"_sr) {
							// if the process is rebooted after the commit,
							// the class source is changed back to command_line
							LocalityData _locality(worker.locality);
							std::vector<ProcessData> _workers = co_await getWorkers(&tx->getTransaction());
							bool found = false;
							for (const auto& w : _workers) {
								auto w_addr = formatIpPort(w.address.ip, w.address.port);
								if (w_addr == address) {
									ASSERT(w.locality.describeProcessId() != _locality.describeProcessId());
									found = true;
									break;
								}
							}
							ASSERT(found);
						} else {
							ASSERT(class_source.get() == "set_class"_sr);
						}
					}

					tx->reset();
				} else {
					// If no worker process returned, skip the test
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest").log();
				}
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tx->onError(err);
			}
		}
		// test lock and unlock
		// maske sure we lock the database
		while (true) {
			Error err;
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// lock the database
				UID uid = deterministicRandom()->randomUniqueID();
				tx->set(SpecialKeySpace::getManagementApiCommandPrefix("lock"), uid.toString());
				// commit
				co_await tx->commit();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent(SevDebug, "DatabaseLockFailure").error(err);
			// In case commit_unknown_result is thrown by buggify, we may try to lock more than once
			// The second lock commit will throw special_keys_api_failure error
			if (err.code() == error_code_special_keys_api_failure) {
				Optional<Value> errorMsg =
				    co_await tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin);
				ASSERT(errorMsg.present());
				std::string errorStr;
				auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
				auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
				// special_key_space_management_api_error_msg schema validation
				ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
				ASSERT(valueObj["command"].get_str() == "lock" && !valueObj["retriable"].get_bool());
				break;
			} else if (err.code() == error_code_database_locked) {
				// Database is already locked. This can happen if a previous attempt
				// failed with unknown_result.
				break;
			} else {
				co_await tx->onError(err);
			}
		}
		TraceEvent(SevDebug, "DatabaseLocked").log();
		// if database locked, fdb read should get database_locked error
		tx->reset();
		while (true) {
			Error err;
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				RangeResult res = co_await tx->getRange(normalKeys, 1);
			} catch (Error& e) {
				err = e;
			}
			if (!err.isValid()) {
				ASSERT(false);
			}
			if (err.code() == error_code_actor_cancelled)
				throw err;
			if (err.code() == error_code_grv_proxy_memory_limit_exceeded ||
			    err.code() == error_code_batch_transaction_throttled) {
				if (err.isValid()) {
					co_await tx->onError(err);
				}
			} else {
				ASSERT(err.code() == error_code_database_locked);
				break;
			}
		}
		// make sure we unlock the database
		// unlock is idempotent, thus we can commit many times until successful
		tx->reset();
		while (true) {
			Error err;
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// unlock the database
				tx->clear(SpecialKeySpace::getManagementApiCommandPrefix("lock"));
				co_await tx->commit();
				TraceEvent(SevDebug, "DatabaseUnlocked").log();
				break;
			} catch (Error& e) {
				err = e;
			}
			TraceEvent(SevDebug, "DatabaseUnlockFailure").error(err);
			ASSERT(err.code() != error_code_database_locked);
			co_await tx->onError(err);
		}

		tx->reset();
		while (true) {
			Error err;
			try {
				// read should be successful
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				RangeResult res = co_await tx->getRange(normalKeys, 1);
				break;
			} catch (Error& e) {
				err = e;
			}
			co_await tx->onError(err);
		}

		// test consistencycheck which only used by ConsistencyCheck Workload
		// Note: we have exclusive ownership of fdbShouldConsistencyCheckBeSuspended,
		// no existing workloads can modify the key
		tx->reset();
		{
			Error err;
			try {
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val1 = co_await tx->get(fdbShouldConsistencyCheckBeSuspended);
				bool ccSuspendSetting =
				    val1.present() ? BinaryReader::fromStringRef<bool>(val1.get(), Unversioned()) : false;
				Optional<Value> val2 =
				    co_await tx->get(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"));
				// Make sure the read result from special key consistency with the system key
				ASSERT(ccSuspendSetting ? val2.present() : !val2.present());
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// Make sure by default, consistencycheck is enabled
				ASSERT(!ccSuspendSetting);
				// Disable consistencycheck
				tx->set(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"), ValueRef());
				co_await tx->commit();
				tx->reset();
				// Read system key to make sure it is disabled
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val3 = co_await tx->get(fdbShouldConsistencyCheckBeSuspended);
				bool ccSuspendSetting2 =
				    val3.present() ? BinaryReader::fromStringRef<bool>(val3.get(), Unversioned()) : false;
				ASSERT(ccSuspendSetting2);
				tx->reset();
			} catch (Error& e) {
				err = e;
			}
			if (err.isValid()) {
				co_await tx->onError(err);
			}
		}
		// make sure we enable consistencycheck by the end
		{
			while (true) {
				Error err;
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->clear(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"));
					co_await tx->commit();
					tx->reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tx->onError(err);
			}
		}
		// coordinators
		// test read, makes sure it's the same as reading from coordinatorsKey
		while (true) {
			{
				Error err;
				try {
					tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
					Optional<Value> res = co_await tx->get(coordinatorsKey);
					ASSERT(res.present()); // Otherwise, database is in a bad state
					ClusterConnectionString cs(res.get().toString());
					Optional<Value> coordinator_processes_key = co_await tx->get(
					    "processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")));
					ASSERT(coordinator_processes_key.present());
					std::vector<std::string> process_addresses;
					boost::split(
					    process_addresses, coordinator_processes_key.get().toString(), [](char c) { return c == ','; });
					ASSERT(process_addresses.size() == cs.coords.size() + cs.hostnames.size());
					// compare the coordinator process network addresses one by one
					std::vector<NetworkAddress> coordinators = co_await cs.tryResolveHostnames();
					for (const auto& network_address : coordinators) {
						ASSERT(std::find(process_addresses.begin(),
						                 process_addresses.end(),
						                 network_address.toString()) != process_addresses.end());
					}
					tx->reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				co_await tx->onError(err);
			}
		} // advanceversion
		Error advanceVersionErr;
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			Version v1 = co_await tx->getReadVersion();
			TraceEvent(SevDebug, "InitialReadVersion").detail("Version", v1);
			Version v2 = 2 * v1;
			while (true) {
				{
					Error err;
					try {
						// loop until the grv is larger than the set version
						Version v3 = co_await tx->getReadVersion();
						if (v3 > v2) {
							TraceEvent(SevDebug, "AdvanceVersionSuccess").detail("Version", v3);
							break;
						}
						tx->setOption(FDBTransactionOptions::RAW_ACCESS);
						tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
						// force the cluster to recover at v2
						tx->set(SpecialKeySpace::getManagementApiCommandPrefix("advanceversion"), std::to_string(v2));
						co_await tx->commit();
						ASSERT(false); // Should fail with commit_unknown_result
					} catch (Error& e) {
						err = e;
					}
					TraceEvent(SevDebug, "AdvanceVersionCommitFailure").error(err);
					if (err.isValid()) {
						co_await tx->onError(err);
					}
				}
			}
			tx->reset();
		} catch (Error& e) {
			advanceVersionErr = e;
		}
		if (advanceVersionErr.isValid()) {
			co_await tx->onError(advanceVersionErr);
		}
		// make sure when we change dd related special keys, we grab the two system keys,
		// i.e. moveKeysLockOwnerKey and moveKeysLockWriteKey
		{
			Reference<ReadYourWritesTransaction> tr1(new ReadYourWritesTransaction(cx));
			Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(cx));
			while (true) {
				Error err;
				try {
					tr1->setOption(FDBTransactionOptions::RAW_ACCESS);
					tr1->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tr2->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

					Version readVersion = co_await tr1->getReadVersion();
					tr2->setVersion(readVersion);
					KeyRef ddPrefix = SpecialKeySpace::getManagementApiCommandPrefix("datadistribution");
					tr1->set("mode"_sr.withPrefix(ddPrefix), "1"_sr);
					co_await tr1->commit();
					// randomly read the moveKeysLockOwnerKey/moveKeysLockWriteKey
					// both of them should be grabbed when changing dd mode
					co_await tr2->get(deterministicRandom()->coinflip() ? moveKeysLockOwnerKey : moveKeysLockWriteKey);
					// tr2 should never succeed, just write to a key to make it not a read-only transaction
					tr2->addWriteConflictRange(singleKeyRange(""_sr));
					co_await tr2->commit();
					ASSERT(false); // commit should always fail due to conflict
				} catch (Error& e) {
					err = e;
				}
				if (err.code() != error_code_not_committed) {
					// when buggify is enabled, it's possible we get other retriable errors
					if (err.isValid()) {
						co_await tr2->onError(err);
					}
					tr1->reset();
				} else {
					// loop until we get conflict error
					break;
				}
			}
		}
	}
};

WorkloadFactory<SpecialKeySpaceRobustnessWorkload> SpecialKeySpaceRobustnessFactory;
