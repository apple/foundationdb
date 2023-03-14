/*
 * SpecialKeySpaceRobustness.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/ReadYourWrites.h"
#include "fdbclient/Schemas.h"
#include "fdbclient/SpecialKeySpace.actor.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "flow/actorcompiler.h"

struct SpecialKeySpaceRobustnessWorkload : TestWorkload {
	static constexpr auto NAME = "SpecialKeySpaceRobustness";

	SpecialKeySpaceRobustnessWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {}

	Future<Void> setup(Database const& cx) override { return _setup(cx, this); }
	Future<Void> start(Database const& cx) override { return _start(cx, this); }
	Future<bool> check(Database const& cx) override { return true; }
	void getMetrics(std::vector<PerfMetric>& m) override {}

	Future<Void> _setup(Database cx, SpecialKeySpaceRobustnessWorkload* self) { return Void(); }

	ACTOR Future<Void> _start(Database cx, SpecialKeySpaceRobustnessWorkload* self) {
		// Only use one client to avoid potential conflicts on changing cluster configuration
		if (self->clientId == 0)
			wait(self->managementApiCorrectnessActor(cx, self));
		return Void();
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

	ACTOR Future<Void> managementApiCorrectnessActor(Database cx, SpecialKeySpaceRobustnessWorkload* self) {
		// Management api related tests that can run during failure injections
		state Reference<ReadYourWritesTransaction> tx = makeReference<ReadYourWritesTransaction>(cx);
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
			RangeResult result = wait(tx->getRange(
			    KeyRangeRef("options/"_sr, "options0"_sr)
			        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin),
			    CLIENT_KNOBS->TOO_MANY));
			ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
			ASSERT(result.size() == SpecialKeySpace::getManagementApiOptionsSet().size());
			ASSERT(self->getRangeResultInOrder(result));
			tx->reset();
		}
		// "exclude" error message shema check
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
			tx->set("Invalid_Network_Address"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("exclude")),
			        ValueRef());
			wait(tx->commit());
			ASSERT(false);
		} catch (Error& e) {
			if (e.code() == error_code_actor_cancelled)
				throw;
			if (e.code() == error_code_special_keys_api_failure) {
				Optional<Value> errorMsg =
				    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
				ASSERT(errorMsg.present());
				std::string errorStr;
				auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
				auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
				// special_key_space_management_api_error_msg schema validation
				ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
				ASSERT(valueObj["command"].get_str() == "exclude" && !valueObj["retriable"].get_bool());
			} else {
				TraceEvent(SevDebug, "UnexpectedError").error(e).detail("Command", "Exclude");
				wait(tx->onError(e));
			}
			tx->reset();
		}
		// "setclass"
		{
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// test getRange
				state RangeResult result = wait(tx->getRange(
				    KeyRangeRef("process/class_type/"_sr, "process/class_type0"_sr)
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY));
				ASSERT(!result.more && result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(result));
				// check correctness of classType of each process
				std::vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
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
					state ProcessData worker = deterministicRandom()->randomChoice(workers);
					state Key addr =
					    Key("process/class_type/" + formatIpPort(worker.address.ip, worker.address.port))
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin);
					tx->set(addr, "InvalidProcessType"_sr);
					// test ryw
					Optional<Value> processType = wait(tx->get(addr));
					ASSERT(processType.present() && processType.get() == "InvalidProcessType"_sr);
					// test ryw disabled
					tx->setOption(FDBTransactionOptions::READ_YOUR_WRITES_DISABLE);
					Optional<Value> originalProcessType = wait(tx->get(addr));
					ASSERT(originalProcessType.present() &&
					       originalProcessType.get() == worker.processClass.toString());
					// test error handling (invalid value type)
					wait(tx->commit());
					ASSERT(false);
				} else {
					// If no worker process returned, skip the test
					TraceEvent(SevDebug, "EmptyWorkerListInSetClassTest").log();
				}
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				if (e.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					// special_key_space_management_api_error_msg schema validation
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT(valueObj["command"].get_str() == "setclass" && !valueObj["retriable"].get_bool());
				} else {
					TraceEvent(SevDebug, "UnexpectedError").error(e).detail("Command", "Setclass");
					wait(tx->onError(e));
				}
				tx->reset();
			}
		}
		// read class_source
		{
			try {
				// test getRange
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				state RangeResult class_source_result = wait(tx->getRange(
				    KeyRangeRef("process/class_source/"_sr, "process/class_source0"_sr)
				        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
				    CLIENT_KNOBS->TOO_MANY));
				ASSERT(!class_source_result.more && class_source_result.size() < CLIENT_KNOBS->TOO_MANY);
				ASSERT(self->getRangeResultInOrder(class_source_result));
				// check correctness of classType of each process
				std::vector<ProcessData> workers = wait(getWorkers(&tx->getTransaction()));
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
					state std::string address = formatIpPort(worker.address.ip, worker.address.port);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->set(
					    Key("process/class_type/" + address)
					        .withPrefix(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin),
					    Value(worker.processClass.toString())); // Set it as the same class type as before, thus only
					                                            // class source will be changed
					wait(tx->commit());
					tx->reset();
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> class_source = wait(tx->get(
					    Key("process/class_source/" + address)
					        .withPrefix(
					            SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::CONFIGURATION).begin)));
					TraceEvent(SevDebug, "SetClassSourceDebug")
					    .detail("Address", address)
					    .detail("Present", class_source.present())
					    .detail("ClassSource", class_source.present() ? class_source.get().toString() : "__Nothing");
					// Very rarely, we get an empty worker list, thus no class_source data
					if (class_source.present()) {
						if (class_source.get() == "command_line"_sr) {
							// if the process is rebooted after the commit,
							// the class source is changed back to command_line
							state LocalityData _locality(worker.locality);
							std::vector<ProcessData> _workers = wait(getWorkers(&tx->getTransaction()));
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
				wait(tx->onError(e));
			}
		}
		// test lock and unlock
		// maske sure we lock the database
		loop {
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// lock the database
				UID uid = deterministicRandom()->randomUniqueID();
				tx->set(SpecialKeySpace::getManagementApiCommandPrefix("lock"), uid.toString());
				// commit
				wait(tx->commit());
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "DatabaseLockFailure").error(e);
				// In case commit_unknown_result is thrown by buggify, we may try to lock more than once
				// The second lock commit will throw special_keys_api_failure error
				if (e.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    wait(tx->get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					// special_key_space_management_api_error_msg schema validation
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT(valueObj["command"].get_str() == "lock" && !valueObj["retriable"].get_bool());
					break;
				} else if (e.code() == error_code_database_locked) {
					// Database is already locked. This can happen if a previous attempt
					// failed with unknown_result.
					break;
				} else {
					wait(tx->onError(e));
				}
			}
		}
		TraceEvent(SevDebug, "DatabaseLocked").log();
		// if database locked, fdb read should get database_locked error
		tx->reset();
		loop {
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				RangeResult res = wait(tx->getRange(normalKeys, 1));
			} catch (Error& e) {
				if (e.code() == error_code_actor_cancelled)
					throw;
				if (e.code() == error_code_grv_proxy_memory_limit_exceeded ||
				    e.code() == error_code_batch_transaction_throttled) {
					wait(tx->onError(e));
				} else {
					ASSERT(e.code() == error_code_database_locked);
					break;
				}
			}
		}
		// make sure we unlock the database
		// unlock is idempotent, thus we can commit many times until successful
		tx->reset();
		loop {
			try {
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// unlock the database
				tx->clear(SpecialKeySpace::getManagementApiCommandPrefix("lock"));
				wait(tx->commit());
				TraceEvent(SevDebug, "DatabaseUnlocked").log();
				break;
			} catch (Error& e) {
				TraceEvent(SevDebug, "DatabaseUnlockFailure").error(e);
				ASSERT(e.code() != error_code_database_locked);
				wait(tx->onError(e));
			}
		}

		tx->reset();
		loop {
			try {
				// read should be successful
				tx->setOption(FDBTransactionOptions::RAW_ACCESS);
				RangeResult res = wait(tx->getRange(normalKeys, 1));
				break;
			} catch (Error& e) {
				wait(tx->onError(e));
			}
		}

		// test consistencycheck which only used by ConsistencyCheck Workload
		// Note: we have exclusive ownership of fdbShouldConsistencyCheckBeSuspended,
		// no existing workloads can modify the key
		tx->reset();
		{
			try {
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val1 = wait(tx->get(fdbShouldConsistencyCheckBeSuspended));
				state bool ccSuspendSetting =
				    val1.present() ? BinaryReader::fromStringRef<bool>(val1.get(), Unversioned()) : false;
				Optional<Value> val2 =
				    wait(tx->get(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck")));
				// Make sure the read result from special key consistency with the system key
				ASSERT(ccSuspendSetting ? val2.present() : !val2.present());
				tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				// Make sure by default, consistencycheck is enabled
				ASSERT(!ccSuspendSetting);
				// Disable consistencycheck
				tx->set(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"), ValueRef());
				wait(tx->commit());
				tx->reset();
				// Read system key to make sure it is disabled
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> val3 = wait(tx->get(fdbShouldConsistencyCheckBeSuspended));
				bool ccSuspendSetting2 =
				    val3.present() ? BinaryReader::fromStringRef<bool>(val3.get(), Unversioned()) : false;
				ASSERT(ccSuspendSetting2);
				tx->reset();
			} catch (Error& e) {
				wait(tx->onError(e));
			}
		}
		// make sure we enable consistencycheck by the end
		{
			loop {
				try {
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tx->clear(SpecialKeySpace::getManagementApiCommandPrefix("consistencycheck"));
					wait(tx->commit());
					tx->reset();
					break;
				} catch (Error& e) {
					wait(tx->onError(e));
				}
			}
		}
		// coordinators
		// test read, makes sure it's the same as reading from coordinatorsKey
		loop {
			try {
				tx->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);
				Optional<Value> res = wait(tx->get(coordinatorsKey));
				ASSERT(res.present()); // Otherwise, database is in a bad state
				state ClusterConnectionString cs(res.get().toString());
				Optional<Value> coordinator_processes_key = wait(
				    tx->get("processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators"))));
				ASSERT(coordinator_processes_key.present());
				state std::vector<std::string> process_addresses;
				boost::split(
				    process_addresses, coordinator_processes_key.get().toString(), [](char c) { return c == ','; });
				ASSERT(process_addresses.size() == cs.coords.size() + cs.hostnames.size());
				// compare the coordinator process network addresses one by one
				std::vector<NetworkAddress> coordinators = wait(cs.tryResolveHostnames());
				for (const auto& network_address : coordinators) {
					ASSERT(std::find(process_addresses.begin(), process_addresses.end(), network_address.toString()) !=
					       process_addresses.end());
				}
				tx->reset();
				break;
			} catch (Error& e) {
				wait(tx->onError(e));
			}
		}
		// advanceversion
		try {
			tx->setOption(FDBTransactionOptions::RAW_ACCESS);
			Version v1 = wait(tx->getReadVersion());
			TraceEvent(SevDebug, "InitialReadVersion").detail("Version", v1);
			state Version v2 = 2 * v1;
			loop {
				try {
					// loop until the grv is larger than the set version
					Version v3 = wait(tx->getReadVersion());
					if (v3 > v2) {
						TraceEvent(SevDebug, "AdvanceVersionSuccess").detail("Version", v3);
						break;
					}
					tx->setOption(FDBTransactionOptions::RAW_ACCESS);
					tx->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					// force the cluster to recover at v2
					tx->set(SpecialKeySpace::getManagementApiCommandPrefix("advanceversion"), std::to_string(v2));
					wait(tx->commit());
					ASSERT(false); // Should fail with commit_unknown_result
				} catch (Error& e) {
					TraceEvent(SevDebug, "AdvanceVersionCommitFailure").error(e);
					wait(tx->onError(e));
				}
			}
			tx->reset();
		} catch (Error& e) {
			wait(tx->onError(e));
		}
		// make sure when we change dd related special keys, we grab the two system keys,
		// i.e. moveKeysLockOwnerKey and moveKeysLockWriteKey
		{
			state Reference<ReadYourWritesTransaction> tr1(new ReadYourWritesTransaction(cx));
			state Reference<ReadYourWritesTransaction> tr2(new ReadYourWritesTransaction(cx));
			loop {
				try {
					tr1->setOption(FDBTransactionOptions::RAW_ACCESS);
					tr1->setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
					tr2->setOption(FDBTransactionOptions::READ_SYSTEM_KEYS);

					Version readVersion = wait(tr1->getReadVersion());
					tr2->setVersion(readVersion);
					KeyRef ddPrefix = SpecialKeySpace::getManagementApiCommandPrefix("datadistribution");
					tr1->set("mode"_sr.withPrefix(ddPrefix), "1"_sr);
					wait(tr1->commit());
					// randomly read the moveKeysLockOwnerKey/moveKeysLockWriteKey
					// both of them should be grabbed when changing dd mode
					wait(success(
					    tr2->get(deterministicRandom()->coinflip() ? moveKeysLockOwnerKey : moveKeysLockWriteKey)));
					// tr2 should never succeed, just write to a key to make it not a read-only transaction
					tr2->addWriteConflictRange(singleKeyRange(""_sr));
					wait(tr2->commit());
					ASSERT(false); // commit should always fail due to conflict
				} catch (Error& e) {
					if (e.code() != error_code_not_committed) {
						// when buggify is enabled, it's possible we get other retriable errors
						wait(tr2->onError(e));
						tr1->reset();
					} else {
						// loop until we get conflict error
						break;
					}
				}
			}
		}
		return Void();
	}
};

WorkloadFactory<SpecialKeySpaceRobustnessWorkload> SpecialKeySpaceRobustnessFactory;
