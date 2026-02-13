/*
 * ChangeConfig.actor.cpp
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

#include "fdbclient/ManagementAPI.actor.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbserver/workloads/workloads.actor.h"
#include "fdbrpc/simulator.h"
#include "flow/actorcompiler.h" // This must be the last #include.

struct ChangeConfigWorkload : TestWorkload {
	static constexpr auto NAME = "ChangeConfig";
	double minDelayBeforeChange, maxDelayBeforeChange;
	std::string configMode;
	std::string networkAddresses;
	int coordinatorChanges;

	ChangeConfigWorkload(WorkloadContext const& wcx) : TestWorkload(wcx) {
		minDelayBeforeChange = getOption(options, "minDelayBeforeChange"_sr, 0.0);
		maxDelayBeforeChange = getOption(options, "maxDelayBeforeChange"_sr, 0.0);
		ASSERT(maxDelayBeforeChange >= minDelayBeforeChange);

		configMode = getOption(options, "configMode"_sr, StringRef()).toString();
		networkAddresses = getOption(options, "coordinators"_sr, StringRef()).toString();
		coordinatorChanges = getOption(options, "coordinatorChanges"_sr, 1);
		if (networkAddresses != "auto") {
			coordinatorChanges = 1;
		}
	}

	void disableFailureInjectionWorkloads(std::set<std::string>& out) const override { out.insert("all"); }

	Future<Void> start(Database const& cx) override {
		if (this->clientId != 0) {
			return Void();
		}
		return changeConfigClient(cx->clone(), this);
	}

	Future<bool> check(Database const& cx) override { return true; }

	void getMetrics(std::vector<PerfMetric>& m) override {}

	std::string getConfigMode(const std::string& mode, bool existingDB) {
		std::string res = mode;
		if (existingDB) {
			size_t pos = res.find("new ");
			if (pos != std::string::npos) {
				res.replace(pos, 4, "");
			}
		}
		return res;
	}

	ACTOR static Future<Void> configureExtraDatabase(ChangeConfigWorkload* self, Database db) {
		wait(delay(5 * deterministicRandom()->random01()));
		if (self->configMode.size()) {
			state bool existingDB = false;
			if (g_simulator->startingDisabledConfiguration != "") {
				wait(success(
				    ManagementAPI::changeConfig(db.getReference(), g_simulator->startingDisabledConfiguration, true)));
				TraceEvent("WaitForReplicasExtra").log();
				wait(waitForFullReplication(db));
				TraceEvent("WaitForReplicasExtraEnd").log();
				existingDB = true;
			}
			std::string mode = self->getConfigMode(self->configMode, existingDB);
			wait(success(ManagementAPI::changeConfig(db.getReference(), mode, true)));
		}
		if (self->networkAddresses.size()) {
			if (self->networkAddresses == "auto") {
				wait(coordinatorsChangeActor(db, self, true));
			} else {
				wait(coordinatorsChangeActor(db, self));
			}
		}

		wait(delay(5 * deterministicRandom()->random01()));
		return Void();
	}

	Future<Void> configureExtraDatabases(ChangeConfigWorkload* self) {
		std::vector<Future<Void>> futures;
		if (g_network->isSimulated()) {
			for (auto extraDatabase : g_simulator->extraDatabases) {
				Database db = Database::createSimulatedExtraDatabase(extraDatabase);
				futures.push_back(configureExtraDatabase(self, db));
			}
		}
		return waitForAll(futures);
	}

	ACTOR static Future<Void> changeConfigClient(Database cx, ChangeConfigWorkload* self) {
		wait(delay(self->minDelayBeforeChange +
		           deterministicRandom()->random01() * (self->maxDelayBeforeChange - self->minDelayBeforeChange)));

		state bool extraConfigureBefore = deterministicRandom()->random01() < 0.5;
		if (extraConfigureBefore) {
			wait(self->configureExtraDatabases(self));
		}

		if (self->configMode.size()) {
			state bool existingDB = false;
			if (g_network->isSimulated() && g_simulator->startingDisabledConfiguration != "") {
				wait(success(
				    ManagementAPI::changeConfig(cx.getReference(), g_simulator->startingDisabledConfiguration, true)));
				TraceEvent("WaitForReplicas").log();
				wait(waitForFullReplication(cx));
				TraceEvent("WaitForReplicasEnd").log();
				existingDB = true;
			}
			std::string mode = self->getConfigMode(self->configMode, existingDB);
			wait(success(ManagementAPI::changeConfig(cx.getReference(), mode, true)));
		}

		if (self->networkAddresses.size()) {
			state int i;
			for (i = 0; i < self->coordinatorChanges; ++i) {
				if (i > 0) {
					wait(delay(20));
				}
				wait(coordinatorsChangeActor(cx, self, self->networkAddresses == "auto"));
			}
		}

		if (!extraConfigureBefore) {
			wait(self->configureExtraDatabases(self));
		}

		return Void();
	}

	ACTOR static Future<Void> coordinatorsChangeActor(Database cx,
	                                                  ChangeConfigWorkload* self,
	                                                  bool autoChange = false) {
		state ReadYourWritesTransaction tr(cx);
		state int notEnoughMachineResults = 0;
		state std::string desiredCoordinatorsKey;

		if (autoChange) {
			loop {
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> newCoordinatorsKey = wait(tr.get("auto_coordinators"_sr.withPrefix(
					    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin)));
					ASSERT(newCoordinatorsKey.present());
					desiredCoordinatorsKey = newCoordinatorsKey.get().toString();
					tr.reset();
					break;
				} catch (Error& e) {
					if (e.code() == error_code_special_keys_api_failure) {
						Optional<Value> errorMsg =
						    wait(tr.get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
						ASSERT(errorMsg.present());
						std::string errorStr;
						auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
						auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
						TraceEvent(SevDebug, "GetAutoCoordinatorsChange")
						    .detail("ErrorMessage", valueObj["message"].get_str());
						ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
						ASSERT(valueObj["command"].get_str() == "auto_coordinators");
						if (valueObj["retriable"].get_bool() && notEnoughMachineResults < 1) {
							notEnoughMachineResults++;
							wait(delay(1.0));
							tr.reset();
						} else {
							break;
						}
					} else {
						wait(tr.onError(e));
					}
					wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
				}
			}
		} else {
			desiredCoordinatorsKey = self->networkAddresses;
		}

		loop {
			try {
				tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr.set("processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
				       Value(desiredCoordinatorsKey));
				TraceEvent(SevDebug, "CoordinatorsChangeBeforeCommit")
				    .detail("Auto", autoChange)
				    .detail("NewCoordinatorsKey", describe(desiredCoordinatorsKey));
				wait(tr.commit());
				ASSERT(false);
			} catch (Error& e) {
				state Error err(e);
				if (e.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    wait(tr.get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin));
					ASSERT(errorMsg.present());
					std::string errorStr;
					auto valueObj = readJSONStrictly(errorMsg.get().toString()).get_obj();
					auto schema = readJSONStrictly(JSONSchemas::managementApiErrorSchema.toString()).get_obj();
					TraceEvent(SevDebug, "CoordinatorsChangeError")
					    .detail("Auto", autoChange)
					    .detail("ErrorMessage", valueObj["message"].get_str());
					ASSERT(schemaMatch(schema, valueObj, errorStr, SevError, true));
					ASSERT(valueObj["command"].get_str() == "coordinators");
					break;
				} else {
					wait(tr.onError(err));
				}
				wait(delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY));
			}
		}
		return Void();
	}
};

WorkloadFactory<ChangeConfigWorkload> ChangeConfigWorkloadFactory;
