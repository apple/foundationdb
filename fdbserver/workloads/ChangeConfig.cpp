/*
 * ChangeConfig.cpp
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

#include "fdbclient/ManagementAPI.h"
#include "fdbclient/NativeAPI.actor.h"
#include "fdbclient/Schemas.h"
#include "fdbserver/tester/workloads.h"
#include "fdbrpc/simulator.h"

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
		return changeConfigClient(cx->clone());
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

	Future<Void> configureExtraDatabase(Database db) {
		co_await delay(5 * deterministicRandom()->random01());
		if (!configMode.empty()) {
			bool existingDB = false;
			if (!fdbSimulationPolicyState().startingDisabledConfiguration.empty()) {
				co_await ManagementAPI::changeConfig(
				    db.getReference(), fdbSimulationPolicyState().startingDisabledConfiguration, true);
				TraceEvent("WaitForReplicasExtra").log();
				co_await waitForFullReplication(db);
				TraceEvent("WaitForReplicasExtraEnd").log();
				existingDB = true;
			}
			std::string mode = getConfigMode(configMode, existingDB);
			co_await ManagementAPI::changeConfig(db.getReference(), mode, true);
		}
		if (!networkAddresses.empty()) {
			if (networkAddresses == "auto") {
				co_await coordinatorsChangeActor(db, true);
			} else {
				co_await coordinatorsChangeActor(db);
			}
		}

		co_await delay(5 * deterministicRandom()->random01());
	}

	Future<Void> configureExtraDatabases() {
		std::vector<Future<Void>> futures;
		if (g_network->isSimulated()) {
			for (const auto& extraDatabase : g_simulator->extraDatabases) {
				Database db = Database::createSimulatedExtraDatabase(extraDatabase);
				futures.push_back(configureExtraDatabase(db));
			}
		}
		return waitForAll(futures);
	}

	Future<Void> changeConfigClient(Database cx) {
		co_await delay(minDelayBeforeChange +
		               deterministicRandom()->random01() * (maxDelayBeforeChange - minDelayBeforeChange));

		bool extraConfigureBefore = deterministicRandom()->random01() < 0.5;
		if (extraConfigureBefore) {
			co_await configureExtraDatabases();
		}

		if (!configMode.empty()) {
			bool existingDB = false;
			if (g_network->isSimulated() && !fdbSimulationPolicyState().startingDisabledConfiguration.empty()) {
				co_await ManagementAPI::changeConfig(
				    cx.getReference(), fdbSimulationPolicyState().startingDisabledConfiguration, true);
				TraceEvent("WaitForReplicas").log();
				co_await waitForFullReplication(cx);
				TraceEvent("WaitForReplicasEnd").log();
				existingDB = true;
			}
			std::string mode = getConfigMode(configMode, existingDB);
			co_await ManagementAPI::changeConfig(cx.getReference(), mode, true);
		}

		if (!networkAddresses.empty()) {
			for (int i = 0; i < coordinatorChanges; ++i) {
				if (i > 0) {
					co_await delay(20);
				}
				co_await coordinatorsChangeActor(cx, networkAddresses == "auto");
			}
		}

		if (!extraConfigureBefore) {
			co_await configureExtraDatabases();
		}
	}

	Future<Void> coordinatorsChangeActor(Database cx, bool autoChange = false) {
		ReadYourWritesTransaction tr(cx);
		int notEnoughMachineResults = 0;
		std::string desiredCoordinatorsKey;

		if (autoChange) {
			while (true) {
				Error err;
				try {
					tr.setOption(FDBTransactionOptions::RAW_ACCESS);
					Optional<Value> newCoordinatorsKey = co_await tr.get("auto_coordinators"_sr.withPrefix(
					    SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::MANAGEMENT).begin));
					ASSERT(newCoordinatorsKey.present());
					desiredCoordinatorsKey = newCoordinatorsKey.get().toString();
					tr.reset();
					break;
				} catch (Error& e) {
					err = e;
				}
				if (err.code() == error_code_special_keys_api_failure) {
					Optional<Value> errorMsg =
					    co_await tr.get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin);
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
						co_await delay(1.0);
						tr.reset();
					} else {
						break;
					}
				} else {
					co_await tr.onError(err);
				}
				co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
			}
		} else {
			desiredCoordinatorsKey = networkAddresses;
		}

		while (true) {
			Error caughtErr;
			try {
				tr.setOption(FDBTransactionOptions::SPECIAL_KEY_SPACE_ENABLE_WRITES);
				tr.set("processes"_sr.withPrefix(SpecialKeySpace::getManagementApiCommandPrefix("coordinators")),
				       Value(desiredCoordinatorsKey));
				TraceEvent(SevDebug, "CoordinatorsChangeBeforeCommit")
				    .detail("Auto", autoChange)
				    .detail("NewCoordinatorsKey", describe(desiredCoordinatorsKey));
				co_await tr.commit();
				ASSERT(false);
			} catch (Error& e) {
				caughtErr = e;
			}
			Error err(caughtErr);
			if (caughtErr.code() == error_code_special_keys_api_failure) {
				Optional<Value> errorMsg =
				    co_await tr.get(SpecialKeySpace::getModuleRange(SpecialKeySpace::MODULE::ERRORMSG).begin);
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
				if (err.isValid()) {
					co_await tr.onError(err);
				}
			}
			co_await delay(FLOW_KNOBS->PREVENT_FAST_SPIN_DELAY);
		}
	}
};

WorkloadFactory<ChangeConfigWorkload> ChangeConfigWorkloadFactory;
