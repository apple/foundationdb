/*
 * sim_validation.cpp
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

#include "sim_validation.h"
#include "fdbrpc/TraceFileIO.h"
#include "flow/network.h"
#include "fdbrpc/simulator.h"

// used for simulation validations
static std::map<std::string, int64_t> validationData;
static std::map<int64_t, double> timedVersionsValidationData;
static std::set<UID> disabledMachines;

void debug_setVersionCheckEnabled(UID uid, bool enabled) {
	if (enabled)
		disabledMachines.erase(uid);
	else {
		disabledMachines.insert(uid);
		debug_removeVersions(uid);
	}
}

void debug_advanceCommittedVersions(UID id, int64_t minVersion, int64_t maxVersion) {
	debug_advanceMinCommittedVersion(id, minVersion);
	debug_advanceMaxCommittedVersion(id, maxVersion);
}

void debug_advanceVersion(UID id, int64_t version, const char* suffix) {
	if (!disabledMachines.count(id)) {
		auto& entry = validationData[id.toString() + suffix];
		if (version > entry)
			entry = version;
	}
}

void debug_advanceMinCommittedVersion(UID id, int64_t version) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return;
	debug_advanceVersion(id, version, "min");
}

void debug_advanceMaxCommittedVersion(UID id, int64_t version) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return;
	debug_advanceVersion(id, version, "max");
}

bool debug_checkPartRestoredVersion(UID id,
                                    int64_t version,
                                    std::string context,
                                    std::string minormax,
                                    Severity sev = SevError) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return false;
	if (disabledMachines.count(id))
		return false;
	if (!validationData.count(id.toString() + minormax)) {
		TraceEvent(SevWarn, (context + "UnknownVersion").c_str(), id).detail("RestoredVersion", version);
		return false;
	}
	int sign = minormax == "min" ? 1 : -1;
	if (version * sign < validationData[id.toString() + minormax] * sign) {
		TraceEvent(sev, (context + "DurabilityError").c_str(), id)
		    .detail("RestoredVersion", version)
		    .detail("Checking", minormax)
		    .detail("MinVersion", validationData[id.toString() + "min"])
		    .detail("MaxVersion", validationData[id.toString() + "max"]);
		return true;
	}
	return false;
}

bool debug_checkRestoredVersion(UID id, int64_t version, std::string context, Severity sev) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return false;
	return debug_checkPartRestoredVersion(id, version, context, "min", sev) ||
	       debug_checkPartRestoredVersion(id, version, context, "max", sev);
}

void debug_removeVersions(UID id) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return;
	validationData.erase(id.toString() + "min");
	validationData.erase(id.toString() + "max");
}

bool debug_versionsExist(UID id) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return false;
	return validationData.count(id.toString() + "min") != 0 || validationData.count(id.toString() + "max") != 0;
}

bool debug_checkMinRestoredVersion(UID id, int64_t version, std::string context, Severity sev) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return false;
	return debug_checkPartRestoredVersion(id, version, context, "min", sev);
}

bool debug_checkMaxRestoredVersion(UID id, int64_t version, std::string context, Severity sev) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return false;
	return debug_checkPartRestoredVersion(id, version, context, "max", sev);
}

static bool checkRelocationDuration;

bool debug_isCheckRelocationDuration() {
	return checkRelocationDuration;
}

void debug_setCheckRelocationDuration(bool check) {
	checkRelocationDuration = check;
}
void debug_advanceVersionTimestamp(int64_t version, double t) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return;
	timedVersionsValidationData[version] = t;
}

bool debug_checkVersionTime(int64_t version, double t, std::string context, Severity sev) {
	if (!g_network->isSimulated() || g_simulator.extraDB)
		return false;
	if (!timedVersionsValidationData.count(version)) {
		TraceEvent(SevWarn, (context + "UnknownTime").c_str())
		    .detail("VersionChecking", version)
		    .detail("TimeChecking", t);
		return false;
	}
	if (t > timedVersionsValidationData[version]) {
		TraceEvent(sev, (context + "VersionTimeError").c_str())
		    .detail("VersionChecking", version)
		    .detail("TimeChecking", t)
		    .detail("MaxTime", timedVersionsValidationData[version]);
		return true;
	}
	return false;
}