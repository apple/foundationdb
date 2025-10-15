/*
 * TagPartitionedLogSystemRecoveryTests.actor.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/TagPartitionedLogSystem.actor.h"
#include "flow/UnitTest.h"

#include "flow/actorcompiler.h" // This must be the last #include.

namespace {

Reference<LogSet> makeSingleLogSet(const std::vector<TLogInterface>& tlogs, bool isLocal = true) {
	Reference<LogSet> logSet = makeReference<LogSet>();
	logSet->isLocal = isLocal;
	for (const auto& tlog : tlogs) {
		logSet->logServers.push_back(
		    makeReference<AsyncVar<OptionalInterface<TLogInterface>>>(OptionalInterface<TLogInterface>(tlog)));
	}
	return logSet;
}

std::tuple<int, std::vector<TLogLockResult>, bool> makeLogGroupResults(
    int replicationFactor,
    const std::vector<std::vector<UnknownCommittedVersions>>& perTLogUCV,
    const std::vector<TLogInterface>& tlogs,
    bool nonAvailableTLogsCompletePolicy = true,
    const std::vector<Version>& knownCommitted = {}) {
	std::vector<TLogLockResult> lockResults;
	lockResults.reserve(tlogs.size());
	for (int i = 0; i < tlogs.size(); ++i) {
		TLogLockResult result;
		result.logId = tlogs[i].id();
		result.knownCommittedVersion = (i < knownCommitted.size()) ? knownCommitted[i] : 0;
		for (const auto& ucv : perTLogUCV[i]) {
			result.unknownCommittedVersions.push_back(ucv);
		}
		lockResults.push_back(result);
	}
	return std::make_tuple(replicationFactor, std::move(lockResults), nonAvailableTLogsCompletePolicy);
}

} // namespace

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/Simple") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Construct two local tLogs backed by a single LogSet.
	// Both tLogs have known committed version 100 and report a higher durable
	// version 110 that was sent to both log servers.
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv }, { ucv } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "SimpleTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	if (recoverVersion != 110) {
		TraceEvent(SevError, "SimpleTestRecoverVersionFailed").detail("Expected", 110).detail("Got", recoverVersion);
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/FallbackToMaxKCV") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// When no unknown committed versions are reported, should fall back to maxKCV
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	auto logGroupResults = makeLogGroupResults(2, { {}, {} }, { tlogA, tlogB }, true, { 80, 90 });

	Version minDV = 70;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 90) {
		TraceEvent(SevError, "FallbackTestMaxKCVFailed").detail("Expected", 90).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 90);

	if (recoverVersion != 90) {
		TraceEvent(SevError, "FallbackTestRecoverVersionFailed").detail("Expected", 90).detail("Got", recoverVersion);
	}
	ASSERT(recoverVersion == 90);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/HaltOnMissingDelivery") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// When an available tLog didn't receive a version, recovery should halt at the previous version
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucvLate(120, 110, std::vector<uint16_t>{ 0, 1 });
	// Only tlogA reports the 120 version (tlogB missed it).
	auto logGroupResults = makeLogGroupResults(2, { { ucv, ucvLate }, { ucv } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "MissingDeliveryTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Because not all available tLogs received 120, the recovery version should stay at 110.
	if (recoverVersion != 110) {
		TraceEvent(SevError, "MissingDeliveryTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "tlogB did not receive version 120");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/PolicyNotSatisfied") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// When a version was sent to both tLogs but only received by one (insufficient for RF=2)
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucv2(120, 110, std::vector<uint16_t>{ 0, 1 });
	// Version 120 was sent to BOTH tLogs but only tlogA received it.
	// With replication factor 2, we need both to receive it.
	auto logGroupResults = makeLogGroupResults(2, { { ucv, ucv2 }, { ucv } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "PolicyNotSatisfiedTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	if (recoverVersion != 110) {
		TraceEvent(SevError, "PolicyNotSatisfiedTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "Version 120 sent to both tLogs but only received by 1 (RF=2)");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/MinDVRespected") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that recovery version respects maxKCV when minDV < maxKCV
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	UnknownCommittedVersions ucv(95, 90, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv }, { ucv } }, { tlogA, tlogB }, true, { 90, 90 });

	Version minDV = 80;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 90) {
		TraceEvent(SevError, "MinDVRespectedTestMaxKCVFailed").detail("Expected", 90).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 90);

	if (recoverVersion != 95) {
		TraceEvent(SevError, "MinDVRespectedTestRecoverVersionFailed")
		    .detail("Expected", 95)
		    .detail("Got", recoverVersion)
		    .detail("MinDV", minDV)
		    .detail("MaxKCV", maxKCV);
	}
	ASSERT(recoverVersion == 95);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/BrokenChain") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that recovery halts when prevVersion chain is broken
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	// Version 110 and 120 sent to both, but 120's prevVersion != 110 (broken chain)
	UnknownCommittedVersions ucv110(110, 100, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucv120(120, 115, std::vector<uint16_t>{ 0, 1 }); // prevVersion=115, not 110!
	auto logGroupResults =
	    makeLogGroupResults(2, { { ucv110, ucv120 }, { ucv110, ucv120 } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "BrokenChainTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Should stop at 110 because prevVersion chain breaks at 120
	if (recoverVersion != 110) {
		TraceEvent(SevError, "BrokenChainTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "Version 120 has prevVersion=115, expected 110");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/MultipleLogSets") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests recovery with multiple LogSets (primary + satellite)
	LocalityData locality;
	TLogInterface primary1(locality), primary2(locality);
	TLogInterface satellite1(locality), satellite2(locality);

	// Two LogSets: primary (local) + satellite (non-local)
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ primary1, primary2 }, true),
		                                       makeSingleLogSet({ satellite1, satellite2 }, false) };

	// Only the 2 primary tLogs report version 110 (satellite LogSet is non-local and not in logGroupResults)
	UnknownCommittedVersions ucv(110, 100, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv }, { ucv } }, { primary1, primary2 }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "MultipleLogSetsTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	if (recoverVersion != 110) {
		TraceEvent(SevError, "MultipleLogSetsTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("NumLogSets", logServers.size());
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/PartialAvailabilityPolicyFail") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that available tLogs must satisfy replication policy
	LocalityData locality;
	TLogInterface tlogA(locality), tlogB(locality), tlogC(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB, tlogC }) };

	// Version 120 sent to all 3, but only 2 of 3 received it (not enough for RF=3)
	UnknownCommittedVersions ucv110(110, 100, std::vector<uint16_t>{ 0, 1, 2 });
	UnknownCommittedVersions ucv120(120, 110, std::vector<uint16_t>{ 0, 1, 2 });
	// Only tlogA and tlogB report receiving 120
	auto logGroupResults = makeLogGroupResults(
	    3, { { ucv110, ucv120 }, { ucv110, ucv120 }, { ucv110 } }, { tlogA, tlogB, tlogC }, false, { 100, 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "PartialAvailabilityTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Should stay at 110 because 120 doesn't satisfy RF=3
	if (recoverVersion != 110) {
		TraceEvent(SevError, "PartialAvailabilityTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "Only 2 of 3 tLogs received version 120 (RF=3)");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/VersionsBelowMaxKCV") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that versions <= maxKCV are filtered out
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	// Report version 80 (below maxKCV=100), should be ignored
	UnknownCommittedVersions ucv80(80, 70, std::vector<uint16_t>{ 0, 1 });
	auto logGroupResults = makeLogGroupResults(2, { { ucv80 }, { ucv80 } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "VersionsBelowMaxKCVTestMaxKCVFailed").detail("Expected", 100).detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Should fall back to maxKCV since all UCVs are <= maxKCV
	if (recoverVersion != 100) {
		TraceEvent(SevError, "VersionsBelowMaxKCVTestRecoverVersionFailed")
		    .detail("Expected", 100)
		    .detail("Got", recoverVersion)
		    .detail("Reason", "All UCVs below maxKCV should be filtered");
	}
	ASSERT(recoverVersion == 100);
	return Void();
}

TEST_CASE("/TagPartitionedLogSystem/GetRecoverVersionUnicast/RandomVersionsPartialDelivery") {
	if (!SERVER_KNOBS->ENABLE_VERSION_VECTOR_TLOG_UNICAST) {
		return Void();
	}

	// Tests that recovery handles random versions in range (maxKCV, highest_version) that:
	// 1. Are not reported at all (missing from UCVs)
	// 2. Are only received by a subset of logs (partial delivery)
	LocalityData locality;
	TLogInterface tlogA(locality);
	TLogInterface tlogB(locality);
	std::vector<Reference<LogSet>> logServers{ makeSingleLogSet({ tlogA, tlogB }) };

	// Setup: maxKCV=100, potential versions 110, 112, 115, 118, 120, 125
	// Version 110: received by both (full delivery)
	// Version 112: NOT REPORTED at all (missing from UCVs)
	// Version 115: only received by tlogA (partial delivery - indicated by tLogLocIds={0})
	// Version 118: NOT REPORTED at all (missing from UCVs)
	// Version 120: received by both (full delivery)
	// Version 125: only received by tlogA (partial delivery - indicated by tLogLocIds={0})

	UnknownCommittedVersions ucv110(110, 100, std::vector<uint16_t>{ 0, 1 });
	// Version 112 is missing - not in any UCV list
	UnknownCommittedVersions ucv115(115, 110, std::vector<uint16_t>{ 0 }); // Only tlogA (loc 0)
	// Version 118 is missing - not in any UCV list
	UnknownCommittedVersions ucv120(120, 115, std::vector<uint16_t>{ 0, 1 });
	UnknownCommittedVersions ucv125(125, 120, std::vector<uint16_t>{ 0 }); // Only tlogA (loc 0)

	// tlogA reports versions 110, 115, 120, 125
	// tlogB only reports versions 110, 120 (missing 115, 125)
	auto logGroupResults = makeLogGroupResults(
	    2, { { ucv110, ucv115, ucv120, ucv125 }, { ucv110, ucv120 } }, { tlogA, tlogB }, true, { 100, 100 });

	Version minDV = 90;
	Optional<std::tuple<Version, Version>> result = getRecoverVersionUnicast(logServers, logGroupResults, minDV);
	ASSERT(result.present());
	Version maxKCV = std::get<0>(result.get());
	Version recoverVersion = std::get<1>(result.get());

	if (maxKCV != 100) {
		TraceEvent(SevError, "RandomVersionsPartialDeliveryTestMaxKCVFailed")
		    .detail("Expected", 100)
		    .detail("Got", maxKCV);
	}
	ASSERT(maxKCV == 100);

	// Recovery should stop at 110 because:
	// - 110 was received by both tLogs (satisfies RF=2)
	// - 115 was only received by tlogA (tLogLocIds={0}, doesn't satisfy RF=2)
	// Even though 120 was received by both, the prevVersion chain requires 115 first
	if (recoverVersion != 110) {
		TraceEvent(SevError, "RandomVersionsPartialDeliveryTestRecoverVersionFailed")
		    .detail("Expected", 110)
		    .detail("Got", recoverVersion)
		    .detail("Reason",
		            "Version 115 received by only tlogA (subset), breaks recovery before 120. "
		            "Missing versions (112, 118) also demonstrate gaps in the range.");
	}
	ASSERT(recoverVersion == 110);
	return Void();
}
