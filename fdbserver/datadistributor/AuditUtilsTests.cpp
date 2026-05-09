/*
 * AuditUtilsTests.cpp
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

#include "fdbclient/AuditUtils.h"
#include "fdbclient/FDBTypes.h"
#include "flow/UnitTest.h"

void forceLinkAuditUtilsTests() {}

TEST_CASE("/AuditUtils/coalesceRangeList") {
	// Empty input
	{
		auto result = coalesceRangeList({});
		ASSERT(result.empty());
	}

	// Single range unchanged
	{
		KeyRange r = Standalone(KeyRangeRef("a"_sr, "c"_sr));
		auto result = coalesceRangeList({ r });
		ASSERT(result.size() == 1);
		ASSERT(result[0] == r);
	}

	// Non-overlapping ranges preserved
	{
		KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "b"_sr));
		KeyRange r2 = Standalone(KeyRangeRef("d"_sr, "e"_sr));
		auto result = coalesceRangeList({ r2, r1 });
		ASSERT(result.size() == 2);
		ASSERT(result[0] == r1);
		ASSERT(result[1] == r2);
	}

	// Overlapping ranges merged
	{
		KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "d"_sr));
		KeyRange r2 = Standalone(KeyRangeRef("c"_sr, "f"_sr));
		auto result = coalesceRangeList({ r2, r1 });
		ASSERT(result.size() == 1);
		ASSERT(result[0].begin == "a"_sr);
		ASSERT(result[0].end == "f"_sr);
	}

	// Adjacent ranges merged
	{
		KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "c"_sr));
		KeyRange r2 = Standalone(KeyRangeRef("c"_sr, "f"_sr));
		auto result = coalesceRangeList({ r1, r2 });
		ASSERT(result.size() == 1);
		ASSERT(result[0].begin == "a"_sr);
		ASSERT(result[0].end == "f"_sr);
	}

	// Contained range absorbed
	{
		KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "f"_sr));
		KeyRange r2 = Standalone(KeyRangeRef("b"_sr, "d"_sr));
		auto result = coalesceRangeList({ r1, r2 });
		ASSERT(result.size() == 1);
		ASSERT(result[0] == r1);
	}

	return Void();
}

TEST_CASE("/AuditUtils/rangesSame") {
	// Both empty — no mismatch
	{
		auto result = rangesSame({}, {});
		ASSERT(!result.present());
	}

	// One empty, one not — mismatch
	{
		KeyRange r = Standalone(KeyRangeRef("a"_sr, "c"_sr));
		auto result = rangesSame({}, { r });
		ASSERT(result.present());
		ASSERT(result.get().first.empty());
		ASSERT(result.get().second == r);
	}

	// Identical single range — no mismatch
	{
		KeyRange r = Standalone(KeyRangeRef("a"_sr, "c"_sr));
		auto result = rangesSame({ r }, { r });
		ASSERT(!result.present());
	}

	// Different split points covering same range — no mismatch
	{
		KeyRange whole = Standalone(KeyRangeRef("a"_sr, "f"_sr));
		KeyRange left = Standalone(KeyRangeRef("a"_sr, "c"_sr));
		KeyRange right = Standalone(KeyRangeRef("c"_sr, "f"_sr));
		auto result = rangesSame({ whole }, { left, right });
		ASSERT(!result.present());
	}

	// Different begin — mismatch
	{
		KeyRange rA = Standalone(KeyRangeRef("a"_sr, "f"_sr));
		KeyRange rB = Standalone(KeyRangeRef("b"_sr, "f"_sr));
		auto result = rangesSame({ rA }, { rB });
		ASSERT(result.present());
	}

	// Different end — mismatch
	{
		KeyRange rA = Standalone(KeyRangeRef("a"_sr, "e"_sr));
		KeyRange rB = Standalone(KeyRangeRef("a"_sr, "f"_sr));
		auto result = rangesSame({ rA }, { rB });
		ASSERT(result.present());
	}

	// Gap in one list — mismatch
	{
		KeyRange rA1 = Standalone(KeyRangeRef("a"_sr, "b"_sr));
		KeyRange rA2 = Standalone(KeyRangeRef("d"_sr, "f"_sr));
		KeyRange rB = Standalone(KeyRangeRef("a"_sr, "f"_sr));
		auto result = rangesSame({ rA1, rA2 }, { rB });
		ASSERT(result.present());
	}

	return Void();
}

TEST_CASE("/AuditUtils/checkLocationMetadataConsistency/Consistent") {
	UID server1(1, 0);
	UID server2(2, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));
	KeyRange r2 = Standalone(KeyRangeRef("m"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapKS;
	mapKS[server1] = { r1 };
	mapKS[server2] = { r2 };

	std::unordered_map<UID, std::vector<KeyRange>> mapSK;
	mapSK[server1] = { r1 };
	mapSK[server2] = { r2 };

	auto errors = checkLocationMetadataConsistency(mapKS, mapSK, claimRange);
	ASSERT(errors.empty());

	return Void();
}

TEST_CASE("/AuditUtils/checkLocationMetadataConsistency/ServerInKeyServersNotServerKeys") {
	UID server1(1, 0);
	UID server2(2, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapKS;
	mapKS[server1] = { r1 };
	mapKS[server2] = { r1 };

	std::unordered_map<UID, std::vector<KeyRange>> mapSK;
	mapSK[server1] = { r1 };
	// server2 missing from ServerKeys

	auto errors = checkLocationMetadataConsistency(mapKS, mapSK, claimRange);
	ASSERT(errors.size() == 1);
	ASSERT(errors[0].message.find("KeyServers but not ServerKeys") != std::string::npos);

	return Void();
}

TEST_CASE("/AuditUtils/checkLocationMetadataConsistency/ServerInServerKeysNotKeyServers") {
	UID server1(1, 0);
	UID server2(2, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapKS;
	mapKS[server1] = { r1 };
	// server2 missing from KeyServers

	std::unordered_map<UID, std::vector<KeyRange>> mapSK;
	mapSK[server1] = { r1 };
	mapSK[server2] = { r1 };

	auto errors = checkLocationMetadataConsistency(mapKS, mapSK, claimRange);
	ASSERT(errors.size() == 1);
	ASSERT(errors[0].message.find("ServerKeys but not KeyServers") != std::string::npos);

	return Void();
}

TEST_CASE("/AuditUtils/checkLocationMetadataConsistency/RangeMismatch") {
	UID server1(1, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapKS;
	mapKS[server1] = { Standalone(KeyRangeRef("a"_sr, "m"_sr)) };

	std::unordered_map<UID, std::vector<KeyRange>> mapSK;
	mapSK[server1] = { Standalone(KeyRangeRef("a"_sr, "f"_sr)) };

	auto errors = checkLocationMetadataConsistency(mapKS, mapSK, claimRange);
	ASSERT(errors.size() == 1);
	ASSERT(errors[0].message.find("mismatch on Server") != std::string::npos);

	return Void();
}

TEST_CASE("/AuditUtils/checkLocationMetadataConsistency/MultipleErrors") {
	UID server1(1, 0);
	UID server2(2, 0);
	UID server3(3, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange r1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapKS;
	mapKS[server1] = { r1 };
	// server2 only in KS
	mapKS[server2] = { r1 };

	std::unordered_map<UID, std::vector<KeyRange>> mapSK;
	mapSK[server1] = { Standalone(KeyRangeRef("a"_sr, "f"_sr)) };
	// server3 only in SK
	mapSK[server3] = { r1 };

	auto errors = checkLocationMetadataConsistency(mapKS, mapSK, claimRange);
	// server1: range mismatch, server2: in KS not SK, server3: in SK not KS
	ASSERT(errors.size() == 3);

	int ksNotSk = 0, skNotKs = 0, rangeMismatch = 0;
	for (const auto& e : errors) {
		if (e.message.find("KeyServers but not ServerKeys") != std::string::npos)
			ksNotSk++;
		else if (e.message.find("ServerKeys but not KeyServers") != std::string::npos)
			skNotKs++;
		else if (e.message.find("mismatch on Server") != std::string::npos)
			rangeMismatch++;
	}
	ASSERT(ksNotSk == 1);
	ASSERT(skNotKs == 1);
	ASSERT(rangeMismatch == 1);

	return Void();
}

TEST_CASE("/AuditUtils/checkLocationMetadataConsistency/BothEmpty") {
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	std::unordered_map<UID, std::vector<KeyRange>> mapKS;
	std::unordered_map<UID, std::vector<KeyRange>> mapSK;

	auto errors = checkLocationMetadataConsistency(mapKS, mapSK, claimRange);
	ASSERT(errors.empty());

	return Void();
}

TEST_CASE("/AuditUtils/LocationMetadataProductionPath/Consistent") {
	UID server1(1, 0);
	UID server2(2, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange shard1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));
	KeyRange shard2 = Standalone(KeyRangeRef("m"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServersRaw;
	mapFromKeyServersRaw[server1] = { shard1 };
	mapFromKeyServersRaw[server2] = { shard2 };

	std::unordered_map<UID, std::vector<KeyRange>> serverOwnRangesMap;
	serverOwnRangesMap[server1] = { shard1 };
	serverOwnRangesMap[server2] = { shard2 };

	auto builtMaps = buildLocationMetadataMaps(mapFromKeyServersRaw, serverOwnRangesMap, claimRange);

	auto errors = checkLocationMetadataConsistency(builtMaps.fromKeyServers, builtMaps.fromServerKeys, claimRange);
	ASSERT(errors.empty());

	return Void();
}

TEST_CASE("/AuditUtils/LocationMetadataProductionPath/CorruptMissingServerKeys") {
	UID server1(1, 0);
	UID server2(2, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange shard1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));
	KeyRange shard2 = Standalone(KeyRangeRef("m"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServersRaw;
	mapFromKeyServersRaw[server1] = { shard1 };
	mapFromKeyServersRaw[server2] = { shard2 };

	std::unordered_map<UID, std::vector<KeyRange>> serverOwnRangesMap;
	serverOwnRangesMap[server1] = { shard1 };

	auto builtMaps = buildLocationMetadataMaps(mapFromKeyServersRaw, serverOwnRangesMap, claimRange);

	auto errors = checkLocationMetadataConsistency(builtMaps.fromKeyServers, builtMaps.fromServerKeys, claimRange);
	ASSERT(!errors.empty());
	bool found = false;
	for (const auto& e : errors) {
		if (e.message.find("KeyServers but not ServerKeys") != std::string::npos)
			found = true;
	}
	ASSERT(found);

	return Void();
}

TEST_CASE("/AuditUtils/LocationMetadataProductionPath/CorruptPhantomServer") {
	UID server1(1, 0);
	UID server2(2, 0);
	UID server3(3, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange shard1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));
	KeyRange shard2 = Standalone(KeyRangeRef("m"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServersRaw;
	mapFromKeyServersRaw[server1] = { shard1 };
	mapFromKeyServersRaw[server2] = { shard2 };

	std::unordered_map<UID, std::vector<KeyRange>> serverOwnRangesMap;
	serverOwnRangesMap[server1] = { shard1 };
	serverOwnRangesMap[server2] = { shard2 };
	serverOwnRangesMap[server3] = { shard1 };

	auto builtMaps = buildLocationMetadataMaps(mapFromKeyServersRaw, serverOwnRangesMap, claimRange);

	auto errors = checkLocationMetadataConsistency(builtMaps.fromKeyServers, builtMaps.fromServerKeys, claimRange);
	ASSERT(!errors.empty());
	bool found = false;
	for (const auto& e : errors) {
		if (e.message.find("ServerKeys but not KeyServers") != std::string::npos)
			found = true;
	}
	ASSERT(found);

	return Void();
}

TEST_CASE("/AuditUtils/LocationMetadataProductionPath/CorruptShiftedBoundary") {
	UID server1(1, 0);
	UID server2(2, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange shard1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));
	KeyRange shard2 = Standalone(KeyRangeRef("m"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServersRaw;
	mapFromKeyServersRaw[server1] = { shard1 };
	mapFromKeyServersRaw[server2] = { shard2 };

	std::unordered_map<UID, std::vector<KeyRange>> serverOwnRangesMap;
	serverOwnRangesMap[server1] = { Standalone(KeyRangeRef("a"_sr, "g"_sr)) };
	serverOwnRangesMap[server2] = { shard2 };

	auto builtMaps = buildLocationMetadataMaps(mapFromKeyServersRaw, serverOwnRangesMap, claimRange);

	auto errors = checkLocationMetadataConsistency(builtMaps.fromKeyServers, builtMaps.fromServerKeys, claimRange);
	ASSERT(!errors.empty());
	bool found = false;
	for (const auto& e : errors) {
		if (e.message.find("mismatch on Server") != std::string::npos)
			found = true;
	}
	ASSERT(found);

	return Void();
}

TEST_CASE("/AuditUtils/LocationMetadataProductionPath/CorruptPartialOwnership") {
	UID server1(1, 0);
	KeyRange claimRange = Standalone(KeyRangeRef("a"_sr, "z"_sr));
	KeyRange shard1 = Standalone(KeyRangeRef("a"_sr, "m"_sr));
	KeyRange shard2 = Standalone(KeyRangeRef("m"_sr, "z"_sr));

	std::unordered_map<UID, std::vector<KeyRange>> mapFromKeyServersRaw;
	mapFromKeyServersRaw[server1] = { shard1, shard2 };

	std::unordered_map<UID, std::vector<KeyRange>> serverOwnRangesMap;
	serverOwnRangesMap[server1] = { shard1 };

	auto builtMaps = buildLocationMetadataMaps(mapFromKeyServersRaw, serverOwnRangesMap, claimRange);

	auto errors = checkLocationMetadataConsistency(builtMaps.fromKeyServers, builtMaps.fromServerKeys, claimRange);
	ASSERT(!errors.empty());
	bool found = false;
	for (const auto& e : errors) {
		if (e.message.find("mismatch on Server") != std::string::npos)
			found = true;
	}
	ASSERT(found);

	return Void();
}
