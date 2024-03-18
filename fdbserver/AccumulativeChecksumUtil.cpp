/*
 * AccumulativeChecksumUtil.cpp
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

#include "fdbserver/AccumulativeChecksumUtil.h"
#include "fdbserver/Knobs.h"

uint16_t getCommitProxyAccumulativeChecksumIndex(uint16_t commitProxyIndex) {
	// We leave flexibility in acs index generated from different components
	// Acs index ends with 1 indicates the mutation is from a commit proxy
	return commitProxyIndex * 10 + 1;
}

uint32_t calculateAccumulativeChecksum(uint32_t currentAccumulativeChecksum, uint32_t newChecksum) {
	return currentAccumulativeChecksum ^ newChecksum;
}

bool tagSupportAccumulativeChecksum(Tag tag) {
	return tag.locality > 0;
}

uint32_t AccumulativeChecksumBuilder::update(Tag tag, uint32_t checksum, Version version) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint32_t newAcs = 0;
	if (acsTable.find(tag) == acsTable.end()) {
		newAcs = checksum;
	} else {
		ASSERT(acsTable[tag].isValid());
		ASSERT(version >= acsTable[tag].version);
		ASSERT(version >= currentVersion);
		newAcs = calculateAccumulativeChecksum(acsTable[tag].acs, checksum);
	}
	acsTable[tag] = AccumulativeChecksumState(newAcs, version);
	currentVersion = version;
	return newAcs;
}

void AccumulativeChecksumBuilder::resetTag(Tag tag) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(tag) != acsTable.end()) {
		acsTable.erase(tag);
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsBuilderResetAccumulativeChecksum")
			    .detail("AcsIndex", acsIndex)
			    .detail("AcsTag", tag);
		}
	}
}

Optional<AccumulativeChecksumState> AccumulativeChecksumBuilder::get(Tag tag) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(tag) == acsTable.end()) {
		return Optional<AccumulativeChecksumState>();
	}
	return acsTable[tag];
}

Optional<std::pair<uint32_t, uint32_t>> AccumulativeChecksumValidator::updateAcs(uint16_t acsIndex, uint32_t checksum) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint32_t newAcs = 0;
	uint32_t oldAcs = 0;
	Version newVersion = 0;
	if (acsTable.find(acsIndex) == acsTable.end()) {
		newAcs = checksum;
	} else {
		ASSERT(acsTable[acsIndex].isValid());
		if (acsTable[acsIndex].restoring) {
			if (acsCacheTableForRestore.find(acsIndex) == acsCacheTableForRestore.end()) {
				acsCacheTableForRestore[acsIndex] = acsTable[acsIndex];
			}
			uint32_t newVersionCache = acsCacheTableForRestore[acsIndex].version;
			Version newAcsCache = calculateAccumulativeChecksum(acsCacheTableForRestore[acsIndex].acs, checksum);
			acsCacheTableForRestore[acsIndex] = AccumulativeChecksumState(newAcsCache, newVersionCache);
			return Optional<std::pair<uint32_t, uint32_t>>();
		}
		newVersion = acsTable[acsIndex].version;
		oldAcs = acsTable[acsIndex].acs;
		newAcs = calculateAccumulativeChecksum(acsTable[acsIndex].acs, checksum);
	}
	acsTable[acsIndex] = AccumulativeChecksumState(newAcs, newVersion);
	return std::make_pair(newAcs, oldAcs);
}

void AccumulativeChecksumValidator::updateVersion(uint16_t acsIndex, Version version) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsTable.find(acsIndex) != acsTable.end());
	acsTable[acsIndex] = AccumulativeChecksumState(acsTable[acsIndex].acs, version);
}

void AccumulativeChecksumValidator::completeRestore(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsTable.find(acsIndex) != acsTable.end());
	acsTable[acsIndex].restoring = false;
}

bool AccumulativeChecksumValidator::isRestoring(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsTable.find(acsIndex) != acsTable.end());
	return acsTable[acsIndex].restoring;
}

bool AccumulativeChecksumValidator::validate(uint16_t acsIndex, uint32_t acsValueToCheck, Tag tag) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(acsIndex) == acsTable.end() || acsTable[acsIndex].acs != acsValueToCheck) {
		TraceEvent e(SevError, "SSAccumulativeChecksumValidateError0");
		e.detail("AcsTag", tag);
		e.detail("AcsIndex", acsIndex);
		e.detail("AcsValueToCheck", acsValueToCheck);
		if (acsTable.find(acsIndex) == acsTable.end()) {
			e.detail("Reason", "AcsIndexNotPresented");
		} else {
			e.detail("Reason", "AcsValueMismatch");
			e.detail("AcsInTable", acsTable[acsIndex].acs);
		}
		return false;
	}
	return true;
}

Version AccumulativeChecksumValidator::getCurrentVersion(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(acsIndex) != acsTable.end()) {
		return acsTable[acsIndex].version;
	} else {
		return -1;
	}
}

bool AccumulativeChecksumValidator::exist(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	return acsTable.find(acsIndex) != acsTable.end();
}

void AccumulativeChecksumValidator::restore(uint16_t acsIndex, AccumulativeChecksumState acsState) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsTable.find(acsIndex) == acsTable.end());
	acsState.restoring = true;
	acsTable[acsIndex] = acsState;
}

void AccumulativeChecksumValidator::loadCacheForRestore(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsCacheTableForRestore.find(acsIndex) != acsCacheTableForRestore.end());
	ASSERT(acsTable.find(acsIndex) != acsTable.end());
	acsTable[acsIndex] = acsCacheTableForRestore[acsIndex];
	return;
}

void AccumulativeChecksumValidator::clearCacheForRestore(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsCacheTableForRestore.find(acsIndex) != acsCacheTableForRestore.end());
	acsCacheTableForRestore.erase(acsIndex);
}

void AccumulativeChecksumValidator::markAllAcsIndexOutdated(std::string context, Tag tag, UID ssid) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	outdated = true;
	for (const auto& [acsIndex, acsState] : acsTable) {
		acsTable[acsIndex].outdated = true;
	}
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorMarkAllIndexOutdated", ssid).detail("Context", context).detail("AcsTag", tag);
	}
}

bool AccumulativeChecksumValidator::isOutdated(uint16_t acsIndex) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(acsIndex) == acsTable.end()) {
		return false;
	} else {
		return acsTable[acsIndex].outdated;
	}
}

TEST_CASE("noSim/AccumulativeChecksum/MutationRef") {
	printf("testing MutationRef encoding/decoding\n");
	MutationRef m(MutationRef::SetValue, "TestKey"_sr, "TestValue"_sr);
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM) {
		m.setAccumulativeChecksumIndex(512, true);
	}
	BinaryWriter wr(AssumeVersion(ProtocolVersion::withMutationChecksum()));

	wr << m;

	Standalone<StringRef> value = wr.toValue();
	TraceEvent("EncodedMutation").detail("RawBytes", value);

	BinaryReader rd(value, AssumeVersion(ProtocolVersion::withMutationChecksum()));
	Standalone<MutationRef> de;

	rd >> de;

	printf("Deserialized mutation: %s\n", de.toString().c_str());

	if (de.type != m.type || de.param1 != m.param1 || de.param2 != m.param2) {
		TraceEvent(SevError, "MutationMismatch")
		    .detail("OldType", m.type)
		    .detail("NewType", de.type)
		    .detail("OldParam1", m.param1)
		    .detail("NewParam1", de.param1)
		    .detail("OldParam2", m.param2)
		    .detail("NewParam2", de.param2);
		ASSERT(false);
	}

	ASSERT(de.validateChecksum());

	MutationRef acsMutation;
	acsMutation.type = MutationRef::AccumulativeChecksum;
	acsMutation.param1 = accumulativeChecksumKey;
	acsMutation.param2 = accumulativeChecksumValue(AccumulativeChecksumState(1, 20));
	acsMutation.setAccumulativeChecksumIndex(1, true);
	acsMutation.populateChecksum();
	BinaryWriter acsWr(AssumeVersion(ProtocolVersion::withMutationChecksum()));
	acsWr << acsMutation;
	Standalone<StringRef> acsValue = acsWr.toValue();
	TraceEvent("EncodedMutation").detail("RawBytes", acsValue);
	BinaryReader acsRd(acsValue, AssumeVersion(ProtocolVersion::withMutationChecksum()));
	Standalone<MutationRef> acsDe;
	acsRd >> acsDe;
	printf("Deserialized mutation: %s\n", acsDe.toString().c_str());
	if (acsDe.type != acsMutation.type || acsDe.param1 != acsMutation.param1 || acsDe.param2 != acsMutation.param2) {
		TraceEvent(SevError, "MutationMismatch")
		    .detail("OldType", acsMutation.type)
		    .detail("NewType", acsDe.type)
		    .detail("OldParam1", acsMutation.param1)
		    .detail("NewParam1", acsDe.param1)
		    .detail("OldParam2", acsMutation.param2)
		    .detail("NewParam2", acsDe.param2);
		ASSERT(false);
	}
	ASSERT(acsDe.validateChecksum());

	return Void();
}
