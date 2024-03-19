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

// Update acs to cache
void AccumulativeChecksumValidator::updateAcs(UID ssid, Tag tag, MutationRef mutation, Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(mutation.checksum.present());
	ASSERT(mutation.accumulativeChecksumIndex.present());
	uint32_t checksum = mutation.checksum.get();
	uint16_t acsIndex = mutation.accumulativeChecksumIndex.get();
	uint32_t newAcs = 0;
	uint32_t oldAcs = 0;
	Version atAcsVersion = 0;
	if (acsTable.find(acsIndex) == acsTable.end()) {
		oldAcs = 0;
		newAcs = 0;
		atAcsVersion = 0;
		acsTable[acsIndex] = AccumulativeChecksumState(); // init
	} else {
		atAcsVersion = acsTable[acsIndex].version;
		if (!acsTable[acsIndex].cachedAcs.present()) {
			oldAcs = acsTable[acsIndex].acs;
		} else {
			oldAcs = acsTable[acsIndex].cachedAcs.get();
		}
		newAcs = calculateAccumulativeChecksum(oldAcs, checksum);
	}
	acsTable[acsIndex].cachedAcs = newAcs;
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorUpdateAcsCache", ssid)
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("Checksum", checksum)
		    .detail("Mutation", mutation.toString())
		    .detail("NewAcs", newAcs)
		    .detail("OldAcs", oldAcs)
		    .detail("AtAcsVersion", atAcsVersion)
		    .detail("SSVersion", ssVersion);
	}
}

// Validate and update acs table
bool AccumulativeChecksumValidator::validateAcs(UID ssid,
                                                Tag tag,
                                                uint16_t acsIndex,
                                                AccumulativeChecksumState acsMutationState) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(acsIndex) == acsTable.end()) {
		return true;
	}
	ASSERT(acsTable[acsIndex].cachedAcs.present());
	uint32_t cachedAcs = acsTable[acsIndex].cachedAcs.get();

	if (acsMutationState.version > acsTable[acsIndex].version) {
		if (cachedAcs != acsMutationState.acs) {
			TraceEvent e(SevError, "AccumulativeChecksumValidateError", ssid);
			e.detail("AcsTag", tag);
			e.detail("AcsIndex", acsIndex);
			e.detail("AcsValueToCheck", acsMutationState.acs);
			if (acsTable.find(acsIndex) == acsTable.end()) {
				e.detail("Reason", "AcsIndexNotPresented");
			} else {
				e.detail("Reason", "AcsValueMismatch");
				e.detail("CachedAcs", cachedAcs);
			}
			return false; // Zhe: need to do something?
		} else {
			acsTable[acsIndex] = AccumulativeChecksumState(cachedAcs, acsMutationState.version); // with cleared cache
			if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
				TraceEvent(SevInfo, "AccumulativeChecksumValidated", ssid)
				    .detail("AcsTag", tag)
				    .detail("AcsIndex", acsIndex)
				    .detail("AcsInTable", acsTable[acsIndex].toString());
			}
		}
	}
	return true;
}

void AccumulativeChecksumValidator::restore(UID ssid, Tag tag, uint16_t acsIndex, AccumulativeChecksumState acsState) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsTable.find(acsIndex) == acsTable.end());
	acsTable[acsIndex] = acsState; // with cleared cache
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AccumulativeChecksumValidatorRestore", ssid)
		    .detail("AcsIndex", acsIndex)
		    .detail("AcsTag", tag)
		    .detail("AcsState", acsState.toString());
	}
}

void AccumulativeChecksumValidator::markAllAcsIndexOutdated(UID ssid, Tag tag) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	outdated = true;
	for (const auto& [acsIndex, acsState] : acsTable) {
		acsTable[acsIndex].outdated = true;
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "MarkAcsValidatorAsOutdated", ssid)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("AcsState", acsState.toString());
		}
	}
}

bool AccumulativeChecksumValidator::isOutdated(UID ssid, Tag tag, uint16_t acsIndex, MutationRef mutation) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(acsIndex) == acsTable.end()) {
		return false;
	} else if (acsTable[acsIndex].outdated) {
		return false;
	} else {
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsValidatorIsOutdated", ssid)
			    .detail("Context", "Check with Acs mutation")
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("Mutation", mutation);
		}
		return true;
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
