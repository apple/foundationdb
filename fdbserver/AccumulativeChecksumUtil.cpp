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
	return tag.locality >= 0;
}

bool mutationSupportAccumulativeChecksum(const MutationRef& mutation) {
	return mutation.checksum.present() && mutation.accumulativeChecksumIndex.present();
}

void AccumulativeChecksumBuilder::addMutation(const MutationRef& mutation,
                                              const std::vector<Tag>& tags,
                                              LogEpoch epoch,
                                              UID commitProxyId,
                                              Version commitVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(mutationSupportAccumulativeChecksum(mutation));
	int appliedCount = 0;
	for (const auto& tag : tags) {
		if (!tagSupportAccumulativeChecksum(tag)) {
			continue;
		}
		uint32_t oldAcs = 0;
		if (acsTable.find(tag) != acsTable.end()) {
			oldAcs = acsTable[tag].acsState.acs;
		}
		updateTable(tag, mutation.checksum.get(), commitVersion, epoch);
		appliedCount = appliedCount + 1;
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsBuilderAddMutation", commitProxyId)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", mutation.accumulativeChecksumIndex.get())
			    .detail("CommitVersion", commitVersion)
			    .detail("OldAcs", oldAcs)
			    .detail("NewAcs", acsTable[tag].acsState.acs)
			    .detail("Mutation", mutation);
		}
	}
	if (appliedCount == 0 && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevError, "AcsBuilderNotValidTagToUpdate", commitProxyId)
		    .detail("AcsTags", describe(tags))
		    .detail("AcsIndex", mutation.accumulativeChecksumIndex.get())
		    .detail("CommitVersion", commitVersion)
		    .detail("Mutation", mutation);
	}
}

uint32_t AccumulativeChecksumBuilder::updateTable(Tag tag, uint32_t checksum, Version version, LogEpoch epoch) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint32_t newAcs = 0;
	if (acsTable.find(tag) == acsTable.end()) {
		newAcs = checksum;
	} else {
		ASSERT(version >= acsTable[tag].acsState.version);
		ASSERT(version >= currentVersion);
		newAcs = calculateAccumulativeChecksum(acsTable[tag].acsState.acs, checksum);
	}
	acsTable[tag] = Entry(AccumulativeChecksumState(acsIndex, newAcs, version, epoch));
	currentVersion = version;
	return newAcs;
}

void AccumulativeChecksumBuilder::newTag(Tag tag, Version commitVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	bool overwrite = acsTable.find(tag) != acsTable.end();
	acsTable[tag] = Entry();
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsBuilderNewAcsTag")
		    .detail("AcsIndex", acsIndex)
		    .detail("AcsTag", tag)
		    .detail("CommitVersion", commitVersion)
		    .detail("Overwrite", overwrite);
	}
}

// Add mutations to cache
void AccumulativeChecksumValidator::addMutation(const MutationRef& mutation, UID ssid, Tag tag, Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(mutation.checksum.present());
	ASSERT(mutation.accumulativeChecksumIndex.present());
	const uint16_t& acsIndex = mutation.accumulativeChecksumIndex.get();
	Version atAcsVersion = 0;
	if (acsTable.find(acsIndex) == acsTable.end()) {
		acsTable[acsIndex] = Entry(); // init
	}
	acsTable[acsIndex].cachedMutations.push_back(mutation);
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorAddMutation", ssid)
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("Mutation", mutation.toString())
		    .detail("LastAcsVersion", atAcsVersion)
		    .detail("SSVersion", ssVersion);
	}
}

// Validate and update acs table
// Return acs state to persist
Optional<AccumulativeChecksumState> AccumulativeChecksumValidator::processAccumulativeChecksum(
    const AccumulativeChecksumState& acsMutationState,
    UID ssid,
    Tag tag,
    Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	const LogEpoch& epoch = acsMutationState.epoch;
	const uint16_t& acsIndex = acsMutationState.acsIndex;
	if (acsTable.find(acsIndex) == acsTable.end()) {
		// Unexpected, since we assign acs mutation in commit batch
		// So, there must be acs entry set up when adding the mutations of the batch
		acsTable[acsIndex] = Entry(); // with cleared cache
		acsTable[acsIndex].newAcsState(acsMutationState);
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevError, "AcsValidatorAcsMutationSkip", ssid)
			    .detail("Reason", "No Entry")
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("SSVersion", ssVersion)
			    .detail("Epoch", epoch);
		}
		return acsMutationState;
	}
	const bool epochExists = acsTable[acsIndex].acsStates.find(epoch) != acsTable[acsIndex].acsStates.end();
	if (epochExists && acsMutationState.version <= acsTable[acsIndex].acsStates[epoch].version) {
		acsTable[acsIndex].cachedMutations.clear();
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsValidatorAcsMutationSkip", ssid)
			    .detail("Reason", "Acs Mutation Too Old")
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("SSVersion", ssVersion)
			    .detail("AcsMutation", acsMutationState.toString())
			    .detail("Epoch", epoch);
		}
		return Optional<AccumulativeChecksumState>();
	}
	// Apply mutations in cache to acs
	ASSERT(acsTable[acsIndex].cachedMutations.size() >= 1);
	uint32_t oldAcs = epochExists ? acsTable[acsIndex].acsStates[epoch].acs : 0;
	Version oldVersion = epochExists ? acsTable[acsIndex].acsStates[epoch].version : 0;
	uint32_t newAcs = 0;
	bool init = false;
	if (!epochExists) {
		init = true;
		ASSERT(acsTable[acsIndex].cachedMutations[0].checksum.present());
		newAcs = acsTable[acsIndex].cachedMutations[0].checksum.get();
		for (int i = 1; i < acsTable[acsIndex].cachedMutations.size(); i++) {
			ASSERT(acsTable[acsIndex].cachedMutations[i].checksum.present());
			newAcs = calculateAccumulativeChecksum(newAcs, acsTable[acsIndex].cachedMutations[i].checksum.get());
		}
	} else {
		init = false;
		newAcs = acsTable[acsIndex].acsStates[epoch].acs;
		for (int i = 0; i < acsTable[acsIndex].cachedMutations.size(); i++) {
			ASSERT(acsTable[acsIndex].cachedMutations[i].checksum.present());
			newAcs = calculateAccumulativeChecksum(newAcs, acsTable[acsIndex].cachedMutations[i].checksum.get());
		}
	}
	Version newVersion = acsMutationState.version;
	if (newAcs != acsMutationState.acs) {
		TraceEvent(SevError, "AcsValidatorAcsMutationMismatch", ssid)
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("SSVersion", ssVersion)
		    .detail("FromAcs", oldAcs)
		    .detail("FromVersion", oldVersion)
		    .detail("ToAcs", newAcs)
		    .detail("ToVersion", newVersion)
		    .detail("AcsToValidate", acsMutationState.acs)
		    .detail("Epoch", acsMutationState.epoch)
		    .detail("Init", init);
		// Currently, force to reconcile
		// Zhe: need to do something?
		ASSERT(false);
		return acsMutationState;
	} else {
		AccumulativeChecksumState newState(acsIndex, newAcs, acsMutationState.version, acsMutationState.epoch);
		acsTable[acsIndex].newAcsState(newState); // with cleared cache
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsValidatorAcsMutationValidated", ssid)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("SSVersion", ssVersion)
			    .detail("FromAcs", oldAcs)
			    .detail("FromVersion", oldVersion)
			    .detail("ToAcs", newAcs)
			    .detail("ToVersion", newVersion)
			    .detail("Epoch", acsMutationState.epoch)
			    .detail("Init", init);
		}
		return newState;
	}
}

void AccumulativeChecksumValidator::restore(const AccumulativeChecksumState& acsState,
                                            UID ssid,
                                            Tag tag,
                                            Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	const uint16_t& acsIndex = acsState.acsIndex;
	if (acsTable.find(acsIndex) == acsTable.end()) {
		acsTable[acsIndex] = Entry(); // with cleared cache
	}
	acsTable[acsIndex].newAcsState(acsState);
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorRestore", ssid)
		    .detail("AcsIndex", acsIndex)
		    .detail("AcsTag", tag)
		    .detail("AcsState", acsState.toString())
		    .detail("SSVersion", ssVersion)
		    .detail("Epoch", acsState.epoch);
	}
}

TEST_CASE("noSim/AccumulativeChecksum/MutationRef") {
	printf("testing MutationRef encoding/decoding\n");
	MutationRef m(MutationRef::SetValue, "TestKey"_sr, "TestValue"_sr);
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM) {
		m.setAccumulativeChecksumIndex(512);
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
	acsMutation.param2 = accumulativeChecksumValue(AccumulativeChecksumState(1, 1, 20, 0));
	acsMutation.setAccumulativeChecksumIndex(1);
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
