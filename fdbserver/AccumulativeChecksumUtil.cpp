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

bool mutationSupportAccumulativeChecksum(MutationRef mutation) {
	if (timeKeeperPrefixRange.contains(mutation.param1)) {
		return false; // bypass time keeper for now, TODO: remove later
	}
	if (mutation.type == MutationRef::ClearRange &&
	    serverTagKeys.withPrefix(systemKeys.begin).intersects(KeyRangeRef(mutation.param1, mutation.param2))) {
		// bypass private mutation of removal of server tag now, TODO: remove later
		return false;
	}
	if (mutation.param1 >= systemKeys.begin) {
		return false; // Only support user data at this time
	}
	return true;
}

void acsBuilderUpdateAccumulativeChecksum(UID commitProxyId,
                                          std::shared_ptr<AccumulativeChecksumBuilder> acsBuilder,
                                          MutationRef mutation,
                                          std::vector<Tag> tags,
                                          Version commitVersion,
                                          LogEpoch epoch) {
	if (acsBuilder == nullptr) {
		// ACS is open when acsBuilder is set
		// Currently, acsBuilder is set only when the mutation is issued by commit proxy
		return;
	}
	if (!mutation.checksum.present() || !mutation.accumulativeChecksumIndex.present()) {
		return;
	}
	if (!mutationSupportAccumulativeChecksum(mutation)) {
		return;
	}
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsBuilder->isValid());
	int appliedCount = 0;
	for (const auto& tag : tags) {
		if (!tagSupportAccumulativeChecksum(tag)) {
			continue;
		}
		acsBuilder->addAliveTag(tag);
		uint32_t oldAcs = 0;
		if (acsBuilder->get(tag).present()) {
			oldAcs = acsBuilder->get(tag).get().acs;
		}
		acsBuilder->update(tag, mutation.checksum.get(), commitVersion, epoch);
		appliedCount = appliedCount + 1;
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsBuilderUpdateAccumulativeChecksum", commitProxyId)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", mutation.accumulativeChecksumIndex.get())
			    .detail("CommitVersion", commitVersion)
			    .detail("OldAcs", oldAcs)
			    .detail("NewAcs", acsBuilder->get(tag).get().acs)
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

uint32_t AccumulativeChecksumBuilder::update(Tag tag, uint32_t checksum, Version version, LogEpoch epoch) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint32_t newAcs = 0;
	if (acsTable.find(tag) == acsTable.end()) {
		newAcs = checksum;
	} else {
		ASSERT(version >= acsTable[tag].version);
		ASSERT(version >= currentVersion);
		newAcs = calculateAccumulativeChecksum(acsTable[tag].acs, checksum);
	}
	acsTable[tag] = AccumulativeChecksumState(newAcs, version, epoch);
	currentVersion = version;
	return newAcs;
}

void AccumulativeChecksumBuilder::resetTag(Tag tag, Version commitVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(tag) != acsTable.end()) {
		acsTable.erase(tag);
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AcsBuilderResetAccumulativeChecksum")
			    .detail("AcsIndex", acsIndex)
			    .detail("AcsTag", tag)
			    .detail("CommitVersion", commitVersion);
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

// Add mutations to cache
void AccumulativeChecksumValidator::cacheMutation(UID ssid, Tag tag, MutationRef mutation, Version ssVersion) {
	if (!mutationSupportAccumulativeChecksum(mutation)) {
		return;
	}
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(mutation.checksum.present());
	ASSERT(mutation.accumulativeChecksumIndex.present());
	uint16_t acsIndex = mutation.accumulativeChecksumIndex.get();
	Version atAcsVersion = 0;
	if (acsTable.find(acsIndex) == acsTable.end()) {
		acsTable[acsIndex] = AccumulativeChecksumEntry(); // init
	}
	acsTable[acsIndex].cachedMutations.push_back(mutation);
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorUpdateAcsCache", ssid)
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("Mutation", mutation.toString())
		    .detail("AtAcsVersion", atAcsVersion)
		    .detail("SSVersion", ssVersion);
	}
}

// Validate and update acs table
bool AccumulativeChecksumValidator::validateAcs(UID ssid,
                                                Tag tag,
                                                uint16_t acsIndex,
                                                AccumulativeChecksumState acsMutationState,
                                                Version ssVersion,
                                                bool& updated) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (acsTable.find(acsIndex) == acsTable.end()) {
		acsTable[acsIndex] = AccumulativeChecksumEntry(acsMutationState); // with cleared cache
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevError, "AccumulativeChecksumSkipValidation", ssid)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("SSVersion", ssVersion)
			    .detail("Epoch", acsMutationState.epoch);
		}
		updated = true;
		return true;
	}
	if (acsTable[acsIndex].acsState.present() &&
	    acsMutationState.version <= acsTable[acsIndex].acsState.get().version) {
		acsTable[acsIndex].cachedMutations.clear();
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AccumulativeChecksumMutationTooOld", ssid)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("SSVersion", ssVersion)
			    .detail("AcsMutation", acsMutationState.toString())
			    .detail("Epoch", acsMutationState.epoch);
		}
		updated = false;
		return true;
	}
	if (acsTable[acsIndex].acsState.present() && acsTable[acsIndex].acsState.get().epoch != acsMutationState.epoch) {
		acsTable[acsIndex].cachedMutations.clear();
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AccumulativeChecksumOutdated", ssid)
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("SSVersion", ssVersion)
			    .detail("AcsMutation", acsMutationState.toString())
			    .detail("Epoch", acsMutationState.epoch);
		}
		updated = false;
		return true;
	}
	// Apply mutations in cache to acs
	ASSERT(!acsTable[acsIndex].acsState.present() || acsTable[acsIndex].cachedMutations.size() >= 1);
	uint32_t oldAcs = acsTable[acsIndex].acsState.present() ? acsTable[acsIndex].acsState.get().acs : 0;
	Version oldVersion = acsTable[acsIndex].acsState.present() ? acsTable[acsIndex].acsState.get().version : 0;
	uint32_t newAcs = 0;
	bool init = false;
	if (!acsTable[acsIndex].acsState.present()) {
		init = true;
		newAcs = acsTable[acsIndex].cachedMutations[0].checksum.get();
		for (int i = 1; i < acsTable[acsIndex].cachedMutations.size(); i++) {
			ASSERT(acsTable[acsIndex].cachedMutations[i].checksum.present());
			newAcs = calculateAccumulativeChecksum(newAcs, acsTable[acsIndex].cachedMutations[i].checksum.get());
		}
	} else {
		init = false;
		newAcs = acsTable[acsIndex].acsState.get().acs;
		for (int i = 0; i < acsTable[acsIndex].cachedMutations.size(); i++) {
			ASSERT(acsTable[acsIndex].cachedMutations[i].checksum.present());
			newAcs = calculateAccumulativeChecksum(newAcs, acsTable[acsIndex].cachedMutations[i].checksum.get());
		}
	}
	Version newVersion = acsMutationState.version;
	if (newAcs != acsMutationState.acs) {
		TraceEvent(SevError, "AccumulativeChecksumValidateError", ssid)
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
		updated = false;
		return false; // Zhe: need to do something?
	} else {
		acsTable[acsIndex] = AccumulativeChecksumEntry(
		    AccumulativeChecksumState(newAcs, acsMutationState.version, acsMutationState.epoch)); // with cleared cache
		if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
			TraceEvent(SevInfo, "AccumulativeChecksumValidated", ssid)
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
		updated = true;
		return true;
	}
}

void AccumulativeChecksumValidator::restore(UID ssid,
                                            Tag tag,
                                            uint16_t acsIndex,
                                            AccumulativeChecksumState acsState,
                                            Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM);
	ASSERT(CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(acsTable.find(acsIndex) == acsTable.end());
	acsTable[acsIndex] = AccumulativeChecksumEntry(acsState); // with cleared cache
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AccumulativeChecksumValidatorRestore", ssid)
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
	acsMutation.param2 = accumulativeChecksumValue(AccumulativeChecksumState(1, 20, 0));
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
