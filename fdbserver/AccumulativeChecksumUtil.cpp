/*
 * AccumulativeChecksumUtil.cpp
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

#include "fdbserver/AccumulativeChecksumUtil.h"
#include "fdbserver/Knobs.h"

void updateMutationWithAcsAndAddMutationToAcsBuilder(std::shared_ptr<AccumulativeChecksumBuilder> acsBuilder,
                                                     MutationRef& mutation,
                                                     Tag inputTag,
                                                     uint16_t acsIndex,
                                                     LogEpoch epoch,
                                                     Version commitVersion,
                                                     UID commitProxyId) {
	mutation.populateChecksum();
	mutation.setAccumulativeChecksumIndex(acsIndex);
	acsBuilder->addMutation(mutation, inputTag, epoch, commitProxyId, commitVersion);
	return;
}

void updateMutationWithAcsAndAddMutationToAcsBuilder(std::shared_ptr<AccumulativeChecksumBuilder> acsBuilder,
                                                     MutationRef& mutation,
                                                     const std::vector<Tag>& inputTags,
                                                     uint16_t acsIndex,
                                                     LogEpoch epoch,
                                                     Version commitVersion,
                                                     UID commitProxyId) {
	mutation.populateChecksum();
	mutation.setAccumulativeChecksumIndex(acsIndex);
	for (const auto& inputTag : inputTags) {
		acsBuilder->addMutation(mutation, inputTag, epoch, commitProxyId, commitVersion);
	}
	return;
}

void updateMutationWithAcsAndAddMutationToAcsBuilder(std::shared_ptr<AccumulativeChecksumBuilder> acsBuilder,
                                                     MutationRef& mutation,
                                                     const std::set<Tag>& inputTags,
                                                     uint16_t acsIndex,
                                                     LogEpoch epoch,
                                                     Version commitVersion,
                                                     UID commitProxyId) {
	mutation.populateChecksum();
	mutation.setAccumulativeChecksumIndex(acsIndex);
	for (const auto& inputTag : inputTags) {
		acsBuilder->addMutation(mutation, inputTag, epoch, commitProxyId, commitVersion);
	}
	return;
}

void AccumulativeChecksumBuilder::addMutation(const MutationRef& mutation,
                                              Tag tag,
                                              LogEpoch epoch,
                                              UID commitProxyId,
                                              Version commitVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (!tagSupportAccumulativeChecksum(tag)) {
		return;
	}
	uint32_t oldAcs = 0;
	auto it = acsTable.find(tag);
	if (it != acsTable.end()) {
		oldAcs = it->second.acs;
	}
	uint32_t newAcs = updateTable(tag, mutation.checksum.get(), commitVersion, epoch);
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsBuilderAddMutation", commitProxyId)
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", mutation.accumulativeChecksumIndex.get())
		    .detail("CommitVersion", commitVersion)
		    .detail("OldAcs", oldAcs)
		    .detail("NewAcs", newAcs)
		    .detail("Mutation", mutation);
	}
	return;
}

uint32_t AccumulativeChecksumBuilder::updateTable(Tag tag, uint32_t checksum, Version version, LogEpoch epoch) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint32_t newAcs = 0;
	auto it = acsTable.find(tag);
	if (it == acsTable.end()) {
		newAcs = checksum;
		acsTable[tag] = AccumulativeChecksumState(acsIndex, newAcs, version, epoch);
	} else {
		ASSERT(version >= it->second.version);
		ASSERT(version >= currentVersion);
		newAcs = calculateAccumulativeChecksum(it->second.acs, checksum);
		it->second = AccumulativeChecksumState(acsIndex, newAcs, version, epoch);
	}
	currentVersion = version;
	return newAcs;
}

void AccumulativeChecksumBuilder::newTag(Tag tag, UID ssid, Version commitVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	bool exist = acsTable.erase(tag) > 0;
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsBuilderNewAcsTag")
		    .detail("AcsIndex", acsIndex)
		    .detail("AcsTag", tag)
		    .detail("CommitVersion", commitVersion)
		    .detail("Exist", exist)
		    .detail("SSID", ssid);
	}
}

void AccumulativeChecksumValidator::addMutation(const MutationRef& mutation,
                                                UID ssid,
                                                Tag tag,
                                                Version ssVersion,
                                                Version mutationVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	ASSERT(mutation.checksum.present() && mutation.accumulativeChecksumIndex.present());
	const uint16_t& acsIndex = mutation.accumulativeChecksumIndex.get();
	if (!mutationBuffer.empty()) {
		ASSERT(mutationBuffer[0].second.accumulativeChecksumIndex.present());
		if (mutationBuffer[0].first != mutationVersion) {
			TraceEvent(SevError, "AcsValidatorCorruptionDetected", ssid)
			    .detail("Reason", "Mutation version changed when AddMutation")
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("MissingVersion", mutationBuffer[0].first)
			    .detail("Mutation", mutation.toString())
			    .detail("SSVersion", ssVersion)
			    .detail("MutationVersion", mutationVersion);
			throw please_reboot();
		} else if (mutationBuffer[0].second.accumulativeChecksumIndex.get() != acsIndex) {
			TraceEvent(SevError, "AcsValidatorCorruptionDetected", ssid)
			    .detail("Reason", "Mutation ACSIndex changed when AddMutation")
			    .detail("AcsTag", tag)
			    .detail("AcsIndex", acsIndex)
			    .detail("MissingAcsIndex", mutationBuffer[0].second.accumulativeChecksumIndex.get())
			    .detail("Mutation", mutation.toString())
			    .detail("SSVersion", ssVersion)
			    .detail("MutationVersion", mutationVersion);
			throw please_reboot();
		}
	}
	mutationBuffer.push_back(mutationBuffer.arena(), std::make_pair(mutationVersion, mutation));
	totalAddedMutations++;
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorAddMutation", ssid)
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("Mutation", mutation.toString())
		    .detail("SSVersion", ssVersion)
		    .detail("MutationVersion", mutationVersion);
	}
}

Optional<AccumulativeChecksumState> AccumulativeChecksumValidator::processAccumulativeChecksum(
    const AccumulativeChecksumState& acsMutationState,
    UID ssid,
    Tag tag,
    Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	const uint16_t& acsIndex = acsMutationState.acsIndex;
	auto it = acsTable.find(acsIndex);
	bool existInTable = true;
	if (it == acsTable.end()) {
		existInTable = false;
	} else if (acsMutationState.epoch > it->second.epoch) {
		acsTable.erase(it); // Clear the old acs state if new epoch comes
		existInTable = false;
	}
	// Calculate acs value by mutation buffer and compare it with acs value in acs mutation
	if (mutationBuffer.size() == 0) {
		TraceEvent(SevError, "AcsValidatorCorruptionDetected", ssid)
		    .detail("Reason", "Mutation buffer is empty when processAccumulativeChecksum")
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("SSVersion", ssVersion);
		throw please_reboot();
	}
	uint32_t oldAcs = !existInTable ? initialAccumulativeChecksum : it->second.acs;
	Version oldVersion = !existInTable ? 0 : it->second.version; // used for logging only, simply set it 0 if newEpoch
	uint32_t newAcs = aggregateAcs(oldAcs, mutationBuffer);
	checkedMutations = checkedMutations + mutationBuffer.size();
	checkedVersions = checkedVersions + 1;
	Version newVersion = acsMutationState.version;
	if (newAcs != acsMutationState.acs) {
		TraceEvent(SevError, "AcsValidatorCorruptionDetected", ssid)
		    .detail("Reason", "ACS value mismatch when processAccumulativeChecksum")
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("SSVersion", ssVersion)
		    .detail("FromAcs", oldAcs)
		    .detail("FromVersion", oldVersion)
		    .detail("ToAcs", newAcs)
		    .detail("ToVersion", newVersion)
		    .detail("AcsToValidate", acsMutationState.acs)
		    .detail("Epoch", acsMutationState.epoch)
		    .detail("ExistInTable", existInTable);
		throw please_reboot();
	} else if (newVersion != mutationBuffer.back().first) {
		TraceEvent(SevError, "AcsValidatorCorruptionDetected", ssid)
		    .detail(
		        "Reason",
		        "ACS mutation version is different from mutation version in buffer when processAccumulativeChecksum")
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("LastMutationVersion", mutationBuffer.back().first)
		    .detail("LastMutation", mutationBuffer.back().second.toString())
		    .detail("SSVersion", ssVersion)
		    .detail("AcsState", acsMutationState.toString());
		throw please_reboot();
	} else {
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
			    .detail("ExistInTable", existInTable);
		}
	}
	if (existInTable) {
		it->second = acsMutationState;
	} else {
		acsTable[acsIndex] = acsMutationState;
	}
	mutationBuffer.clear();
	return acsMutationState;
}

void AccumulativeChecksumValidator::restore(const AccumulativeChecksumState& acsState,
                                            UID ssid,
                                            Tag tag,
                                            Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	const uint16_t& acsIndex = acsState.acsIndex;
	if (acsState.version > ssVersion) {
		TraceEvent(SevError, "AcsValidatorCorruptionDetected", ssid)
		    .detail("Reason", "Restored ACS version is larger than storage server version")
		    .detail("AcsTag", tag)
		    .detail("AcsIndex", acsIndex)
		    .detail("SSVersion", ssVersion)
		    .detail("AcsState", acsState.toString());
		throw please_reboot();
	}
	auto res = acsTable.insert({ acsIndex, acsState });
	ASSERT(res.second); // Each acsIndex has persisted one ACS value
	if (CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM_LOGGING) {
		TraceEvent(SevInfo, "AcsValidatorRestore", ssid)
		    .detail("AcsIndex", acsIndex)
		    .detail("AcsTag", tag)
		    .detail("AcsState", acsState.toString())
		    .detail("SSVersion", ssVersion)
		    .detail("Epoch", acsState.epoch);
	}
}

void AccumulativeChecksumValidator::clearCache(UID ssid, Tag tag, Version ssVersion) {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	if (!mutationBuffer.empty()) {
		TraceEvent(SevError, "AcsValidatorCachedMutationNotChecked", ssid)
		    .detail("AcsTag", tag)
		    .detail("SSVersion", ssVersion);
	}
}

uint64_t AccumulativeChecksumValidator::getAndClearCheckedMutations() {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint64_t res = checkedMutations;
	checkedMutations = 0;
	return res;
}

uint64_t AccumulativeChecksumValidator::getAndClearCheckedVersions() {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint64_t res = checkedVersions;
	checkedVersions = 0;
	return res;
}

uint64_t AccumulativeChecksumValidator::getAndClearTotalMutations() {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint64_t res = totalMutations;
	totalMutations = 0;
	return res;
}

uint64_t AccumulativeChecksumValidator::getAndClearTotalAcsMutations() {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint64_t res = totalAcsMutations;
	totalAcsMutations = 0;
	return res;
}

uint64_t AccumulativeChecksumValidator::getAndClearTotalAddedMutations() {
	ASSERT(CLIENT_KNOBS->ENABLE_MUTATION_CHECKSUM && CLIENT_KNOBS->ENABLE_ACCUMULATIVE_CHECKSUM);
	uint64_t res = totalAddedMutations;
	totalAddedMutations = 0;
	return res;
}

TEST_CASE("noSim/AccumulativeChecksum/MutationRef") {
	printf("testing MutationRef encoding/decoding\n");
	MutationRef m(MutationRef::SetValue, "TestKey"_sr, "TestValue"_sr);
	m.setAccumulativeChecksumIndex(512);
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

	Standalone<MutationRef> acsMutation;
	LogEpoch epoch = 0;
	uint16_t acsIndex = 1;
	Standalone<StringRef> param2 = accumulativeChecksumValue(AccumulativeChecksumState(acsIndex, 1, 20, epoch));
	acsMutation.type = MutationRef::SetValue;
	acsMutation.param1 = accumulativeChecksumKey;
	acsMutation.param2 = param2;
	acsMutation.setAccumulativeChecksumIndex(1);
	acsMutation.populateChecksum();
	BinaryWriter acsWr(IncludeVersion());
	acsWr << acsMutation;
	Standalone<StringRef> acsValue = acsWr.toValue();
	BinaryReader acsRd(acsValue, IncludeVersion());
	Standalone<MutationRef> acsDe;
	acsRd >> acsDe;
	if (acsDe.type != MutationRef::SetValue || acsDe.param1 != accumulativeChecksumKey || acsDe.param2 != param2) {
		TraceEvent(SevError, "AcsMutationMismatch");
		ASSERT(false);
	}
	ASSERT(acsDe.validateChecksum());

	return Void();
}
