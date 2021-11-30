/*
 * GeneratedCommits.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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

#include "fdbserver/ptxn/test/CommitUtils.h"
#include "fdbserver/ptxn/test/Utils.h"

namespace ptxn::test {

bool CommitRecordTag::allValidated() const {
	return this->tLogValidated && this->storageServerValidated;
}

int CommitRecord::getNumTotalMessages() const {
	int sum = 0;
	for (const auto& [version, teamedMessages] : messages) {
		for (const auto& [storageTeamID, message] : teamedMessages) {
			sum += message.size();
		}
	}
	return sum;
}

std::vector<VersionSubsequenceMessage> CommitRecord::getMessagesFromStorageTeams(
    const std::unordered_set<StorageTeamID>& storageTeamIDs) const {

	std::vector<VersionSubsequenceMessage> vsm;
	for (const auto& [version, _1] : this->messages) {
		for (const auto& [storageTeamID, _2] : _1) {
			if (!storageTeamIDs.empty() && storageTeamIDs.find(storageTeamID) == std::end(storageTeamIDs)) {
				continue;
			}
			for (const auto& [subsequence, message] : _2) {
				vsm.emplace_back(version, subsequence, message);
			}
		}
	}
	std::sort(std::begin(vsm), std::end(vsm));
	return vsm;
}

const std::pair<int, int> DEFAULT_KEY_LENGTH_RANGE = { 10, 20 };
const std::pair<int, int> DEFAULT_VALUE_LENGTH_RANGE = { 100, 200 };

MutationRef generateRandomSetValue(Arena& arena,
                                   const std::pair<int, int>& keyLengthRange,
                                   const std::pair<int, int>& valueLengthRange) {
	StringRef key = StringRef(arena, getRandomAlnum(keyLengthRange.first, keyLengthRange.second));
	StringRef value = StringRef(arena, getRandomAlnum(valueLengthRange.first, valueLengthRange.second));
	return MutationRef(MutationRef::SetValue, key, value);
}

void generateMutationRefs(const int numMutations,
                          Arena& arena,
                          VectorRef<MutationRef>& mutationRefs,
                          const std::pair<int, int>& keyLengthRange,
                          const std::pair<int, int>& valueLengthRange) {
	for (int i = 0; i < numMutations; ++i) {
		mutationRefs.push_back(arena, generateRandomSetValue(arena, keyLengthRange, valueLengthRange));
	}
}

void distributeMutationRefs(VectorRef<MutationRef>& mutationRefs,
                            const Version& commitVersion,
                            const Version& storageTeamVersion,
                            const std::vector<StorageTeamID>& allStorageTeamIDs,
                            CommitRecord& commitRecord) {

	for (auto storageTeamID : allStorageTeamIDs) {
		if (commitRecord.storageTeamEpochVersionRange.count(storageTeamID) == 0) {
			commitRecord.storageTeamEpochVersionRange[storageTeamID] = { commitVersion, commitVersion + 1 };
		} else {
			commitRecord.storageTeamEpochVersionRange.at(storageTeamID).second = commitVersion + 1;
		}
	}

	auto& storageTeamMessageMap = commitRecord.messages[commitVersion];
	auto storageTeamIDs = randomlyPick<std::vector<StorageTeamID>>(
	    allStorageTeamIDs, deterministicRandom()->randomInt(1, allStorageTeamIDs.size() + 1));

	// Distribute the mutations
	Subsequence subsequence = 0;
	for (const auto& mutationRef : mutationRefs) {
		const StorageTeamID storageTeamID = randomlyPick(storageTeamIDs);
		storageTeamMessageMap[storageTeamID].push_back(
		    commitRecord.messageArena, { ++subsequence, MutationRef(commitRecord.messageArena, mutationRef) });
	}

	// Update commit version range
	commitRecord.firstVersion = std::min(commitRecord.firstVersion, commitVersion);
	commitRecord.lastVersion = std::max(commitRecord.lastVersion, commitVersion);

	commitRecord.commitVersionStorageTeamVersionMapper[commitVersion] = storageTeamVersion;
}

void prepareProxySerializedMessages(
    const CommitRecord& commitRecord,
    const Version& commitVersion,
    std::function<std::shared_ptr<ProxySubsequencedMessageSerializer>(const StorageTeamID&)> serializerGen) {

	if (commitRecord.messages.find(commitVersion) == commitRecord.messages.end()) {
		// Version not found, skips the serialization
		return;
	}

	for (const auto& [storageTeamID, _1] : commitRecord.messages.at(commitVersion)) {
		for (const auto& [subsequence, message] : _1) {
			auto pSerializer = serializerGen(storageTeamID);
			// setSubsequence requires subsequence increasing, but for the first message, the subsequence is 1 and the
			// initial subsequence for the serializer is 1 too, this causes an ASSERT failure
			if (pSerializer->getSubsequence() != subsequence) {
				pSerializer->setSubsequence(subsequence);
			}
			switch (message.getType()) {
			case Message::Type::MUTATION_REF:
				pSerializer->write(std::get<MutationRef>(message), storageTeamID);
				break;
			case Message::Type::SPAN_CONTEXT_MESSAGE:
				// This might be nasty, as SPAN_CONTEXT_MESSAGE is broadcasted once, and then all new StorageTeamIDs
				// will have it later. Do not support this at this stage.
				ASSERT(false);
				break;
			case Message::Type::LOG_PROTOCOL_MESSAGE:
				pSerializer->write(std::get<LogProtocolMessage>(message), storageTeamID);
				break;
			default:
				throw internal_error_msg("Unsupported type");
			}
		}
	}
}

bool isAllRecordsValidated(const CommitRecord& commitRecord) {
	for (const auto& [_1, storageTeamTagMap] : commitRecord.tags) {
		for (const auto& [_2, commitRecordTag] : storageTeamTagMap) {
			if (!commitRecordTag.allValidated()) {
				return false;
			}
		}
	}
	return true;
}

} // namespace ptxn::test