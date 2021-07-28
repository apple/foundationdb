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

void generateMutationRefs(const int numMutations,
                          Arena& arena,
                          VectorRef<MutationRef>& mutationRefs,
                          const std::pair<int, int>& keyLengthRange,
                          const std::pair<int, int>& valueLengthRange) {
	for (int i = 0; i < numMutations; ++i) {
		StringRef key = StringRef(arena, getRandomAlnum(keyLengthRange.first, keyLengthRange.second));
		StringRef value = StringRef(arena, getRandomAlnum(valueLengthRange.first, valueLengthRange.second));
		mutationRefs.emplace_back(arena, MutationRef(MutationRef::SetValue, key, value));
	}
}

void distributeMutationRefs(VectorRef<MutationRef>& mutationRefs,
                            const Version& version,
                            const std::vector<StorageTeamID>& storageTeamIDs,
                            CommitRecord& commitRecord) {
	auto& storageTeamMessageMap = commitRecord.messages[version];

	// Find the maximum subsequence used in the commitRecord for the given version
	Subsequence subsequence = invalidSubsequence;
	for (const auto& [_, messages] : storageTeamMessageMap) {
		subsequence = std::max(messages.back().first, subsequence);
	}

	// Distribute the mutations
	for (const auto& mutationRef : mutationRefs) {
		const StorageTeamID storageTeamID = randomlyPick(storageTeamIDs);
		storageTeamMessageMap[storageTeamID].push_back(commitRecord.messageArena, { ++subsequence, mutationRef });
	}
}

void prepareProxySerializedMessages(
    const CommitRecord& commitRecord,
    const Version& version,
    std::function<std::shared_ptr<ProxySubsequencedMessageSerializer>(StorageTeamID)> serializer) {
	if (commitRecord.messages.find(version) == commitRecord.messages.end()) {
		// Version not found, skips the serialization
		return;
	}
	for (const auto& [storageTeamID, messages] : commitRecord.messages.at(version)) {
		for (const auto& [subsequence, message] : messages) {
			switch (message.getType()) {
			case Message::Type::MUTATION_REF:
				serializer(storageTeamID)->write(std::get<MutationRef>(message), storageTeamID);
				break;
			case Message::Type::SPAN_CONTEXT_MESSAGE:
				serializer(storageTeamID)->broadcastSpanContext(std::get<SpanContextMessage>(message));
				break;
			default:
				throw internal_error_msg("Unsupported type");
			}
		}
	}
}

void distributeMutationRefs(VectorRef<MutationRef>& mutationRefs,
                            const Version& version,
                            const StorageTeamID& storageTeamID,
                            CommitRecord& commitRecord) {
	distributeMutationRefs(mutationRefs, version, { storageTeamID }, commitRecord);
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