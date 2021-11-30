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

#ifndef FDBSERVER_PTXN_TEST_GENERATEDCOMMITS_H
#define FDBSERVER_PTXN_TEST_GENERATEDCOMMITS_H

#include <map>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "fdbclient/FDBTypes.h"
#include "fdbserver/ptxn/MessageSerializer.h"
#include "flow/Arena.h"

namespace ptxn::test {

struct CommitRecordTag {
	bool tLogValidated = false;
	bool storageServerValidated = false;

	bool allValidated() const;
};

// Record generated commits in the current epoch
struct CommitRecord {
	// Format:
	// Commit Version ---+ Storage Team ID -- <Subsequence, Message>
	//                   | Storage Team ID -- <Subsequence, Message>
	//                   | Storage Team ID -- <Subsequence, Message>
	//                     ...
	// Commit Version ---+ Storage Team ID -- <Subsequence, Message>
	//                   | Storage Team ID -- <Subsequence, Message>
	//                     ...
	using RecordType = std::map<Version, std::unordered_map<StorageTeamID, VectorRef<std::pair<Subsequence, Message>>>>;

	// The arena contains the messages
	Arena messageArena;

	RecordType messages;

	// Maps commit version to corresponding storage team version
	std::map<Version, Version> commitVersionStorageTeamVersionMapper;

	// Maps StorageTeamID to corresponding epoch
	std::unordered_map<StorageTeamID, std::pair<Version, Version>> storageTeamEpochVersionRange;

	// Add tag for each Commit version -- Storage Team ID pair. The tag is used for verifying the data.
	std::map<Version, std::unordered_map<StorageTeamID, CommitRecordTag>> tags;

	// Get messages from a given set of storage teams, in format
	//     CommitVersion -- Subsequence -- Messag
	// If the set is empty, return *all* messages in CommitRecord
	std::vector<VersionSubsequenceMessage> getMessagesFromStorageTeams(
	    const std::unordered_set<StorageTeamID>& storageTeamIDs = std::unordered_set<StorageTeamID>()) const;
	int getNumTotalMessages() const;

	// The first commit version
	Version firstVersion = MAX_VERSION;

	// The last commit version
	Version lastVersion = invalidVersion;
};

extern const std::pair<int, int> DEFAULT_KEY_LENGTH_RANGE;
extern const std::pair<int, int> DEFAULT_VALUE_LENGTH_RANGE;

// Generate a random SetValue MutationRef
MutationRef generateRandomSetValue(Arena& arena,
                                   const std::pair<int, int>& keyLengthRange = DEFAULT_KEY_LENGTH_RANGE,
                                   const std::pair<int, int>& valueLengthRange = DEFAULT_VALUE_LENGTH_RANGE);

// Generates a series of random mutations
void generateMutationRefs(const int numMutations,
                          Arena& arena,
                          VectorRef<MutationRef>& mutationRefs,
                          const std::pair<int, int>& keyLengthRange = DEFAULT_KEY_LENGTH_RANGE,
                          const std::pair<int, int>& valueLengthRange = DEFAULT_VALUE_LENGTH_RANGE);

// Distribute MutationRefs to StorageTeamIDs in CommitRecord.
void distributeMutationRefs(VectorRef<MutationRef>& mutationRefs,
                            const Version& commitVersion,
                            const Version& storageTeamVersion,
                            const std::vector<StorageTeamID>& storageTeamIDs,
                            CommitRecord& commitRecord);

// For a given version, serialize the messages from CommitRecord for Proxy use
// The lambda function will receive a StorageTeamID, and return the serializer for its corresponding TLogGroupID
// FIXME: use TestEnvironment for the mapping
void prepareProxySerializedMessages(
    const CommitRecord& commitRecord,
    const Version& commitVersion,
    std::function<std::shared_ptr<ProxySubsequencedMessageSerializer>(const StorageTeamID&)> serializerGen);

// Check if all records are validated
bool isAllRecordsValidated(const CommitRecord& commitRecord);

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_GENERATEDCOMMITS_H
