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

#include <unordered_map>
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

// Record generated commits
struct CommitRecord {
	Arena messageArena;
	std::unordered_map<Version, std::unordered_map<StorageTeamID, VectorRef<std::pair<Subsequence, Message>>>> messages;
	std::unordered_map<Version, std::unordered_map<StorageTeamID, CommitRecordTag>> tags;

	int getNumTotalMessages() const;
};

// Generates a series of random mutations
void generateMutationRefs(const int numMutations,
                          Arena& arena,
                          VectorRef<MutationRef>& mutationRefs,
                          const std::pair<int, int>& keyLengthRange = { 10, 15 },
                          const std::pair<int, int>& valueLengthRange = { 10, 1000 });

// Distribute MutationRefs to StorageTeamIDs in CommitRecord
void distributeMutationRefs(VectorRef<MutationRef>& mutationRefs,
                            const Version& version,
                            const std::vector<StorageTeamID>& storageTeamIDs,
                            CommitRecord& commitRecord);

// For a given version, serialize the messages from CommitRecord for Proxy use
void prepareProxySerializedMessages(
    const CommitRecord& commitRecord,
    const Version& version,
    std::function<std::shared_ptr<ProxySubsequencedMessageSerializer>(StorageTeamID)> serializer);

// Check if all records are validated
bool isAllRecordsValidated(const CommitRecord& commitRecord);

} // namespace ptxn::test

#endif // FDBSERVER_PTXN_TEST_GENERATEDCOMMITS_H
