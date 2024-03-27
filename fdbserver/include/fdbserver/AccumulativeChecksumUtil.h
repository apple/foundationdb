/*
 * AccumulativeChecksumUtil.h
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

#ifndef ACCUMULATIVECHECKSUMUTIL_H
#define ACCUMULATIVECHECKSUMUTIL_H
#pragma once

#include "fdbclient/AccumulativeChecksum.h"
#include "fdbclient/CommitTransaction.h"
#include "fdbclient/SystemData.h"

static const uint16_t invalidAccumulativeChecksumIndex = 0;
static const uint16_t resolverAccumulativeChecksumIndex = 2;

uint16_t getCommitProxyAccumulativeChecksumIndex(uint16_t commitProxyIndex);

uint32_t calculateAccumulativeChecksum(uint32_t currentAccumulativeChecksum, uint32_t newChecksum);

bool tagSupportAccumulativeChecksum(Tag tag);

// A builder to generate accumulative checksum and keep tracking
// the accumulative checksum for each tag
// Currently, accumulative checksum only supports the mutation
// generated by commit proxy and the encryption is disabled
class AccumulativeChecksumBuilder {
	struct Entry {
		Entry() : acsState(AccumulativeChecksumState()) {}
		Entry(AccumulativeChecksumState acsState) : acsState(acsState) {}

		AccumulativeChecksumState acsState;
	};

public:
	AccumulativeChecksumBuilder(uint16_t acsIndex) : acsIndex(acsIndex), currentVersion(0) {}

	bool isValid() { return acsIndex != invalidAccumulativeChecksumIndex; }

	void newTag(Tag tag, UID ssid, Version commitVersion);

	void addMutation(const MutationRef& mutation,
	                 const std::vector<Tag>& tags,
	                 LogEpoch epoch,
	                 UID commitProxyId,
	                 Version commitVersion);

	const std::unordered_map<Tag, Entry>& getAcsTable() { return acsTable; }

private:
	uint16_t acsIndex;
	Version currentVersion;
	std::unordered_map<Tag, Entry> acsTable;

	uint32_t updateTable(Tag tag, uint32_t checksum, Version version, LogEpoch epoch);
};

// A validator to check if the accumulative checksum is correct for
// each version that has mutations
class AccumulativeChecksumValidator {
	struct Entry {
		Entry() {}

		Entry(const MutationRef& mutation) { cachedMutations.push_back(cachedMutations.arena(), mutation); }

		Entry(const AccumulativeChecksumState& acsState) : acsState(acsState) {}

		Optional<AccumulativeChecksumState> acsState;
		Optional<Version> liveLatestVersion;
		Standalone<VectorRef<MutationRef>> cachedMutations; // Do we really want to do deep copy here?
	};

public:
	AccumulativeChecksumValidator() {}

	void addMutation(const MutationRef& mutation, UID ssid, Tag tag, Version ssVersion);

	Optional<AccumulativeChecksumState> processAccumulativeChecksum(const AccumulativeChecksumState& acsMutationState,
	                                                                UID ssid,
	                                                                Tag tag,
	                                                                Version ssVersion);

	void restore(const AccumulativeChecksumState& acsState, UID ssid, Tag tag, Version ssVersion);

	uint64_t getAndClearCheckedMutations() {
		uint64_t res = checkedMutations;
		checkedMutations = 0;
		return res;
	}

	uint64_t getAndClearCheckedVersions() {
		uint64_t res = checkedVersions;
		checkedVersions = 0;
		return res;
	}

	uint64_t getAndClearTotalMutations() {
		uint64_t res = totalMutations;
		totalMutations = 0;
		return res;
	}

	void incrementTotalMutations() { totalMutations++; }

private:
	std::unordered_map<uint16_t, Entry> acsTable;
	uint64_t checkedMutations = 0;
	uint64_t checkedVersions = 0;
	uint64_t totalMutations = 0;
};

#endif
