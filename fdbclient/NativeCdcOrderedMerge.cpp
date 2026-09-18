/*
 * NativeCdcOrderedMerge.cpp
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

#include "NativeCdcOrderedMerge.h"

#include <algorithm>
#include <initializer_list>
#include <limits>
#include <utility>

#include "flow/Error.h"
#include "flow/UnitTest.h"

namespace {

int64_t addEstimatedBytes(int64_t total, int64_t bytes) {
	return total > std::numeric_limits<int64_t>::max() - bytes ? std::numeric_limits<int64_t>::max() : total + bytes;
}

int64_t estimatedMutationBytes(VectorRef<MutationRef> const& mutations) {
	int64_t bytes = 0;
	for (const auto& mutation : mutations) {
		bytes = addEstimatedBytes(bytes, sizeof(MutationRef));
		bytes = addEstimatedBytes(bytes, mutation.param1.size());
		bytes = addEstimatedBytes(bytes, mutation.param2.size());
	}
	return bytes;
}

void validateCommonPosition(Version position) {
	if (position < invalidVersion || position == std::numeric_limits<Version>::max()) {
		throw client_invalid_operation();
	}
}

} // namespace

NativeCdcOrderedMerge::NativeCdcOrderedMerge(size_t partitionCount, Version commonPosition, int64_t bufferByteLimit)
  : currentPosition(commonPosition), bufferByteLimit(bufferByteLimit) {
	validateCommonPosition(commonPosition);
	if (partitionCount == 0 || partitionCount > std::numeric_limits<int>::max() || bufferByteLimit <= 0) {
		throw client_invalid_operation();
	}
	partitions.resize(partitionCount);
	reset(commonPosition);
}

bool NativeCdcOrderedMerge::needsRead(size_t index) const {
	if (index >= partitions.size()) {
		throw client_invalid_operation();
	}
	return partitions[index].through == currentPosition;
}

Version NativeCdcOrderedMerge::frontier(size_t index) const {
	if (index >= partitions.size()) {
		throw client_invalid_operation();
	}
	return partitions[index].through;
}

int64_t NativeCdcOrderedMerge::estimatedReplyBytes(CDCConsumeReply const& reply) {
	int64_t bytes = 0;
	for (const auto& versioned : reply.mutations) {
		bytes = addEstimatedBytes(bytes, sizeof(VersionedMutationsRef));
		bytes = addEstimatedBytes(bytes, estimatedMutationBytes(versioned.mutations));
	}
	return bytes;
}

void NativeCdcOrderedMerge::accept(size_t index, CDCConsumeReply reply) {
	if (!needsRead(index)) {
		throw client_invalid_operation();
	}
	auto& partition = partitions[index];
	validateCommonPosition(reply.lastConsumedVersion);
	if (reply.lastConsumedVersion < partition.through) {
		throw client_invalid_operation();
	}
	Version previous = partition.through;
	for (const auto& versioned : reply.mutations) {
		if (versioned.version <= previous || versioned.version > reply.lastConsumedVersion) {
			throw client_invalid_operation();
		}
		previous = versioned.version;
	}
	const int64_t bytes = estimatedReplyBytes(reply);
	if (bytes > bufferByteLimit - retainedBytes) {
		throw server_overloaded();
	}
	ASSERT(!partition.reply.present());
	partition.through = reply.lastConsumedVersion;
	if (!reply.mutations.empty()) {
		partition.reply = std::move(reply);
		partition.retainedBytes = bytes;
		retainedBytes += bytes;
	}
}

CDCConsumeReply NativeCdcOrderedMerge::next(int64_t replyByteLimit) {
	if (replyByteLimit <= 0) {
		throw client_invalid_operation();
	}
	Version through = std::numeric_limits<Version>::max();
	std::vector<size_t> offsets;
	offsets.reserve(partitions.size());
	for (const auto& partition : partitions) {
		through = std::min(through, partition.through);
		offsets.push_back(partition.offset);
	}
	CDCConsumeReply result;
	result.lastConsumedVersion = through;
	int64_t selectedBytes = 0;
	std::vector<bool> retainedArena(partitions.size(), false);
	while (true) {
		Version version = std::numeric_limits<Version>::max();
		for (size_t i = 0; i < partitions.size(); ++i) {
			const auto& partition = partitions[i];
			if (partition.reply.present() && offsets[i] < partition.reply.get().mutations.size()) {
				version = std::min(version, partition.reply.get().mutations[offsets[i]].version);
			}
		}
		if (version > through) {
			break;
		}
		int64_t versionBytes = sizeof(VersionedMutationsRef);
		int64_t mutationCount = 0;
		for (size_t i = 0; i < partitions.size(); ++i) {
			const auto& partition = partitions[i];
			if (partition.reply.present() && offsets[i] < partition.reply.get().mutations.size()) {
				const auto& versioned = partition.reply.get().mutations[offsets[i]];
				if (versioned.version == version) {
					versionBytes = addEstimatedBytes(versionBytes, estimatedMutationBytes(versioned.mutations));
					mutationCount += versioned.mutations.size();
				}
			}
		}
		if (versionBytes > replyByteLimit - selectedBytes || mutationCount > std::numeric_limits<int>::max()) {
			if (result.mutations.empty()) {
				throw server_overloaded();
			}
			result.lastConsumedVersion = version - 1;
			break;
		}
		VectorRef<MutationRef> merged;
		merged.reserve(result.arena, static_cast<int>(mutationCount));
		for (size_t i = 0; i < partitions.size(); ++i) {
			const auto& partition = partitions[i];
			if (partition.reply.present() && offsets[i] < partition.reply.get().mutations.size()) {
				const auto& versioned = partition.reply.get().mutations[offsets[i]];
				if (versioned.version == version) {
					if (!retainedArena[i]) {
						result.arena.dependsOn(partition.reply.get().arena);
						retainedArena[i] = true;
					}
					for (const auto& mutation : versioned.mutations) {
						merged.push_back(result.arena, mutation);
					}
					++offsets[i];
				}
			}
		}
		result.mutations.push_back(result.arena, VersionedMutationsRef(version, merged));
		selectedBytes += versionBytes;
	}
	// Commit offsets only after the entire bounded reply is materialized, so an
	// oversized first version or allocation failure cannot skip accepted mutations.
	for (size_t i = 0; i < partitions.size(); ++i) {
		auto& partition = partitions[i];
		partition.offset = offsets[i];
		if (partition.reply.present() && partition.offset == partition.reply.get().mutations.size()) {
			retainedBytes -= partition.retainedBytes;
			partition.retainedBytes = 0;
			partition.offset = 0;
			partition.reply.reset();
		}
	}
	currentPosition = result.lastConsumedVersion;
	return result;
}

void NativeCdcOrderedMerge::reset(Version commonPosition) {
	validateCommonPosition(commonPosition);
	for (auto& partition : partitions) {
		partition.reply.reset();
		partition.offset = 0;
		partition.through = commonPosition;
		partition.retainedBytes = 0;
	}
	retainedBytes = 0;
	currentPosition = commonPosition;
}

namespace {

struct TestVersion {
	Version version;
	std::vector<MutationRef> mutations;
};

CDCConsumeReply makeOrderedReply(Version through, std::initializer_list<TestVersion> versions = {}) {
	CDCConsumeReply reply;
	reply.lastConsumedVersion = through;
	for (const auto& version : versions) {
		VectorRef<MutationRef> mutations;
		for (const auto& mutation : version.mutations) {
			mutations.push_back_deep(reply.arena, mutation);
		}
		reply.mutations.push_back(reply.arena, VersionedMutationsRef(version.version, mutations));
	}
	return reply;
}

MutationRef orderedSet(StringRef key, StringRef value = "v"_sr) {
	return MutationRef(MutationRef::SetValue, key, value);
}

template <class Function>
void expectOrderedMergeError(int code, Function&& operation) {
	bool failed = false;
	try {
		operation();
	} catch (Error& error) {
		ASSERT_EQ(error.code(), code);
		failed = true;
	}
	ASSERT(failed);
}

} // namespace

TEST_CASE("/NativeCDC/OrderedMerge/AsymmetricFrontiersAndEmptyGaps") {
	NativeCdcOrderedMerge merge(2, 99, 1 << 20);
	merge.accept(0, makeOrderedReply(110, { { 100, { orderedSet("a"_sr) } }, { 108, { orderedSet("b"_sr) } } }));
	ASSERT(merge.next(1 << 20).mutations.empty());
	ASSERT_EQ(merge.position(), 99);
	ASSERT(!merge.needsRead(0));
	ASSERT(merge.needsRead(1));
	merge.accept(1, makeOrderedReply(104));
	const int64_t retained = merge.bufferedBytes();
	CDCConsumeReply first = merge.next(1 << 20);
	ASSERT_EQ(first.lastConsumedVersion, 104);
	ASSERT_EQ(first.mutations.size(), 1);
	ASSERT_EQ(first.mutations[0].version, 100);
	ASSERT_EQ(merge.bufferedBytes(), retained);
	merge.accept(1, makeOrderedReply(120));
	CDCConsumeReply second = merge.next(1 << 20);
	ASSERT_EQ(second.lastConsumedVersion, 110);
	ASSERT_EQ(second.mutations.size(), 1);
	ASSERT_EQ(second.mutations[0].version, 108);
	ASSERT_EQ(merge.bufferedBytes(), 0);
	merge.accept(0, makeOrderedReply(125));
	CDCConsumeReply gap = merge.next(1 << 20);
	ASSERT(gap.mutations.empty());
	ASSERT_EQ(gap.lastConsumedVersion, 120);
	ASSERT_EQ(first.mutations[0].mutations[0].param1, "a"_sr);
	ASSERT_EQ(second.mutations[0].mutations[0].param1, "b"_sr);
	return Void();
}

TEST_CASE("/NativeCDC/OrderedMerge/CompleteVersionsAndClearOrder") {
	NativeCdcOrderedMerge merge(2, 99, 1 << 20);
	merge.accept(0,
	             makeOrderedReply(100,
	                              { { 100,
	                                  { orderedSet("a"_sr, "before"_sr),
	                                    MutationRef(MutationRef::ClearRange, "a"_sr, "m"_sr),
	                                    orderedSet("b"_sr, "after"_sr) } } }));
	merge.accept(
	    1,
	    makeOrderedReply(
	        100,
	        { { 100, { MutationRef(MutationRef::ClearRange, "m"_sr, "z"_sr), orderedSet("x"_sr, "after"_sr) } } }));
	CDCConsumeReply reply = merge.next(1 << 20);
	ASSERT_EQ(reply.mutations.size(), 1);
	ASSERT_EQ(reply.mutations[0].version, 100);
	const auto& mutations = reply.mutations[0].mutations;
	ASSERT_EQ(mutations.size(), 5);
	ASSERT_EQ(mutations[0].param2, "before"_sr);
	ASSERT_EQ(mutations[1].type, MutationRef::ClearRange);
	ASSERT_EQ(mutations[1].param2, "m"_sr);
	ASSERT_EQ(mutations[2].param1, "b"_sr);
	ASSERT_EQ(mutations[3].type, MutationRef::ClearRange);
	ASSERT_EQ(mutations[3].param1, "m"_sr);
	ASSERT_EQ(mutations[4].param1, "x"_sr);
	ASSERT_EQ(merge.bufferedBytes(), 0);
	return Void();
}

TEST_CASE("/NativeCDC/OrderedMerge/ReplyAndBufferBounds") {
	CDCConsumeReply left = makeOrderedReply(105, { { 100, { orderedSet("a"_sr) } }, { 105, { orderedSet("b"_sr) } } });
	CDCConsumeReply right = makeOrderedReply(105, { { 100, { orderedSet("x"_sr) } } });
	const int64_t bytes =
	    NativeCdcOrderedMerge::estimatedReplyBytes(left) + NativeCdcOrderedMerge::estimatedReplyBytes(right);
	NativeCdcOrderedMerge merge(2, 99, bytes);
	merge.accept(0, left);
	merge.accept(1, right);
	const int64_t firstVersionBytes = sizeof(VersionedMutationsRef) + 2 * (sizeof(MutationRef) + 2);
	expectOrderedMergeError(error_code_server_overloaded, [&]() { merge.next(firstVersionBytes - 1); });
	ASSERT_EQ(merge.position(), 99);
	ASSERT_EQ(merge.bufferedBytes(), bytes);
	CDCConsumeReply first = merge.next(firstVersionBytes);
	ASSERT_EQ(first.mutations.size(), 1);
	ASSERT_EQ(first.mutations[0].mutations.size(), 2);
	ASSERT_EQ(first.lastConsumedVersion, 104);
	CDCConsumeReply second = merge.next(firstVersionBytes);
	ASSERT_EQ(second.mutations.size(), 1);
	ASSERT_EQ(second.mutations[0].version, 105);
	ASSERT_EQ(second.lastConsumedVersion, 105);
	ASSERT_EQ(merge.bufferedBytes(), 0);
	NativeCdcOrderedMerge bounded(2, 99, bytes - 1);
	bounded.accept(0, left);
	expectOrderedMergeError(error_code_server_overloaded, [&]() { bounded.accept(1, right); });
	ASSERT(bounded.needsRead(1));
	ASSERT_EQ(bounded.frontier(1), 99);
	ASSERT_EQ(bounded.bufferedBytes(), NativeCdcOrderedMerge::estimatedReplyBytes(left));
	return Void();
}

TEST_CASE("/NativeCDC/OrderedMerge/ReplayRequiresCommonReset") {
	NativeCdcOrderedMerge merge(2, 99, 1 << 20);
	merge.accept(0, makeOrderedReply(110, { { 100, { orderedSet("a"_sr) } } }));
	merge.accept(1, makeOrderedReply(110));
	CDCConsumeReply retained = merge.next(1 << 20);
	expectOrderedMergeError(error_code_client_invalid_operation, [&]() { merge.accept(0, makeOrderedReply(100)); });
	ASSERT_EQ(merge.position(), 110);
	merge.reset(99);
	ASSERT_EQ(merge.bufferedBytes(), 0);
	ASSERT(merge.needsRead(0));
	ASSERT(merge.needsRead(1));
	merge.accept(0, makeOrderedReply(100, { { 100, { orderedSet("a"_sr) } } }));
	merge.accept(1, makeOrderedReply(100));
	CDCConsumeReply replayed = merge.next(1 << 20);
	ASSERT_EQ(replayed.lastConsumedVersion, 100);
	ASSERT_EQ(retained.mutations[0].mutations[0].param1, replayed.mutations[0].mutations[0].param1);
	return Void();
}

TEST_CASE("/NativeCDC/OrderedMerge/RejectsUncertifiedOrOverlappingReplies") {
	NativeCdcOrderedMerge merge(2, 99, 1 << 20);
	expectOrderedMergeError(error_code_client_invalid_operation,
	                        [&]() { merge.accept(0, makeOrderedReply(100, { { 101, { orderedSet("a"_sr) } } })); });
	expectOrderedMergeError(error_code_client_invalid_operation, [&]() {
		merge.accept(0, makeOrderedReply(101, { { 101, { orderedSet("a"_sr) } }, { 100, { orderedSet("b"_sr) } } }));
	});
	ASSERT_EQ(merge.bufferedBytes(), 0);
	ASSERT(merge.needsRead(0));
	merge.accept(0, makeOrderedReply(100));
	expectOrderedMergeError(error_code_client_invalid_operation, [&]() { merge.accept(0, makeOrderedReply(101)); });
	ASSERT_EQ(merge.frontier(0), 100);
	return Void();
}

void forceLinkNativeCdcOrderedMergeTests() {}
