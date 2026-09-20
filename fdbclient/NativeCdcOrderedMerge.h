/*
 * NativeCdcOrderedMerge.h
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

#ifndef FDBCLIENT_NATIVECDCORDEREDMERGE_H
#define FDBCLIENT_NATIVECDCORDEREDMERGE_H
#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "fdbclient/CDCProxyInterface.h"

// Holds one reply per disjoint partition and emits complete commit-version groups.
// Replies must continue from frontier(index), and their cursors must certify all
// mutations through that version, including empty gaps. Owner replacement or
// replay requires resetting every partition to one common acknowledged position
// before accepting another reply. This class never acknowledges physical streams.
class NativeCdcOrderedMerge {
public:
	NativeCdcOrderedMerge(size_t partitionCount, Version commonPosition, int64_t bufferByteLimit);

	size_t partitionCount() const { return partitions.size(); }
	bool needsRead(size_t index) const;
	Version frontier(size_t index) const;
	Version position() const { return currentPosition; }
	// Record/payload estimate for complete retained reply arenas, including their
	// already-returned prefixes. Allocator overhead and caller-owned replies are excluded.
	int64_t bufferedBytes() const { return retainedBytes; }
	static int64_t estimatedReplyBytes(CDCConsumeReply const& reply);

	// Accepts only a partition needing another read. Failure leaves merge state unchanged.
	void accept(size_t index, CDCConsumeReply reply);
	// An unchanged empty reply means another partition must advance. A first
	// complete version exceeding the limit fails without advancing any partition.
	CDCConsumeReply next(int64_t replyByteLimit);
	void reset(Version commonPosition);

private:
	class Partition {
	public:
		Partition() = default;

	private:
		friend class NativeCdcOrderedMerge;
		Optional<CDCConsumeReply> reply;
		size_t offset = 0;
		Version through = invalidVersion;
		int64_t retainedBytes = 0;
	};

	std::vector<Partition> partitions;
	Version currentPosition;
	int64_t bufferByteLimit;
	int64_t retainedBytes = 0;
};

void forceLinkNativeCdcOrderedMergeTests();

#endif // FDBCLIENT_NATIVECDCORDEREDMERGE_H
