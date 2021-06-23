/*
 * SequentialWriteTracking.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBSERVER_SEQUENTIAL_WRITE_TRACKING_H
#define FDBSERVER_SEQUENTIAL_WRITE_TRACKING_H

#include "flow/Arena.h"
#include "flow/FastRef.h"
#include <vector>

// Abbreviate SequentialWriteState to S4 in the enum.
// Unknown is "it could be sequential but we don't have enough history to tell for sure"
// NonSequential is "it is definitely not sequential"
// And Sequential Increasing/Decreasing is "it is sequential in that direction"

enum SequentialWriteState {
	S4Unknown = 0,
	S4NonSequential = 1,
	S4SequentialIncreasing = 2,
	S4SequentialDecreasing = 3
};
static const char* SequentialWriteStateNames[] = { "Unknown",
	                                               "Non-Sequential",
	                                               "SequentialIncreasing",
	                                               "SequentialDecreasing" };

static const char* getSequentialWriteStateName(SequentialWriteState st) {
	return SequentialWriteStateNames[st];
}

// ShardSplitLineage

// ShardSplitLineage represents a tree of the history of how a current shard came to be, through splits.
// The purpose of this data structure is to detect sequential and semi-sequential write patterns,
// by checking if the hot shard being split is always at the beginning/end of the previously split shard.
//
// For example, the following tree could represent a history of shard splits:
//     [00-FF)
//     /     \
// [00-70)  [70-FF)
//          /     \
//      [70-80)  [80-FF)
//               /     \
//           [80-A0)  [A0-FF)
//
// Where the current shards in the DB are:
// [00-70), [70-80), [80-A0), [A0-FF)
//
// In this particular example, the shard being split was always the right-most shard in this section of the key-space,
// which indicates a sequential or mostly sequential write pattern.
// So, if [A0-FF) was the next shard to split, we could assume that the sequential write pattern was likely to continue,
// and split that shard to have the higher split be smaller and the lower split be larger.
// But, if any of the other shards ( [00-70), [70-80), or [80-A0) ) were the next to split in this subtree instead of
// [A0-FF), then it is likely that the write pattern is not sequential, and when [A0-FF) does need to split eventually,
// it would be split normally.
// This pattern would also apply to the left-most shard always splitting for reverse sequential or reverse
// semi-sequential writes.
// Because the current shards are actively tracked with the key ranges, we don't actually need to store the key ranges
// in the split history, just the overall "shape" of the history of splits.

// The memory model is that a child always has a pointer to the parent, and all of the leaves are explicitly kept in a
// reference. So when those child references are freed, all of the parents in the DAG are freed as well.
// The only complication is that, the parent holds a non-reference counted pointer to the child, so the child has to
// tell the parent to null out the pointer to itself when destructing
struct ShardSplitLineage : ReferenceCounted<ShardSplitLineage>, NonCopyable {
	Reference<ShardSplitLineage> parent;
	std::vector<ShardSplitLineage*> children{}; // allocate a vector with zero capacity initially

	ShardSplitLineage() : parent(Reference<ShardSplitLineage>()) {
		// TODO REMOVE
		ASSERT(children.capacity() == 0);
	}

	ShardSplitLineage(Reference<ShardSplitLineage> parent) : parent(parent) {
		// TODO REMOVE
		ASSERT(children.capacity() == 0);
	}

	void debugPrintTree(int indent);
	int countChildrenWithSplits();
	SequentialWriteState getSplitState(int checkDepth);
	void removeSelfFromParent();
	~ShardSplitLineage();
};

// not a method on struct because it needs the wrapping reference to the shard
std::vector<Reference<ShardSplitLineage>> splitShardLineage(Reference<ShardSplitLineage> shard,
                                                            int numChildren,
                                                            int pruneDepth);

// SequentialWriteTracker

// The Sequential write tracker tracks the history of the order of mutations written to a shard, to identify if the
// shard is being written to sequentially. It requires a certain amount of writes to determine that the workload is
// indeed sequential.

// FIXME: might be slightly more efficient to combine this with the SS byte sample? Might have drawbacks too though.

struct SequentialWriteTracker {

	void trackKey(StringRef key);
	SequentialWriteState getWriteState();

	// TODO knob for default limit?
	SequentialWriteTracker() : isSeqIncreasing(true), isSeqDecreasing(true), trackCount(0), trackThreshold(100) {}
	SequentialWriteTracker(int trackThreshold)
	  : isSeqIncreasing(true), isSeqDecreasing(true), trackCount(0), trackThreshold(trackThreshold) {}

private:
	bool isSeqIncreasing;
	bool isSeqDecreasing;
	int trackCount;
	int trackThreshold;
	// for tracking largest/smallest keys
	Arena arena;
	StringRef largest;
	StringRef smallest;
};

#endif