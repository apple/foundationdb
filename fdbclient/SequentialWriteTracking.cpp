/*
 * SequentialWriteTracking.cpp
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

#include "flow/UnitTest.h"
#include "fdbclient/SequentialWriteTracking.h"

// TODO REMOVE!! just for debugging
void ShardSplitLineage::debugPrintTree(int indent) {
	for (int i = 0; i < indent; i++) {
		printf(" ");
	}
	printf("%p (%d):\n", this, debugGetReferenceCount());
	for (auto& it : children) {
		if (it != nullptr) {
			it->debugPrintTree(indent + 2);
		} else {
			for (int i = 0; i < indent + 2; i++) {
				printf(" ");
			}
			printf("nullptr\n");
		}
	}
}

int ShardSplitLineage::countChildrenWithSplits() {
	int cnt = 0;
	for (auto& child : children) {
		// a non-valid (nullptr) child means it was either split past the max depth, or was split and later merged
		if (child == nullptr || child->children.size()) {
			cnt++;
		}
	}
	return cnt;
}

void ShardSplitLineage::removeSelfFromParent() {
	if (!parent.isValid()) {
		return;
	}

	for (auto childPtr = parent->children.begin(); childPtr != parent->children.end(); ++childPtr) {
		// clear parent's pointer to me
		if (*childPtr == this) {
			*childPtr = nullptr;
			break;
		}
	}
	// if child wasn't cleared something is very wrong, but since this is called in the destructor we can't assert it
}

ShardSplitLineage::~ShardSplitLineage() {
	removeSelfFromParent();
	// destruction will decref parent reference
}

// This algorithm walks up the tree from the child, to see if N splits in a row were only right-most or only
// left-most splits.
// depth is the number of consecutive right/left side splits required to count as "sequential". This means it will
// look at depth+1 nodes.
SequentialWriteState ShardSplitLineage::getSplitState(int depth) {
	// depth == 1 just means this shard is the rightmost/leftmost from its parent, which doesn't predict anything
	// useful
	ASSERT(depth > 1);

	if (!parent.isValid()) {
		return S4Unknown;
	}

	ShardSplitLineage* child = this;

	bool checkingRight;
	// if parent has a split that isn't our split, it's non-sequential.
	// For leaves, this means the parent having any splits means it's non-sequential.
	// For non-leaves, this node counts towards split children, so it must have one other split.
	int parentSplitCount = parent->countChildrenWithSplits();
	if (parentSplitCount > 1 || (parentSplitCount == 1 && children.size() == 0)) {
		return S4NonSequential;
	}
	// if the child wasn't the rightmost/leftmost in the parent, it's not sequential
	if (parent->children[0] == child) {
		checkingRight = false;
	} else if (parent->children[parent->children.size() - 1] == child) {
		checkingRight = true;
	} else {
		return S4NonSequential;
	}

	ShardSplitLineage* p = parent.getPtr();
	while (depth > 1) {
		if (!p->parent.isValid()) {
			return S4Unknown;
		}
		child = p;
		p = p->parent.getPtr();
		depth--;

		// if the parent had more than 1 child that has split, it's not sequential
		if (p->countChildrenWithSplits() != 1) {
			// TODO REMOVE this assert later
			ASSERT(p->countChildrenWithSplits() != 0);
			return S4NonSequential;
		}
		// if the previous node was a rightmost/leftmost split, this one has to be the same
		if (checkingRight && p->children[p->children.size() - 1] != child) {
			return S4NonSequential;
		}
		if (!checkingRight && p->children[0] != child) {
			return S4NonSequential;
		}
	}
	return checkingRight ? S4SequentialIncreasing : S4SequentialDecreasing;
}

// TODO make prune depth a knob instead of a parameter! it should always be the same
// prune depth is the number of splits the tracker should keep (meaning a height of pruneDepth+1 nodes)
// However, only need to track the full depth if it is all potentially sequential, we can prune all parts past the first
// node with > 1 split children, even if it's not at the full depth.
std::vector<Reference<ShardSplitLineage>> splitShardLineage(Reference<ShardSplitLineage> shard,
                                                            int numChildren,
                                                            int pruneDepth) {
	ASSERT(shard->children.size() == 0);
	std::vector<Reference<ShardSplitLineage>> splitChildren;
	splitChildren.reserve(numChildren);
	shard->children.reserve(numChildren);

	for (int i = 0; i < numChildren; i++) {
		// make a new child with shard as its parent
		splitChildren.push_back(makeReference<ShardSplitLineage>(shard));
		shard->children.push_back(splitChildren[i].getPtr());
	}
	// ensure linked tree from this shard has at most depth parents, to conserve memory. Should only have to prune
	// at most 1 level
	if (shard->parent.isValid()) {
		ShardSplitLineage* node = shard->parent.getPtr();
		bool prunedEarly = false;
		// want to keep pruneDepth+1 nodes. We already have node's new children, and node itself.
		while (pruneDepth > 2) {
			if (!node->parent.isValid()) {
				prunedEarly = true;
				break;
			}
			// if this intermediate node has multiple split children, we can prune all nodes above it too
			if (node->countChildrenWithSplits() > 1) {
				node->removeSelfFromParent();
				node->parent.clear();

				prunedEarly = true;
				break;
			}
			node = node->parent.getPtr();
			pruneDepth--;
		}
		if (!prunedEarly) {
			// if we previously pruned parent's parent up to pruneDepth, it shouldn't have a valid parent
			node->removeSelfFromParent();
			node->parent.clear();
		}
	}

	return splitChildren;
}

void SequentialWriteTracker::trackKey(StringRef key) {
	if (isSeqIncreasing && (trackCount == 0 || key > largest)) {
		largest = StringRef(arena, key);
	} else {
		isSeqIncreasing = false;
	}

	if (isSeqDecreasing && (trackCount == 0 || key < smallest)) {
		smallest = StringRef(arena, key);
	} else {
		isSeqDecreasing = false;
	}

	trackCount++;

	// TODO REMOVE!!
	/*if ((trackCount & (trackCount - 1)) == 0) {
	    printf("Seq tracking write %s: %s(%d/%d)%s\n",
	           key.printable().c_str(),
	           isSeqDecreasing ? "<- " : "",
	           trackCount,
	           trackThreshold,
	           isSeqIncreasing ? " ->" : "");
	}*/
}

SequentialWriteState SequentialWriteTracker::getWriteState() {
	if (isSeqIncreasing && trackCount >= trackThreshold) {
		return S4SequentialIncreasing;
	}
	if (isSeqDecreasing && trackCount >= trackThreshold) {
		return S4SequentialDecreasing;
	}
	if ((isSeqIncreasing || isSeqDecreasing) && trackCount < trackThreshold) {
		return S4Unknown;
	} else {
		return S4NonSequential;
	}
}

// SequentialWriteTracker test
TEST_CASE("/SequentialWriteTracking/SequentialWriteTracker") {
	// test strictly increasing writes with 3 write threshold
	SequentialWriteTracker a(3);
	ASSERT(a.getWriteState() == S4Unknown);

	a.trackKey(LiteralStringRef("A"));
	ASSERT(a.getWriteState() == S4Unknown);

	a.trackKey(LiteralStringRef("B"));
	ASSERT(a.getWriteState() == S4Unknown);

	a.trackKey(LiteralStringRef("C"));
	ASSERT(a.getWriteState() == S4SequentialIncreasing);

	a.trackKey(LiteralStringRef("D"));
	ASSERT(a.getWriteState() == S4SequentialIncreasing);

	a.trackKey(LiteralStringRef("D"));
	ASSERT(a.getWriteState() == S4NonSequential);

	// test strictly decreasing writes with 3 write threshold
	SequentialWriteTracker b(3);
	ASSERT(b.getWriteState() == S4Unknown);

	b.trackKey(LiteralStringRef("Z"));
	ASSERT(b.getWriteState() == S4Unknown);

	b.trackKey(LiteralStringRef("Y"));
	ASSERT(b.getWriteState() == S4Unknown);

	b.trackKey(LiteralStringRef("X"));
	ASSERT(b.getWriteState() == S4SequentialDecreasing);

	b.trackKey(LiteralStringRef("W"));
	ASSERT(b.getWriteState() == S4SequentialDecreasing);

	b.trackKey(LiteralStringRef("Z"));
	ASSERT(b.getWriteState() == S4NonSequential);

	// test sequential writes after non-sequential writes to make sure it remembers the previous ones
	SequentialWriteTracker c(3);
	ASSERT(c.getWriteState() == S4Unknown);

	c.trackKey(LiteralStringRef("Z"));
	ASSERT(c.getWriteState() == S4Unknown);

	c.trackKey(LiteralStringRef("A"));
	ASSERT(c.getWriteState() == S4Unknown);

	c.trackKey(LiteralStringRef("B"));
	ASSERT(c.getWriteState() == S4NonSequential);

	c.trackKey(LiteralStringRef("C"));
	ASSERT(c.getWriteState() == S4NonSequential);

	c.trackKey(LiteralStringRef("D"));
	ASSERT(c.getWriteState() == S4NonSequential);

	c.trackKey(LiteralStringRef("E"));
	ASSERT(c.getWriteState() == S4NonSequential);

	return Void();
}

// Overview of ShardSplitLineage test:
// Use check and pruning depth of 3 splits.
// Start with initial split history of:
//
//   B
//  / \
// A   C
//    / \
//   D   E
//
// Then split shard E into F + G. Now the tree is:
//
//   B
//  / \
// A   C
//    / \
//   D   E
//      / \
//     F   G
//
// Now G should be sequential-increasing.
// If we then split A into H+I, the tree would look like:
//
//      B
//    /   \
//   A     C
//  / \   / \
//  H I   D E
//         / \
//         F G
//
// And now G should be non-sequential, along with all of the other leaf shards.
// If we then merge H+I, the A+H+I subtree should be removed, but it should still be counted as a split child in B, so G
// should still be non-sequential, resulting in a tree of:
//
//      B
//    /   \
//         C
//        / \
//        D E
//         / \
//         F G
//
// If we split G into J+K, we can test pruning. Now the tree is:
//
//   C
//  / \
// D   E
//    / \
//   F   G
//      / \
//     J   K
//
// K should be sequential-increasing, and B should have been freed.
// If we then split D into L + M, L into N + O, and N into P + Q, we should get the tree(s):
//
//                 C
//                / \
//         D         E
//        / \       / \
//       L   M     F   G
//      / \           / \
//     N   O         J   K
//    / \
//   P   Q
//
// K would no longer be sequential-increasing because it lost its split history for C, but now P should be
// sequential-decreasing.

TEST_CASE("/SequentialWriteTracking/ShardSplitLineage") {
	int checkDepth = 3;
	int pruneDepth = checkDepth;
	Reference<ShardSplitLineage> rootRef = makeReference<ShardSplitLineage>();
	ShardSplitLineage* nodeB = rootRef.getPtr();

	// split B into A+C
	std::vector<Reference<ShardSplitLineage>> vAC = splitShardLineage(rootRef, 2, pruneDepth);
	ShardSplitLineage* nodeA = vAC[0].getPtr();
	ShardSplitLineage* nodeC = vAC[1].getPtr();
	rootRef.clear();

	// split C into D+E
	std::vector<Reference<ShardSplitLineage>> vDE = splitShardLineage(vAC[1], 2, pruneDepth);
	ShardSplitLineage* nodeD = vDE[0].getPtr();
	ShardSplitLineage* nodeE = vDE[1].getPtr();
	vAC[1].clear();

	// TODO REMOVE
	nodeB->debugPrintTree(0);

	// check first state
	ASSERT(nodeA->debugGetReferenceCount() == 1);
	ASSERT(nodeB->debugGetReferenceCount() == 2);
	ASSERT(nodeC->debugGetReferenceCount() == 2);
	ASSERT(nodeD->debugGetReferenceCount() == 1);
	ASSERT(nodeE->debugGetReferenceCount() == 1);

	ASSERT(nodeA->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeD->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeE->getSplitState(checkDepth) == S4Unknown);

	// split E into F+G
	std::vector<Reference<ShardSplitLineage>> vFG = splitShardLineage(vDE[1], 2, pruneDepth);
	ShardSplitLineage* nodeF = vFG[0].getPtr();
	ShardSplitLineage* nodeG = vFG[1].getPtr();
	vDE[1].clear();

	// TODO REMOVE
	nodeB->debugPrintTree(0);

	ASSERT(nodeE->debugGetReferenceCount() == 2);
	ASSERT(nodeF->debugGetReferenceCount() == 1);
	ASSERT(nodeG->debugGetReferenceCount() == 1);

	ASSERT(nodeA->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeD->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeG->getSplitState(checkDepth) == S4SequentialIncreasing);

	// split A into H+I
	std::vector<Reference<ShardSplitLineage>> vHI = splitShardLineage(vAC[0], 2, pruneDepth);
	ShardSplitLineage* nodeH = vHI[0].getPtr();
	ShardSplitLineage* nodeI = vHI[1].getPtr();
	vAC[0].clear();

	// TODO REMOVE
	nodeB->debugPrintTree(0);

	ASSERT(nodeA->debugGetReferenceCount() == 2);
	ASSERT(nodeH->debugGetReferenceCount() == 1);
	ASSERT(nodeI->debugGetReferenceCount() == 1);

	ASSERT(nodeD->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeG->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeH->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeI->getSplitState(checkDepth) == S4NonSequential);

	// delete H
	vHI[0].clear();

	ASSERT(nodeA->debugGetReferenceCount() == 1);
	ASSERT(nodeB->debugGetReferenceCount() == 2);
	ASSERT(nodeA->parent.isValid());

	// delete I
	vHI[1].clear();

	ASSERT(nodeB->debugGetReferenceCount() == 1);
	ASSERT(nodeB->children[0] == nullptr);

	ASSERT(nodeD->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeG->getSplitState(checkDepth) == S4NonSequential);

	// split G into J+K
	std::vector<Reference<ShardSplitLineage>> vJK = splitShardLineage(vFG[1], 2, pruneDepth);
	ShardSplitLineage* nodeJ = vJK[0].getPtr();
	ShardSplitLineage* nodeK = vJK[1].getPtr();
	vFG[1].clear();

	nodeC->debugPrintTree(0);

	ASSERT(nodeG->debugGetReferenceCount() == 2);
	ASSERT(nodeJ->debugGetReferenceCount() == 1);
	ASSERT(nodeK->debugGetReferenceCount() == 1);

	// check B was pruned
	ASSERT(nodeC->debugGetReferenceCount() == 2);
	ASSERT(!nodeC->parent.isValid());

	ASSERT(nodeD->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeJ->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeK->getSplitState(checkDepth) == S4SequentialIncreasing);

	// split D into L+M
	std::vector<Reference<ShardSplitLineage>> vLM = splitShardLineage(vDE[0], 2, pruneDepth);
	ShardSplitLineage* nodeL = vLM[0].getPtr();
	ShardSplitLineage* nodeM = vLM[1].getPtr();
	vDE[0].clear();

	nodeC->debugPrintTree(0);

	ASSERT(nodeD->debugGetReferenceCount() == 2);
	ASSERT(nodeL->debugGetReferenceCount() == 1);
	ASSERT(nodeM->debugGetReferenceCount() == 1);

	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeJ->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeK->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeL->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeM->getSplitState(checkDepth) == S4NonSequential);

	// split L into N+O
	std::vector<Reference<ShardSplitLineage>> vNO = splitShardLineage(vLM[0], 2, pruneDepth);
	ShardSplitLineage* nodeN = vNO[0].getPtr();
	ShardSplitLineage* nodeO = vNO[1].getPtr();
	vLM[0].clear();

	nodeC->debugPrintTree(0);

	ASSERT(nodeL->debugGetReferenceCount() == 2);
	ASSERT(nodeN->debugGetReferenceCount() == 1);
	ASSERT(nodeO->debugGetReferenceCount() == 1);

	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeJ->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeK->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeM->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeN->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeO->getSplitState(checkDepth) == S4NonSequential);

	// split N into P+Q
	std::vector<Reference<ShardSplitLineage>> vPQ = splitShardLineage(vNO[0], 2, pruneDepth);
	ShardSplitLineage* nodeP = vPQ[0].getPtr();
	ShardSplitLineage* nodeQ = vPQ[1].getPtr();
	vNO[0].clear();

	nodeD->debugPrintTree(0);
	nodeE->debugPrintTree(0);

	ASSERT(nodeN->debugGetReferenceCount() == 2);
	ASSERT(nodeP->debugGetReferenceCount() == 1);
	ASSERT(nodeQ->debugGetReferenceCount() == 1);

	// check that D and C were separated
	ASSERT(nodeC->debugGetReferenceCount() == 1);
	ASSERT(nodeD->debugGetReferenceCount() == 2);
	ASSERT(!nodeD->parent.isValid());
	ASSERT(nodeC->children[0] == nullptr);

	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeJ->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeK->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeM->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeO->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeP->getSplitState(checkDepth) == S4SequentialDecreasing);
	ASSERT(nodeQ->getSplitState(checkDepth) == S4NonSequential);

	return Void();
}

// Build the following split lineage tree:
//            A
//          /   \
//         B     C
//       /   \
//      D     E
//     / \
//    F   G
//
// Splitting E should cause A to get pruned from B's subtree because B has multiple split children. So keeping A is
// useless because no child of B can be considered sequential, so A is removed from that subtree even though it's not at
// the prune depth.

// TODO UNCOMMENT!!!
TEST_CASE("/SequentialWriteTracking/ShardSplitLineage/testEarlyPrune") {
	int checkDepth = 5; // larger so that we don't prune automatically
	int pruneDepth = checkDepth;
	Reference<ShardSplitLineage> rootRef = makeReference<ShardSplitLineage>();
	ShardSplitLineage* nodeA = rootRef.getPtr();

	// split A into B+C
	std::vector<Reference<ShardSplitLineage>> vBC = splitShardLineage(rootRef, 2, pruneDepth);
	ShardSplitLineage* nodeB = vBC[0].getPtr();
	ShardSplitLineage* nodeC = vBC[1].getPtr();
	rootRef.clear();

	// split B into D+E
	std::vector<Reference<ShardSplitLineage>> vDE = splitShardLineage(vBC[0], 2, pruneDepth);
	ShardSplitLineage* nodeD = vDE[0].getPtr();
	ShardSplitLineage* nodeE = vDE[1].getPtr();
	vBC[0].clear();

	// split D into F+G
	std::vector<Reference<ShardSplitLineage>> vFG = splitShardLineage(vDE[0], 2, pruneDepth);
	ShardSplitLineage* nodeF = vFG[0].getPtr();
	ShardSplitLineage* nodeG = vFG[1].getPtr();
	vDE[0].clear();

	// quick sanity check

	nodeA->debugPrintTree(0);

	ASSERT(nodeA->debugGetReferenceCount() == 2);
	ASSERT(nodeB->debugGetReferenceCount() == 2);
	ASSERT(nodeC->debugGetReferenceCount() == 1);
	ASSERT(nodeD->debugGetReferenceCount() == 2);
	ASSERT(nodeE->debugGetReferenceCount() == 1);
	ASSERT(nodeF->debugGetReferenceCount() == 1);
	ASSERT(nodeG->debugGetReferenceCount() == 1);

	ASSERT(nodeF->getSplitState(checkDepth) == S4Unknown);
	ASSERT(nodeG->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeE->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeC->getSplitState(checkDepth) == S4NonSequential);

	// split E into H+I
	std::vector<Reference<ShardSplitLineage>> vHI = splitShardLineage(vDE[1], 2, pruneDepth);
	ShardSplitLineage* nodeH = vHI[0].getPtr();
	ShardSplitLineage* nodeI = vHI[1].getPtr();
	vDE[1].clear();

	// make sure A->B was pruned
	ASSERT(nodeA->debugGetReferenceCount() == 1);
	ASSERT(!nodeB->parent.isValid());

	ASSERT(nodeE->debugGetReferenceCount() == 2);
	ASSERT(nodeH->debugGetReferenceCount() == 1);
	ASSERT(nodeI->debugGetReferenceCount() == 1);

	ASSERT(nodeF->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeH->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeI->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeE->getSplitState(checkDepth) == S4NonSequential);
	ASSERT(nodeC->getSplitState(checkDepth) == S4NonSequential);

	return Void();
}