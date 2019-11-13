/*
 * DeltaTree.h
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

#pragma once

#include "flow/flow.h"
#include "flow/Arena.h"
#include "fdbclient/FDBTypes.h"
#include "fdbserver/Knobs.h"
#include <string.h>

typedef uint64_t Word;
static inline int commonPrefixLength(uint8_t const* ap, uint8_t const* bp, int cl) {
	int i = 0;
	const int wordEnd = cl - sizeof(Word) + 1;

	for(; i < wordEnd; i += sizeof(Word)) {
		Word a = *(Word *)ap;
		Word b = *(Word *)bp;
		if(a != b) {
			return i + ctzll(a ^ b) / 8;
		}
		ap += sizeof(Word);
		bp += sizeof(Word);
	}

	for (; i < cl; i++) {
		if (*ap != *bp) {
			return i;
		}
		++ap;
		++bp;
	}
	return cl;
}

static int commonPrefixLength(StringRef a, StringRef b) {
	return commonPrefixLength(a.begin(), b.begin(), std::min(a.size(), b.size()));
}

// This appears to be the fastest version
static int lessOrEqualPowerOfTwo(int n) {
	int p;
	for (p = 1; p+p <= n; p+=p);
	return p;
}

/*
static int _lessOrEqualPowerOfTwo(uint32_t n) {
	if(n == 0)
		return n;
	int trailing = __builtin_ctz(n);
	int leading = __builtin_clz(n);
	if(trailing + leading == ((sizeof(n) * 8) - 1))
		return n;
	return 1 << ( (sizeof(n) * 8) - leading - 1);
}

static int __lessOrEqualPowerOfTwo(unsigned int n) {
	int p = 1;
	for(; p <= n; p <<= 1);
	return p >> 1;
}
*/

static int perfectSubtreeSplitPoint(int subtree_size) {
	// return the inorder index of the root node in a subtree of the given size
	// consistent with the resulting binary search tree being "perfect" (having minimal height 
	// and all missing nodes as far right as possible).
	// There has to be a simpler way to do this.
	int s = lessOrEqualPowerOfTwo((subtree_size - 1) / 2 + 1) - 1;
	return std::min(s * 2 + 1, subtree_size - s - 1);
}

static int perfectSubtreeSplitPointCached(int subtree_size) {
	static uint16_t *points = nullptr;
	static const int max = 500;
	if(points == nullptr) {
		points = new uint16_t[max];
		for(int i = 0; i < max; ++i)
			points[i] = perfectSubtreeSplitPoint(i);
	}

	if(subtree_size < max)
		return points[subtree_size];
	return perfectSubtreeSplitPoint(subtree_size);
}

// Delta Tree is a memory mappable binary tree of T objects such that each node's item is
// stored as a Delta which can reproduce the node's T item given the node's greatest
// lesser ancestor and the node's least greater ancestor.
//
// The Delta type is intended to make use of ordered prefix compression and borrow all
// available prefix bytes from the ancestor T which shares the most prefix bytes with
// the item T being encoded.
//
// T requirements
//
//    Must be compatible with Standalone<T> and must implement the following additional methods:
//
//    // Writes to d a delta which can create *this from base
//    // commonPrefix can be passed in if known
//    void writeDelta(dT &d, const T &base, int commonPrefix = -1) const;
//
//    // Compare *this to t, returns < 0 for less than, 0 for equal, > 0 for greater than
//    int compare(const T &rhs) const;
//
//    // Get the common prefix bytes between *this and base
//    // skip is a hint of how many prefix bytes are already known to be the same
//    int getCommonPrefixLen(const T &base, int skip) const;
//
//    // Returns the size of the delta object needed to make *this from base
//    // TODO: Explain contract required for deltaSize to be used to predict final 
//    // balanced tree size incrementally while adding sorted items to a build set
//    int deltaSize(const T &base) const;
//
// DeltaT requirements
//
//    // Returns the size of this dT instance
//    int size();
//
//    // Returns the T created by applying the delta to prev or next
//    T apply(const T &base, Arena &localStorage) const;
//
//    // Stores a boolean which DeltaTree will later use to determine the base node for a node's delta
//    void setPrefixSource(bool val);
//
//    // Retrieves the previously stored boolean
//    bool getPrefixSource() const;
//
#pragma pack(push,1)
template <typename T, typename DeltaT = typename T::Delta, typename OffsetT = uint16_t>
struct DeltaTree {

	static int MaximumTreeSize() {
		return std::numeric_limits<OffsetT>::max();
	};

	struct Node {
		OffsetT leftChildOffset;
		OffsetT rightChildOffset;

		inline DeltaT & delta() {
			return *(DeltaT *)(this + 1);
		};

		inline const DeltaT & delta() const {
			return *(const DeltaT *)(this + 1);
		};

		Node * rightChild() const {
			//printf("Node(%p): leftOffset=%d  rightOffset=%d  deltaSize=%d\n", this, (int)leftChildOffset, (int)rightChildOffset, (int)delta().size());
			return rightChildOffset == 0 ? nullptr : (Node *)((uint8_t *)&delta() + rightChildOffset);
		}

		Node * leftChild() const {
			//printf("Node(%p): leftOffset=%d  rightOffset=%d  deltaSize=%d\n", this, (int)leftChildOffset, (int)rightChildOffset, (int)delta().size());
			return leftChildOffset == 0 ? nullptr : (Node *)((uint8_t *)&delta() + leftChildOffset);
		}

		int size() const {
			return sizeof(Node) + delta().size();
		}
	};

	struct {
		OffsetT nodeBytes;     // Total size of all Nodes including the root
		uint8_t initialDepth;  // Levels in the tree as of the last rebuild
	};
#pragma pack(pop)

	inline Node & root() {
		return *(Node *)(this + 1);
	}

	inline const Node & root() const {
		return *(const Node *)(this + 1);
	}

	int size() const {
		return sizeof(DeltaTree) + nodeBytes; 
	}

public:
	// Get count of total overhead bytes (everything but the user-formatted Delta) for a tree given size n
	static inline int GetTreeOverhead(int n = 0) {
		return sizeof(DeltaTree) + (n * sizeof(Node));
	}

	struct DecodedNode {
		DecodedNode(Node *raw, const T *prev, const T *next, Arena &arena)
		  : raw(raw), parent(nullptr), left(nullptr), right(nullptr), prev(prev), next(next),
		    item(raw->delta().apply(raw->delta().getPrefixSource() ? *prev : *next, arena))
		{
			//printf("DecodedNode1 raw=%p delta=%s\n", raw, raw->delta().toString().c_str());
		}
		  
		DecodedNode(Node *raw, DecodedNode *parent, bool left, Arena &arena)
		  : parent(parent), raw(raw), left(nullptr), right(nullptr),
		    prev(left ? parent->prev : &parent->item),
		    next(left ? &parent->item : parent->next),
		    item(raw->delta().apply(raw->delta().getPrefixSource() ? *prev : *next, arena))
		{
			//printf("DecodedNode2 raw=%p delta=%s\n", raw, raw->delta().toString().c_str());
		}

		Node *raw;
		DecodedNode *parent;
		DecodedNode *left;
		DecodedNode *right;
		const T *prev;  // greatest ancestor to the left
		const T *next;  // least ancestor to the right
		T item;

		DecodedNode *getRight(Arena &arena) {
			if(right == nullptr) {
				Node *n = raw->rightChild();
				if(n != nullptr) {
					right = new (arena) DecodedNode(n, this, false, arena);
				}
			}
			return right;
		}

		DecodedNode *getLeft(Arena &arena) {
			if(left == nullptr) {
				Node *n = raw->leftChild();
				if(n != nullptr) {
					left = new (arena) DecodedNode(n, this, true, arena);
				}
			}
			return left;
		}
	};

	struct Cursor;

	// A Reader is used to read a Tree by getting cursors into it.
	// Any node decoded by any cursor is placed in cache for use
	// by other cursors.
	struct Reader : FastAllocated<Reader> {
		Reader(const void *treePtr = nullptr, const T *lowerBound = nullptr, const T *upperBound = nullptr)
			: tree((DeltaTree *)treePtr), lower(lowerBound), upper(upperBound)  {

			// TODO: Remove these copies into arena and require users of Reader to keep prev and next alive during its lifetime
			lower = new(arena) T(arena, *lower);
			upper = new(arena) T(arena, *upper);

			root = (tree->nodeBytes == 0) ? nullptr : new (arena) DecodedNode(&tree->root(), lower, upper, arena);
		}

		const T *lowerBound() const {
			return lower;
		}

		const T *upperBound() const {
			return upper;
		}

		Arena arena;
		DeltaTree *tree;
		DecodedNode *root;
		const T *lower;
		const T *upper;

		Cursor getCursor() {
			return Cursor(this);
		}
	};

	// Cursor provides a way to seek into a DeltaTree and iterate over its contents
	// All Cursors from a Reader share the same decoded node 'cache' (tree of DecodedNodes)
	struct Cursor {
		Cursor() : reader(nullptr), node(nullptr) {
		}

		Cursor(Reader *r) : reader(r), node(reader->root) {
		}

		Reader *reader;
		DecodedNode *node;

		bool valid() const {
			return node != nullptr;
		}

		const T & get() const {
			return node->item;
		}

		const T & getOrUpperBound() const {
			return valid() ? node->item : *reader->upperBound();
		}

		// Moves the cursor to the node with the greatest key less than or equal to s.  If successful,
		// returns true, otherwise returns false and the cursor will be at the node with the next key
		// greater than s.
		bool seekLessThanOrEqual(const T &s) {
			node = nullptr;
			DecodedNode *n = reader->root;

			while(n != nullptr) {
				int cmp = s.compare(n->item);

				if(cmp == 0) {
					node = n;
					return true;
				}

				if(cmp < 0) {
					n = n->getLeft(reader->arena);
				}
				else {
					// n < s so store it in node as a potential result
					node = n;
					n = n->getRight(reader->arena);
				}
			}

			return node != nullptr;
		}

		bool moveFirst() {
			DecodedNode *n = reader->root;
			node = n;
			while(n != nullptr) {
				n = n->getLeft(reader->arena);
				if(n != nullptr)
					node = n;
			}
			return node != nullptr;
		}

		bool moveLast() {
			DecodedNode *n = reader->root;
			node = n;
			while(n != nullptr) {
				n = n->getRight(reader->arena);
				if(n != nullptr)
					node = n;
			}
			return node != nullptr;
		}

		bool moveNext() {
			// Try to go right
			DecodedNode *n = node->getRight(reader->arena);
			if(n != nullptr) {
				// Go left as far as possible
				while(n != nullptr) {
					node = n;
					n = n->getLeft(reader->arena);
				}
				return true;
			}

			// Follow parent links until a greater parent is found
			while(node->parent != nullptr) {
				bool greaterParent = node->parent->left == node;
				node = node->parent;
				if(greaterParent) {
					return true;
				}
			}

			node = nullptr;
			return false;
		}

		bool movePrev() {
			// Try to go left
			DecodedNode *n = node->getLeft(reader->arena);
			if(n != nullptr) {
				// Go right as far as possible
				while(n != nullptr) {
					node = n;
					n = n->getRight(reader->arena);
				}
				return true;
			}

			// Follow parent links until a lesser parent is found
			while(node->parent != nullptr) {
				bool lesserParent = node->parent->right == node;
				node = node->parent;
				if(lesserParent) {
					return true;
				}
			}

			node = nullptr;
			return false;
		}
	};

	// Returns number of bytes written
	int build(const T *begin, const T *end, const T *prev, const T *next) {
		//printf("tree size: %d   node size: %d\n", sizeof(DeltaTree), sizeof(Node));
		int count = end - begin;
		initialDepth = (uint8_t)log2(count) + 1;

		// The boundary leading to the new page acts as the last time we branched right
		if(begin != end) {
			nodeBytes = build(root(), begin, end, prev, next, prev->getCommonPrefixLen(*next, 0));
		}
		else {
			nodeBytes = 0;
		}
		return size();
	}

private:
	static OffsetT build(Node &root, const T *begin, const T *end, const T *prev, const T *next, int subtreeCommon) {
		//printf("build: %s to %s\n", begin->toString().c_str(), (end - 1)->toString().c_str());
		//printf("build: root at %p  sizeof(Node) %d  delta at %p  \n", &root, sizeof(Node), &root.delta());
		ASSERT(end != begin);
		int count = end - begin;

		// Find key to be stored in root
		int mid = perfectSubtreeSplitPointCached(count);
		const T &item = begin[mid];

		int commonWithPrev = item.getCommonPrefixLen(*prev, subtreeCommon);
		int commonWithNext = item.getCommonPrefixLen(*next, subtreeCommon);

		bool prefixSourcePrev;
		int commonPrefix;
		const T *base;
		if(commonWithPrev >= commonWithNext) {
			prefixSourcePrev = true;
			commonPrefix = commonWithPrev;
			base = prev;
		}
		else {
			prefixSourcePrev = false;
			commonPrefix = commonWithNext;
			base = next;
		}

		int deltaSize = item.writeDelta(root.delta(), *base, commonPrefix);
		root.delta().setPrefixSource(prefixSourcePrev);
		//printf("Serialized %s to %p\n", item.toString().c_str(), &root.delta());

		// Continue writing after the serialized Delta.
		uint8_t *wptr = (uint8_t *)&root.delta() + deltaSize;

		// Serialize left child
		if(count > 1) {
			wptr += build(*(Node *)wptr, begin, begin + mid, prev, &item, commonWithPrev);
			root.leftChildOffset = deltaSize;
		}
		else {
			root.leftChildOffset = 0;
		}

		// Serialize right child
		if(count > 2) {
			root.rightChildOffset = wptr - (uint8_t *)&root.delta();
			wptr += build(*(Node *)wptr, begin + mid + 1, end, &item, next, commonWithNext);
		}
		else {
			root.rightChildOffset = 0;
		}

		return wptr - (uint8_t *)&root;
	}
};
