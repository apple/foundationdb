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
//    // The first skipLen bytes can be assumed to be equal
//    int compare(const T &rhs, int skipLen) const;
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
			return rightChildOffset == 0 ? nullptr : (Node *)((uint8_t *)this + rightChildOffset);
		}

		Node * leftChild() const {
			//printf("Node(%p): leftOffset=%d  rightOffset=%d  deltaSize=%d\n", this, (int)leftChildOffset, (int)rightChildOffset, (int)delta().size());
			return leftChildOffset == 0 ? nullptr : (Node *)((uint8_t *)this + leftChildOffset);
		}

		int size() const {
			return sizeof(Node) + delta().size();
		}
	};

	struct {
		OffsetT numItems;         // Number of items in the tree.
		OffsetT nodeBytesUsed;    // Bytes in use by tree, exluding overhead
		OffsetT nodeBytesFree;    // Bytes left at end of tree to expand into
		OffsetT nodeBytesDeleted; // Delta bytes deleted from tree.  Note that some of these bytes could be borrowed by descendents.
		uint8_t initialHeight;    // Height of tree as originally built
		uint8_t maxHeight;        // Maximum height of tree after any insertion.  Value of 0 means no insertions done.
	};
#pragma pack(pop)

	inline Node & root() {
		return *(Node *)(this + 1);
	}

	inline const Node & root() const {
		return *(const Node *)(this + 1);
	}

	int size() const {
		return sizeof(DeltaTree) + nodeBytesUsed; 
	}

	inline Node & newNode() {
		return *(Node *)((uint8_t *)this + size());
	}

public:
	// Get count of total overhead bytes (everything but the user-formatted Delta) for a tree given size n
	static inline int GetTreeOverhead(int n = 0) {
		return sizeof(DeltaTree) + (n * sizeof(Node));
	}

	struct DecodedNode {
		DecodedNode() {}

		// construct root node
		DecodedNode(Node *raw, const T *prev, const T *next, Arena &arena)
		  : raw(raw), parent(nullptr), otherAncestor(nullptr), leftChild(nullptr), rightChild(nullptr), prev(prev), next(next),
		    item(raw->delta().apply(raw->delta().getPrefixSource() ? *prev : *next, arena))
		{
			//printf("DecodedNode1 raw=%p delta=%s\n", raw, raw->delta().toString().c_str());
		}
		  
		// Construct non-root node
		// wentLeft indicates that we've gone left to get to the raw node.		  
		DecodedNode(Node *raw, DecodedNode *parent, bool wentLeft, Arena &arena)
		  : parent(parent), otherAncestor(wentLeft ? parent->getPrevAncestor() : parent->getNextAncestor()),
		  	prev(wentLeft ? parent->prev : &parent->item),
			next(wentLeft ? &parent->item : parent->next),
			leftChild(nullptr), rightChild(nullptr),
			raw(raw), item(raw->delta().apply(raw->delta().getPrefixSource() ? *prev : *next, arena))
		{
			//printf("DecodedNode2 raw=%p delta=%s\n", raw, raw->delta().toString().c_str());
		}

		// Returns true if otherAncestor is the previous ("greatest lesser") ancestor
		bool otherAncestorPrev() const {
			return parent && parent->leftChild == this;
		}

		// Returns true if otherAncestor is the next ("least greator") ancestor
		bool otherAncestorNext() const {
			return parent && parent->rightChild == this;
		}

		DecodedNode * getPrevAncestor() const {
			return otherAncestorPrev() ? otherAncestor : parent;
		}

		DecodedNode * getNextAncestor() const {
			return otherAncestorNext() ? otherAncestor : parent;
		}

		DecodedNode * jumpNext(DecodedNode *root) const {
			if(otherAncestorNext()) {
				return (otherAncestor != nullptr) ? otherAncestor : rightChild;
			}
			else {
				if(this == root) {
					return rightChild;
				}
				return (otherAncestor != nullptr) ? otherAncestor->rightChild : root;
			}
		}

		DecodedNode * jumpPrev(DecodedNode *root) const {
			if(otherAncestorPrev()) {
				return (otherAncestor != nullptr) ? otherAncestor : leftChild;
			}
			else {
				if(this == root) {
					return leftChild;
				}
				return (otherAncestor != nullptr) ? otherAncestor->leftChild : root;
			}
		}

		void setDeleted(bool deleted) {
			raw->delta().setDeleted(deleted);
		}

		bool isDeleted() const {
			return raw->delta().getDeleted();
		}

		Node *raw;
		DecodedNode *parent;
		DecodedNode *otherAncestor;
		DecodedNode *leftChild;
		DecodedNode *rightChild;
		const T *prev;  // greatest ancestor to the left, or tree lower bound
		const T *next;  // least ancestor to the right, or tree upper bound
		T item;

		DecodedNode *getRightChild(Arena &arena) {
			if(rightChild == nullptr) {
				Node *n = raw->rightChild();
				if(n != nullptr) {
					rightChild = new (arena) DecodedNode(n, this, false, arena);
				}
			}
			return rightChild;
		}

		DecodedNode *getLeftChild(Arena &arena) {
			if(leftChild == nullptr) {
				Node *n = raw->leftChild();
				if(n != nullptr) {
					leftChild = new (arena) DecodedNode(n, this, true, arena);
				}
			}
			return leftChild;
		}
	};

	struct Cursor;

	// A Mirror is an accessor for a DeltaTree which allows insertion and reading.  Both operations are done
	// using cursors which point to and share nodes in an tree that is built on-demand and mirrors the compressed
	// structure but with fully reconstituted items (which reference DeltaTree bytes or Arena bytes, based
	// on the behavior of T::Delta::apply())
	struct Mirror : FastAllocated<Mirror> {
		friend class Cursor;

		Mirror(const void *treePtr = nullptr, const T *lowerBound = nullptr, const T *upperBound = nullptr)
			: tree((DeltaTree *)treePtr), lower(lowerBound), upper(upperBound)
		{
			// TODO: Remove these copies into arena and require users of Mirror to keep prev and next alive during its lifetime
			lower = new(arena) T(arena, *lower);
			upper = new(arena) T(arena, *upper);

			root = (tree->nodeBytesUsed == 0) ? nullptr : new (arena) DecodedNode(&tree->root(), lower, upper, arena);
		}

		const T *lowerBound() const {
			return lower;
		}

		const T *upperBound() const {
			return upper;
		}

private:
		Arena arena;
		DeltaTree *tree;
		DecodedNode *root;
		const T *lower;
		const T *upper;
public:

		Cursor getCursor() {
			return Cursor(this);
		}

		// Try to insert k into the DeltaTree, updating byte counts and initialHeight if they
		// have changed (they won't if k already exists in the tree but was deleted).
		// Returns true if successful, false if k does not fit in the space available
		// or if k is already in the tree (and was not already deleted).
		bool insert(const T &k, int skipLen = 0, int maxHeightAllowed = std::numeric_limits<int>::max()) {
			int height = 1;
			DecodedNode *n = root;
			bool addLeftChild = false;

			while(n != nullptr) {
				int cmp = k.compare(n->item, skipLen);

				if(cmp >= 0) {
					// If we found an item identical to k then if it is deleted, undeleted it,
					// otherwise fail
					if(cmp == 0) {
						auto &d = n->raw->delta();
						if(d.getDeleted()) {
							d.setDeleted(false);
							++tree->numItems;
							return true;
						}
						else {
							return false;
						}
					}

					DecodedNode *right = n->getRightChild(arena);

					if(right == nullptr) {
						break;
					}

					n = right;
				}
				else {
					DecodedNode *left = n->getLeftChild(arena);

					if(left == nullptr) {
						addLeftChild = true;
						break;
					}

					n = left;
				}
				++height;
			}

			if(height > maxHeightAllowed) {
				return false;
			}

			// Insert k as the left or right child of n, depending on the value of addLeftChild
			// First, see if it will fit.
			const T *prev = addLeftChild ? n->prev : &n->item;
			const T *next = addLeftChild ? &n->item : n->next;

			int common = prev->getCommonPrefixLen(*next, skipLen);
			int commonWithPrev = k.getCommonPrefixLen(*prev, common);
			int commonWithNext = k.getCommonPrefixLen(*next, common);
			bool basePrev = commonWithPrev >= commonWithNext;

			int commonPrefix = basePrev ? commonWithPrev : commonWithNext;
			const T *base = basePrev ? prev : next;

			int deltaSize = k.deltaSize(*base, false, commonPrefix);
			int nodeSpace = deltaSize + sizeof(Node);
			if(nodeSpace > tree->nodeBytesFree) {
				return false;
			}

			DecodedNode *newNode = new (arena) DecodedNode();
			Node *raw = &tree->newNode();
			raw->leftChildOffset = 0;
			raw->rightChildOffset = 0;
			int newOffset = (uint8_t *)raw - (uint8_t *)n->raw;
			//printf("Inserting %s at offset %d\n", k.toString().c_str(), newOffset);

			if(addLeftChild) {
				n->leftChild = newNode;
				n->raw->leftChildOffset = newOffset;
			}
			else {
				n->rightChild = newNode;
				n->raw->rightChildOffset = newOffset;
			}

			newNode->parent = n;
			newNode->leftChild = nullptr;
			newNode->rightChild = nullptr;
			newNode->raw = raw;
			newNode->otherAncestor = addLeftChild ? n->getPrevAncestor() : n->getNextAncestor();
			newNode->prev = prev;
			newNode->next = next;

			ASSERT(deltaSize == k.writeDelta(raw->delta(), *base, commonPrefix));
			raw->delta().setPrefixSource(basePrev);

			// Initialize node's item from the delta (instead of copying into arena) to avoid unnecessary arena space usage
			newNode->item = raw->delta().apply(*base, arena);

			tree->nodeBytesUsed += nodeSpace;
			tree->nodeBytesFree -= nodeSpace;
			++tree->numItems;

			// Update max height of the tree if necessary
			if(height > tree->maxHeight) {
				tree->maxHeight = height;
			}

			return true;
		}

		// Erase k by setting its deleted flag to true.  Returns true only if k existed
		bool erase(const T &k, int skipLen = 0) {
			Cursor c = getCursor();
			bool r = c.seek(k);
			if(r) {
				c.erase();
			}
			return r;
		}
	};

	// Cursor provides a way to seek into a DeltaTree and iterate over its contents
	// All Cursors from a Mirror share the same decoded node 'cache' (tree of DecodedNodes)
	struct Cursor {
		Cursor() : mirror(nullptr), node(nullptr) {
		}

		Cursor(Mirror *r) : mirror(r), node(mirror->root) {
		}

		Mirror *mirror;
		DecodedNode *node;

		bool valid() const {
			return node != nullptr;
		}

		const T & get() const {
			return node->item;
		}

		const T & getOrUpperBound() const {
			return valid() ? node->item : *mirror->upperBound();
		}

		bool operator==(const Cursor &rhs) const {
			return node == rhs.node;
		}

		bool operator!=(const Cursor &rhs) const {
			return node != rhs.node;
		}

		void erase() {
			node->setDeleted(true);
			--mirror->tree->numItems;
			moveNext();
		}

		bool seekLessThanOrEqual(const T &s, int skipLen = 0) {
			return seekLessThanOrEqual(s, skipLen, nullptr, 0);
		}

		bool seekLessThanOrEqual(const T &s, int skipLen, const Cursor *pHint) {
			if(pHint->valid()) {
				return seekLessThanOrEqual(s, skipLen, pHint, s.compare(pHint->get(), skipLen));
			}
			return seekLessThanOrEqual(s, skipLen, nullptr, 0);
		}

		// Moves the cursor to the node with the greatest key less than or equal to s.  If successful,
		// returns true, otherwise returns false and the cursor position will be invalid.
		// If pHint is given then initialCmp must be logically equivalent to s.compare(pHint->get())
		// If hintFwd is omitted, it will be calculated (see other definitions above)
		bool seekLessThanOrEqual(const T &s, int skipLen, const Cursor *pHint, int initialCmp) {
			DecodedNode *n;

			// If there's a hint position, use it
			// At the end of using the hint, if n is valid it should point to a node which has not yet been compared to.
			if(pHint != nullptr && pHint->node != nullptr) {
				n = pHint->node;
				if(initialCmp == 0) {
					node = n;
					return _hideDeletedBackward();
				}
				if(initialCmp > 0) {
					node = n;
					while(n != nullptr) {
						n = n->jumpNext(mirror->root);
						if(n == nullptr) {
							break;
						}

						int cmp = s.compare(n->item, skipLen);
						if(cmp > 0) {
							node = n;
							continue;
						}
						if(cmp == 0) {
							node = n;
							n = nullptr;
						}
						else {
							n = n->leftChild;
						}
						break;
					}
				}
				else {
					while(n != nullptr) {
						n = n->jumpPrev(mirror->root);
						if(n == nullptr) {
							break;
						}
						int cmp = s.compare(n->item, skipLen);
						if(cmp >= 0) {
							node = n;
							n = (cmp == 0) ? nullptr : n->rightChild;
							break;
						}
					}
				}
			}
			else {
				// Start at root, clear current position
				n = mirror->root;
				node = nullptr;
			}

			while(n != nullptr) {
				int cmp = s.compare(n->item, skipLen);

				if(cmp < 0) {
					n = n->getLeftChild(mirror->arena);
				}
				else {
					// n <= s so store it in node as a potential result
					node = n;

					if(cmp == 0) {
						break;
					}

					n = n->getRightChild(mirror->arena);
				}
			}

			return _hideDeletedBackward();
		}

		// Moves the cursor to the node with the lowest key greater than or equal to s.  If successful,
		// returns true, otherwise returns false and the cursor position will be invalid.
		bool seekGreaterThanOrEqual(const T &s, int skipLen = 0) {
			DecodedNode *n = mirror->root;
			node = nullptr;

			while(n != nullptr) {
				int cmp = s.compare(n->item, skipLen);

				if(cmp > 0) {
					n = n->getRightChild(mirror->arena);
				}
				else {
					// n >= s so store it in node as a potential result
					node = n;

					if(cmp == 0) {
						break;
					}

					n = n->getLeftChild(mirror->arena);
				}
			}

			return _hideDeletedForward();
		}

		// Moves the cursor to the node with exactly item s
		// If successful, returns true, otherwise returns false and the cursor position will be invalid.
		bool seek(const T &s, int skipLen = 0) {
			DecodedNode *n = mirror->root;
			node = nullptr;

			while(n != nullptr) {
				int cmp = s.compare(n->item, skipLen);

				if(cmp == 0) {
					if(n->isDeleted()) {
						return false;
					}
					node = n;
					return true;
				}

				n = (cmp > 0) ? n->getRightChild(mirror->arena) : n->getLeftChild(mirror->arena);
			}

			return false;
		}

		bool moveFirst() {
			DecodedNode *n = mirror->root;
			node = n;
			while(n != nullptr) {
				n = n->getLeftChild(mirror->arena);
				if(n != nullptr)
					node = n;
			}
			return _hideDeletedForward();
		}

		bool moveLast() {
			DecodedNode *n = mirror->root;
			node = n;
			while(n != nullptr) {
				n = n->getRightChild(mirror->arena);
				if(n != nullptr)
					node = n;
			}
			return _hideDeletedBackward();
		}

		// Try to move to next node, sees deleted nodes.
		void _moveNext() {
			// Try to go right
			DecodedNode *n = node->getRightChild(mirror->arena);

			// If we couldn't go right, then the answer is our next ancestor
			if(n == nullptr) {
				node = node->getNextAncestor();
			}
			else {
				// Go left as far as possible
				while(n != nullptr) {
					node = n;
					n = n->getLeftChild(mirror->arena);
				}
			}
		}

		// Try to move to previous node, sees deleted nodes.
		void _movePrev() {
			// Try to go left
			DecodedNode *n = node->getLeftChild(mirror->arena);

			// If we couldn't go left, then the answer is our prev ancestor
			if(n == nullptr) {
				node = node->getPrevAncestor();
			}
			else {
				// Go right as far as possible
				while(n != nullptr) {
					node = n;
					n = n->getRightChild(mirror->arena);
				}
			}
		}

		bool moveNext() {
			_moveNext();
			return _hideDeletedForward();
		}

		bool movePrev() {
			_movePrev();
			return _hideDeletedBackward();
		}

	private:
		bool _hideDeletedBackward() {
			while(node != nullptr && node->isDeleted()) {
				_movePrev();
			}
			return node != nullptr;
		}

		bool _hideDeletedForward() {
			while(node != nullptr && node->isDeleted()) {
				_moveNext();
			}
			return node != nullptr;
		}
	};

	// Returns number of bytes written
	int build(int spaceAvailable, const T *begin, const T *end, const T *prev, const T *next) {
		//printf("tree size: %d   node size: %d\n", sizeof(DeltaTree), sizeof(Node));
		int count = end - begin;
		numItems = count;
		nodeBytesDeleted = 0;
		initialHeight = (uint8_t)log2(count) + 1;
		maxHeight = 0;

		// The boundary leading to the new page acts as the last time we branched right
		if(begin != end) {
			nodeBytesUsed = build(root(), begin, end, prev, next, prev->getCommonPrefixLen(*next, 0));
		}
		else {
			nodeBytesUsed = 0;
		}
		nodeBytesFree = spaceAvailable - size();
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
			root.leftChildOffset = sizeof(Node) + deltaSize;
		}
		else {
			root.leftChildOffset = 0;
		}

		// Serialize right child
		if(count > 2) {
			root.rightChildOffset = wptr - (uint8_t *)&root;
			wptr += build(*(Node *)wptr, begin + mid + 1, end, &item, next, commonWithNext);
		}
		else {
			root.rightChildOffset = 0;
		}

		return wptr - (uint8_t *)&root;
	}
};
