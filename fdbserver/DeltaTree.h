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
// Get the number of prefix bytes that are the same between a and b, up to their common length of cl
static inline int commonPrefixLength(uint8_t const* ap, uint8_t const* bp, int cl) {
	int i = 0;
	const int wordEnd = cl - sizeof(Word) + 1;

	for (; i < wordEnd; i += sizeof(Word)) {
		Word a = *(Word*)ap;
		Word b = *(Word*)bp;
		if (a != b) {
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

static inline int commonPrefixLength(const StringRef& a, const StringRef& b) {
	return commonPrefixLength(a.begin(), b.begin(), std::min(a.size(), b.size()));
}

static inline int commonPrefixLength(const StringRef& a, const StringRef& b, int skipLen) {
	return commonPrefixLength(a.begin() + skipLen, b.begin() + skipLen, std::min(a.size(), b.size()) - skipLen);
}

// This appears to be the fastest version
static int lessOrEqualPowerOfTwo(int n) {
	int p;
	for (p = 1; p + p <= n; p += p)
		;
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
	static uint16_t* points = nullptr;
	static const int max = 500;
	if (points == nullptr) {
		points = new uint16_t[max];
		for (int i = 0; i < max; ++i)
			points[i] = perfectSubtreeSplitPoint(i);
	}

	if (subtree_size < max)
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
#pragma pack(push, 1)
template <typename T, typename DeltaT = typename T::Delta>
struct DeltaTree {
	struct Node {
		union {
			struct {
				uint32_t left;
				uint32_t right;
			} largeOffsets;
			struct {
				uint16_t left;
				uint16_t right;
			} smallOffsets;
		};

		static int headerSize(bool large) { return large ? sizeof(largeOffsets) : sizeof(smallOffsets); }

		inline DeltaT& delta(bool large) {
			return large ? *(DeltaT*)(&largeOffsets + 1) : *(DeltaT*)(&smallOffsets + 1);
		};

		inline const DeltaT& delta(bool large) const {
			return large ? *(const DeltaT*)(&largeOffsets + 1) : *(const DeltaT*)(&smallOffsets + 1);
		};

		Node* resolvePointer(int offset) const { return offset == 0 ? nullptr : (Node*)((uint8_t*)this + offset); }

		Node* rightChild(bool large) const { return resolvePointer(large ? largeOffsets.right : smallOffsets.right); }

		Node* leftChild(bool large) const { return resolvePointer(large ? largeOffsets.left : smallOffsets.left); }

		void setRightChildOffset(bool large, int offset) {
			if (large) {
				largeOffsets.right = offset;
			} else {
				smallOffsets.right = offset;
			}
		}

		void setLeftChildOffset(bool large, int offset) {
			if (large) {
				largeOffsets.left = offset;
			} else {
				smallOffsets.left = offset;
			}
		}

		int size(bool large) const {
			return delta(large).size() + (large ? sizeof(smallOffsets) : sizeof(largeOffsets));
		}
	};

	static constexpr int SmallSizeLimit = std::numeric_limits<uint16_t>::max();
	static constexpr int LargeTreePerNodeExtraOverhead = sizeof(Node::largeOffsets) - sizeof(Node::smallOffsets);

	struct {
		uint16_t numItems; // Number of items in the tree.
		uint32_t nodeBytesUsed; // Bytes used by nodes (everything after the tree header)
		uint32_t nodeBytesFree; // Bytes left at end of tree to expand into
		uint32_t nodeBytesDeleted; // Delta bytes deleted from tree.  Note that some of these bytes could be borrowed by
		                           // descendents.
		uint8_t initialHeight; // Height of tree as originally built
		uint8_t maxHeight; // Maximum height of tree after any insertion.  Value of 0 means no insertions done.
		bool largeNodes; // Node size, can be calculated as capacity > SmallSizeLimit but it will be used a lot
	};
#pragma pack(pop)

	inline Node& root() { return *(Node*)(this + 1); }

	inline const Node& root() const { return *(const Node*)(this + 1); }

	int size() const { return sizeof(DeltaTree) + nodeBytesUsed; }

	int capacity() const { return size() + nodeBytesFree; }

	inline Node& newNode() { return *(Node*)((uint8_t*)this + size()); }

public:
	struct DecodedNode {
		DecodedNode() {}

		// construct root node
		DecodedNode(Node* raw, const T* prev, const T* next, Arena& arena, bool large)
		  : raw(raw), parent(nullptr), otherAncestor(nullptr), leftChild(nullptr), rightChild(nullptr), prev(prev),
		    next(next), item(raw->delta(large).apply(raw->delta(large).getPrefixSource() ? *prev : *next, arena)),
		    large(large) {
			// printf("DecodedNode1 raw=%p delta=%s\n", raw, raw->delta(large).toString().c_str());
		}

		// Construct non-root node
		// wentLeft indicates that we've gone left to get to the raw node.
		DecodedNode(Node* raw, DecodedNode* parent, bool wentLeft, Arena& arena)
		  : parent(parent), large(parent->large),
		    otherAncestor(wentLeft ? parent->getPrevAncestor() : parent->getNextAncestor()),
		    prev(wentLeft ? parent->prev : &parent->item), next(wentLeft ? &parent->item : parent->next),
		    leftChild(nullptr), rightChild(nullptr), raw(raw),
		    item(raw->delta(large).apply(raw->delta(large).getPrefixSource() ? *prev : *next, arena)) {
			// printf("DecodedNode2 raw=%p delta=%s\n", raw, raw->delta(large).toString().c_str());
		}

		// Returns true if otherAncestor is the previous ("greatest lesser") ancestor
		bool otherAncestorPrev() const { return parent && parent->leftChild == this; }

		// Returns true if otherAncestor is the next ("least greator") ancestor
		bool otherAncestorNext() const { return parent && parent->rightChild == this; }

		DecodedNode* getPrevAncestor() const { return otherAncestorPrev() ? otherAncestor : parent; }

		DecodedNode* getNextAncestor() const { return otherAncestorNext() ? otherAncestor : parent; }

		DecodedNode* jumpUpNext(DecodedNode* root, bool& othersChild) const {
			if (parent != nullptr) {
				if (parent->rightChild == this) {
					return otherAncestor;
				}
				if (otherAncestor != nullptr) {
					othersChild = true;
					return otherAncestor->rightChild;
				}
			}
			return parent;
		}

		DecodedNode* jumpUpPrev(DecodedNode* root, bool& othersChild) const {
			if (parent != nullptr) {
				if (parent->leftChild == this) {
					return otherAncestor;
				}
				if (otherAncestor != nullptr) {
					othersChild = true;
					return otherAncestor->leftChild;
				}
			}
			return parent;
		}

		DecodedNode* jumpNext(DecodedNode* root) const {
			if (otherAncestorNext()) {
				return (otherAncestor != nullptr) ? otherAncestor : rightChild;
			} else {
				if (this == root) {
					return rightChild;
				}
				return (otherAncestor != nullptr) ? otherAncestor->rightChild : root;
			}
		}

		DecodedNode* jumpPrev(DecodedNode* root) const {
			if (otherAncestorPrev()) {
				return (otherAncestor != nullptr) ? otherAncestor : leftChild;
			} else {
				if (this == root) {
					return leftChild;
				}
				return (otherAncestor != nullptr) ? otherAncestor->leftChild : root;
			}
		}

		void setDeleted(bool deleted) { raw->delta(large).setDeleted(deleted); }

		bool isDeleted() const { return raw->delta(large).getDeleted(); }

		bool large; // Node size
		Node* raw;
		DecodedNode* parent;
		DecodedNode* otherAncestor;
		DecodedNode* leftChild;
		DecodedNode* rightChild;
		const T* prev; // greatest ancestor to the left, or tree lower bound
		const T* next; // least ancestor to the right, or tree upper bound
		T item;

		DecodedNode* getRightChild(Arena& arena) {
			if (rightChild == nullptr) {
				Node* n = raw->rightChild(large);
				if (n != nullptr) {
					rightChild = new (arena) DecodedNode(n, this, false, arena);
				}
			}
			return rightChild;
		}

		DecodedNode* getLeftChild(Arena& arena) {
			if (leftChild == nullptr) {
				Node* n = raw->leftChild(large);
				if (n != nullptr) {
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

		Mirror(const void* treePtr = nullptr, const T* lowerBound = nullptr, const T* upperBound = nullptr)
		  : tree((DeltaTree*)treePtr), lower(lowerBound), upper(upperBound) {
			// TODO: Remove these copies into arena and require users of Mirror to keep prev and next alive during its
			// lifetime
			lower = new (arena) T(arena, *lower);
			upper = new (arena) T(arena, *upper);

			root = (tree->nodeBytesUsed == 0) ? nullptr
			                                  : new (arena)
			                                        DecodedNode(&tree->root(), lower, upper, arena, tree->largeNodes);
		}

		const T* lowerBound() const { return lower; }

		const T* upperBound() const { return upper; }

		DeltaTree* tree;
		Arena arena;

	private:
		DecodedNode* root;
		const T* lower;
		const T* upper;

	public:
		Cursor getCursor() { return Cursor(this); }

		// Try to insert k into the DeltaTree, updating byte counts and initialHeight if they
		// have changed (they won't if k already exists in the tree but was deleted).
		// Returns true if successful, false if k does not fit in the space available
		// or if k is already in the tree (and was not already deleted).
		// Insertion on an empty tree returns false as well.
		bool insert(const T& k, int skipLen = 0, int maxHeightAllowed = std::numeric_limits<int>::max()) {
			if (root == nullptr) {
				return false;
			}
			int height = 1;
			DecodedNode* n = root;
			bool addLeftChild = false;

			while (true) {
				int cmp = k.compare(n->item, skipLen);

				if (cmp >= 0) {
					// If we found an item identical to k then if it is deleted, undeleted it,
					// otherwise fail
					if (cmp == 0) {
						auto& d = n->raw->delta(tree->largeNodes);
						if (d.getDeleted()) {
							d.setDeleted(false);
							++tree->numItems;
							return true;
						} else {
							return false;
						}
					}

					DecodedNode* right = n->getRightChild(arena);

					if (right == nullptr) {
						break;
					}

					n = right;
				} else {
					DecodedNode* left = n->getLeftChild(arena);

					if (left == nullptr) {
						addLeftChild = true;
						break;
					}

					n = left;
				}
				++height;
			}

			if (height > maxHeightAllowed) {
				return false;
			}

			// Insert k as the left or right child of n, depending on the value of addLeftChild
			// First, see if it will fit.
			const T* prev = addLeftChild ? n->prev : &n->item;
			const T* next = addLeftChild ? &n->item : n->next;

			int common = prev->getCommonPrefixLen(*next, skipLen);
			int commonWithPrev = k.getCommonPrefixLen(*prev, common);
			int commonWithNext = k.getCommonPrefixLen(*next, common);
			bool basePrev = commonWithPrev >= commonWithNext;

			int commonPrefix = basePrev ? commonWithPrev : commonWithNext;
			const T* base = basePrev ? prev : next;

			int deltaSize = k.deltaSize(*base, commonPrefix, false);
			int nodeSpace = deltaSize + Node::headerSize(tree->largeNodes);
			if (nodeSpace > tree->nodeBytesFree) {
				return false;
			}

			DecodedNode* newNode = new (arena) DecodedNode();
			Node* raw = &tree->newNode();
			raw->setLeftChildOffset(tree->largeNodes, 0);
			raw->setRightChildOffset(tree->largeNodes, 0);
			int newOffset = (uint8_t*)raw - (uint8_t*)n->raw;
			// printf("Inserting %s at offset %d\n", k.toString().c_str(), newOffset);

			if (addLeftChild) {
				n->leftChild = newNode;
				n->raw->setLeftChildOffset(tree->largeNodes, newOffset);
			} else {
				n->rightChild = newNode;
				n->raw->setRightChildOffset(tree->largeNodes, newOffset);
			}

			newNode->parent = n;
			newNode->large = tree->largeNodes;
			newNode->leftChild = nullptr;
			newNode->rightChild = nullptr;
			newNode->raw = raw;
			newNode->otherAncestor = addLeftChild ? n->getPrevAncestor() : n->getNextAncestor();
			newNode->prev = prev;
			newNode->next = next;

			int written = k.writeDelta(raw->delta(tree->largeNodes), *base, commonPrefix);
			ASSERT(deltaSize == written);
			raw->delta(tree->largeNodes).setPrefixSource(basePrev);

			// Initialize node's item from the delta (instead of copying into arena) to avoid unnecessary arena space
			// usage
			newNode->item = raw->delta(tree->largeNodes).apply(*base, arena);

			tree->nodeBytesUsed += nodeSpace;
			tree->nodeBytesFree -= nodeSpace;
			++tree->numItems;

			// Update max height of the tree if necessary
			if (height > tree->maxHeight) {
				tree->maxHeight = height;
			}

			return true;
		}

		// Erase k by setting its deleted flag to true.  Returns true only if k existed
		bool erase(const T& k, int skipLen = 0) {
			Cursor c = getCursor();
			int cmp = c.seek(k);
			// If exactly k is found
			if (cmp == 0 && !c.node->isDeleted()) {
				c.erase();
				return true;
			}
			return false;
		}
	};

	// Cursor provides a way to seek into a DeltaTree and iterate over its contents
	// All Cursors from a Mirror share the same decoded node 'cache' (tree of DecodedNodes)
	struct Cursor {
		Cursor() : mirror(nullptr), node(nullptr) {}

		Cursor(Mirror* r) : mirror(r), node(mirror->root) {}

		Mirror* mirror;
		DecodedNode* node;

		bool valid() const { return node != nullptr; }

		const T& get() const { return node->item; }

		const T& getOrUpperBound() const { return valid() ? node->item : *mirror->upperBound(); }

		const T& lowerBound() const { return *mirror->lowerBound(); }

		const T& upperBound() const { return *mirror->upperBound(); }

		bool operator==(const Cursor& rhs) const { return node == rhs.node; }

		bool operator!=(const Cursor& rhs) const { return node != rhs.node; }

		void erase() {
			node->setDeleted(true);
			--mirror->tree->numItems;
			moveNext();
		}

		// TODO:  Make hint-based seek() use the hint logic in this, which is better and actually improves seek times,
		// then remove this function.
		bool seekLessThanOrEqualOld(const T& s, int skipLen, const Cursor* pHint, int initialCmp) {
			DecodedNode* n;

			// If there's a hint position, use it
			// At the end of using the hint, if n is valid it should point to a node which has not yet been compared to.
			if (pHint != nullptr && pHint->node != nullptr) {
				n = pHint->node;
				if (initialCmp == 0) {
					node = n;
					return _hideDeletedBackward();
				}
				if (initialCmp > 0) {
					node = n;
					while (n != nullptr) {
						n = n->jumpNext(mirror->root);
						if (n == nullptr) {
							break;
						}
						int cmp = s.compare(n->item, skipLen);
						if (cmp > 0) {
							node = n;
							continue;
						}
						if (cmp == 0) {
							node = n;
							n = nullptr;
						} else {
							n = n->leftChild;
						}
						break;
					}
				} else {
					while (n != nullptr) {
						n = n->jumpPrev(mirror->root);
						if (n == nullptr) {
							break;
						}
						int cmp = s.compare(n->item, skipLen);
						if (cmp >= 0) {
							node = n;
							n = (cmp == 0) ? nullptr : n->rightChild;
							break;
						}
					}
				}
			} else {
				// Start at root, clear current position
				n = mirror->root;
				node = nullptr;
			}

			while (n != nullptr) {
				int cmp = s.compare(n->item, skipLen);

				if (cmp < 0) {
					n = n->getLeftChild(mirror->arena);
				} else {
					// n <= s so store it in node as a potential result
					node = n;

					if (cmp == 0) {
						break;
					}

					n = n->getRightChild(mirror->arena);
				}
			}

			return _hideDeletedBackward();
		}

		// The seek methods, of the form seek[Less|Greater][orEqual](...) are very similar.
		// They attempt move the cursor to the [Greatest|Least] item, based on the name of the function.
		// Then will not "see" erased records.
		// If successful, they return true, and if not then false a while making the cursor invalid.
		// These methods forward arguments to the seek() overloads, see those for argument descriptions.
		template <typename... Args>
		bool seekLessThan(Args... args) {
			int cmp = seek(args...);
			if (cmp < 0 || (cmp == 0 && node != nullptr)) {
				movePrev();
			}
			return _hideDeletedBackward();
		}

		template <typename... Args>
		bool seekLessThanOrEqual(Args... args) {
			int cmp = seek(args...);
			if (cmp < 0) {
				movePrev();
			}
			return _hideDeletedBackward();
		}

		template <typename... Args>
		bool seekGreaterThan(Args... args) {
			int cmp = seek(args...);
			if (cmp > 0 || (cmp == 0 && node != nullptr)) {
				moveNext();
			}
			return _hideDeletedForward();
		}

		template <typename... Args>
		bool seekGreaterThanOrEqual(Args... args) {
			int cmp = seek(args...);
			if (cmp > 0) {
				moveNext();
			}
			return _hideDeletedForward();
		}

		// seek() moves the cursor to a node containing s or the node that would be the parent of s if s were to be
		// added to the tree. If the tree was empty, the cursor will be invalid and the return value will be 0.
		// Otherwise, returns the result of s.compare(item at cursor position)
		// Does not skip/avoid deleted nodes.
		int seek(const T& s, int skipLen = 0) {
			DecodedNode* n = mirror->root;
			node = nullptr;
			int cmp = 0;
			while (n != nullptr) {
				node = n;
				cmp = s.compare(n->item, skipLen);
				if (cmp == 0) {
					break;
				}

				n = (cmp > 0) ? n->getRightChild(mirror->arena) : n->getLeftChild(mirror->arena);
			}

			return cmp;
		}

		// Same usage as seek() but with a hint of a cursor, which can't be null, whose starting position
		// should be close to s in the tree to improve seek time.
		// initialCmp should be logically equivalent to s.compare(pHint->get()) or 0, in which
		// case the comparison will be done in this method.
		// TODO:  This is broken, it's not faster than not using a hint.  See Make thisUnfortunately in a microbenchmark
		// attempting to approximate a common use case, this version of using a cursor hint is actually slower than not
		// using a hint.
		int seek(const T& s, int skipLen, const Cursor* pHint, int initialCmp = 0) {
			DecodedNode* n = mirror->root;
			node = nullptr;
			int cmp;

			// If there's a hint position, use it
			// At the end of using the hint, if n is valid it should point to a node which has not yet been compared to.
			if (pHint->node != nullptr) {
				n = pHint->node;
				if (initialCmp == 0) {
					initialCmp = s.compare(pHint->get());
				}
				cmp = initialCmp;

				while (true) {
					node = n;
					if (cmp == 0) {
						return cmp;
					}

					// Attempt to jump up and past s
					bool othersChild = false;
					n = (initialCmp > 0) ? n->jumpUpNext(mirror->root, othersChild)
					                     : n->jumpUpPrev(mirror->root, othersChild);
					if (n == nullptr) {
						n = (cmp > 0) ? node->rightChild : node->leftChild;
						break;
					}

					// Compare s to the node jumped to
					cmp = s.compare(n->item, skipLen);

					// n is on the oposite side of s than node is, then n is too far.
					if (cmp != 0 && ((initialCmp ^ cmp) < 0)) {
						if (!othersChild) {
							n = (cmp < 0) ? node->rightChild : node->leftChild;
						}
						break;
					}
				}
			} else {
				// Start at root, clear current position
				n = mirror->root;
				node = nullptr;
				cmp = 0;
			}

			// Search starting from n, which is either the root or the result of applying the hint
			while (n != nullptr) {
				node = n;
				cmp = s.compare(n->item, skipLen);
				if (cmp == 0) {
					break;
				}

				n = (cmp > 0) ? n->getRightChild(mirror->arena) : n->getLeftChild(mirror->arena);
			}

			return cmp;
		}

		bool moveFirst() {
			DecodedNode* n = mirror->root;
			node = n;
			while (n != nullptr) {
				n = n->getLeftChild(mirror->arena);
				if (n != nullptr)
					node = n;
			}
			return _hideDeletedForward();
		}

		bool moveLast() {
			DecodedNode* n = mirror->root;
			node = n;
			while (n != nullptr) {
				n = n->getRightChild(mirror->arena);
				if (n != nullptr)
					node = n;
			}
			return _hideDeletedBackward();
		}

		// Try to move to next node, sees deleted nodes.
		void _moveNext() {
			// Try to go right
			DecodedNode* n = node->getRightChild(mirror->arena);

			// If we couldn't go right, then the answer is our next ancestor
			if (n == nullptr) {
				node = node->getNextAncestor();
			} else {
				// Go left as far as possible
				while (n != nullptr) {
					node = n;
					n = n->getLeftChild(mirror->arena);
				}
			}
		}

		// Try to move to previous node, sees deleted nodes.
		void _movePrev() {
			// Try to go left
			DecodedNode* n = node->getLeftChild(mirror->arena);

			// If we couldn't go left, then the answer is our prev ancestor
			if (n == nullptr) {
				node = node->getPrevAncestor();
			} else {
				// Go right as far as possible
				while (n != nullptr) {
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
			while (node != nullptr && node->isDeleted()) {
				_movePrev();
			}
			return node != nullptr;
		}

		bool _hideDeletedForward() {
			while (node != nullptr && node->isDeleted()) {
				_moveNext();
			}
			return node != nullptr;
		}
	};

	// Returns number of bytes written
	int build(int spaceAvailable, const T* begin, const T* end, const T* prev, const T* next) {
		largeNodes = spaceAvailable > SmallSizeLimit;
		int count = end - begin;
		numItems = count;
		nodeBytesDeleted = 0;
		initialHeight = count ? (uint8_t)log2(count) + 1 : 0;
		maxHeight = 0;

		// The boundary leading to the new page acts as the last time we branched right
		if (begin != end) {
			nodeBytesUsed = buildSubtree(root(), begin, end, prev, next, prev->getCommonPrefixLen(*next, 0));
		} else {
			nodeBytesUsed = 0;
		}
		nodeBytesFree = spaceAvailable - size();
		return size();
	}

private:
	int buildSubtree(Node& node, const T* begin, const T* end, const T* prev, const T* next, int subtreeCommon) {
		// printf("build: %s to %s\n", begin->toString().c_str(), (end - 1)->toString().c_str());
		// printf("build: root at %p  Node::headerSize %d  delta at %p  \n", &root, Node::headerSize(largeNodes),
		// &node.delta(largeNodes));
		ASSERT(end != begin);
		int count = end - begin;

		// Find key to be stored in root
		int mid = perfectSubtreeSplitPointCached(count);
		const T& item = begin[mid];

		int commonWithPrev = item.getCommonPrefixLen(*prev, subtreeCommon);
		int commonWithNext = item.getCommonPrefixLen(*next, subtreeCommon);

		bool prefixSourcePrev;
		int commonPrefix;
		const T* base;
		if (commonWithPrev >= commonWithNext) {
			prefixSourcePrev = true;
			commonPrefix = commonWithPrev;
			base = prev;
		} else {
			prefixSourcePrev = false;
			commonPrefix = commonWithNext;
			base = next;
		}

		int deltaSize = item.writeDelta(node.delta(largeNodes), *base, commonPrefix);
		node.delta(largeNodes).setPrefixSource(prefixSourcePrev);
		// printf("Serialized %s to %p\n", item.toString().c_str(), &root.delta(largeNodes));

		// Continue writing after the serialized Delta.
		uint8_t* wptr = (uint8_t*)&node.delta(largeNodes) + deltaSize;

		// Serialize left child
		if (count > 1) {
			wptr += buildSubtree(*(Node*)wptr, begin, begin + mid, prev, &item, commonWithPrev);
			node.setLeftChildOffset(largeNodes, Node::headerSize(largeNodes) + deltaSize);
		} else {
			node.setLeftChildOffset(largeNodes, 0);
		}

		// Serialize right child
		if (count > 2) {
			node.setRightChildOffset(largeNodes, wptr - (uint8_t*)&node);
			wptr += buildSubtree(*(Node*)wptr, begin + mid + 1, end, &item, next, commonWithNext);
		} else {
			node.setRightChildOffset(largeNodes, 0);
		}

		return wptr - (uint8_t*)&node;
	}
};
