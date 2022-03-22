/*
 * DeltaTree.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2022 Apple Inc. and the FoundationDB project authors
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

#define DELTATREE_DEBUG 0

#if DELTATREE_DEBUG
#define deltatree_printf(...) printf(__VA_ARGS__)
#else
#define deltatree_printf(...)
#endif

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

static inline int perfectSubtreeSplitPoint(int subtree_size) {
	// return the inorder index of the root node in a subtree of the given size
	// consistent with the resulting binary search tree being "perfect" (having minimal height
	// and all missing nodes as far right as possible).
	// There has to be a simpler way to do this.
	int s = lessOrEqualPowerOfTwo((subtree_size - 1) / 2 + 1) - 1;
	return std::min(s * 2 + 1, subtree_size - s - 1);
}

static inline int perfectSubtreeSplitPointCached(int subtree_size) {
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
		  : large(large), raw(raw), parent(nullptr), otherAncestor(nullptr), leftChild(nullptr), rightChild(nullptr),
		    prev(prev), next(next),
		    item(raw->delta(large).apply(raw->delta(large).getPrefixSource() ? *prev : *next, arena)) {
			// printf("DecodedNode1 raw=%p delta=%s\n", raw, raw->delta(large).toString().c_str());
		}

		// Construct non-root node
		// wentLeft indicates that we've gone left to get to the raw node.
		DecodedNode(Node* raw, DecodedNode* parent, bool wentLeft, Arena& arena)
		  : large(parent->large), raw(raw), parent(parent),
		    otherAncestor(wentLeft ? parent->getPrevAncestor() : parent->getNextAncestor()), leftChild(nullptr),
		    rightChild(nullptr), prev(wentLeft ? parent->prev : &parent->item),
		    next(wentLeft ? &parent->item : parent->next),
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

	class Cursor;

	// A Mirror is an accessor for a DeltaTree which allows insertion and reading.  Both operations are done
	// using cursors which point to and share nodes in an tree that is built on-demand and mirrors the compressed
	// structure but with fully reconstituted items (which reference DeltaTree bytes or Arena bytes, based
	// on the behavior of T::Delta::apply())
	struct Mirror : FastAllocated<Mirror> {
		friend class Cursor;

		Mirror(const void* treePtr = nullptr, const T* lowerBound = nullptr, const T* upperBound = nullptr)
		  : tree((DeltaTree*)treePtr), lower(lowerBound), upper(upperBound) {
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
	class Cursor {
	public:
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
		deltatree_printf("Serialized %s to offset %d data: %s\n",
		                 item.toString().c_str(),
		                 (uint8_t*)&node - (uint8_t*)this,
		                 StringRef((uint8_t*)&node.delta(largeNodes), deltaSize).toHexString().c_str());

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

// DeltaTree2 is a memory mappable binary tree of T objects such that each node's item is
// stored as a Delta which can reproduce the node's T item given either
//    - The node's greatest lesser ancestor, called the "left parent"
//    - The node's least greater ancestor, called the "right parent"
//  One of these ancestors will also happen to be the node's direct parent.
//
// The Delta type is intended to make use of ordered prefix compression and borrow all
// available prefix bytes from the ancestor T which shares the most prefix bytes with
// the item T being encoded.  If T is implemented properly, this results in perfect
// prefix compression while performing O(log n) comparisons for a seek.
//
// T requirements
//
//    Must be compatible with Standalone<T> and must implement the following additional things:
//
//    // Return the common prefix length between *this and T
//    // skipLen is a hint, representing the length that is already known to be common.
//    int getCommonPrefixLen(const T& other, int skipLen) const;
//
//    // Compare *this to rhs, returns < 0 for less than, 0 for equal, > 0 for greater than
//    // skipLen is a hint, representing the length that is already known to be common.
//    int compare(const T &rhs, int skipLen) const;
//
//    // Writes to d a delta which can create *this from base
//    // commonPrefix is a hint, representing the length that is already known to be common.
//    // DeltaT's size need not be static, for more details see below.
//    void writeDelta(DeltaT &d, const T &base, int commonPrefix) const;
//
//    // Returns the size in bytes of the DeltaT required to recreate *this from base
//    int deltaSize(const T &base) const;
//
//    // A type which represents the parts of T that either borrowed from the base T
//    // or can be borrowed by other T's using the first T as a base
//    // Partials must allocate any heap storage in the provided Arena for any operation.
//    typedef Partial;
//
//    // Update cache with the Partial for *this, storing any heap memory for the Partial in arena
//    void updateCache(Optional<Partial> cache, Arena& arena) const;
//
//    // For debugging, return a useful human-readable string representation of *this
//    std::string toString() const;
//
// DeltaT requirements
//
//    DeltaT can be variable sized, larger than sizeof(DeltaT), and implement the following:
//
//    // Returns the size in bytes of this specific DeltaT instance
//    int size();
//
//    // Apply *this to base and return the resulting T
//    // Store the Partial for T into cache, allocating any heap memory for the Partial in arena
//    T apply(Arena& arena, const T& base, Optional<T::Partial>& cache);
//
//    // Recreate T from *this and the Partial for T
//    T apply(const T::Partial& cache);
//
//    // Set or retrieve a boolean flag representing which base ancestor the DeltaT is to be applied to
//    void setPrefixSource(bool val);
//    bool getPrefixSource() const;
//
//    // Set of retrieve a boolean flag representing that a DeltaTree node has been erased
//    void setDeleted(bool val);
//    bool getDeleted() const;
//
//    // For debugging, return a useful human-readable string representation of *this
//    std::string toString() const;
//
#pragma pack(push, 1)
template <typename T, typename DeltaT = typename T::Delta>
struct DeltaTree2 {
	typedef typename T::Partial Partial;

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

	// Node is not fixed size.  Most node methods require the context of whether the node is in small or large
	// offset mode, passed as a boolean
	struct Node {
		// Offsets are relative to the start of the DeltaTree
		union {
			struct {
				uint32_t leftChild;
				uint32_t rightChild;

			} largeOffsets;
			struct {
				uint16_t leftChild;
				uint16_t rightChild;
			} smallOffsets;
		};

		static int headerSize(bool large) { return large ? sizeof(largeOffsets) : sizeof(smallOffsets); }

		// Delta is located after the offsets, which differs by node size
		DeltaT& delta(bool large) { return large ? *(DeltaT*)(&largeOffsets + 1) : *(DeltaT*)(&smallOffsets + 1); };

		// Delta is located after the offsets, which differs by node size
		const DeltaT& delta(bool large) const {
			return large ? *(DeltaT*)(&largeOffsets + 1) : *(DeltaT*)(&smallOffsets + 1);
		};

		std::string toString(DeltaTree2* tree) const {
			return format("Node{offset=%d leftChild=%d rightChild=%d delta=%s}",
			              tree->nodeOffset(this),
			              getLeftChildOffset(tree->largeNodes),
			              getRightChildOffset(tree->largeNodes),
			              delta(tree->largeNodes).toString().c_str());
		}

#define getMember(m) (large ? largeOffsets.m : smallOffsets.m)
#define setMember(m, v)                                                                                                \
	if (large) {                                                                                                       \
		largeOffsets.m = v;                                                                                            \
	} else {                                                                                                           \
		smallOffsets.m = v;                                                                                            \
	}

		void setRightChildOffset(bool large, int offset) { setMember(rightChild, offset); }
		void setLeftChildOffset(bool large, int offset) { setMember(leftChild, offset); }

		int getRightChildOffset(bool large) const { return getMember(rightChild); }
		int getLeftChildOffset(bool large) const { return getMember(leftChild); }

		int size(bool large) const { return delta(large).size() + headerSize(large); }
#undef getMember
#undef setMember
	};

	static constexpr int SmallSizeLimit = std::numeric_limits<uint16_t>::max();
	static constexpr int LargeTreePerNodeExtraOverhead = sizeof(Node::largeOffsets) - sizeof(Node::smallOffsets);

	int nodeOffset(const Node* n) const { return (uint8_t*)n - (uint8_t*)this; }
	Node* nodeAt(int offset) { return offset == 0 ? nullptr : (Node*)((uint8_t*)this + offset); }
	Node* root() { return numItems == 0 ? nullptr : (Node*)(this + 1); }
	int rootOffset() { return sizeof(DeltaTree2); }

	int size() const { return sizeof(DeltaTree2) + nodeBytesUsed; }
	int capacity() const { return size() + nodeBytesFree; }

public:
	// DecodedNode represents a Node of a DeltaTree and its T::Partial.
	// DecodedNodes are created on-demand, as DeltaTree Nodes are visited by a Cursor.
	// DecodedNodes link together to form a binary tree with the same Node relationships as their
	// corresponding DeltaTree Nodes.  Additionally, DecodedNodes store links to their left and
	// right ancestors which correspond to possible base Nodes on which the Node's Delta is based.
	//
	// DecodedNode links are not pointers, but rather indices to be looked up in the DecodeCache
	// defined below.  An index value of -1 is uninitialized, meaning it is not yet known whether
	// the corresponding DeltaTree Node link is non-null in any version of the DeltaTree which is
	// using or has used the DecodeCache.
	struct DecodedNode {
		DecodedNode(int nodeOffset, int leftParentIndex, int rightParentIndex)
		  : nodeOffset(nodeOffset), leftParentIndex(leftParentIndex), rightParentIndex(rightParentIndex),
		    leftChildIndex(-1), rightChildIndex(-1) {}
		int nodeOffset;
		int16_t leftParentIndex;
		int16_t rightParentIndex;
		int16_t leftChildIndex;
		int16_t rightChildIndex;
		Optional<Partial> partial;

		Node* node(DeltaTree2* tree) const { return tree->nodeAt(nodeOffset); }

		std::string toString() {
			return format("DecodedNode{nodeOffset=%d leftChildIndex=%d rightChildIndex=%d leftParentIndex=%d "
			              "rightParentIndex=%d}",
			              (int)nodeOffset,
			              (int)leftChildIndex,
			              (int)rightChildIndex,
			              (int)leftParentIndex,
			              (int)rightParentIndex);
		}
	};
#pragma pack(pop)

	// The DecodeCache is a reference counted structure that stores DecodedNodes by an integer index
	// and can be shared across a series of updated copies of a DeltaTree.
	//
	// DecodedNodes are stored in a contiguous vector, which sometimes must be expanded, so care
	// must be taken to resolve DecodedNode pointers again after the DecodeCache has new entries added.
	struct DecodeCache : FastAllocated<DecodeCache>, ReferenceCounted<DecodeCache> {
		DecodeCache(const T& lowerBound = T(), const T& upperBound = T(), int64_t* pMemoryTracker = nullptr)
		  : lowerBound(arena, lowerBound), upperBound(arena, upperBound), lastKnownUsedMemory(0),
		    pMemoryTracker(pMemoryTracker) {
			decodedNodes.reserve(10);
			deltatree_printf("DecodedNode size: %d\n", sizeof(DecodedNode));
		}

		~DecodeCache() {
			if (pMemoryTracker != nullptr) {
				// Do not update, only subtract the last known amount which would have been
				// published to the counter
				*pMemoryTracker -= lastKnownUsedMemory;
			}
		}

		Arena arena;
		T lowerBound;
		T upperBound;

		// Track the amount of memory used by the vector and arena and publish updates to some counter.
		// Note that no update is pushed on construction because a Cursor will surely soon follow.
		// Updates are pushed to the counter on
		//    DecodeCache clear
		//    DecodeCache destruction
		//    Cursor destruction
		// as those are the most efficient times to publish an update.
		int lastKnownUsedMemory;
		int64_t* pMemoryTracker;

		// Index 0 is always the root
		std::vector<DecodedNode> decodedNodes;

		DecodedNode& get(int index) { return decodedNodes[index]; }

		void updateUsedMemory() {
			int usedNow = sizeof(DeltaTree2) + arena.getSize(FastInaccurateEstimate::True) +
			              (decodedNodes.capacity() * sizeof(DecodedNode));
			if (pMemoryTracker != nullptr) {
				*pMemoryTracker += (usedNow - lastKnownUsedMemory);
			}
			lastKnownUsedMemory = usedNow;
		}

		template <class... Args>
		int emplace_new(Args&&... args) {
			int index = decodedNodes.size();
			decodedNodes.emplace_back(args...);
			return index;
		}

		bool empty() const { return decodedNodes.empty(); }

		void clear() {
			decodedNodes.clear();
			Arena a;
			lowerBound = T(a, lowerBound);
			upperBound = T(a, upperBound);
			arena = a;
			updateUsedMemory();
		}
	};

	// Cursor provides a way to seek into a DeltaTree and iterate over its contents
	// The cursor needs a DeltaTree pointer and a DecodeCache, which can be shared
	// with other DeltaTrees which were incrementally modified to produce the the
	// tree that this cursor is referencing.
	struct Cursor {
		Cursor() : cache(nullptr), nodeIndex(-1) {}

		Cursor(DecodeCache* cache, DeltaTree2* tree) : tree(tree), cache(cache), nodeIndex(-1) {}

		Cursor(DecodeCache* cache, DeltaTree2* tree, int nodeIndex) : tree(tree), cache(cache), nodeIndex(nodeIndex) {}

		// Copy constructor does not copy item because normally a copied cursor will be immediately moved.
		Cursor(const Cursor& c) : tree(c.tree), cache(c.cache), nodeIndex(c.nodeIndex) {}

		~Cursor() {
			if (cache != nullptr) {
				cache->updateUsedMemory();
			}
		}

		Cursor next() const {
			Cursor c = *this;
			c.moveNext();
			return c;
		}

		Cursor previous() const {
			Cursor c = *this;
			c.movePrev();
			return c;
		}

		int rootIndex() {
			if (!cache->empty()) {
				return 0;
			} else if (tree->numItems != 0) {
				return cache->emplace_new(tree->rootOffset(), -1, -1);
			}
			return -1;
		}

		DeltaTree2* tree;
		DecodeCache* cache;
		int nodeIndex;
		mutable Optional<T> item;

		Node* node() const { return tree->nodeAt(cache->get(nodeIndex).nodeOffset); }

		std::string toString() const {
			if (nodeIndex == -1) {
				return format("Cursor{nodeIndex=-1}");
			}
			return format("Cursor{item=%s indexItem=%s nodeIndex=%d decodedNode=%s node=%s ",
			              item.present() ? item.get().toString().c_str() : "<absent>",
			              get(cache->get(nodeIndex)).toString().c_str(),
			              nodeIndex,
			              cache->get(nodeIndex).toString().c_str(),
			              node()->toString(tree).c_str());
		}

		bool valid() const { return nodeIndex != -1; }

		// Get T for Node n, and provide to n's delta the base and local decode cache entries to use/modify
		const T get(DecodedNode& decoded) const {
			DeltaT& delta = decoded.node(tree)->delta(tree->largeNodes);

			// If this node's cached partial is populated, then the delta can create T from that alone
			if (decoded.partial.present()) {
				return delta.apply(decoded.partial.get());
			}

			// Otherwise, get the base T
			bool basePrev = delta.getPrefixSource();
			int baseIndex = basePrev ? decoded.leftParentIndex : decoded.rightParentIndex;

			// If baseOffset is -1, then base T is DecodeCache's lower or upper bound
			if (baseIndex == -1) {
				return delta.apply(cache->arena, basePrev ? cache->lowerBound : cache->upperBound, decoded.partial);
			}

			// Otherwise, get the base's decoded node
			DecodedNode& baseDecoded = cache->get(baseIndex);

			// If the base's partial is present, apply delta to it to get result
			if (baseDecoded.partial.present()) {
				return delta.apply(cache->arena, baseDecoded.partial.get(), decoded.partial);
			}

			// Otherwise apply delta to base T
			return delta.apply(cache->arena, get(baseDecoded), decoded.partial);
		}

	public:
		// Get the item at the cursor
		// Behavior is undefined if the cursor is not valid.
		// If the cursor is moved, the reference object returned will be modified to
		// the cursor's new current item.
		const T& get() const {
			if (!item.present()) {
				item = get(cache->get(nodeIndex));
			}
			return item.get();
		}

		void switchTree(DeltaTree2* newTree) {
			tree = newTree;
			// Reset item because it may point into tree memory
			item.reset();
		}

		// If the cursor is valid, return a reference to the cursor's internal T.
		// Otherwise, returns a reference to the cache's upper boundary.
		const T& getOrUpperBound() const { return valid() ? get() : cache->upperBound; }

		bool operator==(const Cursor& rhs) const { return nodeIndex == rhs.nodeIndex; }
		bool operator!=(const Cursor& rhs) const { return nodeIndex != rhs.nodeIndex; }

		// The seek methods, of the form seek[Less|Greater][orEqual](...) are very similar.
		// They attempt move the cursor to the [Greatest|Least] item, based on the name of the function.
		// Then will not "see" erased records.
		// If successful, they return true, and if not then false a while making the cursor invalid.
		// These methods forward arguments to the seek() overloads, see those for argument descriptions.
		template <typename... Args>
		bool seekLessThan(Args... args) {
			int cmp = seek(args...);
			if (cmp < 0 || (cmp == 0 && nodeIndex != -1)) {
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
			if (cmp > 0 || (cmp == 0 && nodeIndex != -1)) {
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

		// Get the right child index for parentIndex
		int getRightChildIndex(int parentIndex) {
			DecodedNode* parent = &cache->get(parentIndex);

			// The cache may have a child index, but since cache covers multiple versions of a DeltaTree
			// it can't be used unless the node in the tree has a child.
			int childOffset = parent->node(tree)->getRightChildOffset(tree->largeNodes);

			if (childOffset == 0) {
				return -1;
			}

			// parent has this child so return the index if it is in DecodedNode
			if (parent->rightChildIndex != -1) {
				return parent->rightChildIndex;
			}

			// Create the child's DecodedNode and get its index
			int childIndex = cache->emplace_new(childOffset, parentIndex, parent->rightParentIndex);

			// Set the index in the parent.  The cache lookup is repeated because the cache has changed.
			cache->get(parentIndex).rightChildIndex = childIndex;
			return childIndex;
		}

		// Get the left child index for parentIndex
		int getLeftChildIndex(int parentIndex) {
			DecodedNode* parent = &cache->get(parentIndex);

			// The cache may have a child index, but since cache covers multiple versions of a DeltaTree
			// it can't be used unless the node in the tree has a child.
			int childOffset = parent->node(tree)->getLeftChildOffset(tree->largeNodes);

			if (childOffset == 0) {
				return -1;
			}

			// parent has this child so return the index if it is in DecodedNode
			if (parent->leftChildIndex != -1) {
				return parent->leftChildIndex;
			}

			// Create the child's DecodedNode and get its index
			int childIndex = cache->emplace_new(childOffset, parent->leftParentIndex, parentIndex);

			// Set the index in the parent.  The cache lookup is repeated because the cache has changed.
			cache->get(parentIndex).leftChildIndex = childIndex;
			return childIndex;
		}

		// seek() moves the cursor to a node containing s or the node that would be the parent of s if s were to be
		// added to the tree. If the tree was empty, the cursor will be invalid and the return value will be 0.
		// Otherwise, returns the result of s.compare(item at cursor position)
		// Does not skip/avoid deleted nodes.
		int seek(const T& s, int skipLen = 0) {
			nodeIndex = -1;
			item.reset();
			deltatree_printf("seek(%s) start %s\n", s.toString().c_str(), toString().c_str());
			int nIndex = rootIndex();
			int cmp = 0;

			while (nIndex != -1) {
				nodeIndex = nIndex;
				item.reset();
				cmp = s.compare(get(), skipLen);
				deltatree_printf("seek(%s) loop cmp=%d %s\n", s.toString().c_str(), cmp, toString().c_str());
				if (cmp == 0) {
					break;
				}

				if (cmp > 0) {
					nIndex = getRightChildIndex(nIndex);
				} else {
					nIndex = getLeftChildIndex(nIndex);
				}
			}

			return cmp;
		}

		bool moveFirst() {
			nodeIndex = -1;
			item.reset();
			int nIndex = rootIndex();
			deltatree_printf("moveFirst start %s\n", toString().c_str());
			while (nIndex != -1) {
				nodeIndex = nIndex;
				deltatree_printf("moveFirst moved %s\n", toString().c_str());
				nIndex = getLeftChildIndex(nIndex);
			}
			return _hideDeletedForward();
		}

		bool moveLast() {
			nodeIndex = -1;
			item.reset();
			int nIndex = rootIndex();
			deltatree_printf("moveLast start %s\n", toString().c_str());
			while (nIndex != -1) {
				nodeIndex = nIndex;
				deltatree_printf("moveLast moved %s\n", toString().c_str());
				nIndex = getRightChildIndex(nIndex);
			}
			return _hideDeletedBackward();
		}

		// Try to move to next node, sees deleted nodes.
		void _moveNext() {
			deltatree_printf("_moveNext start %s\n", toString().c_str());
			item.reset();
			// Try to go right
			int nIndex = getRightChildIndex(nodeIndex);

			// If we couldn't go right, then the answer is our next ancestor
			if (nIndex == -1) {
				nodeIndex = cache->get(nodeIndex).rightParentIndex;
				deltatree_printf("_moveNext move1 %s\n", toString().c_str());
			} else {
				// Go left as far as possible
				do {
					nodeIndex = nIndex;
					deltatree_printf("_moveNext move2 %s\n", toString().c_str());
					nIndex = getLeftChildIndex(nodeIndex);
				} while (nIndex != -1);
			}
		}

		// Try to move to previous node, sees deleted nodes.
		void _movePrev() {
			deltatree_printf("_movePrev start %s\n", toString().c_str());
			item.reset();
			// Try to go left
			int nIndex = getLeftChildIndex(nodeIndex);
			// If we couldn't go left, then the answer is our prev ancestor
			if (nIndex == -1) {
				nodeIndex = cache->get(nodeIndex).leftParentIndex;
				deltatree_printf("_movePrev move1 %s\n", toString().c_str());
			} else {
				// Go right as far as possible
				do {
					nodeIndex = nIndex;
					deltatree_printf("_movePrev move2 %s\n", toString().c_str());
					nIndex = getRightChildIndex(nodeIndex);
				} while (nIndex != -1);
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

		DeltaT& getDelta() const { return cache->get(nodeIndex).node(tree)->delta(tree->largeNodes); }

		bool isErased() const { return getDelta().getDeleted(); }

		// Erase current item by setting its deleted flag to true.
		// Tree header is updated if a change is made.
		// Cursor is then moved forward to the next non-deleted node.
		void erase() {
			auto& delta = getDelta();
			if (!delta.getDeleted()) {
				delta.setDeleted(true);
				--tree->numItems;
				tree->nodeBytesDeleted += (delta.size() + Node::headerSize(tree->largeNodes));
			}
			moveNext();
		}

		// Erase k by setting its deleted flag to true.  Returns true only if k existed
		bool erase(const T& k, int skipLen = 0) {
			Cursor c(cache, tree);
			if (c.seek(k, skipLen) == 0 && !c.isErased()) {
				c.erase();
				return true;
			}
			return false;
		}

		// Try to insert k into the DeltaTree, updating byte counts and initialHeight if they
		// have changed (they won't if k already exists in the tree but was deleted).
		// Returns true if successful, false if k does not fit in the space available
		// or if k is already in the tree (and was not already deleted).
		// Insertion on an empty tree returns false as well.
		// Insert does NOT change the cursor position.
		bool insert(const T& k, int skipLen = 0, int maxHeightAllowed = std::numeric_limits<int>::max()) {
			deltatree_printf("insert %s\n", k.toString().c_str());

			int nIndex = rootIndex();
			int parentIndex = nIndex;
			DecodedNode* parentDecoded;
			// Result of comparing node at parentIndex
			int cmp = 0;
			// Height of the inserted node
			int height = 0;

			// Find the parent to add the node to
			// This is just seek but modifies parentIndex instead of nodeIndex and tracks the insertion height
			deltatree_printf(
			    "insert(%s) start %s\n", k.toString().c_str(), Cursor(cache, tree, parentIndex).toString().c_str());
			while (nIndex != -1) {
				++height;
				parentIndex = nIndex;
				parentDecoded = &cache->get(parentIndex);
				cmp = k.compare(get(*parentDecoded), skipLen);
				deltatree_printf("insert(%s) moved cmp=%d %s\n",
				                 k.toString().c_str(),
				                 cmp,
				                 Cursor(cache, tree, parentIndex).toString().c_str());

				if (cmp == 0) {
					break;
				}

				if (cmp > 0) {
					deltatree_printf("insert(%s) move right\n", k.toString().c_str());
					nIndex = getRightChildIndex(nIndex);
				} else {
					deltatree_printf("insert(%s) move left\n", k.toString().c_str());
					nIndex = getLeftChildIndex(nIndex);
				}
			}

			// If the item is found, mark it erased if it isn't already
			if (cmp == 0) {
				DeltaT& delta = tree->nodeAt(parentDecoded->nodeOffset)->delta(tree->largeNodes);
				if (delta.getDeleted()) {
					delta.setDeleted(false);
					++tree->numItems;
					tree->nodeBytesDeleted -= (delta.size() + Node::headerSize(tree->largeNodes));
					deltatree_printf("insert(%s) deleted item restored %s\n",
					                 k.toString().c_str(),
					                 Cursor(cache, tree, parentIndex).toString().c_str());
					return true;
				}
				deltatree_printf("insert(%s) item exists %s\n",
				                 k.toString().c_str(),
				                 Cursor(cache, tree, parentIndex).toString().c_str());
				return false;
			}

			// If the tree was empty or the max insertion height is exceeded then fail
			if (parentIndex == -1 || height > maxHeightAllowed) {
				return false;
			}

			// Find the base base to borrow from, see if the resulting delta fits into the tree
			int leftBaseIndex, rightBaseIndex;
			bool addingRight = cmp > 0;
			if (addingRight) {
				leftBaseIndex = parentIndex;
				rightBaseIndex = parentDecoded->rightParentIndex;
			} else {
				leftBaseIndex = parentDecoded->leftParentIndex;
				rightBaseIndex = parentIndex;
			}

			T leftBase = leftBaseIndex == -1 ? cache->lowerBound : get(cache->get(leftBaseIndex));
			T rightBase = rightBaseIndex == -1 ? cache->upperBound : get(cache->get(rightBaseIndex));

			// If seek has reached a non-edge node then whatever bytes the left and right bases
			// have in common are definitely in common with k.  However, for an edge node there
			// is no guarantee, as one of the bases will be the lower or upper decode boundary
			// and it is possible to add elements to the DeltaTree beyond those boundaries.
			int common;
			if (leftBaseIndex == -1 || rightBaseIndex == -1) {
				common = 0;
			} else {
				common = leftBase.getCommonPrefixLen(rightBase, skipLen);
			}

			int commonWithLeftParent = k.getCommonPrefixLen(leftBase, common);
			int commonWithRightParent = k.getCommonPrefixLen(rightBase, common);
			bool borrowFromLeft = commonWithLeftParent >= commonWithRightParent;

			const T* base;
			int commonPrefix;
			if (borrowFromLeft) {
				base = &leftBase;
				commonPrefix = commonWithLeftParent;
			} else {
				base = &rightBase;
				commonPrefix = commonWithRightParent;
			}

			int deltaSize = k.deltaSize(*base, commonPrefix, false);
			int nodeSpace = deltaSize + Node::headerSize(tree->largeNodes);

			if (nodeSpace > tree->nodeBytesFree) {
				return false;
			}

			int childOffset = tree->size();
			Node* childNode = tree->nodeAt(childOffset);
			childNode->setLeftChildOffset(tree->largeNodes, 0);
			childNode->setRightChildOffset(tree->largeNodes, 0);

			// Create the decoded node and link it to the parent
			// Link the parent's decodednode to the child's decodednode
			// Link the parent node in the tree to the new child node
			// true if node is being added to right child
			int childIndex = cache->emplace_new(childOffset, leftBaseIndex, rightBaseIndex);

			// Get a new parentDecoded pointer as the cache may have changed allocations
			parentDecoded = &cache->get(parentIndex);

			if (addingRight) {
				// Adding child to right of parent
				parentDecoded->rightChildIndex = childIndex;
				parentDecoded->node(tree)->setRightChildOffset(tree->largeNodes, childOffset);
			} else {
				// Adding child to left of parent
				parentDecoded->leftChildIndex = childIndex;
				parentDecoded->node(tree)->setLeftChildOffset(tree->largeNodes, childOffset);
			}

			// Give k opportunity to populate its cache partial record
			k.updateCache(cache->get(childIndex).partial, cache->arena);

			DeltaT& childDelta = childNode->delta(tree->largeNodes);
			deltatree_printf("insert(%s) writing delta from %s\n", k.toString().c_str(), base->toString().c_str());
			int written = k.writeDelta(childDelta, *base, commonPrefix);
			ASSERT(deltaSize == written);
			childDelta.setPrefixSource(borrowFromLeft);

			tree->nodeBytesUsed += nodeSpace;
			tree->nodeBytesFree -= nodeSpace;
			++tree->numItems;

			// Update max height of the tree if necessary
			if (height > tree->maxHeight) {
				tree->maxHeight = height;
			}

			deltatree_printf("insert(%s) done  parent=%s\n",
			                 k.toString().c_str(),
			                 Cursor(cache, tree, parentIndex).toString().c_str());
			deltatree_printf("insert(%s) done  child=%s\n",
			                 k.toString().c_str(),
			                 Cursor(cache, tree, childIndex).toString().c_str());

			return true;
		}

	private:
		bool _hideDeletedBackward() {
			while (nodeIndex != -1 && getDelta().getDeleted()) {
				_movePrev();
			}
			return nodeIndex != -1;
		}

		bool _hideDeletedForward() {
			while (nodeIndex != -1 && getDelta().getDeleted()) {
				_moveNext();
			}
			return nodeIndex != -1;
		}
	};

	// Returns number of bytes written
	int build(int spaceAvailable, const T* begin, const T* end, const T* lowerBound, const T* upperBound) {
		largeNodes = spaceAvailable > SmallSizeLimit;
		int count = end - begin;
		numItems = count;
		nodeBytesDeleted = 0;
		initialHeight = (uint8_t)log2(count) + 1;
		maxHeight = 0;

		// The boundary leading to the new page acts as the last time we branched right
		if (count > 0) {
			nodeBytesUsed = buildSubtree(
			    *root(), begin, end, lowerBound, upperBound, lowerBound->getCommonPrefixLen(*upperBound, 0));
		} else {
			nodeBytesUsed = 0;
		}
		nodeBytesFree = spaceAvailable - size();
		return size();
	}

private:
	int buildSubtree(Node& node,
	                 const T* begin,
	                 const T* end,
	                 const T* leftParent,
	                 const T* rightParent,
	                 int subtreeCommon) {

		int count = end - begin;

		// Find key to be stored in root
		int mid = perfectSubtreeSplitPointCached(count);
		const T& item = begin[mid];

		int commonWithPrev = item.getCommonPrefixLen(*leftParent, subtreeCommon);
		int commonWithNext = item.getCommonPrefixLen(*rightParent, subtreeCommon);

		bool prefixSourcePrev;
		int commonPrefix;
		const T* base;
		if (commonWithPrev >= commonWithNext) {
			prefixSourcePrev = true;
			commonPrefix = commonWithPrev;
			base = leftParent;
		} else {
			prefixSourcePrev = false;
			commonPrefix = commonWithNext;
			base = rightParent;
		}

		int deltaSize = item.writeDelta(node.delta(largeNodes), *base, commonPrefix);
		node.delta(largeNodes).setPrefixSource(prefixSourcePrev);

		// Continue writing after the serialized Delta.
		uint8_t* wptr = (uint8_t*)&node.delta(largeNodes) + deltaSize;

		int leftChildOffset;
		// Serialize left subtree
		if (count > 1) {
			leftChildOffset = wptr - (uint8_t*)this;
			deltatree_printf("%p: offset=%d count=%d serialize left subtree leftChildOffset=%d\n",
			                 this,
			                 nodeOffset(&node),
			                 count,
			                 leftChildOffset);

			wptr += buildSubtree(*(Node*)wptr, begin, begin + mid, leftParent, &item, commonWithPrev);
		} else {
			leftChildOffset = 0;
		}

		int rightChildOffset;
		// Serialize right subtree
		if (count > 2) {
			rightChildOffset = wptr - (uint8_t*)this;
			deltatree_printf("%p: offset=%d count=%d serialize right subtree rightChildOffset=%d\n",
			                 this,
			                 nodeOffset(&node),
			                 count,
			                 rightChildOffset);

			wptr += buildSubtree(*(Node*)wptr, begin + mid + 1, end, &item, rightParent, commonWithNext);
		} else {
			rightChildOffset = 0;
		}

		node.setLeftChildOffset(largeNodes, leftChildOffset);
		node.setRightChildOffset(largeNodes, rightChildOffset);

		deltatree_printf("%p: Serialized %s as %s\n", this, item.toString().c_str(), node.toString(this).c_str());

		return wptr - (uint8_t*)&node;
	}
};
