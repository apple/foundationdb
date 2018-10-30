/*
 * PrefixTree.h
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
		register Word a = *(Word *)ap;
		register Word b = *(Word *)bp;
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

struct PrefixTree {
	// TODO: Make PrefixTree use a more complex record type with a multi column key
	typedef KeyValueRef EntryRef;
	typedef Standalone<EntryRef> Entry;

	static int MaximumTreeSize() {
		return std::numeric_limits<uint16_t>::max();
	};

	struct Node {
		uint8_t flags;

/* 
 * Node fields
 * 
 * Logically, a node has the following things
 *  - Flags describing what is in the node
 *  - Optional left child
 *  - Optional right child
 *  - Prefix string, described by a length and a source (which is the most recent left or right ancestor)
 *  - Optional split string, which contains any bytes after prefix which are needed to make a branching decision
 *  - Optional suffix string, containing any remaining key bytes after the split string
 *  - Optional value string
 * 
 * The physical layout places the left child subtree immediately after the split string so that it is likely
 * that the bytes read to make a branching decision and then choosing left (as should happen half of the time)
 * will have a high cache hit rate.
 * 
 * If necessary, the flags byte could be an enumeration into a set of possible options, since not all options 
 * combinations are needed.  For example,
 * 
 *   - The tree is balanced and filled from the left at the last level, so a node cannot have only a right child.
 *   - If there are no children, there is no point in splitting any key bytes after the prefix into separate strings.
 *   - If there is exactly one child (left) then the key bytes after the prefix can all go in the split string.  The
 *     traversal decision is to either stop or go left and one of those options (stop) will still have good memory
 *     locality.
 * 
 *   8 valid/necessary option combinations for presense of (Left, Right, Split, Suffix) out of 16 possibilities
 * 
 *   L  R  Split  Suffix
 *
 *   N  N  N       N    # No children, key has no bytes after prefix
 *   N  N  Y       N    # No children, key has bytes after prefix
 *   Y  N  N       N    # One child, key has no bytes after prefix
 *   Y  N  Y       N    # One child, key has bytes after prefix
 *   Y  Y  N       N    # Two children, key has no bytes after prefix
 *   Y  Y  N       Y    # Two children, branch decision can be made using only prefix bytes but there are more key bytes after
 *   Y  Y  Y       N    # Two children, branch decision requires all key bytes after prefix
 *   Y  Y  Y       Y    # Two children, branch decision requires some but not all bytes after prefix
 * 
 *   This can be represent with just 3 bits, if necessary, but for now there is space in the flags byte for all 4.
 * 
 *   Flag Bits
 *
 *   prefix borrow from next
 *      true -  borrow from the closest ancestor greater than this node
 *      false - borrow from the closest ancestor less    than this node
 *   large lengths = use 2 byte ints instead of 1 byte for prefix, split, suffix, and value lengths
 *     (TODO: It might be better to just not use a suffix at all when large is lengths is set)
 *   left child present
 *   right child present
 *   split string present
 *   suffix string present
 *   value string present
 * 
 *   Serialized format:
 *     All lengths are in the header, which has variable size
 *
 *     flags           1 byte
 *     prefix length   1-2 bytes based on large lengths flag
 *     split length    0-2 bytes based on split string present flag
 *     suffix length   0-2 bytes based on suffix string present and large lengths flags
 *     value length    0-1 bytes based on value string present and large lengths flag
 *     left length     0 or 2 bytes depending on left child present
 *     split           0+ bytes
 *     left child      0+ bytes
 *     suffix          0+ bytes
 *     value           0+ bytes
 *     right child     0+ bytes
 *     
 */
		enum EFlags {
			USE_LARGE_LENGTHS   = 1 << 0,
			PREFIX_SOURCE_NEXT  = 1 << 1,
			HAS_LEFT_CHILD      = 1 << 2,
			HAS_RIGHT_CHILD     = 1 << 3,
			HAS_SPLIT           = 1 << 4,
			HAS_SUFFIX          = 1 << 5,
			HAS_VALUE           = 1 << 6
		};

		// Stores decoded offsets (from beginning) of Node components
		struct Parser {
			Parser() {}
			Parser(const Node *n) {
				init(n);
			}

			const Node *node;

			typedef uint16_t OffsetT;
			OffsetT headerLen;
			OffsetT prefixLen;
			OffsetT leftPos;
			OffsetT suffixPos;
			OffsetT valuePos;
			OffsetT rightPos;

			StringRef splitString() const {
				return StringRef((const uint8_t *)node + headerLen, leftPos);
			}
			StringRef suffixString() const {
				return StringRef((const uint8_t *)node + headerLen + suffixPos, valuePos - suffixPos);
			}
			StringRef valueString() const {
				return StringRef((const uint8_t *)node + headerLen + valuePos, rightPos - valuePos);
			}
			const Node *leftChild() const {
				if(node->flags & HAS_LEFT_CHILD)
					return (const Node *)((const uint8_t *)node + headerLen + leftPos);
				return nullptr;
			}
			const Node *rightChild() const {
				if(node->flags & HAS_RIGHT_CHILD)
					return (const Node *)((const uint8_t *)node + headerLen + rightPos);
				return nullptr;
			}
			int keyLen() const {
				int len = prefixLen + leftPos + (valuePos - suffixPos);
				ASSERT(len >= 0);
				return len;
			}

			void init(const Node *n) {
				node = n;
				register union {
					const uint8_t *p8;
					const uint16_t *p16;
				};
				p8 = (const uint8_t *)&n->flags + 1;

				register int flags = n->flags;
				register bool large = flags & USE_LARGE_LENGTHS;

				prefixLen = large ? *p16++ : *p8++;

				if(flags & HAS_SPLIT)
					leftPos = large ? *p16++ : *p8++;
				else
					leftPos = 0;
				suffixPos = leftPos;
				if(flags & HAS_LEFT_CHILD) 
					suffixPos += *p16++;

				valuePos = suffixPos;
				if(flags & HAS_SUFFIX)
					valuePos += (large ? *p16++ : *p8++);

				rightPos = valuePos;
				if(flags & HAS_VALUE)
					rightPos += (large ? *p16++ : *p8++);

				register int header = 2;    // flags byte, first prefix len byte
				if(large)
					++header;  // second prefix len byte
				if(flags & HAS_SPLIT)
					header += large ? 2 : 1;
				if(flags & HAS_LEFT_CHILD)
					header += 2;
				if(flags & HAS_SUFFIX)
					header += large ? 2 : 1;
				if(flags & HAS_VALUE)
					header += large ? 2 : 1;
				headerLen = header;
			}
		};

		static inline int getMaxOverhead(int index, int keySize, int valueSize) {
			bool large = keySize > 255 || valueSize > 255;
			int overhead = 1 + (large ? 2 : 1);  // flags and prefix len
			// Value length size if present
			if(valueSize > 0)
				overhead += large ? 2 : 1;
			overhead += large ? 6 : 3;  // Worst case scenario for value, split and suffix lengths
			if((index & 0x01) != 0)
				overhead += 2;  // Left child length, one less than half of nodes will have one.
			return overhead;
		}

	public:

		// Methods for decoding specific Node members on-demand
		inline int getPrefixLen() const {
			return Parser(this).prefixLen;
		}

		inline StringRef getSplitString() const {
			return Parser(this).splitString();
		}

		inline StringRef getSuffixString() const {
			return Parser(this).suffixString();
		}

		inline StringRef getValueString() const {
			return Parser(this).valueString();
		}

		inline const Node * getLeftChild() const {
			return Parser(this).leftChild();
		}

		inline const Node * getRightChild() const {
			return Parser(this).rightChild();
		}

		inline int getKeySize() const {
			return Parser(this).keyLen();
		}
	};

#pragma pack(push,1)
	uint16_t size;   // size in bytes
	Node root;
#pragma pack(pop)

	static inline int GetHeaderSize() {
		return sizeof(PrefixTree) - sizeof(root);
	}

private:
	struct PathEntry {
		const Node *node;
		Node::Parser parser;

		// Key may or may not point to the space within keyBuffer.
		// Key will always contain at least the prefix bytes borrowed by node
		// KeyBuffer will always be large enough to hold the entire reconstituted key for node
		//
		// These are mutable because getting key bytes from this PathEntry can change these
		// but they're really just a read cache for reconstituted key bytes.
		mutable StringRef key;
		mutable Standalone<VectorRef<uint8_t>> keyBuffer;

		// Path entry was reached by going left from the previous node
		bool nodeIsLeftChild;
		// number of consecutive moves in same direction
		int moves;

		PathEntry() : node(nullptr) {
		}
		PathEntry(const PathEntry &rhs) {
			*this = rhs;
		}

		// Initialize the key byte buffer to hold bytes of a new node.  Use a new arena
		// if the old arena is being held by any users.
		void initKeyBufferSpace() {
			if(node != nullptr) {
				int size = parser.keyLen();
				if(keyBuffer.arena().impl && !keyBuffer.arena().impl->isSoleOwnerUnsafe()) {
					keyBuffer = Standalone<VectorRef<uint8_t>>();
				}
				keyBuffer.reserve(keyBuffer.arena(), size);
			}
		}

		PathEntry & operator= (const PathEntry &rhs) {
			node = rhs.node;
			parser = rhs.parser;
			nodeIsLeftChild = rhs.nodeIsLeftChild;
			moves = rhs.moves;
			// New key buffer must be able to hold full reconstituted key, not just the
			// part of it referenced by rhs.key (which may not be the whole thing)
			initKeyBufferSpace();
			if(node != nullptr && rhs.key.size() > 0) {
				// Copy rhs.key into keyBuffer and set key to the destination bytes
				memcpy(keyBuffer.begin(), rhs.key.begin(), rhs.key.size());
				key = StringRef(keyBuffer.begin(), rhs.key.size());
			}
			else {
				key = rhs.key;
			}
			return *this;
		}

		void init(StringRef s) {
			node = nullptr;
			key = s;
		}

		void init(const Node *_node, const PathEntry *prefixSource, bool isLeft, int numMoves) {
			node = _node;
			parser.init(node);
			nodeIsLeftChild = isLeft;
			moves = numMoves;

			// keyBuffer will be large enough to hold the full reconstituted key but initially
			// key will be a reference returned from prefixSource->getKeyRef()
			// See comments near keyBuffer and key for more info.
			initKeyBufferSpace();
			key = prefixSource->getKeyRef(parser.prefixLen);
		}

		inline bool valid() const {
			return node != nullptr;
		}

		int compareToKey(StringRef s) const {
			// Key has at least this node's borrowed prefix bytes in it.
			// If s is shorter than key, we only need to compare it to key
			if(s.size() < key.size())
				return s.compare(key);

			int cmp = s.substr(0, key.size()).compare(key);
			if(cmp != 0)
				return cmp;

			// The borrowed prefix bytes and possibly more have already been compared and were equal
			int comparedLen = key.size();
			s = s.substr(comparedLen);
			StringRef split = parser.splitString();
			int splitSizeOriginal = split.size();
			int splitStart = comparedLen - parser.prefixLen;
			if(splitStart < split.size()) {
				split = split.substr(splitStart);
				if(s.size() < split.size())
					return s.compare(split);
				cmp = s.substr(0, split.size()).compare(split);
				if(cmp != 0)
					return cmp;
				s = s.substr(split.size());
				comparedLen += split.size();
			}

			int suffixStart = comparedLen - (parser.prefixLen + splitSizeOriginal);
			StringRef suffix = parser.suffixString();
			ASSERT(suffixStart >= 0 && suffixStart <= suffix.size());
			return s.compare(suffix.substr(suffixStart));
		}

		// Make sure that key refers to bytes in keyBuffer, copying if necessary
		void ensureKeyInBuffer() const {
			if(key.begin() != keyBuffer.begin()) {
				memcpy(keyBuffer.begin(), key.begin(), key.size());
				key = StringRef(keyBuffer.begin(), key.size());
			}
		}

		// Get the borrowed prefix string.  Key must contain all of those bytes but it could contain more.
		StringRef getPrefix() const {
			if(node == nullptr)
				return key;
			return key.substr(0, parser.prefixLen);
		}

		// Return a reference to the first size bytes of the key.
		//
		// If size <= key's size then a substring of key will be returned, but if alwaysUseKeyBuffer
		// is true then before returning the existing value of key (not just the first size bytes)
		// will be copied into keyBuffer and key will be updated to point there.
		//
		// If size is greater than key's size, then key will be moved into keyBuffer if it is not already there
		// and the remaining needed bytes will be copied into keyBuffer from the split and suffix strings.
		KeyRef getKeyRef(int size = -1, bool alwaysUseKeyBuffer = false) const {
			if(size < 0)
				size = parser.keyLen();

			// If size is less than key then return a substring of it, possibly after moving it to the keyBuffer.
			if(size <= key.size()) {
				if(alwaysUseKeyBuffer)
					ensureKeyInBuffer();
				return key.substr(0, size);
			}

			ASSERT(node != nullptr);
			ensureKeyInBuffer();

			// The borrowed prefix bytes and possibly more must already be in key
			int writtenLen = key.size();
			StringRef split = parser.splitString();
			StringRef suffix = parser.suffixString();
			int splitStart = writtenLen - parser.prefixLen;
			if(splitStart < split.size()) {
				int splitLen = std::min(split.size() - splitStart, size - writtenLen);
				memcpy(mutateString(key) + writtenLen, split.begin() + splitStart, splitLen);
				writtenLen += splitLen;
			}
			int suffixStart = writtenLen - parser.prefixLen - split.size();
			if(suffixStart < suffix.size()) {
				int suffixLen = std::min(suffix.size() - suffixStart, size - writtenLen);
				memcpy(mutateString(key) + writtenLen, suffix.begin() + suffixStart, suffixLen);
				writtenLen += suffixLen;
			}
			ASSERT(writtenLen == size);
			key = StringRef(key.begin(), size);
			return key;
		}

		// Return keyRef(size) and the arena that keyBuffer resides in.
		Key getKey(int size = -1) const {
			StringRef k = getKeyRef(size, true);
			return Key(k, keyBuffer.arena());
		}
	};

public:
	// Cursor provides a way to seek into a PrefixTree and iterate over its content
	// Seek and move methods can return false can return false if they fail to achieve the desired effect
	// but a cursor will remain 'valid' as long as the tree is not empty.
	//
	// It coalesces prefix bytes into a contiguous buffer for each node along the traversal
	// path to make iteration faster.
	struct Cursor {
		Cursor() : pathLen(0) {
		}

		Cursor(const Node *root, StringRef prevAncestor, StringRef nextAncestor) {
			init(root, prevAncestor, nextAncestor);
		}

		static const int initialPathLen = 3;
		static const int initialPathCapacity = 20;
		// This is a separate function so that Cursors can be reused to search different PrefixTrees
		// which avoids cursor destruction and creation which involves unnecessary memory churn.
		// The root node is arbitrarily assumed to be a right child of prevAncestor which itself is a left child of nextAncestor
		void init(const Node *root, StringRef prevAncestor, StringRef nextAncestor) {
			if(path.size() < initialPathCapacity)
				path.resize(initialPathCapacity);
			pathLen = initialPathLen;
			path[0].init(nextAncestor);
			path[1].init(prevAncestor);
			path[2].init(root, &path[root->flags & Node::PREFIX_SOURCE_NEXT ? 0 : 1], false, 1);
		}

		bool operator == (const Cursor &rhs) const {
			return pathBack().node == rhs.pathBack().node;
		}

		StringRef leftParentBoundary;
		StringRef rightParentBoundary;
		std::vector<PathEntry> path;
		// pathLen is the number of elements in path which are in use.  This is to prevent constantly destroying
		// and constructing PathEntry objects which would unnecessarily churn through memory in Arena for storing
		// coalesced prefixes.
		int pathLen;

		bool valid() const {
			return pathLen != 0 && pathBack().valid();
		}

		// Get a reference to the current key which is valid until the Cursor is moved.
		KeyRef getKeyRef() const {
			return pathBack().getKeyRef();
		}

		// Get a Standalone<KeyRef> for the current key which will still be valid after the Cursor is moved.
		Key getKey() const {
			return pathBack().getKey();
		}

		// Get a reference to the current value which is valid as long as the Cursor's page memory exists.
		ValueRef getValueRef() const {
			return pathBack().parser.valueString();
		}

		// Get a key/value reference that is valid until the Cursor is moved.
		EntryRef getKVRef() const {
			return EntryRef(getKeyRef(), getValueRef());
		}

		// Returns a standalone EntryRef where both key and value exist in the standalone's arena,
		// unless copyValue is false in which case the value will be a reference into tree memory.
		Entry getKV(bool copyValue = true) const {
			Key k = getKey();
			ValueRef v = getValueRef();
			if(copyValue)
				v = ValueRef(k.arena(), getValueRef());
			return Entry(EntryRef(k, v), k.arena());
		}

		// Moves the cursor to the node with the greatest key less than or equal to s.  If successful,
		// returns true, otherwise returns false and the cursor will be at the node with the next key
		// greater than s.
		bool seekLessThanOrEqual(StringRef s) {
			if(pathLen == 0)
				return false;

			pathLen = initialPathLen;

			// TODO: Track position of difference and use prefix reuse bytes and prefix sources
			// to skip comparison of some prefix bytes when possible
			while(1) {
				const PathEntry &p = pathBack();
				const Node *right = p.parser.rightChild();
				_mm_prefetch((const char*)right, _MM_HINT_T0);

				int cmp = p.compareToKey(s);
				if(cmp == 0)
					return true;

				if(cmp < 0) {
					// Try to traverse left
					const Node *left = p.parser.leftChild();
					if(left == nullptr) {
						// If we're at the root, cursor should now be before the first element
						if(pathLen == initialPathLen) {
							return false;
						}

						if(p.nodeIsLeftChild) {
							// If we only went left, cursor should now be before the first element
							if((p.moves + initialPathLen) == pathLen) {
								return false;
							}

							// Otherwise, go to the parent of the last right child traversed,
							// which is the last node from which we went right
							popPath(p.moves + 1);
							return true;
						}

						// p.directionLeft is false, so p.node is a right child, so go to its parent.
						popPath(1);
						return true;
					}

					int newMoves = p.nodeIsLeftChild ? p.moves + 1 : 1;
					const PathEntry *borrowSource = (left->flags & Node::PREFIX_SOURCE_NEXT) ? &p : &p - newMoves;
					pushPath(left, borrowSource, true, newMoves);
				}
				else {
					// Try to traverse right
					if(right == nullptr) {
						return true;
					}

					int newMoves = p.nodeIsLeftChild ? 1 : p.moves + 1;
					const PathEntry *borrowSource = (right->flags & Node::PREFIX_SOURCE_NEXT) ? &p - newMoves : &p;
					pushPath(right, borrowSource, false, newMoves);
				}
			}
		}

		inline const PathEntry &pathBack() const {
			return path[pathLen - 1];
		}

		inline PathEntry &pathBack() {
			return path[pathLen - 1];
		}

		inline void pushPath(const Node *node, const PathEntry *borrowSource, bool left, int moves) {
			++pathLen;
			if(path.size() < pathLen) {
				path.resize(pathLen);
			}
			pathBack().init(node, borrowSource, left, moves);
		}

		inline void popPath(int n) {
			pathLen -= n;
		}

		std::string pathToString() const {
			std::string s;
			for(int i = 0; i < pathLen; ++i) {
				s += format("(%d: ", i);
				const Node *node = path[i].node;
				if(node != nullptr) {
					s += "childDir=";
					s += (path[i].nodeIsLeftChild ? "left " : "right ");
				}
				s += format("prefix='%s'", path[i].getPrefix().toHexString(20).c_str());
				if(node != nullptr) {
					s += format(" split='%s' suffix='%s' value='%s'", node->getSplitString().toHexString(20).c_str(), node->getSuffixString().toHexString(20).c_str(), node->getValueString().toHexString(20).c_str());
				}
				else
					s += ") ";
			}
			return s;
		}

		bool moveFirst() {
			if(pathLen == 0)
				return false;

			pathLen = initialPathLen;

			while(1) {
				const PathEntry &p = pathBack();
				const Node *left = p.parser.leftChild();

				if(left == nullptr)
					break;

				// TODO:  This can be simpler since it only goes left
				int newMoves = p.nodeIsLeftChild ? p.moves + 1 : 1;
				const PathEntry *borrowSource = (left->flags & Node::PREFIX_SOURCE_NEXT) ? &p : &p - newMoves;
				pushPath(left, borrowSource, true, newMoves);
			}

			return true;
		}

		bool moveLast() {
			if(pathLen == 0)
				return false;

			pathLen = initialPathLen;

			while(1) {
				const PathEntry &p = pathBack();
				const Node *right = p.parser.rightChild();

				if(right == nullptr)
					break;

				// TODO:  This can be simpler since it only goes right
				int newMoves = p.nodeIsLeftChild ? 1 : p.moves + 1;
				const PathEntry *borrowSource = (right->flags & Node::PREFIX_SOURCE_NEXT) ? &p - newMoves : &p;
				pushPath(right, borrowSource, false, newMoves);
			}

			return true;
		}

		bool moveNext() {
			const PathEntry &p = pathBack();

			// If p isn't valid
			if(!p.valid()) {
				return false;
			}

			const Node *right = p.parser.rightChild();

			// If we can't go right, then go upward to the parent of the last left child
			if(right == nullptr) {
				// If current node was a left child then pop one node and we're done
				if(p.nodeIsLeftChild) {
					popPath(1);
					return true;
				}

				// Current node is a right child.
				// If we are at the rightmost tree node return false and don't move.
				if(p.moves + initialPathLen - 1 == pathLen) {
					return false;
				}

				// Truncate path to the parent of the last left child
				popPath(p.moves + 1);
				return true;
			}

			// Go right
			int newMoves = p.nodeIsLeftChild ? 1 : p.moves + 1;
			const PathEntry *borrowSource = (right->flags & Node::PREFIX_SOURCE_NEXT) ? &p - newMoves : &p;
			pushPath(right, borrowSource, false, newMoves);

			// Go left as far as possible
			while(1) {
				const PathEntry &p = pathBack();
				const Node *left = p.parser.leftChild();
				if(left == nullptr) {
					return true;
				}

				int newMoves = p.nodeIsLeftChild ? p.moves + 1 : 1;
				const PathEntry *borrowSource = (left->flags & Node::PREFIX_SOURCE_NEXT) ? &p : &p - newMoves;
				pushPath(left, borrowSource, true, newMoves);
			}
		}

		bool movePrev() {
			const PathEntry &p = pathBack();

			// If p isn't valid
			if(!p.valid()) {
				return false;
			}

			const Node *left = p.parser.leftChild();

			// If we can't go left, then go upward to the parent of the last right child
			if(left == nullptr) {
				// If current node was a right child
				if(!p.nodeIsLeftChild) {
					// If we are at the root then don't move and return false.
					if(pathLen == initialPathLen)
						return false;

					// Otherwise, pop one node from the path and return true.
					popPath(1);
					return true;
				}

				// Current node is a left child.
				// If we are at the leftmost tree node then return false and don't move.
				if(p.moves + 3 == pathLen) {
					return false;
				}

				// Truncate path to the parent of the last right child
				popPath(p.moves + 1);
				return true;
			}

			// Go left
			int newMoves = p.nodeIsLeftChild ? p.moves + 1 : 1;
			const PathEntry *borrowSource = (left->flags & Node::PREFIX_SOURCE_NEXT) ? &p : &p - newMoves;
			pushPath(left, borrowSource, true, newMoves);

			// Go right as far as possible
			while(1) {
				const PathEntry &p = pathBack();
				const Node *right = p.parser.rightChild();
				if(right == nullptr) {
					return true;
				}

				int newMoves = p.nodeIsLeftChild ? 1 : p.moves + 1;
				const PathEntry *borrowSource = (right->flags & Node::PREFIX_SOURCE_NEXT) ? &p - newMoves : &p;
				pushPath(right, borrowSource, false, newMoves);
			}
		}

	};

	Cursor getCursor(StringRef prevAncestor, StringRef nextAncestor) const {
		return (size != 0) ? Cursor(&root, prevAncestor, nextAncestor) : Cursor();
	}

	static std::string escapeForDOT(StringRef s) {
		std::string r = "\"";
		for(char c : s) {
			if(c == '\n')
				r += "\\n";
			else if(isprint(c) && c != '"')
				r += c;
			else
				r += format("{%02X}", c);
		}
		return r + '"';
	}

	std::string toDOT(StringRef prevAncestor, StringRef nextAncestor) const {
		auto c = getCursor(prevAncestor, nextAncestor);
		c.moveFirst();

		std::string r;
		r += format("digraph PrefixTree%p {\n", this);

		do {
			const PathEntry &p = c.pathBack();
			const Node *n = p.node;
			const Node *left = p.parser.leftChild();
			const Node *right = p.parser.rightChild();

			std::string label = escapeForDOT(format("PrefixSource: %s\nPrefix: [%s]\nSplit: %s\nSuffix: %s",
				n->flags & Node::PREFIX_SOURCE_NEXT ? "Left" : "Right",
				p.getPrefix().toString().c_str(),
				p.parser.splitString().toString().c_str(),
				p.parser.suffixString().toString().c_str()
			));

			r += format("node%p [ label = %s ];\nnode%p -> { %s %s };\n", n, label.c_str(), n,
				left ? format("node%p", left).c_str() : "",
				right ? format("node%p", right).c_str() : ""
			);

		} while(c.moveNext());

		r += "}\n";

		return r;
	}

	// Returns number of bytes written
	int build(const EntryRef *begin, const EntryRef *end, StringRef prevAncestor, StringRef nextAncestor) {
		// The boundary leading to the new page acts as the last time we branched right
		if(begin == end) {
			size = 0;
		}
		else {
			size = sizeof(size) + build(root, begin, end, nextAncestor, prevAncestor);
		}
		ASSERT(size <= MaximumTreeSize());
		return size;
	}

private:
	static uint16_t build(Node &root, const EntryRef *begin, const EntryRef *end, const StringRef &nextAncestor, const StringRef &prevAncestor) {
		ASSERT(end != begin);

		int count = end - begin;

		// Find key to be stored in root
		int mid = perfectSubtreeSplitPointCached(count);
		const StringRef &key = begin[mid].key;
		const StringRef &val = begin[mid].value;

		// Since key must be between lastLeft and lastRight, any common prefix they share must be shared by key
		// so rather than comparing all of key to each one separately we can just compare lastLeft and lastRight
		// to each other and then skip over the resulting length in key
		int nextPrevCommon = commonPrefixLength(nextAncestor.begin(), prevAncestor.begin(), std::min(nextAncestor.size(), prevAncestor.size()));

		// Pointer to remainder of key after the left/right common bytes
		const uint8_t *keyExt = key.begin() + nextPrevCommon;

		// Find out how many bytes beyond leftRightCommon key has with each last left/right string separately
		int extNext = commonPrefixLength(keyExt, nextAncestor.begin() + nextPrevCommon, std::min(key.size(), nextAncestor.size()) - nextPrevCommon);
		int extPrev = commonPrefixLength(keyExt, prevAncestor.begin() + nextPrevCommon, std::min(key.size(), prevAncestor.size()) - nextPrevCommon);

		// Use the longer result
		bool prefixSourceNext = extNext > extPrev;
		
		int prefixLen = nextPrevCommon + (prefixSourceNext ? extNext : extPrev);

		int splitLen;   // Bytes after prefix required to make traversal decision
		int suffixLen;  // Remainder of key bytes after split key portion

		//printf("build: '%s'\n  prefixLen %d  prefixSourceNext %d\n", key.toHexString(20).c_str(), prefixLen, prefixSourceNext);

		// 2 entries or less means no right child, so just put all remaining key bytes into split string.
		if(count < 3) {
			splitLen = key.size() - prefixLen;
			suffixLen = 0;
		}
		else {
			// There are 2 children 
			// Avoid using the suffix at all if the remainder is small enough.
			splitLen = key.size() - prefixLen;
			if(splitLen < SERVER_KNOBS->PREFIX_TREE_IMMEDIATE_KEY_SIZE_LIMIT) {
				suffixLen = 0;
			}
			else {
				// Remainder of the key was not small enough to put entirely before the left child, so find the actual required to make the branch decision
				const StringRef &prevKey = begin[mid - 1].key;
				splitLen = commonPrefixLength(key.begin(), prevKey.begin(), std::min(key.size(), prevKey.size())) + 1 - prefixLen;

				// Put at least the minimum immediate byte count in the split key (before the left child)
				if(splitLen < SERVER_KNOBS->PREFIX_TREE_IMMEDIATE_KEY_SIZE_MIN)
					splitLen = std::min(key.size() - prefixLen, SERVER_KNOBS->PREFIX_TREE_IMMEDIATE_KEY_SIZE_MIN);

				suffixLen = key.size() - splitLen - prefixLen;
			}
		}

		// We now know enough about the fields present and their lengths to set the flag bits and write a header
		// If any int is more than 8 bits then use large ints
		bool large = prefixLen > 255 || splitLen > 255 || suffixLen > 255 || val.size() > 255;
		root.flags = large ? Node::USE_LARGE_LENGTHS : 0;

		if(prefixSourceNext)
			root.flags |= Node::PREFIX_SOURCE_NEXT;

		union {
			uint8_t *p8;
			uint16_t *p16;
		};
		p8 = &root.flags + 1;

		if(large)
			*p16++ = prefixLen;
		else
			*p8++ = prefixLen;

		if(splitLen > 0) {
			root.flags |= Node::HAS_SPLIT;
			if(large)
				*p16++ = splitLen;
			else
				*p8++ = splitLen;
		}

		uint16_t *pLeftLen = p16;
		if(count > 1) {
			++p16;
		}

		if(suffixLen > 0) {
			root.flags |= Node::HAS_SUFFIX;
			if(large)
				*p16++ = suffixLen;
			else
				*p8++ = suffixLen;
		}

		if(val.size() > 0) {
			root.flags |= Node::HAS_VALUE;
			if(large)
				*p16++ = val.size();
			else
				*p8++ = val.size();
		}

		// Header is written, now write strings and children in order.
		const uint8_t *keyPtr = key.begin() + prefixLen;

		// Serialize split bytes
		if(splitLen > 0) {
			memcpy(p8, keyPtr, splitLen);
			p8 += splitLen;
			keyPtr += splitLen;
		}

		// Serialize left child
		if(count > 1) {
			root.flags |= Node::HAS_LEFT_CHILD;
			int leftLen = build(*(Node *)(p8), begin, begin + mid, key, prevAncestor);
			*pLeftLen = leftLen;
			p8 += leftLen;
		}

		// Serialize suffix bytes
		if(suffixLen > 0) {
			memcpy(p8, keyPtr, suffixLen);
			p8 += suffixLen;
		}

		// Serialize value bytes
		if(val.size() > 0) {
			memcpy(p8, val.begin(), val.size());
			p8 += val.size();
		}

		// Serialize right child
		if(count > 2) {
			root.flags |= Node::HAS_RIGHT_CHILD;
			int rightLen = build(*(Node *)(p8), begin + mid + 1, end, nextAncestor, key);
			p8 += rightLen;
		}

/*
printf("\nBuilt: key '%s'  c %d  p %d  spl %d  suf %d\nRaw: %s\n", key.toString().c_str(), count, prefixLen, splitLen, suffixLen, StringRef(&root.flags, p8 - &root.flags).toHexString(20).c_str());
Node::Parser p(&root);
printf("parser: headerLen %d prefixLen %d leftPos %d rightPos %d split %s suffix %s val %s\n", 
	   p.headerLen, p.prefixLen, p.leftPos, p.rightPos, p.splitString().toString().c_str(), p.suffixString().toString().c_str(), p.valueString().toString().c_str());
*/
		return p8 - (uint8_t *)&root;
	}
};
