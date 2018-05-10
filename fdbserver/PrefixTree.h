#pragma once

#include "flow/flow.h"
#include "flow/Arena.h"

typedef uint64_t Word;
static inline int commonPrefixLength(uint8_t const* ap, uint8_t const* bp, int cl) {
	int i = 0;
	const int wordEnd = cl - sizeof(Word) + 1;

	for(; i < wordEnd; i += sizeof(Word)) {
		register Word a = *(Word *)ap;
		register Word b = *(Word *)bp;
		if(a != b) {
			return i + __builtin_ctzll(a ^ b) / 8;
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

static int perfectSubtreeSplitPoint(int subtree_size) {
	// return the inorder index of the root node in a subtree of the given size
	// consistent with the resulting binary search tree being "perfect" (having minimal height 
	// and all missing nodes as far right as possible).
	// There has to be a simpler way to do this.
	int s = lessOrEqualPowerOfTwo((subtree_size - 1) / 2 + 1) - 1;
	return std::min(s * 2 + 1, subtree_size - s - 1);
}

#pragma pack(push, 1)
struct PrefixTree {

	struct Node {
		uint8_t flags;
		uint8_t data[0];

		enum EFlags {
			PREFIX_BORROW_LEFT  = 1 << 0,
			PREFIX_BORROW_LARGE = 1 << 1,
			HAS_RIGHT_CHILD     = 1 << 2,
			HAS_LEFT_CHILD      = 1 << 3,
			HAS_SPLIT_STRING    = 1 << 4,  // Almost always 1 but we have a bit available so why not
			SPLIT_LEN_LARGE     = 1 << 5,
			HAS_SUFFIX_STRING   = 1 << 6,
			SUFFIX_LEN_LARGE    = 1 << 7
		};

		/* Byte format, in order, lengths are based on flags
		 *   flags            1 byte
		 *   prefix len       1 or 2 bytes
		 *   split len        0, 1 or 2 bytes
		 *   split string     0+ bytes
		 *   suffix len       0, 1, or 2 bytes
		 *   left child len   0 or 2 bytes
		 *   left child       0+ bytes
		 *   suffix string    0+ bytes
		 *   right child      0+
		 */

		static const int getMaxOverhead() {
			return 9;
		}

		// Write a small/large int at ptr, set largeFlag if it was large, return bytes written
		inline int writeInt(uint8_t *ptr, int value, EFlags largeFlag) {
			if(value > std::numeric_limits<uint8_t>::max()) {
				ASSERT(value <= std::numeric_limits<uint16_t>::max());
				flags |= largeFlag;
				*(uint16_t *)(ptr) = value;
				return sizeof(uint16_t);
			}
			else {
				*ptr = value;
				return sizeof(uint8_t);
			}
		}

	private:
		inline int getInt(const uint8_t *&ptr, EFlags largeFlag) const {
			int r;
			if(flags & largeFlag) {
				r = *(uint16_t *)(ptr);
				ptr += sizeof(uint16_t);
			}
			else {
				r = *(uint8_t *)(ptr);
				ptr += sizeof(uint8_t);
			}

			return r;
		}

		// Helper function to make decoding the variable length structure a little cleaner
		static inline int readLen(const uint8_t *&ptr, bool present, bool large) {
			if(!present)
				return 0;

			int r;
			if(large) {
				r = *(uint16_t *)(ptr);
				ptr += sizeof(uint16_t);
			}
			else {
				r = *(uint8_t *)(ptr);
				ptr += sizeof(uint8_t);
			}

			return r;
		}

	public:

		// Structure for decoding all of the Node members at once to a native structure
		struct DecodedMembers {
			StringRef prefix;
			StringRef split;
			StringRef suffix;
			const Node *leftChild;
			const Node *rightChild;

			DecodedMembers(const Node *node, const StringRef &lastLeft, const StringRef &lastRight) {
				const uint8_t *ptr = node->data;
				uint8_t flags = node->flags;
				prefix = (flags & PREFIX_BORROW_LEFT ? lastLeft : lastRight).substr(0, readLen(ptr, true, flags & PREFIX_BORROW_LARGE));

				int splitLen = readLen(ptr, flags & HAS_SPLIT_STRING, flags & SPLIT_LEN_LARGE);
				if(splitLen > 0) {
					split = StringRef(ptr, splitLen);
					ptr += splitLen;
				}

				int suffixLen = readLen(ptr, flags & HAS_SUFFIX_STRING, flags & SUFFIX_LEN_LARGE);

				int leftLen = readLen(ptr, flags & HAS_LEFT_CHILD, true);
				leftChild = leftLen ? (Node *)ptr : nullptr;
				ptr += leftLen;

				if(suffixLen > 0) {
					suffix = StringRef(ptr, suffixLen);
					ptr += suffixLen;
				}

				rightChild = (flags & HAS_RIGHT_CHILD) ? (Node *)ptr : nullptr;
			}

			// Copies all of the key bytes into a single contiguous buffer
			StringRef key(Arena &arena) const {
				ASSERT(prefix.size() == 0 || prefix.begin() != nullptr);

				StringRef r = makeString(prefix.size() + split.size() + suffix.size(), arena);
				uint8_t *wptr = mutateString(r);
				if(prefix.size() > 0) {
					memcpy(wptr, prefix.begin(), prefix.size());
					wptr += prefix.size();
				}
				if(split.size() > 0) {
					memcpy(wptr, split.begin(), split.size());
					wptr += split.size();
				}
				if(suffix.size() > 0) {
					memcpy(wptr, suffix.begin(), suffix.size());
					wptr += suffix.size();
				}
				return r;
			}

			Standalone<StringRef> key() const {
				Standalone<StringRef> r;
				(StringRef &)r = key(r.arena());
				return r;
			}
		};

		// Methods for decoding specific Node members on-demand
		inline int getPrefixLen() const {
			const uint8_t *ptr = data;
			return readLen(ptr, true, flags & PREFIX_BORROW_LARGE);
		}

		inline StringRef getSplitString() const {
			if(flags & HAS_SPLIT_STRING) {
				// skip prefix len
				const uint8_t *ptr = data + (flags & PREFIX_BORROW_LARGE ? 2 : 1);
				// read split len
				int splitLen = readLen(ptr, flags & HAS_SPLIT_STRING, flags & SPLIT_LEN_LARGE);

				return StringRef(ptr, splitLen);
			}
			return StringRef();
		}

		inline const Node * getLeftChild() const {
			if(flags & HAS_LEFT_CHILD) {
				// skip prefix len
				const uint8_t *ptr = data + (flags & PREFIX_BORROW_LARGE ? 2 : 1);
				// read split len, skip over split string
				ptr += readLen(ptr, flags & HAS_SPLIT_STRING, flags & SPLIT_LEN_LARGE);
				// read and skip suffix len if present
				readLen(ptr, flags & HAS_SUFFIX_STRING, flags & SUFFIX_LEN_LARGE);
				// skip left child length
				ptr += 2;

				return (Node *)ptr;
			}
			return nullptr;
		}

		inline StringRef getSuffixString() const {
			if(flags & HAS_SUFFIX_STRING) {
				// skip prefix len
				const uint8_t *ptr = data + (flags & PREFIX_BORROW_LARGE ? 2 : 1);
				// read split len, skip over split string
				ptr += readLen(ptr, flags & HAS_SPLIT_STRING, flags & SPLIT_LEN_LARGE);
				// read suffix len
				int suffixLen = readLen(ptr, flags & HAS_SUFFIX_STRING, flags & SUFFIX_LEN_LARGE);
				// read left child len, skip over left child, if exists
				ptr += readLen(ptr, flags & HAS_LEFT_CHILD, true);

				return StringRef(ptr, suffixLen);
			}
			return StringRef();
		}

		inline const Node * getRightChild() const {
			if(flags & HAS_RIGHT_CHILD) {
				// skip prefix len
				const uint8_t *ptr = data + (flags & PREFIX_BORROW_LARGE ? 2 : 1);
				// read split len, skip over split string
				ptr += readLen(ptr, flags & HAS_SPLIT_STRING, flags & SPLIT_LEN_LARGE);
				// read suffix len
				int suffixLen = readLen(ptr, flags & HAS_SUFFIX_STRING, flags & SUFFIX_LEN_LARGE);
				// read left child len, skip over left child, if exists
				ptr += readLen(ptr, flags & HAS_LEFT_CHILD, true);
				// skip over suffix
				ptr += suffixLen;

				return (Node *)ptr;
			}
			return nullptr;
		}

		inline int getKeySize() const {
			const uint8_t *ptr = data;
			int s = readLen(ptr, true, flags & PREFIX_BORROW_LARGE);
			int splitLen = readLen(ptr, flags & HAS_SPLIT_STRING, flags & SPLIT_LEN_LARGE);
			s += splitLen;
			ptr += splitLen;
			s += readLen(ptr, flags & HAS_SUFFIX_STRING, flags & SUFFIX_LEN_LARGE);
			return s;
		}

		static std::string escapeForDOT(StringRef s) {
			std::string r;
			for(char c : s) {
				if(isprint(c) && c != '"')
					r += c;
				else
					r += format("{%02X}", c);
			}
			return r;
		}

		std::string toDOT(const StringRef &lastLeft, const StringRef &lastRight) const {
			DecodedMembers m(this, lastLeft, lastRight);

			std::string r = format("node%p [ label = \"%s\" ];\nnode%p -> { %s %s };\n",
				this,
				(std::string("Prefix Source: ") + (flags & PREFIX_BORROW_LEFT ? "Left" : "Right") + "\\n[" + escapeForDOT(m.prefix) + "]\\n" + escapeForDOT(m.split) + "\\n" + escapeForDOT(m.suffix)).c_str(),
				this,
				m.leftChild ? format("node%p", m.leftChild).c_str() : "",
				m.rightChild ? format("node%p", m.rightChild).c_str() : ""
			);

			if(m.leftChild)
				r += m.leftChild->toDOT(m.key().toString(), lastRight);

			if(m.rightChild)
				r += m.rightChild->toDOT(lastLeft, m.key().toString());

			return r;
		}
	};

	uint16_t size;   // size in bytes
	Node root;

private:
	struct PathEntry {
		const Node *node;
		StringRef prefix;
		bool nodeTypeLeft;
		int moves;  // number of consecutive moves in same direction

		PathEntry(StringRef s = StringRef()) : node(nullptr), prefix(s) {
		}
		PathEntry(const Node *node, StringRef prefix, bool left, int moves) : node(node), prefix(prefix), nodeTypeLeft(left), moves(moves) {
		}

		inline bool valid() const {
			return node != nullptr;
		}

		int compareToKey(StringRef s) const {
			// If s is shorter than prefix, compare s to prefix for final result
			if(s.size() < prefix.size())
				return s.compare(prefix);

			// Compare prefix len of s to prefix
			int cmp = s.substr(0, prefix.size()).compare(prefix);

			// If they are the same, move on to split string
			if(cmp == 0) {
				s = s.substr(prefix.size());
				StringRef split = node->getSplitString();

				// If s is shorter than split, compare s to split for final result
				if(s.size() < split.size())
					return s.compare(split);

				// Compare split len of s to split
				cmp = s.substr(0, split.size()).compare(split);

				// If they are the same, move on to suffix string
				if(cmp == 0) {
					s = s.substr(split.size());
					return s.compare(node->getSuffixString());
				}
			}
			return cmp;
		}

		// Extract size bytes from the reconstituted key, allocating in arena if needed
		StringRef key(Arena &arena, int size = -1) const {
			if(size >= 0 && size <= prefix.size()) {
				return prefix.substr(0, size);
			}

			ASSERT(node != nullptr);

			if(size < 0) {
				size = node->getKeySize();
			}

			StringRef r = makeString(size, arena);
			uint8_t *wptr = mutateString(r);

			// The entire prefix is definitely needed
			if(prefix.size() > 0) {
				memcpy(wptr, prefix.begin(), prefix.size());
				wptr += prefix.size();
				size -= prefix.size();
				if(size == 0)
					return r;
			}

			StringRef split = node->getSplitString();
			if(split.size() > 0) {
				const int b = std::min(size, split.size());
				memcpy(wptr, split.begin(), b);
				wptr += b;
				size -= b;
				if(size == 0)
					return r;
			}

			StringRef suffix = node->getSuffixString();
			ASSERT(size <= suffix.size());
			if(suffix.size() > 0) {
				memcpy(wptr, suffix.begin(), size);
			}

			return r;
		}
	};

public:
	// This Cursor coalesces prefix bytes into a contiguous buffer for each node
	struct Cursor {
		Cursor(const Node *root, StringRef parentBoundary) {
			ASSERT(!(root->flags & Node::PREFIX_BORROW_LEFT));
			path.reserve(20);
			path.push_back(PathEntry(parentBoundary));
			path.push_back(PathEntry(root, parentBoundary.substr(0, root->getPrefixLen()), false, 1));
		}

		bool operator == (const Cursor &rhs) const {
			return path.back().node == rhs.path.back().node;
		}

		StringRef parentBoundary;
		std::vector<PathEntry> path;
		Arena arena;

		bool valid() {
			return path.back().valid();
		}

		StringRef getKey(Arena &arena) {
			return path.back().key(arena);
		}

		Standalone<StringRef> getKey() {
			Standalone<StringRef> s;
			(StringRef &)s = path.back().key(s.arena());
			return s;
		}

		bool seekLessThanOrEqual(StringRef s) {
			path.resize(2);
			arena = Arena();   // free previously used bytes

			// TODO: Track position of difference and use prefix reuse bytes and prefix sources
			// to skip comparison of some prefix bytes when possible
			while(1) {
				const PathEntry &p = path.back();
				int cmp = p.compareToKey(s);

				if(cmp == 0)
					return true;

				if(cmp < 0) {
					const Node *left = p.node->getLeftChild();
					if(left == nullptr) {
						if(p.nodeTypeLeft) {
							// If we only went left, cursor is now before the first element
							if((p.moves + 2) == path.size()) {
								path.push_back(PathEntry());
								return false;
							}

							// Otherwise, go to the parent of the last right child traversed,
							// which is the last node from which we went right
							path.resize(path.size() - (p.moves + 1));
							return true;
						}

						// p.directionLeft is false, so p.node is a right child, so go to its parent.
						path.pop_back();
						return true;
					}

					int newMoves = p.nodeTypeLeft ? p.moves + 1 : 1;
					const PathEntry *borrowSource = (left->flags & Node::PREFIX_BORROW_LEFT) ? &p : &p - newMoves;
					path.push_back(PathEntry(left, borrowSource->key(arena, left->getPrefixLen()), true, newMoves));
				}
				else {
					const Node *right = p.node->getRightChild();
					if(right == nullptr) {
						return true;
					}

					int newMoves = p.nodeTypeLeft ? 1 : p.moves + 1;
					const PathEntry *borrowSource = (right->flags & Node::PREFIX_BORROW_LEFT) ? &p - newMoves : &p;
					path.push_back(PathEntry(right, borrowSource->key(arena, right->getPrefixLen()), false, newMoves));
				}
			}
		}

		void printPath() {
			for(int i = 0; i < path.size(); ++i) {
				printf("path(%p_)[%d] = (%p) left %d moves %d\n", this, i, path[i].node, path[i].nodeTypeLeft, path[i].moves);
			}
		}

		bool moveNext() {
			const PathEntry &p = path.back();

			// If p isn't valid
			if(!p.valid()) {
				// If its parent is not a left child, return false as cursor is past the end
				if(!path[path.size() - 2].nodeTypeLeft) {
					return false;
				}
				else {
					// Cursor is before the first element so pop the last path entry and return true
					path.pop_back();
					return true;
				}
			}

			const Node *right = p.node->getRightChild();

			// If we can't go right, then go upward to the parent of the last left child
			if(right == nullptr) {
				// If current node was a left child then pop one node and we're done
				if(p.nodeTypeLeft) {
					path.pop_back();
					return true;
				}

				// Current node is a right child
				// If we are at the rightmost tree node, move cursor past the end
				if(p.moves + 1 == path.size()) {
					path.push_back(PathEntry());
					return false;
				}

				// Truncate path to the parent of the last left child
				path.resize(path.size() - (p.moves + 1));
				return true;
			}

			// Go right
			int newMoves = p.nodeTypeLeft ? 1 : p.moves + 1;
			const PathEntry *borrowSource = (right->flags & Node::PREFIX_BORROW_LEFT) ? &p - newMoves : &p;
			path.push_back(PathEntry(right, borrowSource->key(arena, right->getPrefixLen()), false, newMoves));

			// Go left as far as possible
			while(1) {
				const PathEntry &p = path.back();
				const Node *left = p.node->getLeftChild();
				if(left == nullptr)
					return true;

				int newMoves = p.nodeTypeLeft ? p.moves + 1 : 1;
				const PathEntry *borrowSource = (left->flags & Node::PREFIX_BORROW_LEFT) ? &p : &p - newMoves;
				path.push_back(PathEntry(left, borrowSource->key(arena, left->getPrefixLen()), true, newMoves));
			}
		}

		bool movePrev() {
			const PathEntry &p = path.back();

			// If p isn't valid
			if(!p.valid()) {
				// If its parent is a left child, return false as cursor is before the start
				if(path[path.size() - 2].nodeTypeLeft) {
					return false;
				}
				else {
					// Cursor is before the first element so pop the last path entry and return true
					path.pop_back();
					return true;
				}
			}

			const Node *left = p.node->getLeftChild();

			// If we can't go left, then go upward to the parent of the last right child
			if(left == nullptr) {
				// If current node was a right child then pop one node and we're done
				if(!p.nodeTypeLeft) {
					path.pop_back();
					return true;
				}

				// Current node is a left child
				// If we are at the leftmost tree node, move cursor before the start
				if(p.moves + 2 == path.size()) {
					path.push_back(PathEntry());
					return false;
				}

				// Truncate path to the parent of the last right child
				path.resize(path.size() - (p.moves + 1));
				return true;
			}

			// Go left
			int newMoves = p.nodeTypeLeft ? p.moves + 1 : 1;
			const PathEntry *borrowSource = (left->flags & Node::PREFIX_BORROW_LEFT) ? &p : &p - newMoves;
			path.push_back(PathEntry(left, borrowSource->key(arena, left->getPrefixLen()), true, newMoves));

			// Go right as far as possible
			while(1) {
				const PathEntry &p = path.back();
				const Node *right = p.node->getRightChild();
				if(right == nullptr)
					return true;

				int newMoves = p.nodeTypeLeft ? 1 : p.moves + 1;
				const PathEntry *borrowSource = (right->flags & Node::PREFIX_BORROW_LEFT) ? &p - newMoves : &p;
				path.push_back(PathEntry(right, borrowSource->key(arena, right->getPrefixLen()), false, newMoves));
			}
		}

	};

	Cursor getCursor(StringRef boundary = StringRef()) {
		return Cursor(&root, boundary);
	}

	std::string toDOT(const StringRef &boundary) const {
		std::string r;
		r += format("digraph PrefixTree%p {\n", this);
		r += root.toDOT(StringRef(), boundary);
		r += "}\n";
		return r;
	}

	// Returns number of bytes written
	uint16_t build(const std::vector<StringRef> keys, const StringRef &boundary) {
		// The boundary leading to the new page acts as the last time we branched right
		if(keys.empty()) {
			size = 0;
		}
		else {
			size = sizeof(size) + build(root, &*keys.begin(), &*keys.end(), StringRef(), boundary);
		}
		return size;
	}

	static uint16_t build(Node &root, const StringRef *begin, const StringRef *end, const StringRef &lastLeft, const StringRef &lastRight) {
		ASSERT(end != begin);

		int count = end - begin;

		// Find key to be stored in root
		int mid = perfectSubtreeSplitPoint(count);
		StringRef key = begin[mid];

		// Since key must be between lastLeft and lastRight, any common prefix they share must be shared by key
		// so rather than comparing all of key to each one separately we can just compare lastLeft and lastRight
		// to each other and then skip over the resulting length in key
		int leftRightCommon = commonPrefixLength(lastLeft.begin(), lastRight.begin(), std::min(lastLeft.size(), lastRight.size()));

		// Pointer to remainder of key after the left/right common bytes
		const uint8_t *keyExt = key.begin() + leftRightCommon;

		// Find out how many bytes beyond leftRightCommon key has with each last left/right string separately
		int extLeft = commonPrefixLength(keyExt, lastLeft.begin() + leftRightCommon, std::min(key.size(), lastLeft.size()) - leftRightCommon);
		int extRight = commonPrefixLength(keyExt, lastRight.begin() + leftRightCommon, std::min(key.size(), lastRight.size()) - leftRightCommon);

		// Use the longer result
		bool useLeft = extLeft > extRight;
		int prefixLen = leftRightCommon + (useLeft ? extLeft : extRight);

		uint8_t *ptr = root.data;
		root.flags = useLeft ? Node::PREFIX_BORROW_LEFT : 0;
		ptr += root.writeInt(ptr, prefixLen, Node::PREFIX_BORROW_LARGE);

		// Serialize split string, if necessary
		int splitLen;  // Bytes after prefix required to make traversal decision
		int suffixLen; // Remainder of key bytes after split key portion

		// If this is a leaf node, just put everything after the prefix into the suffix
		if(count == 1) {
			splitLen = 0;
			suffixLen = key.size() - prefixLen;
		}
		else {
			// Avoid using the suffix at all if the remainder of the key after the prefix is small enough
			// < 30 bytes plus overhead will fit in half of a cache line
			splitLen = key.size() - prefixLen;
			if(splitLen < 30) {
				suffixLen = 0;
			}
			else {
				// Remainder of the key was not small enough to , so find the actual required split key length
				splitLen = commonPrefixLength(key.begin(), begin[mid - 1].begin(), std::min(key.size(), begin[mid -1].size())) + 1 - prefixLen;
				if(splitLen < 0)
					splitLen = 0;
				suffixLen = key.size() - splitLen - prefixLen;
			}

			// Now serialize the split string, if there is one
			if(splitLen > 0) {
				root.flags |= Node::HAS_SPLIT_STRING;
				ptr += root.writeInt(ptr, splitLen, Node::SPLIT_LEN_LARGE);
				memcpy(ptr, key.begin() + prefixLen, splitLen);
				ptr += splitLen;
			}
		}

		// Serialize suffix length, if necessary
		if(suffixLen > 0) {
			root.flags |= Node::HAS_SUFFIX_STRING;
			ptr += root.writeInt(ptr, suffixLen, Node::SUFFIX_LEN_LARGE);
		}

		// Serialize left child, if not empty
		if(begin != begin + mid) {
			root.flags |= Node::HAS_LEFT_CHILD;
			int leftLen = build(*(Node *)(ptr + 2), begin, begin + mid, key, lastRight);
			*(uint16_t *)ptr = leftLen;
			ptr += sizeof(uint16_t) + leftLen;
		}

		// Serialize suffix string, if necessary
		if(suffixLen > 0) {
			memcpy(ptr, key.begin() + prefixLen + splitLen, suffixLen);
			ptr += suffixLen;
		}

		// Serialize right child
		if( (begin + mid + 1) != end) {
			root.flags |= Node::HAS_RIGHT_CHILD;
			int rightLen = build(*(Node *)(ptr), begin + mid + 1, end, lastLeft, key);
			ptr += rightLen;
		}

		return ptr - &root.flags;
	}

};

#pragma pack(pop)