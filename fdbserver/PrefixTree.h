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

struct PrefixTree {

	#pragma pack(push,1)
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

		/* Byte format, in order, based on flags
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

	public:

		// Structure for decoding a variable length Node to make the members more accessible
		// TODO:  Something more efficient where things are decoded on-demand
		struct Members {
			StringRef prefix;
			StringRef split;
			StringRef suffix;
			const Node *leftChild;
			const Node *rightChild;

			// Copies all of the key bytes into a single contiguous buffer
			// TODO:  Method to extract a prefix efficiently
			StringRef key(Arena &arena) const {
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

		void decode(Members &out, const StringRef &lastLeft, const StringRef &lastRight) const {
			const uint8_t *ptr = data;
			out.prefix = (flags & PREFIX_BORROW_LEFT ? lastLeft : lastRight).substr(0, getInt(ptr, PREFIX_BORROW_LARGE));

			if(flags & HAS_SPLIT_STRING) {
				int splitLen = getInt(ptr, SPLIT_LEN_LARGE);
				out.split = StringRef(ptr, splitLen);
				ptr += splitLen;
			}

			int suffixLen = 0;
			if(flags & HAS_SUFFIX_STRING) {
				suffixLen = getInt(ptr, SUFFIX_LEN_LARGE);
			}

			if(flags & HAS_LEFT_CHILD) {
				int leftLen = *(uint16_t *)ptr;
				ptr += sizeof(uint16_t);
				out.leftChild = (Node *)ptr;
				ptr += leftLen;
			}
			else {
				out.leftChild = nullptr;
			}

			if(suffixLen > 0) {
				out.suffix = StringRef(ptr, suffixLen);
				ptr += suffixLen;
			}

			out.rightChild = (flags & HAS_RIGHT_CHILD) ? (Node *)ptr : nullptr;
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
			Members m;
			decode(m, lastLeft, lastRight);

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

	uint16_t size;
	uint16_t count;
	Node root;

	// TODO:  Completely rewrite this to be efficient
	// Suggestion:  Allow comparison and borrowing bytes from non-coalesced keys
	struct Cursor {
		Cursor(const Node *root, StringRef boundary) : root(root), boundary(boundary) {}

		struct Context {
			const Node *node;
			StringRef key;
			StringRef lastLeft;
			StringRef lastRight;
			bool directionRight;
		};

		const Node *root;
		std::vector<Context> path;
		StringRef boundary;
		Arena arena;

		bool valid() {
			return !path.empty() && path.back().node != nullptr;
		}

		StringRef getKey() {
			return path.back().key;
		}

		bool seekLessThanOrEqual(StringRef s) {
			path.clear();
			arena = Arena();

			const Node *node = root;
			StringRef lastLeft;
			StringRef lastRight = boundary;

			while(1) {
				Node::Members m;
				node->decode(m, lastLeft, lastRight);
				StringRef key = m.key(arena);
				path.push_back({node, key, lastLeft, lastRight});

				int cmp = s.compare(key);

				if(cmp == 0)
					return true;

				if(cmp < 0) {
					if(m.leftChild == nullptr) {
						path.pop_back();

						// Find the last node from which we went right
						for(int i = path.size(); i > 0; --i) {
							if(path[i - 1].directionRight) {
								path.resize(i);
								return true;
							}
						}

						// We never went right, apparently, so cursor is now to the left of the leftmost node
						path.push_back({nullptr});
						return false;
					}

					path.back().directionRight = false;
					node = m.leftChild;
					lastLeft = key;
				}
				else {
					if(m.rightChild == nullptr) {
						return true;
					}

					path.back().directionRight = true;
					node = m.rightChild;
					lastRight = key;
				}
			}
		}
	};

	Cursor getCursor(StringRef boundary) {
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
		size = keys.size();
		// The boundary leading to the new page acts as the last time we branched right
		int size = sizeof(size) + sizeof(count) + build(root, &*keys.begin(), &*keys.end(), StringRef(), boundary);
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
