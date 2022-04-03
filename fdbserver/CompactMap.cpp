/*
 * CompactMap.cpp
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
#pragma intrinsic(memcmp)

#include "flow/flow.h"
#include "flow/DeterministicRandom.h"
#include "fdbserver/PrefixTree.h"
#include <stdio.h>

static int nextPowerOfTwo(int n) {
	int p;
	for (p = 1; p < n; p += p)
		;
	return p;
}

static int less(StringRef a, StringRef b) {
	int al = a.size(), bl = b.size();
	int cl = al < bl ? al : bl;
	uint8_t const* ap = a.begin();
	uint8_t const* bp = b.begin();
	for (int i = 0; i < cl; i++) {
		if (ap[i] < bp[i])
			return 1;
		else if (bp[i] < ap[i])
			return 0;
	}
	return al < bl;
}

struct CompactPreOrderTree {
	enum {
		ENABLE_PREFETCH_RIGHT = 1
	}; // Use rather more memory BW, but hide a little latency when a right branch takes us out of a cache line.  Seems
	   // to help slightly.

	struct Node {
		enum { ENABLE_PREFIX = 1 }; // Enable or disable key prefix compression within a CompactPreOrderTree
		enum { ENABLE_LEFT_PTR = 0 };

		// offsets relative to `this`:
		enum { KEY_LENGTH_OFFSET = ENABLE_PREFIX * 1 };
		enum { KEY_DATA_OFFSET = KEY_LENGTH_OFFSET + 1 };

		// offsets relative to `keyEnd()`:
		enum { LPTR_OFFSET = 0 };
		enum { RPTR_OFFSET = 2 * ENABLE_LEFT_PTR };
		enum { END_OFFSET = RPTR_OFFSET + 2 };
		enum { IMPLICIT_LPTR_VALUE = END_OFFSET };

		static int getMaxOverhead() { return KEY_DATA_OFFSET + END_OFFSET; }

		int keyPrefixLength() {
			if (ENABLE_PREFIX)
				return *(uint8_t*)this;
			else
				return 0;
		}
		int keyLength() { return *((uint8_t*)this + KEY_LENGTH_OFFSET); }
		uint8_t const* keyData() { return (uint8_t const*)this + KEY_DATA_OFFSET; }
		uint8_t const* keyEnd() { return (uint8_t const*)this + KEY_DATA_OFFSET + keyLength(); }
		StringRef key() { return StringRef(keyData(), keyLength()); }
		Node* left() {
			auto ke = keyEnd();
			return (Node*)(ke + (ENABLE_LEFT_PTR ? *(int16_t*)(ke + LPTR_OFFSET) : IMPLICIT_LPTR_VALUE));
		}
		Node* right() {
			auto ke = keyEnd();
			return (Node*)(ke + *(uint16_t*)(ke + RPTR_OFFSET));
		}
		uint8_t* getEnd() { return (uint8_t*)keyEnd() + END_OFFSET; }

		void setKeyPrefixLength(int l) {
			if (ENABLE_PREFIX) {
				ASSERT(l < 256);
				*(uint8_t*)this = l;
			} else
				ASSERT(!l);
		}
		void setKeyLength(int l) {
			ASSERT(l < 256);
			*((uint8_t*)this + KEY_LENGTH_OFFSET) = l;
		}
		void setLeftPointer(Node* ptr) {
			auto ke = keyEnd();
			int o = (uint8_t*)ptr - ke;
			ASSERT(ENABLE_LEFT_PTR ? (int16_t(o) == o) : o == IMPLICIT_LPTR_VALUE);
			if (ENABLE_LEFT_PTR)
				*(uint16_t*)(ke + LPTR_OFFSET) = o;
		}
		void setRightPointer(Node* ptr) {
			auto ke = keyEnd();
			int o = (uint8_t*)ptr - ke;
			ASSERT(-32768 <= o && o < 32767);
			*(uint16_t*)(ke + RPTR_OFFSET) = o;
		}
	};

	int nodeCount;
	Node root;

	int relAddr(Node* n) { return (uint8_t*)n - (uint8_t*)this; }

	Node* lastLessOrEqual(StringRef searchKey) {
		Node* n = &root; // n is the root of the subtree we are searching
		Node* b = 0; // b is the greatest node <= searchKey which is a parent of n
		int nBFIndex = 0; // the index of the node n in the entire tree in "breadth first order", i.e. level by level.
		                  // This is NOT the order the tree is stored in!
		int prefixSize = 0; // the number of bytes of searchKey which are equal to the first bytes of the logical key of
		                    // the parent of n
		int dir;

		while (nBFIndex < nodeCount) {
			int np = n->keyPrefixLength();
			if (ENABLE_PREFETCH_RIGHT)
				_mm_prefetch((const char*)n->right(), _MM_HINT_T0);
			if (prefixSize < np) {
				// The searchKey differs from this node's logical key in the prefix this node shares with its parent
				// So the comparison between this node and searchKey has the same result as the comparison with the
				// parent and searchKey (dir is unchanged)
			} else {
				// The searchKey is equal to this node's logical key up to the beginning of the compressed key
				int al = searchKey.size() - np;
				int bl = n->keyLength();
				int cl = al < bl ? al : bl;
				int prefixLen = commonPrefixLength(searchKey.begin() + np, n->keyData(), cl);
				dir = prefixLen == cl ? al < bl : searchKey[np + prefixLen] < n->keyData()[prefixLen];
				if (Node::ENABLE_PREFIX)
					prefixSize = np + prefixLen;
			}

			nBFIndex = nBFIndex + nBFIndex + 2 - dir;
			auto l = n->left(), r = n->right();
			b = dir ? b : n;
			n = dir ? l : r;
		}

		return b;
	}

	static std::pair<Node*, Node*> lastLessOrEqual2(CompactPreOrderTree* this1,
	                                                CompactPreOrderTree* this2,
	                                                StringRef searchKey1,
	                                                StringRef searchKey2) {
		// Do two separate lastLessOrEqual operations at once, to make better use of the memory subsystem.
		// Don't try to read this code, it is write only (constructed by copy/paste from lastLessOrEqual and adding 1
		// and 2 to variables as necessary)

		Node* n1 = &this1->root; // n is the root of the subtree we are searching
		Node* b1 = 0; // b is the greatest node <= searchKey which is a parent of n
		int nBFIndex1 = 0; // the index of the node n in the entire tree in "breadth first order", i.e. level by level.
		                   // This is NOT the order the tree is stored in!
		int prefixSize1 = 0; // the number of bytes of searchKey which are equal to the first bytes of the logical key
		                     // of the parent of n
		int dir1;

		Node* n2 = &this2->root; // n is the root of the subtree we are searching
		Node* b2 = 0; // b is the greatest node <= searchKey which is a parent of n
		int nBFIndex2 = 0; // the index of the node n in the entire tree in "breadth first order", i.e. level by level.
		                   // This is NOT the order the tree is stored in!
		int prefixSize2 = 0; // the number of bytes of searchKey which are equal to the first bytes of the logical key
		                     // of the parent of n
		int dir2;

		while (nBFIndex1 < this1->nodeCount && nBFIndex2 < this2->nodeCount) {
			int np1 = n1->keyPrefixLength();
			int np2 = n2->keyPrefixLength();
			if (ENABLE_PREFETCH_RIGHT) {
				_mm_prefetch((const char*)n1->right(), _MM_HINT_T0);
				_mm_prefetch((const char*)n2->right(), _MM_HINT_T0);
			}
			if (prefixSize1 < np1) {
				// The searchKey differs from this node's logical key in the prefix this node shares with its parent
				// So the comparison between this node and searchKey has the same result as the comparison with the
				// parent and searchKey (dir is unchanged)
			} else {
				// The searchKey is equal to this node's logical key up to the beginning of the compressed key
				int al1 = searchKey1.size() - np1;
				int bl1 = n1->keyLength();
				int cl1 = al1 < bl1 ? al1 : bl1;
				int prefixLen1 = commonPrefixLength(searchKey1.begin() + np1, n1->keyData(), cl1);
				dir1 = prefixLen1 == cl1 ? al1 < bl1 : searchKey1[np1 + prefixLen1] < n1->keyData()[prefixLen1];
				prefixSize1 = np1 + prefixLen1;
			}
			if (prefixSize2 < np2) {
				// The searchKey differs from this node's logical key in the prefix this node shares with its parent
				// So the comparison between this node and searchKey has the same result as the comparison with the
				// parent and searchKey (dir is unchanged)
			} else {
				// The searchKey is equal to this node's logical key up to the beginning of the compressed key
				int al2 = searchKey2.size() - np2;
				int bl2 = n2->keyLength();
				int cl2 = al2 < bl2 ? al2 : bl2;
				int prefixLen2 = commonPrefixLength(searchKey2.begin() + np2, n2->keyData(), cl2);
				dir2 = prefixLen2 == cl2 ? al2 < bl2 : searchKey2[np2 + prefixLen2] < n2->keyData()[prefixLen2];
				prefixSize2 = np2 + prefixLen2;
			}

			nBFIndex1 = nBFIndex1 + nBFIndex1 + 2 - dir1;
			nBFIndex2 = nBFIndex2 + nBFIndex2 + 2 - dir2;
			auto l1 = n1->left(), r1 = n1->right();
			auto l2 = n2->left(), r2 = n2->right();
			b1 = dir1 ? b1 : n1;
			b2 = dir2 ? b2 : n2;
			n1 = dir1 ? l1 : r1;
			n2 = dir2 ? l2 : r2;
		}

		while (nBFIndex1 < this1->nodeCount) {
			int np1 = n1->keyPrefixLength();
			if (prefixSize1 < np1) {
				// The searchKey differs from this node's logical key in the prefix this node shares with its parent
				// So the comparison between this node and searchKey has the same result as the comparison with the
				// parent and searchKey (dir is unchanged)
			} else {
				// The searchKey is equal to this node's logical key up to the beginning of the compressed key
				int al1 = searchKey1.size() - np1;
				int bl1 = n1->keyLength();
				int cl1 = al1 < bl1 ? al1 : bl1;
				int prefixLen1 = commonPrefixLength(searchKey1.begin() + np1, n1->keyData(), cl1);
				dir1 = prefixLen1 == cl1 ? al1 < bl1 : searchKey1[np1 + prefixLen1] < n1->keyData()[prefixLen1];
				prefixSize1 = np1 + prefixLen1;
			}
			nBFIndex1 = nBFIndex1 + nBFIndex1 + 2 - dir1;
			auto l1 = n1->left(), r1 = n1->right();
			b1 = dir1 ? b1 : n1;
			n1 = dir1 ? l1 : r1;
		}

		while (nBFIndex2 < this2->nodeCount) {
			int np2 = n2->keyPrefixLength();
			if (prefixSize2 < np2) {
				// The searchKey differs from this node's logical key in the prefix this node shares with its parent
				// So the comparison between this node and searchKey has the same result as the comparison with the
				// parent and searchKey (dir is unchanged)
			} else {
				// The searchKey is equal to this node's logical key up to the beginning of the compressed key
				int al2 = searchKey2.size() - np2;
				int bl2 = n2->keyLength();
				int cl2 = al2 < bl2 ? al2 : bl2;
				int prefixLen2 = commonPrefixLength(searchKey2.begin() + np2, n2->keyData(), cl2);
				dir2 = prefixLen2 == cl2 ? al2 < bl2 : searchKey2[np2 + prefixLen2] < n2->keyData()[prefixLen2];
				prefixSize2 = np2 + prefixLen2;
			}
			nBFIndex2 = nBFIndex2 + nBFIndex2 + 2 - dir2;
			auto l2 = n2->left(), r2 = n2->right();
			b2 = dir2 ? b2 : n2;
			n2 = dir2 ? l2 : r2;
		}

		return std::make_pair(b1, b2);
	}

#if 0
	enum { ENABLE_FANCY_BUILD=1 };

	struct BuildInfo {
		Node* parent;
		bool rightChild;
		std::string const& prefix;
		std::string* begin;
		std::string* end;
		BuildInfo(Node* parent, bool rightChild, std::string const& prefix, std::string* begin, std::string* end)
			: parent(parent), rightChild(rightChild), prefix(prefix), begin(begin), end(end) {}
	};

	int build(std::vector<std::string>& input, std::string const& prefix = std::string()) {
		nodeCount = input.size();

		Deque< BuildInfo > queue;
		Deque< BuildInfo > deferred;
		queue.push_back(BuildInfo(nullptr, false, prefix, &input[0], &input[0] + input.size()));

		Node* node = &root;
		uint8_t* cacheLineEnd = (uint8_t*)node + 64;
		while (queue.size() || deferred.size()) {
			if (!queue.size()) {
				for (int i = 0; i < deferred.size(); i++)
					queue.push_back( deferred[i] );
				deferred.clear();
			}
			BuildInfo bi = queue.front();
			queue.pop_front();

			int mid = perfectSubtreeSplitPoint(bi.end - bi.begin);
			std::string& s = bi.begin[mid];
			int prefixLen = Node::ENABLE_PREFIX ? commonPrefixLength((uint8_t*)&bi.prefix[0], (uint8_t*)&s[0], std::min(bi.prefix.size(), s.size())) : 0;
			node->setKeyPrefixLength(prefixLen);
			node->setKeyLength(s.size() - prefixLen);
			memcpy((uint8_t*)node->key().begin(), &s[prefixLen], s.size() - prefixLen);

			if (bi.parent) {
				if (bi.rightChild)
					bi.parent->setRightPointer(node);
				else
					bi.parent->setLeftPointer(node);
			}

			if ((uint8_t*)node->getEnd() > cacheLineEnd) {
				cacheLineEnd = (uint8_t*)((intptr_t)node->getEnd() &~63) + 64;
				for (int i = 0; i < queue.size(); i++)
					deferred.push_back(queue[i]);
				queue.clear();
			}

			if (bi.begin != bi.begin + mid)
				queue.push_back(BuildInfo(node, false, s, bi.begin, bi.begin + mid));
			else if (Node::ENABLE_LEFT_PTR)
				node->setLeftPointer(node);

			if (bi.begin + mid + 1 != bi.end)
				queue.push_back(BuildInfo(node, true, s, bi.begin + mid + 1, bi.end));
			else
				node->setRightPointer(node);

			node = (Node*)node->getEnd();
		}

		return (uint8_t*)node - (uint8_t*)this;
	}

#else
	enum { ENABLE_FANCY_BUILD = 0 };

	int build(std::vector<std::string>& input, std::string const& prefix = std::string()) {
		nodeCount = input.size();
		return (uint8_t*)build(root, prefix, &input[0], &input[0] + input.size()) - (uint8_t*)this;
	}
	Node* build(Node& node, std::string const& prefix, std::string* begin, std::string* end) {
		if (begin == end)
			return &node;
		int mid = perfectSubtreeSplitPoint(end - begin);
		std::string& s = begin[mid];
		int prefixLen =
		    Node::ENABLE_PREFIX
		        ? commonPrefixLength((uint8_t*)&prefix[0], (uint8_t*)&s[0], std::min(prefix.size(), s.size()))
		        : 0;
		// printf("Node: %s at %d, subtree size %d, mid=%d, prefix %d\n", s.c_str(), relAddr(&node), end-begin, mid,
		// prefixLen);
		node.setKeyPrefixLength(prefixLen);
		node.setKeyLength(s.size() - prefixLen);
		memcpy((uint8_t*)node.key().begin(), &s[prefixLen], s.size() - prefixLen);

		Node* next = (Node*)node.getEnd();
		if (begin != begin + mid) {
			node.setLeftPointer(next);
			next = build(*node.left(), s, begin, begin + mid);
		} else if (Node::ENABLE_LEFT_PTR)
			node.setLeftPointer(&node);

		if (begin + mid + 1 != end) {
			node.setRightPointer(next);
			next = build(*node.right(), s, begin + mid + 1, end);
		} else
			node.setRightPointer(&node);

		return next;
	}
#endif
};

void compactMapTests(std::vector<std::string> testData,
                     std::vector<std::string> sampleQueries,
                     std::string prefixTreeDOTFile = "") {
	double t1, t2;
	int r = 0;
	std::sort(testData.begin(), testData.end());

	/*for (int i = 0; i < testData.size() - 1; i++) {
	    ASSERT(testData[i + 1].substr(0, 4) != testData[i].substr(0, 4));
	    ASSERT(_byteswap_ulong(*(uint32_t*)&testData[i][0]) < _byteswap_ulong(*(uint32_t*)&testData[i + 1][0]));
	}*/

	int totalKeyBytes = 0;
	for (auto& s : testData)
		totalKeyBytes += s.size();
	printf("%d bytes in %lu keys\n", totalKeyBytes, testData.size());

	for (int i = 0; i < 5; i++)
		printf("  '%s'\n", printable(StringRef(testData[i])).c_str());

	CompactPreOrderTree* t =
	    (CompactPreOrderTree*)new uint8_t[sizeof(CompactPreOrderTree) + totalKeyBytes +
	                                      CompactPreOrderTree::Node::getMaxOverhead() * testData.size()];

	t1 = timer_monotonic();
	int compactTreeBytes = t->build(testData);
	t2 = timer_monotonic();

	printf("Compact tree is %d bytes\n", compactTreeBytes);
	printf("Build time %0.0f us (%0.2f M/sec)\n", (t2 - t1) * 1e6, 1 / (t2 - t1) / 1e6);

	t1 = timer_monotonic();
	const int nBuild = 20000;
	for (int i = 0; i < nBuild; i++)
		r += t->build(testData);
	t2 = timer_monotonic();
	printf("Build time %0.0f us (%0.2f M/sec)\n", (t2 - t1) / nBuild * 1e6, nBuild / (t2 - t1) / 1e6);

	PrefixTree* pt = (PrefixTree*)new uint8_t[sizeof(PrefixTree) + totalKeyBytes +
	                                          testData.size() * PrefixTree::Node::getMaxOverhead(1, 256, 256)];

	std::vector<PrefixTree::EntryRef> keys;
	for (auto& k : testData) {
		keys.emplace_back(k, StringRef());
	}

	t1 = timer_monotonic();
	int prefixTreeBytes = pt->build(&*keys.begin(), &*keys.end(), StringRef(), StringRef());
	t2 = timer_monotonic();

	if (!prefixTreeDOTFile.empty()) {
		FILE* fout = fopen(prefixTreeDOTFile.c_str(), "w");
		fprintf(fout, "%s\n", pt->toDOT(StringRef(), StringRef()).c_str());
		fclose(fout);
	}

	// Calculate perfect prefix-compressed size
	int perfectSize = testData.front().size();
	for (int i = 1; i < testData.size(); ++i) {
		int common = commonPrefixLength(StringRef(testData[i]), StringRef(testData[i - 1]));
		perfectSize += (testData[i].size() - common);
	}

	printf("PrefixTree tree is %d bytes\n", prefixTreeBytes);
	printf("Perfect compressed size with no overhead is %d, average PrefixTree overhead is %.2f per item\n",
	       perfectSize,
	       double(prefixTreeBytes - perfectSize) / testData.size());
	printf("PrefixTree Build time %0.0f us (%0.2f M/sec)\n", (t2 - t1) * 1e6, 1 / (t2 - t1) / 1e6);

	// Test cursor forward iteration
	auto c = pt->getCursor(StringRef(), StringRef());
	ASSERT(c.moveFirst());

	bool end = false;
	for (int i = 0; i < keys.size(); ++i) {
		ASSERT(c.getKeyRef() == keys[i].key);
		end = !c.moveNext();
	}
	ASSERT(end);
	printf("PrefixTree forward scan passed\n");

	// Test cursor backward iteration
	ASSERT(c.moveLast());

	for (int i = keys.size() - 1; i >= 0; --i) {
		ASSERT(c.getKeyRef() == keys[i].key);
		end = !c.movePrev();
	}
	ASSERT(end);
	printf("PrefixTree reverse scan passed\n");

	t1 = timer_monotonic();
	for (int i = 0; i < nBuild; i++)
		r += pt->build(&*keys.begin(), &*keys.end(), StringRef(), StringRef());
	t2 = timer_monotonic();
	printf("PrefixTree Build time %0.0f us (%0.2f M/sec)\n", (t2 - t1) / nBuild * 1e6, nBuild / (t2 - t1) / 1e6);

	t->lastLessOrEqual(LiteralStringRef("8f9fad2e5e2af980a"));

	{
		std::string s, s1;
		CompactPreOrderTree::Node* n;
		for (int i = 0; i < testData.size(); i++) {
			s = testData[i];

			auto s1 = s; // s.substr(0, s.size() - 1);
			if (!s1.back())
				s1 = s1.substr(0, s1.size() - 1);
			else {
				s1.back()--;
				s1 += "\xff\xff\xff\xff\xff\xff";
			}
			auto n = t->lastLessOrEqual(s1);
			// printf("lastLessOrEqual(%s) = %s\n", s1.c_str(), n ? n->key().toString().c_str() : "(null)");
			ASSERT(i ? testData[i - 1].substr(n->keyPrefixLength()) == n->key() : !n);
			n = t->lastLessOrEqual(s);
			// printf("lastLessOrEqual(%s) = %s\n", s.c_str(), n ? n->key().toString().c_str() : "(null)");
			ASSERT(n->key() == s.substr(n->keyPrefixLength()));
			s1 = s + "a";
			auto n1 = t->lastLessOrEqual(s1);
			// printf("lastLessOrEqual(%s) = %s\n", s1.c_str(), n ? n->key().toString().c_str() : "(null)");
			ASSERT(n1->key() == s.substr(n1->keyPrefixLength()));

			ASSERT(CompactPreOrderTree::lastLessOrEqual2(t, t, s, s1) == std::make_pair(n, n1));
		}
		printf("compactMap lastLessOrEqual tests passed\n");
	}

	{
		auto cur = pt->getCursor(StringRef(), StringRef());

		for (int i = 0; i < keys.size(); i++) {
			StringRef s = keys[i].key;

			ASSERT(cur.seekLessThanOrEqual(s));
			ASSERT(cur.valid());
			ASSERT(cur.getKey() == s);

			StringRef shortString = s.substr(0, s.size() - 1);
			bool shorter = cur.seekLessThanOrEqual(shortString);
			if (i > 0) {
				if (shortString >= keys[i - 1].key) {
					ASSERT(shorter);
					ASSERT(cur.valid());
					ASSERT(cur.getKey() == keys[i - 1].key);
				}
			} else {
				ASSERT(!shorter);
			}

			ASSERT(cur.seekLessThanOrEqual(s.toString() + '\0'));
			ASSERT(cur.valid());
			ASSERT(cur.getKey() == s);
		}
		printf("PrefixTree lastLessOrEqual tests passed\n");
	}

	printf("Making %lu copies:\n", 2 * sampleQueries.size());

	std::vector<CompactPreOrderTree*> copies;
	for (int i = 0; i < 2 * sampleQueries.size(); i++) {
		copies.push_back((CompactPreOrderTree*)new uint8_t[compactTreeBytes]);
		memcpy(copies.back(), t, compactTreeBytes);
	}
	deterministicRandom()->randomShuffle(copies);

	std::vector<PrefixTree*> prefixTreeCopies;
	for (int i = 0; i < 2 * sampleQueries.size(); i++) {
		prefixTreeCopies.push_back((PrefixTree*)new uint8_t[prefixTreeBytes]);
		memcpy(prefixTreeCopies.back(), pt, prefixTreeBytes);
	}
	deterministicRandom()->randomShuffle(prefixTreeCopies);

	std::vector<std::vector<std::string>> array_copies;
	for (int i = 0; i < sampleQueries.size(); i++) {
		array_copies.push_back(testData);
	}
	deterministicRandom()->randomShuffle(array_copies);

	printf("shuffled\n");

	t1 = timer_monotonic();
	for (auto& q : sampleQueries)
		r += (intptr_t)t->lastLessOrEqual(q);
	t2 = timer_monotonic();
	printf("compactmap, in cache: %d queries in %0.3f sec: %0.3f M/sec\n",
	       (int)sampleQueries.size(),
	       t2 - t1,
	       sampleQueries.size() / (t2 - t1) / 1e6);

	auto cur = pt->getCursor(StringRef(), StringRef());

	t1 = timer_monotonic();
	for (auto& q : sampleQueries)
		r += cur.seekLessThanOrEqual(StringRef(q)) ? 1 : 0;
	t2 = timer_monotonic();
	printf("prefixtree, in cache: %d queries in %0.3f sec: %0.3f M/sec\n",
	       (int)sampleQueries.size(),
	       t2 - t1,
	       sampleQueries.size() / (t2 - t1) / 1e6);

	/*	t1 = timer_monotonic();
	    for (int q = 0; q < sampleQueries.size(); q += 2) {
	        auto x = CompactPreOrderTree::lastLessOrEqual2(t, t, sampleQueries[q], sampleQueries[q + 1]);
	        r += (intptr_t)x.first + (intptr_t)x.second;
	    }
	    t2 = timer_monotonic();
	    printf("in cache (2x interleaved): %d queries in %0.3f sec: %0.3f M/sec\n", (int)sampleQueries.size(), t2 - t1,
	   sampleQueries.size() / (t2 - t1) / 1e6);
	*/

	t1 = timer_monotonic();
	for (int q = 0; q < sampleQueries.size(); q++)
		r += (intptr_t)copies[q]->lastLessOrEqual(sampleQueries[q]);
	t2 = timer_monotonic();
	printf("compactmap, out of cache: %d queries in %0.3f sec: %0.3f M/sec\n",
	       (int)sampleQueries.size(),
	       t2 - t1,
	       sampleQueries.size() / (t2 - t1) / 1e6);

	std::vector<PrefixTree::Cursor> cursors;
	for (int q = 0; q < sampleQueries.size(); q++)
		cursors.push_back(prefixTreeCopies[q]->getCursor(StringRef(), StringRef()));

	t1 = timer_monotonic();
	for (int q = 0; q < sampleQueries.size(); q++)
		r += cursors[q].seekLessThanOrEqual(sampleQueries[q]) ? 1 : 0;
	t2 = timer_monotonic();
	printf("prefixtree, out of cache: %d queries in %0.3f sec: %0.3f M/sec\n",
	       (int)sampleQueries.size(),
	       t2 - t1,
	       sampleQueries.size() / (t2 - t1) / 1e6);

	/*
	    t1 = timer_monotonic();
	    for (int q = 0; q < sampleQueries.size(); q += 2) {
	        auto x = CompactPreOrderTree::lastLessOrEqual2(copies[q + sampleQueries.size()], copies[q +
	   sampleQueries.size() + 1], sampleQueries[q], sampleQueries[q + 1]); r += (intptr_t)x.first + (intptr_t)x.second;
	    }
	    t2 = timer_monotonic();
	    printf("out of cache (2x interleaved): %d queries in %0.3f sec: %0.3f M/sec\n", (int)sampleQueries.size(), t2 -
	   t1, sampleQueries.size() / (t2 - t1) / 1e6);
	*/

	t1 = timer_monotonic();
	for (int q = 0; q < sampleQueries.size(); q++)
		r += (intptr_t)(std::lower_bound(array_copies[q].begin(), array_copies[q].end(), sampleQueries[q]) -
		                testData.begin());
	t2 = timer_monotonic();
	printf("std::lower_bound: %d queries in %0.3f sec: %0.3f M/sec\n",
	       (int)sampleQueries.size(),
	       t2 - t1,
	       sampleQueries.size() / (t2 - t1) / 1e6);
}

std::vector<std::string> sampleDocuments(int N) {
	std::vector<std::string> testData;
	std::string p = "pre";
	std::string n = "\x01"
	                "name\x00\x00";
	std::string a = "\x01"
	                "address\x00\x00";
	std::string o = "\x01"
	                "orders\x00\x00";
	std::string oi = "\x01"
	                 "id\x00\x00";
	std::string oa = "\x01"
	                 "amount\x00\x00";
	std::string dbl = "\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00";
	for (int i = 0; i < N; i++) {
		std::string id =
		    BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned()).substr(12).toString();
		testData.push_back(p + id + n);
		testData.push_back(p + id + a);
		for (int j = 0; j < 5; j++) {
			std::string okey = p + id + o + dbl + (char)j;
			testData.push_back(okey + oi);
			testData.push_back(okey + oa);
		}
	}
	return testData;
}

StringRef shortestKeyBetween(StringRef a, StringRef b) {
	int p = commonPrefixLength(a.begin(), b.begin(), std::min(a.size(), b.size()));
	ASSERT(p < b.size());
	return b.substr(0, p + 1);
}

std::vector<std::string> sampleBPlusTreeSeparators(std::vector<std::string> rawDocs, int prefixToStrip) {
	// In the middle of a B+Tree, we won't have adjacent document keys but separators between
	// pages.  These need only contain as many bytes as necessary to distinguish the last item
	// in the previous page and the first item in the next page ("suffix compression"), and when
	// balancing the tree we can move a few keys left or right if it makes a big difference in the
	// suffix size ("split interval")
	// The B+Tree will presumably also do its own prefix compression, so we trim off the "obvious"
	// common prefix for this imaginary middle node

	std::vector<std::string> testData;
	std::sort(rawDocs.begin(), rawDocs.end());
	for (int i = 0; i + 1 < rawDocs.size(); i += 1000) {
		StringRef bestSplitPoint = shortestKeyBetween(rawDocs[i], rawDocs[i + 1]);

		for (int j = i + 1; j < i + 11; j++) {
			StringRef s = shortestKeyBetween(rawDocs[j], rawDocs[j + 1]);
			if (s.size() < bestSplitPoint.size())
				bestSplitPoint = s;
		}

		testData.push_back(bestSplitPoint.substr(prefixToStrip).toString());
	}
	return testData;
}

struct Page {
	Page() : tree(nullptr), size(0), sizeBuilt(0), unsortedKeys(0) {}

	std::vector<PrefixTree::EntryRef> keys;
	PrefixTree* tree;
	std::string treeBuffer;
	int size;
	int sizeBuilt;
	int unsortedKeys;

	void add(StringRef k) {
		keys.emplace_back(k, StringRef());
		size += k.size();
		++unsortedKeys;
	}

	void sort() {
		static auto cmp = [=](const PrefixTree::EntryRef& a, const PrefixTree::EntryRef& b) { return a.key < b.key; };
		if (unsortedKeys > 0) {
			// sort newest elements, then merge
			std::sort(keys.end() - unsortedKeys, keys.end(), cmp);
			std::inplace_merge(keys.begin(), keys.end() - unsortedKeys, keys.end(), cmp);
			unsortedKeys = 0;
		}
	}

	int build() {
		if (sizeBuilt != size) {
			sort();
			treeBuffer.reserve(keys.size() * PrefixTree::Node::getMaxOverhead(1, 256, 256) + size);
			tree = (PrefixTree*)treeBuffer.data();
			int b = tree->build(&*keys.begin(), &*keys.end(), StringRef(), StringRef());
			sizeBuilt = size;
			return b;
		}
		return 0;
	}
};

void ingestBenchmark() {
	std::vector<StringRef> keys_generated;
	Arena arena;
	std::set<StringRef> testmap;
	for (int i = 0; i < 1000000; ++i) {
		keys_generated.push_back(StringRef(arena,
		                                   format("........%02X......%02X.....%02X........%02X",
		                                          deterministicRandom()->randomInt(0, 100),
		                                          deterministicRandom()->randomInt(0, 100),
		                                          deterministicRandom()->randomInt(0, 100),
		                                          deterministicRandom()->randomInt(0, 100))));
	}

	double t1 = timer_monotonic();
	for (const auto& k : keys_generated)
		testmap.insert(k);
	double t2 = timer_monotonic();
	printf("Ingested %d elements into map, Speed %f M/s\n",
	       (int)keys_generated.size(),
	       keys_generated.size() / (t2 - t1) / 1e6);

	// sort a group after k elements were added
	for (int k = 5; k <= 20; k += 5) {
		// g is average page delta size
		for (int g = 10; g <= 150; g += 10) {
			// rebuild page after r bytes added
			for (int r = 500; r <= 4000; r += 500) {
				double elapsed = timer_monotonic();
				int builds = 0;
				int buildbytes = 0;
				int keybytes = 0;

				std::vector<Page*> pages;
				int pageCount = keys_generated.size() / g;
				pages.resize(pageCount);

				for (auto& key : keys_generated) {
					int p = deterministicRandom()->randomInt(0, pageCount);
					Page*& pPage = pages[p];
					if (pPage == nullptr)
						pPage = new Page();
					Page& page = *pPage;

					page.add(key);
					keybytes += key.size();

					if (page.keys.size() % k == 0) {
						page.sort();
					}

					// Rebuild page after r bytes added
					if (page.size - page.sizeBuilt > r) {
						int b = page.build();
						if (b > 0) {
							++builds;
							buildbytes += b;
						}
					}
				}

				for (auto p : pages) {
					if (p) {
						int b = p->build();
						if (b > 0) {
							++builds;
							buildbytes += b;
						}
					}
				}

				elapsed = timer_monotonic() - elapsed;
				printf("%6d keys  %6d pages %3f builds/page %6d builds/s  %6d pages/s  %5d avg keys/page  sort every "
				       "%d deltas  rebuild every %5d bytes  %7d keys/s %8d keybytes/s\n",
				       (int)keys_generated.size(),
				       pageCount,
				       (double)builds / pageCount,
				       int(builds / elapsed),
				       int(pageCount / elapsed),
				       g,
				       k,
				       r,
				       int(keys_generated.size() / elapsed),
				       int(keybytes / elapsed));

				for (auto p : pages) {
					delete p;
				}
			}
		}
	}
}

int main() {
	printf("CompactMap test\n");

#ifndef NDEBUG
	printf("Compiler optimization is OFF\n");
#endif

	printf("Key prefix compression is %s\n", CompactPreOrderTree::Node::ENABLE_PREFIX ? "ON" : "OFF");
	printf("Right subtree prefetch is %s\n", CompactPreOrderTree::ENABLE_PREFETCH_RIGHT ? "ON" : "OFF");
	printf("Left pointer is %s\n", CompactPreOrderTree::Node::ENABLE_LEFT_PTR ? "ON" : "OFF");
	printf("Fancy build is %s\n", CompactPreOrderTree::ENABLE_FANCY_BUILD ? "ON" : "OFF");

	setThreadLocalDeterministicRandomSeed(1);

	// ingestBenchmark();

	/*for (int subtree_size = 1; subtree_size < 20; subtree_size++) {
	    printf("Subtree of size %d:\n", subtree_size);

	    int s = lessOrEqualPowerOfTwo((subtree_size - 1) / 2 + 1) - 1;

	    printf("  s=%d\n", s);
	    printf("  1 + s + s=%d\n", 1 + s + s);
	    printf("  left: %d\n", subtree_size - 1 - 2 * s);

	    printf("  s*2+1: %d %d\n", s * 2 + 1, subtree_size - (s * 2 + 1) - 1);
	    printf("  n-s-1: %d %d\n", subtree_size-s-1, s);
	    printf("  min:   %d %d\n", std::min(s * 2 + 1, subtree_size - s - 1), subtree_size - std::min(s * 2 + 1,
	subtree_size - s - 1) - 1);
	}*/

	printf("\n16 byte hexadecimal random keys\n");
	std::vector<std::string> testData;
	for (int i = 0; i < 200; i++) {
		testData.push_back(deterministicRandom()->randomUniqueID().shortString());
	}
	std::vector<std::string> sampleQueries;
	for (int i = 0; i < 10000; i++) {
		sampleQueries.push_back(
		    deterministicRandom()->randomUniqueID().shortString().substr(0, deterministicRandom()->randomInt(0, 16)));
	}
	compactMapTests(testData, sampleQueries);

	printf("\nRaw index keys\n");
	testData.clear();
	sampleQueries.clear();
	for (int i = 0; i < 100; i++) {
		testData.push_back(format("%d Main Street #%d, New York NY 12345, United States of America|",
		                          1234 * (i / 100),
		                          (i / 10) % 10 + 1000) +
		                   deterministicRandom()->randomUniqueID().shortString());
	}
	for (int i = 0; i < 10000; i++)
		sampleQueries.push_back(format("%d Main Street", deterministicRandom()->randomInt(1000, 10000)));
	compactMapTests(testData, sampleQueries, "graph_addresses.dot");

	printf("\nb+tree separators for index keys\n");
	testData.clear();
	for (int i = 0; i < 100000; i++) {
		testData.push_back(format("%d Main Street #%d, New York NY 12345, United States of America|",
		                          12 * (i / 100),
		                          (i / 10) % 10 + 1000) +
		                   deterministicRandom()->randomUniqueID().shortString());
	}
	testData = sampleBPlusTreeSeparators(testData, 0);
	compactMapTests(testData, sampleQueries);

	printf("\nraw document keys\n");
	testData = sampleDocuments(20);
	sampleQueries.clear();
	std::string p = "pre";
	for (int i = 0; i < 10000; i++)
		sampleQueries.push_back(
		    p + BinaryWriter::toValue(deterministicRandom()->randomUniqueID(), Unversioned()).substr(12).toString());
	compactMapTests(testData, sampleQueries);

	printf("\nb+tree split keys for documents\n");
	testData = sampleBPlusTreeSeparators(sampleDocuments(30000), p.size());
	compactMapTests(testData, sampleQueries);

	return 0;
}
