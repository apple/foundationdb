/*
 * AsyncFileWriteChecker.cpp
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

#include "fdbrpc/AsyncFileWriteChecker.actor.h"
#include "flow/UnitTest.h"

Optional<int> AsyncFileWriteChecker::checksumHistoryBudget = {};
int AsyncFileWriteChecker::checksumHistoryPageSize = 4096;

static void compareWriteInfo(AsyncFileWriteChecker::WriteInfo w1, AsyncFileWriteChecker::WriteInfo w2) {
	ASSERT(w1.timestamp == w2.timestamp);
	ASSERT(w1.checksum == w2.checksum);
}

class LRU2 {
private:
	struct node {
		uint32_t page;
		AsyncFileWriteChecker::WriteInfo writeInfo;
		node *next, *prev;
		node(uint32_t _page, AsyncFileWriteChecker::WriteInfo _writeInfo) {
			page = _page;
			writeInfo = _writeInfo;
			next = nullptr;
			prev = NULL;
		}
	};

	node* start;
	node* end;
	std::unordered_map<uint32_t, node*> m;
	std::string fileName;
	int maxFullPagePlusOne;

	void insertHead(node* n) {
		n->next = start->next;
		start->next->prev = n;
		n->prev = start;
		start->next = n;
	}

	void removeFromList(node* n) {
		n->prev->next = n->next;
		n->next->prev = n->prev;
	}

public:
	LRU2(std::string _fileName) {
		fileName = _fileName;
		maxFullPagePlusOne = 0;
		start = new node(0, AsyncFileWriteChecker::WriteInfo());
		end = new node(0, AsyncFileWriteChecker::WriteInfo());
		start->next = end;
		end->prev = start;
	}

	~LRU2() {
		node* cur = start;
		node* next;
		while (cur != nullptr) {
			next = cur->next;
			delete cur;
			cur = next;
		}
		start = nullptr;
		end = nullptr;
	}

	void update(uint32_t page, AsyncFileWriteChecker::WriteInfo writeInfo) {
		if (m.find(page) != m.end()) {
			node* n = m[page];
			removeFromList(n);
			insertHead(n);
			n->writeInfo = writeInfo;
			return;
		}
		node* n = new node(page, writeInfo);
		insertHead(n);
		m[page] = n;
		if (page >= maxFullPagePlusOne) {
			maxFullPagePlusOne = page + 1;
		}
	}

	uint32_t randomPage() {
		if (m.size() == 0) {
			return 0;
		}
		auto it = m.begin();
		std::advance(it, deterministicRandom()->randomInt(0, (int)m.size()));
		return it->first;
	}

	void truncate(int newMaxFullPagePlusOne) {
		// exclude newMaxFullPage
		for (int i = newMaxFullPagePlusOne; i < maxFullPagePlusOne; i++) {
			remove(i);
		}
		if (maxFullPagePlusOne > newMaxFullPagePlusOne) {
			maxFullPagePlusOne = newMaxFullPagePlusOne;
		}
	}

	void print() {
		auto it = end->prev;
		while (it != start) {
			printf("%d\t", it->page);
			it = it->prev;
		}
		printf("\n");
	}

	int size() { return m.size(); }

	bool exist(uint32_t page) { return m.find(page) != m.end(); }

	AsyncFileWriteChecker::WriteInfo find(uint32_t page) {
		auto it = m.find(page);
		if (it == m.end()) {
			TraceEvent(SevError, "LRU2CheckerTryFindingPageNotExist")
			    .detail("FileName", fileName)
			    .detail("Page", page)
			    .log();
			return AsyncFileWriteChecker::WriteInfo();
		}
		return it->second->writeInfo;
	}

	uint32_t leastRecentlyUsedPage() {
		if (m.size() == 0) {
			return 0;
		}
		return end->prev->page;
	}

	void remove(uint32_t page) {
		if (m.find(page) == m.end()) {
			return;
		}
		node* n = m[page];
		removeFromList(n);
		m.erase(page);
		delete n;
	}
};

TEST_CASE("/fdbrpc/AsyncFileWriteChecker/LRU") {
	// run 1000 runs, each run either add or remote an element
	// record the latest add / remove operation for each key
	// try to find the elements that exist/removed, they should be present/absent
	int i = 0;
	int run = 1000;
	// limit is a small number so that page can have conflict
	// also LRU2::truncate has a time complexity of O(size of file), so page cannot be too large
	int limit = 1000;
	AsyncFileWriteChecker::LRU lru("TestLRU");
	LRU2 lru2("TestLRU");
	while (i < run) {
		double r = deterministicRandom()->random01();
		if (lru2.size() == 0 || r > 0.5) {
			// to add/update
			uint32_t page = deterministicRandom()->randomInt(1, limit);
			if (lru2.exist(page)) {
				// the page already exist
				compareWriteInfo(lru.find(page), lru2.find(page));
			}
			// change the content each time
			AsyncFileWriteChecker::WriteInfo wi;
			wi.checksum = deterministicRandom()->randomInt(1, INT_MAX);
			wi.timestamp = AsyncFileWriteChecker::transformTime(now());
			lru.update(page, wi);
			lru2.update(page, wi);
			compareWriteInfo(lru.find(page), lru2.find(page));
			// printf("ASYNC::Insert %d\n", page);
		} else if (r < 0.45) {
			// to remove
			uint32_t page = lru2.randomPage();

			ASSERT(page != 0);
			ASSERT(lru.exist(page));
			ASSERT(lru2.exist(page));
			compareWriteInfo(lru.find(page), lru2.find(page));
			lru.remove(page);
			lru2.remove(page);
			ASSERT(!lru.exist(page));
			ASSERT(!lru2.exist(page));
			// printf("ASYNC::erase %d\n", page);
		} else {
			// to truncate
			uint32_t page = lru2.randomPage();
			uint32_t page2 = lru2.randomPage();
			lru.truncate(page);
			lru2.truncate(page);
			if (page2 >= page) {
				ASSERT(!lru.exist(page2));
				ASSERT(!lru2.exist(page2));
			}
			// printf("ASYNC::truncate %d\n", page);
		}
		// lru2.print();
		if (lru2.size() != 0) {
			uint32_t leastRecentlyPage = lru.leastRecentlyUsedPage();
			uint32_t leastRecentlyPage2 = lru2.leastRecentlyUsedPage();
			ASSERT(leastRecentlyPage == leastRecentlyPage2);
			compareWriteInfo(lru.find(leastRecentlyPage), lru2.find(leastRecentlyPage));
		}

		// printf("Found Page %d, leastRecentlyPage is %d, step is %d\n", page, leastRecentlyPage, it->first);
		i += 1;
	}
	return Void();
}