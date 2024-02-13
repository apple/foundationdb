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

TEST_CASE("/fdbrpc/AsyncFileWriteChecker/LRU") {
	// run 1000 runs, each run either add or remote an element
	// record the latest add / remove operation for each key
	// try to find the elements that exist/removed, they should be present/absent
	int i = 0;
	int run = 1000;
	int limit = 1000; // limit is a small number so that page can have conflict
	AsyncFileWriteChecker::LRU lru("TestLRU");

	std::map<int, uint32_t> stepToKey;
	std::map<uint32_t, int> keyToStep; // std::map is to support ::truncate
	std::unordered_map<uint32_t, AsyncFileWriteChecker::WriteInfo> pageContents;
	while (i < run) {
		double r = deterministicRandom()->random01();
		if (keyToStep.empty() || r > 0.5) {
			// to add/update
			uint32_t page = deterministicRandom()->randomInt(0, limit);
			bool alreadyExist = false;
			if (keyToStep.find(page) != keyToStep.end()) {
				// the page already exist
				alreadyExist = true;
				compareWriteInfo(lru.find(page), pageContents[page]);
			}
			// change the content each time
			pageContents[page].checksum = deterministicRandom()->randomInt(0, INT_MAX);
			pageContents[page].timestamp = deterministicRandom()->randomInt(0, INT_MAX);
			lru.update(page, pageContents[page]);
			compareWriteInfo(lru.find(page), pageContents[page]);

			if (alreadyExist) {
				// remove its old entry in stepToKey
				stepToKey.erase(keyToStep[page]);
			}
			keyToStep[page] = i;
			stepToKey[i] = page;
			// printf("ASYNC::Insert %d\n", page);
		} else if (r < 0.45) {
			// to remove
			auto it = keyToStep.begin();
			std::advance(it, deterministicRandom()->randomInt(0, (int)keyToStep.size()));
			uint32_t page = it->first;
			int step = it->second;

			ASSERT(lru.exist(page));
			lru.remove(page);
			ASSERT(!lru.exist(page));
			pageContents.erase(page);
			keyToStep.erase(it);
			stepToKey.erase(step);
			// printf("ASYNC::erase %d\n", page);
		} else {
			// to truncate
			auto it = keyToStep.begin();
			int step = deterministicRandom()->randomInt(0, (int)keyToStep.size());
			std::advance(it, step);
			uint32_t page = it->first;

			auto it2 = keyToStep.begin();
			int step2 = deterministicRandom()->randomInt(step, (int)keyToStep.size());
			std::advance(it2, step2);

			lru.truncate(page);
			ASSERT(!lru.exist(it2->first)); // make sure nothing after(including) page still exist

			while (it != keyToStep.end()) {
				int step = it->second;
				auto next = it;
				next++;
				keyToStep.erase(it);
				stepToKey.erase(step);
				it = next;
			}
			// printf("ASYNC::truncate %d\n", page);
		}
		uint32_t leastRecentlyPage = lru.leastRecentlyUsedPage();
		if (leastRecentlyPage == -1) {
			i += 1;
			continue;
		}
		auto it = stepToKey.begin();
		int page = it->second;
		// printf("Found Page %d, leastRecentlyPage is %d, step is %d\n", page, leastRecentlyPage, it->first);
		ASSERT(keyToStep.find(page) != keyToStep.end());
		ASSERT(page == leastRecentlyPage);
		compareWriteInfo(pageContents[page], lru.find(leastRecentlyPage));
		i += 1;
	}
	return Void();
}