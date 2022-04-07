/*
 * Deque.cpp
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

#include "flow/UnitTest.h"
#include "flow/Deque.h"

TEST_CASE("/flow/Deque/12345") {
	Deque<int> q;
	q.push_back(1);
	q.push_back(2);
	q.push_back(3);
	q.pop_front();
	q.push_back(4);
	q.pop_back();
	q.push_back(5);
	q.push_back(6);
	q.pop_front();
	q.pop_back();
	ASSERT(q.size() == 2 && q[0] == 3 && q[1] == 5);
	return Void();
}

TEST_CASE("/flow/Deque/queue") {
	std::queue<int, Deque<int>> q;

	int to_push = 0, to_pop = 0;
	while (to_pop != 1000) {
		if (to_push != 1000 && (q.empty() || deterministicRandom()->random01() < 0.55)) {
			q.push(to_push++);
		} else {
			ASSERT(q.front() == to_pop++);
			q.pop();
		}
	}
	ASSERT(q.empty());
	return Void();
}

TEST_CASE("/flow/Deque/max_size") {
	Deque<uint8_t> q;
	for (int i = 0; i < 10; i++)
		q.push_back(i);
	q.pop_front();
	for (int64_t i = 10; i <= q.max_size(); i++)
		q.push_back(i);
	for (int i = 0; i < 100; i++) {
		q.pop_front();
		q.push_back(1);
	}
	for (int i = 0; i < 100; i++)
		ASSERT(q[q.size() - 100 + i] == 1);
	for (int64_t i = 101; i <= q.max_size(); i++) {
		ASSERT(q[i - 101] == uint8_t(i));
	}
	ASSERT(&q.back() + 1 == &q.front());
	try {
		q.push_back(1);
		ASSERT(false);
	} catch (std::bad_alloc&) {
	}

	return Void();
}

struct RandomlyThrows {
	int data = 0;
	RandomlyThrows() = default;
	explicit RandomlyThrows(int data) : data(data) {}
	~RandomlyThrows() = default;
	RandomlyThrows(const RandomlyThrows& other) : data(other.data) { randomlyThrow(); }
	RandomlyThrows& operator=(const RandomlyThrows& other) {
		data = other.data;
		randomlyThrow();
		return *this;
	}

private:
	void randomlyThrow() {
		if (deterministicRandom()->random01() < 0.1) {
			throw success();
		}
	}
};

TEST_CASE("/flow/Deque/grow_exception_safety") {
	Deque<RandomlyThrows> q;
	for (int i = 0; i < 100; ++i) {
		loop {
			try {
				q.push_back(RandomlyThrows{ i });
				break;
			} catch (Error& e) {
			}
		}
	}
	for (int i = 0; i < 100; ++i) {
		ASSERT(q[i].data == i);
	}
	return Void();
}

void forceLinkDequeTests() {}
