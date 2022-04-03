/*
 * IndexedSet.cpp
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

// At the moment, this file just contains tests.  IndexedSet<> is a template
// and so all the important implementation is in the header file

#include "contrib/fmt-8.1.1/include/fmt/format.h"
#include "flow/IndexedSet.h"
#include "flow/IRandom.h"
#include "flow/ThreadPrimitives.h"
#include <cinttypes>
#include <algorithm>
#include <set>
#include <string>
#include <cstring>
#include <deque>
#include <random>
#include <type_traits>
#include "flow/TreeBenchmark.h"
#include "flow/UnitTest.h"
template <class Node>
int ISGetHeight(Node* n) {
	if (!n)
		return 0;
	int lh = ISGetHeight(n->child[0]);
	int rh = ISGetHeight(n->child[1]);
	return std::max(lh, rh) + 1;
}

void indent(int depth) {
	for (int i = 0; i < depth; i++)
		printf(" ");
}

template <class T, class Metric>
std::pair<int, int> IndexedSet<T, Metric>::testonly_assertBalanced(typename IndexedSet<T, Metric>::Node* n,
                                                                   int depth,
                                                                   bool checkAVL) {
	/* An IndexedSet (sub)tree n has the following invariants:
	    (1) BST invariant: Every descendant x of n->child[0] has x->data < n->data, and every descendant x of
	n->child[1] has x->data > n->data (2) Balance invariant: n->balance is the difference between the height (greatest
	distance to a descendant) of n->child[1] and the height of n->child[0] (3) AVL invariant: n->balance is -1, 0 or 1
	    (4) Metric invariant: n->total is the sum of the metric value with which n and each of its descendants was
	inserted (5) Parent invariant: Every child x of n has x->parent==n

	This function checks all of these for all descendants of n.  It assumes that every node was inserted with a metric
	of 3 in order to check the metric invariant. If checkAVL==false, it does not check the AVL invariant (since this is
	often temporarily broken and then restored during operations, this permits checking invariants e.g. before a
	rebalancing operation)
	    */
	if (!n && depth == 0)
		n = root;

	if (!n) {
		return std::make_pair(0, 0);
	}
	bool ok = true;
	for (int i = 0; i < 2; i++) {
		if (n->child[i] && n->child[i]->parent != n) {
			indent(depth);
			printf("Parent check failed\n");
			ok = false;
		}
	}
	if (n->child[0] && !(n->child[0]->data < n->data)) {
		indent(depth);
		printf("Not a binary search tree\n");
		ok = false;
	}
	if (n->child[1] && !(n->data < n->child[1]->data)) {
		indent(depth);
		printf("Not a binary search tree\n");
		ok = false;
	}
	auto lp = testonly_assertBalanced(n->child[0], depth + 1, checkAVL);
	auto rp = testonly_assertBalanced(n->child[1], depth + 1, checkAVL);
	int lh = lp.first;
	int rh = rp.first;
	if (n->balance != rh - lh) {
		indent(depth);
		printf("Balance is incorrect %d %d %d (@%d)\n", n->balance, rh, lh, n->data);
		ok = false;
	}
	if (checkAVL && (n->balance < -1 || n->balance > 1)) {
		indent(depth);
		printf("AVL invariant broken %d %d %d\n", n->balance, rh, lh);
		ok = false;
	}
	if (n->total != lp.second + rp.second + 3) {
		indent(depth);
		printf("Metric totals are wrong %d != %d + %d + 3\n", n->total, lp.second, rp.second);
		ok = false;
	}

	ASSERT(ok);
	return std::make_pair(std::max(lh, rh) + 1, n->total);
}

bool operator<(std::string const& l, const char* r) {
	return strcmp(l.c_str(), r) < 0;
}
bool operator<(const char* l, std::string const& r) {
	return strcmp(l, r.c_str()) < 0;
}

/*IndexedSet<int,int> concat_unbalanced( IndexedSet<int,int> &&a, int v, IndexedSet<int,int> && b ) {
    IndexedSet<int,int>::Node* n = new IndexedSet<int,int>::Node( std::move(v), 1, nullptr );
    n->child[0] = a.root; a.root = nullptr;
    n->child[1] = b.root; b.root = nullptr;
    if (n->child[0]) n->child[0]->parent = n;
    if (n->child[1]) n->child[1]->parent = n;
    n->balance = ISGetHeight( n->child[1] ) - ISGetHeight( n->child[0] );
    if (n->child[0]) n->total += n->child[0]->total;
    if (n->child[1]) n->total += n->child[1]->total;
    IndexedSet<int,int> s;
    s.root = n;
    return std::move(s);
}*/

TEST_CASE("/flow/IndexedSet/erase 400k of 1M") {
	IndexedSet<int, int> is;
	for (int n = 0; n < 1000000; n++)
		is.insert(n, 3);

	is.erase(is.lower_bound(300000), is.lower_bound(700001));

	is.testonly_assertBalanced();

	int count = 0;
	for (auto iter = is.begin(); iter != is.end(); ++iter)
		++count;

	ASSERT(count * 3 == is.sumTo(is.end()));

	return Void();
}

TEST_CASE("/flow/IndexedSet/random ops") {
	for (int t = 0; t < 100; t++) {
		IndexedSet<int, int> is;
		int rr = deterministicRandom()->randomInt(0, 600) * deterministicRandom()->randomInt(0, 600);
		for (int n = 0; n < rr; n++) {
			if (deterministicRandom()->random01() < (double)is.sumTo(is.end()) / rr * 2)
				is.erase(is.lower_bound(deterministicRandom()->randomInt(0, 10000000)));
			else
				is.insert(deterministicRandom()->randomInt(0, 10000000), 3);
		}

		int b = deterministicRandom()->randomInt(0, 10000000);
		// int e = b + deterministicRandom()->randomInt(0, 10);
		int e = deterministicRandom()->randomInt(0, 10000000);
		if (e < b)
			std::swap(b, e);
		auto ib = is.lower_bound(b);
		auto ie = is.lower_bound(e);

		int original_count = is.sumTo(is.end()) / 3;
		int original_incount = is.sumRange(ib, ie) / 3;

		// printf("\n#%d Erasing %d of %d items\n", t, original_incount, original_count);

		is.erase(ib, ie);
		is.testonly_assertBalanced();

		int count = 0, incount = 0;
		for (auto i : is) {
			++count;
			if (i >= b && i < e) {
				// printf("Remaining item: %d (%d - %d)\n", i, b, e);
				incount++;
			}
		}

		// printf("%d items remain, totalling %d\n", count, is.sumTo(is.end()));
		// printf("%d items remain in erased range\n", incount);

		ASSERT(incount == 0);
		ASSERT(count == original_count - original_incount);
		ASSERT(is.sumTo(is.end()) == count * 3);
	}
	return Void();
}

TEST_CASE("/flow/IndexedSet/strings") {
	Map<std::string, int> myMap;
	std::map<std::string, int> aMap;
	myMap["Hello"] = 1;
	myMap["Planet"] = 5;
	for (auto i = myMap.begin(); i != myMap.end(); ++i)
		aMap[i->key] = i->value;

	ASSERT(myMap.find(std::string("Hello"))->value == 1);
	ASSERT(myMap.find(std::string("World")) == myMap.end());
	ASSERT(myMap["Hello"] == 1);

	auto a = myMap.upper_bound("A")->key;
	auto x = myMap.lower_bound("M")->key;

	ASSERT((a + x) == (std::string) "HelloPlanet");

	return Void();
}

template <typename K>
struct IndexedSetHarness {
	using map = IndexedSet<K, int>;
	using const_result = typename map::const_iterator;
	using result = typename map::iterator;
	using key_type = K;

	map s;

	void insert(K const& k) { s.insert(K(k), 1); }
	const_result find(K const& k) const { return s.find(k); }
	result find(K const& k) { return s.find(k); }
	const_result not_found() const { return s.end(); }
	result not_found() { return s.end(); }
	const_result begin() const { return s.begin(); }
	result begin() { return s.begin(); }
	const_result end() const { return s.end(); }
	result end() { return s.end(); }
	const_result lower_bound(K const& k) const { return s.lower_bound(k); }
	result lower_bound(K const& k) { return s.lower_bound(k); }
	const_result upper_bound(K const& k) const { return s.upper_bound(k); }
	result upper_bound(K const& k) { return s.upper_bound(k); }
	void erase(K const& k) { s.erase(k); }
};

TEST_CASE("performance/map/StringRef/IndexedSet") {
	Arena arena;

	IndexedSetHarness<StringRef> is;
	treeBenchmark(is, [&arena]() { return randomStr(arena); });

	return Void();
}

TEST_CASE("performance/map/StringRef/StdMap") {
	Arena arena;

	MapHarness<StringRef> is;
	treeBenchmark(is, [&arena]() { return randomStr(arena); });

	return Void();
}

TEST_CASE("performance/map/int/IndexedSet") {
	IndexedSetHarness<int> is;
	treeBenchmark(is, &randomInt);

	return Void();
}

TEST_CASE("performance/map/int/StdMap") {
	MapHarness<int> is;
	treeBenchmark(is, &randomInt);

	return Void();
}

TEST_CASE("performance/flow/IndexedSet/integers") {
	std::mt19937_64 urng(deterministicRandom()->randomUInt32());

	std::vector<int> x;
	x.reserve(1000000);
	for (int i = 0; i < 1000000; i++)
		x.push_back(deterministicRandom()->randomInt(0, 10000000));

	IndexedSet<int, int> is;
	double start = timer();
	for (int i = 0; i < x.size(); i++) {
		int t = x[i];
		is.insert(std::move(t), 3);
	}
	double end = timer();
	double kps = x.size() / 1000.0 / (end - start);
	printf("%0.1f Kinsert/sec\n", kps);

	start = timer();
	for (int i = 0; i < x.size(); i++)
		ASSERT(is.find(x[i]) != is.end());
	end = timer();
	kps = x.size() / 1000.0 / (end - start);
	printf("%0.1f Kfind/sec\n", kps);

	{
		// std::set<int> ss;
		IndexedSet<int, NoMetric> ss;
		double start = timer();
		for (int i = 0; i < x.size(); i++) {
			int t = x[i];
			ss.insert(t, NoMetric());
		}
		double end = timer();
		printf("%0.1f Kinsert/sec (set)\n", x.size() / 1000.0 / (end - start));

		start = timer();
		for (int i = 0; i < x.size(); i++)
			ASSERT(ss.find(x[i]) != ss.end());
		end = timer();
		printf("%0.1f Kfind/sec\n", x.size() / 1000.0 / (end - start));
	}

	for (int i = 0; i < x.size(); i++)
		ASSERT(is.find(x[i]) != is.end());

	ASSERT(is.find(-6) == is.end());

	std::sort(x.begin(), x.end());
	x.resize(std::unique(x.begin(), x.end()) - x.begin());

	int i = 0;
	for (auto it = is.begin(); it != is.end(); ++it, ++i)
		ASSERT(*it == x[i]);
	ASSERT(i == x.size());

	is.testonly_assertBalanced();

	std::shuffle(x.begin(), x.end(), urng);
	start = timer();
	for (int i = 0; i < x.size(); i++) {
		is.erase(x[i]);
	}
	end = timer();
	is.testonly_assertBalanced();

	printf("%0.1f Kerase/sec\n", x.size() / 1000.0 / (end - start));
	is.testonly_assertBalanced();
	for (int i = 0; i < x.size() / 2; i++) {
		ASSERT(is.find(x[i]) == is.end());
	}

	return Void();
}

TEST_CASE("performance/flow/IndexedSet/strings") {
	constexpr size_t count = 1000000;
	Map<std::string, int> myMap;
	std::map<std::string, int> aMap;
	double start, end;
	int tt = 0;

	std::string const hello{ "Hello" };
	myMap[hello] = 1;
	aMap["Hello"] = 1;

	start = timer();

	for (size_t i = 0; i < count; i++) {
		tt += myMap.find(hello)->value;
	}
	end = timer();

	ASSERT(tt == count);

	printf("%0.1f Map.KfindStr/sec\n", count / 1000.0 / (end - start));

	start = timer();
	for (size_t i = 0; i < count; i++) {
		aMap.find(hello);
	}
	end = timer();
	printf("%0.1f std::map.KfindStr/sec\n", count / 1000.0 / (end - start));

	return Void();
}

TEST_CASE("/flow/IndexedSets/ints") {
	IndexedSet<int, int> is;
	ASSERT(is.find(10) == is.end());
	is.insert(10, 3);
	is.testonly_assertBalanced();
	ASSERT(is.find(10) != is.end());
	ASSERT(is.find(20) == is.end());

	is.insert(20, 3);
	is.testonly_assertBalanced();
	ASSERT(is.find(10) != is.end());
	ASSERT(is.find(20) != is.end());

	for (int i = 20; i < 200; i += 10) {
		is.insert(std::move(i), 3);
		is.testonly_assertBalanced();
		ASSERT(is.find(i) != is.end());
	}

	for (int i = 10; i < 200; i += 10)
		ASSERT(is.find(i) != is.end());
	for (int i = 1; i < 200; i += 10)
		ASSERT(is.find(i) == is.end());

	for (int i = 20; i < 200; i += 10) {
		is.erase(i);
		is.testonly_assertBalanced();
	}
	ASSERT(is.find(10) != is.end());
	ASSERT(is.find(20) == is.end());
	// test( std::distance( is.begin(), is.end() ) == 1 );

	// Only a single `10` should remain
	auto i = is.begin();
	ASSERT(i != is.end());
	ASSERT(*i == 10);
	++i;
	ASSERT(i == is.end());

	return Void();
}

TEST_CASE("/flow/IndexedSet/data constructor and destructor calls match") {
	static int count;
	count = 0;
	struct Counter {
		int value;
		Counter(int value) : value(value) { count++; }
		~Counter() { count--; }
		Counter(const Counter& r) : value(r.value) { count++; }
		void operator=(const Counter& r) { value = r.value; }
		int compare(const Counter& r) const { return ::compare(value, r.value); }
		bool operator<(const Counter& r) const { return value < r.value; }
	};
	IndexedSet<Counter, NoMetric> mySet;
	for (int i = 0; i < 1000000; i++) {
		mySet.insert(Counter(deterministicRandom()->randomInt(0, 1000000)), NoMetric());
		mySet.erase(Counter(deterministicRandom()->randomInt(0, 1000000)));
	}
	int count2 = 0;
	for (int i = 0; i < 1000000; i++)
		count2 += mySet.count(Counter(i));
	ASSERT(count == count2);
	mySet.clear();
	ASSERT(count == 0);
	return Void();
}

TEST_CASE("/flow/IndexedSet/comparison to std::set") {
	IndexedSet<int, int> is;
	std::set<int> ss;
	for (int i = 0; i < 1000000; i++) {
		int p = deterministicRandom()->randomInt(0, 2000000);
		is.insert(std::move(p), 1);
		ss.insert(p);
	}

	for (int i = 0; i < 2000000; i++) {
		int p = i; // deterministicRandom()->randomInt(0, 2000000);
		auto sit = ss.upper_bound(p);
		int snext = sit != ss.end() ? *sit : 2000000;
		auto iit = is.upper_bound(p);
		int inext = iit != is.end() ? *iit : 2000000;
		ASSERT(snext == inext);
		// if (snext != inext)
		//	printf("Fail %d %d %d\n", p, snext, inext);
		// else
		//	printf("OK %d %d %d\n", p, snext, inext);
	}
	return Void();
}

TEST_CASE("/flow/IndexedSet/all numbers") {
	IndexedSet<int, int64_t> is;
	std::mt19937_64 urng(deterministicRandom()->randomUInt32());

	std::vector<int> allNumbers;
	allNumbers.reserve(1000000);
	for (int i = 0; i < 1000000; i++)
		allNumbers.push_back(i);
	std::shuffle(allNumbers.begin(), allNumbers.end(), urng);

	for (int i = 0; i < allNumbers.size(); i++)
		is.insert(allNumbers[i], allNumbers[i]);

	ASSERT(is.sumTo(is.end()) == allNumbers.size() * (allNumbers.size() - 1) / 2);

	for (int i = 0; i < 100000; i++) {
		int b = deterministicRandom()->randomInt(1, (int)allNumbers.size());
		int64_t ntotal = int64_t(b) * (b - 1) / 2;
		int64_t n = ntotal;
		auto ii = is.index(n);
		int ib = ii != is.end() ? *ii : 1000000;
		ASSERT(ib == b);
		if (ib != b) {
			fmt::print("{0} {1} {2} {3} {4} {5}\n", ib == b ? "OK" : "ERROR", n, b, ib, is.sumTo(ii));
		}
	}

	for (int i = 0; i < 100000; i++) {
		int a = deterministicRandom()->randomInt(0, (int)allNumbers.size());
		int b = deterministicRandom()->randomInt(0, (int)allNumbers.size());
		if (a > b)
			std::swap(a, b);

		int64_t itotal = is.sumRange(a, b);
		// int ntotal = int64_t(b)*(b-1)/2 - int64_t(a)*(a-1)/2;
		int64_t ntotal = int64_t(b - a) * (a + b - 1) / 2;
		ASSERT(itotal == ntotal);
		if (itotal != ntotal) {
			fmt::print("{0} {1} {2}\n", itotal == ntotal ? "OK" : "ERROR", ntotal, itotal);
		}
	}

	// double a = timer();
	is.erase(is.begin(), is.end());
	// a = timer() - a;

	return Void();
}

template <class T>
static constexpr bool is_const_ref_v = std::is_const_v<typename std::remove_reference_t<T>>;

TEST_CASE("/flow/IndexedSet/const_iterator") {
	struct Key {
		int key;
		explicit Key(int key) : key(key) {}
	};
	struct Metric {
		int metric;
		explicit Metric(int metric) : metric(metric) {}
	};

	IndexedSet<int, int64_t> is;
	for (int i = 0; i < 10; ++i)
		is.insert(i, 1);

	IndexedSet<int, int64_t>& ncis = is;
	static_assert(!is_const_ref_v<decltype(ncis)>);
	static_assert(!is_const_ref_v<decltype(*ncis.begin())>);
	static_assert(is_const_ref_v<decltype(*ncis.cbegin())>);
	static_assert(!is_const_ref_v<decltype(*ncis.previous(ncis.end()))>);
	static_assert(is_const_ref_v<decltype(*ncis.previous(ncis.cend()))>);
	static_assert(!is_const_ref_v<decltype(*ncis.index(Metric{ 5 }))>);
	static_assert(!is_const_ref_v<decltype(*ncis.find(Key{ 5 }))>);
	static_assert(!is_const_ref_v<decltype(*ncis.upper_bound(Key{ 5 }))>);
	static_assert(!is_const_ref_v<decltype(*ncis.lower_bound(Key{ 5 }))>);
	static_assert(!is_const_ref_v<decltype(*ncis.lastLessOrEqual(Key{ 5 }))>);
	static_assert(!is_const_ref_v<decltype(*ncis.lastItem())>);

	const IndexedSet<int, int64_t>& cis = is;
	static_assert(is_const_ref_v<decltype(cis)>);
	static_assert(is_const_ref_v<decltype(*cis.begin())>);
	static_assert(is_const_ref_v<decltype(*cis.cbegin())>);
	static_assert(is_const_ref_v<decltype(*cis.previous(cis.end()))>);
	static_assert(is_const_ref_v<decltype(*cis.previous(cis.cend()))>);
	static_assert(is_const_ref_v<decltype(*cis.previous(ncis.end()))>);
	static_assert(is_const_ref_v<decltype(*cis.previous(ncis.cend()))>);
	static_assert(is_const_ref_v<decltype(*cis.index(Metric{ 5 }))>);
	static_assert(is_const_ref_v<decltype(*cis.find(Key{ 5 }))>);
	static_assert(is_const_ref_v<decltype(*cis.upper_bound(Key{ 5 }))>);
	static_assert(is_const_ref_v<decltype(*cis.lower_bound(Key{ 5 }))>);
	static_assert(is_const_ref_v<decltype(*cis.lastLessOrEqual(Key{ 5 }))>);
	static_assert(is_const_ref_v<decltype(*cis.lastItem())>);

	for (auto& val : ncis) {
		static_assert(!is_const_ref_v<decltype(val)>);
	}
	for (const auto& val : ncis) {
		static_assert(is_const_ref_v<decltype(val)>);
	}
	for (auto& val : cis) {
		static_assert(is_const_ref_v<decltype(val)>);
	}

	return Void();
}
void forceLinkIndexedSetTests() {}
