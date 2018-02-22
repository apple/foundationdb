/*
 * IndexedSet.cpp
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

// At the moment, this file just contains tests.  IndexedSet<> is a template
// and so all the important implementation is in the header file

#include "IndexedSet.h"
#include "IRandom.h"
#include "ThreadPrimitives.h"
#include <algorithm>
#include <set>
#include <string>
#include <cstring>
#include <deque>
#include "UnitTest.h"

template <class Node>
int ISGetHeight(Node* n){
	if (!n) return 0;
	int lh = ISGetHeight(n->child[0]);
	int rh = ISGetHeight(n->child[1]);
	return std::max(lh, rh) + 1;
}

void indent(int depth) {
	for(int i=0; i<depth; i++)
		printf(" ");
}

template <class T, class Metric>
std::pair<int, int> IndexedSet<T, Metric>::testonly_assertBalanced(typename IndexedSet<T, Metric>::Node* n, int depth, bool checkAVL) {
	/* An IndexedSet (sub)tree n has the following invariants:
		(1) BST invariant: Every descendant x of n->child[0] has x->data < n->data, and every descendant x of n->child[1] has x->data > n->data
		(2) Balance invariant: n->balance is the difference between the height (greatest distance to a descendant) of n->child[1] and the height of n->child[0]
		(3) AVL invariant: n->balance is -1, 0 or 1
		(4) Metric invariant: n->total is the sum of the metric value with which n and each of its descendants was inserted 
		(5) Parent invariant: Every child x of n has x->parent==n

	This function checks all of these for all descendants of n.  It assumes that every node was inserted with a metric of 3 in order to check the metric invariant.
	If checkAVL==false, it does not check the AVL invariant (since this is often temporarily broken and then restored during operations, this permits checking invariants e.g. before a rebalancing operation)
		*/
	if (!n && depth == 0) n = root;

	if (!n) { 
		return std::make_pair(0, 0);
	}
	bool ok = true;
	for(int i=0; i<2; i++) {
		if (n->child[i] && n->child[i]->parent != n) {
			indent(depth); printf("Parent check failed\n");
			ok = false;
		}
	}
	if (n->child[0] && !(n->child[0]->data < n->data)) {
		indent(depth); printf("Not a binary search tree\n");
		ok = false;
	}
	if (n->child[1] && !(n->data < n->child[1]->data)) {
		indent(depth); printf("Not a binary search tree\n");
		ok = false;
	}
	auto lp = testonly_assertBalanced(n->child[0], depth+1, checkAVL);
	auto rp = testonly_assertBalanced(n->child[1], depth + 1, checkAVL);
	int lh = lp.first;
	int rh = rp.first;
	if (n->balance != rh-lh){
		indent(depth); printf("Balance is incorrect %d %d %d (@%d)\n", n->balance, rh, lh, n->data);
		ok = false;
	}
	if (checkAVL && (n->balance < -1 || n->balance > 1)){
		indent(depth); printf("AVL invariant broken %d %d %d\n", n->balance, rh, lh);
		ok = false;
	}
	if (n->total != lp.second + rp.second + 3) {
		indent(depth); printf("Metric totals are wrong %d != %d + %d + 3\n", n->total, lp.second, rp.second);
		ok = false;
	}

	ASSERT(ok);
	return std::make_pair( std::max(lh, rh) + 1, n->total );
}

bool operator < (std::string const& l, const char* r) {
	return strcmp(l.c_str(), r)<0;
}
bool operator < (const char* l, std::string const& r) {
	return strcmp(l, r.c_str())<0;
}

/*IndexedSet<int,int> concat_unbalanced( IndexedSet<int,int> &&a, int v, IndexedSet<int,int> && b ) {
	IndexedSet<int,int>::Node* n = new IndexedSet<int,int>::Node( std::move(v), 1, NULL );
	n->child[0] = a.root; a.root = NULL;
	n->child[1] = b.root; b.root = NULL;
	if (n->child[0]) n->child[0]->parent = n;
	if (n->child[1]) n->child[1]->parent = n;
	n->balance = ISGetHeight( n->child[1] ) - ISGetHeight( n->child[0] );
	if (n->child[0]) n->total += n->child[0]->total;
	if (n->child[1]) n->total += n->child[1]->total;
	IndexedSet<int,int> s;
	s.root = n;
	return std::move(s);
}*/

TEST_CASE("flow/IndexedSet/erase 400k of 1M") {
	IndexedSet<int, int> is;
	for (int n = 0; n<1000000; n++)
		is.insert(n, 3);

	is.erase(is.lower_bound(300000), is.lower_bound(700001));

	is.testonly_assertBalanced();

	int count = 0;
	for (auto i : is) ++count;

	ASSERT(count*3 == is.sumTo(is.end()));

	return Void();
}

/*TEST_CASE("flow/IndexedSet/performance") {
	std::vector<int> x;
	for (int i = 0; i<1000000; i++)
		x.push_back(g_random->randomInt(0, 10000000));

	IndexedSet<int, int> is;
	double start = timer();
	for (int i = 0; i<x.size(); i++) {
		int t = x[i];
		is.insert(std::move(t), 3);
	}
	double end = timer();
	double kps = x.size() / 1000.0 / (end - start);
	printf("%0.1f Kinsert/sec\n", kps);
	ASSERT(kps >= 500);                                           //< Or something?

	start = timer();
	for (int i = 0; i<x.size(); i++)
		ASSERT(is.find(x[i]) != is.end());
	end = timer();
	kps = x.size() / 1000.0 / (end - start);
	printf("%0.1f Kfind/sec\n", kps);
	ASSERT(kps >= 500);

	{
		//std::set<int> ss;
		IndexedSet<int, NoMetric> ss;
		double start = timer();
		for (int i = 0; i<x.size(); i++) {
			int t = x[i];
			ss.insert(t, NoMetric());
		}
		double end = timer();
		printf("%0.1f Kinsert/sec (set)\n", x.size() / 1000.0 / (end - start));

		start = timer();
		for (int i = 0; i<x.size(); i++)
			ASSERT(ss.find(x[i]) != ss.end());
		end = timer();
		printf("%0.1f Kfind/sec\n", x.size() / 1000.0 / (end - start));
	}

	for (int i = 0; i<x.size(); i++)
		ASSERT(is.find(x[i]) != is.end());

	ASSERT(is.find(-6) == is.end());

	std::sort(x.begin(), x.end());
	x.resize(std::unique(x.begin(), x.end()) - x.begin());

	int i = 0;
	for (auto it = is.begin(); it != is.end(); ++it, ++i)
		ASSERT(*it == x[i]);
	ASSERT(i == x.size());

	is.testonly_assertBalanced();

	std::random_shuffle(x.begin(), x.end());
	start = timer();
	for (int i = 0; i<x.size(); i++) {
		is.erase(x[i]);
	}
	end = timer();
	is.testonly_assertBalanced();

	printf("%0.1f Kerase/sec\n", x.size() / 1000.0 / (end - start));
	is.testonly_assertBalanced();
	for (int i = 0; i<x.size() / 2; i++)
		ASSERT(is.find(x[i]) == is.end());
}*/

TEST_CASE("flow/IndexedSet/random ops") {
	for (int t = 0; t<100; t++) {
		IndexedSet<int, int> is;
		int rr = g_random->randomInt(0, 600) * g_random->randomInt(0, 600);
		for (int n = 0; n<rr; n++) {
			if (g_random->random01() < (double)is.sumTo(is.end()) / rr * 2)
				is.erase(is.lower_bound(g_random->randomInt(0, 10000000)));
			else
				is.insert(g_random->randomInt(0, 10000000), 3);
		}

		int b = g_random->randomInt(0, 10000000);
		//int e = b + g_random->randomInt(0, 10);
		int e = g_random->randomInt(0, 10000000);
		if (e<b) std::swap(b, e);
		auto ib = is.lower_bound(b);
		auto ie = is.lower_bound(e);

		int original_count = is.sumTo(is.end())/3;
		int original_incount = is.sumRange(ib, ie)/3;

		//printf("\n#%d Erasing %d of %d items\n", t, original_incount, original_count);

		auto before = timer();
		is.erase(ib, ie);
		auto erase_time = timer() - before;
		is.testonly_assertBalanced();

		int count = 0, incount = 0;
		for (auto i : is) {
			++count;
			if (i >= b && i < e) { 
				//printf("Remaining item: %d (%d - %d)\n", i, b, e); 
				incount++; 
			}
		}

		//printf("%d items remain, totalling %d\n", count, is.sumTo(is.end()));
		//printf("%d items remain in erased range\n", incount);
		//printf("Erase time: %f sec\n", erase_time);

		ASSERT(incount == 0);
		ASSERT(count == original_count - original_incount);
		ASSERT(is.sumTo(is.end()) == count*3);
	}
	return Void();
}

TEST_CASE("flow/IndexedSet/strings") {
	Map< std::string, int > myMap;
	std::map< std::string, int > aMap;
	myMap["Hello"] = 1;
	myMap["Planet"] = 5;
	for (auto i = myMap.begin(); i != myMap.end(); ++i)
		aMap[i->key] = i->value;

	ASSERT(myMap.find("Hello")->value == 1);
	ASSERT(myMap.find("World") == myMap.end());
	ASSERT(myMap["Hello"] == 1);

	auto a = myMap.upper_bound("A")->key;
	auto x = myMap.lower_bound("M")->key;

	ASSERT((a + x) == (std::string)"HelloPlanet");

	/* This was a performance test:

		double start = timer();
		volatile int tt=0;
		for(int i=0; i<1000000; i++)
		tt += myMap.find( "Hello" )->value;
		double end = timer();
		printf("%0.1f Map.KfindStr/sec\n", 1000000/1000.0/(end-start));

		start = timer();
		for(int i=0; i<1000000; i++)
		aMap.find( "Hello" );
		end = timer();
		printf("%0.1f std::map.KfindStr/sec\n", 1000000/1000.0/(end-start));
	*/

	return Void();
}

TEST_CASE("flow/IndexedSets/ints") {
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
	//test( std::distance( is.begin(), is.end() ) == 1 );

	// Only a single `10` should remain
	auto i = is.begin();
	ASSERT(i != is.end());
	ASSERT(*i == 10);
	++i;
	ASSERT(i == is.end());

	return Void();
}

TEST_CASE("flow/IndexedSet/data constructor and destructor calls match") {
	static int count;
	count = 0;
	struct Counter {
		int value;
		Counter(int value) : value(value) { count++; }
		~Counter() { count--; }
		Counter(const Counter& r) :value(r.value) { count++; }
		void operator=(const Counter& r) { value = r.value; }
		bool operator<(const Counter& r) const { return value < r.value; }
	};
	IndexedSet<Counter, NoMetric> mySet;
	for (int i = 0; i<1000000; i++) {
		mySet.insert(Counter(g_random->randomInt(0, 1000000)), NoMetric());
		mySet.erase(Counter(g_random->randomInt(0, 1000000)));
	}
	int count2 = 0;
	for (int i = 0; i<1000000; i++)
		count2 += mySet.count(Counter(i));
	ASSERT(count == count2);
	mySet.clear();
	ASSERT(count == 0);
	return Void();
}

TEST_CASE("flow/IndexedSet/comparison to std::set") {
	IndexedSet<int, int> is;
	std::set<int> ss;
	for (int i = 0; i<1000000; i++) {
		int p = g_random->randomInt(0, 2000000);
		is.insert(std::move(p), 1);
		ss.insert(p);
	}

	for (int i = 0; i<2000000; i++) {
		int p = i; //g_random->randomInt(0, 2000000);
		auto sit = ss.upper_bound(p);
		int snext = sit != ss.end() ? *sit : 2000000;
		auto iit = is.upper_bound(p);
		int inext = iit != is.end() ? *iit : 2000000;
		ASSERT(snext == inext);
		//if (snext != inext)
		//	printf("Fail %d %d %d\n", p, snext, inext);
		//else
		//	printf("OK %d %d %d\n", p, snext, inext);
	}
	return Void();
}

TEST_CASE("flow/IndexedSet/all numbers") {
	IndexedSet<int, int64_t> is;

	std::vector<int> allNumbers;
	for (int i = 0; i<1000000; i++)
		allNumbers.push_back(i);
	std::random_shuffle(allNumbers.begin(), allNumbers.end());

	for (int i = 0; i<allNumbers.size(); i++)
		is.insert(allNumbers[i], allNumbers[i]);

	ASSERT(is.sumTo(is.end()) == allNumbers.size()*(allNumbers.size() - 1) / 2);

	for (int i = 0; i<100000; i++) {
		int b = g_random->randomInt(1, (int)allNumbers.size());
		int64_t ntotal = int64_t(b)*(b - 1) / 2;
		int64_t nmax = int64_t(b + 1)*(b) / 2;
		int64_t n = ntotal;// + g_random->randomInt( 0, int(std::max<int64_t>(1<<30,nmax-ntotal)) );
		auto ii = is.index(n);
		int ib = ii != is.end() ? *ii : 1000000;
		ASSERT(ib == b);
		if (ib != b)
			printf("%s %lld %d %d %lld\n", ib == b ? "OK" : "ERROR", n, b, ib, is.sumTo(ii));
	}

	for (int i = 0; i<100000; i++) {
		int a = g_random->randomInt(0, (int)allNumbers.size());
		int b = g_random->randomInt(0, (int)allNumbers.size());
		if (a>b) std::swap(a, b);

		int64_t itotal = is.sumRange(a, b);
		//int ntotal = int64_t(b)*(b-1)/2 - int64_t(a)*(a-1)/2;
		int64_t ntotal = int64_t(b - a)*(a + b - 1) / 2;
		ASSERT(itotal == ntotal);
		if (itotal != ntotal)
			printf("%s %lld %lld\n", itotal == ntotal ? "OK" : "ERROR", ntotal, itotal);
	}

	//double a = timer();
	is.erase(is.begin(), is.end());
	//a = timer() - a;

	return Void();
}

void forceLinkIndexedSetTests() {}