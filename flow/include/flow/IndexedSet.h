/*
 * IndexedSet.h
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

#ifndef FLOW_INDEXEDSET_H
#define FLOW_INDEXEDSET_H
#pragma once

#include "flow/Arena.h"
#include "flow/Platform.h"
#include "flow/FastAlloc.h"
#include "flow/Trace.h"
#include "flow/Error.h"

#include <deque>
#include <type_traits>
#include <vector>

// IndexedSet<T, Metric> is similar to a std::set<T>, with the following additional features:
//   - Each element in the set is associated with a value of type Metric
//   - sumTo() and sumRange() can report the sum of the metric values associated with a
//     contiguous range of elements in O(lg N) time
//   - index() can be used to find an element having a given sumTo() in O(lg N) time
//   - Search functions (find(), lower_bound(), etc) can accept a type comparable to T instead of T
//     (e.g. StringRef when T is std::string or Standalone<StringRef>).  This can save a lot of needless
//     copying at query time for read-mostly sets with string keys.
//   - the size() function is missing; if the metric being used is a count sumTo(end()) will do instead
// A number of STL compatibility features are missing and should be added as needed.
// T must define operator <, which must define a total order.  Unlike std::set,
//     a user-defined predicate is not currently supported as a template parameter.
// Metric is required to have operators + and - and <, and behavior is undefined if
//     the sum of metrics for all elements of a set overflows the Metric type.

// Map<Key,Value> is similar to a std::map<Key,Value>, except that it inherits the search key type
//     flexibility of IndexedSet<>, uses MapPair<Key,Value> by default instead of pair<Key,Value>
//     (use iterator->key instead of iterator->first), and uses FastAllocator for nodes.

template <class T>
class Future;

class Void;

class StringRef;

template <class T, class Metric>
struct IndexedSet {
	typedef T value_type;
	typedef T key_type;

private: // Forward-declare IndexedSet::Node because Clang is much stricter about this ordering.
	struct Node : FastAllocated<Node> {
		// Here, and throughout all code that indirectly instantiates a Node, we rely on forwarding
		// references so that we don't need to maintain the set of 2^arity lvalue and rvalue reference
		// combinations, but still take advantage of move constructors when available (or required).
		template <class T_, class Metric_>
		Node(T_&& data, Metric_&& m, Node* parent = 0)
		  : data(std::forward<T_>(data)), balance(0), total(std::forward<Metric_>(m)), parent(parent) {
			child[0] = child[1] = nullptr;
		}
		Node(Node const&) = delete;
		Node& operator=(Node const&) = delete;
		~Node() {
			delete child[0];
			delete child[1];
		}

		T data;
		signed char balance; // right height - left height
		Metric total; // this + child[0] + child[1]
		Node* child[2]; // left, right
		Node* parent;
	};

	template <bool isConst>
	struct IteratorImpl {
		typename std::conditional_t<isConst, const IndexedSet::Node, IndexedSet::Node>* node;

		explicit IteratorImpl<isConst>(const IteratorImpl<!isConst>& nonConstIter) : node(nonConstIter.node) {
			static_assert(isConst);
		}

		explicit IteratorImpl(decltype(node) n = nullptr) : node(n){};

		typename std::conditional_t<isConst, const T, T>& operator*() const { return node->data; }

		typename std::conditional_t<isConst, const T, T>* operator->() const { return &node->data; }

		void operator++();
		void decrementNonEnd();
		bool operator==(const IteratorImpl<isConst>& r) const { return node == r.node; }
		bool operator!=(const IteratorImpl<isConst>& r) const { return node != r.node; }
		// following two methods are for memory storage engine(KeyValueStoreMemory class) use only
		// in order to have same interface as radixtree
		typename std::conditional_t<isConst, const StringRef, StringRef>& getKey(uint8_t* dummyContent) const {
			return node->data.key;
		}
		typename std::conditional_t<isConst, const StringRef, StringRef>& getValue() const { return node->data.value; }
	};

	template <bool isConst>
	struct Impl {
		using NodeT = std::conditional_t<isConst, const Node, Node>;
		using IteratorT = IteratorImpl<isConst>;
		using SetT = std::conditional_t<isConst, const IndexedSet<T, Metric>, IndexedSet<T, Metric>>;

		static IteratorT begin(SetT&);

		template <bool constIterator>
		static IteratorImpl<isConst || constIterator> previous(SetT&, IteratorImpl<constIterator>);

		template <class M>
		static IteratorT index(SetT&, const M&);

		template <class Key>
		static IteratorT find(SetT&, const Key&);

		template <class Key>
		static IteratorT upper_bound(SetT&, const Key&);

		template <class Key>
		static IteratorT lower_bound(SetT&, const Key&);

		template <class Key>
		static IteratorT lastLessOrEqual(SetT&, const Key&);

		static IteratorT lastItem(SetT&);
	};

	using ConstImpl = Impl<true>;
	using NonConstImpl = Impl<false>;

public:
	using iterator = IteratorImpl<false>;
	using const_iterator = IteratorImpl<true>;

	IndexedSet() : root(nullptr){};
	~IndexedSet() { delete root; }
	IndexedSet(IndexedSet&& r) noexcept : root(r.root) { r.root = nullptr; }
	IndexedSet& operator=(IndexedSet&& r) noexcept {
		delete root;
		root = r.root;
		r.root = 0;
		return *this;
	}

	const_iterator begin() const { return ConstImpl::begin(*this); };
	iterator begin() { return NonConstImpl::begin(*this); };
	const_iterator cbegin() const { return begin(); }

	const_iterator end() const { return const_iterator{}; }
	iterator end() { return iterator{}; }
	const_iterator cend() const { return end(); }

	const_iterator previous(const_iterator i) const { return ConstImpl::previous(*this, i); }
	const_iterator previous(iterator i) const { return ConstImpl::previous(*this, const_iterator{ i }); }
	iterator previous(iterator i) { return NonConstImpl::previous(*this, i); }

	const_iterator lastItem() const { return ConstImpl::lastItem(*this); }
	iterator lastItem() { return NonConstImpl::lastItem(*this); }

	bool empty() const { return !root; }
	void clear() {
		delete root;
		root = nullptr;
	}
	void swap(IndexedSet& r) { std::swap(root, r.root); }

	// Place data in the set with the given metric.  If an item equal to data is already in the set and,
	//   replaceExisting == true, it will be overwritten (and its metric will be replaced)
	template <class T_, class Metric_>
	iterator insert(T_&& data, Metric_&& metric, bool replaceExisting = true);

	// Insert all items from data into set. All items will use metric. If an item equal to data is already in the set
	// and,
	//   replaceExisting == true, it will be overwritten (and its metric will be replaced). returns the number of items
	//   inserted.
	int insert(const std::vector<std::pair<T, Metric>>& data, bool replaceExisting = true);

	// Increase the metric for the given item by the given amount.  Inserts data into the set if it
	//   doesn't exist. Returns the new sum.
	template <class T_, class Metric_>
	Metric addMetric(T_&& data, Metric_&& metric);

	// Remove the data item, if any, which is equal to key
	template <class Key>
	void erase(const Key& key) {
		erase(find(key));
	}

	// Erase the indicated item.  No effect if item == end().
	// SOMEDAY: Return ++item
	void erase(iterator item);

	// Erase all data items x for which begin<=x<end
	template <class Key>
	void erase(const Key& begin, const Key& end) {
		erase(lower_bound(begin), lower_bound(end));
	}

	// Erase data items with a deferred (async) free process. The data structure has the items removed
	//  synchronously with the invocation of this method so any subsequent call will see this new state.
	template <class Key>
	Future<Void> eraseAsync(const Key& begin, const Key& end);

	// Erase the items in the indicated range.
	void erase(iterator begin, iterator end);

	// Erase data items with a deferred (async) free process. The data structure has the items removed
	//  synchronously with the invocation of this method so any subsequent call will see this new state.
	Future<Void> eraseAsync(iterator begin, iterator end);

	// Returns the number of items equal to key (either 0 or 1)
	template <class Key>
	int count(const Key& key) const {
		return find(key) != end();
	}

	// Returns x such that key==*x, or end()
	template <class Key>
	const_iterator find(const Key& key) const {
		return ConstImpl::find(*this, key);
	}

	template <class Key>
	iterator find(const Key& key) {
		return NonConstImpl::find(*this, key);
	}

	// Returns the smallest x such that *x>=key, or end()
	template <class Key>
	const_iterator lower_bound(const Key& key) const {
		return ConstImpl::lower_bound(*this, key);
	}

	template <class Key>
	iterator lower_bound(const Key& key) {
		return NonConstImpl::lower_bound(*this, key);
	};

	// Returns the smallest x such that *x>key, or end()
	template <class Key>
	const_iterator upper_bound(const Key& key) const {
		return ConstImpl::upper_bound(*this, key);
	}

	template <class Key>
	iterator upper_bound(const Key& key) {
		return NonConstImpl::upper_bound(*this, key);
	};

	// Returns the largest x such that *x<=key, or end()
	template <class Key>
	const_iterator lastLessOrEqual(const Key& key) const {
		return ConstImpl::lastLessOrEqual(*this, key);
	};

	template <class Key>
	iterator lastLessOrEqual(const Key& key) {
		return NonConstImpl::lastLessOrEqual(*this, key);
	}

	// Returns smallest x such that sumTo(x+1) > metric, or end()
	template <class M>
	const_iterator index(M const& metric) const {
		return ConstImpl::index(*this, metric);
	};

	template <class M>
	iterator index(M const& metric) {
		return NonConstImpl::index(*this, metric);
	}

	// Return the metric inserted with item x
	Metric getMetric(const_iterator x) const;
	Metric getMetric(iterator x) const { return getMetric(const_iterator{ x }); }

	// Return the sum of getMetric(x) for begin()<=x<to
	Metric sumTo(const_iterator to) const;
	Metric sumTo(iterator to) const { return sumTo(const_iterator{ to }); }

	// Return the sum of getMetric(x) for begin<=x<end
	Metric sumRange(const_iterator begin, const_iterator end) const { return sumTo(end) - sumTo(begin); }
	Metric sumRange(iterator begin, iterator end) const {
		return sumTo(const_iterator{ end }) - sumTo(const_iterator{ begin });
	}

	// Return the sum of getMetric(x) for all x s.t. begin <= *x && *x < end
	template <class Key>
	Metric sumRange(const Key& begin, const Key& end) const {
		return sumRange(lower_bound(begin), lower_bound(end));
	}

	// Return the amount of memory used by an entry in the IndexedSet
	constexpr static int getElementBytes() { return sizeof(Node); }

private:
	// Copy operations unimplemented.  SOMEDAY: Implement and make public.
	IndexedSet(const IndexedSet&);
	IndexedSet& operator=(const IndexedSet&);

	Node* root;

	Metric eraseHalf(Node* start, Node* end, int eraseDir, int& heightDelta, std::vector<Node*>& toFree);
	void erase(iterator begin, iterator end, std::vector<Node*>& toFree);

	void replacePointer(Node* oldNode, Node* newNode) {
		if (oldNode->parent)
			oldNode->parent->child[oldNode->parent->child[1] == oldNode] = newNode;
		else
			root = newNode;
		if (newNode)
			newNode->parent = oldNode->parent;
	}

	template <int direction, bool isConst>
	static void moveIteratorImpl(std::conditional_t<isConst, const Node, Node>*& node) {
		if (node->child[0 ^ direction]) {
			node = node->child[0 ^ direction];
			while (node->child[1 ^ direction])
				node = node->child[1 ^ direction];
		} else {
			while (node->parent && node->parent->child[0 ^ direction] == node)
				node = node->parent;
			node = node->parent;
		}
	}

	// direction 0 = left, 1 = right
	template <int direction>
	static void moveIterator(Node const*& node) {
		moveIteratorImpl<direction, true>(node);
	}
	template <int direction>
	static void moveIterator(Node*& node) {
		moveIteratorImpl<direction, false>(node);
	}

public: // but testonly
	std::pair<int, int> testonly_assertBalanced(Node* n = 0, int d = 0, bool a = true);
};

class NoMetric {
public:
	NoMetric() {}
	NoMetric(int) {} // NoMetric(1)
	NoMetric operator+(NoMetric const&) const { return NoMetric(); }
	NoMetric operator-(NoMetric const&) const { return NoMetric(); }
	bool operator<(NoMetric const&) const { return false; }
};

template <class Key, class Value>
class MapPair {
public:
	Key key;
	Value value;

	template <class Key_, class Value_>
	MapPair(Key_&& key, Value_&& value) : key(std::forward<Key_>(key)), value(std::forward<Value_>(value)) {}
	void operator=(MapPair const& rhs) {
		key = rhs.key;
		value = rhs.value;
	}
	MapPair(MapPair const& rhs) : key(rhs.key), value(rhs.value) {}

	MapPair(MapPair&& r) noexcept : key(std::move(r.key)), value(std::move(r.value)) {}
	void operator=(MapPair&& r) noexcept {
		key = std::move(r.key);
		value = std::move(r.value);
	}

	int compare(MapPair<Key, Value> const& r) const { return ::compare(key, r.key); }
	template <class CompatibleWithKey>
	int compare(CompatibleWithKey const& r) const {
		return ::compare(key, r);
	}
	bool operator<(MapPair<Key, Value> const& r) const { return key < r.key; }
	bool operator>(MapPair<Key, Value> const& r) const { return key > r.key; }
	bool operator<=(MapPair<Key, Value> const& r) const { return key <= r.key; }
	bool operator>=(MapPair<Key, Value> const& r) const { return key >= r.key; }
	bool operator==(MapPair<Key, Value> const& r) const { return key == r.key; }
	bool operator!=(MapPair<Key, Value> const& r) const { return key != r.key; }

	// private: MapPair( const MapPair& );
};

template <class Key, class Value, class CompatibleWithKey>
inline int compare(CompatibleWithKey const& l, MapPair<Key, Value> const& r) {
	return compare(l, r.key);
}

template <class Key, class Value>
inline MapPair<typename std::decay<Key>::type, typename std::decay<Value>::type> mapPair(Key&& key, Value&& value) {
	return MapPair<typename std::decay<Key>::type, typename std::decay<Value>::type>(std::forward<Key>(key),
	                                                                                 std::forward<Value>(value));
}

template <class Key, class Value, class CompatibleWithKey>
bool operator<(MapPair<Key, Value> const& l, CompatibleWithKey const& r) {
	return l.key < r;
}

template <class Key, class Value, class CompatibleWithKey>
bool operator<(CompatibleWithKey const& l, MapPair<Key, Value> const& r) {
	return l < r.key;
}

template <class Key, class Value, class Pair = MapPair<Key, Value>, class Metric = NoMetric>
class Map {
public:
	typedef typename IndexedSet<Pair, Metric>::iterator iterator;
	typedef typename IndexedSet<Pair, Metric>::const_iterator const_iterator;

	Map() {}
	const_iterator begin() const { return set.begin(); }
	iterator begin() { return set.begin(); }
	const_iterator cbegin() const { return begin(); }
	const_iterator end() const { return set.end(); }
	iterator end() { return set.end(); }
	const_iterator cend() const { return end(); }
	const_iterator lastItem() const { return set.lastItem(); }
	iterator lastItem() { return set.lastItem(); }
	const_iterator previous(const_iterator i) const { return set.previous(i); }
	iterator previous(iterator i) { return set.previous(i); }
	bool empty() const { return set.empty(); }

	Value& operator[](const Key& key) {
		iterator i = set.insert(Pair(key, Value()), Metric(1), false);
		return i->value;
	}

	Value& get(const Key& key, Metric m = Metric(1)) {
		iterator i = set.insert(Pair(key, Value()), m, false);
		return i->value;
	}

	iterator insert(const Pair& p, bool replaceExisting = true, Metric m = Metric(1)) {
		return set.insert(p, m, replaceExisting);
	}
	iterator insert(Pair&& p, bool replaceExisting = true, Metric m = Metric(1)) {
		return set.insert(std::move(p), m, replaceExisting);
	}
	int insert(const std::vector<std::pair<MapPair<Key, Value>, Metric>>& pairs, bool replaceExisting = true) {
		return set.insert(pairs, replaceExisting);
	}

	template <class KeyCompatible>
	void erase(KeyCompatible const& k) {
		set.erase(k);
	}
	void erase(iterator b, iterator e) { set.erase(b, e); }
	void erase(iterator x) { set.erase(x); }
	void clear() { set.clear(); }
	Metric size() const {
		static_assert(!std::is_same<Metric, NoMetric>::value, "size() on Map with NoMetric is not valid!");
		return sumTo(end());
	}

	template <class KeyCompatible>
	const_iterator find(KeyCompatible const& k) const {
		return set.find(k);
	}
	template <class KeyCompatible>
	iterator find(KeyCompatible const& k) {
		return set.find(k);
	}

	template <class KeyCompatible>
	const_iterator lower_bound(KeyCompatible const& k) const {
		return set.lower_bound(k);
	}
	template <class KeyCompatible>
	iterator lower_bound(KeyCompatible const& k) {
		return set.lower_bound(k);
	}

	template <class KeyCompatible>
	const_iterator upper_bound(KeyCompatible const& k) const {
		return set.upper_bound(k);
	}
	template <class KeyCompatible>
	iterator upper_bound(KeyCompatible const& k) {
		return set.upper_bound(k);
	}

	template <class KeyCompatible>
	const_iterator lastLessOrEqual(KeyCompatible const& k) const {
		return set.lastLessOrEqual(k);
	}
	template <class KeyCompatible>
	iterator lastLessOrEqual(KeyCompatible const& k) {
		return set.lastLessOrEqual(k);
	}

	template <class M>
	const_iterator index(M const& metric) const {
		return set.index(metric);
	}
	template <class M>
	iterator index(M const& metric) {
		return set.index(metric);
	}

	Metric getMetric(const_iterator x) const { return set.getMetric(x); }
	Metric getMetric(iterator x) const { return getMetric(const_iterator{ x }); }

	Metric sumTo(const_iterator to) const { return set.sumTo(to); }
	Metric sumTo(iterator to) const { return sumTo(const_iterator{ to }); }

	Metric sumRange(const_iterator begin, const_iterator end) const { return set.sumRange(begin, end); }
	Metric sumRange(iterator begin, iterator end) const { return set.sumRange(begin, end); }
	template <class KeyCompatible>
	Metric sumRange(const KeyCompatible& begin, const KeyCompatible& end) const {
		return set.sumRange(begin, end);
	}

	static int getElementBytes() { return IndexedSet<Pair, Metric>::getElementBytes(); }

	Map(Map&& r) noexcept : set(std::move(r.set)) {}
	void operator=(Map&& r) noexcept { set = std::move(r.set); }

	Future<Void> clearAsync();

private:
	Map(Map<Key, Value, Pair> const&); // unimplemented
	void operator=(Map<Key, Value, Pair> const&); // unimplemented

	IndexedSet<Pair, Metric> set;
};

/////////////////////// implementation //////////////////////////

template <class T, class Metric>
template <bool isConst>
void IndexedSet<T, Metric>::IteratorImpl<isConst>::operator++() {
	moveIterator<1>(node);
}

template <class T, class Metric>
template <bool isConst>
void IndexedSet<T, Metric>::IteratorImpl<isConst>::decrementNonEnd() {
	moveIterator<0>(node);
}

template <class Node>
void ISRotate(Node*& oldRootRef, int d) {
	Node* oldRoot = oldRootRef;
	Node* newRoot = oldRoot->child[1 - d];

	// metrics
	auto orTotal = oldRoot->total - newRoot->total;
	if (newRoot->child[d])
		orTotal = orTotal + newRoot->child[d]->total;
	newRoot->total = oldRoot->total;
	oldRoot->total = orTotal;

	// pointers
	oldRoot->child[1 - d] = newRoot->child[d];
	if (oldRoot->child[1 - d])
		oldRoot->child[1 - d]->parent = oldRoot;
	newRoot->child[d] = oldRoot;
	newRoot->parent = oldRoot->parent;
	oldRoot->parent = newRoot;
	oldRootRef = newRoot;
}

template <class Node>
void ISAdjustBalance(Node* root, int d, int bal) {
	Node* n = root->child[d];
	Node* nn = n->child[1 - d];

	if (!nn->balance)
		root->balance = n->balance = 0;
	else if (nn->balance == bal) {
		root->balance = -bal;
		n->balance = 0;
	} else {
		root->balance = 0;
		n->balance = bal;
	}
	nn->balance = 0;
}

template <class Node>
int ISRebalance(Node*& root) {
	// Pre: root is a tree having the BST, metric, and balance invariants but not (necessarily) the AVL invariant.
	// root->child[0] and root->child[1] are AVL. Post: root is an AVL tree with the same nodes Returns: the change in
	// height of root rebalance is O(1) if abs(root->balance)<=2, and probably O(log N) otherwise.  (The rare "still
	// unbalanced" recursion is hard to analyze)
	//
	// The documentation of this function will be referencing the following tree (where
	// nodes A, C, E, and G represent subtrees of unspecified height). Thus for each node X,
	// we know the value of balance(X), but not height(X).
	//
	// We will assume that balance(F) < 0 (so we will be rotating right).
	// Trees that rotate to the left will perform analagous operations.
	//
	//         F
	//       /   \
	//      B     G
	//     / \
	//    A   D
	//       / \
	//      C   E

	if (!root || (root->balance >= -1 && root->balance <= +1))
		return 0;

	int rebalanceDir = root->balance < 0; // 1 if rotating right, 0 if rotating left
	auto* n = root->child[1 - rebalanceDir]; // Node B
	int bal = rebalanceDir ? +1 : -1; // 1 if rotating right, -1 if rotating left
	int rootBal = root->balance;

	// Depending on the balance at B, we will be required to do one or two rotations.
	// If balance(B) <= 0, then we do only one rotation (the second of the two).
	//
	// In a tree where balance(B) == +1, we are required to do both rotations.
	// The result of the first rotation will be:
	//
	//          F
	//        /   \
	//       D     G
	//      / \
	//     B   E
	//    / \
	//   A   C
	//
	bool doubleRotation = n->balance == bal;
	if (doubleRotation) {
		int x = n->child[rebalanceDir]->balance; // balance of Node D
		ISRotate(root->child[1 - rebalanceDir], 1 - rebalanceDir); // Rotate at Node B

		// Change node pointed to by 'n' to prepare for the second rotation
		// After this first rotation, Node D will be the left child of the root
		n = root->child[1 - rebalanceDir];

		// Compute the balance at the new root node D' of our rotation
		// We know that height(A) == max(height(C), height(E)) because B had balance of +1
		// If height(E) >= height(C), then height(E) == height(A) and balance(D') = -1
		// Otherwise height(C) == height(E) + 1, and therefore balance(D') = -2
		n->balance = ((x == -bal) ? -2 : -1) * bal;

		// Compute the balance at the old root node B' of our rotation
		// As stated above, height(A) == max(height(C), height(E))
		// If height(C) >= height(E), then height(A) == height(C) and balance(B') = 0
		// Otherwise height(A) == height(E) == height(C) + 1, and therefore balance(B') = -1
		n->child[1 - rebalanceDir]->balance = ((x == bal) ? -1 : 0) * bal;
	}

	// At this point, we perform the "second" rotation (which may actually be the first
	// if the "first" rotation was not performed). The rotation that is performed is the
	// same for both trees, but the result will be different depending on which tree we
	// started with:
	//
	//   If unrotated:       If once rotated:
	//
	//         B                      D
	//       /   \                  /   \
	//      A     F                B     F
	//           / \              / \   / \
	//          D   G            A   C E   G
	//         / \
	//        C   E
	//
	// The documentation for this second rotation will be based on the unrotated original tree.

	// Compute the balance at the new root node B'.
	// balance(B') = 1 + max(height(D), height(G)) - height(A) = 1 + max(height(D) - height(A), height(G) - height(A))
	// balance(B') = 1 + max(balance(B), height(G) - height(A))
	//
	// Now, we must find height(G) - height(A):
	// If height(A) >= height(D) (i.e. balance(B) <= 0), then
	// height(G) - height(A) = height(G) - height(B) + 1 = balance(F) + 1
	//
	// Otherwise, height(A) = height(D) - balance(B) = height(B) - 1 - balance(B), so
	// height(G) - height(A) = height(G) - height(B) + 1 + balance(B) = balance(F) + 1 + balance(B)
	//
	// balance(B') = 1 + max(balance(B), balance(F) + 1 + max(balance(B), 0))
	//
	int nBal = n->balance * bal; // Direction corrected balance at Node B
	int newRootBalance = bal * (1 + std::max(nBal, bal * root->balance + 1 + std::max(nBal, 0)));

	// Compute the balance at the old root node F' (which becomes a child of the new root).
	// balance(F') = height(G) - height(D)
	//
	// If height(D) >= height(A) (i.e. balance(B) >= 0), then height(D) = height(B) - 1, so
	// balance(F') = height(G) - height(B) + 1 = balance(F) + 1
	//
	// Otherwise, height(D) = height(A) + balance(B) = height(B) - 1 + balance(B), so
	// balance(F') = height(G) - height(B) + 1 - balance(B) = balance(F) + 1 - balance(B)
	//
	// balance(F') = balance(F) + 1 - min(balance(B), 0)
	//
	int newChildBalance = root->balance + bal * (1 - std::min(nBal, 0));

	ISRotate(root, rebalanceDir);
	root->balance = newRootBalance;
	root->child[rebalanceDir]->balance = newChildBalance;

	// If the original tree is very unbalanced, the unbalance may have been "pushed" down into this subtree, so
	// recursively rebalance that if necessary.
	int childHeightChange = ISRebalance(root->child[rebalanceDir]);
	root->balance += childHeightChange * bal;

	newRootBalance *= bal;

	// Compute the change in height at the root
	// We will look at the single and double rotation cases separately
	//
	// If we did a single rotation, then height(A) >= height(D).
	// As a result, height(A) >= height(G) + 1; otherwise the tree would be balanced and we wouldn't do any rotations.
	//
	// Then the original height of the tree is height(A) + 2,
	// and the new height is max(height(D) + 2 + childHeightChange, height(A) + 1), so
	//
	// heightChange_single = max(height(D) + 2 + childHeightChange, height(A) + 1) - (height(A) + 2)
	// heightChange_single = max(height(D) - height(A) + childHeightChange, -1)
	// heightChange_single = max(balance(B) + childHeightChange, -1)
	//
	// If we did a double rotation, then height(D) = height(A) + 1 in the original tree.
	// As a result, height(D) >= height(G) + 1; otherwise the tree would be balanced and we wouldn't do any rotations.
	//
	// Then the original height of the tree is height(D) + 2,
	// and the new height is max(height(A), height(C), height(E), height(G)) + 2
	//
	// balance(B) == 1, so height(A) == max(height(C), height(E)).
	// Also, height(A) = height(D) - 1 >= height(G)
	// Therefore the new height is height(A) + 2
	//
	// heightChange_double = height(A) + 2 - (height(D) + 2)
	// heightChange_double = height(A) - height(D)
	// heightChange_double = -1
	//
	int heightChange = doubleRotation ? -1 : std::max(nBal + childHeightChange, -1);

	// If the root is still unbalanced, then it should at least be more balanced than before. Recursively rebalance the
	// root until we get a balanced tree.
	if (root->balance < -1 || root->balance > +1) {
		ASSERT(abs(root->balance) < abs(rootBal));
		heightChange += ISRebalance(root);
	}

	return heightChange;
}

template <class Node>
Node* ISCommonSubtreeRoot(Node* first, Node* last) {
	// Finds the smallest common subtree of first and last and returns its root node

	// Find the depth of first and last
	int firstDepth = 0, lastDepth = 0;
	for (auto f = first; f; f = f->parent)
		firstDepth++;
	for (auto f = last; f; f = f->parent)
		lastDepth++;

	// Traverse up the tree from the deeper of first and last until f and l are at the same depth
	auto f = first, l = last;
	for (int i = firstDepth; i > lastDepth; i--)
		f = f->parent;
	for (int i = lastDepth; i > firstDepth; i--)
		l = l->parent;

	// Traverse up from f and l simultaneously until we reach a common node
	while (f != l) {
		f = f->parent;
		l = l->parent;
	}

	return f;
}

template <class T, class Metric>
template <bool isConst>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::begin(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self) {
	NodeT* x = self.root;
	while (x && x->child[0])
		x = x->child[0];
	return IteratorT{ x };
}

template <class T, class Metric>
template <bool isConst>
template <bool constIterator>
typename IndexedSet<T, Metric>::template IteratorImpl<isConst || constIterator>
IndexedSet<T, Metric>::Impl<isConst>::previous(IndexedSet<T, Metric>::Impl<isConst>::SetT& self,
                                               IndexedSet<T, Metric>::IteratorImpl<constIterator> iter) {
	if (iter == self.end())
		return self.lastItem();

	moveIterator<0>(iter.node);
	return iter;
}

template <class T, class Metric>
template <bool isConst>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::lastItem(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self) {
	NodeT* x = self.root;
	while (x && x->child[1])
		x = x->child[1];
	return IteratorT{ x };
}

template <class T, class Metric>
template <class T_, class Metric_>
Metric IndexedSet<T, Metric>::addMetric(T_&& data, Metric_&& metric) {
	auto i = find(data);
	if (i == end()) {
		insert(std::forward<T_>(data), std::forward<Metric_>(metric));
		return metric;
	} else {
		Metric m = metric + getMetric(i);
		insert(std::forward<T_>(data), m);
		return m;
	}
}

template <class T, class Metric>
template <class T_, class Metric_>
typename IndexedSet<T, Metric>::iterator IndexedSet<T, Metric>::insert(T_&& data,
                                                                       Metric_&& metric,
                                                                       bool replaceExisting) {
	if (root == nullptr) {
		root = new Node(std::forward<T_>(data), std::forward<Metric_>(metric));
		return iterator{ root };
	}
	Node* t = root;
	int d; // direction
	// traverse to find insert point
	while (true) {
		int cmp = compare(data, t->data);
		if (cmp == 0) {
			Node* returnNode = t;
			if (replaceExisting) {
				t->data = std::forward<T_>(data);
				Metric delta = t->total;
				t->total = std::forward<Metric_>(metric);
				if (t->child[0])
					t->total = t->total + t->child[0]->total;
				if (t->child[1])
					t->total = t->total + t->child[1]->total;
				delta = t->total - delta;
				while (true) {
					t = t->parent;
					if (!t)
						break;
					t->total = t->total + delta;
				}
			}

			return iterator{ returnNode };
		}
		d = cmp > 0;
		Node* nextT = t->child[d];
		if (!nextT)
			break;
		t = nextT;
	}

	Node* newNode = new Node(std::forward<T_>(data), std::forward<Metric_>(metric), t);
	t->child[d] = newNode;

	while (true) {
		t->balance += d ? 1 : -1;
		t->total = t->total + metric;
		if (t->balance == 0)
			break;
		if (t->balance != 1 && t->balance != -1) {
			Node** parent = t->parent ? &t->parent->child[t->parent->child[1] == t] : &root;
			// assert( *parent == t );

			Node* n = t->child[d];
			int bal = d ? 1 : -1;
			if (n->balance == bal) {
				t->balance = n->balance = 0;
			} else {
				ISAdjustBalance(t, d, bal);
				ISRotate(t->child[d], d);
			}
			ISRotate(*parent, 1 - d);
			t = *parent;
			break;
		}
		if (!t->parent)
			break;

		d = t->parent->child[1] == t;
		t = t->parent;
	}
	while (true) {
		t = t->parent;
		if (!t)
			break;
		t->total = t->total + metric;
	}

	return iterator{ newNode };
}

template <class T, class Metric>
int IndexedSet<T, Metric>::insert(const std::vector<std::pair<T, Metric>>& dataVector, bool replaceExisting) {
	int num_inserted = 0;
	Node* blockStart = nullptr;
	Node* blockEnd = nullptr;

	for (int i = 0; i < dataVector.size(); ++i) {
		Metric metric = dataVector[i].second;
		T data = std::move(dataVector[i].first);

		int d = 1; // direction
		if (blockStart == nullptr || (blockEnd != nullptr && data >= blockEnd->data)) {
			blockEnd = nullptr;
			if (root == nullptr) {
				root = new Node(std::move(data), metric);
				num_inserted++;
				blockStart = root;
				continue;
			}

			Node* t = root;
			// traverse to find insert point
			bool foundNode = false;
			while (true) {
				int cmp = compare(data, t->data);
				d = cmp > 0;
				if (d == 0)
					blockEnd = t;
				if (cmp == 0) {
					Node* returnNode = t;
					if (replaceExisting) {
						num_inserted++;
						t->data = std::move(data);
						Metric delta = t->total;
						t->total = metric;
						if (t->child[0])
							t->total = t->total + t->child[0]->total;
						if (t->child[1])
							t->total = t->total + t->child[1]->total;
						delta = t->total - delta;
						while (true) {
							t = t->parent;
							if (!t)
								break;
							t->total = t->total + delta;
						}
					}

					blockStart = returnNode;
					foundNode = true;
					break;
				}
				Node* nextT = t->child[d];
				if (!nextT) {
					blockStart = t;
					break;
				}
				t = nextT;
			}

			if (foundNode)
				continue;
		}

		Node* t = blockStart;
		while (t->child[d]) {
			t = t->child[d];
			d = 0;
		}

		Node* newNode = new Node(std::move(data), metric, t);
		num_inserted++;

		t->child[d] = newNode;
		blockStart = newNode;

		while (true) {
			t->balance += d ? 1 : -1;
			t->total = t->total + metric;
			if (t->balance == 0)
				break;
			if (t->balance != 1 && t->balance != -1) {
				Node** parent = t->parent ? &t->parent->child[t->parent->child[1] == t] : &root;
				// assert( *parent == t );

				Node* n = t->child[d];
				int bal = d ? 1 : -1;
				if (n->balance == bal) {
					t->balance = n->balance = 0;
				} else {
					ISAdjustBalance(t, d, bal);
					ISRotate(t->child[d], d);
				}
				ISRotate(*parent, 1 - d);
				t = *parent;
				break;
			}
			if (!t->parent)
				break;

			d = t->parent->child[1] == t;
			t = t->parent;
		}
		while (true) {
			t = t->parent;
			if (!t)
				break;
			t->total = t->total + metric;
		}
	}
	return num_inserted;
}

template <class T, class Metric>
Metric IndexedSet<T, Metric>::eraseHalf(Node* start,
                                        Node* end,
                                        int eraseDir,
                                        int& heightDelta,
                                        std::vector<Node*>& toFree) {
	// Removes all nodes between start (inclusive) and end (exclusive) from the set, where start is equal to end or one
	// of its descendants eraseDir 1 means erase the right half (nodes > at) of the left subtree of end.  eraseDir 0
	// means the left half of the right subtree toFree is extended with the roots of completely removed subtrees
	// heightDelta will be set to the change in height of the end node
	// Returns the amount that should be subtracted from end node's metric value (and, by extension, the metric values
	// of all ancestors of the end node).
	//
	// The end node may be left unbalanced (AVL invariant broken)
	// The end node may be left with the incorrect metric total (the correct value is end->total = end->total +
	// metricDelta) scare quotes in comments mean the values when eraseDir==1 (when eraseDir==0, "left" means right etc)

	// metricDelta measures how much should be subtracted from the current node's metrics
	Metric metricDelta = 0;
	heightDelta = 0;

	int fromDir = 1 - eraseDir;

	// Begin removing nodes at start continuing up until we get to end
	while (start != end) {
		start->total = start->total - metricDelta;

		IndexedSet<T, Metric>::Node* parent = start->parent;

		// Obtain the child pointer to start, which rebalance will update with the new root of the subtree currently
		// rooted at start
		IndexedSet<T, Metric>::Node*& node = parent->child[parent->child[1] == start];
		int nextDir = parent->child[1] == start;

		if (fromDir == eraseDir) {
			// The "right" subtree has been half-erased, and the "left" subtree doesn't need to be (nor does node).
			// But this node might be unbalanced by the shrinking "right" subtree. Rebalance and continue up.
			heightDelta += ISRebalance(node);
		} else {
			// The "left" subtree has been half-erased.  `start' and its "right" subtree will be completely erased,
			// leaving only the "left" subtree in its place (which is already AVL balanced).
			heightDelta += -1 - std::max<int>(0, node->balance * (eraseDir ? +1 : -1));
			metricDelta = metricDelta + start->total;

			// If there is a surviving subtree of start, then connect it to start->parent
			IndexedSet<T, Metric>::Node* n = node->child[fromDir];
			node = n; // This updates the appropriate child pointer of start->parent
			if (n) {
				metricDelta = metricDelta - n->total;
				n->parent = start->parent;
			}

			start->child[fromDir] = nullptr;
			toFree.push_back(start);
		}

		int dir = (nextDir ? +1 : -1);
		int oldBalance = parent->balance;

		// The change in height from removing nodes should never increase our height
		ASSERT(heightDelta <= 0);
		parent->balance += heightDelta * dir;

		// Compute the change in height of start's parent based on its change in balance.
		// Because we can only be (possibly) shrinking one subtree of parent:
		//   If we were originally heavier on the shrunken size (oldBalance * dir > 0), then the change in height is at
		//   most abs(oldBalance) == oldBalance * dir. If we were lighter on the shrunken side, then height cannot
		//   change.
		int maxHeightChange = std::max(oldBalance * dir, 0);
		int balanceChange = (oldBalance - parent->balance) * dir;
		heightDelta = -std::min(maxHeightChange, balanceChange);

		start = parent;
		fromDir = nextDir;
	}

	return metricDelta;
}

template <class T, class Metric>
void IndexedSet<T, Metric>::erase(typename IndexedSet<T, Metric>::iterator begin,
                                  typename IndexedSet<T, Metric>::iterator end,
                                  std::vector<Node*>& toFree) {
	// Removes all nodes in the set between first and last, inclusive.
	// toFree is extended with the roots of completely removed subtrees.

	ASSERT(!end.node || (begin.node && (::compare(*begin, *end) <= 0)));

	if (begin == end)
		return;

	IndexedSet<T, Metric>::Node* first = begin.node;
	IndexedSet<T, Metric>::Node* last = previous(end).node;

	IndexedSet<T, Metric>::Node* subRoot = ISCommonSubtreeRoot(first, last);

	Metric metricDelta = 0;
	int leftHeightDelta = 0;
	int rightHeightDelta = 0;

	// Erase all matching nodes that descend from subRoot, by first erasing descendants of subRoot->child[0] and then
	// erasing the descendants of subRoot->child[1] subRoot is not removed from the tree at this time
	metricDelta = metricDelta + eraseHalf(first, subRoot, 1, leftHeightDelta, toFree);
	metricDelta = metricDelta + eraseHalf(last, subRoot, 0, rightHeightDelta, toFree);

	// Change in the height of subRoot due to past activity, before subRoot is rebalanced. subRoot->balance already
	// reflects changes in height to its children.
	int heightDelta = leftHeightDelta + rightHeightDelta;

	// Rebalance and update metrics for all nodes from subRoot up to the root
	for (auto p = subRoot; p != nullptr; p = p->parent) {
		p->total = p->total - metricDelta;

		auto& pc = p->parent ? p->parent->child[p->parent->child[1] == p] : root;
		heightDelta += ISRebalance(pc);
		p = pc;

		// Update the balance and compute heightDelta for p->parent
		if (p->parent) {
			int oldb = p->parent->balance;
			int dir = (p->parent->child[1] == p ? +1 : -1);
			p->parent->balance += heightDelta * dir;

			heightDelta = (std::max(p->parent->balance * dir, 0) - std::max(oldb * dir, 0));
		}
	}

	// Erase the subRoot using the single node erase implementation
	erase(IndexedSet<T, Metric>::iterator(subRoot));
}

template <class T, class Metric>
void IndexedSet<T, Metric>::erase(iterator toErase) {
	Node* rebalanceNode;
	int rebalanceDir;

	{
		// Find the node to erase
		Node* t = toErase.node;
		if (!t)
			return;

		if (!t->child[0] || !t->child[1]) {
			Metric tMetric = t->total;
			if (t->child[0])
				tMetric = tMetric - t->child[0]->total;
			if (t->child[1])
				tMetric = tMetric - t->child[1]->total;
			for (Node* p = t->parent; p; p = p->parent)
				p->total = p->total - tMetric;
			rebalanceNode = t->parent;
			if (rebalanceNode)
				rebalanceDir = rebalanceNode->child[1] == t;
			int d = !t->child[0]; // Only one child, on this side (or no children!)
			replacePointer(t, t->child[d]);
			t->child[d] = 0;
			delete t;
		} else { // Remove node with two children
			Node* predecessor = t->child[0];
			while (predecessor->child[1])
				predecessor = predecessor->child[1];
			rebalanceNode = predecessor->parent;
			if (rebalanceNode == t)
				rebalanceNode = predecessor;
			if (rebalanceNode)
				rebalanceDir = rebalanceNode->child[1] == predecessor;

			Metric tMetric = t->total - t->child[0]->total - t->child[1]->total;
			if (predecessor->child[0])
				predecessor->total = predecessor->total - predecessor->child[0]->total;
			for (Node* p = predecessor->parent; p != t; p = p->parent)
				p->total = p->total - predecessor->total;
			for (Node* p = t->parent; p; p = p->parent)
				p->total = p->total - tMetric;

			// Replace t with predecessor
			replacePointer(predecessor, predecessor->child[0]);
			replacePointer(t, predecessor);
			predecessor->balance = t->balance;
			for (int i = 0; i < 2; i++) {
				Node* c = predecessor->child[i] = t->child[i];
				if (c) {
					c->parent = predecessor;
					predecessor->total = predecessor->total + c->total;
					t->child[i] = 0;
				}
			}
			delete t;
		}
	}

	if (!rebalanceNode)
		return;

	while (true) {
		rebalanceNode->balance += rebalanceDir ? -1 : +1;

		if (rebalanceNode->balance < -1 || rebalanceNode->balance > +1) {
			Node** parent = rebalanceNode->parent
			                    ? &rebalanceNode->parent->child[rebalanceNode->parent->child[1] == rebalanceNode]
			                    : &root;
			Node* n = rebalanceNode->child[1 - rebalanceDir];
			int bal = rebalanceDir ? +1 : -1;
			if (n->balance == -bal) {
				rebalanceNode->balance = n->balance = 0;
				ISRotate(*parent, rebalanceDir);
			} else if (n->balance == bal) {
				ISAdjustBalance(rebalanceNode, 1 - rebalanceDir, -bal);
				ISRotate(rebalanceNode->child[1 - rebalanceDir], 1 - rebalanceDir);
				ISRotate(*parent, rebalanceDir);
			} else { // n->balance == 0
				rebalanceNode->balance = -bal;
				n->balance = bal;
				ISRotate(*parent, rebalanceDir);
				break;
			}
			rebalanceNode = *parent;
		} else if (rebalanceNode->balance) // +/- 1, we are done
			break;

		if (!rebalanceNode->parent)
			break;
		rebalanceDir = rebalanceNode->parent->child[1] == rebalanceNode;
		rebalanceNode = rebalanceNode->parent;
	}
}

// Returns x such that key==*x, or end()
template <class T, class Metric>
template <bool isConst>
template <class Key>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::find(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self,
    const Key& key) {
	NodeT* t = self.root;
	while (t) {
		int cmp = compare(key, t->data);
		if (cmp == 0)
			return IteratorT{ t };
		t = t->child[cmp > 0];
	}
	return self.end();
}

// Returns the smallest x such that *x>=key, or end()
template <class T, class Metric>
template <bool isConst>
template <class Key>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::lower_bound(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self,
    const Key& key) {
	NodeT* t = self.root;
	if (!t)
		return self.end();
	bool less;
	while (true) {
		less = t->data < key;
		NodeT* n = t->child[less];
		if (!n)
			break;
		t = n;
	}

	if (less)
		moveIterator<1>(t);

	return IteratorT{ t };
}

// Returns the smallest x such that *x>key, or end()
template <class T, class Metric>
template <bool isConst>
template <class Key>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::upper_bound(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self,
    const Key& key) {
	NodeT* t = self.root;
	if (!t)
		return self.end();
	bool not_less;
	while (true) {
		not_less = !(key < t->data);
		NodeT* n = t->child[not_less];
		if (!n)
			break;
		t = n;
	}

	if (not_less)
		moveIterator<1>(t);

	return IteratorT{ t };
}

template <class T, class Metric>
template <bool isConst>
template <class Key>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::lastLessOrEqual(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self,
    const Key& key) {
	auto i = self.upper_bound(key);
	if (i == self.begin())
		return self.end();
	return self.previous(i);
}

// Returns first x such that metric < sum(begin(), x+1), or end()
template <class T, class Metric>
template <bool isConst>
template <class M>
typename IndexedSet<T, Metric>::template Impl<isConst>::IteratorT IndexedSet<T, Metric>::Impl<isConst>::index(
    IndexedSet<T, Metric>::Impl<isConst>::SetT& self,
    const M& metric) {
	M m = metric;
	NodeT* t = self.root;
	while (t) {
		if (t->child[0] && m < t->child[0]->total)
			t = t->child[0];
		else {
			m = m - t->total;
			if (t->child[1])
				m = m + t->child[1]->total;
			if (m < M())
				return IteratorT{ t };
			t = t->child[1];
		}
	}
	return self.end();
}

template <class T, class Metric>
Metric IndexedSet<T, Metric>::getMetric(typename IndexedSet<T, Metric>::const_iterator x) const {
	Metric m = x.node->total;
	for (int i = 0; i < 2; i++)
		if (x.node->child[i])
			m = m - x.node->child[i]->total;
	return m;
}

template <class T, class Metric>
Metric IndexedSet<T, Metric>::sumTo(typename IndexedSet<T, Metric>::const_iterator end) const {
	if (!end.node)
		return root ? root->total : Metric();

	Metric m = end.node->child[0] ? end.node->child[0]->total : Metric();
	for (const Node* p = end.node; p->parent; p = p->parent) {
		if (p->parent->child[1] == p) {
			m = m - p->total;
			m = m + p->parent->total;
		}
	}
	return m;
}

#include "flow/flow.h"
#include "flow/IndexedSet.actor.h"

template <class T, class Metric>
void IndexedSet<T, Metric>::erase(typename IndexedSet<T, Metric>::iterator begin,
                                  typename IndexedSet<T, Metric>::iterator end) {
	std::vector<IndexedSet<T, Metric>::Node*> toFree;
	erase(begin, end, toFree);

	ISFreeNodes(toFree, true);
}

template <class T, class Metric>
template <class Key>
Future<Void> IndexedSet<T, Metric>::eraseAsync(const Key& begin, const Key& end) {
	return eraseAsync(lower_bound(begin), lower_bound(end));
}

template <class T, class Metric>
Future<Void> IndexedSet<T, Metric>::eraseAsync(typename IndexedSet<T, Metric>::iterator begin,
                                               typename IndexedSet<T, Metric>::iterator end) {
	std::vector<IndexedSet<T, Metric>::Node*> toFree;
	erase(begin, end, toFree);

	return uncancellable(ISFreeNodes(toFree, false));
}

template <class Key, class Value, class Pair, class Metric>
Future<Void> Map<Key, Value, Pair, Metric>::clearAsync() {
	return set.eraseAsync(set.begin(), set.end());
}

#endif
