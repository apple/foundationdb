/*
 * VersionedMap.h
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

#ifndef FDBCLIENT_VERSIONEDMAP_H
#define FDBCLIENT_VERSIONEDMAP_H
#pragma once

#include "flow/flow.h"
#include "flow/IndexedSet.h"
#include "fdbclient/FDBTypes.h"
#include "flow/IRandom.h"
#include "fdbclient/VersionedMap.actor.h"

// PTree is a persistent balanced binary tree implementation. It is based on a treap as a way to guarantee O(1) space
// for node insertion (rotating is asymptotically cheap), but the constant factors are very large.
//
// Each node has three pointers - the first two are its left and right children, respectively, and the third can be set
// to point to a newer version of the node. This third pointer allows us to maintain persistence without full path
// copying, and is employed to achieve O(1) space node insertion.
//
// PTree also supports efficient finger searches.
namespace PTreeImpl {

#ifdef _MSC_VER
#pragma warning(disable : 4800)
#endif

template <class T>
struct PTree : public ReferenceCounted<PTree<T>>, FastAllocated<PTree<T>>, NonCopyable {
	uint32_t priority;
	Reference<PTree> pointer[3];
	Version lastUpdateVersion;
	bool updated;
	bool replacedPointer;
	T data;

	const Reference<PTree>& child(bool which, Version at) const {
		if (updated && lastUpdateVersion <= at && which == replacedPointer)
			return pointer[2];
		else
			return pointer[which];
	}
	const Reference<PTree>& left(Version at) const { return child(false, at); }
	const Reference<PTree>& right(Version at) const { return child(true, at); }

	PTree(const T& data, Version ver) : lastUpdateVersion(ver), updated(false), data(data) {
		priority = deterministicRandom()->randomUInt32();
	}
	PTree(uint32_t pri, T const& data, Reference<PTree> const& left, Reference<PTree> const& right, Version ver)
	  : priority(pri), lastUpdateVersion(ver), updated(false), data(data) {
		pointer[0] = left;
		pointer[1] = right;
	}

private:
	PTree(PTree const&);
};

template <class T>
class PTreeFinger {
public:
	// This finger size supports trees with up to exp(96/4.3) ~= 4,964,514,749 entries.
	// The number 4.3 comes from here: https://en.wikipedia.org/wiki/Random_binary_tree#The_longest_path
	// see also: check().
	static constexpr size_t kFingerSizeUpperBound = 96;

private:
	using PTreeFingerEntry = PTree<T> const*;
	PTreeFingerEntry entries_[kFingerSizeUpperBound];
	size_t size_ = 0;
	size_t bound_sz_ = 0;

public:
	PTreeFinger() {}

	// Explicit copy constructors ensure we copy the live values in entries_.
	PTreeFinger(PTreeFinger const& f) { *this = f; }
	PTreeFinger(PTreeFinger&& f) { *this = f; }

	PTreeFinger& operator=(PTreeFinger const& f) {
		size_ = f.size_;
		bound_sz_ = f.bound_sz_;
		std::copy(f.entries_, f.entries_ + size_, entries_);
		return *this;
	}

	PTreeFinger& operator=(PTreeFinger&& f) {
		size_ = std::exchange(f.size_, 0);
		bound_sz_ = f.bound_sz_;
		std::copy(f.entries_, f.entries_ + size_, entries_);
		return *this;
	}

	size_t size() const { return size_; }
	PTree<T> const* back() const { return entries_[size_ - 1]; }
	void pop_back() { size_--; }
	void clear() { size_ = 0; }
	PTree<T> const* operator[](size_t i) const { return entries_[i]; }

	void resize(size_t sz) {
		size_ = sz;
		ASSERT(size_ < kFingerSizeUpperBound);
	}

	void push_back(PTree<T> const* node) {
		entries_[size_++] = { node };
		ASSERT(size_ < kFingerSizeUpperBound);
	}

	void push_for_bound(PTree<T> const* node, bool less) {
		push_back(node);
		bound_sz_ = less ? size_ : bound_sz_;
	}

	// remove the end of the finger so that the last entry is less than the probe
	void trim_to_bound() { size_ = bound_sz_; }
};

template <class T>
static Reference<PTree<T>> update(Reference<PTree<T>> const& node,
                                  bool which,
                                  Reference<PTree<T>> const& ptr,
                                  Version at) {
	if (ptr.getPtr() == node->child(which, at).getPtr() /* && node->replacedVersion <= at*/) {
		return node;
	}
	if (node->lastUpdateVersion == at) {
		//&& (!node->updated || node->replacedPointer==which)) {
		if (node->updated && node->replacedPointer != which) {
			// We are going to have to copy this node, but its aux pointer will never be used again
			// and should drop its reference count
			Reference<PTree<T>> r;
			if (which)
				r = makeReference<PTree<T>>(node->priority, node->data, node->child(0, at), ptr, at);
			else
				r = makeReference<PTree<T>>(node->priority, node->data, ptr, node->child(1, at), at);
			node->pointer[2].clear();
			return r;
		} else {
			if (node->updated)
				node->pointer[2] = ptr;
			else
				node->pointer[which] = ptr;
			return node;
		}
	}
	if (node->updated) {
		if (which)
			return makeReference<PTree<T>>(node->priority, node->data, node->child(0, at), ptr, at);
		else
			return makeReference<PTree<T>>(node->priority, node->data, ptr, node->child(1, at), at);
	} else {
		node->lastUpdateVersion = at;
		node->replacedPointer = which;
		node->pointer[2] = ptr;
		node->updated = true;
		return node;
	}
}

template <class T, class X>
bool contains(const Reference<PTree<T>>& p, Version at, const X& x) {
	if (!p)
		return false;
	int cmp = compare(x, p->data);
	bool less = cmp < 0;
	if (cmp == 0)
		return true;
	return contains(p->child(!less, at), at, x);
}

// TODO: Remove the number of invocations of operator<, and replace with something closer to memcmp.
// and same for upper_bound.
template <class T, class X>
void lower_bound(const Reference<PTree<T>>& p, Version at, const X& x, PTreeFinger<T>& f) {
	if (!p) {
		f.trim_to_bound();
		return;
	}
	int cmp = compare(x, p->data);
	bool less = cmp < 0;
	f.push_for_bound(p.getPtr(), less);
	if (cmp == 0)
		return;
	lower_bound(p->child(!less, at), at, x, f);
}

template <class T, class X>
void upper_bound(const Reference<PTree<T>>& p, Version at, const X& x, PTreeFinger<T>& f) {
	if (!p) {
		f.trim_to_bound();
		return;
	}
	bool less = x < p->data;
	f.push_for_bound(p.getPtr(), less);
	upper_bound(p->child(!less, at), at, x, f);
}

template <class T, bool forward>
void move(Version at, PTreeFinger<T>& f) {
	ASSERT(f.size());
	const PTree<T>* n;
	n = f.back();
	if (n->child(forward, at)) {
		n = n->child(forward, at).getPtr();
		do {
			f.push_back(n);
			n = n->child(!forward, at).getPtr();
		} while (n);
	} else {
		do {
			n = f.back();
			f.pop_back();
		} while (f.size() && f.back()->child(forward, at).getPtr() == n);
	}
}

template <class T, bool forward>
int halfMove(Version at, PTreeFinger<T>& f) {
	// Post: f[:return_value] is the finger that would have been returned by move<forward>(at,f), and
	// f[:original_length_of_f] is unmodified
	ASSERT(f.size());
	const PTree<T>* n;
	n = f.back();
	if (n->child(forward, at)) {
		n = n->child(forward, at).getPtr();
		do {
			f.push_back(n);
			n = n->child(!forward, at).getPtr();
		} while (n);
		return f.size();
	} else {
		int s = f.size();
		do {
			n = f[s - 1];
			--s;
		} while (s && f[s - 1]->child(forward, at).getPtr() == n);
		return s;
	}
}

template <class T>
void next(Version at, PTreeFinger<T>& f) {
	move<T, true>(at, f);
}

template <class T>
void previous(Version at, PTreeFinger<T>& f) {
	move<T, false>(at, f);
}

template <class T>
int halfNext(Version at, PTreeFinger<T>& f) {
	return halfMove<T, true>(at, f);
}

template <class T>
int halfPrevious(Version at, PTreeFinger<T>& f) {
	return halfMove<T, false>(at, f);
}

template <class T>
T get(PTreeFinger<T>& f) {
	ASSERT(f.size());
	return f.back()->data;
}

// Modifies p to point to a PTree with x inserted
template <class T>
void insert(Reference<PTree<T>>& root, Version at, const T& x) {
	// Construct a finger to the insertion point, tracking the directions taken
	PTreeFinger<T> finger;
	bool direction[finger.kFingerSizeUpperBound];
	int directionEnd = 0;
	finger.push_back(root.getPtr());
	for (;;) {
		auto* back = finger.back();
		if (back == nullptr) {
			break;
		}
		int c = ::compare(x, back->data);
		if (c == 0) {
			break;
		} else if (c > 0) {
			finger.push_back(back->right(at).getPtr());
			direction[directionEnd++] = true;
		} else {
			finger.push_back(back->left(at).getPtr());
			direction[directionEnd++] = false;
		}
	}

	// Perform insertion and propagate modifications (from insertion and rotations) to the root
	auto node = finger.back() != nullptr ? Reference<PTree<T>>::addRef(const_cast<PTree<T>*>(finger.back()))
	                                     : Reference<PTree<T>>();
	auto* before = node.getPtr();
	if (!node) {
		node = makeReference<PTree<T>>(x, at);
	} else {
		node = makeReference<PTree<T>>(node->priority, x, node->left(at), node->right(at), at);
	}
	for (;;) {
		if (before == node.getPtr()) {
			// Done propagating copies
			return;
		}
		if (finger.size() == 1) {
			// Propagate copy to root
			root = std::move(node);
			return;
		}
		finger.pop_back();
		auto parent = Reference<PTree<T>>::addRef(const_cast<PTree<T>*>(finger.back()));
		const bool dir = direction[--directionEnd];
		// Prepare for next iteration
		node = update(parent, dir, node, at);
		if (node->child(dir, at)->priority > node->priority) {
			rotate(node, at, !dir);
		}
	}
}

template <class T>
Reference<PTree<T>> firstNode(const Reference<PTree<T>>& p, Version at) {
	if (!p)
		ASSERT(false);
	if (!p->left(at))
		return p;
	return firstNode(p->left(at), at);
}

template <class T>
Reference<PTree<T>> lastNode(const Reference<PTree<T>>& p, Version at) {
	if (!p)
		ASSERT(false);
	if (!p->right(at))
		return p;
	return lastNode(p->right(at), at);
}

template <class T, bool last>
void firstOrLastFinger(const Reference<PTree<T>>& p, Version at, PTreeFinger<T>& f) {
	if (!p)
		return;
	f.push_back(p.getPtr());
	firstOrLastFinger<T, last>(p->child(last, at), at, f);
}

template <class T>
void first(const Reference<PTree<T>>& p, Version at, PTreeFinger<T>& f) {
	return firstOrLastFinger<T, false>(p, at, f);
}

template <class T>
void last(const Reference<PTree<T>>& p, Version at, PTreeFinger<T>& f) {
	return firstOrLastFinger<T, true>(p, at, f);
}

// modifies p to point to a PTree with the root of p removed
template <class T>
void removeRoot(Reference<PTree<T>>& p, Version at) {
	if (!p->right(at))
		p = p->left(at);
	else if (!p->left(at))
		p = p->right(at);
	else {
		bool direction = p->right(at)->priority < p->left(at)->priority;
		rotate(p, at, direction);
		Reference<PTree<T>> child = p->child(direction, at);
		removeRoot(child, at);
		p = update(p, direction, child, at);
	}
}

// changes p to point to a PTree with finger removed. p must be the root of the
// tree associated with finger.
//
// Invalidates finger.
template <class T>
void removeFinger(Reference<PTree<T>>& p, Version at, PTreeFinger<T> finger) {
	ASSERT_GT(finger.size(), 0);
	// Start at the end of the finger, remove, and propagate copies up along the
	// search path (finger) as needed.
	auto node = Reference<PTree<T>>::addRef(const_cast<PTree<T>*>(finger.back()));
	auto* before = node.getPtr();
	removeRoot(node, at);
	for (;;) {
		if (before == node.getPtr()) {
			// Done propagating copies
			return;
		}
		if (finger.size() == 1) {
			// Check we passed the correct root for this finger
			ASSERT(p.getPtr() == before);
			// Propagate copy to root
			p = node;
			return;
		}
		finger.pop_back();
		auto parent = Reference<PTree<T>>::addRef(const_cast<PTree<T>*>(finger.back()));
		bool isLeftChild = parent->left(at).getPtr() == before;
		bool isRightChild = parent->right(at).getPtr() == before;
		ASSERT(isLeftChild || isRightChild); // Corrupt finger?
		// Prepare for next iteration
		before = parent.getPtr();
		node = update(parent, isRightChild, node, at);
	}
}

// changes p to point to a PTree with x removed
template <class T, class X>
void remove(Reference<PTree<T>>& p, Version at, const X& x) {
	if (!p)
		ASSERT(false); // attempt to remove item not present in PTree
	int cmp = compare(x, p->data);
	if (cmp < 0) {
		Reference<PTree<T>> child = p->child(0, at);
		remove(child, at, x);
		p = update(p, 0, child, at);
	} else if (cmp > 0) {
		Reference<PTree<T>> child = p->child(1, at);
		remove(child, at, x);
		p = update(p, 1, child, at);
	} else {
		removeRoot(p, at);
	}
}

template <class T, class X>
void remove(Reference<PTree<T>>& p, Version at, const X& begin, const X& end) {
	if (!p)
		return;
	int beginDir, endDir;
	int beginCmp = compare(begin, p->data);
	if (beginCmp < 0)
		beginDir = -1;
	else if (beginCmp > 0)
		beginDir = +1;
	else
		beginDir = 0;
	if (!(p->data < end))
		endDir = -1;
	else
		endDir = +1;

	if (beginDir == endDir) {
		Reference<PTree<T>> child = p->child(beginDir == +1, at);
		remove(child, at, begin, end);
		p = update(p, beginDir == +1, child, at);
	} else {
		if (beginDir == -1) {
			Reference<PTree<T>> left = p->child(0, at);
			removeBeyond(left, at, begin, 1);
			p = update(p, 0, left, at);
		}
		if (endDir == +1) {
			Reference<PTree<T>> right = p->child(1, at);
			removeBeyond(right, at, end, 0);
			p = update(p, 1, right, at);
		}
		if (beginDir < endDir)
			removeRoot(p, at);
	}
}

template <class T, class X>
void removeBeyond(Reference<PTree<T>>& p, Version at, const X& pivot, bool dir) {
	if (!p)
		return;

	if ((p->data < pivot) ^ dir) {
		p = p->child(!dir, at);
		removeBeyond(p, at, pivot, dir);
	} else {
		Reference<PTree<T>> child = p->child(dir, at);
		removeBeyond(child, at, pivot, dir);
		p = update(p, dir, child, at);
	}
}

/*template<class T, class X>
void remove(Reference<PTree<T>>& p, Version at, const X& begin, const X& end) {
    Reference<PTree<T>> left, center, right;
    split(p, begin, left, center, at);
    split(center, end, center, right, at);
    p = append(left, right, at);
}*/

// inputs a PTree with the root node potentially violating the heap property
// modifies p to point to a valid PTree
template <class T>
void demoteRoot(Reference<PTree<T>>& p, Version at) {
	if (!p)
		ASSERT(false);

	uint32_t priority[2];
	for (int i = 0; i < 2; i++)
		if (p->child(i, at))
			priority[i] = p->child(i, at)->priority;
		else
			priority[i] = 0;

	bool higherDirection = priority[1] > priority[0];

	if (priority[higherDirection] < p->priority)
		return;

	// else, child(higherDirection) is a greater priority than us and the other child...
	rotate(p, at, !higherDirection);
	Reference<PTree<T>> child = p->child(!higherDirection, at);
	demoteRoot(child, at);
	p = update(p, !higherDirection, child, at);
}

template <class T>
Reference<PTree<T>> append(const Reference<PTree<T>>& left, const Reference<PTree<T>>& right, Version at) {
	if (!left)
		return right;
	if (!right)
		return left;

	Reference<PTree<T>> r = makeReference<PTree<T>>(lastNode(left, at)->data, at);
	if (EXPENSIVE_VALIDATION) {
		ASSERT(r->data < firstNode(right, at)->data);
	}
	Reference<PTree<T>> a = left;
	remove(a, at, r->data);

	r->pointer[0] = a;
	r->pointer[1] = right;
	demoteRoot(r, at);
	return r;
}

template <class T, class X>
void split(Reference<PTree<T>> p, const X& x, Reference<PTree<T>>& left, Reference<PTree<T>>& right, Version at) {
	if (!p) {
		left = Reference<PTree<T>>();
		right = Reference<PTree<T>>();
		return;
	}

	if (p->data < x) {
		left = p;
		Reference<PTree<T>> lr = left->right(at);
		split(lr, x, lr, right, at);
		left = update(left, 1, lr, at);
	} else {
		right = p;
		Reference<PTree<T>> rl = right->left(at);
		split(rl, x, left, rl, at);
		right = update(right, 0, rl, at);
	}
}

template <class T>
void rotate(Reference<PTree<T>>& n, Version at, bool right) {
	auto l = n->child(!right, at);
	n = update(l, right, update(n, !right, l->child(right, at), at), at);
	// Diagram for right = true
	//   n      l
	//  /        \
	// l    ->    n
	//  \        /
	//   x      x
}

template <class T>
void printTree(const Reference<PTree<T>>& p, Version at, int depth = 0) {
	if (p->left(at))
		printTree(p->left(at), at, depth + 1);
	for (int i = 0; i < depth; i++)
		printf("  ");
	// printf(":%s\n", describe(p->data.value.first).c_str());
	printf(":%s\n", describe(p->data.key).c_str());
	if (p->right(at))
		printTree(p->right(at), at, depth + 1);
}

template <class T>
void printTreeDetails(const Reference<PTree<T>>& p, int depth = 0) {
	// printf("Node %p (depth %d): %s\n", p.getPtr(), depth, describe(p->data.value.first).c_str());
	printf("Node %p (depth %d): %s\n", p.getPtr(), depth, describe(p->data.key).c_str());
	printf("  Left: %p\n", p->pointer[0].getPtr());
	printf("  Right: %p\n", p->pointer[1].getPtr());
	// if (p->pointer[2])
	if (p->updated)
		printf("  Version %lld %s: %p\n",
		       p->lastUpdateVersion,
		       p->replacedPointer ? "Right" : "Left",
		       p->pointer[2].getPtr());
	for (int i = 0; i < 3; i++)
		if (p->pointer[i])
			printTreeDetails(p->pointer[i], depth + 1);
}

/*static int depth(const Reference<PTree<int>>& p, Version at) {
    if (!p) return 0;
    int d1 = depth(p->left(at), at) + 1;
    int d2 = depth(p->right(at), at) + 1;
    return d1 > d2 ? d1 : d2;
}*/

template <class T>
void validate(const Reference<PTree<T>>& p, Version at, T* min, T* max, int& count, int& height, int depth = 0) {
	if (!p) {
		height = 0;
		return;
	}
	ASSERT((!min || *min <= p->data) && (!max || p->data <= *max));
	for (int i = 0; i < 2; i++) {
		if (p->child(i, at))
			ASSERT(p->child(i, at)->priority <= p->priority);
	}

	++count;
	int h1, h2;
	validate(p->left(at), at, min, &p->data, count, h1, depth + 1);
	validate(p->right(at), at, &p->data, max, count, h2, depth + 1);
	height = std::max(h1, h2) + 1;
}

template <class T>
void check(const Reference<PTree<T>>& p) {
	int count = 0, height;
	validate(p, (T*)0, (T*)0, count, height);
	if (count && height > 4.3 * log(double(count))) {
		// printf("height %d; count %d\n", height, count);
		ASSERT(false);
	}
}

// Remove pointers to any child nodes that have been updated at or before the given version
// This essentially gets rid of node versions that will never be read (beyond 5s worth of versions)
// TODO look into making this per-version compaction. (We could keep track of updated nodes at each version for example)
template <class T>
void compact(Reference<PTree<T>>& p, Version newOldestVersion) {
	if (!p) {
		return;
	}
	if (p->updated && p->lastUpdateVersion <= newOldestVersion) {
		/* If the node has been updated, figure out which pointer was replaced. And replace that pointer with the
		   updated pointer. Then we can get rid of the updated child pointer and then make room in the node for future
		   updates */
		auto which = p->replacedPointer;
		p->pointer[which] = p->pointer[2];
		p->updated = false;
		p->pointer[2] = Reference<PTree<T>>();
		// p->pointer[which] = Reference<PTree<T>>();
	}
	Reference<PTree<T>> left = p->left(newOldestVersion);
	Reference<PTree<T>> right = p->right(newOldestVersion);
	compact(left, newOldestVersion);
	compact(right, newOldestVersion);
}

} // namespace PTreeImpl

class ValueOrClearToRef {
public:
	static ValueOrClearToRef value(ValueRef const& v) { return ValueOrClearToRef(v, false); }
	static ValueOrClearToRef clearTo(KeyRef const& k) { return ValueOrClearToRef(k, true); }

	bool isValue() const { return !isClear; };
	bool isClearTo() const { return isClear; }

	ValueRef const& getValue() const {
		ASSERT(isValue());
		return item;
	};
	KeyRef const& getEndKey() const {
		ASSERT(isClearTo());
		return item;
	};

private:
	ValueOrClearToRef(StringRef item, bool isClear) : item(item), isClear(isClear) {}
	StringRef item;
	bool isClear;
};

// VersionedMap provides an interface to a partially persistent tree, allowing you to read the values at a particular
// version, create new versions, modify the current version of the tree, and forget versions prior to a specific
// version.
template <class K, class T>
class VersionedMap : NonCopyable {
	// private:
public:
	typedef PTreeImpl::PTree<MapPair<K, std::pair<T, Version>>> PTreeT;
	typedef PTreeImpl::PTreeFinger<MapPair<K, std::pair<T, Version>>> PTreeFingerT;
	typedef Reference<PTreeT> Tree;

	Version oldestVersion, latestVersion;

	// This deque keeps track of PTree root nodes at various versions. Since the
	// versions increase monotonically, the deque is implicitly sorted and hence
	// binary-searchable.
	std::deque<std::pair<Version, Tree>> roots;

	struct rootsComparator {
		bool operator()(const std::pair<Version, Tree>& value, const Version& key) { return (value.first < key); }
		bool operator()(const Version& key, const std::pair<Version, Tree>& value) { return (key < value.first); }
	};

	Tree const& getRoot(Version v) const {
		auto r = upper_bound(roots.begin(), roots.end(), v, rootsComparator());
		--r;
		return r->second;
	}

	// For each item in the versioned map, 4 PTree nodes are potentially allocated:
	static const int overheadPerItem = nextFastAllocatedSize(sizeof(PTreeT)) * 4;
	struct iterator;

	VersionedMap() : oldestVersion(0), latestVersion(0) { roots.emplace_back(0, Tree()); }
	VersionedMap(VersionedMap&& v) noexcept
	  : oldestVersion(v.oldestVersion), latestVersion(v.latestVersion), roots(std::move(v.roots)) {}
	void operator=(VersionedMap&& v) noexcept {
		oldestVersion = v.oldestVersion;
		latestVersion = v.latestVersion;
		roots = std::move(v.roots);
	}

	Version getLatestVersion() const { return latestVersion; }
	Version getOldestVersion() const { return oldestVersion; }

	// front element should be the oldest version in the deque, hence the next oldest should be at index 1
	Version getNextOldestVersion() const { return roots[1]->first; }

	void forgetVersionsBefore(Version newOldestVersion) {
		ASSERT(newOldestVersion <= latestVersion);
		auto r = upper_bound(roots.begin(), roots.end(), newOldestVersion, rootsComparator());
		auto upper = r;
		--r;
		// if the specified newOldestVersion does not exist, insert a new
		// entry-pair with newOldestVersion and the root from next lower version
		if (r->first != newOldestVersion) {
			r = roots.emplace(upper, newOldestVersion, getRoot(newOldestVersion));
		}

		UNSTOPPABLE_ASSERT(r->first == newOldestVersion);
		roots.erase(roots.begin(), r);
		oldestVersion = newOldestVersion;
	}

	Future<Void> forgetVersionsBeforeAsync(Version newOldestVersion, TaskPriority taskID = TaskPriority::DefaultYield) {
		ASSERT_LE(newOldestVersion, latestVersion);
		auto r = upper_bound(roots.begin(), roots.end(), newOldestVersion, rootsComparator());
		auto upper = r;
		--r;
		// if the specified newOldestVersion does not exist, insert a new
		// entry-pair with newOldestVersion and the root from next lower version
		if (r->first != newOldestVersion) {
			r = roots.emplace(upper, newOldestVersion, getRoot(newOldestVersion));
		}

		UNSTOPPABLE_ASSERT(r->first == newOldestVersion);

		std::vector<Tree> toFree;
		toFree.reserve(10000);
		auto newBegin = r;
		Tree* lastRoot = nullptr;
		for (auto root = roots.begin(); root != newBegin; ++root) {
			if (root->second) {
				if (lastRoot != nullptr && root->second == *lastRoot) {
					(*lastRoot).clear();
				}
				if (root->second->isSoleOwner()) {
					toFree.push_back(root->second);
				}
				lastRoot = &root->second;
			}
		}

		roots.erase(roots.begin(), newBegin);
		oldestVersion = newOldestVersion;
		return deferredCleanupActor(toFree, taskID);
	}

public:
	void createNewVersion(Version version) { // following sets and erases are into the given version, which may now be
		                                     // passed to at().  Must be called in monotonically increasing order.
		if (version > latestVersion) {
			latestVersion = version;
			Tree r = getRoot(version);
			roots.emplace_back(version, r);
		} else
			ASSERT(version == latestVersion);
	}

	// insert() and erase() invalidate atLatest() and all iterators into it
	void insert(const K& k, const T& t) { insert(k, t, latestVersion); }
	void insert(const K& k, const T& t, Version insertAt) {
		PTreeImpl::insert(
		    roots.back().second, latestVersion, MapPair<K, std::pair<T, Version>>(k, std::make_pair(t, insertAt)));
	}
	void erase(const K& begin, const K& end) { PTreeImpl::remove(roots.back().second, latestVersion, begin, end); }
	void erase(const K& key) { // key must be present
		PTreeImpl::remove(roots.back().second, latestVersion, key);
	}
	void erase(iterator const& item) { // iterator must be in latest version!
		ASSERT_EQ(item.at, latestVersion);
		PTreeImpl::removeFinger(roots.back().second, latestVersion, item.finger);
	}

	void printDetail() { PTreeImpl::printTreeDetails(roots.back().second, 0); }

	void printTree(Version at) { PTreeImpl::printTree(roots.back().second, at, 0); }

	void compact(Version newOldestVersion) {
		ASSERT(newOldestVersion <= latestVersion);
		// auto newBegin = roots.lower_bound(newOldestVersion);
		auto newBegin = lower_bound(roots.begin(), roots.end(), newOldestVersion, rootsComparator());
		for (auto root = roots.begin(); root != newBegin; ++root) {
			if (root->second)
				PTreeImpl::compact(root->second, newOldestVersion);
		}
		// printf("\nPrinting the tree at latest version after compaction.\n");
		// PTreeImpl::printTreeDetails(roots.back().second(), 0);
	}

	// for(auto i = vm.at(version).lower_bound(range.begin); i < range.end; ++i)
	struct iterator {
		explicit iterator(Tree const& root, Version at) : root(root), at(at) {}

		K const& key() const { return finger.back()->data.key; }
		Version insertVersion() const {
			return finger.back()->data.value.second;
		} // Returns the version at which the current item was inserted
		operator bool() const { return finger.size() != 0; }
		bool operator<(const K& key) const { return this->key() < key; }

		T const& operator*() { return finger.back()->data.value.first; }
		T const* operator->() { return &finger.back()->data.value.first; }
		void operator++() {
			if (finger.size())
				PTreeImpl::next(at, finger);
			else
				PTreeImpl::first(root, at, finger);
		}
		void operator--() {
			if (finger.size())
				PTreeImpl::previous(at, finger);
			else
				PTreeImpl::last(root, at, finger);
		}
		bool operator==(const iterator& r) const {
			if (finger.size() && r.finger.size())
				return finger.back() == r.finger.back();
			else
				return finger.size() == r.finger.size();
		}
		bool operator!=(const iterator& r) const {
			if (finger.size() && r.finger.size())
				return finger.back() != r.finger.back();
			else
				return finger.size() != r.finger.size();
		}

	private:
		friend class VersionedMap<K, T>;
		Tree root;
		Version at;
		PTreeFingerT finger;
	};

	class ViewAtVersion {
	public:
		ViewAtVersion(Tree const& root, Version at) : root(root), at(at) {}

		iterator begin() const {
			iterator i(root, at);
			PTreeImpl::first(root, at, i.finger);
			return i;
		}
		iterator end() const { return iterator(root, at); }

		// Returns x such that key==*x, or end()
		template <class X>
		iterator find(const X& key) const {
			iterator i(root, at);
			PTreeImpl::lower_bound(root, at, key, i.finger);
			if (i && i.key() == key)
				return i;
			else
				return end();
		}

		// Returns the smallest x such that *x>=key, or end()
		template <class X>
		iterator lower_bound(const X& key) const {
			iterator i(root, at);
			PTreeImpl::lower_bound(root, at, key, i.finger);
			return i;
		}

		// Returns the smallest x such that *x>key, or end()
		template <class X>
		iterator upper_bound(const X& key) const {
			iterator i(root, at);
			PTreeImpl::upper_bound(root, at, key, i.finger);
			return i;
		}

		// Returns the largest x such that *x<=key, or end()
		template <class X>
		iterator lastLessOrEqual(const X& key) const {
			iterator i(root, at);
			PTreeImpl::upper_bound(root, at, key, i.finger);
			--i;
			return i;
		}

		// Returns the largest x such that *x<key, or end()
		template <class X>
		iterator lastLess(const X& key) const {
			iterator i(root, at);
			PTreeImpl::lower_bound(root, at, key, i.finger);
			--i;
			return i;
		}

		void validate() {
			int count = 0, height = 0;
			PTreeImpl::validate<MapPair<K, std::pair<T, Version>>>(root, at, nullptr, nullptr, count, height);
			if (height > 100)
				TraceEvent(SevWarnAlways, "DiabolicalPTreeSize").detail("Size", count).detail("Height", height);
		}

	private:
		Tree root;
		Version at;
	};

	ViewAtVersion at(Version v) const {
		if (v == ::latestVersion) {
			return atLatest();
		}

		return ViewAtVersion(getRoot(v), v);
	}
	ViewAtVersion atLatest() const { return ViewAtVersion(roots.back().second, latestVersion); }

	bool isClearContaining(ViewAtVersion const& view, KeyRef key) {
		auto i = view.lastLessOrEqual(key);
		return i && i->isClearTo() && i->getEndKey() > key;
	}

	// TODO: getHistory?
};

#endif
