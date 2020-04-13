/*
 * VersionedMap.h
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

#ifndef FDBCLIENT_VERSIONEDMAP_H
#define FDBCLIENT_VERSIONEDMAP_H
#pragma once

#include "flow/flow.h"
#include "flow/IndexedSet.h"
#include "fdbclient/FDBTypes.h"
#include "flow/IRandom.h"
#include "fdbclient/VersionedMap.actor.h"


// TODO: clean up the custom allocator code
template <class Object>
class FastAlloc
{
public:
	using value_type    = Object;

	FastAlloc() noexcept {}  // not required, unless used
	template <class U> FastAlloc(FastAlloc<U> const&) noexcept {}

	value_type*
	allocate(size_t s)
	{
		int size = (s*sizeof(value_type)) <= 64 ? 64 : nextFastAllocatedSize(s*sizeof(value_type));
		void* p = allocateFast(size);
		return static_cast<value_type*>(p);
	}

	void
	deallocate(const value_type* p, size_t s) const noexcept
	{
		int size = (s*sizeof(value_type)) <= 64 ? 64 : nextFastAllocatedSize(s*sizeof(value_type));
		freeFast(size, (void *)p);
	}

	template <class U>
	void
	destroy(U* p) noexcept
	{
		p->~U();
	}
};

template <class T, class U>
bool
operator==(FastAlloc<T> const&, FastAlloc<U> const&) noexcept
{
	return true;
}

template <class T, class U>
bool
operator!=(FastAlloc<T> const& x, FastAlloc<U> const& y) noexcept
{
	return !(x == y);
}

template <class Object>
class FastAllocPTree
{
public:
	using value_type    = Object;
	std::shared_ptr<int> totalSize;

	FastAllocPTree(std::shared_ptr<int> sz) : totalSize(sz){}
	template <class O>
	FastAllocPTree(O&& other)
		: totalSize(std::forward<O>(other).totalSize)
	{}
	FastAllocPTree() = delete;
	template <class O>
	FastAllocPTree& operator=(O&& other)
	{
		totalSize = std::forward<O>(other).totalSize;
		return *this;
	}

	value_type*
	allocate(size_t s)
	{
		int size = (s*sizeof(value_type)) <= 64 ? 64 : nextFastAllocatedSize(s*sizeof(value_type));
		void* p = allocateFast(size);
		// Bookkeeping: increment the in-use memory by size
		*totalSize = *totalSize + size;
		return static_cast<value_type*>(p);
	}

	void
	deallocate(value_type* p, std::size_t s) noexcept  // Use pointer if pointer is not a value_type*
	{
		int size = (s*sizeof(value_type)) <= 64 ? 64 : nextFastAllocatedSize(s*sizeof(value_type));
		freeFast(size, (void *)p);
		// Bookkeeping: decrement the in-use memory by size
		*totalSize = *totalSize - size;
	}

};

template <class T, class U>
bool
operator==(FastAllocPTree<T> const&, FastAllocPTree<U> const&) noexcept
{
	return true;
}

template <class T, class U>
bool
operator!=(FastAllocPTree<T> const& x, FastAllocPTree<U> const& y) noexcept
{
	return !(x == y);
}

// PTree is a persistent balanced binary tree implementation. It is based on a treap as a way to guarantee O(1) space for node insertion (rotating is asymptotically cheap),
// but the constant factors are very large.
//
// Each node has three pointers - the first two are its left and right children, respectively, and the third can be set to point to a newer version of the node. 
// This third pointer allows us to maintain persistence without full path copying, and is employed to achieve O(1) space node insertion.
//
// PTree also supports efficient finger searches.
namespace PTreeImpl {

#pragma warning(disable: 4800)

template<class T, class A = FastAlloc<T>>
struct PTree : public NonCopyable {
	uint32_t priority;
	Reference<PTree> pointer[3];
	Version lastUpdateVersion;
	bool updated;
	bool replacedPointer;
	T data;
	using PTreeNodeAllocType = typename std::allocator_traits<A>::template rebind_alloc<PTree<T, A>>;
	PTreeNodeAllocType allocator;

	Reference<PTree> child(bool which, Version at) const {
		if (updated && lastUpdateVersion<=at && which == replacedPointer)
			return pointer[2];
		else
			return pointer[which];
	}
	Reference<PTree> left(Version at) const { return child(false, at); }
	Reference<PTree> right(Version at) const { return child(true, at); }

	PTree(const T& data, Version ver, A alloc = A{}) : data(data), lastUpdateVersion(ver), updated(false), allocator(alloc) {
		priority = deterministicRandom()->randomUInt32();
	}
	PTree( uint32_t pri, T const& data, Reference<PTree> const& left, Reference<PTree> const& right, Version ver, A alloc=A{})  : priority(pri), data(data), lastUpdateVersion(ver), updated(false), allocator(alloc) {
		pointer[0] = left; pointer[1] = right;
	}
	~PTree()
	{
	}
	void addref() const { ++referenceCount; }
	void delref() const {
		if (delref_no_destroy())
		{
			PTreeNodeAllocType alloc = allocator;
			auto tmp = const_cast<PTree*>(this);
			tmp->~PTree();
			alloc.deallocate(tmp, 1);
		}
	}
	bool delref_no_destroy() const { return !--referenceCount; }
	bool isSoleOwner() const { return referenceCount == 1; }
private:
	PTree(PTree const&);
	mutable int32_t referenceCount = 1;
};

template<class T, class A>
using rebinded_alloc = typename std::allocator_traits<A>::template rebind_alloc<PTree<T, A>>;

template<class T, class A = FastAlloc<T>>
static Reference<PTree<T, A>> update( Reference<PTree<T, A>> const& node, bool which, Reference<PTree<T, A>> const& ptr, Version at, rebinded_alloc<T, A> allocator=A{} ) {
	if (ptr.getPtr() == node->child(which, at).getPtr()/* && node->replacedVersion <= at*/) {
		return node;
	}
	if (node->lastUpdateVersion == at) {
		//&& (!node->updated || node->replacedPointer==which)) {
		if (node->updated && node->replacedPointer != which) {
			// We are going to have to copy this node, but its aux pointer will never be used again
			// and should drop its reference count
			Reference<PTree<T, A>> r;
			if (which)
			{
				void *pnode = allocator.allocate(1);
				r = Reference<PTree<T,A>>( new (pnode) PTree<T,A>( node->priority, node->data, node->child(0, at), ptr, at, allocator ) );
			}else {
				void *pnode = allocator.allocate(1);
				r = Reference<PTree<T,A>>( new (pnode) PTree<T,A>( node->priority, node->data, ptr, node->child(1, at), at, allocator ) );
			}
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
	if ( node->updated ) {
		Reference<PTree<T, A>> r;
		if (which)
		{
			void *pnode = allocator.allocate(1);
			r = Reference<PTree<T,A>>( new (pnode) PTree<T, A>( node->priority, node->data, node->child(0, at), ptr, at, allocator  ) );
		}
		else
		{
			void *pnode = allocator.allocate(1);
			r = Reference<PTree<T,A>>( new (pnode) PTree<T, A>( node->priority, node->data, ptr, node->child(1, at), at, allocator ) );
		}
		return r;
	} else {
		node->lastUpdateVersion = at;
		node->replacedPointer = which;
		node->pointer[2] = ptr;
		node->updated = true;
		return node;
	}
}

template<class T, class X, class A = FastAlloc<T>>
bool contains(const Reference<PTree<T,A>>& p, Version at, const X& x) {
	if (!p) return false;
	bool less = x < p->data;
	if (!less && !(p->data<x)) return true;  // x == p->data
	return contains(p->child(!less, at), at, x);
}

template<class T, class X, class A = FastAlloc<T>>
void lower_bound(const Reference<PTree<T, A>>& p, Version at, const X& x, std::vector<const PTree<T, A>*>& f){
	if (!p) {
		while (f.size() && !(x < f.back()->data))
			f.pop_back();
		return;
	}
	f.push_back(p.getPtr());
	bool less = x < p->data;
	if (!less && !(p->data<x)) return;  // x == p->data
	lower_bound(p->child(!less, at), at, x, f);
}

template<class T, class X, class A = FastAlloc<T>>
void upper_bound(const Reference<PTree<T, A>>& p, Version at, const X& x, std::vector<const PTree<T, A>*>& f){
	if (!p) {
		while (f.size() && !(x < f.back()->data))
			f.pop_back();
		return;
	}
	f.push_back(p.getPtr());
	upper_bound(p->child(!(x < p->data), at), at, x, f);
}
	
template<class T, class A = FastAlloc<T>, bool forward>
void move(Version at, std::vector<const PTree<T, A>*>& f){
	ASSERT(f.size());
	const PTree<T, A> *n;
	n = f.back();
	if (n->child(forward, at)){
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

template<class T, class A = FastAlloc<T>, bool forward>
	int halfMove(Version at, std::vector<const PTree<T, A>*>& f) {
	// Post: f[:return_value] is the finger that would have been returned by move<forward>(at,f), and f[:original_length_of_f] is unmodified
	ASSERT(f.size());
	const PTree<T, A> *n;
	n = f.back();
	if (n->child(forward, at)){
		n = n->child(forward, at).getPtr();
		do {
			f.push_back(n);
			n = n->child(!forward, at).getPtr();
		} while (n);
		return f.size();
	} else {
		int s = f.size();
		do {
			n = f[s-1];
			--s;
		} while (s && f[s-1]->child(forward, at).getPtr() == n);
		return s;
	}
}

template<class T, class A = FastAlloc<T>>
void next(Version at, std::vector<const PTree<T,A>*>& f){
	move<T,A,true>(at, f);
}
	
template<class T, class A = FastAlloc<T>>
void previous(Version at, std::vector<const PTree<T,A>*>& f){
	move<T,A,false>(at, f);
}

template<class T, class A = FastAlloc<T>>
int halfNext(Version at, std::vector<const PTree<T,A>*>& f){
	return halfMove<T,A,true>(at, f);
}
	
template<class T, class A = FastAlloc<T>>
int halfPrevious(Version at, std::vector<const PTree<T, A>*>& f){
	return halfMove<T,A,false>(at, f);
}

template<class T, class A = FastAlloc<T>>
T get(std::vector<const PTree<T, A>*>& f){
	ASSERT(f.size());
	return f.back()->data;
}

// Modifies p to point to a PTree with x inserted
template<class T, class A = FastAlloc<T>>
void insert(Reference<PTree<T, A>>& p, Version at, const T& x, rebinded_alloc<T, A> allocator=A{} ) {
	if (!p){
		void * pnode = allocator.allocate(1);
		p = Reference<PTree<T, A>>(new (pnode) PTree<T, A>(x, at, allocator));
	} else {
		bool direction = !(x < p->data);
		Reference<PTree<T, A>> child = p->child(direction, at);
		insert(child, at, x, allocator);
		p = update(p, direction, child, at, allocator);
		if (p->child(direction, at)->priority > p->priority)
			rotate(p, at, !direction, allocator);
	}
}

template<class T, class A = FastAlloc<T>>
Reference<PTree<T,A>> firstNode(const Reference<PTree<T,A>>& p, Version at) {
	if (!p) ASSERT(false);
	if (!p->left(at)) return p;
	return firstNode(p->left(at), at);
}

template<class T, class A = FastAlloc<T>>
Reference<PTree<T,A>> lastNode(const Reference<PTree<T,A>>& p, Version at) {
	if (!p) ASSERT(false);
	if (!p->right(at)) return p;
	return lastNode(p->right(at), at);
}

template<class T, class A = FastAlloc<T>, bool last>
void firstOrLastFinger(const Reference<PTree<T,A>>& p, Version at, std::vector<const PTree<T,A>*>& f) {
	if (!p) return;
	f.push_back(p.getPtr());
	firstOrLastFinger<T, A, last>(p->child(last, at), at, f);
}
	
template<class T, class A = FastAlloc<T>>
void first(const Reference<PTree<T,A>>& p, Version at, std::vector<const PTree<T,A>*>& f) {
	return firstOrLastFinger<T, A, false>(p, at, f);
}

template<class T, class A = FastAlloc<T>>
void last(const Reference<PTree<T,A>>& p, Version at, std::vector<const PTree<T,A>*>& f) {
	return firstOrLastFinger<T, A, true>(p, at, f);
}

// modifies p to point to a PTree with the root of p removed
template<class T, class A = FastAlloc<T>>
void removeRoot(Reference<PTree<T, A>>& p, Version at, rebinded_alloc<T, A> allocator=A{} ) {
	if (!p->right(at))
		p = p->left(at);
	else if (!p->left(at))
		p = p->right(at);
	else {
		bool direction = p->right(at)->priority < p->left(at)->priority;
		rotate(p,at,direction, allocator);
		Reference<PTree<T, A>> child = p->child(direction, at);
		removeRoot(child, at, allocator);
		p = update(p, direction, child, at, allocator);
	}
}

// changes p to point to a PTree with x removed
template<class T, class X, class A = FastAlloc<T>>
void remove(Reference<PTree<T, A>>& p, Version at, const X& x, rebinded_alloc<T, A> allocator=A{} ) {
	if (!p) ASSERT(false); // attempt to remove item not present in PTree
	if (x < p->data) {
		Reference<PTree<T, A>> child = p->child(0, at);
		remove(child, at, x, allocator);
		p = update(p, 0, child, at, allocator);
	} else if (p->data < x) {
		Reference<PTree<T, A>> child = p->child(1, at);
		remove(child, at, x, allocator);
		p = update(p, 1, child, at, allocator);
	} else
		removeRoot(p, at, allocator);
}

template<class T, class X, class A = FastAlloc<T>>
void remove(Reference<PTree<T, A>>& p, Version at, const X& begin, const X& end, rebinded_alloc<T, A> allocator=A{} ) {
	if (!p) return;
	int beginDir, endDir;
	if (begin < p->data) beginDir = -1;
	else if (p->data < begin) beginDir = +1;
	else beginDir = 0;
	if (!(p->data < end)) endDir = -1;
	else endDir = +1;

	if (beginDir == endDir) {
		Reference<PTree<T, A>> child = p->child(beginDir==+1, at);
		remove( child, at, begin, end, allocator );
		p = update(p, beginDir==+1, child, at, allocator);
	} else {
		if (beginDir==-1) {
			Reference<PTree<T, A>> left = p->child(0, at);
			removeBeyond(left, at, begin, 1, allocator);
			p = update(p, 0, left, at, allocator);
		}
		if (endDir==+1) {
			Reference<PTree<T, A>> right = p->child(1, at);
			removeBeyond(right, at, end, 0, allocator);
			p = update(p, 1, right, at, allocator);
		}
		if (beginDir < endDir)
			removeRoot(p, at, allocator);
	}
}

template <class T, class X, class A = FastAlloc<T>>
void removeBeyond(Reference<PTree<T, A>>& p, Version at, const X& pivot, bool dir, rebinded_alloc<T, A> allocator=A{} ) {
	if (!p) return;

	if ( (p->data < pivot)^dir ) {
		p = p->child(!dir, at);
		removeBeyond(p, at, pivot, dir, allocator );
	} else {
		Reference<PTree<T, A>> child = p->child(dir, at);
		removeBeyond(child, at, pivot, dir, allocator);
		p = update(p, dir, child, at, allocator);
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
template<class T, class A = FastAlloc<T>>
void demoteRoot(Reference<PTree<T, A>>& p, Version at, rebinded_alloc<T, A> allocator=A{}){
	if (!p) ASSERT(false);

	uint32_t priority[2];
	for (int i=0;i<2;i++)
		if (p->child(i, at)) priority[i] = p->child(i, at)->priority;
		else priority[i] = 0;

	bool higherDirection = priority[1] > priority[0];

	if (priority[higherDirection] < p->priority) return;

	// else, child(higherDirection) is a greater priority than us and the other child...
	rotate(p, at, !higherDirection, allocator);
	Reference<PTree<T, A>> child = p->child(!higherDirection, at);
	demoteRoot(child, at, allocator);
	p = update(p, !higherDirection, child, at, allocator);
}

template<class T, class A = FastAlloc<T>>
Reference<PTree<T, A>> append(const Reference<PTree<T, A>>& left, const Reference<PTree<T, A>>& right, Version at, rebinded_alloc<T, A> allocator=A{} ) {
	if (!left) return right;
	if (!right) return left;

	void *pnode = allocator.allocate(1);
	Reference<PTree<T, A>> r = Reference<PTree<T, A>>(new (pnode) PTree<T, A>(lastNode(left, at)->data, at, allocator));
	ASSERT( r->data < firstNode(right, at)->data);
	Reference<PTree<T, A>> a = left;
	remove(a, at, r->data, allocator);

	r->pointer[0] = a;
	r->pointer[1] = right;
	demoteRoot(r, at, allocator);
	return r;
}

template<class T, class X, class A = FastAlloc<T>>
void split(Reference<PTree<T, A>> p, const X& x, Reference<PTree<T, A>>& left, Reference<PTree<T, A>>& right, Version at, rebinded_alloc<T, A> allocator=A{} ) {
	if (!p){
		left = Reference<PTree<T, A>>();
		right = Reference<PTree<T, A>>();
		return;
	}

	if (p->data < x){
		left = p;
		Reference<PTree<T, A>> lr = left->right(at);
		split(lr, x, lr, right, at, allocator);
		left = update(left, 1, lr, at, allocator);
	} else {
		right = p;
		Reference<PTree<T, A>> rl = right->left(at);
		split(rl, x, left, rl, at, allocator);
		right = update(right, 0, rl, at, allocator);
	}
}

template<class T, class A = FastAlloc<T>>
void rotate(Reference<PTree<T,A>>& p, Version at, bool right, rebinded_alloc<T, A> allocator=A{} ){
	auto r = p->child(!right, at);

	auto n1 = r->child(!right, at);
	auto n2 = r->child(right, at);
	auto n3 = p->child(right, at);

	auto newC = update( p, !right, n2, at, allocator );
	newC = update( newC, right, n3, at, allocator );
	p = update( r, !right, n1, at, allocator );
	p = update( p, right, newC, at, allocator );
}

template <class T, class A = FastAlloc<T>>
void printTree(const Reference<PTree<T,A>>& p, Version at, int depth = 0) {
	if (p->left(at)) printTree(p->left(at), at, depth+1);
	for (int i=0;i<depth;i++)
		printf("  ");
	//printf(":%s\n", describe(p->data.value.first).c_str());
	printf(":%s\n", describe(p->data.key).c_str());
	if (p->right(at)) printTree(p->right(at), at, depth+1);
}

template <class T, class A = FastAlloc<T>>
void printTreeDetails(const Reference<PTree<T,A>>& p, int depth = 0) {
	//printf("Node %p (depth %d): %s\n", p.getPtr(), depth, describe(p->data.value.first).c_str());
	printf("Node %p (depth %d): %s\n", p.getPtr(), depth, describe(p->data.key).c_str());
	printf("  Left: %p\n", p->pointer[0].getPtr());
	printf("  Right: %p\n", p->pointer[1].getPtr());
	if (p->updated)
		printf("  Version %lld %s: %p\n", p->lastUpdateVersion, p->replacedPointer ? "Right" : "Left", p->pointer[2].getPtr());
	for(int i=0; i<3; i++)
		if (p->pointer[i]) printTreeDetails(p->pointer[i], depth+1);
}

/*static int depth(const Reference<PTree<int>>& p, Version at) {
  if (!p) return 0;
  int d1 = depth(p->left(at), at) + 1;
  int d2 = depth(p->right(at), at) + 1;
  return d1 > d2 ? d1 : d2;
  }*/

template <class T, class A = FastAlloc<T>>
void validate(const Reference<PTree<T,A>>& p, Version at, T* min, T* max, int& count, int& height, int depth=0) {
	if (!p) { height=0; return; }
	ASSERT( (!min || *min <= p->data) && (!max || p->data <= *max) );
	for (int i=0;i<2;i++){
		if (p->child(i, at))
			ASSERT(p->child(i, at)->priority <= p->priority);
	}

	++count;
	int h1, h2;
	validate(p->left(at), at, min, &p->data, count, h1, depth+1);
	validate(p->right(at), at, &p->data, max, count, h2, depth+1);
	height = std::max(h1, h2) + 1;
}

template<class T, class A = FastAlloc<T>>
void check(const Reference<PTree<T,A>>& p){
	int count=0, height;
	validate(p, (T*)0, (T*)0, count, height);
	if (count && height > 4.3 * log(double(count))){
		//printf("height %d; count %d\n", height, count);
		ASSERT(false);
	}
}

//Remove pointers to any child nodes that have been updated at or before the given version
//This essentially gets rid of node versions that will never be read (beyond 5s worth of versions)
//TODO look into making this per-version compaction. (We could keep track of updated nodes at each version for example)
template <class T, class A = FastAlloc<T>>
void compact(Reference<PTree<T,A>>& p, Version newOldestVersion){
	if (!p) {
		return;
	}
	if (p->updated && p->lastUpdateVersion <= newOldestVersion) {
		/* If the node has been updated, figure out which pointer was replaced. And replace that pointer with the updated pointer.
		   Then we can get rid of the updated child pointer and then make room in the node for future updates */
		auto which = p->replacedPointer;
		p->pointer[which] = p->pointer[2];
		p->updated = false;
		p->pointer[2] = Reference<PTree<T,A>>();
		//p->pointer[which] = Reference<PTree<T>>();
	}
	Reference<PTree<T,A>> left = p->left(newOldestVersion);
	Reference<PTree<T,A>> right = p->right(newOldestVersion);
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

	ValueRef const& getValue() const { ASSERT( isValue() ); return item; };
	KeyRef const&  getEndKey() const { ASSERT(isClearTo()); return item; };

private:
	ValueOrClearToRef( StringRef item, bool isClear ) : item(item), isClear(isClear) {}
	StringRef item;
	bool isClear;
};

// VersionedMap provides an interface to a partially persistent tree, allowing you to read the values at a particular version,
// create new versions, modify the current version of the tree, and forget versions prior to a specific version.
template<class K, class T, class A = FastAlloc<K>>
class VersionedMap : NonCopyable {
//private:
public:
	using PTreeValue = MapPair<K,std::pair<T,Version>>;
	using PTreeT = PTreeImpl::PTree<PTreeValue, A>;
	using Tree = Reference<PTreeT>;
	using allocator_type = A;
	allocator_type allocator;

	Version oldestVersion, latestVersion;

	// This deque keeps track of PTree root nodes at various versions. Since the
	// versions increase monotonically, the deque is implicitly sorted and hence
	// binary-searchable.
	std::deque<std::pair<Version, Tree>> roots;

	struct rootsComparator {
		bool operator()(const std::pair<Version, Tree>& value, const Version& key)
		{
			return (value.first < key);
		}
		bool operator()(const Version& key, const std::pair<Version, Tree>& value)
		{
			return (key < value.first);
		}
	};

	Tree const& getRoot( Version v ) const {
		auto r = upper_bound(roots.begin(), roots.end(), v, rootsComparator());
		--r;
		return r->second;
	}

	// For each item in the versioned map, 4 PTree nodes are potentially allocated:
	static const int overheadPerItem = nextFastAllocatedSize(sizeof(PTreeT)) * 4;
	struct iterator;

	VersionedMap(A alloc = A{}) : allocator(alloc), oldestVersion(0), latestVersion(0) {
		roots.emplace_back(0, Tree());
	}
	VersionedMap( VersionedMap&& v, A alloc=A{} ) BOOST_NOEXCEPT : allocator(alloc), oldestVersion(v.oldestVersion), latestVersion(v.latestVersion), roots(std::move(v.roots)) {
	}
	void operator = (VersionedMap && v) BOOST_NOEXCEPT {
		oldestVersion = v.oldestVersion;
		latestVersion = v.latestVersion;
		roots = std::move(v.roots);
	}

	Version getLatestVersion() const { return latestVersion; }
	Version getOldestVersion() const { return oldestVersion; }

	//front element should be the oldest version in the deque, hence the next oldest should be at index 1
	Version getNextOldestVersion() const { return roots[1]->first; }

	void forgetVersionsBefore(Version newOldestVersion) {
		ASSERT( newOldestVersion <= latestVersion );
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

	Future<Void> forgetVersionsBeforeAsync( Version newOldestVersion, TaskPriority taskID = TaskPriority::DefaultYield ) {
		ASSERT( newOldestVersion <= latestVersion );
		auto r = upper_bound(roots.begin(), roots.end(), newOldestVersion, rootsComparator());
		auto upper = r;
		--r;
		// if the specified newOldestVersion does not exist, insert a new
		// entry-pair with newOldestVersion and the root from next lower version
		if (r->first != newOldestVersion) {
			r = roots.emplace(upper, newOldestVersion, getRoot(newOldestVersion));
		}

		UNSTOPPABLE_ASSERT(r->first == newOldestVersion);

		vector<Tree> toFree;
		toFree.reserve(10000);
		auto newBegin = r;
		Tree *lastRoot = nullptr;
		for(auto root = roots.begin(); root != newBegin; ++root) {
			if(root->second) {
				if(lastRoot != nullptr && root->second == *lastRoot) {
					(*lastRoot).clear();
				}
				if(root->second->isSoleOwner()) {
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
	int getRootsSize() {
		return roots.size()*nextFastAllocatedSize(sizeof(PTreeT));
	}

	void createNewVersion(Version version) {     // following sets and erases are into the given version, which may now be passed to at().  Must be called in monotonically increasing order.
		if (version > latestVersion) {
			latestVersion = version;
			Tree r = getRoot(version);
			roots.emplace_back(version, r);
		} else ASSERT( version == latestVersion );
	}

	// insert() and erase() invalidate atLatest() and all iterators into it
	void insert(const K& k, const T& t) {
		insert( k, t, latestVersion );
	}
	void insert(const K& k, const T& t, Version insertAt) {
			if (PTreeImpl::contains(roots.back().second, latestVersion, k )) PTreeImpl::remove( roots.back().second, latestVersion, k, allocator); // FIXME: Make PTreeImpl::insert do this automatically  (see also WriteMap.h FIXME)
		PTreeImpl::insert( roots.back().second, latestVersion, MapPair<K,std::pair<T,Version>>(k,std::make_pair(t,insertAt)), allocator);
	}
	void erase(const K& begin, const K& end) {
		PTreeImpl::remove( roots.back().second, latestVersion, begin, end, allocator);
	}
	void erase(const K& key ) {  // key must be present
		PTreeImpl::remove( roots.back().second, latestVersion, key, allocator);
	}
	void erase(iterator const& item) {  // iterator must be in latest version!
		// SOMEDAY: Optimize to use item.finger and avoid repeated search
		K key = item.key();
		erase(key);
	}

	void printDetail() {
		PTreeImpl::printTreeDetails(roots.back().second, 0);
	}

	void printTree(Version at) {
		PTreeImpl::printTree(roots.back().second, at, 0);
	}

	void compact(Version newOldestVersion) {
		ASSERT( newOldestVersion <= latestVersion );
		auto newBegin = lower_bound(roots.begin(), roots.end(), newOldestVersion, rootsComparator());
		for(auto root = roots.begin(); root != newBegin; ++root) {
			if(root->second)
				PTreeImpl::compact(root->second, newOldestVersion);
		}
		//printf("\nPrinting the tree at latest version after compaction.\n");
		//PTreeImpl::printTreeDetails(roots.back().second(), 0);
	}

	// for(auto i = vm.at(version).lower_bound(range.begin); i < range.end; ++i)
	struct iterator{
		explicit iterator(Tree const& root, Version at) : root(root), at(at) {}

		K const& key() const { return finger.back()->data.key; }
		Version insertVersion() const { return finger.back()->data.value.second; }  // Returns the version at which the current item was inserted
		operator bool() const { return finger.size()!=0; }
		bool operator < (const K& key) const { return this->key() < key; }

		T const& operator*() { return finger.back()->data.value.first; }
		T const* operator->() { return &finger.back()->data.value.first; }
		void operator++() { if (finger.size()) PTreeImpl::next( at, finger ); else PTreeImpl::first(root, at, finger); }
		void operator--() { if (finger.size()) PTreeImpl::previous( at, finger ); else PTreeImpl::last(root, at, finger); }
		bool operator == ( const iterator& r ) const { if (finger.size() && r.finger.size()) return finger.back() == r.finger.back(); else return finger.size()==r.finger.size(); }
		bool operator != ( const iterator& r ) const { if (finger.size() && r.finger.size()) return finger.back() != r.finger.back(); else return finger.size()!=r.finger.size(); }

	private:
		friend class VersionedMap<K,T,A>;
		Tree root;
		Version at;
		vector< PTreeT const* > finger;
	};

	class ViewAtVersion {
	public:
		ViewAtVersion(Tree const& root, Version at) : root(root), at(at) {}

		iterator begin() const { iterator i(root,at); PTreeImpl::first( root, at, i.finger ); return i; }
		iterator end() const { return iterator(root,at); }

		// Returns x such that key==*x, or end()
		template <class X>
		iterator find(const X &key) const { 
			iterator i(root,at); 
			PTreeImpl::lower_bound( root, at, key, i.finger ); 
			if (i && i.key() == key)
				return i;
			else
				return end();
		}

		// Returns the smallest x such that *x>=key, or end()
		template <class X>
		iterator lower_bound(const X &key) const {
			iterator i(root,at); 
			PTreeImpl::lower_bound( root, at, key, i.finger );
			return i;
		}

		// Returns the smallest x such that *x>key, or end()
		template <class X>
		iterator upper_bound(const X &key) const {
			iterator i(root,at); 
			PTreeImpl::upper_bound( root, at, key, i.finger );
			return i;
		}

		// Returns the largest x such that *x<=key, or end()
		template <class X>
		iterator lastLessOrEqual( const X &key ) const {
			iterator i(root,at); 
			PTreeImpl::upper_bound( root, at, key, i.finger );
			--i;
			return i;
		}

		// Returns the largest x such that *x<key, or end()
		template <class X>
		iterator lastLess( const X &key ) const {
			iterator i(root,at); 
			PTreeImpl::lower_bound( root, at, key, i.finger );
			--i;
			return i;
		}

		void validate() {
			int count=0, height=0;
			PTreeImpl::validate<MapPair<K,std::pair<T,Version>>>( root, at, NULL, NULL, count, height );
			if ( height > 100 )
				TraceEvent(SevWarnAlways, "DiabolicalPTreeSize").detail("Size", count).detail("Height", height);
		}
	private:
		Tree root;
		Version at;
	};

	ViewAtVersion at( Version v ) const { return ViewAtVersion(getRoot(v), v); }
	ViewAtVersion atLatest() const { return ViewAtVersion(roots.back().second, latestVersion); }

	bool isClearContaining( ViewAtVersion const& view, KeyRef key ) {
		auto i = view.lastLessOrEqual(key);
		return i && i->isClearTo() && i->getEndKey() > key;
	}

	// TODO: getHistory?

};

#endif
