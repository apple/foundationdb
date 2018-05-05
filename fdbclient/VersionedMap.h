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
#include "FDBTypes.h"
#include "flow/IRandom.h"
#include "VersionedMap.actor.h"

// PTree is a persistent balanced binary tree implementation. It is based on a treap as a way to guarantee O(1) space for node insertion (rotating is asymptotically cheap), 
// but the constant factors are very large.
//
// Each node has three pointers - the first two are its left and right children, respectively, and the third can be set to point to a newer version of the node. 
// This third pointer allows us to maintain persistence without full path copying, and is employed to achieve O(1) space node insertion.
//
// PTree also supports efficient finger searches.
namespace PTreeImpl {

	#pragma warning(disable: 4800)

	template<class T>
	struct PTree : public ReferenceCounted<PTree<T>>, FastAllocated<PTree<T>>, NonCopyable {
		uint32_t priority;
		Reference<PTree> pointer[3];
		Version lastUpdateVersion;
		bool updated;
		bool replacedPointer;
		T data;

		Reference<PTree> child(bool which, Version at) const {
			if (updated && lastUpdateVersion<=at && which == replacedPointer)
				return pointer[2];
			else 
				return pointer[which];
		}
		Reference<PTree> left(Version at) const { return child(false, at); }
		Reference<PTree> right(Version at) const { return child(true, at); }

		PTree(const T& data, Version ver) : data(data), lastUpdateVersion(ver), updated(false) {
			priority = g_random->randomUInt32();
		}
		PTree( uint32_t pri, T const& data, Reference<PTree> const& left, Reference<PTree> const& right, Version ver ) : priority(pri), data(data), lastUpdateVersion(ver), updated(false) {
			pointer[0] = left; pointer[1] = right;
		}
	private:
		PTree(PTree const&);
	};

	template<class T>
	static Reference<PTree<T>> update( Reference<PTree<T>> const& node, bool which, Reference<PTree<T>> const& ptr, Version at ) {
		if (ptr.getPtr() == node->child(which, at).getPtr()/* && node->replacedVersion <= at*/) {
			return node;
		}
		if (node->lastUpdateVersion == at) {
			//&& (!node->updated || node->replacedPointer==which)) {
			if (node->updated && node->replacedPointer != which) {
				// We are going to have to copy this node, but its aux pointer will never be used again
				// and should drop its reference count
				Reference<PTree<T>> r;
				if (which)
					r = Reference<PTree<T>>( new PTree<T>( node->priority, node->data, node->child(0, at), ptr, at ) );
				else
					r = Reference<PTree<T>>( new PTree<T>( node->priority, node->data, ptr, node->child(1, at), at ) );
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
			if (which)
				return Reference<PTree<T>>( new PTree<T>( node->priority, node->data, node->child(0, at), ptr, at ) );
			else
				return Reference<PTree<T>>( new PTree<T>( node->priority, node->data, ptr, node->child(1, at), at ) );
		} else {
			node->lastUpdateVersion = at;
			node->replacedPointer = which;
			node->pointer[2] = ptr;
			node->updated = true;
			return node;
		}
	}

	template<class T, class X>
	bool contains(const Reference<PTree<T>>& p, Version at, const X& x) {
		if (!p) return false;
		bool less = x < p->data;
		if (!less && !(p->data<x)) return true;  // x == p->data
		return contains(p->child(!less, at), at, x);
	}

	template<class T, class X>
	void lower_bound(const Reference<PTree<T>>& p, Version at, const X& x, std::vector<const PTree<T>*>& f){
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

	template<class T, class X>
	void upper_bound(const Reference<PTree<T>>& p, Version at, const X& x, std::vector<const PTree<T>*>& f){
		if (!p) {
			while (f.size() && !(x < f.back()->data))
				f.pop_back();
			return;
		}
		f.push_back(p.getPtr());
		upper_bound(p->child(!(x < p->data), at), at, x, f);
	}
	
	template<class T, bool forward>
	void move(Version at, std::vector<const PTree<T>*>& f){
		ASSERT(f.size());
		const PTree<T> *n;
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

	template<class T, bool forward>
	int halfMove(Version at, std::vector<const PTree<T>*>& f) {
		// Post: f[:return_value] is the finger that would have been returned by move<forward>(at,f), and f[:original_length_of_f] is unmodified
		ASSERT(f.size());
		const PTree<T> *n;
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

	template<class T>
	void next(Version at, std::vector<const PTree<T>*>& f){
		move<T,true>(at, f);
	}
	
	template<class T>
	void previous(Version at, std::vector<const PTree<T>*>& f){
		move<T,false>(at, f);
	}

	template<class T>
	int halfNext(Version at, std::vector<const PTree<T>*>& f){
		return halfMove<T,true>(at, f);
	}
	
	template<class T>
	int halfPrevious(Version at, std::vector<const PTree<T>*>& f){
		return halfMove<T,false>(at, f);
	}

	template<class T>
	T get(std::vector<const PTree<T>*>& f){
		ASSERT(f.size());
		return f.back()->data;
	}

	// Modifies p to point to a PTree with x inserted
	template<class T>
	void insert(Reference<PTree<T>>& p, Version at, const T& x) {
		if (!p){
			p = Reference<PTree<T>>(new PTree<T>(x, at));
		} else {
			bool direction = !(x < p->data);
			Reference<PTree<T>> child = p->child(direction, at);
			insert(child, at, x);
			p = update(p, direction, child, at);
			if (p->child(direction, at)->priority > p->priority)
				rotate(p, at, !direction);
		}
	}

	template<class T>
	Reference<PTree<T>> firstNode(const Reference<PTree<T>>& p, Version at) {
		if (!p) ASSERT(false);
		if (!p->left(at)) return p;
		return firstNode(p->left(at), at);
	}

	template<class T>
	Reference<PTree<T>> lastNode(const Reference<PTree<T>>& p, Version at) {
		if (!p) ASSERT(false);
		if (!p->right(at)) return p;
		return lastNode(p->right(at), at);
	}

	template<class T, bool last>
	void firstOrLastFinger(const Reference<PTree<T>>& p, Version at, std::vector<const PTree<T>*>& f) {
		if (!p) return;
		f.push_back(p.getPtr());
		firstOrLastFinger<T, last>(p->child(last, at), at, f);
	}
	
	template<class T>
	void first(const Reference<PTree<T>>& p, Version at, std::vector<const PTree<T>*>& f) {
		return firstOrLastFinger<T, false>(p, at, f);
	}

	template<class T>
	void last(const Reference<PTree<T>>& p, Version at, std::vector<const PTree<T>*>& f) {
		return firstOrLastFinger<T, true>(p, at, f);
	}

	// modifies p to point to a PTree with the root of p removed
	template<class T>
	void removeRoot(Reference<PTree<T>>& p, Version at) {
		if (!p->right(at))
			p = p->left(at);
		else if (!p->left(at))
			p = p->right(at);
		else {
			bool direction = p->right(at)->priority < p->left(at)->priority;
			rotate(p,at,direction);
			Reference<PTree<T>> child = p->child(direction, at);
			removeRoot(child, at);
			p = update(p, direction, child, at);
		}
	}

	// changes p to point to a PTree with x removed
	template<class T, class X>
	void remove(Reference<PTree<T>>& p, Version at, const X& x) {
		if (!p) ASSERT(false); // attempt to remove item not present in PTree
		if (x < p->data) {
			Reference<PTree<T>> child = p->child(0, at);
			remove(child, at, x);
			p = update(p, 0, child, at);
		} else if (p->data < x) {
			Reference<PTree<T>> child = p->child(1, at);
			remove(child, at, x);
			p = update(p, 1, child, at);
		} else
			removeRoot(p, at);
	}

	template<class T, class X>
	void remove(Reference<PTree<T>>& p, Version at, const X& begin, const X& end) {
		if (!p) return;
		int beginDir, endDir;
		if (begin < p->data) beginDir = -1;
		else if (p->data < begin) beginDir = +1;
		else beginDir = 0;
		if (!(p->data < end)) endDir = -1;
		else endDir = +1;

		if (beginDir == endDir) {
			Reference<PTree<T>> child = p->child(beginDir==+1, at);
			remove( child, at, begin, end );
			p = update(p, beginDir==+1, child, at);
		} else {
			if (beginDir==-1) {
				Reference<PTree<T>> left = p->child(0, at);
				removeBeyond(left, at, begin, 1);
				p = update(p, 0, left, at);
			}
			if (endDir==+1) {
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
		if (!p) return;

		if ( (p->data < pivot)^dir ) {
			p = p->child(!dir, at);
			removeBeyond(p, at, pivot, dir );
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
	template<class T>
	void demoteRoot(Reference<PTree<T>>& p, Version at){
		if (!p) ASSERT(false);

		uint32_t priority[2];
		for (int i=0;i<2;i++)
			if (p->child(i, at)) priority[i] = p->child(i, at)->priority;
			else priority[i] = 0;

		bool higherDirection = priority[1] > priority[0];

		if (priority[higherDirection] < p->priority) return; 

		// else, child(higherDirection) is a greater priority than us and the other child...
		rotate(p, at, !higherDirection);
		Reference<PTree<T>> child = p->child(!higherDirection, at);
		demoteRoot(child, at);
		p = update(p, !higherDirection, child, at);
	}

	template<class T>
	Reference<PTree<T>> append(const Reference<PTree<T>>& left, const Reference<PTree<T>>& right, Version at) {
		if (!left) return right;
		if (!right) return left;

		Reference<PTree<T>> r = Reference<PTree<T>>(new PTree<T>(lastNode(left, at)->data, at));
		ASSERT( r->data < firstNode(right, at)->data);
		Reference<PTree<T>> a = left;
		remove(a, at, r->data);

		r->pointer[0] = a;
		r->pointer[1] = right;
		demoteRoot(r, at);
		return r;
	}

	template<class T, class X>
	void split(Reference<PTree<T>> p, const X& x, Reference<PTree<T>>& left, Reference<PTree<T>>& right, Version at) {
		if (!p){
			left = Reference<PTree<T>>();
			right = Reference<PTree<T>>();
			return;
		}

		if (p->data < x){
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

	template<class T>
	void rotate(Reference<PTree<T>>& p, Version at, bool right){
		auto r = p->child(!right, at);

		auto n1 = r->child(!right, at);
		auto n2 = r->child(right, at);
		auto n3 = p->child(right, at);

		auto newC = update( p, !right, n2, at );
		newC = update( newC, right, n3, at );
		p = update( r, !right, n1, at );
		p = update( p, right, newC, at );
	}

	template <class T>
	void printTree(const Reference<PTree<T>>& p, Version at, int depth = 0) {
		if (p->left(at)) printTree(p->left(at), at, depth+1);
		for (int i=0;i<depth;i++)
			printf("  ");
		printf(":%s\n", describe(p->data).c_str());
		if (p->right(at)) printTree(p->right(at), at, depth+1);
	}

	template <class T>
	void printTreeDetails(const Reference<PTree<T>>& p, int depth = 0) {
		printf("Node %p (depth %d): %s\n", p.getPtr(), depth, describe(p->data).c_str());
		printf("  Left: %p\n", p->pointer[0].getPtr());
		printf("  Right: %p\n", p->pointer[1].getPtr());
		if (p->pointer[2])
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

	template <class T>
	void validate(const Reference<PTree<T>>& p, Version at, T* min, T* max, int& count, int& height, int depth=0) {
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

	template<class T>
	void check(const Reference<PTree<T>>& p){
		int count=0, height;
		validate(p, (T*)0, (T*)0, count, height);
		if (count && height > 4.3 * log(double(count))){
			//printf("height %d; count %d\n", height, count);
			ASSERT(false);
		}
	}

}

// VersionedMap provides an interface to a partially persistent tree, allowing you to read the values at a particular version,
// create new versions, modify the current version of the tree, and forget versions prior to a specific version.
template <class K, class T>
class VersionedMap : NonCopyable {
//private:
public:
	typedef PTreeImpl::PTree<MapPair<K,std::pair<T,Version>>> PTreeT;
	typedef Reference< PTreeT > Tree;

	Version oldestVersion, latestVersion;
	std::map<Version, Tree> roots;
	Tree *latestRoot;

	Tree const& getRoot( Version v ) const {
		auto r = roots.upper_bound(v);
		--r;
		return r->second;
	}

	static const int overheadPerItem = 128*4;
	struct iterator;

	VersionedMap() : oldestVersion(0), latestVersion(0) {
		latestRoot = &roots[0];
	}
	VersionedMap( VersionedMap&& v ) noexcept(true) : oldestVersion(v.oldestVersion), latestVersion(v.latestVersion), roots(std::move(v.roots)) {
		latestRoot = &roots[latestVersion];
	}
	void operator = (VersionedMap && v) noexcept(true) {
		oldestVersion = v.oldestVersion;
		latestVersion = v.latestVersion;
		roots = std::move(v.roots);
		latestRoot = &roots[latestVersion];
	}

	Version getLatestVersion() const { return latestVersion; }
	Version getOldestVersion() const { return oldestVersion; }
	Version getNextOldestVersion() const { return roots.upper_bound(oldestVersion)->first; }

	void forgetVersionsBefore(Version newOldestVersion) {
		ASSERT( newOldestVersion <= latestVersion );
		roots[newOldestVersion] = getRoot(newOldestVersion);
		roots.erase(roots.begin(), roots.lower_bound(newOldestVersion));
		oldestVersion = newOldestVersion;
	}

	Future<Void> forgetVersionsBeforeAsync( Version newOldestVersion, int taskID = 7000 ) {
		ASSERT( newOldestVersion <= latestVersion );
		roots[newOldestVersion] = getRoot(newOldestVersion);

		vector<Tree> toFree;
		auto newBegin = roots.lower_bound(newOldestVersion);
		for(auto root = roots.begin(); root != roots.end() && root != newBegin; ++root) {
			if(root->second && root->second->isSoleOwner())
				toFree.push_back(root->second);
		}

		roots.erase(roots.begin(), newBegin);
		oldestVersion = newOldestVersion;
		return deferredCleanupActor(toFree, taskID);
	}

public:
	void createNewVersion(Version version) {     // following sets and erases are into the given version, which may now be passed to at().  Must be called in monotonically increasing order.
		if (version > latestVersion) {
			latestVersion = version;
			Tree r = getRoot(version);
			latestRoot = &roots[version];
			*latestRoot = r;
		} else ASSERT( version == latestVersion );
	}

	// insert() and erase() invalidate atLatest() and all iterators into it
	void insert(const K& k, const T& t) {
		insert( k, t, latestVersion );
	}
	void insert(const K& k, const T& t, Version insertAt) {
		if (PTreeImpl::contains( *latestRoot, latestVersion, k )) PTreeImpl::remove( *latestRoot, latestVersion, k ); // FIXME: Make PTreeImpl::insert do this automatically  (see also WriteMap.h FIXME)
		PTreeImpl::insert( *latestRoot, latestVersion, MapPair<K,std::pair<T,Version>>(k,std::make_pair(t,insertAt)) );
	}
	void erase(const K& begin, const K& end) {
		PTreeImpl::remove( *latestRoot, latestVersion, begin, end );
	}
	void erase(const K& key ) {  // key must be present
		PTreeImpl::remove( *latestRoot, latestVersion, key );
	}
	void erase(iterator const& item) {  // iterator must be in latest version!
		// SOMEDAY: Optimize to use item.finger and avoid repeated search
		K key = item.key();
		erase(key);
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
		friend class VersionedMap<K,T>;
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
				TraceEvent(SevWarnAlways, "DiabolicalPTreeSize").detail("size", count).detail("height", height);
		}
	private:
		Tree root;
		Version at;
	};

	ViewAtVersion at( Version v ) const { return ViewAtVersion(getRoot(v), v); }
	ViewAtVersion atLatest() const { return ViewAtVersion(*latestRoot, latestVersion); }

	// TODO: getHistory?

};

#endif
