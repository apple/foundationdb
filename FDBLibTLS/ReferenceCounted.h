/*
 * ReferenceCounted.h
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

#ifndef FDB_REFERENCE_COUNTED_H
#define FDB_REFERENCE_COUNTED_H

#pragma once

#include <stdlib.h>

template <class T>
struct ReferenceCounted {
	void addref() { ++referenceCount; }
	void delref() { if (--referenceCount == 0) { delete (T*)this; } }

	ReferenceCounted() : referenceCount(1) {}

private:
	ReferenceCounted(const ReferenceCounted&) = delete;
	void operator=(const ReferenceCounted&) = delete;
	int32_t referenceCount;
};

template <class P>
void addref(P* ptr) { ptr->addref(); }
template <class P>
void delref(P* ptr) { ptr->delref(); }

template <class P>
struct Reference {
	Reference() : ptr(NULL) {}
	explicit Reference( P* ptr ) : ptr(ptr) {}
	static Reference<P> addRef( P* ptr ) { ptr->addref(); return Reference(ptr); }

	Reference(const Reference& r) : ptr(r.getPtr()) { if (ptr) addref(ptr); }
	Reference(Reference && r) : ptr(r.getPtr()) { r.ptr = NULL; }

	template <class Q>
	Reference(const Reference<Q>& r) : ptr(r.getPtr()) { if (ptr) addref(ptr); }
	template <class Q>
	Reference(Reference<Q> && r) : ptr(r.getPtr()) { r.setPtrUnsafe(NULL); }

	~Reference() { if (ptr) delref(ptr); }
	Reference& operator=(const Reference& r) {
		P* oldPtr = ptr;
		P* newPtr = r.ptr;
		if (oldPtr != newPtr) {
			if (newPtr) addref(newPtr);
			ptr = newPtr;
			if (oldPtr) delref(oldPtr);
		}
		return *this;
	}
	Reference& operator=(Reference&& r) {
		P* oldPtr = ptr;
		P* newPtr = r.ptr;
		if (oldPtr != newPtr) {
			r.ptr = NULL;
			ptr = newPtr;
			if (oldPtr) delref(oldPtr);
		}
		return *this;
	}

	void clear() {
		P* oldPtr = ptr;
		if (oldPtr) {
			ptr = NULL;
			delref(oldPtr);
		}
	}

	P* operator->() const { return ptr; }
	P& operator*() const { return *ptr; }
	P* getPtr() const { return ptr; }

	void setPtrUnsafe( P* p ) { ptr = p; }

	P* extractPtr() { auto *p = ptr; ptr = NULL; return p; }

	bool boolean_test() const { return ptr != 0; }
private:
	P *ptr;
};

template <class P> 
bool operator==( const Reference<P>& lhs, const Reference<P>& rhs ) {
	return lhs.getPtr() == rhs.getPtr();
}

#endif /* FDB_REFERENCE_COUNTED_H */
