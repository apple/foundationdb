/*
 * FastRef.h
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

#ifndef FLOW_FASTREF_H
#define FLOW_FASTREF_H
#pragma once

#include <atomic>
#include <cstdint>

// The thread safety this class provides is that it's safe to call addref and
// delref on the same object concurrently in different threads. Subclass does
// not get deleted until after all calls to delref complete.
//
// Importantly, this class does _not_ make accessing Subclass automatically
// thread safe. Clients will need to provide their own external synchronization
// for that.
template <class Subclass>
class ThreadSafeReferenceCounted {
public:
	ThreadSafeReferenceCounted() : referenceCount(1) {}
	// NO virtual destructor!  Subclass should have a virtual destructor if it is not sealed.
	void addref() const { referenceCount.fetch_add(1); }
	// If return value is true, caller is responsible for destruction of object
	bool delref_no_destroy() const {
		// The performance of this seems comparable to a version with less strict memory ordering (see e.g.
		// https://www.boost.org/doc/libs/1_57_0/doc/html/atomic/usage_examples.html#boost_atomic.usage_examples.example_reference_counters),
		// on both x86 and ARM, with gcc8.
		return referenceCount.fetch_sub(1) == 1;
	}
	void delref() const {
		if (delref_no_destroy())
			delete (Subclass*)this;
	}
	void setrefCountUnsafe(int32_t count) const { referenceCount.store(count); }
	int32_t debugGetReferenceCount() const { return referenceCount.load(); }

private:
	ThreadSafeReferenceCounted(const ThreadSafeReferenceCounted&) /* = delete*/;
	void operator=(const ThreadSafeReferenceCounted&) /* = delete*/;
	mutable std::atomic<int32_t> referenceCount;
};

template <class Subclass>
class ThreadUnsafeReferenceCounted {
public:
	ThreadUnsafeReferenceCounted() : referenceCount(1) {}
	// NO virtual destructor!  Subclass should have a virtual destructor if it is not sealed.
	void addref() const { ++referenceCount; }
	void delref() const {
		if (delref_no_destroy())
			delete (Subclass*)this;
	}
	bool delref_no_destroy() const { return !--referenceCount; }
	int32_t debugGetReferenceCount() const { return referenceCount; } // Never use in production code, only for tracing
	bool isSoleOwner() const { return referenceCount == 1; }

private:
	ThreadUnsafeReferenceCounted(const ThreadUnsafeReferenceCounted&) /* = delete*/;
	void operator=(const ThreadUnsafeReferenceCounted&) /* = delete*/;
	mutable int32_t referenceCount;
};

#if FLOW_THREAD_SAFE
#define ReferenceCounted ThreadSafeReferenceCounted
#else
#define ReferenceCounted ThreadUnsafeReferenceCounted
#endif

template <class P>
void addref(P* ptr) {
	ptr->addref();
}

template <class P>
void delref(P* ptr) {
	ptr->delref();
}

template <class P>
class Reference {
public:
	Reference() : ptr(nullptr) {}
	explicit Reference(P* ptr) : ptr(ptr) {}
	static Reference<P> addRef(P* ptr) {
		ptr->addref();
		return Reference(ptr);
	}

	Reference(const Reference& r) : ptr(r.getPtr()) {
		if (ptr)
			addref(ptr);
	}
	Reference(Reference&& r) noexcept : ptr(r.getPtr()) { r.ptr = nullptr; }

	template <class Q>
	Reference(const Reference<Q>& r) : ptr(r.getPtr()) {
		if (ptr)
			addref(ptr);
	}
	template <class Q>
	Reference(Reference<Q>&& r) : ptr(r.getPtr()) {
		r.setPtrUnsafe(nullptr);
	}

	~Reference() {
		if (ptr)
			delref(ptr);
	}
	Reference& operator=(const Reference& r) {
		P* oldPtr = ptr;
		P* newPtr = r.ptr;
		if (oldPtr != newPtr) {
			if (newPtr)
				addref(newPtr);
			ptr = newPtr;
			if (oldPtr)
				delref(oldPtr);
		}
		return *this;
	}
	Reference& operator=(Reference&& r) noexcept {
		P* oldPtr = ptr;
		P* newPtr = r.ptr;
		if (oldPtr != newPtr) {
			r.ptr = nullptr;
			ptr = newPtr;
			if (oldPtr)
				delref(oldPtr);
		}
		return *this;
	}

	void clear() {
		P* oldPtr = ptr;
		if (oldPtr) {
			ptr = nullptr;
			delref(oldPtr);
		}
	}

	P* operator->() const { return ptr; }
	P& operator*() const { return *ptr; }
	P* getPtr() const { return ptr; }

	void setPtrUnsafe(P* p) { ptr = p; }

	P* extractPtr() {
		auto* p = ptr;
		ptr = nullptr;
		return p;
	}

	template <class T>
	Reference<T> castTo() {
		return Reference<T>::addRef((T*)ptr);
	}

	bool isValid() const { return ptr != nullptr; }
	explicit operator bool() const { return ptr != nullptr; }

private:
	P* ptr;
};

template <class P, class... Args>
Reference<P> makeReference(Args&&... args) {
	return Reference<P>(new P(std::forward<Args>(args)...));
}

template <class P>
bool operator==(const Reference<P>& lhs, const Reference<P>& rhs) {
	return lhs.getPtr() == rhs.getPtr();
}
template <class P>
bool operator!=(const Reference<P>& lhs, const Reference<P>& rhs) {
	return !(lhs == rhs);
}

#endif
