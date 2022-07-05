/*
 * OwningResource.h
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

#ifndef FLOW_SAFE_ACCESS_REF_H
#define FLOW_SAFE_ACCESS_REF_H

#include "flow/FastRef.h"

// Consider the following situation:
//
//   1. An ACTOR A0 allocates an object O
//   2. A0 spawns another ACTOR A1, which depends on O
//   3. A0 triggers A1 and then terminates, destroying O
//   4. Since A1 is triggered by A0 while not knowing A0 is terminated and O is released, it would cause a SEGV error.
//
// In this header file, two classes
//
//   * ResourceOwningRef
//   * ResourceWeakRef
//
// are provided. The ResourceOwningRef is the reference that "holds" the resource, When it is destructed, the resource
// is also released; while ResourceWeakRef is the reference that "weakly holds" the resource. Before each access, it is
// the user's responsibility to verify if the resource is still available, via the available() method.
//
// With the two classes, the issue above can be solved by:
//
//    1. A0 allocates the object O via ResourceOwningRef
//    2. A0 forwards O to A1, via ResourceWeakRef
//    3. Every time A1 accesses O, it will verify if the resource is still available.
//    4. When A0 terminates, O is released and all ResourceWeakRef available() call will report the resource is not
//       available anymore, preventing the SEGV error being raised.

namespace details {

// The class holding the pointer to the resource.
// SOMEDAY: Think using std::unique_ptr
template <typename T>
struct Resource : public ReferenceCounted<Resource<T>>, NonCopyable {
	T* resource;

	Resource(T* resource_) : resource(resource_) {}
	~Resource() { delete resource; }

	void reset(T* resource_) {
		delete resource;
		resource = resource_;
	}
};

template <typename T>
class ResourceRef {
protected:
	Reference<Resource<T>> resourceRef;

public:
	ResourceRef(const Reference<Resource<T>>& ref) : resourceRef(ref) {}
	ResourceRef(Reference<Resource<T>>&& ref) : resourceRef(std::move(ref)) {}
	ResourceRef& operator=(const Reference<Resource<T>>& ref) {
		resourceRef = ref.resourceRef;
		return *this;
	}
	ResourceRef& operator=(Reference<Resource<T>>&& ref) {
		resourceRef = std::move(ref);
		return *this;
	}

	T* operator->() { return resourceRef->resource; }
	T& operator*() {
		if (resourceRef->resource == nullptr) {
			throw internal_error();
		} else {
			return *(resourceRef->resource);
		}
	}

	bool available() const { return resourceRef->resource != nullptr; }
};

} // namespace details

// The class that holds a Reference to the details::Resource which holds the real object. If the instance is destroyed,
// the object is destroyed, too.
template <typename T>
class ResourceOwningRef : public details::ResourceRef<T>, NonCopyable {
	template <typename U>
	friend class ResourceWeakRef;

public:
	ResourceOwningRef(T* resource) : details::ResourceRef<T>(makeReference<details::Resource<T>>(resource)) {}
	~ResourceOwningRef() { details::ResourceRef<T>::resourceRef->reset(nullptr); }
};

// The class that weakly holds a Reference tot he etails::Resource. Destroying the reference will have no impact to the
// real object. On the other hand, each time accessing the object requires a verification that the object is still alive
template <typename T>
class ResourceWeakRef : public details::ResourceRef<T> {
public:
	ResourceWeakRef(const ResourceOwningRef<T>& ref) : details::ResourceRef<T>(ref.resourceRef) {}
	ResourceWeakRef(const ResourceWeakRef& ref) : details::ResourceRef<T>(ref.resourceRef) {}
};

#endif // FLOW_SAFE_ACCESS_REF_H