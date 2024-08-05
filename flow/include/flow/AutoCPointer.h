/*
 * AutoCPointer.h
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

#ifndef FLOW_AUTO_C_POINTER_H
#define FLOW_AUTO_C_POINTER_H
#include <cstddef>
#include <memory>

/*
 * Extend std::unique_ptr to apply scope semantics to C-style pointers with matching free functions
 * Also, add implicit conversion to avoid calling get()s when invoking C functions
 *
 * e.g. EVP_PKEY_new() returns EVP_PKEY*, which needs to be freed by EVP_PKEY_free():
 * AutoCPointer pkey(nullptr, &EVP_PKEY_free); // Null-initialized, won't invoke free
 * pkey.reset(EVP_PKEY_new());           // Initialized. Freed when pkey goes out of scope
 * ASSERT(!EVP_PKEY_is_a(pkey, "RSA"));  // Implicit conversion from AutoCPointer<EVP_PKEY> to EVP_PKEY*
 * pkey.release();                       // Choose not to free (useful e.g. after passing as arg to set0 call,
 * transferring ownership)
 */
template <class T, class R>
class AutoCPointer : protected std::unique_ptr<T, R (*)(T*)> {
	using ParentType = std::unique_ptr<T, R (*)(T*)>;

public:
	using DeleterType = R (*)(T*);

	AutoCPointer(T* ptr, R (*deleter)(T*)) noexcept : ParentType(ptr, deleter) {}

	AutoCPointer(std::nullptr_t, R (*deleter)(T*)) noexcept : ParentType(nullptr, deleter) {}

	using ParentType::operator bool;
	using ParentType::release;
	using ParentType::reset;

	operator T*() const { return ParentType::get(); }
};

#endif // FLOW_AUTO_C_POINTER_H
